"""Hard predictive gate for the twelve pre-runtime HCA fresh predictions.

The expectations live in a separately hashed JSON artifact. This runner never
rewrites that artifact or its pinned SHA. It checks exact semantic outcomes,
counts every SECOP source method used by the audited workflows, and runs one
deliberate mutation for each systemic family.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

os.environ["SECOPPAL_ACTOR_PROFILE_ENABLED"] = "true"
os.environ.setdefault(
    "SECOP_ACTOR_ALIAS_INDEX_PATH", str(ROOT / "data" / "actor_alias_index.sqlite")
)
os.environ.pop("DEEPSEEK_API_KEY", None)

EVIDENCE = Path(__file__).resolve().parent
EXPECTATIONS = EVIDENCE / "predictive_expectations.json"
EXPECTED_EXPECTATIONS_SHA256 = (
    "9d18eae36a82c4af4585c6920aa9ab3df3356b9f48a31597067842d8274148a8"
)
BASE_EVIDENCE = (
    ROOT
    / "docs/audits/evidence/2026-08-26_FRESH_FINDINGS_CAUSALITY/causality_probe.json"
)
EXPECTED_BASE_EVIDENCE_SHA256 = (
    "68179d0bcbcfbfac5aa7a925f8ec78c261dd151daca22f66d3794866c914f414"
)

_POSITIVE_STATE_FIELDS = (
    "estado",
    "estado_family",
    "estado_contrato",
    "estado_de_apertura_del_proceso",
    "estado_del_procedimiento",
)
_SOURCE_METHODS = ("query", "count", "aggregate")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _topic(params: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for term in params.get("objeto") or []:
        if isinstance(term, list):
            values.extend(str(value) for value in term)
        else:
            values.append(str(term))
    return values


class SourceRecorder:
    """Fail-safe fake for every SECOP source method reachable in these cases."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def clear(self) -> None:
        self.calls.clear()

    def _record(self, method: str, result: Any):
        def call(*args: Any, **kwargs: Any) -> Any:
            self.calls.append({
                "method": method,
                "dataset": str(args[0]) if args else "",
                "statement": str(args[1]) if len(args) > 1 else "",
                "kwargs": sorted(kwargs),
            })
            return result

        return call

    def attach(self, client: Any) -> None:
        client.query = self._record("query", [])
        client.count = self._record("count", 0)
        client.aggregate = self._record("aggregate", [])
        # A derived client would open a fresh transport and evade the recorder.
        client.with_timeout = lambda *args, **kwargs: client
        client.derived_for_timeout = lambda *args, **kwargs: client

    def snapshot(self) -> dict[str, Any]:
        by_method = Counter(call["method"] for call in self.calls)
        return {
            "source_call_count_total": len(self.calls),
            "source_calls_by_method": {
                method: by_method.get(method, 0) for method in _SOURCE_METHODS
            },
        }


def _build_workflow(stores: Any) -> tuple[Any, SourceRecorder]:
    from app.config import Settings, get_settings
    from app.core.conversation_store import ConversationStore
    from app.core.orchestrator import SecopalWorkflow

    get_settings.cache_clear()
    workflow = SecopalWorkflow(
        Settings(**stores.settings_overrides, deepseek_api_key=None)
    )
    recorder = SourceRecorder()
    recorder.attach(workflow.secop_client)
    workflow.conv_store = ConversationStore(base_path=stores.conversation_dir)
    try:
        workflow.narrator.narrate_with_grounding = lambda **kwargs: None
    except Exception:  # noqa: BLE001 - optional narrator surface
        pass
    return workflow, recorder


def _seed_successful_context(workflow: Any, chat_id: str) -> None:
    from app.core.outcome import Outcome, OutcomeReason

    workflow.conv_store.append_turn(
        chat_id=chat_id,
        user_query="contratos de obra en caldas",
        response="1 resultado",
        parsed_params={
            "dataset": "contratos",
            "objeto": ["obra"],
            "departamento_resolved": "Caldas",
        },
        result_ids=["seed-result"],
        outcome=Outcome.SUCCESS,
        outcome_reason=OutcomeReason.RESULTS_FOUND,
        result_count=1,
    )


def _observe(
    case: dict[str, Any], workflow: Any, recorder: SourceRecorder
) -> dict[str, Any]:
    from app.core.followup_control_tokens import classify_control_intent
    from app.core.query_router import QueryRouter
    from app.utils.spell_correction import correct_query

    query = str(case["query"])
    if case["family"] == "SPELL":
        corrected = correct_query(query)
        return {
            "id": case["id"],
            "family": case["family"],
            "query": query,
            "corrected_query": corrected,
            "corrected_tokens": corrected.split(),
            "source_call_count_total": 0,
            "source_calls_by_method": {method: 0 for method in _SOURCE_METHODS},
        }

    params = QueryRouter().parse(query).params
    chat_id = f"predictive-{case['id'].lower()}"
    if case.get("seed_successful_context"):
        _seed_successful_context(workflow, chat_id)

    recorder.clear()
    result = workflow.run_query(query, channel="telegram", chat_id=chat_id)
    source = recorder.snapshot()
    workflow_params = result.get("parsed_params") or {}
    positive_state = {
        field: workflow_params.get(field)
        for field in _POSITIVE_STATE_FIELDS
        if workflow_params.get(field)
    }
    entity_resolution = params.get("entidad_resolution") or {}
    return {
        "id": case["id"],
        "family": case["family"],
        "query": query,
        "dataset": params.get("dataset"),
        "intent_type": params.get("intent_type"),
        "estado_family": params.get("estado_family"),
        "control_intent": classify_control_intent(query, {}),
        "workflow_intent": (
            result.get("followup_intent_type") or result.get("intent_type")
        ),
        "topic": _topic(params),
        "buyer_entity": params.get("entidad_resolved"),
        "buyer_kind": entity_resolution.get("kind"),
        "valor_min": params.get("valor_min"),
        "valor_max": params.get("valor_max"),
        "money_clarification": bool(params.get("money_clarification")),
        "needs_clarification": bool(result.get("needs_clarification")),
        "outcome": result.get("outcome"),
        "outcome_reason": result.get("outcome_reason"),
        "query_plan_reason": (result.get("query_plan") or {}).get("reason"),
        "positive_state_projection": positive_state,
        "resolved_params": result.get("resolved_params") or {},
        "response": str(result.get("response") or ""),
        **source,
    }


def _evaluate(case: dict[str, Any], observed: dict[str, Any]) -> dict[str, Any]:
    expected = case["expected"]
    checks: dict[str, bool] = {}

    direct = {
        "dataset": "dataset",
        "needs_clarification": "needs_clarification",
        "outcome": "outcome",
        "outcome_reason": "outcome_reason",
        "query_plan_reason": "query_plan_reason",
        "positive_state_projection": "positive_state_projection",
        "buyer_entity": "buyer_entity",
        "buyer_kind": "buyer_kind",
        "control_intent": "control_intent",
        "workflow_intent": "workflow_intent",
        "intent_type": "intent_type",
        "estado_family": "estado_family",
        "corrected_query": "corrected_query",
    }
    for expectation_key, observed_key in direct.items():
        if expectation_key in expected:
            checks[expectation_key] = (
                observed.get(observed_key) == expected[expectation_key]
            )

    if "expected_min" in expected:
        checks["expected_min"] = observed.get("valor_min") == expected["expected_min"]
    if "expected_max" in expected:
        checks["expected_max"] = observed.get("valor_max") == expected["expected_max"]
    if "expected_clarification" in expected:
        checks["expected_clarification"] = (
            observed.get("needs_clarification") == expected["expected_clarification"]
        )
    if "expected_execution" in expected:
        executed = observed.get("source_call_count_total", 0) > 0
        checks["expected_execution"] = executed == expected["expected_execution"]
        if not expected["expected_execution"]:
            checks["blocked_source_calls_total_zero"] = (
                observed.get("source_call_count_total") == 0
            )
    if "expected_topic" in expected:
        checks["expected_topic"] = observed.get("topic") == expected["expected_topic"]
    if "expected_resolved_params" in expected:
        checks["expected_resolved_params"] = (
            observed.get("resolved_params") == expected["expected_resolved_params"]
        )
    if "control_tokens_absent" in expected:
        topic = set(observed.get("topic") or [])
        checks["semantic_residue_none"] = not (
            topic & set(expected["control_tokens_absent"])
        )
    if "response_tokens_absent" in expected:
        response = observed.get("response", "").lower()
        checks["misleading_zero_message_absent"] = not any(
            token.lower() in response for token in expected["response_tokens_absent"]
        )
    if "tokens_absent" in expected:
        checks["tokens_absent"] = not (
            set(observed.get("corrected_tokens") or [])
            & set(expected["tokens_absent"])
        )

    return {
        **observed,
        "expected": expected,
        "checks": checks,
        "passed": bool(checks) and all(checks.values()),
    }


def _run_cases(
    cases: list[dict[str, Any]], workflow: Any, recorder: SourceRecorder
) -> list[dict[str, Any]]:
    return [_evaluate(case, _observe(case, workflow, recorder)) for case in cases]


def _case_by_id(expectations: dict[str, Any], case_id: str) -> dict[str, Any]:
    for case in [*expectations["canonical_cases"], *expectations["sentinels"]]:
        if case["id"] == case_id:
            return case
    raise KeyError(case_id)


def _mutation_result(
    name: str, case: dict[str, Any], observed: dict[str, Any]
) -> dict[str, Any]:
    evaluated = _evaluate(case, observed)
    failed_checks = [
        key for key, passed in evaluated["checks"].items() if not passed
    ]
    return {
        "family": name,
        "case": case["id"],
        "detected": not evaluated["passed"],
        "failed_checks": failed_checks,
    }


def _run_mutations(
    expectations: dict[str, Any], workflow: Any, recorder: SourceRecorder
) -> dict[str, dict[str, Any]]:
    import app.core.intent_vocabulary as intent_vocabulary
    import app.core.query_router as query_router
    import app.utils.spell_correction as spell_correction
    from app.core.money_admission import RangeVerdict
    from app.core.negation_binding import NegationBinding

    results: dict[str, dict[str, Any]] = {}

    state_case = _case_by_id(expectations, "P01")
    no_binding = lambda text, **kwargs: NegationBinding((), text, ())
    with mock.patch.object(query_router, "bind_negations", side_effect=no_binding):
        results["STATE"] = _mutation_result(
            "STATE_NEGATION", state_case, _observe(state_case, workflow, recorder)
        )

    money_case = _case_by_id(expectations, "P03")
    with mock.patch.object(
        query_router, "admit_range", return_value=RangeVerdict.ABSENT
    ):
        results["MONEY"] = _mutation_result(
            "MONEY_RANGE", money_case, _observe(money_case, workflow, recorder)
        )

    spell_case = _case_by_id(expectations, "SPELL-S01")
    weakened_protection = spell_correction.PROTECTED_VOCABULARY - {"otra"}
    with mock.patch.object(
        spell_correction, "PROTECTED_VOCABULARY", weakened_protection
    ):
        results["SPELL"] = _mutation_result(
            "SPELL", spell_case, _observe(spell_case, workflow, recorder)
        )

    buyer_case = _case_by_id(expectations, "P05")
    weakened_buyer_verbs = (
        intent_vocabulary.PROCUREMENT_ACTION_VERB_FORMS - {"celebro", "celebró"}
    )
    with mock.patch.object(
        intent_vocabulary,
        "PROCUREMENT_ACTION_VERB_FORMS",
        weakened_buyer_verbs,
    ):
        results["BUYER"] = _mutation_result(
            "BUYER_ACTION", buyer_case, _observe(buyer_case, workflow, recorder)
        )

    control_case = _case_by_id(expectations, "BIDDER-S01")
    original_extract_intent = intent_vocabulary.extract_intent

    def drop_licitar(text: str) -> tuple[dict[str, Any], str]:
        if "licitar" in text.lower():
            return {}, ""
        return original_extract_intent(text)

    with mock.patch.object(intent_vocabulary, "extract_intent", drop_licitar):
        results["CONTROL"] = _mutation_result(
            "CONTROL_BIDDER", control_case, _observe(control_case, workflow, recorder)
        )

    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default=None)
    parser.add_argument("--skip-mutations", action="store_true")
    args = parser.parse_args()

    expectation_sha_before = _sha256(EXPECTATIONS)
    baseline_sha = _sha256(BASE_EVIDENCE)
    hash_gate = (
        expectation_sha_before == EXPECTED_EXPECTATIONS_SHA256
        and baseline_sha == EXPECTED_BASE_EVIDENCE_SHA256
    )
    expectations = json.loads(EXPECTATIONS.read_text(encoding="utf-8"))

    from scripts.audits.audit_isolation import isolated_runtime_stores

    with isolated_runtime_stores(prefix="hca_predictive_hardened_") as stores:
        workflow, recorder = _build_workflow(stores)
        canonical = _run_cases(
            expectations["canonical_cases"], workflow, recorder
        )
        sentinels = _run_cases(expectations["sentinels"], workflow, recorder)
        mutations = (
            {}
            if args.skip_mutations
            else _run_mutations(expectations, workflow, recorder)
        )

    expectation_sha_after = _sha256(EXPECTATIONS)
    expectation_change_count = int(
        expectation_sha_before != EXPECTED_EXPECTATIONS_SHA256
    ) + int(expectation_sha_after != expectation_sha_before)
    all_rows = [*canonical, *sentinels]
    aggregate_methods = Counter()
    for row in all_rows:
        aggregate_methods.update(row["source_calls_by_method"])

    canonical_clear = sum(row["passed"] for row in canonical)
    sentinels_clear = sum(row["passed"] for row in sentinels)
    mutation_clear = bool(mutations) and all(
        item["detected"] for item in mutations.values()
    )
    verdict = (
        "PASS"
        if hash_gate
        and expectation_change_count == 0
        and len(canonical) == 12
        and canonical_clear == 12
        and sentinels_clear == len(sentinels)
        and (args.skip_mutations or mutation_clear)
        else "FAIL"
    )
    report = {
        "gate": "PREDICTIVE_HARDENED",
        "verdict": verdict,
        "baseline_input_sha": baseline_sha,
        "baseline_input_expected_sha": EXPECTED_BASE_EVIDENCE_SHA256,
        "expectation_sha": expectation_sha_before,
        "expectation_expected_sha": EXPECTED_EXPECTATIONS_SHA256,
        "expectation_change_count": expectation_change_count,
        "canonical_cases": len(canonical),
        "canonical_clear": canonical_clear,
        "sentinels": len(sentinels),
        "sentinels_clear": sentinels_clear,
        "source_call_count_total": sum(
            row["source_call_count_total"] for row in all_rows
        ),
        "source_calls_by_method": {
            method: aggregate_methods.get(method, 0) for method in _SOURCE_METHODS
        },
        "instrumented_source_methods": list(_SOURCE_METHODS),
        "mutations": mutations,
        "cases": canonical,
        "sentinel_cases": sentinels,
    }
    text = json.dumps(report, ensure_ascii=False, indent=1)
    if args.json:
        Path(args.json).write_text(text + "\n", encoding="utf-8")
    print(text)  # noqa: T201 - audit CLI
    raise SystemExit(0 if verdict == "PASS" else 1)


if __name__ == "__main__":
    main()
