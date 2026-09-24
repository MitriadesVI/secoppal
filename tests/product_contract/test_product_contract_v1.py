"""F3 gate: only accredited READY scenarios, each with a killed seam mutant."""
from __future__ import annotations

from copy import deepcopy
from datetime import date

import pytest

from tests.product_contract.actor_pack import build_pack_index, load_pack
from tests.product_contract.frozen_source import InjectedFailure, load_rows_jsonl
from tests.product_contract.harness import ProductHarness, Turn
from tests.product_contract.scenario_schema import FIXTURE_DIR, SCENARIOS, load

READY = [case for case in load(SCENARIOS)["scenarios"] if case["status"] == "READY"]
READY_BY_ID = {case["id"]: case for case in READY}


def _harness(case, root, monkeypatch):
    fixtures = case["fixtures"]
    source_files = fixtures.get("source_rows") or []
    if isinstance(source_files, str):
        source_files = [source_files]
    rows = {}
    for name in source_files:
        for dataset, items in load_rows_jsonl(FIXTURE_DIR / name).items():
            rows.setdefault(dataset, []).extend(items)
    actor_index = None
    if fixtures.get("actor_pack"):
        actor_index = build_pack_index(
            load_pack(FIXTURE_DIR / fixtures["actor_pack"]),
            root / "actors.sqlite", source_dataset="product-contract-v1-f3",
        )
    failures = []
    for failure in fixtures.get("failures") or []:
        if failure["exception"] != "TimeoutError":
            raise ValueError(f"unaccredited failure {failure['exception']!r}")
        failures.append(InjectedFailure(lambda: TimeoutError("frozen timeout"),
                                        operation=failure.get("operation")))
    return ProductHarness(root, monkeypatch, rows_by_dataset=rows,
                          failures=failures, actor_index=actor_index,
                          frame=case.get("frame", False),
                          today=date.fromisoformat(case.get("today", "2026-09-08")))


def _run(case, tmp_path, monkeypatch):
    harness = _harness(case, tmp_path / case["id"], monkeypatch)
    completed = []
    for instruction in case["turns"]:
        turn = harness.say(instruction["text"], chat_id=instruction.get("chat_id", case["id"]),
                           **(instruction.get("run_kwargs") or {}))
        violations = turn.violations(instruction["expect"])
        assert violations == [], f"{case['id']}: {violations}"
        completed.append(turn)
    return completed[-1], case["turns"][-1]["expect"]


def _mutate(turn: Turn, kind: str) -> Turn:
    broken = Turn(turn.text, turn.chat_id, deepcopy(turn.result), deepcopy(turn.source_calls),
                  deepcopy(turn.previous_params), turn.lineage_views)
    params = broken.result.setdefault("resolved_params", {})
    if kind == "select_top_actor":
        params["actor_scope"] = {"role": "contractor", "base_number": "70070101"}
        params["contratista"] = "70070101"
        broken.result["needs_clarification"] = False
    elif kind == "drop_money_floor":
        params.pop("valor_min", None)
    elif kind == "drop_modality":
        params.pop("modalidad_include_procedure_ids", None)
    elif kind == "timeout_to_zero":
        broken.result.update(outcome="empty", outcome_reason="zero_results", response="0 contratos")
    elif kind == "drop_contract_dataset":
        broken.source_calls = [c for c in broken.source_calls if c["dataset"] != "jbjy-vk9h"]
    else:
        raise ValueError(f"unaccredited mutant {kind!r}")
    return broken


@pytest.mark.parametrize("case", READY, ids=lambda case: case["id"])
def test_ready_contract(case, tmp_path, monkeypatch):
    _run(case, tmp_path, monkeypatch)


@pytest.mark.parametrize("case", READY, ids=lambda case: case["id"])
def test_ready_mutation_is_detected(case, tmp_path, monkeypatch):
    turn, expect = _run(case, tmp_path, monkeypatch)
    mutation = case["mutation"]
    violations = _mutate(turn, mutation["kind"]).violations(expect)
    assert any(v.startswith(mutation["expected_failure"]) for v in violations), (
        case["id"], mutation, violations
    )


@pytest.mark.parametrize("scenario_id", ["PC-MONEY-001", "PC-MOD-001", "PC-DUAL-001"])
def test_ready_execution_mutant_is_detected(scenario_id, tmp_path, monkeypatch):
    """Mutate the source wire or client call before running the real workflow."""
    case = READY_BY_ID[scenario_id]
    harness = _harness(case, tmp_path / f"mutant-{scenario_id}", monkeypatch)
    if scenario_id in {"PC-MONEY-001", "PC-MOD-001"}:
        original = harness.transport._execute

        def ignore_filter(operation, dataset, soql):
            if scenario_id == "PC-MONEY-001":
                soql = soql.replace(" AND valor_del_contrato >= 500000000", "")
            else:
                soql = soql.replace(
                    " AND modalidad_de_contratacion IN ('Licitación pública', 'Licitación pública Obra Publica')", ""
                )
            return original(operation, dataset, soql)

        monkeypatch.setattr(harness.transport, "_execute", ignore_filter)
    else:
        original_query = harness.flow.secop_client.query

        def omit_contracts(dataset, soql, **kwargs):
            if dataset == "jbjy-vk9h":
                return []
            return original_query(dataset, soql, **kwargs)

        monkeypatch.setattr(harness.flow.secop_client, "query", omit_contracts)
    instruction = case["turns"][-1]
    turn = harness.say(instruction["text"], chat_id=scenario_id)
    violations = turn.violations(instruction["expect"])
    detector = {
        "PC-MONEY-001": "results.must_exclude_ids",
        "PC-MOD-001": "results.must_exclude_ids",
        "PC-DUAL-001": "datasets_called.includes",
    }[scenario_id]
    assert any(v.startswith(detector) for v in violations), (scenario_id, violations)
