"""F3 gate: only accredited READY scenarios, each with a killed seam mutant."""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
import re

import pytest

from tests.product_contract.actor_pack import build_pack_index, load_pack
from tests.product_contract.frozen_source import InjectedFailure, load_rows_jsonl
from tests.product_contract.harness import ProductHarness, Turn
from tests.product_contract.runtime_mutations import RUNTIME_MUTATIONS, install
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
                          today=date.fromisoformat(case.get("today", "2026-09-08")),
                          reference_instant=(datetime.fromisoformat(case["reference_instant"].replace("Z", "+00:00"))
                                             if case.get("reference_instant") else None))


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
    elif kind == "drop_range_upper":
        params.pop("valor_max", None)
    elif kind == "signed_to_state":
        params["estado"] = "Firmado"
    elif kind == "drop_opportunity_deadline":
        broken.source_calls = [dict(call, soql=_without_opportunity_deadline(call["soql"]))
                               for call in broken.source_calls]
    elif kind == "timeout_to_zero":
        broken.result.update(outcome="empty", outcome_reason="zero_results", response="0 contratos")
    elif kind == "drop_contract_dataset":
        broken.source_calls = [c for c in broken.source_calls if c["dataset"] != "jbjy-vk9h"]
    else:
        raise ValueError(f"unaccredited mutant {kind!r}")
    return broken


def _without_opportunity_deadline(soql: str) -> str:
    return re.sub(
        r" AND case\(fecha_de_recepcion_de IS NULL, .*?fecha_de_recepcion_de >= '[^']+'\)",
        "", soql,
    )


@pytest.mark.parametrize("case", READY, ids=lambda case: case["id"])
def test_ready_contract(case, tmp_path, monkeypatch):
    _run(case, tmp_path, monkeypatch)


@pytest.mark.parametrize("case", READY, ids=lambda case: case["id"])
def test_ready_mutation_is_detected(case, tmp_path, monkeypatch):
    turn, expect = _run(case, tmp_path, monkeypatch)
    mutation = case["mutation"]
    if mutation["kind"] in RUNTIME_MUTATIONS:
        if case["id"] == "PC-AUTH-004":
            candidates = turn.params["actor_pending"]["candidates"]
            assert len(candidates) == 2 and candidates[0]["base_number"] == "90070801"
        harness = _harness(case, tmp_path / f"mutant-{case['id']}", monkeypatch)
        hits = install(mutation["kind"], monkeypatch)
        instruction = case["turns"][0]
        mutant = harness.say(instruction["text"], chat_id=f"mutant-{case['id']}")
        assert hits["count"] > 0, mutation["kind"]
        violations = mutant.violations(expect)
    else:
        violations = _mutate(turn, mutation["kind"]).violations(expect)
    assert any(v.startswith(mutation["expected_failure"]) for v in violations), (
        case["id"], mutation, violations
    )


def test_barranquilla_collision_keeps_buyer_authority_with_frame_on(tmp_path, monkeypatch):
    case = dict(READY_BY_ID["PC-AUTH-001"], frame=True)
    harness = _harness(case, tmp_path / "frame-on", monkeypatch)
    turn = harness.say(case["turns"][0]["text"], chat_id="frame-on")
    buyer = "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"
    assert turn.result["analytical_intent"] == "entity_profile"
    assert buyer in turn.result["response"]
    assert {call["dataset"] for call in turn.source_calls} == {"p6dx-8zbt", "jbjy-vk9h"}
    assert all(call["rows"] == 1 for call in turn.source_calls)
    assert all(buyer in call["soql"] for call in turn.source_calls)
    assert all("7007080" not in call["soql"] for call in turn.source_calls)
    assert "CC 7007080" not in turn.result["response"]

    mutant_harness = _harness(case, tmp_path / "frame-on-mutant", monkeypatch)
    hits = install("force_buyer_as_actor_at_parser", monkeypatch)
    mutant = mutant_harness.say(case["turns"][0]["text"], chat_id="frame-on-mutant")
    assert hits["count"] > 0
    assert mutant.result["analytical_intent"] != "entity_profile"
    assert len(mutant.params["actor_pending"]["candidates"]) == 2
    assert mutant.source_calls == []


@pytest.mark.parametrize("scenario_id", [
    "PC-MONEY-001", "PC-MOD-001", "PC-DUAL-001", "PC-MONEY-002", "PC-OPP-001", "PC-TIME-001",
])
def test_ready_execution_mutant_is_detected(scenario_id, tmp_path, monkeypatch):
    """Mutate the source wire or client call before running the real workflow."""
    case = READY_BY_ID[scenario_id]
    harness = _harness(case, tmp_path / f"mutant-{scenario_id}", monkeypatch)
    if scenario_id in {"PC-MONEY-001", "PC-MOD-001", "PC-MONEY-002", "PC-OPP-001", "PC-TIME-001"}:
        original = harness.transport._execute

        def ignore_filter(operation, dataset, soql):
            if scenario_id == "PC-MONEY-001":
                soql = soql.replace(" AND valor_del_contrato >= 500000000", "")
            elif scenario_id == "PC-MOD-001":
                soql = soql.replace(
                    " AND modalidad_de_contratacion IN ('Licitación pública', 'Licitación pública Obra Publica')", ""
                )
            elif scenario_id == "PC-MONEY-002":
                soql = soql.replace(" AND valor_del_contrato <= 150000000", "")
            elif scenario_id == "PC-TIME-001":
                soql = soql.replace(" AND fecha_de_firma >= '2023-01-01'", "")
                soql = soql.replace(" AND fecha_de_firma <= '2023-12-31'", "")
            else:
                soql = _without_opportunity_deadline(soql)
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
        "PC-MONEY-002": "results.must_exclude_ids",
        "PC-OPP-001": "results.must_exclude_ids",
        "PC-TIME-001": "results.must_exclude_ids",
    }[scenario_id]
    assert any(v.startswith(detector) for v in violations), (scenario_id, violations)
