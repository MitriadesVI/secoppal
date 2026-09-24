"""F2 — el registro canónico de invariantes es válido y sus defensas existen."""
from __future__ import annotations

from tests.contracts import registry
from tests.product_contract import scenario_schema


def _ids():
    scenarios = scenario_schema.load(scenario_schema.SCENARIOS)["scenarios"]
    quarantine = scenario_schema.load(scenario_schema.QUARANTINE)
    return ({s["id"] for s in scenarios}, {g["id"] for g in quarantine["open_product_gaps"]},
            scenarios, quarantine)


def test_the_registry_is_valid_and_every_named_defense_exists():
    scenario_ids, gap_ids, _, _ = _ids()
    assert registry.validate(registry.load(), scenario_ids=scenario_ids, gap_ids=gap_ids) == []


def test_pending_process_state_law_is_recorded_outside_the_main_registry():
    document = registry.load()
    pending = document["candidate_pending_main"]
    assert [item["id"] for item in pending] == ["INV-PROCESS-STATE-NO-SILENT-LOSS"]
    assert all(item["status"] == "CANDIDATE_PENDING_MAIN" for item in pending)
    assert all(item["id"] not in document["invariants"] for item in pending)


def test_every_p0_p1_invariant_has_an_owner_and_a_defense_or_a_declared_gap():
    invariants = registry.load()["invariants"]
    critical = {k: v for k, v in invariants.items() if v["criticality"] in {"P0", "P1"}}
    assert critical
    undefended = [k for k, v in critical.items() if not v["primary_tests"] and v["status"] != "UNDEFENDED"]
    assert undefended == []


def test_scenarios_and_gaps_only_cite_registered_invariants():
    _, _, scenarios, quarantine = _ids()
    known = set(registry.load()["invariants"])
    cited = {i for s in scenarios for i in s["invariants"]}
    cited |= {i for g in quarantine["open_product_gaps"] for i in g["invariants"]}
    assert cited - known == set()


def test_quarantine_reproductions_point_at_real_tests():
    _, _, _, quarantine = _ids()
    refs = [g["reproduction"]["nodeid"] for g in quarantine["open_product_gaps"]]
    refs += [n for d in quarantine["harness_debt"] for n in d["nodeids"]]
    assert [p for p in map(registry.reference_error, refs) if p] == []


def test_the_validator_rejects_a_broken_reference_and_a_duplicated_legacy_id():
    scenario_ids, gap_ids, _, _ = _ids()
    base = registry.load()["invariants"]
    first, second = list(base)[:2]
    broken = {"invariants": {
        first: dict(base[first], primary_tests=["tests/test_actor_routing.py::test_does_not_exist"]),
        second: dict(base[second], legacy_ids=base[first]["legacy_ids"]),
    }}
    errors = registry.validate(broken, scenario_ids=scenario_ids, gap_ids=gap_ids)
    assert any("is not defined" in e for e in errors)
    assert any("already mapped" in e for e in errors)
