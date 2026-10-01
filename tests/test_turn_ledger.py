"""Turn ledger v1 (FOUNDATION_ENTRY_ADMISSION M0): why Foundation took or did not take a turn.

The ledger is observational. These tests pin (1) the exact ADMISSION code per
reachable gate outcome, (2) parity: OFF writes nothing anywhere, ON changes no
observable, and (3) that ``detail`` never carries the user's text.
"""

from __future__ import annotations

import ast
import json
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core import turn_ledger as tl
from app.core.foundation_runtime import eligible_first_search, first_search_ineligibility, run_foundation_search
from app.core.turn_ledger import AdmissionCode, Stage, StageName, turn_ledger_scope
from tests.test_foundation_route_ownership import (  # noqa: F401  (make_flow is a fixture)
    SEARCH, TODAY, jobs, make_flow, observable, run,
)


def trace_lines(flow) -> list[dict]:
    path = flow.settings.secop_feedback_path
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def ledger_of(line: dict) -> list[dict]:
    return line.get("turn_ledger", {}).get("stages", [])


def admission_codes(flow) -> list[str]:
    return [row["reason_code"] for line in trace_lines(flow) for row in ledger_of(line) if row["name"] == "ADMISSION"]


# ── contract of the vocabulary ────────────────────────────────────────────────
def test_stage_and_admission_vocabulary_is_closed_as_in_the_brief():
    assert [s.value for s in StageName] == ["PROTOCOL", "ALPHA", "ADMISSION", "INTERPRETER", "HOST", "CAPABILITY",
                                            "EXECUTION", "PRESENTATION", "SHADOW"]
    assert [c.value for c in AdmissionCode] == [
        "FLAG_OFF", "CONTINUATION_BLOCKED", "INELIGIBLE_SEED_OR_SELECTION", "INELIGIBLE_HISTORY",
        "INELIGIBLE_LEXICAL", "DECLINED_OWNER:ACTOR", "DECLINED_OWNER:ENTITY_PROFILE",
        "DECLINED_OWNER:CONTRACT_ACTIVITY", "DECLINED_OWNER:ANALYTICS", "DECLINED_OWNER:CAPABILITY_BINDING",
        "DECLINED_OWNER:NATURAL_ALPHA_JOB", "ADMITTED"]


def test_every_owner_the_router_can_name_has_a_closed_code():
    tree = ast.parse(Path("app/core/foundation_route_owner.py").read_text())
    owners = set()
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef) and fn.name in {"specialized_owner", "natural_alpha_owner"}:
            owners |= {n.value.value for n in ast.walk(fn) if isinstance(n, ast.Return)
                       and isinstance(n.value, ast.Constant) and isinstance(n.value.value, str)}
    assert owners == {"ACTOR", "ENTITY_PROFILE", "CONTRACT_ACTIVITY", "ANALYTICS", "CAPABILITY_BINDING",
                      "NATURAL_ALPHA_JOB"}
    assert {tl.declined_by(owner).value for owner in owners} <= {c.value for c in AdmissionCode}
    with pytest.raises(ValueError):
        tl.declined_by("NOT_AN_OWNER")


def test_eligibility_wrapper_keeps_its_boolean_contract(make_flow):
    flow = make_flow("foundation_v3")
    for text, eligible in ((SEARCH, True), ("aseo en Cali", False), ("cuenta y suma los contratos", True)):
        ctx = flow._make_ctx(text, chat_id=None, today=TODAY)
        assert eligible_first_search(flow, ctx, None) is eligible
        assert (first_search_ineligibility(flow, ctx, None) is None) is eligible


# ── ADMISSION code per reachable outcome ──────────────────────────────────────
def test_admitted_records_admission_then_the_production_stages(make_flow):
    flow = make_flow("foundation_v3")
    result = run(flow, SEARCH, chat_id="c1")
    assert result["foundation_terminal"] == "READY"
    (line,) = trace_lines(flow)
    rows = ledger_of(line)
    assert [r["name"] for r in rows] == ["ADMISSION", "INTERPRETER", "HOST", "EXECUTION"]
    admission, interpreter, host, execution = rows
    assert (admission["reason_code"], admission["decision"]) == ("ADMITTED", "ADMIT")
    assert admission["input_digest"] and len(admission["input_digest"]) == tl.DIGEST_CHARS
    telemetry = result["foundation_telemetry"]
    assert interpreter["detail"]["acceptance_status"] == telemetry["acceptance_status"] == "READY"
    assert interpreter["detail"]["proposal_valid"] is True
    assert set(interpreter["detail"]) == {"producer_version", "prompt_sha256", "provider_latency_ms",
                                          "proposal_valid", "acceptance_status"}
    assert host["detail"]["h7_authority"] == telemetry["h7_authority"] == ["PRODUCER"]
    assert host["detail"]["h7_rule_ids"] == telemetry["h7_rule_ids"]
    assert execution["detail"]["source_calls"] == telemetry["source_calls"] == len(flow.source.calls)
    assert execution["detail"]["outcome"] == result["outcome"]
    assert execution["decision"] == "READY"


def test_a_follow_up_to_a_foundation_turn_records_continuation_blocked(make_flow):
    flow = make_flow("foundation_v3")
    run(flow, SEARCH, chat_id="c1")
    follow = run(flow, "y de Cali?", chat_id="c1")
    assert follow["route_reason"] == "foundation_continuation_unsupported"
    assert admission_codes(flow) == ["ADMITTED", "CONTINUATION_BLOCKED"]
    blocked = ledger_of(trace_lines(flow)[1])
    assert [r["name"] for r in blocked] == ["ADMISSION"] and blocked[0]["decision"] == "BLOCK"


@pytest.mark.parametrize("kwargs", [
    pytest.param({"seed_resolved_params": {"topic": "aseo"}}, id="seed"),
    pytest.param({"actor_candidate_selection": {"candidate_rank": 1}}, id="actor-selection"),
    pytest.param({"profile_action_selection": {"action_id": "x"}}, id="profile-action"),
])
def test_seed_or_selection_records_ineligible_seed_or_selection(make_flow, kwargs):
    flow = make_flow("foundation_v3")
    seed = kwargs.get("seed_resolved_params")
    ctx = flow._make_ctx(SEARCH, chat_id=None, today=TODAY, **kwargs)
    with turn_ledger_scope() as ledger:
        assert run_foundation_search(flow, ctx, seed) is None
    assert [(s.name, s.reason_code) for s in ledger.stages] == [(StageName.ADMISSION, "INELIGIBLE_SEED_OR_SELECTION")]
    assert flow.foundation_producer.call_count == 0


def test_history_records_ineligible_history_and_the_ledger_is_per_turn(make_flow):
    flow = make_flow("foundation_v3")
    run(flow, "aseo en Cali", chat_id="c1")  # legacy turn: not Foundation-shaped
    run(flow, SEARCH, chat_id="c1")          # search-shaped, but the chat already has history
    assert admission_codes(flow) == ["INELIGIBLE_LEXICAL", "INELIGIBLE_HISTORY"]
    for line in trace_lines(flow):  # nothing leaks from one turn to the next
        assert [r["name"] for r in ledger_of(line)] == ["ADMISSION"]
    assert flow.foundation_producer.call_count == 0


def test_ineligibility_reasons_keep_their_precedence_seed_then_history_then_lexical(make_flow):
    flow = make_flow("foundation_v3")
    run(flow, "aseo en Cali", chat_id="c1")
    both = "aseo en Cali"  # fails the lexical gate AND the chat has history
    ctx = flow._make_ctx(both, chat_id="c1", today=TODAY)
    assert first_search_ineligibility(flow, ctx, None) is AdmissionCode.INELIGIBLE_HISTORY
    assert first_search_ineligibility(flow, ctx, {"topic": "x"}) is AdmissionCode.INELIGIBLE_SEED_OR_SELECTION
    run(flow, both, chat_id="c1")
    assert admission_codes(flow) == ["INELIGIBLE_LEXICAL", "INELIGIBLE_HISTORY"]


def test_a_text_that_is_not_search_shaped_records_ineligible_lexical(make_flow):
    flow = make_flow("foundation_v3")
    result = run(flow, "aseo en Cali")
    assert "foundation_mode" not in result
    assert admission_codes(flow) == ["INELIGIBLE_LEXICAL"]
    assert flow.foundation_producer.call_count == 0


# The real gate decides which door each text uses; these assert what it actually does.
OWNER_CODES = [
    pytest.param("contratos del proveedor FUNDACARIBE", None, "DECLINED_OWNER:ACTOR", id="actor"),
    pytest.param("busca licitaciones de alcaldía de barranquilla en 2024", None, "DECLINED_OWNER:ACTOR",
                 id="actor-legacy-parse"),
    pytest.param("busca proveedor FUNDACARIBE", None, "INELIGIBLE_LEXICAL", id="actor-name-fails-the-gate-first"),
    pytest.param("contratos de la alcaldía de Barranquilla perfil de la entidad", None,
                 "DECLINED_OWNER:ENTITY_PROFILE", id="entity-profile"),
    pytest.param("contratos que celebró la Alcaldía de Soledad en 2026 cuántos", None,
                 "DECLINED_OWNER:CONTRACT_ACTIVITY", id="contract-activity"),
    pytest.param("contratos de aseo cuál es el valor total por año", None, "DECLINED_OWNER:ANALYTICS",
                 id="analytics"),
    pytest.param("muéstrame los proveedores con más contratos de la alcaldía de Barranquilla en 2025",
                 jobs(("ANALYZE", "SUPPLIERS", "proveedores")), "DECLINED_OWNER:CAPABILITY_BINDING",
                 id="capability-binding"),
]


@pytest.mark.parametrize(("text", "alpha", "code"), OWNER_CODES)
def test_owner_decline_records_the_exact_code_and_never_calls_the_producer(make_flow, text, alpha, code):
    flow = make_flow("foundation_v3", alpha)
    result = run(flow, text, chat_id="o1")
    assert admission_codes(flow) == [code]
    assert [r["name"] for r in ledger_of(trace_lines(flow)[0])] == ["ADMISSION"]
    assert flow.foundation_producer.call_count == 0
    assert "foundation_mode" not in result


def test_a_natural_alpha_job_owner_records_natural_alpha_job(make_flow):
    flow = make_flow("foundation_v3")
    ctx = flow._make_ctx(SEARCH, chat_id=None, today=TODAY)
    observation = SimpleNamespace(mode="alpha", status="MATCHED", jobs=(SimpleNamespace(action="ANALYZE"),))
    with turn_ledger_scope() as ledger:
        assert run_foundation_search(flow, ctx, None, observation=observation, binding=None) is None
    assert [s.reason_code for s in ledger.stages] == ["DECLINED_OWNER:NATURAL_ALPHA_JOB"]


def test_a_multi_job_alpha_turn_never_reaches_foundation_so_it_has_no_row(make_flow):
    alpha = jobs(("SEARCH", "CONTRACTS", "busca contratos de aseo"),
                 ("RESULT_ACTION", "EXPORT_XLSX", "exporta los resultados"))
    flow = make_flow("foundation_v3", alpha)
    result = run(flow, "busca contratos de aseo y exporta los resultados a excel", chat_id="m1")
    assert result["route_reason"].startswith("natural_alpha_multi_job")
    assert all("turn_ledger" not in line for line in trace_lines(flow))


def test_the_provider_outage_path_records_admission_and_production_rows(make_flow):
    from app.core.interpretation_contract import AcceptanceStatus, InterpretationAcceptance

    flow = make_flow("foundation_v3")
    flow.foundation_producer.side_effect = lambda raw: InterpretationAcceptance(
        None, AcceptanceStatus.PROVIDER_ERROR, reason="PROVIDER_TIMEOUT")
    result = run(flow, SEARCH)
    assert result["foundation_terminal"] == "PROVIDER_ERROR"
    rows = ledger_of(trace_lines(flow)[0])
    assert [r["name"] for r in rows] == ["ADMISSION", "INTERPRETER", "HOST", "EXECUTION"]
    assert rows[1]["detail"]["proposal_valid"] is False and rows[1]["reason_code"] == "PROVIDER_ERROR"
    assert rows[2]["decision"] == "NOT_APPLIED"
    assert rows[3]["decision"] == "PROVIDER_ERROR"


# ── parity ────────────────────────────────────────────────────────────────────
PARITY_TURNS = [
    pytest.param([SEARCH], id="foundation-search"),
    pytest.param(["aseo en Cali"], id="lexical-legacy"),
    pytest.param(["contratos del proveedor FUNDACARIBE"], id="actor-owner"),
    pytest.param([SEARCH, "y de Cali?"], id="continuation"),
    pytest.param(["aseo en Cali", SEARCH], id="history"),
]


def run_turns(flow, turns, chat_id="p1"):
    return [run(flow, text, chat_id=chat_id) for text in turns]


@pytest.mark.parametrize("turns", PARITY_TURNS)
def test_flag_off_writes_no_ledger_anywhere(make_flow, monkeypatch, turns):
    flow = make_flow("legacy")
    seen, original = [], tl.record
    monkeypatch.setattr(tl, "record", lambda stage: (seen.append(stage), original(stage))[1])
    results = run_turns(flow, turns)
    assert seen == []
    assert all("turn_ledger" not in result for result in results)
    lines = trace_lines(flow)
    assert lines and all("turn_ledger" not in line for line in lines)
    ctx = flow._make_ctx(SEARCH, chat_id=None, today=TODAY)
    with turn_ledger_scope() as ledger:
        assert run_foundation_search(flow, ctx, None) is None
    assert ledger.stages == []


def test_flag_off_keys_equal_a_run_with_the_recorder_disabled(make_flow, monkeypatch):
    plain = make_flow("legacy")
    results = run_turns(plain, ["aseo en Cali", "muéstrame más"])
    lines = trace_lines(plain)
    monkeypatch.setattr(tl, "record", lambda stage: None)
    silent = make_flow("legacy")
    silent_results = run_turns(silent, ["aseo en Cali", "muéstrame más"])
    assert [set(r) for r in results] == [set(r) for r in silent_results]
    assert [set(line) for line in lines] == [set(line) for line in trace_lines(silent)]


def volatile_free(result):
    telemetry = dict(result.get("foundation_telemetry") or {})
    telemetry.pop("foundation_latency_ms", None)
    return {**observable(result), "owner": result.get("_continuation_owner"), "keys": sorted(result),
            "foundation": {key: result.get(key) for key in ("foundation_mode", "foundation_terminal",
                                                            "foundation_plan_decision")},
            "telemetry": {k: v for k, v in telemetry.items() if k != "provider_latency_ms"}}


@pytest.mark.parametrize("turns", PARITY_TURNS)
def test_flag_on_is_observably_identical_with_and_without_the_ledger(make_flow, monkeypatch, turns):
    with_ledger = make_flow("foundation_v3")
    results = run_turns(with_ledger, turns)
    lines = trace_lines(with_ledger)
    monkeypatch.setattr(tl, "record", lambda stage: None)
    without = make_flow("foundation_v3")
    silent_results = run_turns(without, turns)
    assert [volatile_free(r) for r in results] == [volatile_free(r) for r in silent_results]
    assert with_ledger.source.calls == without.source.calls  # same calls, same order
    assert Counter(with_ledger.source.calls) == Counter(without.source.calls)
    assert with_ledger.foundation_producer.call_count == without.foundation_producer.call_count
    assert all("turn_ledger" not in result for result in results)  # trace-only: the result never carries it
    silent_lines = trace_lines(without)
    assert all("turn_ledger" in line for line in lines) and all("turn_ledger" not in l for l in silent_lines)
    assert [set(line) - {"turn_ledger"} for line in lines] == [set(line) for line in silent_lines]


# ── detail never carries user text ────────────────────────────────────────────
PRIVATE_TEXTS = [SEARCH, "contratos del proveedor FUNDACARIBE", "aseo en Cali", "contratos de vigilancia secreta"]


def test_detail_and_rows_never_contain_the_user_text(make_flow):
    flow = make_flow("foundation_v3")
    for index, text in enumerate(PRIVATE_TEXTS):
        run(flow, text, chat_id=f"t{index}")
    lines = trace_lines(flow)
    assert len(lines) == len(PRIVATE_TEXTS)
    for text, line in zip(PRIVATE_TEXTS, lines):
        blob = json.dumps(line["turn_ledger"], ensure_ascii=False).lower()
        assert text.lower() not in blob
        for word in (w for w in text.lower().split() if len(w) >= 4 and w not in {"cali", "contratos"}):
            assert word not in blob, word
        for row in line["turn_ledger"]["stages"]:
            for value in row["detail"].values():
                assert isinstance(value, (bool, int, float, str, list, type(None)))


def test_stage_sanitizes_free_text_nested_objects_and_non_finite_numbers():
    stage = Stage(StageName.HOST, "H7", "busca contratos de aseo", reason_code="free text here",
                  detail={"query": "busca contratos de aseo", "nested": {"a": 1}, "ok": "H7_RULE.1",
                          "n": float("inf"), "flag": True, "rules": [["R1", "has space"]], "long": "x" * 200})
    row = stage.to_dict()
    assert row["decision"] == row["reason_code"] == tl.REDACTED
    assert row["detail"] == {"query": tl.REDACTED, "nested": tl.REDACTED, "ok": "H7_RULE.1", "n": None,
                             "flag": True, "rules": [["R1", tl.REDACTED]], "long": tl.REDACTED}
    json.dumps(row)


def test_digest_is_short_deterministic_and_none_safe():
    assert tl.digest(None) is None
    assert tl.digest("abc") == tl.digest("abc") and len(tl.digest("abc")) == tl.DIGEST_CHARS
    assert tl.digest({"b": 1, "a": 2}) == tl.digest({"a": 2, "b": 1})
    assert tl.digest("abc") != tl.digest("abd")


def test_record_without_an_open_turn_is_a_noop_and_scopes_nest():
    tl.record(tl.admission_stage(AdmissionCode.ADMITTED, "x"))
    assert tl.current_ledger() is None and tl.turn_ledger_trace() == {}
    with turn_ledger_scope() as outer:
        tl.record_admission(AdmissionCode.ADMITTED, "x")
        with turn_ledger_scope() as inner:
            tl.record_admission(AdmissionCode.INELIGIBLE_LEXICAL, "x")
        assert len(inner.stages) == 1 and len(outer.stages) == 1
        assert tl.turn_ledger_trace()["turn_ledger"]["version"] == tl.LEDGER_VERSION
    assert tl.current_ledger() is None
