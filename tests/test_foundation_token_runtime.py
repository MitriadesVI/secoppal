"""N7 whole-token execution: one candidate call, local predicate before total and page."""

from __future__ import annotations

from datetime import datetime

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.foundation_admission import admit_proposal
from app.core.foundation_execution_guard import ExecutionGuard, FoundationGuardError
from app.core.foundation_producers import decode_proposal
from app.core.foundation_projection import project_accepted
from app.core.foundation_token_plan import TokenFilter, select_members, superset_term
from tests.product_contract.frozen_source import InjectedFailure
from tests.product_contract.harness import ProductHarness


RAW = "Busca procesos de obras"
NOW = datetime.fromisoformat("2026-09-26T12:00:00-05:00")
DATASET = "p6dx-8zbt"


def _row(pid, day, name, description=None):
    return {"id_del_proceso": pid, "nombre_del_procedimiento": name,
            "descripci_n_del_procedimiento": description,
            "fecha_de_publicacion_del": f"2026-09-{day:02d}", "precio_base": "100",
            "estado_del_procedimiento": "Publicado", "justificaci_n_modalidad_de": None}


def accepted(raw=RAW):
    noun, topic = raw.index("procesos"), raw.index("obras")
    payload = {"requests": [{"request_id": "q1", "scope_id": "q1", "operation": "SEARCH",
                             "subject": "processes", "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                             "universe_evidence": {"start": noun, "end": noun + 8, "quote": "procesos"},
                             "condition_expression": {"operator": "ATOM", "condition_id": "q1.c1"}}],
               "conditions": [{"condition_id": "q1.c1", "request_ids": ["q1"], "dimension": "TOPIC",
                               "operator": "whole_token_folded", "polarity": "POSITIVE",
                               "evidence": [{"start": topic, "end": topic + 5, "quote": "obras"}]}]}
    return admit_proposal(decode_proposal(payload, raw, arm="C"), raw,
                          entity_resolver=EntityResolver("app/data/aliases_db.json"))


def _say(tmp_path, monkeypatch, rows, failures=()):
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset={DATASET: rows},
                             failures=list(failures), reference_instant=NOW, foundation_producer=accepted,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    return harness.say(RAW)


# Every row contains the folded substring OBRA, so all are candidates; only tokens are members.
MIXED = [
    _row("P01", 20, "Obras de pavimentación"),
    _row("P02", 19, "COBRANZA de cartera"),
    _row("P03", 18, "MANIOBRAS portuarias"),
    _row("P04", 17, "obra-hidráulica menor"),
    _row("P05", 16, None, "cobranza"),       # NULL OR FALSE: unknown, never a member
    _row("P06", 15, None, "[obras] viales"),  # NULL OR TRUE: member
    _row("P07", 14, "ÓBRA civil"),
    _row("P08", 13, "SOBRAS", "maniobra"),
    _row("P09", 12, "obra2 prototipo"),
]


def test_members_are_whole_tokens_in_source_order_with_exact_total_and_no_count(tmp_path, monkeypatch):
    turn = _say(tmp_path, monkeypatch, MIXED)
    group = turn.result["groups"][0]
    assert [call["operation"] for call in turn.transport_calls] == ["query"]
    assert "LIMIT 1000" in turn.transport_calls[0]["soql"]
    assert group["row_ids"] == ["P01", "P04", "P06", "P07"]
    assert group["total"] == 4 and group["total_quality"] == "exact"
    assert "TOKEN_CANDIDATES_TRUNCATED" not in group["warnings"]
    assert turn.result["foundation_terminal"] == "READY"
    assert turn.result["result_order_guarantee"] == "exact"


def test_page_is_the_publication_limit_and_total_counts_all_members(tmp_path, monkeypatch):
    rows = [_row(f"P{at:03d}", 1 + at % 28, f"obra {at}") for at in range(60)]
    turn = _say(tmp_path, monkeypatch, rows)
    group = turn.result["groups"][0]
    assert len(group["rows"]) == 50
    assert group["total"] == 60 and group["total_quality"] == "exact"
    assert [call["operation"] for call in turn.transport_calls] == ["query"]


def test_truncated_candidates_publish_an_exact_prefix_and_only_a_lower_bound(tmp_path, monkeypatch):
    monkeypatch.setattr("app.core.foundation_projection.CANDIDATE_CAP", 5)
    turn = _say(tmp_path, monkeypatch, MIXED)
    group = turn.result["groups"][0]
    assert "LIMIT 5" in turn.transport_calls[0]["soql"]
    # The first five candidates in source order hold P01 and P04; P06 and P07 lie beyond.
    assert group["row_ids"] == ["P01", "P04"]
    assert group["total"] == 2 and group["total_quality"] == "lower_bound"
    assert "TOKEN_CANDIDATES_TRUNCATED" in group["warnings"]
    assert "al menos 2" in turn.result["response"]
    assert turn.result["total_count_approximate"] is True


def test_primary_timeout_does_not_derive_an_unordered_h4_sample(tmp_path, monkeypatch):
    turn = _say(tmp_path, monkeypatch, MIXED,
                [InjectedFailure(lambda: TimeoutError("primary timeout"), operation="query", times=1)])
    assert [call["operation"] for call in turn.transport_calls] == ["query"]
    assert turn.transport_calls[0]["status"] == "error"
    assert turn.result["foundation_terminal"] == "SOURCE_ERROR"
    assert turn.result["groups"][0]["rows"] == [] and turn.result["groups"][0]["total"] is None


def test_malformed_text_value_is_a_source_error_not_a_membership_decision(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="INVALID_TEXT_VALUE"):
        select_members([{"a": 7}], TokenFilter("q1.c1", ("a",), ("OBRA",)))

    def malformed(rows, spec):
        raise ValueError("INVALID_TEXT_VALUE")

    monkeypatch.setattr("app.core.foundation_runtime.select_members", malformed)
    turn = _say(tmp_path, monkeypatch, MIXED)
    group = turn.result["groups"][0]
    assert group["state"] == "source_error" and group["rows"] == [] and group["total"] is None
    assert turn.result["foundation_terminal"] == "SOURCE_ERROR"


def test_guard_never_grants_a_count_of_the_candidate_superset():
    outcome = project_accepted(accepted(), reference_instant=NOW)
    query, count = outcome.manifest
    guard = ExecutionGuard(outcome, RAW, reference_instant=NOW)
    # Even after a full primary page recorded as the parent, the superset is never counted.
    guard.states["q1"].primary_started = True
    guard.query_result("q1", [{}] * query.local_filter.candidate_cap, call_id="call-1")
    with pytest.raises(FoundationGuardError, match="COUNT_PRECONDITION"):
        with guard.permit(count, reason="search:count", parent_call_id="call-1"):
            pytest.fail("grant unexpectedly admitted")


def test_candidate_page_above_its_cap_is_rejected():
    spec = TokenFilter("q1.c1", ("a",), ("OBRA",), candidate_cap=2)
    with pytest.raises(ValueError, match="TOKEN_CANDIDATES_EXCEED_CAP"):
        select_members([{"a": "obra"}] * 3, spec)
    assert select_members([{"a": "obra"}] * 2, spec) == ([{"a": "obra"}] * 2, False)
    assert select_members([{"a": "obra"}, {"a": "cobra"}], TokenFilter("q1.c1", ("a",), ("OBRA",)))[1] is True


@pytest.mark.parametrize("tokens,term", [(("OBRA", "OBRAS"), "OBRA"), (("OBRAS", "OBRA"), "OBRA")])
def test_superset_term_is_contained_in_every_token(tokens, term):
    assert superset_term(tokens) == term


@pytest.mark.parametrize("tokens", [("PUENTE", "PONTON"), ("OBRA DE", "OBRA")])
def test_tokens_without_one_whole_common_term_fail_closed(tokens):
    with pytest.raises(ValueError, match="TOKEN_SUPERSET_NOT_SINGLE_TERM"):
        superset_term(tokens)
