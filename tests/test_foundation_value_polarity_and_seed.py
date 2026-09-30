"""ADR-102 R1: two guards that held but had no test defending them.

* A VALUE condition with NEGATIVE polarity never becomes a positive money bound.
  The projection is the last barrier, so it is tested alone and through the real route.
* A refinement (seed) or a structured selection is never a Foundation first search.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.foundation_projection import ProjectionError, project_accepted
from app.core.foundation_runtime import eligible_first_search, run_foundation_search
from app.core.interpretation_contract import Polarity, ProposedCondition, RawSpan
from app.core.semantic_dimensions import SemanticDimension
from tests.test_foundation_projection import NOW, RAW, _acceptance
from tests.test_foundation_silent_paths import harness_run, proposal, span
from tests.test_foundation_route_ownership import SEARCH, TODAY, make_flow as _make_flow

make_flow = _make_flow  # fixture re-export

AMOUNT = "menos de 200 millones"


def _value_condition(operator, polarity):
    at = RAW.index(AMOUNT)
    return ProposedCondition("q1.c1", ("q1",), SemanticDimension.VALUE, operator, polarity,
                             "200 millones", (RawSpan(at, at + len(AMOUNT), AMOUNT),))


@pytest.mark.parametrize("operator", ["greater_than", "less_than"])
def test_the_projection_refuses_a_negative_value_condition(operator):
    accepted = _acceptance(condition=_value_condition(operator, Polarity.NEGATIVE), resolved="200000000")
    with pytest.raises(ProjectionError, match="UNSUPPORTED_CONDITION"):
        project_accepted(accepted, reference_instant=NOW)


@pytest.mark.parametrize("comparative,operator", [("no más de", "greater_than"), ("no menos de", "less_than"),
                                                   ("más de", "greater_than")])
def test_a_negated_value_reaches_the_source_never_as_a_positive_bound(tmp_path, monkeypatch, comparative, operator):
    raw = f"contratos de aseo de {comparative} 300 millones"
    produced = proposal(raw, [("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE"),
                              ("VALUE", operator, [span(raw, f"{comparative} 300 millones")], "NEGATIVE")])
    turn, harness = harness_run(tmp_path, monkeypatch, raw, produced)
    assert turn.result["foundation_terminal"] in {"UNSUPPORTED", "NEEDS_CLARIFICATION"}
    assert harness.transport.snapshot() == []
    assert "valor_del_contrato" not in str(turn.result.get("soql_query") or "")


def test_the_same_value_with_positive_polarity_does_query_the_source(tmp_path, monkeypatch):
    raw = "contratos de aseo de más de 300 millones"
    produced = proposal(raw, [("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE"),
                              ("VALUE", "greater_than", [span(raw, "más de 300 millones")], "POSITIVE")])
    turn, harness = harness_run(tmp_path, monkeypatch, raw, produced)
    assert turn.result["foundation_terminal"] == "READY"
    assert any("valor_del_contrato >" in call["soql"] for call in harness.transport.snapshot())


# ── the seed / structured-selection guard of eligible_first_search ──────────────

def _ctx(**over):
    base = dict(user_query=SEARCH, actor_candidate_selection=None, profile_action_selection=None,
                chat_id=None, today=TODAY)
    return SimpleNamespace(**{**base, **over})


@pytest.mark.parametrize("over,seed", [
    ({}, {"objeto": ["aseo"]}),
    ({"actor_candidate_selection": object()}, None),
    ({"profile_action_selection": object()}, None),
])
def test_a_seed_or_a_structured_selection_is_never_eligible(over, seed):
    assert eligible_first_search(None, _ctx(**over), seed) is False
    assert eligible_first_search(None, _ctx(), None) is True  # the control: the plain text is eligible


@pytest.mark.parametrize("over,seed", [
    ({}, {"objeto": ["aseo"]}),
    ({"actor_candidate_selection": object()}, None),
    ({"profile_action_selection": object()}, None),
])
def test_a_seed_or_a_selection_never_calls_the_producer(make_flow, over, seed):
    flow = make_flow("foundation_v3")
    assert run_foundation_search(flow, _ctx(**over), seed) is None
    assert flow.foundation_producer.call_count == 0
