"""Atomic semantic evidence admission and product transport regressions."""
from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from datetime import date

import pytest
from burr.core import State
from hypothesis import given, settings, strategies as st

from app.core.analytics.contract_activity import handle_contract_activity
from app.core.analytics.contract_activity.models import DecisionKind
from app.core.semantic_turn_evidence import SemanticTurnEvidence
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions.execution import execute_query
from app.core.workflow.actions.parsing import parse_query
from tests.contract_activity_evidence import _ROUTER, accredited_turn, parsed_turn
from tests.test_contract_activity_narrow_corpus import BUYER_PARAMS, CorpusClient


ATTACK_RAW = "¿Cuántos contratos sobre Galapa firmó la Alcaldía de Galapa?"
VALID_RAW = "¿Cuántos contratos firmó la Alcaldía de Galapa?"


def test_partial_material_attack_cannot_execute_even_with_valid_count() -> None:
    parsed = parsed_turn(ATTACK_RAW)
    assert parsed.material_residual == ("galapa",)
    state = parsed.semantic_evidence.to_state()
    assert state["analytic_controls"]
    state["material_residual"] = []
    state["material_evidence"] = None
    source = CorpusClient()
    decision = handle_contract_activity(
        ATTACK_RAW, BUYER_PARAMS, source, semantic_evidence=state,
    )
    assert decision.kind is DecisionKind.CLARIFY
    assert decision.reason == "SEMANTIC_EVIDENCE_INVALID"
    assert source.calls == []


@pytest.mark.parametrize("mutation", [
    lambda state: state.pop("analytic_controls"),
    lambda state: state.pop("material_evidence"),
    lambda state: state.update(material_evidence=None),
    lambda state: state.update(material_residual=[]),
])
def test_missing_or_partial_receipt_is_terminal_with_zero_source_calls(mutation) -> None:
    state = parsed_turn(ATTACK_RAW).semantic_evidence.to_state()
    mutation(state)
    source = CorpusClient()
    decision = handle_contract_activity(
        ATTACK_RAW, BUYER_PARAMS, source, semantic_evidence=state,
    )
    assert decision.kind is DecisionKind.CLARIFY
    assert decision.reason == "SEMANTIC_EVIDENCE_INVALID"
    assert source.calls == []


def test_missing_receipt_and_legacy_direct_api_are_fail_closed() -> None:
    source = CorpusClient()
    decision = handle_contract_activity(VALID_RAW, BUYER_PARAMS, source)
    assert decision.kind is DecisionKind.CLARIFY
    assert decision.reason == "SEMANTIC_EVIDENCE_REQUIRED"
    assert source.calls == []


def test_stale_or_modified_raw_receipt_is_rejected_before_source_calls() -> None:
    receipt = accredited_turn(VALID_RAW).semantic_evidence
    assert receipt is not None
    source = CorpusClient()
    for raw in (
        "¿Cuántos contratos tiene la Alcaldía de Galapa?",
        VALID_RAW + " ",
    ):
        decision = handle_contract_activity(
            raw, BUYER_PARAMS, source, semantic_evidence=receipt,
        )
        assert decision.kind is DecisionKind.CLARIFY
        assert decision.reason == "SEMANTIC_EVIDENCE_RAW_MISMATCH"
    assert source.calls == []


def test_valid_complete_receipt_is_immutable_json_roundtrippable_and_executes() -> None:
    parsed = accredited_turn(VALID_RAW)
    receipt = parsed.semantic_evidence
    assert receipt is not None
    with pytest.raises(FrozenInstanceError):
        receipt.material_residual = ("altered",)  # type: ignore[misc]
    state = json.loads(json.dumps(receipt.to_state()))
    assert SemanticTurnEvidence.from_state(state) == receipt
    source = CorpusClient()
    decision = handle_contract_activity(
        VALID_RAW, parsed.params, source, semantic_evidence=state,
    )
    assert decision.kind is DecisionKind.ANSWER
    assert len(source.calls) == 2


def test_two_equal_galapa_surfaces_keep_distinct_ownership_and_cannot_execute() -> None:
    parsed = accredited_turn(ATTACK_RAW)
    receipt = parsed.semantic_evidence
    assert receipt is not None
    ownership = receipt.material_evidence.ownership
    assert ownership is not None and ownership.valid()
    assert len(ownership.content_spans) == 1
    (content_start, content_end), = ownership.content_spans
    buyer_start, buyer_end = ownership.buyer_span
    assert ownership.snapshot[content_start:content_end] == "galapa"
    assert "galapa" in ownership.snapshot[buyer_start:buyer_end]
    assert content_end <= buyer_start
    source = CorpusClient()
    decision = handle_contract_activity(
        ATTACK_RAW, parsed.params, source, semantic_evidence=receipt,
    )
    assert decision.kind is DecisionKind.CLARIFY
    assert decision.reason == "UNSUPPORTED_MATERIAL_FILTER"
    assert source.calls == []


def test_real_burr_parse_to_execute_transports_one_complete_receipt() -> None:
    state = parse_query(
        State({"user_query": VALID_RAW, "today": date(2026, 9, 20), "timings_ms": {}}),
        _ROUTER,
    )
    assert "semantic_evidence" in state
    for old_key in ("analytic_controls", "material_residual", "material_evidence"):
        assert old_key not in state
    state = state.update(
        resolved_params=state["parsed_params"],
        dataset_id="jbjy-vk9h",
        soql_query="UNEXECUTED",
    )
    source = CorpusClient()
    result = execute_query(state, source, SoQLBuilder())
    assert result.get("analytical_intent") == "contract_activity"
    assert result.get("needs_clarification") is False
    assert len(source.calls) == 2


@settings(max_examples=24, deadline=None)
@given(
    missing=st.sampled_from((
        "raw_fingerprint", "analytic_controls", "material_residual", "material_evidence",
    )),
    suffix=st.text(min_size=1, max_size=8),
)
def test_receipt_property_missing_field_or_changed_raw_never_calls_source(
    missing: str, suffix: str,
) -> None:
    parsed = accredited_turn(VALID_RAW)
    receipt = parsed.semantic_evidence.to_state()
    receipt.pop(missing)
    source = CorpusClient()
    partial = handle_contract_activity(
        VALID_RAW, parsed.params, source, semantic_evidence=receipt,
    )
    assert partial.kind is DecisionKind.CLARIFY
    assert partial.reason == "SEMANTIC_EVIDENCE_INVALID"
    stale = handle_contract_activity(
        VALID_RAW + suffix, parsed.params, source,
        semantic_evidence=parsed.semantic_evidence,
    )
    assert stale.kind is DecisionKind.CLARIFY
    assert stale.reason == "SEMANTIC_EVIDENCE_RAW_MISMATCH"
    assert source.calls == []
