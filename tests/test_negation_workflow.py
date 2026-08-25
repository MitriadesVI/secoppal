"""Workflow guards for INV-NO-SILENT-NEGATION-DROP."""
from __future__ import annotations

from burr.core import State

from app.core.query_frame import frame_from_params
from app.core.query_router import QueryRouter
from app.core.workflow.actions import parsing as parsing_actions
from app.core.workflow.actions.planning import plan_query


class _RecordingLLM:
    enabled = True

    def __init__(self) -> None:
        self.query = None

    def parse(self, query, existing_params=None):
        self.query = query
        return {"ciudad": "Barranquilla", "objeto": ["barranquilla"]}


def test_parse_action_transports_binding_outside_query_frame() -> None:
    state = State({
        "user_query": "contratos de aseo que no sean de barranquilla",
        "timings_ms": {},
    })
    out = parsing_actions.parse_query(state, QueryRouter())
    assert out["negation_requests"][0]["dimension"] == "CITY"
    assert "barranquilla" not in out["positive_query"]
    frame = frame_from_params(out["parsed_params"])
    assert "negation_requests" not in frame.raw_params


def test_llm_sees_only_positive_residual_text() -> None:
    llm = _RecordingLLM()
    state = State({
        "user_query": "contratos donde el proveedor no sea union temporal aseo",
        "positive_query": "contratos donde el proveedor                         ",
        "negation_requests": [{"dimension": "ACTOR", "verdict": "DEFER"}],
        "needs_llm": True,
        "parsed_params": {"dataset": "contratos"},
        "timings_ms": {},
    })
    parsing_actions.llm_parse(state, llm)
    assert llm.query == state["positive_query"]
    assert "union temporal aseo" not in llm.query


def test_first_turn_deferred_negation_blocks_before_execution() -> None:
    parsed = QueryRouter().parse(
        "contratos de aseo que no sean de barranquilla"
    )
    state = State({
        "parsed_params": parsed.params,
        "raw_query": "contratos de aseo que no sean de barranquilla",
        "prev_params": None,
        "turn_index": 0,
        "modality_interpretation": parsed.modality_interpretation,
        "negation_requests": [r.to_state() for r in parsed.negation_requests],
        "followup": False,
        "followup_intent_type": None,
    })
    out = plan_query(state)
    assert out["query_plan"]["should_execute"] is False
    assert out["query_plan"]["reason"] == "dimensional_negation_deferred"
    assert "DIMENSIONAL_NEGATION_DEFERRED" in out["query_plan"]["risk_flags"]


def _followup_state(current_query: str, previous: dict) -> State:
    parsed = QueryRouter().parse(current_query)
    instruction = parsing_actions.modality_wiring.classify_control(
        current_query,
        parsed.modality_interpretation,
        allow_implicit_replace=False,
    )
    return State({
        "parsed_params": parsed.params,
        "context_params": previous,
        "followup": True,
        "user_query": current_query,
        "positive_query": parsed.positive_text,
        "negation_requests": [r.to_state() for r in parsed.negation_requests],
        "modality_interpretation": parsed.modality_interpretation,
        "modality_followup_instruction": (
            parsing_actions.modality_wiring.instruction_to_state(instruction)
        ),
        "timings_ms": {},
    })


def test_negative_city_cannot_classify_change_scope() -> None:
    previous = QueryRouter().parse("contratos de aseo en atlantico").params
    out = parsing_actions.apply_context(_followup_state("menos barranquilla", previous))
    assert out["needs_clarification"] is True
    assert out["followup_intent_type"] == "negation_deferred"
    assert out["parsed_params"] == previous
    assert "ciudad" not in out["parsed_params"]


def test_negative_year_cannot_classify_change_year() -> None:
    previous = QueryRouter().parse("contratos de aseo en valledupar").params
    out = parsing_actions.apply_context(_followup_state("menos los de 2025", previous))
    assert out["needs_clarification"] is True
    assert out["followup_intent_type"] == "negation_deferred"
    assert out["parsed_params"] == previous
    assert "fecha_desde" not in out["parsed_params"]


def test_existing_modality_exclusion_is_not_blocked() -> None:
    parsed = QueryRouter().parse("procesos de obra sin contratacion directa")
    state = State({
        "parsed_params": parsed.params,
        "raw_query": "procesos de obra sin contratacion directa",
        "prev_params": None,
        "turn_index": 0,
        "modality_interpretation": parsed.modality_interpretation,
        "negation_requests": [r.to_state() for r in parsed.negation_requests],
        "followup": False,
        "followup_intent_type": None,
    })
    out = plan_query(state)
    assert out["query_plan"]["should_execute"] is True
