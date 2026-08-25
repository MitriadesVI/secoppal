"""HCA-R4-S2: executable OBJECT negation without polarity loss."""
from __future__ import annotations

from burr.core import State

from app.core.negation_binding import NegationDimension, NegationVerdict
from app.core.object_negation_policy import object_negation_overlap
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions import parsing as parsing_actions


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


def test_object_cases_project_only_to_existing_negative_authority() -> None:
    router = QueryRouter()
    cases = {
        "contratos sin alimentacion": (None, ["alimentacion"]),
        "contratos de vigilancia sin alimentacion": (
            ["vigilancia"], ["alimentacion"],
        ),
        "contratos de alimentacion sin transporte": (
            ["alimentacion"], ["transporte"],
        ),
        "sin alimentacion ni transporte": (
            None, ["alimentacion", "transporte"],
        ),
        "no de alimentacion sino de transporte": (
            ["transporte"], ["alimentacion"],
        ),
        "contratos de mantenimiento sin alimentacion ni transporte": (
            ["mantenimiento"], ["alimentacion", "transporte"],
        ),
    }

    for query, (positive, negative) in cases.items():
        parsed = router.parse(query)
        assert parsed.params.get("objeto") == positive
        assert parsed.params["objeto_exclude"] == negative
        assert all(
            request.dimension is NegationDimension.OBJECT
            and request.verdict is NegationVerdict.EXECUTE
            for request in parsed.negation_requests
        )
        assert not set(negative) & set(parsed.params.get("objeto") or [])


def test_object_only_exclusion_is_a_real_retrieval_predicate() -> None:
    query = "contratos sin alimentacion"
    parsed = QueryRouter().parse(query)
    plan = compute_query_plan(parsed.params, query)
    assert plan.should_execute is True
    soql = SoQLBuilder().build(SoQLBuilder.CONTRATOS_DATASET, plan.params)
    assert "objeto_del_contrato" in soql
    assert "NOT (" in soql


def test_same_object_positive_and_negative_clarifies_before_execution() -> None:
    query = "contratos de alimentacion pero sin alimentacion"
    parsed = QueryRouter().parse(query)
    assert object_negation_overlap(parsed.params)

    plan = compute_query_plan(parsed.params, query)
    assert plan.should_execute is False
    assert plan.reason == "object_negation_contradiction"
    assert "OBJECT_NEGATION_CONTRADICTION" in plan.risk_flags
    question = plan.clarification_question or ""
    assert "incluido" in question
    assert "excluido" in question
    assert "resultados" not in question


def test_contradiction_uses_projected_morphology_without_new_equivalences() -> None:
    # The retrieval authority expands alimentacion to comedor.  The conflict
    # policy observes that existing projection; it does not add a new variant.
    assert "comedor" in object_negation_overlap({
        "objeto": ["alimentacion"],
        "objeto_exclude": ["comedor"],
    })
    assert not object_negation_overlap({
        "objeto": ["vigilancia"],
        "objeto_exclude": ["alimentacion"],
    })


def test_disjunctive_positive_keeps_a_non_conflicting_alternative() -> None:
    assert not object_negation_overlap({
        "objeto": [["alimentacion", "transporte"]],
        "objeto_exclude": ["alimentacion"],
    })
    assert not object_negation_overlap({
        "objeto_or": ["alimentacion", "transporte"],
        "objeto_exclude": ["alimentacion"],
    })


def test_followup_contradiction_preserves_previous_frame() -> None:
    previous = QueryRouter().parse(
        "contratos de alimentacion en barranquilla"
    ).params
    out = parsing_actions.apply_context(
        _followup_state("sin alimentacion", previous)
    )
    assert out["needs_clarification"] is True
    assert "incluido" in out["clarification_reason"]
    assert "excluido" in out["clarification_reason"]
    assert out["parsed_params"] == previous
    assert "objeto_exclude" not in out["parsed_params"]
    assert out["query_plan"]["should_execute"] is False
    assert out["query_plan"]["reason"] == "object_negation_contradiction"


def test_irrelevant_money_followup_preserves_object_exclusion() -> None:
    previous = QueryRouter().parse(
        "contratos de mantenimiento sin alimentacion en barranquilla"
    ).params
    out = parsing_actions.apply_context(
        _followup_state("por mas de 500 millones", previous)
    )
    merged = out["parsed_params"]
    assert merged["objeto"] == ["mantenimiento"]
    assert merged["objeto_exclude"] == ["alimentacion"]
    assert merged["ciudad"] == "Barranquilla"
    assert merged["valor_min"] == 500_000_000


def test_parse_action_preserves_raw_query_byte_for_byte() -> None:
    query = "Contratos de alimentación sin transporte"
    state = State({
        "user_query": query,
        "raw_query": query,
        "timings_ms": {},
    })
    out = parsing_actions.parse_query(state, QueryRouter())
    assert out["user_query"] == query
    assert out["raw_query"] == query
    assert out["positive_query"] != query
    assert "transporte" not in out["positive_query"]


def test_modality_and_money_markers_remain_outside_object_execution() -> None:
    modality = QueryRouter().parse("procesos de obra sin contratacion directa")
    assert modality.params["objeto"] == ["obra"]
    assert "objeto_exclude" not in modality.params
    assert modality.params["modalidad_exclude_procedure_ids"]

    money = QueryRouter().parse("contratos de vigilancia por menos de 500 millones")
    assert money.params["objeto"] == ["vigilancia"]
    assert money.params["valor_max"] == 500_000_000
    assert "objeto_exclude" not in money.params
