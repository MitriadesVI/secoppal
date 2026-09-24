"""Unconditional semantic gates that precede optional query derivation rules."""
from app.core.actor_identity_coherence import (
    ACTOR_IDENTITY_SPLIT_BRAIN,
    ACTOR_IDENTITY_SPLIT_BRAIN_QUESTION,
    actor_identity_split_brain,
)
from app.core.compound_identity_guard import compound_identity_clarification
from app.core.municipal_territory import municipal_clarification
from app.core.object_negation_policy import (
    OBJECT_NEGATION_CONTRADICTION_QUESTION, object_negation_overlap,
)
from app.core.process_state_request import CLARIFICATION_KEY as PROCESS_STATE_KEY
from app.core.query_plan_types import QueryPlan
from app.core.temporal_gate import temporal_plan_block


def semantic_preflight(params: dict, signals: list) -> QueryPlan | None:
    # Private parser evidence: consumed here so it never reaches the builder.
    if question := params.pop(PROCESS_STATE_KEY, None):
        return QueryPlan(
            should_execute=False, params=params, signals=signals,
            risk_flags=['PROCESS_STATE_UNREPRESENTABLE'],
            clarification_question=str(question), missing_slots=['estado'],
            reason='process_state_unrepresentable',
        )
    if actor_identity_split_brain(params):
        return QueryPlan(
            should_execute=False, params=params, signals=signals,
            risk_flags=[ACTOR_IDENTITY_SPLIT_BRAIN],
            clarification_question=ACTOR_IDENTITY_SPLIT_BRAIN_QUESTION,
            reason='actor_identity_split_brain',
        )
    if question := municipal_clarification(params):
        return QueryPlan(
            should_execute=False, params=params, signals=signals,
            clarification_question=question, missing_slots=['departamento'],
            reason='municipal_territory_ambiguous',
        )
    if question := compound_identity_clarification(params):
        return QueryPlan(
            should_execute=False, params=params, signals=signals,
            clarification_question=question, missing_slots=['entidad'],
            reason='municipal_buyer_identity_unresolved',
        )
    if (temporal := temporal_plan_block(params, signals)) is not None:
        return QueryPlan(params=params, signals=signals, **temporal)
    if object_negation_overlap(params):
        return QueryPlan(
            should_execute=False, params=params, signals=signals,
            risk_flags=['OBJECT_NEGATION_CONTRADICTION'],
            clarification_question=OBJECT_NEGATION_CONTRADICTION_QUESTION,
            reason='object_negation_contradiction',
        )
    return None
