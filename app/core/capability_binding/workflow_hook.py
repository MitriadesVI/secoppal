"""The single seam between a bound capability and the V1.1 execute step.

The contract lives in its own module so that, when Entity Profile Frame V1.2 is
activated, only this adapter changes: the policy does not.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.core.capability_binding.dogfood_telemetry import record_turn
from app.core.capability_binding.registry import SUPPLIERS
from app.core.capability_binding.supplier_contract import Outcome, decide
from app.core.capability_binding.supplier_semantics_wiring import configured_polarity_veto
from app.core.entity_profile.turn import EntityProfileTurn, classify_entity_profile_turn


@dataclass(frozen=True, slots=True)
class BoundTurn:
    reason: str
    turn: EntityProfileTurn | None = None
    clarification: dict | None = None
    # The dogfood receipt for this turn. Recorded even when the contract hands
    # the turn back (NOT_BOUND), because "Alpha proposed SUPPLIERS and the host
    # refused" is the measurement the pilot exists to make.
    telemetry: dict | None = None


def bound_profile_turn(capability: str | None, user_query: str, params: dict) -> BoundTurn | None:
    """``None`` when nothing is bound or the contract does not own the turn."""
    if capability != SUPPLIERS:
        return None
    record: dict = {}
    try:
        decision = decide(
            user_query, params,
            polarity_veto=configured_polarity_veto(), record=record,
        )
    finally:
        # Recorded even when the contract raises: a turn that vanished from the
        # measurement is indistinguishable from one that never happened.
        record_turn(record)
    if decision.outcome is Outcome.NOT_BOUND:
        return BoundTurn(decision.reason, telemetry=record)
    if decision.outcome is Outcome.CLARIFY:
        return BoundTurn(decision.reason, clarification={
            "needs_clarification": True,
            "clarification_reason": decision.response,
            "route_reason": decision.route_reason,
            "resolved_params": params,
        }, telemetry=record)
    return BoundTurn(
        decision.reason,
        turn=classify_entity_profile_turn(
            user_query, decision.admitted_params, bound_focus="suppliers",
        ),
        telemetry=record,
    )
