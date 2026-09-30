"""Host universe policy of Foundation V3 (H7, ADR-101).

The producer says which span names a request's universe; the host decides what
that span means for the product. O3, O6 and O7 are SECOPPAL conventions over a
closed vocabulary, so they are applied here, deterministically, and never taught
to the producer. The policy reads only structured, already-grounded claims: the
request's subject and universe evidence, its conditions, and the other requests
of its scope. It never reads the raw turn.

Precedence: a closed rule the host knows wins; with none, the producer's claim
stands; two closed rules that disagree ask for clarification. Every request gets
exactly one decision, and the decision keeps what the producer said.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

from app.core.foundation_admission import state_value
from app.core.intent_vocabulary import BIDDER_PARTICIPATION_VERBS
from app.core.interpretation_contract import (
    ConditionExpression, ConditionRole, Effect, InterpretationProposal, LogicOperator, Polarity,
    ProposedCondition,
)
from app.core.semantic_dimensions import SemanticDimension
from app.utils.money import normalize_text

POLICY_VERSION = "1"
RECORDS, OPPORTUNITIES = "records", "opportunities"
PRODUCER, HOST_CLOSED_POLICY = "PRODUCER", "HOST_CLOSED_POLICY"
OPPORTUNITY_HEADS = frozenset({"oportunidad", "oportunidades"})               # O3
CALL_HEADS = frozenset({"convocatoria", "convocatorias"})                     # O7
TENDER_HEADS = frozenset({"licitacion", "licitaciones"})                      # O6
PARTICIPATION_VERBS = frozenset(normalize_text(verb) for verb in BIDDER_PARTICIPATION_VERBS)  # O3
# Fixed evaluation order, so the recorded rule never depends on set iteration.
RULE_ORDER = ("O3", "O3-INTENT", "O7", "O6A", "O6B", "O6C")


@dataclass(frozen=True, slots=True)
class RequestUniverseDecision:
    request_id: str
    subject: str
    producer_kind: str
    effective_kind: str
    authority: str
    rules: tuple[str, ...]
    evidence_quote: str
    effect: Effect

    @property
    def disagrees_with_producer(self) -> bool:
        return self.producer_kind != self.effective_kind

    def to_dict(self) -> dict:
        return {"request_id": self.request_id, "subject": self.subject,
                "producer_kind": self.producer_kind, "effective_kind": self.effective_kind,
                "authority": self.authority, "rules": list(self.rules),
                "policy_version": POLICY_VERSION, "evidence_quote": self.evidence_quote,
                "effect": self.effect.value, "disagrees_with_producer": self.disagrees_with_producer}


@dataclass(frozen=True, slots=True)
class UniversePolicyOutcome:
    """The proposal the host admits, with the receipts that explain how it differs."""

    proposal: InterpretationProposal
    decisions: tuple[RequestUniverseDecision, ...]
    conflict: str | None = None

    def to_dict(self) -> dict:
        return {"policy": f"H7@{POLICY_VERSION}", "conflict": self.conflict,
                "decisions": [item.to_dict() for item in self.decisions]}


def _tokens(text: str) -> list[tuple[str, int, int]]:
    return [(normalize_text(m.group()), m.start(), m.end()) for m in re.finditer(r"\w+", text)]


def _rules(proposal, request, conditions) -> tuple[tuple[str, str], ...]:
    """Closed rules that fire on one request, as (rule, kind)."""
    universe = request.universe_evidence
    others = [c for c in conditions if c.role is not ConditionRole.REQUESTED_UNIVERSE]
    # A universe word also cited by another condition is that condition's content: no closed rule.
    heads = {token for token, start, end in _tokens(universe.quote)
             if not any(span.start < universe.start + end and universe.start + start < span.end
                        for c in others for span in c.evidence)}
    fired = []
    # Opportunities exist only among processes (O4): on contracts these words decide nothing.
    if request.subject == "processes":
        if heads & OPPORTUNITY_HEADS:
            fired.append(("O3", OPPORTUNITIES))
        if heads & PARTICIPATION_VERBS:
            fired.append(("O3-INTENT", OPPORTUNITIES))
        if heads & CALL_HEADS:
            fired.append(("O7", OPPORTUNITIES))
    colloquial = (heads & TENDER_HEADS and not any(token.startswith("public") for token in heads)
                  and not any(c.dimension is SemanticDimension.MODALITY for c in others))
    if colloquial:
        dual = sum(r.scope_id == request.scope_id for r in proposal.requests) > 1
        timed = any(c.dimension is SemanticDimension.TIME for c in others)
        # Only a state the host itself resolves counts: an invented one («están») must not flip O6.
        stated = any(c.dimension is SemanticDimension.STATE and state_value(c) is not None for c in others)
        if dual:
            fired.append(("O6C", RECORDS))
        elif request.subject == "processes":
            fired.append(("O6B", RECORDS) if timed or stated else ("O6A", OPPORTUNITIES))
    return tuple(sorted(fired, key=lambda item: RULE_ORDER.index(item[0])))


def _with_atom(expression, condition_id: str) -> ConditionExpression:
    atom = ConditionExpression(LogicOperator.ATOM, condition_id)
    if expression is None:
        return atom
    if expression.operator is LogicOperator.AND:
        return replace(expression, children=(atom, *expression.children))
    return ConditionExpression(LogicOperator.AND, None, (atom, expression))


def _without_atom(expression, condition_id: str):
    if expression is None or (expression.operator is LogicOperator.ATOM
                              and expression.condition_id == condition_id):
        return None
    if expression.operator is not LogicOperator.AND:
        return expression  # The contract keeps the universe atom out of OR/NOT.
    children = tuple(child for child in (_without_atom(c, condition_id) for c in expression.children)
                     if child is not None)
    if not children:
        return None
    return children[0] if len(children) == 1 else replace(expression, children=children)


def _effect(producer_kind: str, effective_kind: str) -> Effect:
    if producer_kind == effective_kind:
        return Effect.EQUIVALENT
    return Effect.NARROWING if effective_kind == OPPORTUNITIES else Effect.WIDENING


def apply_universe_policy(proposal: InterpretationProposal) -> UniversePolicyOutcome:
    """One decision per request; the returned proposal carries the effective universe."""
    requests, conditions = list(proposal.requests), list(proposal.conditions)
    taken = {c.condition_id for c in conditions}
    decisions = []
    for index, request in enumerate(proposal.requests):
        mine = [c for c in proposal.conditions if request.request_id in c.request_ids]
        universe = next((c for c in mine if c.role is ConditionRole.REQUESTED_UNIVERSE), None)
        producer_kind = OPPORTUNITIES if universe is not None else RECORDS
        if request.universe_evidence is None:  # Admission rejects it; nothing to decide on.
            decisions.append(RequestUniverseDecision(request.request_id, request.subject, producer_kind,
                                                     producer_kind, PRODUCER, (), "", Effect.EQUIVALENT))
            continue
        fired = _rules(proposal, request, mine)
        kinds = {kind for _, kind in fired}
        if len(kinds) > 1:
            names = "+".join(rule for rule, _ in fired)
            return UniversePolicyOutcome(proposal, tuple(decisions),
                                         conflict=f"HOST_UNIVERSE_RULE_CONFLICT:{request.request_id}:{names}")
        effective = kinds.pop() if kinds else producer_kind
        decisions.append(RequestUniverseDecision(
            request.request_id, request.subject, producer_kind, effective,
            HOST_CLOSED_POLICY if fired else PRODUCER, tuple(rule for rule, _ in fired),
            request.universe_evidence.quote, _effect(producer_kind, effective)))
        if effective == producer_kind:
            continue
        if effective == OPPORTUNITIES:
            condition_id = f"universe-{request.request_id}"
            while condition_id in taken:
                condition_id = "x-" + condition_id
            taken.add(condition_id)
            conditions.append(ProposedCondition(
                condition_id, (request.request_id,), SemanticDimension.RETRIEVAL_INTENT, "is",
                Polarity.POSITIVE, request.universe_claim, (request.universe_evidence,),
                role=ConditionRole.REQUESTED_UNIVERSE))
            requests[index] = replace(request, condition_expression=_with_atom(
                request.condition_expression, condition_id))
        else:
            conditions.remove(universe)
            requests[index] = replace(request, condition_expression=_without_atom(
                request.condition_expression, universe.condition_id))
    effective_proposal = replace(proposal, requests=tuple(requests), conditions=tuple(conditions))
    return UniversePolicyOutcome(effective_proposal, tuple(decisions))
