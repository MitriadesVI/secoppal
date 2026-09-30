"""Deterministic host resolution shared by all Foundation linguistic producers."""

from __future__ import annotations

import re

from app.core.contract_type_resolver import resolve_contract_types
from app.core.contract_universe import POLICY_VERSION
from app.core.entity_resolver import EntityResolver, department_mention_resolution
from app.core.foundation_evidence import UNIVERSE_SPAN_OMITTED, material_phrase, topic_spans
from app.core.interpretation_contract import (
    AcceptanceStatus, ConditionRole, ContractError, Effect, HostPolicy, HostResolution,
    InterpretationAcceptance, InterpretationProposal, Polarity, RelationKind, ResolutionDecision,
)
from app.core.modality_resolver import resolve as resolve_modality
from app.core.semantic_dimensions import SemanticDimension
from app.core.soql_builder import SoQLBuilder
from app.utils.money import money_to_cop, normalize_text


_PHRASE_DIMENSIONS = frozenset({SemanticDimension.TOPIC, SemanticDimension.BUYER, SemanticDimension.GEOGRAPHY})


def _stop(proposal, status, reason):
    return InterpretationAcceptance(proposal, status, reason=reason)


def state_value(condition) -> str | None:
    """The host value of a STATE claim; None when the host lexicon does not resolve it."""
    folded = normalize_text(" ".join(span.quote for span in condition.evidence))
    if condition.operator == "family" and re.search(r"\babiert[oa]s?\b", folded):
        return "oferta_abierta"
    if re.search(r"\bpublicad[oa]s?\b", folded):
        return "Publicado"
    if re.search(r"\bterminad[oa]s?\b", folded):
        return "Terminado"
    return None


def admit_proposal(proposal: InterpretationProposal, raw: str, *,
                   entity_resolver: EntityResolver) -> InterpretationAcceptance:
    """Resolve grounded linguistic claims; never accept executable values from a producer."""
    try:
        proposal.validate_raw(raw)
    except (ContractError, AttributeError) as exc:
        return _stop(None, AcceptanceStatus.INVALID_PROPOSAL, str(exc))
    if not proposal.requests:
        return _stop(proposal, AcceptanceStatus.NO_ACTION, "NO_REQUEST")
    if len(proposal.requests) > 1:
        shared = [r for r in proposal.relations if r.kind is RelationKind.SHARES_SCOPE]
        if (len(proposal.requests) != 2 or len(shared) != 1 or
                any(r.operation != "SEARCH" for r in proposal.requests)):
            return _stop(proposal, AcceptanceStatus.UNSUPPORTED, "MULTI_REQUEST")
    if any(c.dimension is SemanticDimension.UNSUPPORTED for c in proposal.conditions):
        return _stop(proposal, AcceptanceStatus.UNSUPPORTED, "UNSUPPORTED_CONDITION")
    resolutions, policies = [], []
    datasets = {}
    for request in proposal.requests:
        if request.operation != "SEARCH":
            return _stop(proposal, AcceptanceStatus.UNSUPPORTED, "UNSUPPORTED_OPERATION")
        if request.subject not in {"processes", "contracts"}:
            return _stop(proposal, AcceptanceStatus.UNSUPPORTED, "UNSUPPORTED_UNIVERSE")
        if request.universe_evidence is None:
            return _stop(proposal, AcceptanceStatus.INVALID_PROPOSAL, "UNGROUNDED_REQUEST_UNIVERSE")
        dataset = (SoQLBuilder.PROCESOS_DATASET if request.subject == "processes"
                   else SoQLBuilder.CONTRATOS_DATASET)
        datasets[request.request_id] = dataset
        resolutions.append(HostResolution(request.request_id, ResolutionDecision.RESOLVED,
                                          "host-universe", (dataset,)))
        if request.source_claim is not None and normalize_text(request.source_claim) not in {"secop ii", "secop 2"}:
            return _stop(proposal, AcceptanceStatus.UNSUPPORTED, "UNSUPPORTED_SOURCE")
    for condition in proposal.conditions:
        text = " ".join(span.quote for span in condition.evidence)
        folded = normalize_text(text)
        dimension = condition.dimension
        value = None
        if condition.role is ConditionRole.REQUESTED_UNIVERSE:
            override = any(c.dimension is SemanticDimension.STATE and c.operator == "family"
                           and set(c.request_ids) & set(condition.request_ids) for c in proposal.conditions)
            policies.append(HostPolicy("opportunity-explicit-state" if override else "O1B", "1",
                                       (condition.condition_id,), Effect.EQUIVALENT))
            continue
        authority, spans = "host-dimension", condition.evidence
        if dimension is SemanticDimension.TOPIC:
            # G2: the request's own universe occurrence is never topic content.
            spans, structural, omitted = topic_spans(condition, proposal.requests)
            if structural is not None:
                return _stop(proposal, AcceptanceStatus.NEEDS_CLARIFICATION, structural)
            if omitted:
                authority = f"host-dimension:{UNIVERSE_SPAN_OMITTED}"
        if dimension in _PHRASE_DIMENSIONS:
            # G1/T4: every span is read; the host never keeps only one of several citations.
            phrase, unread = material_phrase(spans, raw, dimension.value)
            if unread is not None:
                return _stop(proposal, AcceptanceStatus.NEEDS_CLARIFICATION, unread)
            text, folded = phrase, normalize_text(phrase)
        if dimension is SemanticDimension.TOPIC:
            value = folded
        elif dimension is SemanticDimension.GEOGRAPHY:
            name = re.sub(r"^departamento\s+de\s+", "", folded)
            geo = department_mention_resolution(name)
            value = geo.canonical if geo.kind == "department" else None
        elif dimension is SemanticDimension.BUYER:
            buyer = entity_resolver.resolve_entidad(text)
            value = buyer.value if (buyer.method in {"exact", "exact_official", "exact_normalized"}
                                   or buyer.method.startswith("strict_")) else None
        elif dimension is SemanticDimension.TIME and condition.operator == "during_year":
            years = re.findall(r"\b(?:19|20)\d{2}\b", folded)
            value = years[0] if len(years) == 1 else None
        elif dimension is SemanticDimension.VALUE:
            phrase = re.sub(r"^(?:por\s+)?(?:mas|menos|mayor|menor)\s+(?:de|que)\s+", "", folded)
            amount = money_to_cop(phrase)
            value = str(amount) if amount is not None and amount >= 0 else None
        elif dimension is SemanticDimension.MODALITY:
            dataset_names = {"procesos" if datasets[r] == SoQLBuilder.PROCESOS_DATASET else "contratos"
                             for r in condition.request_ids}
            modality = resolve_modality(text, dataset=next(iter(dataset_names)) if len(dataset_names) == 1 else None)
            matches = (*modality.includes, *modality.excludes)
            if not modality.issues and len(matches) == 1:
                ids = tuple(matches[0].procedure_ids)
                if ids:
                    resolutions.append(HostResolution(condition.condition_id, ResolutionDecision.RESOLVED,
                                                      "host-modality-catalog", ids))
                    if condition.operator == "family":
                        family = matches[0].retrieval_family
                        policy = {"licitacion_publica": "modality-lp-family",
                                  "contratacion_directa": "modality-direct-family"}.get(family)
                        if policy is None:
                            return _stop(proposal, AcceptanceStatus.UNSUPPORTED, "UNACCREDITED_MODALITY_FAMILY")
                        policies.append(HostPolicy(policy, "1", (condition.condition_id,), Effect.EQUIVALENT))
                    continue
        elif dimension is SemanticDimension.CONTRACT_TYPE_OR_NATURE:
            types = resolve_contract_types(text)
            value = types.include_ids[0] if len(types.include_ids) == 1 else None
        elif dimension is SemanticDimension.STATE:
            value = state_value(condition)
            if value == "oferta_abierta" and len(condition.evidence) > 1:
                policies.append(HostPolicy("ADR-100", "1", (condition.condition_id,), Effect.EQUIVALENT))
            elif value == "Terminado" and condition.polarity is Polarity.NEGATIVE:
                policies.append(HostPolicy("celebrated-state-complement", POLICY_VERSION,
                                           (condition.condition_id,), Effect.EQUIVALENT))
            elif value is None and re.search(r"\bcerrad[oa]s?\b", folded):
                peers = [c for c in proposal.conditions if c.dimension is SemanticDimension.STATE
                         and set(c.request_ids) & set(condition.request_ids)]
                if len(peers) > 1:
                    return _stop(proposal, AcceptanceStatus.NEEDS_CLARIFICATION, "CONTRADICTORY_STATE")
        if value is None:
            return _stop(proposal, AcceptanceStatus.NEEDS_CLARIFICATION,
                         f"UNRESOLVED_HOST_DIMENSION:{dimension.value}")
        resolutions.append(HostResolution(condition.condition_id, ResolutionDecision.RESOLVED,
                                          authority, (value,)))
    if len(datasets) > 1 and len(set(datasets.values())) != 2:
        return _stop(proposal, AcceptanceStatus.UNSUPPORTED, "MULTI_REQUEST")
    return InterpretationAcceptance(proposal, AcceptanceStatus.READY, tuple(resolutions), tuple(policies))
