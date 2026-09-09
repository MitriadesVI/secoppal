"""Typed, intent-gated mutations for conversational follow-ups.

This module owns the boundary between a parser result and conversational state:
the parser may request any field, but only the active intent policy may mutate it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.core.followup_control_tokens import (
    STATE_FIELDS,
    is_control_only_turn,
    requested_clear_fields,
)
from app.core.negation_followup import is_object_exclusion_replace


TOPIC_FIELDS = frozenset({"objeto", "objeto_or", "objeto_phrases", "objeto_exclude", "topic_facets"})
MONEY_FIELDS = frozenset({"valor_min", "valor_max", "money_metadata"})
MODALITY_FIELDS = frozenset({
    "modalidad_include_procedure_ids",
    "modalidad_exclude_procedure_ids",
})
CONTRACT_TYPE_FIELDS = frozenset({
    "contract_type_include",
})
SCOPE_FIELDS = frozenset({
    "entidad", "entidad_resolved", "entidad_resolution", "entidad_like",
    "departamento", "departamento_resolved", "departamento_resolution",
    "departamentos_in", "ciudad", "ciudad_resolved", "region", "region_resolution",
    "municipal_territory",
})
FILTER_FIELDS = frozenset({
    *MONEY_FIELDS, "fecha_desde", "fecha_hasta", "_temporal_replace_scope",
    *MODALITY_FIELDS,
    *CONTRACT_TYPE_FIELDS,
    "contratista", *STATE_FIELDS, "contract_universe", "contract_universe_version",
})
PAGING_FIELDS = frozenset({"offset", "_display_offset", "_shown_total"})
CLASSIFICATION_FIELDS = frozenset({"intent_type", "opportunity_mode", "filter_active_only"})
DATASET_FIELDS = frozenset({"dataset", "dataset_explicit", "dataset_scope", "force_dataset"})


@dataclass(frozen=True)
class MutationPolicy:
    allowed_set: frozenset[str]
    allowed_clear: frozenset[str] = frozenset()
    transition_cleanup: str | None = None

    def allows_topic_mutation(self) -> bool:
        return bool(self.allowed_set & TOPIC_FIELDS)


MUTATION_POLICIES: dict[str, MutationPolicy] = {
    "pagination_more": MutationPolicy(frozenset()),
    "change_order": MutationPolicy(
        frozenset({"ordering_signal", "order_by", *MONEY_FIELDS}),
    ),
    "change_year": MutationPolicy(
        frozenset({
            "fecha_desde", "fecha_hasta", "_temporal_replace_scope",
            *MONEY_FIELDS,
        }),
    ),
    "change_dataset": MutationPolicy(
        frozenset({"dataset", "dataset_explicit", *FILTER_FIELDS}),
        allowed_clear=frozenset({
            *STATE_FIELDS, "fecha_desde", "fecha_hasta", *PAGING_FIELDS,
            "dataset_scope", *CLASSIFICATION_FIELDS,
        }),
        transition_cleanup="normalize_dataset_transition",
    ),
    "change_scope": MutationPolicy(
        frozenset({*SCOPE_FIELDS, *FILTER_FIELDS}),
        allowed_clear=SCOPE_FIELDS,
        transition_cleanup="normalize_scope_transition",
    ),
    "refine_filter": MutationPolicy(
        frozenset({*FILTER_FIELDS, "ordering_signal", *CLASSIFICATION_FIELDS}),
        allowed_clear=STATE_FIELDS,
        transition_cleanup="normalize_filter_transition",
    ),
    "remove_filter": MutationPolicy(
        frozenset({"objeto_exclude"}),
        allowed_clear=frozenset({
            "valor_min", "valor_max", "money_metadata",
            "fecha_desde", "fecha_hasta", *STATE_FIELDS, "objeto_exclude",
        }),
        transition_cleanup="normalize_remove_filter",
    ),
    "replace_topic": MutationPolicy(
        frozenset({
            "objeto", "objeto_or", "objeto_phrases", "objeto_exclude", "contratista",
            "dataset", "dataset_explicit", *CLASSIFICATION_FIELDS,
        }),
        allowed_clear=frozenset({
            "valor_min", "valor_max", "money_metadata", *MODALITY_FIELDS,
            "objeto_or", "objeto_phrases", "objeto_exclude",
            "opportunity_mode", "filter_active_only",
        }),
        transition_cleanup="normalize_topic_transition",
    ),
    "refine_topic": MutationPolicy(TOPIC_FIELDS),
    "exclude_topic": MutationPolicy(
        frozenset({"objeto_exclude"}),
        allowed_clear=frozenset({"objeto_exclude"}),
    ),
    "contextual_requery": MutationPolicy(
        frozenset({
            "objeto", "objeto_or", "objeto_phrases", "objeto_exclude",
            "contratista", *CLASSIFICATION_FIELDS,
        }),
        allowed_clear=frozenset({
            "valor_min", "valor_max", "money_metadata", *MODALITY_FIELDS,
            "objeto_or", "objeto_phrases", "objeto_exclude",
            "opportunity_mode", "filter_active_only",
        }),
        transition_cleanup="normalize_topic_transition",
    ),
    # Un reinicio no acepta nada del turno: la orden es borrar, y las palabras
    # de la orden no son contenido.
    "reset_context": MutationPolicy(frozenset()),
    "unclear": MutationPolicy(frozenset()),
}


@dataclass
class GatedDelta:
    accepted_delta: dict[str, Any] = field(default_factory=dict)
    cleaned_previous: dict[str, Any] = field(default_factory=dict)
    requested_fields: list[str] = field(default_factory=list)
    accepted_fields: list[str] = field(default_factory=list)
    transition_cleared_fields: list[str] = field(default_factory=list)
    rejected_fields: list[str] = field(default_factory=list)
    rejection_reasons: dict[str, str] = field(default_factory=dict)

    def trace(self, intent: str) -> dict[str, Any]:
        return {
            "intent": intent,
            "requested_fields": self.requested_fields,
            "accepted_fields": self.accepted_fields,
            "transition_cleared_fields": self.transition_cleared_fields,
            "rejected_fields": self.rejected_fields,
            "rejection_reasons": self.rejection_reasons,
        }


def calculate_delta(current_params: dict[str, Any], intent: str) -> dict[str, Any]:
    """Return fields requested by this turn, excluding parser defaults."""
    delta = {
        key: value for key, value in current_params.items()
        if value not in (None, [], "", {}, False)
    }
    if intent != "new_search" and not current_params.get("dataset_explicit"):
        delta.pop("dataset", None)
        delta.pop("dataset_explicit", None)
    return delta


def _topic_changed(previous: dict[str, Any], delta: dict[str, Any]) -> bool:
    old_values = [
        *(previous.get("objeto", []) or []),
        *(previous.get("objeto_or", []) or []),
        *(previous.get("objeto_phrases", []) or []),
    ]
    old = {str(x).lower() for x in old_values if not isinstance(x, list)}
    new = {str(x).lower() for x in delta.get("objeto", []) if not isinstance(x, list)}
    return bool(new and old and not (new & old))


def _transition_clears(
    policy: MutationPolicy,
    intent: str,
    text: str,
    previous: dict[str, Any],
    accepted: dict[str, Any],
) -> set[str]:
    clears: set[str] = set()
    if policy.transition_cleanup == "normalize_dataset_transition":
        clears.update(policy.allowed_clear)
    elif policy.transition_cleanup == "normalize_scope_transition" and any(k in accepted for k in SCOPE_FIELDS):
        clears.update(SCOPE_FIELDS - set(accepted))
    elif policy.transition_cleanup == "normalize_filter_transition":
        if ({"valor_min", "valor_max"} & set(accepted)) and "money_metadata" not in accepted:
            clears.add("money_metadata")
        if set(accepted) & MODALITY_FIELDS:
            clears.update(MODALITY_FIELDS)
        if "contratista" in accepted and len(set(accepted) & FILTER_FIELDS) == 1:
            clears.update(TOPIC_FIELDS | SCOPE_FIELDS | (FILTER_FIELDS - {"contratista"}))
        if any(k in accepted for k in STATE_FIELDS):
            clears.update(STATE_FIELDS - set(accepted))
            if accepted.get("estado_family") != "oferta_abierta":
                clears.update({"intent_type", "opportunity_mode", "filter_active_only"})
    elif policy.transition_cleanup == "normalize_remove_filter":
        clears.update(requested_clear_fields(text) & policy.allowed_clear)
    elif policy.transition_cleanup == "normalize_topic_transition" and _topic_changed(previous, accepted):
        clears.update(policy.allowed_clear - set(accepted))
        if accepted.get("dataset") and accepted.get("dataset") != previous.get("dataset"):
            clears.update(STATE_FIELDS | PAGING_FIELDS | {"fecha_desde", "fecha_hasta", "dataset_scope"})
        if accepted.get("intent_type") and accepted.get("intent_type") != previous.get("intent_type"):
            clears.update({"opportunity_mode", "filter_active_only"})

    if intent == "exclude_topic" and is_object_exclusion_replace(text):
        clears.update({"objeto_exclude"} & policy.allowed_clear)

    accepted_money = set(accepted) & MONEY_FIELDS
    if accepted_money:
        clears.add("money_clarification")
        # Follow-up bounds keep the existing intersection when it is coherent,
        # but an explicit replacement can never inherit an impossible opposite
        # bound.  A complete range replaces both previous endpoints.
        if {"valor_min", "valor_max"} <= accepted_money:
            clears.update({"valor_min", "valor_max"})
        elif "valor_min" in accepted_money:
            clears.add("valor_min")
            old_max = previous.get("valor_max")
            if old_max is not None and accepted["valor_min"] > old_max:
                clears.add("valor_max")
        elif "valor_max" in accepted_money:
            clears.add("valor_max")
            old_min = previous.get("valor_min")
            if old_min is not None and accepted["valor_max"] < old_min:
                clears.add("valor_min")

    if accepted.get("_temporal_replace_scope"):
        clears.update({"fecha_desde", "fecha_hasta"})

    semantic_fields = TOPIC_FIELDS | SCOPE_FIELDS | FILTER_FIELDS | DATASET_FIELDS
    if set(accepted) & semantic_fields:
        # Cursors describe one exact WHERE universe.  Every accepted semantic
        # delta starts from page one and must trace the invalidation explicitly.
        clears.update(PAGING_FIELDS)
    return clears


def _normalize_filter_for_dataset(
    accepted: dict[str, Any],
    previous: dict[str, Any],
) -> dict[str, Any]:
    """Remap a state family parsed without context to the inherited dataset."""
    family_name = accepted.get("estado_family")
    dataset = previous.get("dataset")
    if not family_name or dataset not in {"procesos", "contratos"}:
        return accepted

    from app.core.estado_families import ESTADO_FAMILIES, rebase_family_to_dataset

    def _project(name: str) -> dict[str, list[str]]:
        declared = ESTADO_FAMILIES.get(name, {}).get(str(dataset), {})
        return {key: list(values) for key, values in declared.items() if values}

    filters = _project(str(family_name))
    if not filters:
        # La familia no existe en el dataset heredado. Conservarla dejaría los
        # campos del otro dataset colgando de la consulta; releerla en este
        # ciclo de vida devuelve la misma petición del usuario.
        rebased = rebase_family_to_dataset(str(family_name), str(dataset))
        if rebased == family_name:
            return accepted
        family_name = rebased
        filters = _project(rebased)
        if not filters:
            return accepted

    normalized = {key: value for key, value in accepted.items() if key not in STATE_FIELDS}
    normalized["estado_family"] = family_name
    normalized.update(filters)
    first_field = next(iter(filters))
    normalized["estado"] = filters[first_field][0]
    normalized["estado_field"] = first_field
    return normalized


def gate_delta(
    *,
    intent: str,
    text: str,
    delta: dict[str, Any],
    previous_params: dict[str, Any],
) -> GatedDelta:
    """Validate, prune and clean a delta before it can mutate the QueryFrame."""
    policy = MUTATION_POLICIES.get(intent)
    if intent == "new_search":
        policy = MutationPolicy(frozenset(delta))
    elif policy is None:
        policy = MUTATION_POLICIES["unclear"]

    accepted: dict[str, Any] = {}
    rejected: dict[str, str] = {}
    control_only = policy.allows_topic_mutation() and is_control_only_turn(text, delta)
    for key, value in delta.items():
        if key == "money_clarification":
            # A blocker is evidence about this turn, never an executable
            # mutation to prune. P16 must see it even on a follow-up.
            accepted[key] = value
        elif key not in policy.allowed_set:
            rejected[key] = "field_not_allowed_for_intent"
        elif control_only and key in TOPIC_FIELDS:
            rejected[key] = "control_only_without_new_domain_topic"
        else:
            accepted[key] = value

    if policy.transition_cleanup == "normalize_filter_transition":
        accepted = _normalize_filter_for_dataset(accepted, previous_params)

    clears = _transition_clears(policy, intent, text, previous_params, accepted)
    cleaned = dict(previous_params)
    actual_clears = sorted(key for key in clears if key in cleaned)
    for key in actual_clears:
        cleaned.pop(key, None)

    return GatedDelta(
        accepted_delta=accepted,
        cleaned_previous=cleaned,
        requested_fields=sorted(delta),
        accepted_fields=sorted(accepted),
        transition_cleared_fields=actual_clears,
        rejected_fields=sorted(rejected),
        rejection_reasons={key: rejected[key] for key in sorted(rejected)},
    )


_WHERE_RELEVANT_FIELDS = frozenset({
    *TOPIC_FIELDS, *SCOPE_FIELDS, *FILTER_FIELDS,
    "intent_type", "entidad_ecosystem", "entidad_contains_all", "entidad_in_candidates",
})


def validate_mutation_invariants(
    *,
    intent: str,
    previous_params: dict[str, Any],
    merged_params: dict[str, Any],
    gated: GatedDelta,
) -> None:
    """Fail closed if a gated mutation violates structural invariants."""
    if set(gated.accepted_fields) & set(gated.rejected_fields):
        raise ValueError("INV-8: a field cannot be both accepted and rejected")

    if intent == "change_order":
        before = {k: previous_params.get(k) for k in _WHERE_RELEVANT_FIELDS if previous_params.get(k)}
        after = {k: merged_params.get(k) for k in _WHERE_RELEVANT_FIELDS if merged_params.get(k)}
        if before != after:
            raise ValueError("INV-1: change_order mutated WHERE-relevant fields")

    if intent == "change_dataset" and merged_params.get("objeto") != previous_params.get("objeto"):
        raise ValueError("INV-2: change_dataset mutated objeto")

    if intent == "remove_filter" and merged_params.get("objeto") != previous_params.get("objeto"):
        raise ValueError("INV-4: remove_filter mutated objeto")
