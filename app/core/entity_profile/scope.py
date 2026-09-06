"""Build a BuyerUnit/BuyerGroup scope without resolving text or territory."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from app.core.buyer_groups import BuyerGroupError, group_for_params
from app.core.entity_profile.models import (
    EntityProfileIdentity,
    EntityProfileScope,
    InstitutionReference,
    ScopeType,
    TerritoryContext,
)
from app.core.territorial_knowledge import load as load_territorial_knowledge


PRODUCT_RECOGNIZED = "PRODUCT_RECOGNIZED"


@dataclass(frozen=True, slots=True)
class ScopeResolution:
    scope: EntityProfileScope | None
    identity: EntityProfileIdentity | None
    reason: str | None = None


@lru_cache(maxsize=1)
def _recognized_knowledge() -> dict[str, tuple[Any, Any]]:
    """Exact product canonical -> (Institution, Territory), metadata only."""
    registry = load_territorial_knowledge()
    recognized: dict[str, tuple[Any, Any]] = {}
    for institution in registry.institutions:
        canonical = institution.product_buyer_unit
        if institution.knowledge_status != PRODUCT_RECOGNIZED or not canonical:
            continue
        territory = registry.territory_index[institution.territory_divipola]
        if canonical in recognized:
            raise ValueError(f"duplicate product BuyerUnit in knowledge: {canonical}")
        recognized[canonical] = (institution, territory)
    return recognized


def _topic(params: dict) -> tuple[str, ...]:
    items: list[str] = []
    for value in params.get("objeto") or ():
        if isinstance(value, list):
            items.extend(str(item) for item in value)
        else:
            items.append(str(value))
    items.extend(str(item) for item in params.get("objeto_or") or ())
    items.extend(str(item) for item in params.get("objeto_phrases") or ())
    return tuple(dict.fromkeys(item for item in items if item))


def _modality(params: dict) -> tuple[str, ...]:
    values = params.get("modalidad_include_procedure_ids") or ()
    if isinstance(values, str):
        return (values,)
    return tuple(str(value) for value in values if value)


def _territory_context(canonicals: tuple[str, ...]) -> tuple[TerritoryContext, ...]:
    contexts: list[TerritoryContext] = []
    for canonical in canonicals:
        matched = _recognized_knowledge().get(canonical)
        if not matched:
            continue
        institution, territory = matched
        contexts.append(TerritoryContext(
            divipola=territory.divipola,
            municipality=territory.municipality,
            department=territory.department,
            macro_region=territory.macro_region,
            institution_id=institution.institution_id,
            institutional_family=institution.family,
            knowledge_status=institution.knowledge_status,
            provenance=institution.provenance_refs,
        ))
    return tuple(contexts)


def _identity(
    display: str,
    canonicals: tuple[str, ...],
    group_id: str | None,
    contexts: tuple[TerritoryContext, ...],
) -> EntityProfileIdentity:
    context_by_id = {context.institution_id: context for context in contexts}
    institution_refs: list[InstitutionReference] = []
    knowledge = _recognized_knowledge()
    for canonical in canonicals:
        matched = knowledge.get(canonical)
        if not matched:
            continue
        institution, _ = matched
        context = context_by_id[institution.institution_id]
        institution_refs.append(InstitutionReference(
            institution_id=institution.institution_id,
            buyer_canonical=canonical,
            territory_divipola=context.divipola,
            knowledge_status=institution.knowledge_status,
            provenance=institution.provenance_refs,
        ))
    return EntityProfileIdentity(
        display_name=display,
        canonical_buyers=canonicals,
        buyer_group_id=group_id,
        institution_refs=tuple(institution_refs),
        resolution_authority=(
            "DECLARED_BUYER_GROUP" if group_id else "PRODUCT_BUYER_UNIT_RESOLUTION"
        ),
    )


def resolve_profile_scope(params: dict) -> ScopeResolution:
    """Accept only existing productive buyer authority; geography never qualifies."""
    try:
        group = group_for_params(params)
    except BuyerGroupError as exc:
        return ScopeResolution(None, None, str(exc))
    if group is not None:
        canonicals = tuple(group.members)
        contexts = _territory_context(canonicals)
        scope = EntityProfileScope(
            scope_type=ScopeType.BUYER_GROUP,
            buyer_units=canonicals,
            buyer_group_id=group.group_id,
            territory_context=contexts,
            time_from=_text_or_none(params.get("fecha_desde")),
            time_to=_text_or_none(params.get("fecha_hasta")),
            modality=_modality(params),
            topic=_topic(params),
        )
        return ScopeResolution(
            scope,
            _identity(group.label, canonicals, group.group_id, contexts),
        )

    canonical = params.get("entidad_resolved")
    resolution = params.get("entidad_resolution") or {}
    if not isinstance(canonical, str) or not canonical.strip():
        return ScopeResolution(None, None, "BUYER_SCOPE_REQUIRED")
    if not isinstance(resolution, dict):
        return ScopeResolution(None, None, "PRODUCTIVE_BUYER_RESOLUTION_REQUIRED")
    if resolution.get("confidence") != "high" or resolution.get("kind") != "buyer_entity":
        return ScopeResolution(None, None, "PRODUCTIVE_BUYER_RESOLUTION_REQUIRED")

    canonicals = (canonical,)
    contexts = _territory_context(canonicals)
    scope = EntityProfileScope(
        scope_type=ScopeType.BUYER_UNIT,
        buyer_units=canonicals,
        territory_context=contexts,
        time_from=_text_or_none(params.get("fecha_desde")),
        time_to=_text_or_none(params.get("fecha_hasta")),
        modality=_modality(params),
        topic=_topic(params),
    )
    return ScopeResolution(scope, _identity(canonical, canonicals, None, contexts))


def _text_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def validate_query_scope(scope: EntityProfileScope, params: dict) -> None:
    """Reject execution parameters that do not describe the displayed scope."""
    from app.core.actor_scope_policy import has_actor_scope

    if has_actor_scope(params) or params.get("contratista"):
        raise ValueError("Entity Profile scope cannot contain an actor filter")
    if any(params.get(key) for key in (
        "entidad_like", "entidad_ecosystem", "entidad_contains_all",
        "entidad_in_candidates",
    )):
        raise ValueError("Entity Profile scope has an unaccredited buyer override")
    resolved = resolve_profile_scope(params)
    if resolved.scope != scope:
        raise ValueError("Entity Profile scope does not match execution parameters")
    resolution = params.get("entidad_resolution") or {}
    if (
        not scope.buyer_group_id
        and resolution.get("value") not in (None, scope.buyer_units[0])
    ):
        raise ValueError("Entity Profile scope disagrees with buyer resolution")
