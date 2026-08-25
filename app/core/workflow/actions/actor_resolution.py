"""Impure AP3-C orchestration between parse/context and pure QueryPlan."""
from __future__ import annotations

import re

from burr.core import State, action

from app.config import actor_profile_enabled
from app.core.actor_arbitration import (
    detect_bare_actor_probe,
    has_canonical_geography_evidence,
    has_entity_like_name_evidence,
    has_strong_topic_evidence,
    scrub_strong_actor_leaks,
)
from app.core.actor_intent import (
    strong_actor_modifier_suffix,
    strong_actor_name_prefixes,
)
from app.core.actor_name import ActorResolutionStatus
from app.core.actor_resolution import (
    METHOD_EXPLICIT_DOCUMENT,
    METHOD_NAME_UNRESOLVED,
    actor_resolution_method,
    project_actor_resolution,
)
from app.core.negation_binding import requests_require_policy
from app.data.actor_alias_resolver import ActorNameResolver


_BOUNDARY_MODIFIER_KEYS = frozenset({
    "fecha_desde", "fecha_hasta", "ordering_signal",
    "valor_min", "valor_max", "money_metadata",
    "ciudad", "ciudad_resolved",
    "departamento", "departamento_resolved", "departamento_resolution",
    "region", "departamentos_in",
    "estado", "estado_field", "estado_family",
    "estado_contrato", "estado_del_procedimiento",
    "estado_de_apertura_del_proceso",
    "modalidad", "modalidad_resolved", "modalidad_resolution",
})


# Conectores que introducen un modificador externo. Un sufijo que no empiece
# por uno de ellos no es una cola: es la continuación del nombre.
_EXTERNAL_MODIFIER_LEAD_RE = re.compile(
    r"^(?:en|por|para|desde|hasta|entre|entre\s+el|con|de\s+mas|de\s+menos|"
    r"mayor(?:es)?|menor(?:es)?|mas|menos|superior(?:es)?|inferior(?:es)?|"
    r"antes|despues|durante|solo|solamente|unicamente)\b",
    re.IGNORECASE,
)


def _parse_exact_boundary_modifier_suffix(
    raw_query: str,
    actor_name: str,
) -> dict | None:
    """Validate and recover only a supported suffix after an exact alias."""
    suffix = strong_actor_modifier_suffix(raw_query, actor_name)
    if suffix is None:
        return None
    if not suffix:
        return {}
    if not _EXTERNAL_MODIFIER_LEAD_RE.match(suffix):
        # INV-ACTOR-RESOLUTION-REQUIRES-COMPLETE-MENTION.
        #
        # Un modificador externo viene INTRODUCIDO —"… en Bolívar", "… en
        # 2025", "… por mayor valor"—. Un token que va pegado al final del
        # nombre es parte del nombre, por mucho que parezca geografía:
        # "ALCALDIA DE PUERTO SALGAR CUNDINAMARCA" y "CONSORCIO SALUD
        # BOLIVAR 2024" se llaman así.
        #
        # Sin esta guarda el buscador de frontera podía recortar hasta
        # cualquier prefijo cuyo resto se dejara reparsear, y con un
        # directorio de 1,2 M de aliases eso encontraba a otro actor:
        # "proveedor E.S.E HOSPITAL PEDRO LEON ALVAREZ DIAZ en 2025"
        # terminaba en "e s", el alias exacto de la empresa E&S.
        return None

    # Reparse only the remainder, never the actor name.  This gives the normal
    # deterministic modifier grammar a clean view after exact alias evidence
    # fixed the boundary.  Any thematic residue makes the boundary indefensible.
    from app.core.query_router import QueryRouter

    parsed = dict(QueryRouter().parse(f"contratos {suffix}").params)
    # Una entidad nombrada en el resto delata que el corte partió un nombre
    # propio por la mitad: las colas admisibles son fecha, orden, monto,
    # geografía, estado y modalidad, nunca una entidad.
    if any(parsed.get(key) for key in (
        "objeto", "objeto_phrases", "actor_scope",
        "entidad", "entidad_resolved", "entidad_like",
    )):
        return None
    modifiers = {
        key: value
        for key, value in parsed.items()
        if key in _BOUNDARY_MODIFIER_KEYS and value not in (None, "", [], {}, ())
    }
    return modifiers or None


@action(
    reads=["parsed_params", "raw_query", "negation_requests"],
    writes=["parsed_params"],
)
def resolve_actor_name(state: State, actor_name_resolver: ActorNameResolver) -> State:
    """Resolve strong mentions fully and bare names by exact lookup only."""
    # Negative ACTOR/BUYER spans are already bound. No directory probe,
    # candidate retrieval or actor_pending projection may run before the
    # fail-closed policy clarification.
    if requests_require_policy(state.get("negation_requests") or []):
        return state
    if not actor_profile_enabled():
        return state

    params = dict(state.get("parsed_params", {}) or {})
    raw_query = str(state.get("raw_query", "") or "")
    method = actor_resolution_method(params)

    if method == METHOD_NAME_UNRESOLVED:
        scope = params.get("actor_scope")
        actor_name = scope.get("observed_name") if isinstance(scope, dict) else None
        if not isinstance(actor_name, str) or not actor_name.strip():
            return state
        result = None
        recovered_modifiers: dict = {}
        # Give the strong mention a chance to claim its longest exact alias
        # before accepting geography/date/state tokens as suffix modifiers.
        # Exact lookup may decide the boundary; fuzzy retrieval never may.
        for candidate in strong_actor_name_prefixes(
            raw_query,
            minimum_name=actor_name,
        ):
            exact = actor_name_resolver.resolve_exact(candidate)
            if exact.status in {
                ActorResolutionStatus.RESOLVED,
                ActorResolutionStatus.AMBIGUOUS,
            }:
                suffix_modifiers = _parse_exact_boundary_modifier_suffix(
                    raw_query, candidate,
                )
                if suffix_modifiers is None:
                    continue
                actor_name = candidate
                result = exact
                recovered_modifiers = suffix_modifiers
                break
            if exact.status == ActorResolutionStatus.INDEX_UNAVAILABLE:
                result = exact
                break

        params = scrub_strong_actor_leaks(params, actor_name)
        params.update(recovered_modifiers)
        scope = params.get("actor_scope")
        if isinstance(scope, dict):
            params["actor_scope"] = {**scope, "observed_name": actor_name}
        # As with AP2's explicit-document door, an implicit dataset must move
        # to contracts, the dataset that publishes contractor documents.
        if not params.get("dataset_explicit"):
            params["dataset"] = "contratos"
        if result is None:
            result = actor_name_resolver.resolve(actor_name)
        projected = project_actor_resolution(
            params,
            result,
            actor_name=actor_name,
            original_query=raw_query,
            origin="strong_actor_syntax",
        )
        return state.update(parsed_params=projected)

    # Any already-classified actor (especially an explicit document) bypasses
    # the optional bare-name probe.
    if method:
        if method == METHOD_EXPLICIT_DOCUMENT and not params.get(
            "actor_resolution_method"
        ):
            params["actor_resolution_method"] = METHOD_EXPLICIT_DOCUMENT
            params["actor_resolution_status"] = ActorResolutionStatus.RESOLVED
            return state.update(parsed_params=params)
        return state

    conversation_probe = params.pop("actor_conversation_probe", None)
    if isinstance(conversation_probe, dict):
        probe_name = conversation_probe.get("actor_name")
        if isinstance(probe_name, str) and probe_name.strip():
            result = actor_name_resolver.resolve_exact(probe_name)
            if result.status in {
                ActorResolutionStatus.RESOLVED,
                ActorResolutionStatus.AMBIGUOUS,
            }:
                projected = scrub_strong_actor_leaks(params, probe_name)
                projected.pop("objeto", None)
                projected.pop("objeto_phrases", None)
                projected = project_actor_resolution(
                    projected,
                    result,
                    actor_name=probe_name,
                    original_query=raw_query,
                    origin="actor_conversation_exact_probe",
                )
                return state.update(parsed_params=projected)
        # A probe is not actor evidence by itself.  An exact miss/unavailable
        # keeps the generic interpretation and never opens fuzzy retrieval.
        params.pop("actor_conversation_operation", None)
        params.pop("actor_conversation_construction", None)
        return state.update(parsed_params=params)

    probe = detect_bare_actor_probe(raw_query, params)
    if probe is None:
        return state
    if has_canonical_geography_evidence(probe.actor_name, params):
        # INV-BARE-GEOGRAPHY-PRECEDENCE: el frame territorial del parser se
        # conserva intacto. La sonda desnuda es evidencia opcional, y un
        # topónimo canónico no deja de serlo porque alguien se llame así en
        # el directorio.
        return state
    actor_name = probe.unbounded_name
    result = actor_name_resolver.resolve_exact(actor_name)
    if (
        result.status == ActorResolutionStatus.UNRESOLVED
        and actor_name != probe.actor_name
    ):
        actor_name = probe.actor_name
        result = actor_name_resolver.resolve_exact(actor_name)
    if result.status in {
        ActorResolutionStatus.UNRESOLVED,
        ActorResolutionStatus.INDEX_UNAVAILABLE,
    }:
        # Known topics retain their exact baseline semantics.  An entity-like
        # miss, however, may be an uncovered actor and must not silently become
        # a morphology-expanded thematic answer.
        entity_like = has_entity_like_name_evidence(probe)
        if has_strong_topic_evidence(probe.actor_name, params) and not entity_like:
            return state
        article_led_buyer = bool(re.match(
            r"^(?:el|la|los|las|un|una)\s+(?:alcaldia|gobernacion|"
            r"secretaria|municipio|departamento|distrito|entidad)\b",
            probe.unbounded_name,
            re.IGNORECASE,
        ))
        if article_led_buyer and (
            params.get("entidad") or params.get("entidad_resolved")
        ):
            # ``contratos de la alcaldía de X`` already has independent buyer
            # evidence.  After an exact actor miss, preserve that established
            # reading instead of replacing it with a bare-name clarification.
            # The article alone is not buyer evidence: real actors such as
            # ``LA PREVISORA S.A. COMPAÑÍA DE SEGUROS`` must fail closed when
            # an entity scan recognizes only a prefix of their complete name.
            return state
        if not entity_like:
            return state
        actor_name = probe.actor_name
        if result.status == ActorResolutionStatus.UNRESOLVED:
            # Independent entity-shaped evidence authorizes candidate
            # retrieval, but the candidates remain suggestions.  Retrieval
            # itself is never used as evidence that the phrase was an actor.
            fuzzy = actor_name_resolver.resolve(actor_name)
            if fuzzy.status in {
                ActorResolutionStatus.CANDIDATES,
                ActorResolutionStatus.AMBIGUOUS,
            }:
                projected = scrub_strong_actor_leaks(params, actor_name)
                projected.pop("objeto", None)
                projected.pop("objeto_phrases", None)
                projected["actor_conversation_operation"] = "SET_ACTOR"
                projected["actor_conversation_construction"] = "bare_actor_like"
                projected = project_actor_resolution(
                    projected,
                    fuzzy,
                    actor_name=actor_name,
                    original_query=raw_query,
                    origin="bare_actor_like",
                )
                return state.update(parsed_params=projected)
        projected = scrub_strong_actor_leaks(params, actor_name)
        projected.pop("objeto", None)
        projected.pop("objeto_phrases", None)
        projected["actor_resolution_status"] = result.status
        projected["actor_resolution_method"] = METHOD_NAME_UNRESOLVED
        projected["actor_resolution"] = METHOD_NAME_UNRESOLVED
        projected["actor_scope"] = {
            "role": "contractor",
            "resolution_method": METHOD_NAME_UNRESOLVED,
            "requires_resolution": True,
            "observed_name": actor_name,
        }
        if result.status == ActorResolutionStatus.UNRESOLVED:
            projected["actor_intent_conflict"] = {
                "kind": "entity_like_no_exact",
                "actor_name": actor_name,
                "resolution_status": result.status,
            }
        return state.update(parsed_params=projected)

    baseline_params = params
    params = scrub_strong_actor_leaks(params, actor_name)
    params.pop("objeto", None)
    params.pop("objeto_phrases", None)
    projected = project_actor_resolution(
        params,
        result,
        actor_name=actor_name,
        original_query=raw_query,
        origin="bare_exact_probe",
    )
    if has_strong_topic_evidence(probe.actor_name, baseline_params):
        # Restore baseline topic fields: neither interpretation won.  P17 will
        # ask whether X is the topic or the observed provider.
        for key in ("objeto", "objeto_phrases"):
            if key in baseline_params:
                projected[key] = baseline_params[key]
        projected["actor_intent_conflict"] = {
            "actor_name": actor_name,
            "resolution_status": result.status,
        }
    return state.update(parsed_params=projected)
