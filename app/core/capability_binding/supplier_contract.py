"""SUPPLIERS capability input contract (A2 Driver Remediation V1).

SUPPLIERS receives only dimensions an authority can justify. What is not
accredited is not dropped, not interpreted and not executed: the turn ends in a
host-authored clarification with zero source calls, and it never falls back to
the legacy parser that produced the residue.

Outcomes:
- EXECUTE: accredited buyer, supported period/universe, no residue.
- CLARIFY: residue, unsupported cardinality/dimension, missing or unaccredited buyer.
- NOT_BOUND: the turn is not this capability's (actor subject, geographic
  universe without buyer). The existing route keeps ownership; this is the host
  precondition that contained every bad binding in A2 X0.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date as Date
from enum import Enum

from app.core.capability_binding.dogfood_telemetry import note
from app.core.capability_binding.residual import TOPIC_KEYS, account
from app.core.capability_binding.supplier_dogfood_guard import ambiguous_cardinal_surface
from app.core.capability_binding.supplier_semantics import (
    PolarityVeto,
    VetoOutcome,
    mention_refs,
    quantity_refs,
)
from app.core.capability_binding.supplier_request_authority import assess_supplier_request
from app.core.capability_binding.supplier_request_types import (
    Authorization,
    CardinalityRelation,
    CardinalityResidueReason,
    Polarity,
)
from app.core.entity_profile.supplier_ranking_request import (
    DEFAULT_SUPPLIER_LIMIT,
    DEFAULT_SUPPLIER_REQUEST,
    MAX_SUPPLIER_LIMIT,
    UNSUPPORTED_CARDINALITY_TEXT,
    SupplierRankingRequest,
    ranking_metric,
    resolve_supplier_ranking_request,
    transport_params,
)
from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_profile.turn import (
    GEOGRAPHY_PARAM_KEYS,
    profile_query_params,
    territory_clarification,
)


class Outcome(str, Enum):
    EXECUTE = "EXECUTE"
    CLARIFY = "CLARIFY"
    NOT_BOUND = "NOT_BOUND"


@dataclass(frozen=True, slots=True)
class Decision:
    outcome: Outcome
    reason: str
    response: str = ""
    detail: tuple[str, ...] = ()
    # EXECUTE only: what the capability may receive. Every topic-like term was
    # accounted for by another authority (source, criterion, control, buyer,
    # territory), so it is not topic and must not reach the query as one.
    admitted_params: dict | None = None

    @property
    def route_reason(self) -> str:
        return f"a2_suppliers_{self.reason.lower()}"


_BUYER = frozenset({"entidad", "entidad_resolved", "entidad_resolution", "buyer_group"})
_TIME = frozenset({"fecha_desde", "fecha_hasta"})
_UNIVERSE = frozenset({"contract_universe", "contract_universe_version"})
_META = frozenset({"dataset", "dataset_explicit", "limit", "offset", "intent_type"})
_ACTOR = ("contratista", "actor_scope", "actor_pending", "actor_intent_conflict", "actor_resolution")
_ADMITTED = _BUYER | _TIME | _UNIVERSE | _META | frozenset(TOPIC_KEYS)

_LABELS = (
    ("modalidad", "la modalidad"), ("valor_", "el filtro de valor"),
    ("estado", "el estado"), ("contract_type", "el tipo de contrato"),
    ("objeto_exclude", "la exclusión de un tema"), ("entidad_", "un comprador aproximado o múltiple"),
)


def _label(key: str) -> str:
    return next((label for prefix, label in _LABELS if key.startswith(prefix)), key)


def _clarify(reason: str, response: str, detail=(), record: dict | None = None) -> Decision:
    note(record, a2_route=reason, a2_outcome=Outcome.CLARIFY.value,
         a2_source_calls_expected=0)
    return Decision(Outcome.CLARIFY, reason, response, tuple(detail))


def _not_bound(reason: str, record: dict | None = None) -> Decision:
    note(record, a2_route=reason, a2_outcome=Outcome.NOT_BOUND.value,
         a2_source_calls_expected=0)
    return Decision(Outcome.NOT_BOUND, reason)


def _execute(admitted: dict, record: dict | None = None) -> Decision:
    note(record, a2_route="ACCREDITED", a2_outcome=Outcome.EXECUTE.value)
    return Decision(Outcome.EXECUTE, "ACCREDITED", admitted_params=admitted)


def _actor_subject(params: dict) -> bool:
    return (any(params.get(key) for key in _ACTOR)
            or params.get("actor_conversation_operation") == "SET_ACTOR")


def _unsupported(params: dict, ordering_accounted: bool) -> list[str]:
    keys = []
    for key, value in params.items():
        if key.startswith("_") or key in _ADMITTED or key in GEOGRAPHY_PARAM_KEYS:
            continue
        if value in (None, "", [], (), {}):
            continue
        if key == "ordering_signal" and ordering_accounted:
            continue
        if key in ("actor_conversation_operation", "actor_conversation_construction") and (
            params.get("actor_conversation_operation") == "SET_BUYER"
        ):
            continue
        keys.append(key)
    return keys


def _buyer_accredited(params: dict) -> bool:
    resolution = params.get("entidad_resolution")
    if isinstance(resolution, dict):
        method = str((resolution.get("metadata") or {}).get("resolution_method") or resolution.get("method") or "")
        if method.startswith("fuzzy") or resolution.get("clarification_needed"):
            return False
    return resolve_profile_scope(profile_query_params(params)).scope is not None


def _has_buyer_mention(params: dict) -> bool:
    return any(params.get(key) for key in _BUYER)


def _buyer_geography(params: dict, territory_names: tuple[str, ...]) -> tuple[str, ...]:
    """Return geography that would be lost by the profile projection.

    ``profile_query_params`` intentionally removes geography for a standalone
    profile.  SUPPLIERS V1 cannot prove that a geographic mention belongs to the
    buyer rather than being an independent filter, so an existing structured
    geography (or a territory still visible in the raw turn) is retained as a
    blocking witness instead of being silently discarded.
    """
    keys = [
        key for key in sorted(GEOGRAPHY_PARAM_KEYS)
        if params.get(key) not in (None, "", [], (), {})
    ]
    return tuple(dict.fromkeys([*keys, *territory_names]))


def decide(
    user_query: str,
    params: dict,
    *,
    polarity_veto: PolarityVeto | None = None,
    record: dict | None = None,
    today: Date | None = None,
) -> Decision:
    """``polarity_veto`` is consulted last and can only withhold EXECUTE.

    It is asked nothing the deterministic authority has not already approved,
    so it cannot widen this contract: with no veto wired the decision is
    exactly the one V1 made.

    ``record`` collects the dogfood receipt for this turn. It is write-only and
    read by nothing here, so a broken recorder cannot change a decision.
    """
    evidence = assess_supplier_request(user_query)
    note(
        record,
        a2_capability="entity_profile.suppliers",
        a2_mention_witnesses=len(evidence.mentions),
        a2_quantity_witnesses=len(evidence.cardinality.quantity_spans),
        a2_deterministic_polarity=evidence.polarity.value,
        a2_cardinal_relation=evidence.cardinality.relation.value,
        a2_deterministic_executable=evidence.authorization is Authorization.EXECUTABLE,
        a2_sidecar_called=False,
        a2_sidecar_status="NOT_WIRED",
        a2_rescued_by_sidecar=False,
    )
    # Actor Profile owns the subject before SUPPLIERS may interpret polarity,
    # cardinality, or any other guard.  Those guards describe the requested
    # supplier list; they cannot take ownership from an identified actor.
    if _actor_subject(params):
        return _not_bound("ACTOR_SUBJECT", record)
    if not evidence.mentions:
        return _not_bound("INTENT_WITNESS_MISSING", record)
    if evidence.polarity is not Polarity.POSITIVE:
        return _clarify("INTENT_WITNESS_NOT_POSITIVE", (
            "Entendí una mención negativa, mixta o incierta de proveedores y no voy a "
            "convertirla en una búsqueda positiva. ¿Quieres ver los proveedores de la "
            "entidad?"
        ), (evidence.polarity.value,), record)
    # DOGFOOD_CARDINAL_AMBIGUITY_GUARD: the x2 surfaces that opened an
    # executable route are refused here, before the sidecar is asked, so the
    # turn costs zero sidecar calls and zero source calls. It only blocks.
    ambiguity = ambiguous_cardinal_surface(user_query, evidence)
    if ambiguity is not None:
        note(record, a2_ambiguity_class=ambiguity)
        return _clarify("DOGFOOD_CARDINAL_AMBIGUITY", (
            f"Entendí una cantidad aproximada o indirecta de proveedores, y no voy a "
            f"resolverla adivinando. La vista de proveedores muestra los {DEFAULT_SUPPLIER_LIMIT} "
            f"principales por número de contratos y por valor. ¿Te sirven los "
            f"{DEFAULT_SUPPLIER_LIMIT} principales?"
        ), (ambiguity,), record)

    # A cardinal the local grammar could not read is deferred, not refused:
    # the sidecar is asked about the relation later, over a quantity the host
    # has already grounded. Without a sidecar wired this is False and the
    # deterministic whitelist decides alone, so the fallback never opens.
    #
    # Exactly one located quantity: the sidecar arbitrates a relation between
    # the mention and one number the host grounded, never a choice among
    # several. A competing quantity is refused earlier, by the admission
    # authority, which names it AMBIGUOUS_ATTACHMENT rather than context.
    deferred_cardinal = (
        polarity_veto is not None
        and evidence.cardinality.residue_reason
        is CardinalityResidueReason.UNACCREDITED_CONTEXT
        and len(evidence.cardinality.quantity_spans) == 1
    )
    if evidence.authorization is not Authorization.EXECUTABLE and not deferred_cardinal:
        requested = (
            "varias cantidades"
            if evidence.cardinality.relation is CardinalityRelation.UNKNOWN
            else " y ".join(str(value) for value in evidence.cardinality.values)
        ) or "una cantidad no acreditada"
        return _clarify("UNSUPPORTED_CARDINALITY", (
            f"La vista de proveedores muestra los {DEFAULT_SUPPLIER_LIMIT} principales por número de "
            f"contratos y por valor. Pediste {requested}; no voy a responder otra cantidad "
            f"en silencio. ¿Te sirven los {DEFAULT_SUPPLIER_LIMIT} principales?"
        ), (requested,), record)

    accounting = account(user_query, params, supplier_evidence=evidence, today=today)
    # A structured dimension the capability cannot apply is named first: its own
    # numbers ("más de 500 millones") are that dimension's, not free residue.
    unsupported = _unsupported(params, accounting.ordering_accounted)
    if unsupported:
        labels = ", ".join(dict.fromkeys(_label(key) for key in unsupported))
        return _clarify("UNSUPPORTED_DIMENSION", (
            f"La vista de proveedores todavía no aplica {labels}. No voy a quitar ese filtro "
            "en silencio. ¿Quieres los proveedores de la entidad sin ese filtro?"
        ), unsupported, record)

    if accounting.unaccredited:
        terms = ", ".join(f"«{term}»" for term in accounting.unaccredited)
        return _clarify("UNACCREDITED_RESIDUAL", (
            f"Para ver proveedores necesito entender toda la pregunta, y no puedo acreditar "
            f"qué significa {terms} aquí. No lo voy a ignorar ni a adivinar. La vista de "
            "proveedores responde hoy por entidad compradora y periodo; si es un tema del "
            "objeto contractual, todavía no puedo filtrarlo en esta vista."
        ), accounting.unaccredited, record)

    if accounting.period_conflict:
        return _clarify("PERIOD_NOT_REPRESENTED", (
            "Reconocí un periodo en tu pregunta, pero no puedo aplicarlo a la vista de "
            "proveedores exactamente como lo pediste. No voy a cambiarlo ni a quitarlo en "
            "silencio. Indica un año, mes o rango explícito (por ejemplo «en 2025» o "
            "«entre enero y marzo de 2025»)."
        ), (accounting.period_conflict,), record)

    if not _has_buyer_mention(params):
        if accounting.territory_names:
            return _clarify("TERRITORY_NOT_BUYER", territory_clarification(user_query),
                            accounting.territory_names, record)
        if any(params.get(key) for key in GEOGRAPHY_PARAM_KEYS):
            return _not_bound("UNIVERSE_WITHOUT_BUYER", record)
        return _clarify("BUYER_REQUIRED", (
            "Para ver los proveedores necesito una entidad compradora. Indica, por ejemplo, "
            "«Alcaldía de Galapa» o «Gobernación del Atlántico»."
        ), (), record)

    if not _buyer_accredited(params):
        note(record, a2_buyer_accredited=False)
        resolution = params.get("entidad_resolution")
        hint = resolution.get("clarification_hint") if isinstance(resolution, dict) else None
        mention = params.get("entidad") or "esa entidad"
        return _clarify("BUYER_NOT_ACCREDITED", hint or (
            f"No pude acreditar con exactitud la entidad compradora «{mention}». "
            "¿Cuál es su nombre institucional completo?"
        ), (str(mention),), record)

    geography = _buyer_geography(params, accounting.territory_names)
    if geography:
        return _clarify("GEOGRAPHY_WITH_BUYER", (
            "La vista de proveedores todavía no combina una entidad compradora con un "
            "filtro territorial independiente. No voy a quitar ese filtro en silencio. "
            "¿Quieres los proveedores de la entidad sin ese filtro?"
        ), geography, record)

    note(record, a2_buyer_accredited=True,
         a2_period_accredited=any(params.get(key) for key in _TIME))
    admitted = {key: value for key, value in params.items() if key not in TOPIC_KEYS}
    ranking = resolve_supplier_ranking_request(user_query, evidence)
    if ranking is not None:
        note(record, a2_ranking_metric=ranking.metric.value, a2_ranking_limit=ranking.limit)
        # El request viaja ya resuelto (dos primitivas) para que el plan lo
        # reconstruya validado: nadie aguas abajo vuelve a leer el español.
        admitted.update(transport_params(ranking))
    elif not deferred_cardinal:
        # La construcción o la cantidad no se pueden servir: se aclara en vez de
        # ejecutar otra vista en silencio. Un turno DIFERIDO no se cierra aquí:
        # su cantidad la arbitra el sidecar, y el request se construye con su
        # veredicto (unas líneas más abajo) o el turno aclara.
        return _clarify("UNSUPPORTED_CARDINALITY", UNSUPPORTED_CARDINALITY_TEXT,
                        (evidence.cardinality.relation.value,), record)
    if polarity_veto is None:
        return _execute(admitted, record)

    # Asked last, once every other authority has already accredited the turn:
    # a veto costs one call only where the turn was otherwise going to execute.
    quantities = quantity_refs(user_query, evidence)
    started = time.perf_counter()
    verdict = polarity_veto(user_query, mention_refs(user_query, evidence), quantities)
    note(
        record,
        a2_sidecar_called=True,
        a2_sidecar_latency_ms=round((time.perf_counter() - started) * 1000, 3),
        a2_sidecar_status=verdict.outcome.value,
        a2_turn_polarity=verdict.turn_polarity,
        a2_veto_reason=verdict.detail,
    )
    if verdict.outcome is VetoOutcome.CONFIRMED and deferred_cardinal:
        # The rescue is narrow on purpose: only an exact claim on a cardinality
        # the view can serve revives the turn. Every other relation the sentence
        # might express ends in a clarification. The request is built from the
        # verdict — the only place the host did not read the number itself.
        rescued = verdict.relation_value
        if (verdict.relation != "EXACT" or isinstance(rescued, bool)
                or not isinstance(rescued, int)
                or not 1 <= rescued <= MAX_SUPPLIER_LIMIT):
            return _clarify("UNSUPPORTED_CARDINALITY", (
                f"La vista de proveedores muestra los {DEFAULT_SUPPLIER_LIMIT} principales por número "
                f"de contratos y por valor, y entendí que pedías otra cantidad. "
                f"¿Te sirven los {DEFAULT_SUPPLIER_LIMIT} principales?"
            ), (verdict.relation,), record)
        request = SupplierRankingRequest(ranking_metric(user_query), rescued)
        admitted.update(transport_params(request))
        note(record, a2_rescued_by_sidecar=True, a2_cardinal_relation="EXACT",
             a2_ranking_metric=request.metric.value, a2_ranking_limit=request.limit)
        return _execute(admitted, record)
    if verdict.outcome is VetoOutcome.CONFIRMED:
        return _execute(admitted, record)
    if verdict.outcome is VetoOutcome.UNAVAILABLE:
        return _clarify("POLARITY_SIDECAR_UNAVAILABLE", (
            "No pude confirmar si querías incluir o excluir a los proveedores en esta "
            "pregunta, y no voy a suponerlo. ¿Quieres ver los proveedores de la entidad?"
        ), (verdict.detail,), record)
    return _clarify("POLARITY_VETO", (
        "Entendí que no querías ver a los proveedores en esta pregunta, o no me quedó "
        "claro si los querías. No voy a responder por proveedores sin estar seguro. "
        "¿Quieres ver los proveedores de la entidad?"
    ), (verdict.turn_polarity,), record)
