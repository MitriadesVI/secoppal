"""Parsing actions — user_query → parsed_params.

Three Burr nodes that turn the raw text into a structured parameter
dict before any SECOP call:

- ``parse_query``    — deterministic parser (QueryRouter).
- ``llm_parse``      — LLM fallback when the parser flags ``needs_llm``.
- ``apply_context``  — merges previous-turn frame into the current
                      parsed_params for follow-up turns.

Moved here verbatim from ``app/core/orchestrator.py`` in Paso 5B. No
behavior changes; the @action decorators, ``reads``/``writes``
declarations and bodies are byte-for-byte identical.
"""
from __future__ import annotations

import logging
import time
from dataclasses import asdict

from burr.core import State, action

from app.core.query_plan import QueryPlan

from app.core import modality_followup_wiring as modality_wiring
from app.core.actor_conversation import (
    ActorConversationOperation,
    ActorPendingTurn,
    classify_actor_pending_turn,
    purge_actor_state,
)
from app.core.actor_resolution import is_authoritative_actor_resolution
from app.core.actor_resolution_admission import can_resolve_new_actor
from app.core.followup_engine import FollowupGuards, detect_and_merge
from app.core.llm_handler import LLMHandler
from app.core.negation_binding import (
    NegationDimension,
    NegationRequest,
    blocking_negation_reason,
    negation_clarification_question,
    requests_require_policy,
)
from app.core.negation_policy import apply_negation_policy
from app.core.negation_followup import (
    REMOVE_EXCLUSION,
    REPLACE_EXCLUSION,
    REINCLUDE,
    classify_department_reinclude,
    classify_object_exclusion_mutation,
)
from app.core.object_negation_policy import (
    OBJECT_NEGATION_CONTRADICTION_QUESTION,
    object_negation_overlap,
)
from app.core.query_frame import frame_from_params, params_from_frame
from app.core.query_router import QueryRouter
from app.core.workflow.helpers import _with_timing

logger = logging.getLogger(__name__)

# ── R2.2 · Autoridad del fallback LLM: allowlist monotónica ────────────────
#
# La política previa era "el LLM puede escribir todo salvo lo que vayamos
# descubriendo y protegiendo", y por eso cada auditoría encontraba otra puerta
# (modalidad en R2, dataset en R2.1, estado_contrato en el canary). Aquí se
# invierte: el LLM no puede escribir NADA salvo estos campos, de modo que
# dataset, modalidad, estados, intent, orden, paginación, IDs y cualquier campo
# FUTURO quedan protegidos por defecto, sin necesidad de enumerarlos.
#
# Monto y fecha quedan deliberadamente FUERA de esta primera versión: cambian el
# universo de resultados de forma silenciosa (un "1.000 millones" leído como 100
# es una consulta válida y semánticamente falsa) y todavía no hay un validador
# estricto propio al que someter la propuesta.
# Dimensiones geográficas/de entidad: independientes entre sí, valor escalar.
#
# DEUDA LLM-GEO-COHERENCE-001 (fuera del alcance de M3C-C): al ser independientes,
# el LLM puede completar las tres a la vez con una combinación individualmente
# válida pero mutuamente incoherente (entidad de Medellín + ciudad Barranquilla +
# departamento Cundinamarca). La validación cruzada exige resolución determinística
# contra el gazetteer y no se improvisa aquí.
_LLM_SCALAR_FIELDS = frozenset({"entidad", "ciudad", "departamento"})

# R2.3 — El topic es UNA dimensión semántica repartida en tres claves. La
# monotonicidad debe medirse por dimensión, no por clave: si el parser ya resolvió
# el tema en cualquiera de ellas, el LLM no puede escribir NINGUNA. Sin esto,
# ``_promote_synonym_expansion`` del handler real (que convierte ``objeto`` en
# ``objeto_or``) permitía ampliar un tema ya interpretado por una clave lateral.
_LLM_TOPIC_FIELDS = frozenset({"objeto", "objeto_or", "objeto_phrases"})

# ``objeto_phrases`` queda FUERA de lo escribible: no está en el schema del tool y
# sus valores son identificadores canónicos del catálogo de frases, que ningún
# canonizador autorizado del LLM produce. Si ya viene en los params se preserva;
# lo que no puede es crearlo ni reemplazarlo.
_LLM_TOPIC_WRITABLE = frozenset({"objeto", "objeto_or"})

_LLM_ASSISTED_FIELDS = _LLM_SCALAR_FIELDS | _LLM_TOPIC_WRITABLE

# Un valor "vacío" no resuelve nada: ni cuenta como propuesta del LLM ni como
# campo ya resuelto por el parser.
_EMPTY_VALUES = (None, "", [], {}, (), False)


@action(
    reads=["user_query", "today", "timings_ms"],
    writes=[
        "parsed_params", "needs_llm", "route_reason",
        "modality_interpretation", "modality_followup_instruction", "timings_ms",
        "negation_requests", "positive_query",
    ],
)
def parse_query(state: State, query_router: QueryRouter) -> State:
    start = time.perf_counter()
    result = query_router.parse(state["user_query"], today=state.get("today"))
    # M3C-C: clasifica SOLO lenguaje de control (allow_implicit_replace=False; el
    # REPLACE implícito se decide en apply_context, ya conocido el intent real).
    # M2 no se re-ejecuta: la instrucción viaja como metadata efímera JSON-native.
    instruction = modality_wiring.classify_control(
        state["user_query"], result.modality_interpretation, allow_implicit_replace=False
    )
    return _with_timing(
        state,
        "parse_ms",
        start,
        parsed_params=result.params,
        # A blocked negation must not trigger an external LLM call before the
        # deterministic clarification plan is built.
        needs_llm=(
            result.needs_llm
            and not requests_require_policy(result.negation_requests)
        ),
        route_reason=result.route_reason,
        modality_interpretation=result.modality_interpretation,
        modality_followup_instruction=modality_wiring.instruction_to_state(instruction),
        negation_requests=[request.to_state() for request in result.negation_requests],
        positive_query=result.positive_text,
    )


def _already_resolved(params: dict, field: str) -> bool:
    """True si el parser determinístico ya fijó el campo (o su resolución de gazetteer).

    ``entidad``/``departamento``/``ciudad`` tienen una contraparte ``*_resolved``
    producida por el gazetteer: si existe, el campo está resuelto aunque la clave
    base venga vacía, y el LLM no puede proponer una alternativa.
    """
    for key in (field, f"{field}_resolved"):
        if params.get(key) not in _EMPTY_VALUES:
            return True
    return False


def _topic_is_resolved(params: dict) -> bool:
    """True si el parser ya fijó el TEMA en cualquiera de las claves del cluster."""
    return any(params.get(key) not in _EMPTY_VALUES for key in _LLM_TOPIC_FIELDS)


def _is_canonical_str(value: object) -> bool:
    """``str`` EXACTO, con contenido y YA canónico (sin whitespace de borde).

    R2.4 — política ESTRICTA, no normalizadora: esta función es frontera hostil y
    "sin coerciones" también vale para el whitespace. Validar con ``strip()`` y
    guardar el original dejaba pasar ``"  Cali  "``, que rompe el gazetteer, mete
    espacios en el SoQL y produce fingerprints distintos para valores equivalentes.
    El LLM entrega la forma canónica o su propuesta se rechaza.
    """
    return type(value) is str and value != "" and value == value.strip()


def _is_valid_topic_list(value: object) -> bool:
    """``list`` EXACTA, no vacía, de ``str`` canónicos.

    Rechaza str (que ``list()`` partiría en caracteres), tuple, set, dict,
    generadores, números, ``bool`` y listas anidadas: los grupos OR anidados son
    una construcción del parser, no algo que el LLM pueda proponer.
    """
    if type(value) is not list or not value:
        return False
    return all(_is_canonical_str(item) for item in value)


def _merge_llm_proposal(incoming: dict, proposed: object) -> tuple[dict, list[str], list[str]]:
    """Merge MONOTÓNICO y TIPADO de la propuesta del LLM sobre los params del parser.

    El LLM puede COMPLETAR huecos, nunca deshacer una decisión válida del parser:

    * se parte siempre de ``incoming`` ⇒ borrar una clave es imposible;
    * una clave fuera de ``_LLM_ASSISTED_FIELDS`` se descarta ⇒ cambiar dataset,
      modalidad, estados o cualquier campo cerrado (presente o futuro) es imposible;
    * una dimensión que el parser YA resolvió se descarta ⇒ reemplazar un valor
      determinístico es imposible. Para el topic la unidad es la DIMENSIÓN, no la
      clave: un ``objeto`` resuelto bloquea también ``objeto_or``;
    * un valor con forma inválida se descarta ⇒ esta función es frontera de
      seguridad, así que valida tipos y no solo nombres.

    Devuelve ``(merged, accepted, rejected)`` con los NOMBRES de las claves, para
    telemetría sin exponer valores del usuario.
    """
    if not isinstance(proposed, dict):
        raise ValueError(f"resultado del LLM debe ser dict, no {type(proposed).__name__}")
    merged = dict(incoming)
    accepted: list[str] = []
    rejected: list[str] = []
    # R2.4 — El cluster temático admite UNA sola escritura por turno, decidida por
    # política y NO por el orden en que el modelo serializó su JSON. Si llegan dos
    # representaciones nuevas de la misma dimensión (una AND, otra OR) son
    # contradictorias entre sí: se rechazan AMBAS en vez de dejar que gane la que
    # aparezca primero en el dict.
    topic_candidates = [
        key for key in _LLM_TOPIC_WRITABLE
        if key in proposed and not (key in incoming and proposed[key] == incoming[key])
    ]
    topic_open = not _topic_is_resolved(incoming) and len(topic_candidates) <= 1
    for key, value in proposed.items():
        if key in incoming and value == incoming[key]:
            continue  # eco de lo que ya entró: no es una propuesta
        if key in _LLM_SCALAR_FIELDS:
            if _already_resolved(incoming, key) or not _is_canonical_str(value):
                rejected.append(key)
                continue
            merged[key] = value
        elif key in _LLM_TOPIC_WRITABLE:
            if not topic_open or not _is_valid_topic_list(value):
                rejected.append(key)
                continue
            merged[key] = list(value)  # copia: la lista del LLM no queda referenciada
        else:
            rejected.append(key)
            continue
        accepted.append(key)
    return merged, accepted, rejected


@action(
    reads=[
        "user_query", "positive_query", "negation_requests",
        "needs_llm", "parsed_params", "timings_ms",
    ],
    writes=["parsed_params", "timings_ms"],
)
def llm_parse(state: State, llm_handler: LLMHandler) -> State:
    """Fallback LLM bajo allowlist monotónica (R2.2).

    El LLM amplía la cobertura del parser, pero nunca amplía sus propios permisos.
    Solo puede rellenar los campos de ``_LLM_ASSISTED_FIELDS`` que el parser dejó
    sin resolver; todo lo demás del turno se preserva exactamente como entró.

    Eso cubre de una vez —y por defecto, también para campos futuros— lo que antes
    se parcheaba caso a caso: la modalidad canónica (autoridad de M2 en el turno
    inicial y del motor puro en el follow-up), el ``dataset`` que la hace aplicable,
    y los estados acoplados al dataset, que ya ni siquiera llegan al merge. M2 no
    se re-ejecuta y no se reconstruye modalidad desde texto.

    Los invariantes determinísticos que ``LLMHandler.parse`` aplica alrededor del
    modelo (oportunidad/dataset firmado) no se pierden al descartarse aquí: P10,
    P11 y P12 los reimponen sobre ``raw_query`` en ``plan_query``, aguas abajo.
    """
    start = time.perf_counter()
    if not state["needs_llm"]:
        return _with_timing(state, "llm_parse_ms", start)
    incoming_params = state["parsed_params"]
    # A model cannot recreate positive evidence from a governed negative span.
    llm_query = state.get("positive_query") or state["user_query"]
    proposed = llm_handler.parse(llm_query, existing_params=incoming_params)
    merged, accepted, rejected = _merge_llm_proposal(incoming_params, proposed)
    if accepted or rejected:
        logger.info(
            "llm_parse allowlist: accepted=%s rejected=%s", sorted(accepted), sorted(rejected)
        )
    return _with_timing(state, "llm_parse_ms", start, parsed_params=merged)


_NONE_INSTRUCTION = None  # inicializado perezosamente para evitar import en tiempo de carga


def _none_instruction():
    """Instrucción 'sin operación' de respaldo cuando no hay metadata efímera."""
    global _NONE_INSTRUCTION
    if _NONE_INSTRUCTION is None:
        from app.core.modality_followup import ModalityFollowupInstruction
        _NONE_INSTRUCTION = ModalityFollowupInstruction(
            status="none", operation=None, control_code=None, construction_id=None,
            source_text="", control_spans=(), target_required=False, explanation="",
        )
    return _NONE_INSTRUCTION


def _load_instruction(state: State):
    """Carga la instrucción efímera con semántica EXACTA de ausencia (§4).

    Solo ``None`` significa "no hay metadata" (el nodo ``parse_query`` no corrió).
    Cualquier otro payload —incluidos los falsy ``{}``, ``[]``, ``""``, ``0`` y
    ``False``— es corrupción interna del state y debe fallar CERRADO en la
    deserialización estricta, nunca degradarse en silencio a un no-op que dejaría
    la modalidad sin gobierno. No se convierte en aclaración de usuario: no es
    ambigüedad conversacional.
    """
    payload = state.get("modality_followup_instruction")
    if payload is None:
        return _none_instruction()
    return modality_wiring.instruction_from_state(payload)


def _block_negation_lifecycle_mutation(
    state: State,
    start: float,
    *,
    previous_params: dict,
    reason: str,
    clarification: str,
    lifecycle_trace: dict,
) -> State:
    """Block one unconfirmed lifecycle command without mutating context."""
    blocked_plan = asdict(QueryPlan(
        should_execute=False,
        params=dict(previous_params),
        risk_flags=["NEGATION_LIFECYCLE_CLARIFICATION"],
        clarification_question=clarification,
        reason=reason,
    ))
    return _with_timing(
        state,
        "apply_context_ms",
        start,
        parsed_params=dict(previous_params),
        resolved_params=dict(previous_params),
        needs_llm=False,
        needs_clarification=True,
        clarification_reason=clarification,
        intent_type=reason,
        followup_intent_type=reason,
        followup_mutation={
            "intent": reason,
            "requested_fields": ["objeto_exclude"],
            "accepted_fields": [],
            "transition_cleared_fields": [],
            "rejected_fields": ["negation_mutation"],
            "rejection_reasons": {"negation_mutation": reason},
            "negation_lifecycle": lifecycle_trace,
        },
        query_plan=blocked_plan,
    )


@action(
    reads=[
        "parsed_params", "context_params", "followup", "user_query",
        "positive_query", "negation_requests", "modality_interpretation",
        "modality_followup_instruction", "timings_ms",
    ],
    writes=[
        "parsed_params", "needs_llm", "needs_clarification",
        "clarification_reason", "intent_type", "followup_intent_type",
        "followup_mutation", "query_plan", "resolved_params",
        "negation_requests", "timings_ms",
    ],
)
def apply_context(state: State) -> State:
    """Fusiona context_params del turno anterior con parsed_params del parse actual.
    Usa followup_engine.detect_and_merge para clasificar la intención granular
    y aplicar políticas de merge por tipo de follow-up.

    M3C-C: la modalidad se delega FUERA del merge genérico. Las dos claves
    canónicas se retiran del delta antes de detect_and_merge y el motor puro es
    la única autoridad del filtro final. Un target M2 bloqueado cortocircuita el
    turno (filtro previo intacto, sin deltas ortogonales, sin ejecución).

    Solo corre cuando followup=True.
    """
    start = time.perf_counter()
    if not state.get("followup"):
        return _with_timing(state, "apply_context_ms", start)

    context_params = state.get("context_params", {})
    parsed_params = state.get("parsed_params", {})
    user_query = state.get("user_query", "")
    interpretation = state.get("modality_interpretation")
    instruction = _load_instruction(state)
    previous_frame = frame_from_params(context_params)

    negation_requests = state.get("negation_requests") or []
    if negation_requests:
        typed_requests = tuple(
            request
            if isinstance(request, NegationRequest)
            else NegationRequest.from_state(request)
            for request in negation_requests
        )
        # A department set may come from the confirmed conversational frame,
        # not from the isolated follow-up text. Re-evaluate capability against
        # that effective scope before deciding to clarify.
        capability_params = {**context_params, **parsed_params}
        policy = apply_negation_policy(typed_requests, capability_params)
        negation_requests = [request.to_state() for request in policy.requests]
        state = state.update(negation_requests=negation_requests)
        if not policy.blocks_execution and any(
            request.dimension is NegationDimension.DEPARTMENT
            for request in policy.requests
        ):
            parsed_params = dict(parsed_params)
            parsed_params["departamentos_in"] = list(
                policy.params["departamentos_in"]  # type: ignore[arg-type]
            )

    if requests_require_policy(negation_requests):
        # Fail closed before actor precedence or generic follow-up
        # classification. The previous frame remains intact.
        previous_params = dict(context_params)
        clarification = negation_clarification_question(negation_requests)
        reason, risk_flag = blocking_negation_reason(negation_requests)
        blocked_plan = asdict(QueryPlan(
            should_execute=False,
            params=dict(previous_params),
            risk_flags=[risk_flag],
            clarification_question=clarification,
            reason=reason,
        ))
        return _with_timing(
            state,
            "apply_context_ms",
            start,
            parsed_params=previous_params,
            resolved_params=dict(previous_params),
            negation_requests=negation_requests,
            needs_llm=False,
            needs_clarification=True,
            clarification_reason=clarification,
            intent_type=reason,
            followup_intent_type=reason,
            followup_mutation={
                "intent": reason,
                "requested_fields": [],
                "accepted_fields": [],
                "transition_cleared_fields": [],
                "rejected_fields": ["negative_span"],
                "rejection_reasons": {
                    "negative_span": risk_flag,
                },
            },
            query_plan=blocked_plan,
        )

    temporal_error = parsed_params.get("_temporal_error")
    if temporal_error:
        # Una mutación temporal inválida es transaccional: no mezcla un extremo
        # nuevo con el scope confirmado ni reemplaza el frame conversacional.
        previous_params = dict(context_params)
        clarification = str(parsed_params.get("_temporal_clarification") or (
            "No pude resolver esa referencia temporal con seguridad. "
            "¿Qué intervalo querías usar?"
        ))
        blocked_plan = asdict(QueryPlan(
            should_execute=False,
            params=previous_params,
            risk_flags=[f"TEMPORAL_{str(temporal_error).upper()}"],
            clarification_question=clarification,
            reason="invalid_temporal_scope",
        ))
        return _with_timing(
            state,
            "apply_context_ms",
            start,
            parsed_params=previous_params,
            resolved_params=previous_params,
            needs_llm=False,
            needs_clarification=True,
            clarification_reason=clarification,
            intent_type="invalid_temporal_scope",
            followup_intent_type="invalid_temporal_scope",
            followup_mutation={
                "intent": "invalid_temporal_scope",
                "requested_fields": ["fecha_desde", "fecha_hasta"],
                "accepted_fields": [],
                "transition_cleared_fields": [],
                "rejected_fields": ["temporal_scope"],
                "rejection_reasons": {"temporal_scope": str(temporal_error)},
            },
            query_plan=blocked_plan,
        )

    pending = context_params.get("actor_pending")
    if isinstance(pending, dict) and classify_actor_pending_turn(
        user_query, parsed_params, pending,
    ) != ActorPendingTurn.NEW_SEARCH:
        # A rank/exact list name was already handled by actor_selection.
        # An ambiguous reply must not mutate the original frame or guess a
        # candidate. P17 will ask again using the same list and nonce.
        return _with_timing(
            state, "apply_context_ms", start,
            parsed_params=dict(context_params),
            resolved_params=dict(context_params),
            needs_llm=False,
            intent_type="actor_pending_ambiguous",
            followup_intent_type="actor_pending_ambiguous",
        )

    # AP3-F precedence: explicit actor operations and contextual buyer
    # relations mutate the conversational frame before the generic classifier
    # can reinterpret their words as topic/geography.
    operation = parsed_params.get("actor_conversation_operation")
    if operation in {
        ActorConversationOperation.SET_ACTOR,
        ActorConversationOperation.REPLACE_ACTOR,
    } and (
        parsed_params.get("actor_resolution")
        or parsed_params.get("actor_scope")
        or parsed_params.get("contratista")
        or parsed_params.get("actor_conversation_probe")
    ):
        if operation == ActorConversationOperation.REPLACE_ACTOR:
            previous_params = purge_actor_state(params_from_frame(previous_frame))
            current = dict(parsed_params)
            if not current.get("dataset_explicit"):
                current.pop("dataset", None)
                current.pop("dataset_explicit", None)
            merged = {**previous_params, **current}
            intent = "replace_actor"
        else:
            # A complete actor operation starts a new actor search and cannot
            # inherit any old IdentityKey or pending candidate list.
            merged = dict(parsed_params)
            intent = "set_actor"
        mutation_trace = {
            "intent": intent,
            "requested_fields": sorted(parsed_params),
            "accepted_fields": sorted(parsed_params),
            "transition_cleared_fields": sorted(
                set(context_params) - set(merged)
            ),
            "rejected_fields": [],
            "rejection_reasons": {},
        }
        return _finish_apply_context(
            state, start, merged, intent, mutation_trace,
        )

    if (
        operation == ActorConversationOperation.SET_BUYER
        and is_authoritative_actor_resolution(context_params)
    ):
        merged = dict(params_from_frame(previous_frame))
        entity_cluster = {
            "entidad", "entidad_resolved", "entidad_resolution", "entidad_like",
        }
        for key in entity_cluster:
            merged.pop(key, None)
        for key, value in parsed_params.items():
            if key in entity_cluster or key in {
                "objeto", "objeto_phrases", "actor_conversation_operation",
                "actor_conversation_construction",
            }:
                if value not in (None, "", [], {}, ()):
                    merged[key] = value
        mutation_trace = {
            "intent": "set_buyer",
            "requested_fields": sorted(parsed_params),
            "accepted_fields": sorted(
                key for key in parsed_params
                if key in entity_cluster or key in {"objeto", "objeto_phrases"}
            ),
            "transition_cleared_fields": sorted(
                key for key in entity_cluster if key in context_params
            ),
            "rejected_fields": [],
            "rejection_reasons": {},
        }
        return _finish_apply_context(
            state, start, merged, "set_buyer", mutation_trace,
        )

    if (
        operation == ActorConversationOperation.SET_TERRITORY
        and is_authoritative_actor_resolution(context_params)
    ):
        merged = dict(params_from_frame(previous_frame))
        territory_cluster = {
            "ciudad", "ciudad_resolved", "departamento",
            "departamento_resolved", "departamento_resolution", "region",
            "departamentos_in",
        }
        for key in territory_cluster:
            merged.pop(key, None)
        accepted = territory_cluster | {
            "actor_conversation_operation", "actor_conversation_construction",
        }
        for key, value in parsed_params.items():
            if key in accepted and value not in (None, "", [], {}, ()):
                merged[key] = value
        mutation_trace = {
            "intent": "set_territory",
            "requested_fields": sorted(parsed_params),
            "accepted_fields": sorted(key for key in parsed_params if key in accepted),
            "transition_cleared_fields": sorted(
                key for key in territory_cluster if key in context_params
            ),
            "rejected_fields": [],
            "rejection_reasons": {},
        }
        return _finish_apply_context(
            state, start, merged, "set_territory", mutation_trace,
        )

    # OBJECT lifecycle uses the existing negative slot; no pending state is
    # introduced. Department reinclusion is reconstructed only from explicit
    # region provenance and otherwise fails closed transactionally.
    object_mutation = None
    department_mutation = None
    if instruction.status == "none":
        object_mutation = classify_object_exclusion_mutation(
            user_query,
            parsed_params,
            list(context_params.get("objeto_exclude") or []),
        )
        if object_mutation is None:
            department_mutation = classify_department_reinclude(
                user_query,
                parsed_params,
                context_params,
            )

    if object_mutation is not None and not object_mutation.should_apply:
        clarification = object_mutation.clarification or (
            "No pude aplicar esa mutación de exclusión con seguridad."
        )
        return _block_negation_lifecycle_mutation(
            state,
            start,
            previous_params=context_params,
            reason=object_mutation.reason,
            clarification=clarification,
            lifecycle_trace=object_mutation.trace(
                list(context_params.get("objeto_exclude") or [])
            ),
        )

    if department_mutation is not None:
        lifecycle_trace = department_mutation.trace(
            list(context_params.get("departamentos_in") or []),
            context_params.get("region"),
        )
        if not department_mutation.should_apply:
            clarification = department_mutation.clarification or (
                "No pude reconstruir ese alcance territorial con seguridad."
            )
            return _block_negation_lifecycle_mutation(
                state,
                start,
                previous_params=context_params,
                reason=department_mutation.reason,
                clarification=clarification,
                lifecycle_trace=lifecycle_trace,
            )
        merged = params_from_frame(previous_frame)
        merged["departamentos_in"] = list(
            department_mutation.resulting_departments
        )
        mutation_trace = {
            "intent": "department_reinclude",
            "requested_fields": ["departamentos_in"],
            "accepted_fields": ["departamentos_in"],
            "transition_cleared_fields": [],
            "rejected_fields": [],
            "rejection_reasons": {},
            "negation_lifecycle": lifecycle_trace,
        }
        return _finish_apply_context(
            state, start, merged, "department_reinclude", mutation_trace,
        )

    # El follow-up genérico ve la modalidad (la usa para clasificar refine_filter
    # vs new_search) pero su resultado de modalidad se descarta: el motor puro es
    # la única autoridad del output. Solo se enmascaran los spans de control y se
    # limpian del objeto los verbos de control (§8: nunca entran al topic).
    positive_query = state.get("positive_query") or user_query
    masked_query = modality_wiring.mask_control_spans(positive_query, instruction)
    generic_current = modality_wiring.scrub_control_topic(
        parsed_params, user_query, instruction.control_spans
    )

    merge_text = masked_query
    intent_override = None
    if object_mutation is not None:
        merge_text = user_query
        if object_mutation.operation in {REMOVE_EXCLUSION, REINCLUDE}:
            # The parser sees control words as a positive topic. Replace that
            # tentative frame with the authorized negative-slot result.
            generic_current = {}
            if object_mutation.resulting:
                generic_current["objeto_exclude"] = list(
                    object_mutation.resulting
                )
            intent_override = "remove_filter"
        elif (
            object_mutation.operation == REPLACE_EXCLUSION
            and generic_current.get("objeto")
        ):
            # "no X sino Y" explicitly replaces the positive topic while
            # retaining X as the new exclusion. The generic exclusion intent
            # would reject Y and manufacture a positive/negative conflict.
            intent_override = "contextual_requery"

    mutation_trace: dict = {}
    intent, merged = detect_and_merge(
        text=merge_text,
        current_params=generic_current,
        previous_frame=previous_frame,
        mutation_trace=mutation_trace,
        intent_override=intent_override,
    )

    if object_mutation is not None:
        mutation_trace["negation_lifecycle"] = object_mutation.trace(
            list(context_params.get("objeto_exclude") or [])
        )

    if object_negation_overlap(merged):
        # The contradictory delta never becomes conversational state.  Preserve
        # the previous frame and clarify before planning or SECOP execution.
        previous_params = dict(context_params)
        blocked_plan = asdict(QueryPlan(
            should_execute=False,
            params=dict(previous_params),
            risk_flags=["OBJECT_NEGATION_CONTRADICTION"],
            clarification_question=OBJECT_NEGATION_CONTRADICTION_QUESTION,
            reason="object_negation_contradiction",
        ))
        return _with_timing(
            state,
            "apply_context_ms",
            start,
            parsed_params=previous_params,
            needs_llm=False,
            needs_clarification=True,
            clarification_reason=OBJECT_NEGATION_CONTRADICTION_QUESTION,
            intent_type="object_negation_contradiction",
            followup_intent_type="object_negation_contradiction",
            followup_mutation={
                **mutation_trace,
                "rejected_fields": ["objeto_exclude"],
                "rejection_reasons": {
                    "objeto_exclude": "OBJECT_NEGATION_CONTRADICTION",
                },
            },
            query_plan=blocked_plan,
        )

    # REPLACE implícito: promover solo ahora que el intent genérico real es conocido.
    if modality_wiring.is_eligible_for_implicit_replace(instruction, interpretation, intent):
        instruction = modality_wiring.classify_control(
            user_query, interpretation, allow_implicit_replace=True
        )

    if instruction.status == "none":
        # Sin operación de modalidad → paridad M3B-2: el merge genérico ya manejó
        # la modalidad (heredar / limpiar por cambio de topic) como siempre.
        return _finish_apply_context(state, start, merged, intent, mutation_trace)

    # Operación de modalidad: el motor puro reemplaza lo que el merge genérico
    # hizo con la modalidad (autoridad única) y el trace la marca como delegada.
    previous_filter = modality_wiring.filter_from_params(context_params)
    decision = modality_wiring.apply_engine(previous_filter, interpretation, instruction)
    modality_wiring.mark_modality_delegated(mutation_trace)
    mutation_trace["modality"] = decision.trace

    if decision.needs_clarification:
        # Target bloqueado o control ambiguo/en conflicto: filtro previo intacto,
        # sin deltas ortogonales ni ejecución. Se pregunta en el turno actual.
        # Se emite el mismo query_plan bloqueado que produciría plan_query para que
        # el clasificador de outcome lo lea como aclaración (no como 0-tras-refinamiento).
        previous_params = params_from_frame(previous_frame)
        blocked_plan = asdict(QueryPlan(
            should_execute=False,
            params=dict(previous_params),
            reason=f"modality_{decision.clarification_code}",
        ))
        return _with_timing(
            state, "apply_context_ms", start,
            parsed_params=previous_params,
            needs_llm=False,
            needs_clarification=True,
            clarification_reason=modality_wiring.message_for(decision.clarification_code),
            intent_type=intent,
            followup_intent_type=intent,
            followup_mutation=mutation_trace,
            query_plan=blocked_plan,
        )

    merged = modality_wiring.write_final_filter(merged, decision.final_filter)
    return _finish_apply_context(state, start, merged, intent, mutation_trace)


def _finish_apply_context(state, start, merged, intent, mutation_trace) -> State:
    """Aplica el guard WHERE 1=1 y decide el ruteo tras el merge (con o sin modalidad)."""
    # Guard contra WHERE 1=1 global.
    # Excepción: una opportunity_search sin scope/topic NO es "contexto
    # perdido" — es una oportunidad demasiado amplia que aún necesita rubro.
    # Dejamos que P15 (plan_query) la gobierne, que da una aclaración mucho
    # mejor ("¿qué producto/servicio buscas?") que el genérico de abajo.
    is_opportunity = merged.get("intent_type") == "opportunity_search"
    pending_yields = intent == "new_search" and bool(
        (state.get("context_params") or {}).get("actor_pending")
    )
    resolution_pending = can_resolve_new_actor(merged, intent)
    # A fresh, possibly broad search is not lost follow-up context. Let the
    # normal planner enforce scope while committing the replacement frame.
    if not is_opportunity and not pending_yields and not resolution_pending and not FollowupGuards.check_no_where_1_1(merged):
        return _with_timing(
            state,
            "apply_context_ms",
            start,
            parsed_params=merged,
            needs_llm=False,
            needs_clarification=True,
            clarification_reason=(
                "Listo, borre los filtros anteriores. Que quieres buscar ahora?"
                if intent == "reset_context"
                else
                "Te entendi como refinamiento, pero perdi el contexto anterior. "
                "Haz una busqueda base primero o usa reset."
            ),
            intent_type=intent,
            followup_intent_type=intent,
            followup_mutation=mutation_trace,
        )

    # Si el merge produjo contexto suficiente, no necesita LLM
    has_context = FollowupGuards.check_no_where_1_1(merged)
    if has_context or resolution_pending:
        return _with_timing(state, "apply_context_ms", start, parsed_params=merged, needs_llm=False, intent_type=intent, followup_intent_type=intent, followup_mutation=mutation_trace)

    return _with_timing(state, "apply_context_ms", start, parsed_params=merged, intent_type=intent, followup_intent_type=intent, followup_mutation=mutation_trace)
