"""query_frame.py — Política global de conversación para SECOPPAL v1.2.

Reemplaza el merge_params() ad-hoc con un sistema basado en QueryFrame:
dataset, scope, topic, modifiers, intent_type.

Las decisiones de merge se toman por tipo de intención, no por keyword
individual. Esto elimina los parches por frase.
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# QueryFrame — representación estructural de una consulta
# ---------------------------------------------------------------------------

@dataclass
class QueryFrame:
    dataset: str | None = None
    dataset_explicit: bool = False

    # Scope: alcance territorial / entidad
    scope: dict = field(default_factory=dict)

    # Topic: objeto contractual
    topic: list[str] = field(default_factory=list)

    # Modifiers: fecha, estado, valor, orden, contratista, modalidad
    modifiers: dict = field(default_factory=dict)

    # Raw params (para reconstrucción sin pérdida)
    raw_params: dict = field(default_factory=dict)

    # Intención conversacional
    intent_type: str = "new_search"


# ---------------------------------------------------------------------------
# frame_from_params — convierte params dict a QueryFrame
# ---------------------------------------------------------------------------

_SCOPE_KEYS = frozenset({
    "entidad", "entidad_resolved", "entidad_resolution",
    "departamento", "departamento_resolved", "departamento_resolution",
    "ciudad",
})

_MODIFIER_KEYS = frozenset({
    "fecha_desde", "fecha_hasta",
    "valor_min", "valor_max",
    "estado", "estado_field", "estado_family",
    "estado_contrato", "estado_de_apertura_del_proceso", "estado_del_procedimiento",
    "ordering_signal", "modalidad", "contratista",
})


def frame_from_params(params: dict) -> QueryFrame:
    """Construye un QueryFrame a partir de un dict de params del parser."""
    p = params or {}
    scope = {k: v for k, v in p.items() if k in _SCOPE_KEYS and v not in (None, [], "", {}, False)}
    topic = list(p.get("objeto", []) or [])
    modifiers = {k: v for k, v in p.items()
                 if k in _MODIFIER_KEYS and v not in (None, [], "", {}, False)}
    # Excluir scope y modifier keys de raw_params para no duplicar
    raw = {k: v for k, v in p.items()
           if k not in _SCOPE_KEYS and k not in _MODIFIER_KEYS
           and v not in (None, [], "", {}, False)}
    raw["objeto"] = topic  # siempre incluir objeto en raw

    return QueryFrame(
        dataset=p.get("dataset"),
        dataset_explicit=bool(p.get("dataset_explicit")),
        scope=scope,
        topic=topic,
        modifiers=modifiers,
        raw_params=raw,
    )


def params_from_frame(frame: QueryFrame) -> dict:
    """Reconstruye un dict de params desde un QueryFrame."""
    params: dict = {}
    if frame.dataset:
        params["dataset"] = frame.dataset
    params.update(frame.scope)
    if frame.topic:
        params["objeto"] = list(frame.topic)
    params.update(frame.modifiers)
    # raw_params va por último para no sobreescribir campos estructurados
    for k, v in frame.raw_params.items():
        if k not in params:
            params[k] = v
    # El cursor de paginación solo es válido para la ruta directa pagination_more.
    # Cualquier refinamiento/follow-up semántico debe arrancar desde la primera página.
    if frame.intent_type != "pagination_more":
        params.pop("offset", None)
    return params


# ---------------------------------------------------------------------------
# classify_turn — determina intención conversacional (DEPRECATED)
# ---------------------------------------------------------------------------

def classify_turn(current: QueryFrame, previous: QueryFrame | None) -> str:
    """Clasifica el turno actual vs anterior. (DEPRECATED)

    Esta función está marcada como legacy y será removida en el futuro.
    Usa el sistema de intent_type en QueryFrame en su lugar.

    Retorna uno de: new_search, refine_delta, contextual_requery,
    switch_scope, switch_dataset, suggestion_selection, reset.
    """
    warnings.warn(
        "classify_turn() is deprecated and will be removed. "
        "Use FollowupClassifier/detect_and_merge() to classify follow-up intent, "
        "then carry the result in QueryFrame.intent_type.",
        DeprecationWarning,
        stacklevel=2
    )

    if previous is None:
        return "new_search"

    # Reset y selección se manejan antes en run_query

    # 1. Dataset explicitamente cambiado (más específico)
    if (current.dataset_explicit
            and current.dataset
            and previous.dataset
            and current.dataset != previous.dataset):
        return "switch_dataset"

    # 2. Scope cambiado (ciudad/entidad/departamento distinto)
    if current.scope and _scope_differs(current.scope, previous.scope):
        return "switch_scope"

    # 3. Tiene topic nuevo, posiblemente modifiers, sin scope propio
    if current.topic:
        return "contextual_requery"

    # 4. Solo modificadores sin topic ni scope → refine_delta
    if current.modifiers:
        return "refine_delta"

    # Caso base: es consulta nueva
    return "new_search"


def _scope_differs(current_scope: dict, previous_scope: dict) -> bool:
    """Retorna True si el scope actual tiene contenido distinto al anterior."""
    if not previous_scope and current_scope:
        return True  # nuevo scope donde no había
    for key in ("ciudad", "departamento_resolved", "entidad_resolved"):
        cv = current_scope.get(key)
        pv = previous_scope.get(key)
        if cv and pv and cv != pv:
            return True
    return False


def is_complete_new_search(frame: QueryFrame) -> bool:
    """Retorna True si el frame representa una búsqueda completa nueva.
    Tiene dataset_explicit=True, topic no vacío, y scope o fecha presente.
    """
    if not frame.dataset_explicit or not frame.topic:
        return False
    has_scope_or_date = bool(
        frame.scope
        or frame.modifiers.get("fecha_desde")
        or frame.modifiers.get("fecha_hasta")
    )
    return has_scope_or_date


# ---------------------------------------------------------------------------
# merge_frames — fusiona frames según intención
# ---------------------------------------------------------------------------

def merge_frames(previous: QueryFrame, current: QueryFrame) -> QueryFrame:
    """Fusiona dos frames según la intención del turno actual.

    El intent_type DEBE estar seteado previamente en `current.intent_type`.
    (classify_turn ya no se usa para esto).
    """
    intent = current.intent_type

    if intent == "refine_delta":
        return _merge_refine_delta(previous, current)
    elif intent == "contextual_requery":
        return _merge_contextual_requery(previous, current)
    elif intent == "switch_scope":
        return _merge_switch_scope(previous, current)
    elif intent == "switch_dataset":
        return _merge_switch_dataset(previous, current)
    else:
        # new_search o desconocido → usar current tal cual
        return current


def _merge_refine_delta(previous: QueryFrame, current: QueryFrame) -> QueryFrame:
    """Solo modificadores nuevos (ordering, fecha, estado, valor).
    Hereda dataset, scope y topic del previous.
    """
    return QueryFrame(
        dataset=previous.dataset,
        dataset_explicit=previous.dataset_explicit,
        scope=dict(previous.scope),
        topic=list(previous.topic),
        modifiers={**previous.modifiers, **current.modifiers},
        raw_params={**previous.raw_params, **current.raw_params},
        intent_type="refine_delta",
    )


def _merge_contextual_requery(previous: QueryFrame, current: QueryFrame) -> QueryFrame:
    """Nuevo topic, posiblemente nuevos modifiers. Hereda scope y dataset."""
    merged_topic = list(current.topic) if current.topic else list(previous.topic)
    return QueryFrame(
        dataset=previous.dataset,
        dataset_explicit=previous.dataset_explicit,
        scope=dict(previous.scope),
        topic=merged_topic,
        modifiers={**previous.modifiers, **current.modifiers},
        raw_params={**previous.raw_params, **current.raw_params},
        intent_type="contextual_requery",
    )


def _merge_switch_scope(previous: QueryFrame, current: QueryFrame) -> QueryFrame:
    """Nuevo scope (ciudad, entidad, departamento). Hereda dataset y topic."""
    return QueryFrame(
        dataset=previous.dataset,
        dataset_explicit=previous.dataset_explicit,
        scope=dict(current.scope),
        topic=list(current.topic) if current.topic else list(previous.topic),
        modifiers={**previous.modifiers, **current.modifiers},
        raw_params={**previous.raw_params, **current.raw_params},
        intent_type="switch_scope",
    )


def _merge_switch_dataset(previous: QueryFrame, current: QueryFrame) -> QueryFrame:
    """Nuevo dataset explícito. Hereda scope y topic del previous si current no trae."""
    return QueryFrame(
        dataset=current.dataset,
        dataset_explicit=True,
        scope=dict(current.scope) if current.scope else dict(previous.scope),
        topic=list(current.topic) if current.topic else list(previous.topic),
        modifiers={**previous.modifiers, **current.modifiers},
        raw_params={**previous.raw_params, **current.raw_params},
        intent_type="switch_dataset",
    )
