"""followup_engine.py — Subsistema de follow-up conversacional para SECOPPAL.

Reemplaza el merge_params() ad-hoc con un clasificador de intención explícito
y políticas de merge por tipo de follow-up.

Tipos de intención (followup_intent_type):
  - pagination_more        : "muéstrame más" (paginación pura, sin filtros nuevos)
  - refine_filter          : "muéstrame más contratos de adulto mayor" (hereda + filtro nuevo)
  - change_year            : "solo 2026", "ahora en 2025"
  - change_order           : "los más caros", "ordena por valor"
  - change_scope           : "ahora en Medellín"
  - change_dataset         : "ahora procesos abiertos"
  - contextual_requery     : follow-up con nuevo topic, hereda scope/dataset
  - suggestion_selection   : "1", "2", "3" (se maneja antes en run_query)
  - explain_result         : "explícame el primero" (reservado v1.3)
  - new_search             : consulta completamente nueva
  - unclear                : no se pudo determinar la intención
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.core.query_frame import QueryFrame, frame_from_params, params_from_frame
from app.core._followup_constants import (
    FOLLOWUP_WINDOW_MINUTES,
    CONTINUATION_WORDS,
    NEW_SEARCH_VERBS,
    NOISE_PREFIXES,
    SCOPE_TOPIC_KEYS,
)


# ── Constantes de follow-up ────────────────────────────────────────────────

# Re-exportadas desde _followup_constants.py (fuente única de verdad H3).
# Se mantienen como alias a nivel de módulo para backward compat.


# ── Tipos de intención ─────────────────────────────────────────────────────

FOLLOWUP_INTENTS = frozenset({
    "pagination_more", "refine_filter", "change_year",
    "change_order", "change_scope", "change_dataset",
    "contextual_requery", "suggestion_selection", "explain_result",
    "new_search", "unclear",
})


# ── Context Ledger — estado conversacional rico ────────────────────────────

@dataclass
class ConversationContext:
    """Estado completo de la conversación en un punto dado."""
    parsed_params: dict = field(default_factory=dict)
    dataset_id: str = ""
    soql_query: str = ""
    total_count: int = 0
    ordering_signal: str = ""
    offset: int = 0
    limit: int = 10
    result_count: int = 0
    query_frame: QueryFrame | None = None
    timestamp: str = ""


# ── Detector de contexto ───────────────────────────────────────────────────

class ContextDetector:
    """Determina si hay contexto conversacional válido."""

    @staticmethod
    def has_valid_history(last_turn: Any | None) -> bool:
        """Verifica si el último turno existe y está dentro de la ventana."""
        return last_turn is not None

    @staticmethod
    def is_recent(last_turn: Any | None) -> bool:
        """Verifica si el último turno está dentro de la ventana temporal."""
        if last_turn is None:
            return False
        try:
            age = ContextDetector._age_minutes(last_turn)
            return age <= FOLLOWUP_WINDOW_MINUTES
        except Exception:
            return False

    @staticmethod
    def has_results(last_turn: Any | None) -> bool:
        """Verifica si el último turno tenía resultados (para explain, compare)."""
        if last_turn is None:
            return False
        try:
            return bool(last_turn.result_ids)
        except Exception:
            return False

    @staticmethod
    def _age_minutes(turn) -> float:
        ts_str = turn.timestamp if hasattr(turn, "timestamp") else ""
        if not ts_str:
            return float("inf")
        try:
            ts = datetime.fromisoformat(ts_str)
            now = datetime.now(timezone.utc)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return (now - ts).total_seconds() / 60
        except Exception:
            return float("inf")


# ── Normalizador de texto ──────────────────────────────────────────────────

class TextNormalizer:
    """Normaliza texto de query para clasificación."""

    _ACCENT_MAP = str.maketrans(
        "áéíóúüñÁÉÍÓÚÜÑ",
        "aeiouunAEIOUUN",
    )

    _CORTESIAS_RE = re.compile(
        r"\b(?:por favor|gracias|please|pls|porfa|dale)\b",
        re.IGNORECASE,
    )

    _PUNCTUATION_RE = re.compile(r"[¿?¡!.,;:]+")

    @classmethod
    def normalize(cls, text: str) -> str:
        """Normaliza: minúsculas, sin tildes, sin cortesías, sin puntuación."""
        t = text.strip().lower()
        t = t.translate(cls._ACCENT_MAP)
        t = cls._PUNCTUATION_RE.sub(" ", t)
        t = cls._CORTESIAS_RE.sub(" ", t)
        t = re.sub(r"\s+", " ", t).strip()
        return t

    @classmethod
    def strip_noise(cls, text: str) -> str:
        """Elimina prefijos ruidosos."""
        t = text.strip().lower()
        # Eliminar tildes primero para matching
        t = t.translate(cls._ACCENT_MAP)
        changed = True
        while changed:
            changed = False
            for p in NOISE_PREFIXES:
                # Normalizar también el prefijo
                p_norm = p.translate(cls._ACCENT_MAP)
                if t.startswith(p_norm):
                    t = t[len(p_norm):].strip()
                    changed = True
                    break
        return t


# ── Clasificador de intención ──────────────────────────────────────────────

class FollowupClassifier:
    """Clasifica la intención del follow-up según el texto y el frame resultante."""

    # ── Patrones de paginación pura ────────────────────────────────────
    # Solo matchean si NO hay filtros nuevos en el texto.
    _PAGINATION_PATTERNS = re.compile(
        r"^(?:muestrame mas|muestra mas|dame mas|ver mas|"
        r"mas resultados|siguientes|los siguientes|"
        r"otros 10|otros diez|continuar|sigue|"
        r"siguiente pagina|quiero ver mas|"
        r"mas por favor"
        r")$",
        re.IGNORECASE,
    )

    # ── Señales de filtros nuevos (NO paginación) ──────────────────────
    _NEW_FILTER_SIGNALS = re.compile(
        r"\b("
        r"contratos?|procesos?|licitaciones?|convocatorias?|"
        r"adulto\s+mayor|primera\s+infancia|pae|alimentacion|"
        r"mantenimiento|pavimentacion|infraestructura|"
        r"en\s+ejecucion|firmados|suspendidos|terminados|"
        r"abiertos?|celebrados"
        r")\b",
        re.IGNORECASE,
    )

    # ── Señales de año ─────────────────────────────────────────────────
    _YEAR_PATTERN = re.compile(r"\b20\d{2}\b")

    # ── Señales de entidad/departamento ────────────────────────────────
    _SCOPE_SIGNALS = re.compile(
        r"\b(?:en\s+|de\s+|para\s+)?"
        r"(?:medellin|barranquilla|bogota|cali|"
        r"cartagena|santamarta|bucaramanga|manizales|"
        r"pereira|ibague|cucuta|villavicencio|"
        r"pasto|popayan|neiva|monteria|sincelejo|"
        r"valledupar|riohacha|leticia|quibdo|"
        r"atlantico|bolivar|antioquia|"
        r"choco|magdalena|guajira|cesar|"
        r"norte\s+de\s+santander|santander|"
        r"cundinamarca|boyaca|caldas|risaralda|"
        r"quindio|tolima|huila|cauca|nariño"
        r")\b",
        re.IGNORECASE,
    )

    @classmethod
    def is_pagination_pure(cls, text: str) -> bool:
        """Retorna True solo si es paginación pura (sin filtros nuevos)."""
        norm = TextNormalizer.normalize(text)
        # Si tiene señales de filtro nuevo, NO es paginación
        if cls._NEW_FILTER_SIGNALS.search(norm):
            return False
        # Si tiene año, NO es paginación
        if cls._YEAR_PATTERN.search(norm):
            return False
        # Si tiene señales de scope nuevo, NO es paginación
        if cls._SCOPE_SIGNALS.search(norm):
            return False
        return bool(cls._PAGINATION_PATTERNS.fullmatch(norm))

    @classmethod
    def classify_followup(
        cls,
        text: str,
        current_frame: QueryFrame,
        previous_frame: QueryFrame | None,
    ) -> str:
        """Clasifica la intención del follow-up.

        Orden de evaluación (importante):
        0. new_search_verb — verbo de busqueda nueva explicito
        1. pagination_more — paginación pura sin filtros nuevos
        2. change_dataset — dataset explicitamente cambiado
        3. change_year — solo año sin topic nuevo
        4. change_order — solo ordenamiento sin topic nuevo
        5. change_scope — entidad/ciudad/departamento distinto
        6. refine_filter — más + filtro nuevo, o topic sin scope propio
        7. contextual_requery — topic nuevo heredando scope
        8. new_search — búsqueda completa nueva
        9. unclear — no se pudo determinar
        """
        if previous_frame is None:
            return "new_search"

        # 0. Verbo de búsqueda nueva explícito (H8: evita contaminación de contexto)
        #    Si el usuario empieza con "busca", "necesito", "encuentra", etc.,
        #    quiere una búsqueda nueva, no un follow-up.
        noise_free = TextNormalizer.strip_noise(text)
        for verb in NEW_SEARCH_VERBS:
            if noise_free.startswith(verb):
                # Si hay contenido después del verbo, es búsqueda nueva
                rest = noise_free[len(verb):].strip()
                if len(rest.split()) >= 1:
                    return "new_search"

        # 1. Paginación pura
        if cls.is_pagination_pure(text):
            return "pagination_more"

        # 2. Dataset cambiado explícitamente
        if (current_frame.dataset_explicit
                and current_frame.dataset
                and previous_frame.dataset
                and current_frame.dataset != previous_frame.dataset):
            return "change_dataset"

        # 3. Solo cambio de año (sin topic nuevo, sin scope nuevo)
        norm_text = TextNormalizer.normalize(text)
        has_year = bool(cls._YEAR_PATTERN.search(norm_text))
        has_scope_signal = bool(cls._SCOPE_SIGNALS.search(norm_text))
        has_order = cls._has_ordering_signal(norm_text)

        if has_year and not current_frame.topic and not has_scope_signal and not has_order:
            return "change_year"

        # 4. Solo cambio de orden (sin topic nuevo, sin año, sin scope)
        if has_order and not current_frame.topic and not has_year and not has_scope_signal:
            return "change_order"

        # 5. Cambio de scope (entidad/ciudad diferente)
        if current_frame.scope:
            if cls._scope_differs(current_frame.scope, previous_frame.scope):
                return "change_scope"

        # 6. Refinamiento: "más" + filtro nuevo, o solo modificadores
        has_mas = bool(re.search(r"\bm[áa]s\b", norm_text))
        has_new_filters = bool(cls._NEW_FILTER_SIGNALS.search(norm_text))

        if has_mas and has_new_filters:
            return "refine_filter"

        # 7. Solo modificadores sin topic nuevo
        if current_frame.modifiers and not current_frame.topic:
            return "refine_filter"

        # 6. Tiene topic — contextual requery
        if current_frame.topic:
            return "contextual_requery"

        # 7. Solo modified params sin contexto — refine
        if current_frame.modifiers:
            return "refine_filter"

        # 8. Tiene parámetros significativos — new_search
        if cls._has_real_content(current_frame):
            return "new_search"

        return "unclear"

    @staticmethod
    def _has_ordering_signal(text: str) -> bool:
        """Detecta señales de ordenamiento en texto normalizado."""
        return bool(re.search(
            r"\b(?:"
            r"mayor\s+valor|mayor\s+cuantia|"
            r"mas\s+caro|mas\s+caros|mas\s+alta|mas\s+alto|"
            r"mas\s+reciente|mas\s+recientes|"
            r"mas\s+nuevo|mas\s+nuevos|"
            r"mas\s+grande|mas\s+grandes|"
            r"ordena|ordenalos|por\s+valor|por\s+fecha"
            r")\b",
            text,
            re.IGNORECASE,
        ))

    @staticmethod
    def _scope_differs(current_scope: dict, previous_scope: dict) -> bool:
        if not previous_scope and current_scope:
            return True
        if not current_scope and previous_scope:
            return True
        # Si las keys son distintas (ciudad vs departamento), es cambio
        current_keys = {k for k, v in current_scope.items() if v}
        previous_keys = {k for k, v in previous_scope.items() if v}
        if current_keys != previous_keys:
            return True
        # Mismos keys: verificar valores
        for key in ("ciudad", "departamento_resolved", "entidad_resolved"):
            cv = current_scope.get(key)
            pv = previous_scope.get(key)
            if cv and pv and cv != pv:
                return True
        return False

    @staticmethod
    def _has_real_content(frame: QueryFrame) -> bool:
        """El frame tiene contenido de búsqueda sustantivo."""
        return bool(frame.topic or frame.scope or frame.dataset)


# ── Merger — políticas de merge por intención ──────────────────────────────

class FollowupMerger:
    """Aplica políticas de merge según la intención clasificada."""

    @staticmethod
    def merge(
        previous_frame: QueryFrame,
        current_frame: QueryFrame,
        intent: str,
    ) -> QueryFrame:
        """Fusiona dos frames según la intención del follow-up."""
        policy = _MERGE_POLICIES.get(intent, _merge_new_search)
        return policy(previous_frame, current_frame)


def _merge_pagination_more(prev: QueryFrame, curr: QueryFrame) -> QueryFrame:
    """Hereda TODO. Solo incrementa offset."""
    offset = prev.raw_params.get("offset", 0)
    limit = prev.raw_params.get("limit", 10)
    return QueryFrame(
        dataset=prev.dataset,
        dataset_explicit=prev.dataset_explicit,
        scope=dict(prev.scope),
        topic=list(prev.topic),
        modifiers=dict(prev.modifiers),
        raw_params={
            **prev.raw_params,
            **curr.raw_params,
            "offset": int(offset) + int(limit),
            "limit": limit,
        },
        intent_type="pagination_more",
    )


def _merge_refine_filter(prev: QueryFrame, curr: QueryFrame) -> QueryFrame:
    """Hereda dataset/scope/topic. Aplica filtros nuevos. Offset = 0."""
    return QueryFrame(
        dataset=prev.dataset,
        dataset_explicit=prev.dataset_explicit,
        scope=dict(prev.scope),
        topic=list(curr.topic) if curr.topic else list(prev.topic),
        modifiers={**prev.modifiers, **curr.modifiers},
        raw_params={**prev.raw_params, **curr.raw_params},
        intent_type="refine_filter",
    )


def _merge_change_year(prev: QueryFrame, curr: QueryFrame) -> QueryFrame:
    """Hereda dataset/scope/topic/order. Reemplaza fecha. Offset = 0."""
    return QueryFrame(
        dataset=prev.dataset,
        dataset_explicit=prev.dataset_explicit,
        scope=dict(prev.scope),
        topic=list(prev.topic),
        modifiers={**prev.modifiers, **curr.modifiers},
        raw_params={**prev.raw_params, **curr.raw_params},
        intent_type="change_year",
    )


def _merge_change_order(prev: QueryFrame, curr: QueryFrame) -> QueryFrame:
    """Hereda dataset/scope/topic/fecha. Reemplaza ordering. Offset = 0."""
    modifiers = dict(prev.modifiers)
    # Si el current tiene ordering_signal, usarlo; si no, conservar anterior
    if curr.modifiers.get("ordering_signal"):
        modifiers["ordering_signal"] = curr.modifiers["ordering_signal"]
    return QueryFrame(
        dataset=prev.dataset,
        dataset_explicit=prev.dataset_explicit,
        scope=dict(prev.scope),
        topic=list(prev.topic),
        modifiers=modifiers,
        raw_params={**prev.raw_params, **curr.raw_params},
        intent_type="change_order",
    )


def _merge_change_scope(prev: QueryFrame, curr: QueryFrame) -> QueryFrame:
    """Nuevo scope. Hereda dataset y topic. Offset = 0."""
    return QueryFrame(
        dataset=prev.dataset,
        dataset_explicit=prev.dataset_explicit,
        scope=dict(curr.scope) if curr.scope else dict(prev.scope),
        topic=list(curr.topic) if curr.topic else list(prev.topic),
        modifiers={**prev.modifiers, **curr.modifiers},
        raw_params={**prev.raw_params, **curr.raw_params},
        intent_type="change_scope",
    )


def _merge_change_dataset(prev: QueryFrame, curr: QueryFrame) -> QueryFrame:
    """Nuevo dataset. Hereda scope y topic si aplican."""
    return QueryFrame(
        dataset=curr.dataset,
        dataset_explicit=True,
        scope=dict(curr.scope) if curr.scope else dict(prev.scope),
        topic=list(curr.topic) if curr.topic else list(prev.topic),
        modifiers={**prev.modifiers, **curr.modifiers},
        raw_params={**prev.raw_params, **curr.raw_params},
        intent_type="change_dataset",
    )


def _merge_contextual_requery(prev: QueryFrame, curr: QueryFrame) -> QueryFrame:
    """Nuevo topic. Hereda scope y dataset. Offset = 0."""
    merged_topic = list(curr.topic) if curr.topic else list(prev.topic)
    # OPP-003: preserve opportunity objeto when follow-up only adds geography
    if getattr(prev, "intent_type", None) == "opportunity_search" and not curr.topic:
        merged_topic = list(prev.topic)
    return QueryFrame(
        dataset=prev.dataset,
        dataset_explicit=prev.dataset_explicit,
        scope=dict(prev.scope),
        topic=merged_topic,
        modifiers={**prev.modifiers, **curr.modifiers},
        raw_params={**prev.raw_params, **curr.raw_params},
        intent_type="contextual_requery",
    )


def _merge_new_search(prev: QueryFrame, curr: QueryFrame) -> QueryFrame:
    """Búsqueda nueva. No hereda nada del anterior. Offset = 0."""
    # Asegurarse que offset NO se herede
    raw = dict(curr.raw_params)
    raw.pop("offset", None)
    return QueryFrame(
        dataset=curr.dataset,
        dataset_explicit=curr.dataset_explicit,
        scope=dict(curr.scope) if curr.scope else {},
        topic=list(curr.topic) if curr.topic else [],
        modifiers=dict(curr.modifiers),
        raw_params=raw,
        intent_type="new_search",
    )


# ── Mapa de políticas ─────────────────────────────────────────────────────

_MERGE_POLICIES: dict[str, callable] = {
    "pagination_more":      _merge_pagination_more,
    "refine_filter":       _merge_refine_filter,
    "change_year":          _merge_change_year,
    "change_order":         _merge_change_order,
    "change_scope":         _merge_change_scope,
    "change_dataset":       _merge_change_dataset,
    "contextual_requery":   _merge_contextual_requery,
    "new_search":           _merge_new_search,
}


# ── Guards ─────────────────────────────────────────────────────────────────

class FollowupGuards:
    """Invariantes del subsistema de follow-up."""

    @staticmethod
    def check_no_where_1_1(params: dict) -> bool:
        """Retorna False si no hay scope ni topic (WHERE 1=1 riesgo).
        Usa SCOPE_TOPIC_KEYS desde _followup_constants.py (H9: fuente única).
        """
        return any(
            params.get(k) and params.get(k) not in (None, [], "", {}, False)
            for k in SCOPE_TOPIC_KEYS
        )

    @staticmethod
    def check_no_offset_on_refinement(intent: str) -> bool:
        """True si el offset es válido para esta intención (solo pagination_more)."""
        return intent == "pagination_more"


# ── API pública ────────────────────────────────────────────────────────────

def detect_and_merge(
    text: str,
    current_params: dict,
    previous_frame: QueryFrame | None,
) -> tuple[str, dict]:
    """Detecta la intención y aplica merge, retornando (intent_type, merged_params).

    Este es el punto de entrada principal del subsistema.

    Args:
        text: texto normalizado de la query actual
        current_params: params parseados de la query actual
        previous_frame: QueryFrame del turno anterior (None si no hay historial)

    Returns:
        tuple[str, dict]: (intent_type, merged_params_dict)
    """
    current_frame = frame_from_params(current_params)
    intent = FollowupClassifier.classify_followup(
        text=text,
        current_frame=current_frame,
        previous_frame=previous_frame,
    )
    current_frame.intent_type = intent

    if previous_frame is None:
        return intent, params_from_frame(current_frame)

    merged_frame = FollowupMerger.merge(previous_frame, current_frame, intent)
    merged_params = params_from_frame(merged_frame)

    return intent, merged_params
