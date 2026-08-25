"""Bounded grammars for conversational control turns.

These patterns recognize command structures. They are intentionally not a
general stopword list and do not decide which fields may mutate; that authority
belongs to :mod:`app.core.followup_mutations`.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from app.core.negation_followup import requests_object_exclusion_clear


STATE_FIELDS = frozenset({
    "estado", "estado_field", "estado_family", "estado_contrato",
    "estado_contrato_adicional", "estado_de_apertura_del_proceso",
    "estado_del_procedimiento",
})


def _plain(text: str) -> str:
    folded = unicodedata.normalize("NFKD", text.lower())
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", folded).strip(" ¿?¡!.,;:")


_PAGINATION_CONTROL_RE = re.compile(
    r"^(?:(?:muestra|muestrame|dame)\s+)?(?:los\s+)?siguientes(?:\s+resultados)?$|"
    r"^(?:muestrame|muestra|dame|ver)\s+mas$|^(?:continuar|continua|sigue)$"
)
_REMOVE_RANGE_RE = re.compile(
    r"^(?:ahora\s+)?(?:quita|quitar|elimina|eliminar|borra|borrar)\s+"
    r"(?:(?:ese|el|ese mismo)\s+)?(?:rango(?:\s+de\s+valor)?|valor(?:es)?)$"
)
_REMOVE_DATE_RE = re.compile(
    r"^(?:ahora\s+)?(?:quita|elimina|borra)\s+(?:(?:ese|el)\s+)?"
    r"(?:rango\s+de\s+fechas?|fechas?|ano)$"
)
_REMOVE_STATE_RE = re.compile(
    r"^(?:ahora\s+)?(?:quita|elimina|borra)\s+(?:(?:ese|el)\s+)?estado$"
)
_ORDER_CONTROL_RE = re.compile(
    r"^(?:(?:ahora\s+)?(?:ordena|ordenalos|ponlos|dejame)"
    r"(?:\s+(?:los|solo\s+los))?(?:\s+(?:contratos|procesos))?\s*)?"
    r"(?:del\s+)?(?:mas\s+caro(?:s)?(?:\s+al?\s+mas\s+barato(?:s)?)?|"
    r"por\s+(?:mayor\s+)?valor|de\s+mayor\s+a\s+menor|"
    r"(?:los\s+)?mas\s+recientes?)$"
)
_DATASET_CONTROL_RE = re.compile(
    r"^(?:ahora\s+)?(?:vuelve|regresa|cambia|pasa)(?:\s+de\s+nuevo)?\s+a\s+(?:los\s+)?"
    r"(?:contratos|procesos)$"
)
_STATE_CONTROL_RE = re.compile(
    r"^(?:dejame\s+)?solo\s+(?:las\s+que\s+)?(?:todavia\s+)?(?:reciben|reciba|aceptan)\s+ofertas$"
)
_KEEP_CONTEXT_RE = re.compile(
    r"^(?:entonces\s+)?(?:dejalo|dejame(?:lo)?)\s+asi$"
)


def requested_clear_fields(text: str) -> frozenset[str]:
    norm = _plain(text)
    if _REMOVE_RANGE_RE.fullmatch(norm):
        return frozenset({"valor_min", "valor_max"})
    if _REMOVE_DATE_RE.fullmatch(norm):
        return frozenset({"fecha_desde", "fecha_hasta"})
    if _REMOVE_STATE_RE.fullmatch(norm):
        return STATE_FIELDS
    if requests_object_exclusion_clear(text):
        return frozenset({"objeto_exclude"})
    return frozenset()


def classify_control_intent(text: str, current_params: dict[str, Any]) -> str | None:
    norm = _plain(text)
    if _PAGINATION_CONTROL_RE.fullmatch(norm):
        return "pagination_more"
    # OBJECT exclusion removal is context-sensitive: a bare "quita X" is a
    # negative mutation only when X matches a persisted exclusion.  The
    # lifecycle classifier authorizes that operation and supplies an explicit
    # intent override; do not reinterpret it here without that context.
    clear_fields = requested_clear_fields(text)
    if clear_fields - {"objeto_exclude"}:
        return "remove_filter"
    if _DATASET_CONTROL_RE.fullmatch(norm):
        return "change_dataset"
    if _KEEP_CONTEXT_RE.fullmatch(norm):
        return "refine_filter"
    if current_params.get("objeto_exclude"):
        return "exclude_topic"
    if current_params.get("ordering_signal") and _ORDER_CONTROL_RE.fullmatch(norm):
        return "change_order"
    if any(current_params.get(key) for key in STATE_FIELDS) and _STATE_CONTROL_RE.fullmatch(norm):
        return "refine_filter"
    return None


def is_contextual_topic_request(text: str) -> bool:
    return bool(re.match(r"^ahora\s+(?:busca|buscar|encuentra|consulta|lista)\s+", _plain(text)))


def is_control_only_turn(text: str, parsed_delta: dict[str, Any]) -> bool:
    """Second defense for topic-capable intents misclassified from control turns."""
    norm = _plain(text)
    return bool(
        _PAGINATION_CONTROL_RE.fullmatch(norm)
        or _REMOVE_RANGE_RE.fullmatch(norm)
        or _REMOVE_DATE_RE.fullmatch(norm)
        or _REMOVE_STATE_RE.fullmatch(norm)
        or _DATASET_CONTROL_RE.fullmatch(norm)
        or _KEEP_CONTEXT_RE.fullmatch(norm)
        or (parsed_delta.get("ordering_signal") and _ORDER_CONTROL_RE.fullmatch(norm))
        or (any(parsed_delta.get(key) for key in STATE_FIELDS) and _STATE_CONTROL_RE.fullmatch(norm))
    )
