"""Bounded grammars for conversational control turns.

These patterns recognize command structures. They are intentionally not a
general stopword list and do not decide which fields may mutate; that authority
belongs to :mod:`app.core.followup_mutations`.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from app.core.control_language import is_pure_control_turn
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


# ── Paginación ───────────────────────────────────────────────────────────
#
# La gramática se compone de sus partes en vez de enumerar frases completas.
# La versión anterior aceptaba "muéstrame los siguientes" y rechazaba
# "muéstrame la siguiente página": la misma orden con otro sustantivo, que se
# iba al extractor temático y consultaba `LIKE '%pagina%'` sobre el universo
# heredado. Un hueco en una lista de frases, no en una regla.
_PAGE_LEAD = (
    r"(?:muestrame|muestreme|muestra|dame|damelos|damelas|traeme|trae|ver"
    r"|pasa|pasame|continua|continuar|sigue|seguir)"
)
_PAGE_NEXT = r"(?:siguientes|siguiente|mas|proxim[oa]s?)"
_PAGE_NOUN = r"(?:resultados?|paginas?|contratos?|procesos?|registros?)"
_PAGINATION_CONTROL_RE = re.compile(
    rf"^(?:{_PAGE_LEAD}\s+)?"
    rf"(?:(?:a\s+la|con|hacia)\s+)?"
    rf"(?:(?:los|las|el|la)\s+)?"
    rf"{_PAGE_NEXT}"
    rf"(?:\s+{_PAGE_NOUN})?$"
    rf"|^(?:continuar|continua|sigue|seguir)$"
)

# ── Reinicio de contexto ─────────────────────────────────────────────────
#
# No existía. "limpia todos los filtros y empecemos de cero" conservaba Caldas
# y 2024 y ejecutaba con las palabras de la orden como tema. Igual que arriba,
# la gramática se compone: verbo de borrado + alcance, o una fórmula de
# empezar otra vez, opcionalmente coordinadas.
_RESET_VERB = (
    r"(?:limpia|limpiar|limpiame|borra|borrar|borrame|elimina|eliminar"
    r"|quita|quitar|quitalo|quitalos|quitame|reinicia|reiniciar|resetea|resetear)"
)
_RESET_ALL = r"(?:todos|todas|todo|toda)"
_RESET_NOUN = r"(?:filtros?|criterios?|busqueda|consulta|contexto)"
_RESET_START_OVER = (
    r"(?:empecemos|empezemos|empezamos|comencemos|comenzamos|empezar|comenzar"
    r"|arranquemos|arrancar)"
    r"\s+(?:de\s+cero|desde\s+cero|de\s+nuevo|otra\s+vez)"
)
_RESET_CORE = (
    rf"(?:{_RESET_VERB}(?:\s+{_RESET_ALL})?(?:\s+(?:los|las|el|la))?\s+{_RESET_NOUN})"
    rf"|(?:{_RESET_VERB}\s+{_RESET_ALL})"
    rf"|(?:{_RESET_START_OVER})"
    rf"|(?:(?:nueva|otra)\s+busqueda)"
)
_RESET_CONTROL_RE = re.compile(
    rf"^(?:ahora\s+)?(?:{_RESET_CORE})(?:\s+(?:y|e)\s+(?:{_RESET_CORE}))?$"
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


def is_pure_navigation_turn(text: str) -> bool:
    """True cuando el turno ENTERO es una orden de navegar o reiniciar.

    Lo consume el parser para no fabricar un tema con las palabras de la orden.
    Qué hacer con la orden sigue siendo autoridad de la capa conversacional:
    esto solo garantiza que "muéstrame la siguiente página" no viaje como
    ``LIKE '%pagina%'`` mientras tanto.
    """
    norm = _plain(text)
    return bool(
        _PAGINATION_CONTROL_RE.fullmatch(norm) or _RESET_CONTROL_RE.fullmatch(norm)
    )


def requests_context_reset(text: str) -> bool:
    """True cuando el turno pide empezar de cero, no refinar."""
    return bool(_RESET_CONTROL_RE.fullmatch(_plain(text)))


def classify_control_intent(text: str, current_params: dict[str, Any]) -> str | None:
    norm = _plain(text)
    if _PAGINATION_CONTROL_RE.fullmatch(norm):
        return "pagination_more"
    # Antes que cualquier borrado de campo concreto: "borra los filtros" no
    # nombra un campo, los nombra todos.
    if _RESET_CONTROL_RE.fullmatch(norm):
        return "reset_context"
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
    if current_params.get("ordering_signal") and is_pure_control_turn(norm):
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
        or _RESET_CONTROL_RE.fullmatch(norm)
        or _REMOVE_RANGE_RE.fullmatch(norm)
        or _REMOVE_DATE_RE.fullmatch(norm)
        or _REMOVE_STATE_RE.fullmatch(norm)
        or _DATASET_CONTROL_RE.fullmatch(norm)
        or _KEEP_CONTEXT_RE.fullmatch(norm)
        or (parsed_delta.get("ordering_signal") and is_pure_control_turn(norm))
        or (any(parsed_delta.get(key) for key in STATE_FIELDS) and _STATE_CONTROL_RE.fullmatch(norm))
    )
