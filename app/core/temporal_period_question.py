"""A period question stays open until a period answers it (one responsibility).

Two situations ask the person to write the period out instead of guessing it:

* a relative window next to another period in the same turn
  (``temporal_window_anchor``: «los dos últimos meses de 2025»);
* a relative window in a follow-up while the conversation already has a period
  («ahora solo el último mes» after «… en 2025»): counted back from today or inside the
  previous period? The follow-up engine would inherit the old period or replace it with
  the clock one, silently.

A blocked turn is persisted with the scope it understood (so a reply can complete it) and
without a date. Left alone, the reply («sí», «2», «la primera», «2025 completo») is read as
a new follow-up: a search without a period, or with a word of the reply as the topic. So the
blocked turn leaves a marker and the next turn is an ANSWER only when it names an explicit
(non-relative) period and brings no stray topic along (a topic is fine when the frame already has
it, or when the turn names the buyer too: a whole new search); any other reply asks again, with the
way out written in the question. An answer executes the period it wrote (``answered``).

An answer also keeps the frame: a reply that brings a topic, buyer, territory, value, status or other
condition DIFFERENT from the blocked frame's is not an answer to its question (executing the old frame
with the new period would run a search nobody asked for; merging the two, a hybrid). It asks again, naming
the way out («busca …», the verb the follow-up engine reads as a new search, or /reset), unless the
router already classed it as a complete ``new_search`` (then the new search runs, unmerged). Repeating
the frame's own topic, buyer and value with an explicit period still answers.

The marker (``MARKER``) is an EPHEMERAL CONTROL SEAM, not a semantic dimension: it travels in the
``resolved_params`` of the blocked turn because the repo has no pending-clarification slot apart from the
frame itself (``actor_pending`` travels the same way), and a new persistence slot is not a minimal hook.
Seam to retire with the Conversation IR (M2). Invariants (``test_m1_wave1_r4_marker_invariants``): it never
reaches the SoQL, never changes a filter, never survives an answer, a reset or a complete new search.
Every turn a temporal error blocked carries it (R5), but only the window questions (``OPEN_CODES``) keep a
question open: the marker of any other code only tells pagination the frame was not executed.

Pure: no I/O, no clock. The lexicon of windows is ``temporal_semantics``'s (both lexicons).
"""
from __future__ import annotations

from typing import Any, NamedTuple

from app.core import temporal_window_anchor
from app.core.temporal_semantics import relative_window_in

MARKER = "_temporal_open_question"
FOLLOWUP_CODE = "relative_window_with_previous_period"
OPEN_CODES = frozenset({temporal_window_anchor.ERROR_CODE, FOLLOWUP_CODE})
_NO_FOLLOWUP = frozenset({"new_search", "reset_context"})
_TOPIC_KEYS = ("objeto", "objeto_or", "objeto_phrases")
_WAY_OUT = " Si en realidad quieres otra búsqueda, escríbela completa (entidad, tema y periodo)."
_CHANGED_WAY_OUT = (
    " Tu mensaje cambia la búsqueda que dejé en espera (tema, entidad, valor u otra condición) y no voy a "
    "ejecutar la anterior ni a mezclar las dos. Si es otra búsqueda, escríbela completa empezando por "
    "«busca …» (por ejemplo «busca contratos de aseo de la Alcaldía de Soledad en 2024»); si quieres "
    "descartar la pendiente, escribe /reset; si sigues con la anterior, respóndeme sólo el periodo."
)
# Conditions of a frame that identify what is searched (the topic is compared apart, by subset).
_CONDITION_KEYS = (
    "entidad_resolved", "buyer_group", "departamento_resolved", "departamentos_in", "ciudad_resolved",
    "municipal_territory", "region", "valor_min", "valor_max", "estado", "estado_family",
    "estado_contrato", "estado_de_apertura_del_proceso", "estado_del_procedimiento",
    "estado_contrato_adicional", "contratista", "modalidad_include_procedure_ids",
    "modalidad_exclude_procedure_ids", "contract_type_include", "objeto_exclude",
)


class Block(NamedTuple):
    """The question to show (``shown``) and the one to keep open (``question``)."""

    code: str
    question: str
    changed_frame: bool = False

    @property
    def shown(self) -> str:
        return self.question + (_CHANGED_WAY_OUT if self.changed_frame else _WAY_OUT)


def reopen(params: dict[str, Any], code: str, question: str) -> None:
    """Mark ``params`` (a blocked frame) as a turn that a temporal error did not execute.

    Only ``OPEN_CODES`` keep a question open (``take`` yields a pending block for them): the marker of
    any other temporal error («invalid_date») only records that the frame was NOT executed, so that
    pagination refuses it (``pagination_blocked``); the next turn reads it as an ordinary follow-up.
    """
    params[MARKER] = {"code": code, "question": question}


def take(params: dict[str, Any]) -> tuple[dict[str, Any], Block | None]:
    """``params`` without the marker (it never merges into a frame) and the marker, if any."""
    marker = params.get(MARKER)
    pending = None
    if isinstance(marker, dict) and marker.get("code") in OPEN_CODES:
        pending = Block(str(marker["code"]), str(marker.get("question") or ""))
    return {key: value for key, value in params.items() if key != MARKER}, pending


def _has_period(params: dict[str, Any]) -> bool:
    return bool(params.get("fecha_desde") or params.get("fecha_hasta"))


def _terms(value: Any) -> set[str]:
    """Every string inside ``value`` (lists, tuples, sets, nested), lower-cased."""
    if isinstance(value, str):
        return {value.casefold().strip()} - {""}
    if isinstance(value, (list, tuple, set, frozenset)):
        return {term for item in value for term in _terms(item)}
    return set()


def _topic(params: dict[str, Any]) -> set[str]:
    return {term for key in _TOPIC_KEYS for term in _terms(params.get(key))}


def _condition(value: Any) -> Any:
    """A comparable form of a condition (strings and lists compare by their terms)."""
    if isinstance(value, (str, list, tuple, set, frozenset)):
        return _terms(value) or None
    return value


def _new_topic(parsed: dict[str, Any], previous: dict[str, Any]) -> bool:
    """The reply says a topic the frame does not have (a subset of the frame's topic is the frame's)."""
    topic = _topic(parsed)
    return bool(topic) and not topic <= _topic(previous)


def _names_buyer(parsed: dict[str, Any]) -> bool:
    return bool(parsed.get("entidad_resolved") or parsed.get("entidad"))


def _changed_conditions(parsed: dict[str, Any], previous: dict[str, Any]) -> bool:
    """A buyer, territory, value, status or dataset the reply names that the frame does not have as such.

    Only what the reply SAYS counts (an absent condition is the frame's, inherited); a condition the
    frame does not have is new, and one it has with another value is a change.
    """
    for key in _CONDITION_KEYS:
        mine = parsed.get(key)
        if mine in (None, "", [], {}, (), False):
            continue
        if _condition(mine) != _condition(previous.get(key)):
            return True
    if parsed.get("dataset_explicit") and parsed.get("dataset") != previous.get("dataset"):
        return True
    if "entidad_resolved" not in parsed and parsed.get("entidad"):
        return _terms(parsed["entidad"]) != _terms(previous.get("entidad"))
    return False


def changes_frame(parsed: dict[str, Any], previous: dict[str, Any]) -> bool:
    """True when ``parsed`` names a topic, buyer, territory, value or status the frame does not have."""
    return _new_topic(parsed, previous) or _changed_conditions(parsed, previous)


def _is_another_search(parsed: dict[str, Any], previous: dict[str, Any]) -> bool:
    """A reply that reads as ANOTHER search (not as a stray word): different conditions, or a new topic
    together with a buyer. Only this one is told about the way out of changing the frame."""
    return _changed_conditions(parsed, previous) or (_new_topic(parsed, previous) and _names_buyer(parsed))


def _is_answer(text: str, parsed: dict[str, Any], intent: str, previous: dict[str, Any]) -> bool:
    """An explicit period (never a window) that keeps the blocked frame.

    The way out written in the question promises that a whole search is accepted: a complete
    ``new_search`` runs as such. Otherwise the reply answers the question only when it brings nothing
    different from the frame (``changes_frame``): the same query with an explicit period
    («contratos de aseo de la Alcaldía de Galapa de octubre a diciembre de 2025» after the same query with
    a window) or the period alone. A different topic, buyer, value or condition asks again, and so does
    a topic the frame lacks («2025 completo», «los de aseo de octubre a diciembre de 2025»: a word of a
    reply cannot be told from a topic).
    """
    if parsed.get("_temporal_error") or not _has_period(parsed) or relative_window_in(text):
        return False
    return intent == "new_search" or not changes_frame(parsed, previous)


def answered(merged: dict[str, Any], parsed: dict[str, Any]) -> dict[str, Any]:
    """``merged`` carrying exactly the period the answer wrote.

    The follow-up merge of a turn that names a topic keeps the period of the previous frame even
    when the turn writes another explicit one (a pre-existing behaviour of ``contextual_requery``:
    «… de abril a septiembre de 2026» after «… en 2025» executed 2025). An answer to a period
    question exists to set the period, so it replaces the pair atomically, like any new temporal
    scope: an open end is information, never an invitation to inherit the old one.
    """
    result = dict(merged)
    for key in ("fecha_desde", "fecha_hasta"):
        if parsed.get(key):
            result[key] = parsed[key]
        else:
            result.pop(key, None)
    return result


def settle(
    merged: dict[str, Any], parsed: dict[str, Any], *, pending: Block | None, intent: str,
) -> dict[str, Any]:
    """``merged`` after a turn that passed ``block_for``: an answer sets the period it wrote.

    Without an open question, or when the person left it with an explicit reset, nothing changes.
    """
    if pending is None or intent == "reset_context":
        return merged
    return answered(merged, parsed)


def _followup_question(window: str, previous: dict[str, Any]) -> str:
    period = " a ".join(
        str(value) for value in (previous.get("fecha_desde"), previous.get("fecha_hasta")) if value
    )
    return (
        f"Mencionas «{' '.join(window.split())}», pero esta conversación ya tiene un periodo "
        f"({period}). No sé si la ventana se cuenta hacia atrás desde hoy o dentro de ese "
        f"periodo, y no voy a elegir por ti. {temporal_window_anchor.rewrite_hint()}"
    )


def block_for(
    *,
    text: str,
    intent: str,
    parsed: dict[str, Any],
    previous: dict[str, Any],
    pending: Block | None,
) -> Block | None:
    """The question this follow-up turn must ask, or ``None``.

    ``previous`` is the confirmed frame (without the marker) and ``pending`` the marker taken
    from it. With a question open only an answer passes; otherwise a window in a follow-up
    that inherits or replaces the previous period asks.
    """
    if pending is not None:
        # An explicit reset is the person leaving the question: nothing is executed by it.
        if intent == "reset_context" or _is_answer(text, parsed, intent, previous):
            return None
        if (
            intent != "new_search" and _has_period(parsed) and not parsed.get("_temporal_error")
            and not relative_window_in(text) and _is_another_search(parsed, previous)
        ):
            return pending._replace(changed_frame=True)
        return pending
    if intent in _NO_FOLLOWUP or not _has_period(previous):
        return None
    if (window := relative_window_in(text)) is None:
        return None
    return Block(FOLLOWUP_CODE, _followup_question(window, previous))
