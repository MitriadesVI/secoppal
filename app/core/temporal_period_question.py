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

The marker (``MARKER``) is an EPHEMERAL CONTROL SEAM, not a semantic dimension: it travels in the
``resolved_params`` of the blocked turn because the repo has no pending-clarification slot apart from the
frame itself (``actor_pending`` travels the same way), and a new persistence slot is not a minimal hook.
Seam to retire with the Conversation IR (M2). Invariants (``test_m1_wave1_r4_marker_invariants``): it never
reaches the SoQL, never changes a filter, never survives an answer, a reset or a complete new search.

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


class Block(NamedTuple):
    """The question to show (``shown``) and the one to keep open (``question``)."""

    code: str
    question: str

    @property
    def shown(self) -> str:
        return self.question + _WAY_OUT


def reopen(params: dict[str, Any], code: str, question: str) -> None:
    """Mark ``params`` (a blocked frame) as waiting for a period; other codes are not touched."""
    if code in OPEN_CODES:
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


def _is_answer(text: str, parsed: dict[str, Any], intent: str, previous: dict[str, Any]) -> bool:
    """An explicit period (never a window) that brings no stray topic.

    The way out written in the question promises that a whole search is accepted, so a turn with a
    topic is an answer when the topic is the one the frame already had («contratos de aseo de la
    Alcaldía de Galapa de octubre a diciembre de 2025» after the same query with a window) or when
    it also names the buyer (entity + topic + period: a new search). A topic that is neither
    («2025 completo», «los de aseo de octubre a diciembre de 2025»: a word of a reply cannot be told
    from a topic) asks again.
    """
    if parsed.get("_temporal_error") or not _has_period(parsed) or relative_window_in(text):
        return False
    if intent == "new_search":
        return True
    topic = _topic(parsed)
    if not topic or topic <= _topic(previous):
        return True
    return bool(parsed.get("entidad_resolved") or parsed.get("entidad"))


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
        return pending
    if intent in _NO_FOLLOWUP or not _has_period(previous):
        return None
    if (window := relative_window_in(text)) is None:
        return None
    return Block(FOLLOWUP_CODE, _followup_question(window, previous))
