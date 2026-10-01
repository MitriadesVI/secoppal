"""«hoy» as the currentness of a reported state, not as a signature day (one responsibility).

«¿Cuántos contratos figuran hoy suspendidos?» and «desglosa por estado … a hoy»
ask for the state each contract reports TODAY; the signature period is whatever
else the text says (or none). The temporal authority resolves the same word as
the day «hoy», so a reader that compares its period with the authority's must
set that one word aside, and only that word, and only where it plainly is the
currentness of the state:

* next to a verb of state («figuran hoy», «están hoy», «hoy figuran», «se
  encuentran hoy»…), in a text that names a reported state or breaks down by it;
* «a hoy» in a breakdown by state, when it is the only period the text names.

* after a verb of state, however many words apart («están suspendidos hoy», «están en
  ejecución hoy»), and «al día de hoy».

Everything else stays with the authority: «antes de hoy», «hasta hoy» and
«desde 2024 hasta hoy» are bounds on the signature date; «firmados hoy», «celebrados
hoy», «que firmó hoy» and «de hoy» are the signature day — the verb of state that comes
AFTER them does not change that («firmados hoy están en ejecución»); outside a
reported-state context «hoy» is the signature day.

The rule is by position: what governs «hoy» is the last of {event word, verb of state}
before it, so «firmados ayer están hoy en ejecución» is currentness and «firmados el día
de hoy están en ejecución» is a signature day. Event words are a closed list of stems
(``_EVENT``): the verbs that date a contract.

Pure: no I/O; the caller injects the clock.
"""
from __future__ import annotations

import re
from datetime import date as Date

from app.core.estado_families import detect_estado_family
from app.core.query_text_consumption import mask_consumed_spans
from app.core.temporal_semantics import civil_today, resolve_temporal_scope

_HOY = re.compile(r"hoy", re.IGNORECASE)
_STATE_VERB = (
    r"(?:figura|figuran|esta|estan|se\s+encuentra|se\s+encuentran|aparece|aparecen|"
    r"sigue|siguen)"
)
_STATE_VERB_RE = re.compile(rf"(?<![a-z0-9]){_STATE_VERB}(?![a-z0-9])")
_VERB_AFTER = re.compile(rf"\s+{_STATE_VERB}(?![a-z0-9])")
# The verbs (and their nouns) that date a contract: firmar, celebrar, suscribir, publicar,
# adjudicar, perfeccionar, registrar, otorgar, aprobar, expedir, legalizar — any inflection.
_EVENT = re.compile(
    r"(?<![a-z0-9])(?:firm|celebr|suscri[bt]|public|adjudic|perfeccion|registr|otorg|aprob|"
    r"exped|expid|legaliz)[a-z]*"
)
_AS_OF_TODAY = re.compile(r"(?<![a-z0-9])al\s+dia\s+de\s*$")
_OF_TODAY = re.compile(r"(?<![a-z0-9])(?:de|del)\s*$")
_CLAUSE_START = re.compile(r"[.;:?!¿¡]")
_A_HOY = re.compile(r"(?<![a-z0-9])a\s+hoy(?![a-z0-9])")


def _last_end(pattern: re.Pattern[str], text: str) -> int:
    ends = [hit.end() for hit in pattern.finditer(text)]
    return max(ends) if ends else -1


def _is_currentness_position(text: str, span: tuple[int, int]) -> bool:
    """By position: the last of {event word, verb of state} before «hoy» governs it."""
    clause = list(_CLAUSE_START.finditer(text, 0, span[0]))
    before = text[clause[-1].end():span[0]] if clause else text[:span[0]]
    event, state = _last_end(_EVENT, before), _last_end(_STATE_VERB_RE, before)
    if event > state:
        return False  # «firmados hoy», «que firmó hoy», «firmados el día de hoy»
    if _AS_OF_TODAY.search(before):
        return True   # «al día de hoy»
    if _OF_TODAY.search(before):
        return False  # «los contratos de hoy»: the contracts OF that day
    if state >= 0:
        return True   # «figuran hoy», «están suspendidos hoy»
    return bool(_VERB_AFTER.match(text, span[1]))  # «hoy están» (but never «de hoy»)


def governs_reported_state(content: str, *, grouped_by_state: bool) -> bool:
    """A reported state is asked for: a breakdown by state, or a state family named."""
    if grouped_by_state:
        return True
    tokenized = " ".join(re.findall(r"\b\w+\b", content))
    return detect_estado_family(tokenized, "contratos") is not None


def currentness_hoy(
    text: str, today: Date | None, *, grouped_by_state: bool,
) -> tuple[tuple[int, int], tuple[str, str]] | None:
    """The span of a bare «hoy» that is currentness and the (day, day) the authority makes of it.

    ``None`` when the authority's whole resolution is not that bare word (with a
    bound, «antes de hoy», the resolution phrase is longer and nothing is set
    aside) or when the word is not in a currentness position.
    """
    found = resolve_temporal_scope(text, today=today)
    if (
        found.error_code is not None
        or not found.matched
        or found.span is None
        or found.precision != "day"
        or not _HOY.fullmatch(found.phrase.strip())
    ):
        return None
    span = found.span
    day = civil_today(today).isoformat()
    if _is_currentness_position(text, span):
        return span, (day, day)
    if grouped_by_state and any(
        hit.start() <= span[0] and span[1] <= hit.end() for hit in _A_HOY.finditer(text)
    ):
        rest = resolve_temporal_scope(mask_consumed_spans(text, (span,)), today=today)
        if not rest.matched and not rest.error_code:
            return span, (day, day)
    return None
