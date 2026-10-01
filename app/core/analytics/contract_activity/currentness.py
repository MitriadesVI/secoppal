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

Everything else stays with the authority: «antes de hoy», «hasta hoy» and
«desde 2024 hasta hoy» are bounds on the signature date; «firmados hoy» is the
signature day (the verb of state elsewhere in the turn does not change that);
outside a reported-state context «hoy» is the signature day.

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
_NEXT_TO_A_STATE_VERB = re.compile(
    rf"(?<![a-z0-9])(?:{_STATE_VERB}\s+hoy|hoy\s+{_STATE_VERB})(?![a-z0-9])"
)
_A_HOY = re.compile(r"(?<![a-z0-9])a\s+hoy(?![a-z0-9])")


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
    if any(hit.start() <= span[0] and span[1] <= hit.end() for hit in _NEXT_TO_A_STATE_VERB.finditer(text)):
        return span, (day, day)
    if grouped_by_state and any(
        hit.start() <= span[0] and span[1] <= hit.end() for hit in _A_HOY.finditer(text)
    ):
        rest = resolve_temporal_scope(mask_consumed_spans(text, (span,)), today=today)
        if not rest.matched and not rest.error_code:
            return span, (day, day)
    return None
