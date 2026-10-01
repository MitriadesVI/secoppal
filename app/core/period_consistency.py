"""Does a consumer apply the period the temporal authority resolved? (one responsibility)

``temporal_semantics`` is the only reader of Spanish temporal expressions. A
consumer that executes a period of its own (a year read with its own regex, the
``fecha_desde``/``fecha_hasta`` of a parse it did not make) is correct only when
that period IS the authority's. This module compares the two and names the
disagreement; it never repairs it: the consumer fails closed (asks) instead of
reinterpreting the text or dropping the date.

It exists because D-TIME made the authority resolve forms that used to go
unrecognised (open bounds «antes/después de», «a partir de», «a la fecha»,
relative windows «últimos 3 años», an inferred year). A consumer that read only
a year, or that was saved by the leftover words of an unrecognised phrase,
started executing «antes de 2024» as 2024 and «últimos 3 años» with no date.

Pure: no I/O, no clock of its own (the caller injects ``today``).
"""
from __future__ import annotations

from datetime import date as Date

from app.core.query_text_consumption import mask_consumed_spans
from app.core.temporal_semantics import resolve_temporal_scope

Period = tuple[str | None, str | None]

TEMPORAL_UNRESOLVED = "TEMPORAL_UNRESOLVED"
PERIOD_NOT_REPRESENTED = "PERIOD_NOT_REPRESENTED"
PERIOD_AMBIGUOUS = "PERIOD_AMBIGUOUS"


def _clean(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def period_of(params: dict) -> Period:
    """The (desde, hasta) a parse carries; an absent bound is ``None``."""
    return _clean(params.get("fecha_desde")), _clean(params.get("fecha_hasta"))


def period_conflict(
    text: str,
    represented: Period,
    *,
    today: Date | None = None,
    protected_spans: tuple[tuple[int, int], ...] = (),
) -> str | None:
    """A reason code when ``represented`` is not what the authority resolved in ``text``.

    * the authority found an error («29 de febrero de 2023») → ``TEMPORAL_UNRESOLVED``;
    * it found a period and ``represented`` differs in either bound (an open bound
      is ``None`` on both sides) → ``PERIOD_NOT_REPRESENTED``;
    * a second period survives masking the first → ``PERIOD_AMBIGUOUS``.

    ``None`` when ``text`` names no period (the consumer's own period, if any,
    is then not this module's to judge) or when both agree exactly.
    """
    found = resolve_temporal_scope(text, today=today, protected_spans=protected_spans)
    if found.should_clarify:
        return TEMPORAL_UNRESOLVED
    if not found.matched:
        return None
    if represented != (_clean(found.fecha_desde), _clean(found.fecha_hasta)):
        return PERIOD_NOT_REPRESENTED
    if found.span is not None:
        rest = resolve_temporal_scope(
            mask_consumed_spans(text, (found.span,)), today=today,
            protected_spans=protected_spans,
        )
        if rest.should_clarify or rest.matched:
            return PERIOD_AMBIGUOUS
    return None
