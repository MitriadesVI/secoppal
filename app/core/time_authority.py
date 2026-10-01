"""Autoridad pura del valor ejecutable de una condición TIME (M1-D-TIME).

El intérprete propone ``{TIME, operator, polarity, quote}`` citando el span; esta
función decide el valor. Lee **sólo** ``quote``: ningún texto fuera de la cita, y
el reloj entra siempre como ``today`` explícito (sin ``date.today()``).

Reglas, todas fail-closed (CLARIFY pregunta, BLOCK no se ejecuta como TIME):

* polaridad NEGATIVE → BLOCK ``TIME_NEGATION_UNSUPPORTED`` (el host no ejecuta
  exclusión temporal);
* el operador propuesto debe ser lo que dice la cita (como ``VALUE`` con
  ``foundation_value_direction``): ``during_year`` con una cita que nombra mes,
  cota o rango es ``TIME_OPERATOR_EVIDENCE_MISMATCH``, no el año de la cita;
* **no se infiere el año del reloj**: «marzo» sin año es CLARIFY
  ``TIME_YEAR_NOT_STATED`` (el reloj sólo resuelve expresiones explícitamente
  relativas: «este mes», «últimos 6 meses»);
* dos hechos temporales en la cita sólo se aceptan como cotas opuestas
  («después de 2020 y antes de 2024»); cualquier otro resto es CLARIFY;
* «A y B» sin «entre» es lista o rango: CLARIFY, nunca el rango silencioso;
* deícticos «ahora/hoy/actualmente/en este momento» no son TIME (C3): BLOCK.

La cota superior es exclusiva (``fecha_hasta_exclusive``); ``None`` = abierta.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal

from app.core.temporal_semantics import TemporalResolution, resolve_temporal_scope

AUTHORITY = "time-authority@1"
OPERATORS = ("during_year", "during_month", "between_dates", "relative_period")

Decision = Literal["RESOLVED", "CLARIFY", "BLOCK"]

TIME_NEGATION_UNSUPPORTED = "TIME_NEGATION_UNSUPPORTED"
TIME_POLARITY_INVALID = "TIME_POLARITY_INVALID"
TIME_OPERATOR_UNKNOWN = "TIME_OPERATOR_UNKNOWN"
TIME_OPERATOR_EVIDENCE_MISMATCH = "TIME_OPERATOR_EVIDENCE_MISMATCH"
TIME_DEICTIC_NOT_CONDITION = "TIME_DEICTIC_NOT_CONDITION"
TIME_NO_TEMPORAL_EVIDENCE = "TIME_NO_TEMPORAL_EVIDENCE"
TIME_YEAR_NOT_STATED = "TIME_YEAR_NOT_STATED"
TIME_MULTIPLE_TEMPORAL_FACTS = "TIME_MULTIPLE_TEMPORAL_FACTS"
TIME_RANGE_OR_LIST_AMBIGUOUS = "TIME_RANGE_OR_LIST_AMBIGUOUS"
TIME_REVERSE_RANGE = "TIME_REVERSE_RANGE"

# Códigos de la autoridad legada → razón pública.
_LEGACY_REASON = {
    "invalid_date": "TIME_INVALID_DATE",
    "reverse_range": TIME_REVERSE_RANGE,
    "missing_range_year": TIME_YEAR_NOT_STATED,
    "unsupported_temporal_expression": "TIME_UNSUPPORTED_EXPRESSION",
}

_DEICTIC = re.compile(
    r"(?:hoy|ahora|ahorita|actualmente|en\s+este\s+momento|en\s+la\s+actualidad)",
    re.IGNORECASE,
)
_RANGE_PRECISIONS = frozenset({"day_range", "month_range", "year_range"})
_OPEN_PRECISIONS = frozenset({"open_from", "open_until"})
# Operadores que una cita EXPLÍCITA admite, por precisión resuelta.
_EXPLICIT_OPERATORS = {"year": {"during_year"}, "month": {"during_month"}}
_EXPLICIT_DEFAULT = {"between_dates"}  # día, rangos, cotas, trimestre, semestre
# Una cita relativa es relative_period; además es del mismo tamaño que el operador.
_RELATIVE_OPERATORS = {
    "year": {"relative_period", "during_year"},
    "month": {"relative_period", "during_month"},
    "open_from": {"relative_period", "between_dates"},
    "open_until": {"relative_period", "between_dates"},
}
_RELATIVE_DEFAULT = {"relative_period"}


@dataclass(frozen=True)
class TimeResolution:
    decision: Decision
    fecha_desde: date | None = None
    fecha_hasta_exclusive: date | None = None
    precision: str | None = None
    reason: str | None = None
    authority: str = AUTHORITY


def _refuse(decision: Decision, reason: str) -> TimeResolution:
    return TimeResolution(decision=decision, reason=reason)


def _legacy_failure(found: TemporalResolution) -> TimeResolution | None:
    if found.error_code:
        return _refuse("CLARIFY", _LEGACY_REASON.get(found.error_code, "TIME_COUNT_OUT_OF_RANGE"))
    if not found.matched or not found.fecha_desde and not found.fecha_hasta:
        return _refuse("CLARIFY", TIME_NO_TEMPORAL_EVIDENCE)
    if found.clock == "inferred_year":
        return _refuse("CLARIFY", TIME_YEAR_NOT_STATED)
    return None


def _is_list_not_range(found: TemporalResolution) -> bool:
    phrase = found.phrase.lower()
    return (
        found.precision in _RANGE_PRECISIONS
        and re.search(r"\by\b", phrase) is not None
        and re.search(r"\bentre\b", phrase) is None
    )


def _bound_pair(first: TemporalResolution, second: TemporalResolution) -> tuple[str | None, str | None] | None:
    """Cotas opuestas (una desde, otra hasta) o None si no se complementan."""
    kinds = {first.precision, second.precision}
    if kinds != _OPEN_PRECISIONS:
        return None
    lower = first if first.precision == "open_from" else second
    upper = second if lower is first else first
    return lower.fecha_desde, upper.fecha_hasta


def _remainder(quote: str, found: TemporalResolution) -> str:
    start, end = found.span or (0, len(quote))
    return f"{quote[:start]} {quote[end:]}"


def _allowed(found: TemporalResolution) -> set[str]:
    if found.clock == "relative":
        return _RELATIVE_OPERATORS.get(found.precision, _RELATIVE_DEFAULT)
    return _EXPLICIT_OPERATORS.get(found.precision, _EXPLICIT_DEFAULT)


def resolve_time_condition(
    quote: str,
    operator: str,
    polarity: str,
    *,
    today: date,
) -> TimeResolution:
    """Valor ejecutable de una condición TIME, o la razón por la que no lo hay."""
    if not isinstance(today, date):
        raise TypeError("today must be an explicit datetime.date")
    clock = today.date() if isinstance(today, datetime) else today
    if polarity == "NEGATIVE":
        return _refuse("BLOCK", TIME_NEGATION_UNSUPPORTED)
    if polarity != "POSITIVE":
        return _refuse("BLOCK", TIME_POLARITY_INVALID)
    if operator not in OPERATORS:
        return _refuse("BLOCK", TIME_OPERATOR_UNKNOWN)
    text = quote or ""
    if _DEICTIC.fullmatch(text.strip(" \t.,;:¿?¡!")):
        return _refuse("BLOCK", TIME_DEICTIC_NOT_CONDITION)

    found = resolve_temporal_scope(text, today=clock)
    if failure := _legacy_failure(found):
        return failure
    if _is_list_not_range(found):
        return _refuse("CLARIFY", TIME_RANGE_OR_LIST_AMBIGUOUS)

    desde, hasta, precision, allowed = (
        found.fecha_desde, found.fecha_hasta, found.precision, _allowed(found),
    )
    rest = resolve_temporal_scope(_remainder(text, found), today=clock)
    if rest.matched:
        if rest.error_code or rest.clock == "inferred_year":
            return _refuse("CLARIFY", TIME_MULTIPLE_TEMPORAL_FACTS)
        pair = _bound_pair(found, rest)
        if pair is None:
            return _refuse("CLARIFY", TIME_MULTIPLE_TEMPORAL_FACTS)
        desde, hasta = pair
        precision, allowed = "bounded", {"between_dates"}
        if found.clock == "relative" or rest.clock == "relative":
            allowed = {"between_dates", "relative_period"}
        if desde and hasta and desde > hasta:
            return _refuse("CLARIFY", TIME_REVERSE_RANGE)
    if operator not in allowed:
        return _refuse("CLARIFY", TIME_OPERATOR_EVIDENCE_MISMATCH)
    return TimeResolution(
        decision="RESOLVED",
        fecha_desde=date.fromisoformat(desde) if desde else None,
        fecha_hasta_exclusive=(
            date.fromisoformat(hasta) + timedelta(days=1) if hasta else None
        ),
        precision=precision,
    )
