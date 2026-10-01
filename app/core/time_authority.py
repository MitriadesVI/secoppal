"""Autoridad pura del valor ejecutable de una condición TIME (M1-D-TIME).

El intérprete propone ``{TIME, operator, polarity, quote}`` citando el span; esta
función decide el valor. El valor sale **sólo** de ``quote``; la ventana (tokens
vecinos) únicamente puede retirar autoridad. El reloj entra siempre como ``today``
explícito (sin ``date.today()``) y nunca se recibe texto crudo.

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
* deícticos «ahora/hoy/actualmente/en este momento» no son TIME (C3): BLOCK;
* fechas numéricas que la autoridad no lee («15/03/2025», «2025-03») y palabras
  del léxico de calificadores que sobran dentro de la cita («finales de 2025»)
  se preguntan: nunca se ejecutan como el año.

Ventana (C0 §5.5): la función recibe la cita y hasta 4 tokens plegados a cada lado
(``left``/``right``, en orden de lectura), nunca texto crudo. El contexto es
MONÓTONO: sólo puede retirar autoridad (RESOLVED(v) con contexto vacío → RESOLVED(v)
idéntico, ``UNCITED_QUALIFIER:TIME`` o peor, jamás otro valor); con una cita que no
resuelve, el contexto no cambia nada.

Salida: ``TimeValue(start, end_exclusive, precision)`` con al menos un extremo; el
extremo superior es exclusivo y ``None`` = cota abierta. ``reason`` es una de las
razones cerradas de C0 (``OPERATOR_EVIDENCE_MISMATCH:TIME``, ``UNCITED_QUALIFIER:TIME``,
``NEGATION_UNSUPPORTED:TIME``, ``UNRESOLVED_HOST_DIMENSION:TIME``); ``detail`` conserva el
motivo fino (``TIME_*``) sólo para diagnóstico.
"""
from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal

from app.core.temporal_semantics import TemporalResolution, resolve_temporal_scope
from app.core.time_window import (
    WINDOW_TOKENS,
    numeric_date_in,
    unparsed_qualifier,
    window_qualifier,
)

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
TIME_UNCITED_QUALIFIER = "TIME_UNCITED_QUALIFIER"
TIME_UNPARSED_QUALIFIER = "TIME_UNPARSED_QUALIFIER"
TIME_NUMERIC_DATE_UNSUPPORTED = "TIME_NUMERIC_DATE_UNSUPPORTED"

# Razones cerradas de C0 §5.5.1; todo lo demás es UNRESOLVED_HOST_DIMENSION:TIME.
_REASON_OF_DETAIL = {
    TIME_NEGATION_UNSUPPORTED: "NEGATION_UNSUPPORTED:TIME",
    TIME_OPERATOR_EVIDENCE_MISMATCH: "OPERATOR_EVIDENCE_MISMATCH:TIME",
    TIME_UNCITED_QUALIFIER: "UNCITED_QUALIFIER:TIME",
}
_DEFAULT_REASON = "UNRESOLVED_HOST_DIMENSION:TIME"

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
_ISO_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")
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
class TimeValue:
    """Período ejecutable: [start, end_exclusive); ``None`` = cota abierta (al menos una)."""

    start: date | None
    end_exclusive: date | None
    precision: str


@dataclass(frozen=True)
class TimeResolution:
    decision: Decision
    value: TimeValue | None = None  # sí y sólo sí RESOLVED
    reason: str | None = None  # razón cerrada de C0; sí y sólo sí no RESOLVED
    detail: str | None = None  # motivo fino TIME_*, sólo diagnóstico
    authority: str = AUTHORITY

    @property
    def fecha_desde(self) -> date | None:
        return self.value.start if self.value else None

    @property
    def fecha_hasta_exclusive(self) -> date | None:
        return self.value.end_exclusive if self.value else None

    @property
    def precision(self) -> str | None:
        return self.value.precision if self.value else None


def _refuse(decision: Decision, detail: str) -> TimeResolution:
    return TimeResolution(
        decision=decision, reason=_REASON_OF_DETAIL.get(detail, _DEFAULT_REASON), detail=detail,
    )


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


def _check_window(name: str, tokens: Sequence[str]) -> None:
    if isinstance(tokens, str) or len(tokens) > WINDOW_TOKENS or not all(
        isinstance(token, str) for token in tokens
    ):
        raise ValueError(f"{name} must be at most {WINDOW_TOKENS} folded string tokens")


def _resolve_quote(text: str, operator: str, clock: date) -> TimeResolution:
    """Lectura de la cita sola (sin ventana); es la única fuente de valor."""
    if _DEICTIC.fullmatch(text.strip(" \t.,;:¿?¡!")):
        return _refuse("BLOCK", TIME_DEICTIC_NOT_CONDITION)
    if numeric_date_in(text):
        return _refuse("CLARIFY", TIME_NUMERIC_DATE_UNSUPPORTED)
    found = resolve_temporal_scope(text, today=clock)
    if failure := _legacy_failure(found):
        return failure
    if _is_list_not_range(found):
        return _refuse("CLARIFY", TIME_RANGE_OR_LIST_AMBIGUOUS)

    desde, hasta, precision, allowed = (
        found.fecha_desde, found.fecha_hasta, found.precision, _allowed(found),
    )
    if precision == "open_from" and _ISO_DAY.fullmatch(found.phrase.strip()):
        hasta, precision = desde, "day"  # una fecha ISO sola es ese día, no «desde» ese día
        allowed = _EXPLICIT_DEFAULT
    remainder = _remainder(text, found)
    rest = resolve_temporal_scope(remainder, today=clock)
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
    elif unparsed_qualifier(remainder):
        return _refuse("CLARIFY", TIME_UNPARSED_QUALIFIER)
    if operator not in allowed:
        return _refuse("CLARIFY", TIME_OPERATOR_EVIDENCE_MISMATCH)
    return TimeResolution(
        decision="RESOLVED",
        value=TimeValue(
            start=date.fromisoformat(desde) if desde else None,
            end_exclusive=date.fromisoformat(hasta) + timedelta(days=1) if hasta else None,
            precision=precision,
        ),
    )


def resolve_time_condition(
    quote: str,
    left: Sequence[str],
    right: Sequence[str],
    operator: str,
    polarity: str,
    *,
    today: date,
) -> TimeResolution:
    """Valor ejecutable de una condición TIME, o la razón por la que no lo hay.

    ``left``/``right``: ≤ 4 tokens plegados alrededor de la cita, en orden de lectura
    (el más cercano es ``left[-1]`` / ``right[0]``). Pueden estar vacíos.
    """
    if not isinstance(today, date):
        raise TypeError("today must be an explicit datetime.date")
    _check_window("left", left)
    _check_window("right", right)
    clock = today.date() if isinstance(today, datetime) else today
    if polarity == "NEGATIVE":
        return _refuse("BLOCK", TIME_NEGATION_UNSUPPORTED)
    if polarity != "POSITIVE":
        return _refuse("BLOCK", TIME_POLARITY_INVALID)
    if operator not in OPERATORS:
        return _refuse("BLOCK", TIME_OPERATOR_UNKNOWN)
    resolution = _resolve_quote(quote or "", operator, clock)
    if resolution.decision == "RESOLVED" and window_qualifier(left, right):
        return _refuse("CLARIFY", TIME_UNCITED_QUALIFIER)  # el contexto sólo retira autoridad
    return resolution
