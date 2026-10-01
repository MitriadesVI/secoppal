"""Periodos relativos al reloj que la autoridad legada no resuelve (una responsabilidad).

La autoridad legada ya lee «este mes», «este año», «últimos N meses/años/semanas/días»
y «hace N meses/años». Quedaban sin resolver cuatro formas que el Gold marca EXECUTE:

* «este trimestre» / «este semestre»: la franja calendario en curso (como «este mes»);
* «el último año» / «el último mes»: ventana MÓVIL de 12 / 1 meses terminada hoy
  (como «últimos 12 meses»), no el año calendario pasado;
* «estos dos últimos meses», «los dos últimos años»: el número va ANTES de «últimos»;
* «hace dos semanas»: la ventana [hoy-14 días, hoy] (ventana, no la semana calendario;
  el host la anuncia como «últimas 2 semanas»).

Devuelve ``TemporalResolution`` con ``clock="relative"``, así que el resto de la
autoridad (resto de la cita, operador, ventana) las trata como a cualquier otra.
Puro: el reloj entra por parámetro y sólo se consulta cuando la legada no resolvió.
"""
from __future__ import annotations

import re
from datetime import date as Date, timedelta

from app.core.temporal_semantics import (
    _COUNT,
    _UNSUPPORTED_TIME_QUESTION,
    TemporalResolution,
    _count,
    _error_resolution,
    _last_day,
    _months_back,
    _resolution,
)

_FRANJA = re.compile(r"\b(?:de\s+|del\s+)?est[ea]\s+(trimestre|semestre)\b", re.IGNORECASE)
_ULTIMO_UNICO = re.compile(r"\b(?:el\s+|este\s+)?[uú]ltimo\s+(a[nñ]o|mes)\b", re.IGNORECASE)
_NUMERO_ANTES = re.compile(
    rf"\b(?:(?:estos|estas|los|las)\s+)?({_COUNT})\s+[uú]ltim[oa]s\s+"
    rf"(d[ií]as|semanas|meses|a[nñ]os)\b",
    re.IGNORECASE,
)
_HACE_SEMANAS = re.compile(rf"\bhace\s+({_COUNT})\s+semanas?\b", re.IGNORECASE)

# unidad -> (máximo, precisión, código de error); mismos topes que la autoridad legada.
_LIMITS = {
    "dias": (365, "trailing_days", "unsupported_day_count"),
    "semanas": (52, "trailing_weeks", "unsupported_week_count"),
    "meses": (120, "trailing_months", "unsupported_month_count"),
    "anos": (30, "trailing_years", "unsupported_year_count"),
}


def _trailing(match: re.Match[str], unit: str, count: int, clock: Date) -> TemporalResolution:
    limit, precision, code = _LIMITS[unit]
    if not 1 <= count <= limit:
        return _error_resolution(match, code, _UNSUPPORTED_TIME_QUESTION)
    if unit == "dias":
        start = clock - timedelta(days=count)
    elif unit == "semanas":
        start = clock - timedelta(days=7 * count)
    else:
        start = _months_back(clock, count * (12 if unit == "anos" else 1))
    return _resolution(match, desde=start, hasta=clock, precision=precision, clock="relative")


def _unit_key(word: str) -> str:
    plain = word.lower().replace("í", "i").replace("ñ", "n")
    return {"dia": "dias", "ano": "anos", "mes": "meses"}.get(plain, plain)


def _franja(match: re.Match[str], clock: Date) -> TemporalResolution:
    span, label = (3, "quarter") if match.group(1).lower() == "trimestre" else (6, "semester")
    first = (clock.month - 1) // span * span + 1
    return _resolution(
        match,
        desde=Date(clock.year, first, 1),
        hasta=Date(clock.year, first + span - 1, _last_day(clock.year, first + span - 1)),
        precision=label,
        clock="relative",
    )


def resolve_relative(text: str, clock: Date) -> TemporalResolution | None:
    """Franja en curso, ventana móvil o «hace N semanas»; None si el texto no las trae."""
    if match := _FRANJA.search(text):
        return _franja(match, clock)
    if match := _ULTIMO_UNICO.search(text):
        return _trailing(match, _unit_key(match.group(1)), 1, clock)
    if match := _NUMERO_ANTES.search(text):
        return _trailing(match, _unit_key(match.group(2)), _count(match.group(1)), clock)
    if match := _HACE_SEMANAS.search(text):
        return _trailing(match, "semanas", _count(match.group(1)), clock)
    return None
