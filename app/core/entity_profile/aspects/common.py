"""Small parsing and query helpers shared by Entity Profile aspects."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def build_where(builder: Any, dataset_id: str, params: dict) -> tuple[Any, str]:
    spec = builder.SPECS[dataset_id]
    where = builder._build_where(spec, params)
    if not where.strip() or "1=1" in where:
        raise ValueError("Entity Profile refuses an unscoped SECOP query")
    return spec, where


def as_optional_int(value: object) -> int | None:
    """Un conteo exacto, o nada. Nunca un número que la fuente no dijo.

    ``int(float(value))`` truncaba: "0.5" se convertía en **0 procesos** y "-1"
    en **-1 procesos**, ambos presentados como hechos. Un conteo de filas es
    entero, finito y no negativo por construcción; cualquier otra cosa es una
    respuesta que no acredita cifra alguna.

    Se usa ``Decimal`` y no ``float`` para que "4.0" —un entero exacto que la
    fuente serializó con decimal— se acepte, mientras que 0.5 se rechaza. Con
    ``float`` ambos casos serían indistinguibles después de truncar.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    try:
        number = Decimal(str(value))
    except (TypeError, ValueError, ArithmeticError, InvalidOperation):
        return None
    if not number.is_finite() or number < 0:
        return None
    if number != number.to_integral_value():
        return None
    return int(number)


def as_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def iso_date(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value[:10]


def scalar_url(value: object) -> str | None:
    if isinstance(value, dict):
        value = value.get("url")
    return value if isinstance(value, str) and value else None
