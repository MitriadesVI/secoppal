"""Contract count and reported values from one server-side aggregate."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import math
from typing import Any

from app.core.entity_profile.aspects.common import as_float, build_where
from app.core.entity_profile.models import CONTRACT_DATASET


@dataclass(frozen=True, slots=True)
class ContractValues:
    """Conteo y valor son métricas INDEPENDIENTES del mismo agregado.

    ``count(*)`` no depende de la columna de valor: sobre un universo cuyos
    contratos tienen `valor_del_contrato` nulo, la fuente devuelve un conteo
    cierto y una suma nula. Invalidar el conteo por eso perdía un hecho
    acreditado; convertir la suma en 0 habría inventado otro.
    """

    contract_count: int
    total_contract_value: float | None
    average_contract_value: float | None


def build_contract_values_query(builder: Any, params: dict) -> str:
    spec, where = build_where(builder, CONTRACT_DATASET, params)
    value = spec.value
    return (
        f"SELECT count(*) AS contract_count, sum({value}) AS total_contract_value, "
        f"avg({value}) AS average_contract_value WHERE {where}"
    )


def parse_contract_values(rows: list[dict]) -> ContractValues | None:
    """Only an explicit, valid count can establish an empty contract universe.

    Devuelve ``None`` sólo cuando el agregado ENTERO es inservible: forma
    inesperada, conteo no numérico, negativo o fraccionario, o un cero
    contradicho por su propia suma. Una suma ausente o no finita junto a un
    conteo válido no invalida el conteo: deja esa métrica no disponible.
    """
    if len(rows) != 1 or not isinstance(rows[0], dict):
        return None
    row = rows[0]
    try:
        count = Decimal(str(row.get("contract_count")))
    except InvalidOperation:
        return None
    if not count.is_finite() or count < 0 or count != count.to_integral_value():
        return None
    total = as_float(row.get("total_contract_value"))
    if total is None and count == 0 and row.get("total_contract_value") is None:
        total = 0.0  # SUM(NULL) over the explicitly confirmed empty universe.
    if count == 0 and total not in (None, 0):
        return None  # un cero que su propia suma desmiente no es un cero.
    if total is not None and not math.isfinite(total):
        total = None
    average = as_float(row.get("average_contract_value"))
    if average is not None and not math.isfinite(average):
        average = None
    return ContractValues(
        contract_count=int(count),
        total_contract_value=total,
        average_contract_value=average,
    )
