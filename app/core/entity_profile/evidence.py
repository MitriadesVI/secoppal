"""Validate evidence without changing the recorded transport outcome.

Una respuesta HTTP exitosa es NECESARIA pero no suficiente para afirmar un
hecho. Dos cosas la invalidan: un agregado ilegible, y un cero que otra llamada
exitosa del MISMO plan desmiente con filas.

La revisión independiente (F7) encontró que la reconciliación existía sólo para
contratos: un agregado de procesos podía decir 0 mientras la consulta de
procesos recientes devolvía filas, y el producto mostraba ambas cosas a la vez
—«Publicó 0 procesos» junto a un proceso reciente—. La asimetría venía de BASE;
aquí se cierra en los dos sentidos.
"""
from dataclasses import replace

from app.core.entity_profile.aspects.activity import parse_process_activity
from app.core.entity_profile.aspects.values import parse_contract_values
from app.core.entity_profile.models import SourceCall


# Qué otra evidencia del mismo plan puede desmentir un cero de cada agregado.
# Sólo llamadas del MISMO dataset y el MISMO scope: una fuente no contradice a
# la otra, porque miden universos distintos.
_CONTRADICTED_BY = {
    "contract_values": ("recent_contracts", "supplier_count", "supplier_value"),
    "process_activity": ("recent_processes",),
}


def _has_contradicting_rows(
    call_id: str, rows: dict[str, list[dict]], calls: dict[str, SourceCall],
) -> bool:
    return any(
        calls.get(key) is not None
        and calls[key].status == "SUCCESS"
        and rows.get(key)
        for key in _CONTRADICTED_BY[call_id]
    )


def _verdict(
    call_id: str, count: int | None, rows: dict, calls: dict,
) -> str | None:
    if count is None:
        return "INVALID_RESPONSE"
    if count == 0 and _has_contradicting_rows(call_id, rows, calls):
        return "INCONSISTENT_RESPONSE"
    return None


def section_evidence_calls(
    rows: dict[str, list[dict]], calls: dict[str, SourceCall],
) -> dict[str, SourceCall]:
    """A successful HTTP response is necessary, but not sufficient, for a fact."""
    updated = calls
    for call_id, count in (
        ("contract_values", _contract_count(rows)),
        ("process_activity", parse_process_activity(
            rows.get("process_activity", []),
        ).process_count),
    ):
        call = calls.get(call_id)
        if call is None or call.status != "SUCCESS":
            continue
        reason = _verdict(call_id, count, rows, calls)
        if reason is not None:
            updated = {**updated, call_id: replace(call, status=reason)}
    return updated


def _contract_count(rows: dict[str, list[dict]]) -> int | None:
    parsed = parse_contract_values(rows.get("contract_values", []))
    return None if parsed is None else parsed.contract_count
