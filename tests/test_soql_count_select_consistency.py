from __future__ import annotations

from app.core.soql_builder import SoQLBuilder


def _where_from_select(soql: str) -> str:
    return soql.split(" WHERE ", 1)[1].split(" ORDER BY ", 1)[0]


def _where_from_count(soql: str) -> str:
    return soql.split(" WHERE ", 1)[1]


def test_count_query_uses_same_where_as_select_for_contract_filters():
    """COUNT y SELECT deben consultar exactamente el mismo universo."""
    builder = SoQLBuilder()
    dataset_id = SoQLBuilder.CONTRATOS_DATASET
    params = {
        "dataset": "contratos",
        "departamento_resolved": "Atlántico",
        "ciudad": "Barranquilla",
        "entidad_like": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
        "objeto": ["mantenimiento", ["adulto_mayor", "primera_infancia"]],
        "valor_min": 100_000_000,
        "valor_max": 900_000_000,
        "estado_contrato": ["En ejecución", "Modificado", "Prorrogado"],
        "fecha_desde": "2025-01-01",
        "fecha_hasta": "2026-12-31",
        "contratista": "900123456",
        "ordering_signal": "valor_desc",
        "limit": 10,
        "offset": 20,
    }

    select_soql = builder.build(dataset_id, params)
    count_soql = builder.build_count(dataset_id, params)

    assert _where_from_count(count_soql) == _where_from_select(select_soql)


def test_count_query_uses_same_where_as_select_for_process_filters():
    """COUNT y SELECT deben compartir WHERE también en procesos."""
    builder = SoQLBuilder()
    dataset_id = SoQLBuilder.PROCESOS_DATASET
    params = {
        "dataset": "procesos",
        "departamento_resolved": "Antioquia",
        "ciudad": "Medellín",
        "objeto": ["obra", "mantenimiento"],
        "estado": "Abierto",
        "estado_field": "estado_de_apertura_del_proceso",
        "fecha_desde": "2026-01-01",
        "fecha_hasta": "2026-12-31",
        "valor_min": 50_000_000,
    }

    select_soql = builder.build(dataset_id, params)
    count_soql = builder.build_count(dataset_id, params)

    assert _where_from_count(count_soql) == _where_from_select(select_soql)
