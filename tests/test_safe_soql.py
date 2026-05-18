"""SAFE-SOQL-001 — SoQLBuilder must refuse to emit accidental global queries.

A "global query" is one whose WHERE clause collapses to a tautology like
``1=1`` because no substantial filter was provided. Filters like fecha,
valor and ordering are NOT considered substantial on their own — they
narrow nothing without a scope or topic.

Callers that genuinely need a global query (e.g. internal admin tools or
analytics over a pre-validated universe) must opt in explicitly with
``allow_global=True``.
"""
from __future__ import annotations

import pytest

from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError


PROCESOS = SoQLBuilder.PROCESOS_DATASET
CONTRATOS = SoQLBuilder.CONTRATOS_DATASET


def test_build_contratos_with_no_filters_raises() -> None:
    builder = SoQLBuilder()
    with pytest.raises(UnsafeGlobalQueryError):
        builder.build(CONTRATOS, {})


def test_build_procesos_with_no_filters_raises() -> None:
    builder = SoQLBuilder()
    with pytest.raises(UnsafeGlobalQueryError):
        builder.build(PROCESOS, {})


def test_build_count_contratos_with_no_filters_raises() -> None:
    builder = SoQLBuilder()
    with pytest.raises(UnsafeGlobalQueryError):
        builder.build_count(CONTRATOS, {})


@pytest.mark.parametrize(
    "params",
    [
        {"fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31"},
        {"valor_min": 100_000_000},
        {"valor_max": 50_000_000},
        {"valor_min": 100_000_000, "valor_max": 500_000_000},
        {"ordering_signal": "valor_desc"},
        {"fecha_desde": "2026-01-01", "ordering_signal": "valor_desc"},
    ],
)
def test_only_date_value_or_order_is_not_a_substantial_filter(params: dict) -> None:
    """fecha/valor/orden por sí solos no son filtros base suficientes."""
    builder = SoQLBuilder()
    with pytest.raises(UnsafeGlobalQueryError):
        builder.build(CONTRATOS, params)


def test_valid_opportunity_query_still_builds() -> None:
    """Una consulta válida de oportunidad debe seguir generando SoQL."""
    builder = SoQLBuilder()
    params = {
        "dataset": "procesos",
        "intent_type": "opportunity_search",
        "estado_family": "oferta_abierta",
        "estado_del_procedimiento": ["Publicado", "Borrador", "Abierto"],
        "departamento_resolved": "Atlántico",
        "objeto": ["mantenimiento"],
    }
    soql = builder.build(PROCESOS, params)
    assert "SELECT" in soql and " WHERE " in soql
    assert "1=1" not in soql
    assert "Atlántico" in soql
    assert "mantenimiento" in soql.lower() or "MANTENIMIENTO" in soql


def test_valid_contrato_query_still_builds() -> None:
    """Una consulta válida de contrato debe seguir generando SoQL."""
    builder = SoQLBuilder()
    params = {
        "dataset": "contratos",
        "objeto": ["primera_infancia"],
        "fecha_desde": "2026-01-01",
        "fecha_hasta": "2026-12-31",
        "departamento_resolved": "Atlántico",
    }
    soql = builder.build(CONTRATOS, params)
    assert "1=1" not in soql
    assert "Atlántico" in soql
    assert "primera_infancia" in soql.lower() or "PRIMERA_INFANCIA" in soql.upper()


def test_allow_global_opt_in_bypasses_guard() -> None:
    """Callers que necesiten un global query DEBEN pasar allow_global=True."""
    builder = SoQLBuilder()
    soql = builder.build(CONTRATOS, {}, allow_global=True)
    assert " WHERE 1=1 " in soql


def test_build_count_allow_global_opt_in() -> None:
    builder = SoQLBuilder()
    soql = builder.build_count(PROCESOS, {}, allow_global=True)
    assert soql == "SELECT count(*) WHERE 1=1"


def test_analytics_aggregate_without_scope_returns_needs_clarification() -> None:
    """Analytics aggregate sin scope sigue devolviendo needs_clarification,
    no UnsafeGlobalQueryError crudo. La excepción la atrapa la capa de
    analítica y la traduce a needs_clarification.
    """
    from app.core.analytics import maybe_handle_analytical_query

    class _StubSecop:
        def aggregate(self, *args, **kwargs):  # pragma: no cover
            raise AssertionError("aggregate should not be called when scope is missing")

    builder = SoQLBuilder()
    result = maybe_handle_analytical_query(
        user_query="cuanto se contrato en total",
        params={"dataset": "contratos"},
        dataset_id=CONTRATOS,
        secop_client=_StubSecop(),
        soql_builder=builder,
    )
    assert result is not None
    assert result.get("needs_clarification") is True
    assert "filtro" in (result.get("clarification_reason") or "").lower()


def test_substantial_filters_recognized() -> None:
    """Cualquiera de los filtros base debe permitir construir SoQL."""
    builder = SoQLBuilder()
    substantial_cases = [
        {"objeto": ["mantenimiento"]},
        {"departamento_resolved": "Atlántico"},
        {"ciudad": "Barranquilla"},
        {"entidad_resolved": "ALCALDIA DE BARRANQUILLA"},
        {"entidad_like": "ALCALDIA"},
        {"contratista": "1234567890"},
        {"estado_del_procedimiento": ["Publicado"]},
        {"estado_contrato": "Activo"},
        {"modalidad": "Licitación Pública"},
    ]
    for params in substantial_cases:
        soql = builder.build(CONTRATOS, params)
        assert "1=1" not in soql, f"params={params} should not produce 1=1"
