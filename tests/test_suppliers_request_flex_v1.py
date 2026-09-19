"""SUPPLIERS 1.1 — el request explícito, el corpus y las propiedades (§15-16).

Un solo módulo porque las tres cosas miden lo mismo desde distinto ángulo: la
autoridad de cardinalidad, el resolver de la petición y el transporte que la
lleva intacta hasta el SQL. Nada de aquí toca la red ni la ejecución real (esa
vive en ``test_suppliers_request_flex_v1_integration``).
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from hypothesis import given, settings, strategies as st

from app.core.capability_binding.supplier_request_authority import assess_supplier_request
from app.core.entity_profile.frame_compiler import _job_catalog, _ranked_specs
from app.core.entity_profile.supplier_ranking_request import (
    DEFAULT_SUPPLIER_LIMIT,
    DEFAULT_SUPPLIER_REQUEST,
    LIMIT_PARAM,
    MAX_SUPPLIER_LIMIT,
    METRIC_PARAM,
    SupplierRankingMetric,
    SupplierRankingRequest,
    ranking_metric,
    request_from_params,
    resolve_supplier_ranking_request,
    transport_params,
)
from app.core.soql_builder import SoQLBuilder


CORPUS = json.loads(
    Path("tests/fixtures/suppliers_request_flex_v1_corpus.json").read_text(encoding="utf-8"),
)["cases"]

BUILDER = SimpleNamespace(
    SPECS=SoQLBuilder.SPECS,
    _build_where=lambda spec, params: "nombre_entidad = 'BUYER'",
)


def resolved(query: str) -> SupplierRankingRequest | None:
    return resolve_supplier_ranking_request(query, assess_supplier_request(query))


def sqls_for(request: SupplierRankingRequest) -> dict[str, str]:
    specs = _ranked_specs(_job_catalog(request)["suppliers"], request)
    return {spec.call_id: spec.build(BUILDER, {}) for spec in specs}


# ── el request: validación y órdenes ─────────────────────────────────────────

def test_request_is_immutable_and_validates_its_own_values():
    request = SupplierRankingRequest(SupplierRankingMetric.CONTRACT_COUNT, 3)
    assert request.metric is SupplierRankingMetric.CONTRACT_COUNT
    assert request.limit == 3
    assert request.fetch_limit == 4
    for bad_limit in (0, -1, MAX_SUPPLIER_LIMIT + 1, True, "3", None):
        with pytest.raises(ValueError):
            SupplierRankingRequest(SupplierRankingMetric.BOTH, bad_limit)
    with pytest.raises(ValueError):
        SupplierRankingRequest("count", 3)


def test_metric_asks_its_own_rankings():
    both = DEFAULT_SUPPLIER_REQUEST
    assert both.metric.asks("count") and both.metric.asks("value")
    count = SupplierRankingRequest(SupplierRankingMetric.CONTRACT_COUNT, 5)
    assert count.metric.asks("count") and not count.metric.asks("value")
    value = SupplierRankingRequest(SupplierRankingMetric.CONTRACT_VALUE, 5)
    assert value.metric.asks("value") and not value.metric.asks("count")


# ── el corpus (§15) ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("case", CORPUS, ids=lambda case: case["id"])
def test_corpus_resolution_is_exactly_the_declared_one(case):
    request = resolved(case["query"])
    got = {
        "metric": request.metric.value if request else None,
        "limit": request.limit if request else None,
    }
    assert got == case["expect"], case["query"]
    if request is not None:
        assert 1 <= request.limit <= MAX_SUPPLIER_LIMIT


# ── transporte entre carriles ────────────────────────────────────────────────

def test_transport_roundtrip_is_json_safe_and_validated():
    request = SupplierRankingRequest(SupplierRankingMetric.CONTRACT_VALUE, 12)
    primitives = transport_params(request)
    assert primitives == {LIMIT_PARAM: 12, METRIC_PARAM: "value"}
    assert json.loads(json.dumps(primitives)) == primitives
    assert request_from_params(primitives) == request
    assert request_from_params({}) == DEFAULT_SUPPLIER_REQUEST


@pytest.mark.parametrize("malformed", [
    {LIMIT_PARAM: 0, METRIC_PARAM: "count"},
    {LIMIT_PARAM: MAX_SUPPLIER_LIMIT + 1, METRIC_PARAM: "count"},
    {LIMIT_PARAM: True, METRIC_PARAM: "count"},
    {LIMIT_PARAM: "3", METRIC_PARAM: "count"},
    {LIMIT_PARAM: 3, METRIC_PARAM: "otro"},
    {LIMIT_PARAM: 3, METRIC_PARAM: None},
    {LIMIT_PARAM: None, METRIC_PARAM: "count"},
    # Presente con None también es fail-closed: sólo AUSENTE es el default.
    {LIMIT_PARAM: None, METRIC_PARAM: None},
])
def test_malformed_transport_fails_closed(malformed):
    assert request_from_params(malformed) is None


# ── fronteras cerradas por la revisión independiente (§11) ───────────────────

@pytest.mark.parametrize("surface", [
    "el top 30 de los proveedores de la alcaldía de Barranquilla",
    "top 30 de proveedores de la alcaldía de Barranquilla",
    "dame los 30 de los proveedores de la alcaldía de Barranquilla",
    "veintiséis proveedores de la alcaldía de Barranquilla",
    "veintinueve proveedores de la alcaldía de Barranquilla",
    "doscientos proveedores de la alcaldía de Barranquilla",
])
def test_a_dangling_or_spelled_number_out_of_range_never_falls_to_the_default(surface):
    """D1/D2: el número no adherido («de los») o en palabras se cierra, no se sirve."""
    assert resolved(surface) is None


@pytest.mark.parametrize("surface", [
    "el 10 por ciento de los proveedores de la alcaldía de Barranquilla",
    "diez por ciento de los proveedores de la alcaldía de Barranquilla",
    "la mitad de los proveedores de la alcaldía de Barranquilla",
])
def test_a_proportion_never_falls_to_the_default(surface):
    """D3: la proporción (símbolo o palabras) no se reinterpreta como corte."""
    assert resolved(surface) is None


def test_the_year_decoy_is_not_a_dangling_quantity():
    # La regresión que la ventana corta evita: un año lejano no es una cantidad
    # colgante y el decoy sigue resolviendo la vista por defecto.
    assert resolved("en 2025 quiero ver proveedores") == DEFAULT_SUPPLIER_REQUEST


# ── propiedades (§16) ────────────────────────────────────────────────────────

@given(value=st.integers(min_value=1, max_value=MAX_SUPPLIER_LIMIT))
def test_every_exact_within_the_range_becomes_exactly_that_limit(value):
    request = resolved(f"{value} proveedores de la alcaldía de Barranquilla")
    assert request is not None and request.limit == value


@given(value=st.integers(min_value=MAX_SUPPLIER_LIMIT + 1, max_value=9999))
def test_no_exact_beyond_the_range_ever_executes(value):
    assert resolved(f"{value} proveedores de la alcaldía de Barranquilla") is None


@given(value=st.integers(min_value=-100, max_value=0))
def test_zero_and_negative_never_become_a_limit(value):
    request = resolved(f"{value} proveedores de la alcaldía de Barranquilla")
    assert request is None or request.limit >= 1


@given(amount=st.integers(min_value=1, max_value=10**7))
def test_a_monetary_amount_never_changes_the_requested_view(amount):
    """El monto no participa: con y sin él, la resolución es exactamente la misma.

    (La versión anterior comparaba `limit != amount`, que con amount=5 coincide
    con el default y no probaba nada: ZEUS lo encontró.)
    """
    base = resolved("proveedores de la alcaldía de Barranquilla")
    for surface in (
        f"proveedores de la alcaldía de Barranquilla por más de {amount} millones",
        f"proveedores de la alcaldía de Barranquilla con contratos por {amount} millones",
    ):
        assert resolved(surface) == base


@given(year=st.integers(min_value=1990, max_value=2100))
def test_a_year_never_becomes_the_cut(year):
    request = resolved(f"proveedores de la alcaldía de Barranquilla en {year}")
    assert request == DEFAULT_SUPPLIER_REQUEST


@given(document=st.integers(min_value=10**6, max_value=10**10))
def test_a_document_never_becomes_the_cut(document):
    request = resolved(f"dame 5 proveedores de la alcaldía de Barranquilla con documento {document}")
    assert request is not None
    assert request.limit == DEFAULT_SUPPLIER_LIMIT
    assert request.limit != document


@given(value=st.integers(min_value=1, max_value=MAX_SUPPLIER_LIMIT))
def test_metric_count_never_orders_by_value(value):
    request = SupplierRankingRequest(SupplierRankingMetric.CONTRACT_COUNT, value)
    sqls = sqls_for(request)
    assert set(sqls) == {"supplier_count"}
    assert "ORDER BY contract_count DESC" in sqls["supplier_count"]
    assert "total_contract_value DESC" not in sqls["supplier_count"]
    assert sqls["supplier_count"].endswith(f"LIMIT {value + 1}")


@given(value=st.integers(min_value=1, max_value=MAX_SUPPLIER_LIMIT))
def test_metric_value_never_orders_by_count(value):
    request = SupplierRankingRequest(SupplierRankingMetric.CONTRACT_VALUE, value)
    sqls = sqls_for(request)
    assert set(sqls) == {"supplier_value"}
    assert "ORDER BY total_contract_value DESC" in sqls["supplier_value"]
    assert "contract_count DESC" not in sqls["supplier_value"]
    assert sqls["supplier_value"].endswith(f"LIMIT {value + 1}")


@given(value=st.integers(min_value=1, max_value=MAX_SUPPLIER_LIMIT))
@settings(max_examples=20, deadline=None)
def test_both_executes_exactly_the_two_rankings(value):
    request = SupplierRankingRequest(SupplierRankingMetric.BOTH, value)
    sqls = sqls_for(request)
    assert set(sqls) == {"supplier_count", "supplier_value"}
    assert all(sql.endswith(f"LIMIT {value + 1}") for sql in sqls.values())


# ── paridad V1 (§5) ──────────────────────────────────────────────────────────

def test_the_default_view_is_exactly_the_v1_view():
    # V1 ⊂ V1.1: la consulta genérica conserva métrica BOTH y corte 5, y los dos
    # rankings se ejecutan. Es la propiedad que el baseline AFTER debe repetir.
    assert DEFAULT_SUPPLIER_LIMIT == 5
    assert DEFAULT_SUPPLIER_REQUEST == SupplierRankingRequest(
        SupplierRankingMetric.BOTH, DEFAULT_SUPPLIER_LIMIT,
    )
    request = resolved("proveedores de la alcaldía de Galapa")
    assert request == DEFAULT_SUPPLIER_REQUEST
    assert set(sqls_for(request)) == {"supplier_count", "supplier_value"}


def test_ranking_metric_reads_one_criterion_or_none():
    assert ranking_metric("proveedores con más contratos de X") is SupplierRankingMetric.CONTRACT_COUNT
    assert ranking_metric("proveedores con mayor valor contratado de X") is SupplierRankingMetric.CONTRACT_VALUE
    assert ranking_metric("proveedores de X") is SupplierRankingMetric.BOTH
