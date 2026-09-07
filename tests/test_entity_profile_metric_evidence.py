"""Verdad por métrica: transporte exitoso no es lo mismo que hecho acreditado."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st

from app.core.entity_profile.aspects.activity import parse_process_activity
from app.core.entity_profile.aspects.temporal import merge_periods
from app.core.entity_profile.aspects.suppliers import parse_suppliers
from app.core.entity_profile.aspects.values import parse_contract_values
from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.models import CONTRACT_DATASET, SectionStatus
from app.core.entity_profile.render import render_entity_profile
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from test_entity_profile_queries import FakeClient


TODAY = date(2026, 9, 5)


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


@pytest.fixture
def turn(router):
    text = "analiza alcaldia de baranoa en 2026"
    return classify_entity_profile_turn(text, router.parse(text, today=TODAY).params)


def _profile(turn, client, focus="overview"):
    return build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=focus, secop_client=client, soql_builder=SoQLBuilder(),
        plan=turn.plan if focus == turn.focus else None,
    )


# ── Independencia de métricas del mismo agregado ────────────────────────────

@pytest.mark.parametrize("total", [None, "", "n/d", "NaN", "Infinity"])
def test_an_unusable_total_never_invalidates_a_valid_count(total):
    parsed = parse_contract_values([{
        "contract_count": "7", "total_contract_value": total,
    }])
    assert parsed is not None
    assert parsed.contract_count == 7
    assert parsed.total_contract_value is None


def test_an_empty_universe_still_confirms_a_zero_sum():
    parsed = parse_contract_values([{
        "contract_count": "0", "total_contract_value": None,
    }])
    assert parsed is not None
    assert parsed.contract_count == 0
    assert parsed.total_contract_value == 0.0


def test_a_zero_denied_by_its_own_sum_is_not_a_fact():
    assert parse_contract_values([{
        "contract_count": "0", "total_contract_value": "100",
    }]) is None


@settings(max_examples=50, derandomize=True)
@given(count=st.integers(min_value=1, max_value=10**12))
def test_a_valid_count_is_never_lost_to_an_absent_value(count):
    parsed = parse_contract_values([{
        "contract_count": str(count), "total_contract_value": None,
    }])
    assert parsed is not None and parsed.contract_count == count


# ── Procesos ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw", ["unknown", None, "", "n/a"])
def test_an_illegible_process_count_is_unknown_and_not_zero(raw):
    parsed = parse_process_activity([
        {"modalidad_de_contratacion": "Contratación directa", "process_count": raw},
    ])
    assert parsed.process_count is None


def test_an_illegible_process_aggregate_says_so_instead_of_reporting_zero(turn):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "GROUP BY modalidad_de_contratacion" in soql:
                return [{
                    "modalidad_de_contratacion": "Contratación directa",
                    "process_count": "unknown",
                }]
            return super().aggregate(dataset, soql)

    profile = _profile(turn, Client())
    rendered = render_entity_profile(profile)
    assert profile.activity.fact("process_count") is None
    assert "Publicó 0 procesos" not in rendered
    assert any("ilegible" in warning for warning in profile.warnings)


# ── Series temporales ───────────────────────────────────────────────────────

def test_a_period_without_a_reported_value_is_unavailable_not_zero():
    periods = merge_periods(
        None,
        [{"period": "2026-01-01T00:00:00.000", "row_count": "4", "contract_value": None}],
    )
    assert periods[0].contract_count == 4
    assert periods[0].contract_value is None


def test_an_illegible_period_count_is_unavailable_not_zero():
    periods = merge_periods(
        [{"period": "2026-01-01T00:00:00.000", "row_count": "muchos"}], None,
    )
    assert periods[0].process_count is None


# ── Proveedores ─────────────────────────────────────────────────────────────

def test_a_supplier_without_a_reported_value_is_unavailable_not_zero():
    parsed = parse_suppliers([{
        "documento_proveedor": "900123456",
        "tipodocproveedor": "NIT",
        "supplier_name": "PROVEEDOR SAS",
        "contract_count": "3",
        "total_contract_value": None,
    }])
    assert parsed[0].contract_count == 3
    assert parsed[0].total_contract_value is None


def test_an_empty_supplier_ranking_does_not_deny_contractual_activity(turn):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "GROUP BY documento_proveedor" in soql:
                return []
            return super().aggregate(dataset, soql)

    profile = _profile(turn, Client())
    rendered = render_entity_profile(profile)
    # El ranking está vacío y la actividad contractual sigue acreditada por su
    # propio agregado: son evidencias distintas.
    assert profile.suppliers.status is SectionStatus.SUPPORTED
    assert profile.suppliers.fact("top_by_contract_count") == ()
    assert profile.activity.fact("contract_count") == 4
    assert "0 contratos" not in rendered


def test_a_supplier_with_no_value_renders_as_unavailable_not_as_zero(turn):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "GROUP BY documento_proveedor" in soql:
                return [{
                    "documento_proveedor": "900123456",
                    "tipodocproveedor": "NIT",
                    "supplier_name": "PROVEEDOR SAS",
                    "contract_count": "3",
                    "total_contract_value": None,
                }]
            return super().aggregate(dataset, soql)

    profile = _profile(turn, Client(), focus="suppliers")
    rendered = render_entity_profile(profile)
    assert "$0" not in rendered
    assert "valor no disponible" in rendered


# ── Contradicción ───────────────────────────────────────────────────────────

def test_a_zero_contradicted_by_other_successful_calls_is_never_shown(turn):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "AS contract_count" in soql and "GROUP BY" not in soql:
                return [{"contract_count": "0", "total_contract_value": None}]
            return super().aggregate(dataset, soql)

    profile = _profile(turn, Client())
    rendered = render_entity_profile(profile)
    assert profile.activity.fact("contract_count") is None
    assert "0 contratos" not in rendered
    assert any("contradictorios" in warning for warning in profile.warnings)


def test_a_transport_success_with_an_unreadable_payload_is_not_a_fact(turn):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "AS contract_count" in soql and "GROUP BY" not in soql:
                return [{"contract_count": "invalid"}]
            return super().aggregate(dataset, soql)

    profile = _profile(turn, Client())
    call = next(
        c for c in profile.source_calls.calls if c.call_id == "contract_values"
    )
    # El transporte SÍ respondió, y SourceCalls lo conserva: el veredicto de
    # evidencia decide qué se puede afirmar, no qué ocurrió en la red.
    assert call.status == "SUCCESS"
    assert profile.activity.fact("contract_count") is None
    assert profile.values.status is SectionStatus.UNAVAILABLE
    assert any("inválido" in warning for warning in profile.warnings)


def test_the_recorded_transport_outcome_survives_the_evidence_verdict(turn):
    """Un veredicto de evidencia no reescribe la latencia ni el SoQL enviado."""
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "AS contract_count" in soql and "GROUP BY" not in soql:
                return [{"contract_count": "invalid"}]
            return super().aggregate(dataset, soql)

    profile = _profile(turn, Client())
    call = next(
        c for c in profile.source_calls.calls if c.call_id == "contract_values"
    )
    assert call.dataset_id == CONTRACT_DATASET
    assert call.soql
    assert call.rows_received == 1
