"""Contratos congelados del incidente P1 y de la fuga semántica que lo produce.

Ritual de Etapa 0: la regresión se congela ANTES de tocar runtime. Cada test
afirma el contrato OBJETIVO; los que hoy no se cumplen llevan
``xfail(strict=True)``, de modo que cuando el runtime los satisfaga la suite se
ponga roja y obligue a retirar la marca. Los que ya se cumplen NO llevan marca:
son la no-regresión que el sprint no puede romper.

Incidente:

    "analizame que contratos firmo alcaldia de baranoa en agosto"
    -> 0 procesos, 0 contratos, $0, habiendo actividad real.

Causa observada (probe offline, 2026-09-07): ``firmo`` sobrevive como tema y se
proyecta como ``LIKE '%firmo%'`` sobre el objeto contractual, y el plan de
overview consulta procesos aunque el usuario pidió contratos.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET, SectionStatus
from app.core.entity_profile.render import render_entity_profile
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from test_entity_profile_queries import FakeClient


TODAY = date(2026, 9, 5)

INCIDENT = "analizame que contratos firmo alcaldia de baranoa en agosto"
PROCESS_TOPIC_EVENT = "analiza procesos de vias publicados por alcaldia de baranoa en agosto"
CURRENT_STATE = "analiza procesos actualmente publicados por alcaldia de baranoa"
EVENT_AND_STATE = (
    "analiza contratos firmados en agosto y actualmente terminados "
    "de alcaldia de baranoa"
)
TOPIC_CONTRACTS = "analiza contratos de mantenimiento de alcaldia de baranoa en agosto"
EXPLICIT_BOTH = "analiza procesos y contratos de alcaldia de baranoa"
SOURCE_CONTRADICTION = "analiza procesos de alcaldia de baranoa con proveedores"
LISTING = "que contratos firmo alcaldia de baranoa en agosto"
UNSUPPORTED_EVENT = "analiza contratos terminados en agosto de alcaldia de baranoa"
TOPIC_WITH_SOURCE_WORD = "analiza contratos de firma digital de alcaldia de baranoa"
DEFAULT_OVERVIEW = "analiza alcaldia de baranoa"


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _turn(router: QueryRouter, text: str):
    return classify_entity_profile_turn(text, router.parse(text, today=TODAY).params)


def _run(router: QueryRouter, text: str, client: FakeClient | None = None):
    """Devuelve (turn, cliente ejecutado, render) para un turno de perfil."""
    turn = _turn(router, text)
    client = client or FakeClient()
    if turn.scope is None or turn.identity is None or turn.focus is None:
        return turn, client, ""
    profile = build_entity_profile(
        scope=turn.scope,
        identity=turn.identity,
        params=turn.params,
        focus=turn.focus,
        secop_client=client,
        soql_builder=SoQLBuilder(),
    )
    return turn, client, render_entity_profile(profile)


def _datasets(client: FakeClient) -> list[str]:
    return [dataset for _, dataset, _ in client.calls]


def _soql_for(client: FakeClient, dataset_id: str) -> list[str]:
    return [soql for _, dataset, soql in client.calls if dataset == dataset_id]


# ── Fuga semántica: la acción contractual no es tema ────────────────────────

@pytest.mark.xfail(strict=True, reason="P1: 'firmo' sobrevive como objeto")
def test_contract_action_verb_is_not_a_topic(router):
    turn = _turn(router, INCIDENT)
    assert turn.scope is not None
    assert turn.scope.topic == ()
    assert turn.params.get("objeto") in (None, [], ())


@pytest.mark.xfail(strict=True, reason="P1: LIKE '%firmo%' sobre el objeto")
def test_incident_query_does_not_filter_by_the_action_verb(router):
    _, client, _ = _run(router, INCIDENT)
    assert not any("firmo" in soql.lower() for _, _, soql in client.calls)


# ── Fuente solicitada vs fuente ejecutada ───────────────────────────────────

@pytest.mark.xfail(strict=True, reason="P1: overview consulta procesos sin pedirlos")
def test_contracts_request_issues_no_process_calls(router):
    _, client, _ = _run(router, INCIDENT)
    assert PROCESS_DATASET not in _datasets(client)
    assert CONTRACT_DATASET in _datasets(client)


@pytest.mark.xfail(strict=True, reason="P1: overview consulta contratos sin pedirlos")
def test_processes_request_issues_no_contract_calls(router):
    _, client, _ = _run(router, PROCESS_TOPIC_EVENT)
    assert CONTRACT_DATASET not in _datasets(client)
    assert PROCESS_DATASET in _datasets(client)


@pytest.mark.xfail(
    strict=True, reason="P1: 'proveedores' cambia la fuente en silencio",
)
def test_explicit_process_source_is_not_widened_by_a_supplier_question(router):
    turn = _turn(router, SOURCE_CONTRADICTION)
    assert turn.clarification is not None
    _, client, _ = _run(router, SOURCE_CONTRADICTION)
    assert client.calls == []


def test_explicit_both_sources_execute_both_datasets(router):
    _, client, _ = _run(router, EXPLICIT_BOTH)
    assert PROCESS_DATASET in _datasets(client)
    assert CONTRACT_DATASET in _datasets(client)


def test_default_overview_executes_both_datasets(router):
    _, client, _ = _run(router, DEFAULT_OVERVIEW)
    assert PROCESS_DATASET in _datasets(client)
    assert CONTRACT_DATASET in _datasets(client)


# ── Predicados incompatibles entre datasets ─────────────────────────────────

@pytest.mark.xfail(
    strict=True, reason="P1: estado_del_procedimiento viaja al dataset de contratos",
)
def test_contract_queries_carry_no_process_only_predicate(router):
    _, client, _ = _run(router, PROCESS_TOPIC_EVENT)
    for soql in _soql_for(client, CONTRACT_DATASET):
        assert "estado_del_procedimiento" not in soql
        assert "estado_de_apertura_del_proceso" not in soql


@pytest.mark.xfail(
    strict=True, reason="P1: estado_contrato viaja al dataset de procesos",
)
def test_process_queries_carry_no_contract_only_predicate(router):
    _, client, _ = _run(router, EVENT_AND_STATE)
    for soql in _soql_for(client, PROCESS_DATASET):
        assert "estado_contrato" not in soql


# ── Evento temporal ≠ estado actual ─────────────────────────────────────────

@pytest.mark.xfail(
    strict=True, reason="P1: 'publicados en agosto' añade filtro de estado actual",
)
def test_publication_event_does_not_imply_a_current_state_filter(router):
    _, client, _ = _run(router, PROCESS_TOPIC_EVENT)
    for soql in _soql_for(client, PROCESS_DATASET):
        assert "estado_del_procedimiento" not in soql
        assert "estado_de_apertura_del_proceso =" not in soql
        assert "fecha_de_publicacion_del >=" in soql


def test_current_state_request_keeps_its_state_filter(router):
    _, client, _ = _run(router, CURRENT_STATE)
    process_soql = _soql_for(client, PROCESS_DATASET)
    assert process_soql
    assert all("estado_del_procedimiento" in soql for soql in process_soql)


@pytest.mark.xfail(
    strict=True, reason="P1: 'terminados en agosto' se ejecuta como firma en agosto",
)
def test_unsupported_historical_event_clarifies_without_calling_the_source(router):
    turn = _turn(router, UNSUPPORTED_EVENT)
    assert turn.clarification is not None
    _, client, _ = _run(router, UNSUPPORTED_EVENT)
    assert client.calls == []


def test_signature_event_with_current_state_executes_both_dimensions(router):
    _, client, _ = _run(router, EVENT_AND_STATE)
    contract_soql = _soql_for(client, CONTRACT_DATASET)
    assert contract_soql
    assert all("fecha_de_firma >=" in soql for soql in contract_soql)
    assert all("estado_contrato" in soql for soql in contract_soql)


# ── Tema legítimo sobrevive ─────────────────────────────────────────────────

def test_real_topic_survives_next_to_a_contract_source_word(router):
    turn = _turn(router, TOPIC_CONTRACTS)
    assert turn.scope is not None
    assert turn.scope.topic == ("mantenimiento",)


def test_signature_noun_is_a_topic_and_not_a_temporal_event(router):
    turn = _turn(router, TOPIC_WITH_SOURCE_WORD)
    assert turn.scope is not None
    assert turn.scope.topic == ("firma", "digital")
    _, client, _ = _run(router, TOPIC_WITH_SOURCE_WORD)
    for soql in _soql_for(client, CONTRACT_DATASET):
        assert "fecha_de_firma >=" not in soql


def test_topic_survives_on_a_process_source(router):
    turn = _turn(router, PROCESS_TOPIC_EVENT)
    assert turn.scope is not None
    assert turn.scope.topic == ("vias",)


# ── Listado vs perfil ───────────────────────────────────────────────────────

def test_a_plain_listing_question_is_not_an_entity_profile(router):
    turn = _turn(router, LISTING)
    assert turn.detected is False


# ── Verdad de la evidencia por métrica ──────────────────────────────────────

@pytest.mark.xfail(
    strict=True, reason="P1: un valor ausente invalida también el conteo",
)
def test_contract_count_survives_a_missing_contract_value(router):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "AS contract_count" in soql and "GROUP BY" not in soql:
                return [{
                    "contract_count": "4",
                    "total_contract_value": None,
                    "average_contract_value": None,
                }]
            return super().aggregate(dataset, soql)

    turn = _turn(router, DEFAULT_OVERVIEW)
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus="overview", secop_client=Client(), soql_builder=SoQLBuilder(),
    )
    assert profile.activity.fact("contract_count") == 4
    assert profile.values.fact("total_contract_value") is None
    assert profile.values.status is SectionStatus.UNAVAILABLE
    assert "$0" not in render_entity_profile(profile)


@pytest.mark.xfail(strict=True, reason="P1: as_int convierte un agregado inválido en 0")
def test_invalid_process_aggregate_is_not_zero_processes(router):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "GROUP BY modalidad_de_contratacion" in soql:
                return [{
                    "modalidad_de_contratacion": "Contratación directa",
                    "process_count": "unknown",
                }]
            return super().aggregate(dataset, soql)

    turn = _turn(router, DEFAULT_OVERVIEW)
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus="overview", secop_client=Client(), soql_builder=SoQLBuilder(),
    )
    assert profile.activity.fact("process_count") != 0
    assert "Publicó 0 procesos" not in render_entity_profile(profile)


@pytest.mark.xfail(strict=True, reason="P1: as_float(...) or 0.0 inventa un cero")
def test_temporal_period_without_value_is_not_zero_pesos(router):
    class Client(FakeClient):
        def aggregate(self, dataset, soql):
            if "date_trunc_y" in soql and dataset == CONTRACT_DATASET:
                return [{
                    "period": "2026-01-01T00:00:00.000",
                    "row_count": "4",
                    "contract_value": None,
                }]
            return super().aggregate(dataset, soql)

    turn = _turn(router, DEFAULT_OVERVIEW)
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus="temporal", secop_client=Client(), soql_builder=SoQLBuilder(),
    )
    periods = profile.temporal.fact("periods") or ()
    assert periods
    assert periods[0].contract_count == 4
    assert periods[0].contract_value is None


# ── Coherencia del render con lo ejecutado ──────────────────────────────────

@pytest.mark.xfail(strict=True, reason="P1: un perfil de contratos habla de procesos")
def test_contract_only_profile_does_not_report_processes(router):
    _, _, rendered = _run(router, INCIDENT)
    assert "procesos" not in rendered.lower()
    assert "Contratos" in rendered


@pytest.mark.xfail(strict=True, reason="P1: el foco no-overview reduce agosto a '2026'")
def test_focused_render_keeps_the_requested_month(router):
    turn = _turn(router, INCIDENT)
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus="values", secop_client=FakeClient(), soql_builder=SoQLBuilder(),
    )
    rendered = render_entity_profile(profile)
    assert "2026-08-01" in rendered or "agosto" in rendered.lower()
