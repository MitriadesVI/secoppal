"""AP2-C — la huella contractual observada: métricas, parcialidad y límites.

Las pruebas corren contra un doble del cliente SECOP construido con las
filas y los agregados REALES que el contrato de fuente congeló
(``tests/fixtures/actor_sources/``). No tocan la red.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from app.core.actor_profile import ActorProfileResult, build_actor_profile
from app.core.actor_profile.aspects import ASPECTS, aspect_names
from app.core.actor_profile.models import AspectStatus, ObservedName
from app.core.outcome import Outcome, OutcomeReason
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder

CONTRACT = json.loads(
    Path("tests/fixtures/actor_sources/secop2_contratos_source_contract.json")
    .read_text(encoding="utf-8")
)
REFERENCE = CONTRACT["reference_actor"]
CONTRATOS = SoQLBuilder.CONTRATOS_DATASET


# ---------------------------------------------------------------------------
# Doble del cliente: responde por la FORMA del SoQL, como lo haría Socrata.
# ---------------------------------------------------------------------------


class FakeSecopClient:
    """Cliente falso que devuelve los agregados reales del actor de referencia.

    ``fail`` nombra los aspectos que deben reventar, para poder ejercitar el
    ensamblaje parcial sin depender de que la fuente se caiga sola.
    """

    def __init__(self, fail: set[str] | None = None, exc: Exception | None = None):
        self.fail = fail or set()
        self.exc = exc or RuntimeError("source down")
        self.calls: list[str] = []

    def with_timeout(self, timeout: int) -> "FakeSecopClient":
        return self

    def _dispatch(self, soql: str) -> list[dict]:
        self.calls.append(soql)
        if "SUM(" in soql:
            self._maybe_fail("core")
            return [{"total_value": REFERENCE["total_reported_value"],
                     "total_count": REFERENCE["contract_count"]}]
        if "date_min" in soql:
            self._maybe_fail("date_range")
            return [{"date_min": f"{REFERENCE['first_signature_date']}T00:00:00.000",
                     "date_max": f"{REFERENCE['last_signature_date']}T00:00:00.000"}]
        if "date_trunc_y" in soql:
            self._maybe_fail("years")
            return [{"yr": f"{year}-01-01T00:00:00.000", "cnt": str(count)}
                    for year, count in REFERENCE["years"].items()]
        if "GROUP BY nombre_entidad" in soql:
            self._maybe_fail("buyers")
            return [dict(row, cnt=str(row["cnt"])) for row in REFERENCE["top_buyers"]]
        if "GROUP BY modalidad_de_contratacion" in soql:
            self._maybe_fail("modalities")
            return [dict(row, cnt=str(row["cnt"])) for row in REFERENCE["top_modalities"]]
        if "GROUP BY departamento" in soql:
            self._maybe_fail("territories")
            return [dict(row, cnt=str(row["cnt"])) for row in REFERENCE["top_territories"]]
        if "GROUP BY proveedor_adjudicado" in soql:
            self._maybe_fail("observed_names")
            return [{"proveedor_adjudicado": n["name"],
                     "documento_proveedor": REFERENCE["documento_proveedor"],
                     "tipodocproveedor": REFERENCE["tipodocproveedor"],
                     "cnt": str(n["count"])}
                    for n in CONTRACT["observed_names_sample"]["names"]]
        self._maybe_fail("recent_contracts")
        return list(CONTRACT["frozen_rows"])

    def _maybe_fail(self, name: str) -> None:
        if name in self.fail:
            raise self.exc

    def aggregate(self, dataset_id: str, soql: str) -> list[dict]:
        return self._dispatch(soql)

    def query(self, dataset_id: str, soql: str) -> list[dict]:
        return self._dispatch(soql)


@pytest.fixture
def params() -> dict:
    return dict(QueryRouter().parse("contratos del NIT 860002400").params)


def profile_for(params: dict, client: FakeSecopClient | None = None) -> ActorProfileResult:
    return build_actor_profile(
        params, CONTRATOS, client or FakeSecopClient(), SoQLBuilder(),
    )


# ---------------------------------------------------------------------------
# Las diez métricas del encargo.
# ---------------------------------------------------------------------------


def test_the_profile_carries_every_metric_of_the_contractual_footprint(params):
    profile = profile_for(params)

    assert profile.contract_count == REFERENCE["contract_count"]
    assert profile.total_reported_value == REFERENCE["total_reported_value"]
    assert profile.first_observed_date == REFERENCE["first_signature_date"]
    assert profile.last_observed_date == REFERENCE["last_signature_date"]
    assert profile.buyers[0][0] == REFERENCE["top_buyers"][0]["nombre_entidad"]
    assert profile.years[0] == ("2026", REFERENCE["years"]["2026"])
    assert profile.modalities[0][0] == REFERENCE["top_modalities"][0]["modalidad_de_contratacion"]
    assert profile.territories[0][0] == REFERENCE["top_territories"][0]["departamento"]
    assert profile.observed_names
    assert profile.recent_contracts


def test_the_year_label_is_a_year_not_a_socrata_timestamp(params):
    """``date_trunc_y`` devuelve "2026-01-01T00:00:00.000"; el perfil dice 2026."""
    profile = profile_for(params)
    assert all(len(year) == 4 and year.isdigit() for year, _ in profile.years)


def test_dates_are_calendar_dates_not_instants(params):
    profile = profile_for(params)
    assert profile.first_observed_date is not None
    assert len(profile.first_observed_date) == 10


def test_counts_arrive_as_integers_even_though_socrata_sends_strings(params):
    profile = profile_for(params)
    assert all(isinstance(count, int) for _, count in profile.buyers)
    assert all(isinstance(count, int) for _, count in profile.years)


# ---------------------------------------------------------------------------
# INV-CUTOFF — cada perfil sabe cuándo se consultó.
# ---------------------------------------------------------------------------


def test_inv_cutoff_the_profile_is_timezone_aware(params):
    profile = profile_for(params)
    assert isinstance(profile.queried_at, datetime)
    assert profile.queried_at.tzinfo is not None
    assert profile.queried_at.utcoffset() is not None


def test_inv_cutoff_a_naive_timestamp_is_rejected_at_construction():
    """No se puede fabricar un perfil sin saber cuándo se miró la fuente."""
    with pytest.raises(ValueError):
        ActorProfileResult(
            queried_at=datetime(2026, 8, 18, 14, 32),
            identity={}, dataset_id=CONTRATOS,
        )


def test_inv_cutoff_one_timestamp_governs_the_whole_profile(params):
    """Los agregados corren en paralelo contra una fuente viva; fechar cada uno
    por separado sugeriría una precisión que no existe."""
    profile = profile_for(params)
    assert not hasattr(profile.aspects[0], "queried_at")
    assert profile.queried_at.utcoffset() is not None


# ---------------------------------------------------------------------------
# INV-PARTIAL — un agregado caído no derriba el perfil.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "broken", ["buyers", "years", "modalities", "territories", "observed_names"],
)
def test_inv_partial_a_secondary_aggregate_failing_never_destroys_the_profile(params, broken):
    profile = profile_for(params, FakeSecopClient(fail={broken}))

    assert profile.contract_count == REFERENCE["contract_count"]
    assert profile.total_reported_value == REFERENCE["total_reported_value"]
    assert profile.partial is True
    assert broken in profile.failed_aspects


def test_inv_partial_a_failed_aggregate_is_error_never_empty(params):
    """La distinción que decide si un timeout se lee como "no tiene actividad".

    ``EMPTY`` significa que la fuente confirmó cero filas. Un fallo de
    transporte no confirmó nada, y presentarlo como vacío convertiría una
    consulta fallida en una afirmación sobre el actor.
    """
    profile = profile_for(params, FakeSecopClient(fail={"territories"}))
    status = next(a for a in profile.aspects if a.name == "territories")

    assert status.outcome == Outcome.ERROR
    assert status.reason == OutcomeReason.QUERY_ERROR
    assert profile.territories == ()


def test_inv_partial_a_timeout_is_told_apart_from_any_other_error(params):
    profile = profile_for(
        params, FakeSecopClient(fail={"buyers"}, exc=TimeoutError("read timed out")),
    )
    status = next(a for a in profile.aspects if a.name == "buyers")
    assert status.reason == OutcomeReason.FETCH_TIMEOUT


def test_inv_partial_a_confirmed_zero_is_not_a_failure(params):
    """Cero filas confirmadas por la fuente es información, no avería."""

    class EmptyClient(FakeSecopClient):
        def _dispatch(self, soql: str) -> list[dict]:
            self.calls.append(soql)
            return []

    profile = profile_for(params, EmptyClient())
    assert profile.partial is False
    assert all(a.outcome == Outcome.EMPTY for a in profile.aspects)
    assert profile.contract_count == 0


def test_inv_partial_even_the_core_metrics_failing_leaves_a_usable_profile(params):
    profile = profile_for(params, FakeSecopClient(fail={"core"}))

    assert profile.contract_count is None
    assert profile.has_core_metrics is False
    assert profile.partial is True
    assert profile.buyers  # el resto sigue siendo cierto


def test_a_complete_profile_declares_itself_complete(params):
    profile = profile_for(params)
    assert profile.partial is False
    assert profile.failed_aspects == ()


def test_every_aspect_reports_a_status(params):
    profile = profile_for(params)
    assert {a.name for a in profile.aspects} == set(aspect_names())


# ---------------------------------------------------------------------------
# INV-OBSERVED-NAME — nombres vistos, no razones sociales.
# ---------------------------------------------------------------------------


def test_inv_observed_name_names_keep_their_document_and_type(params):
    """Sin documento y tipo al lado, un nombre observado se leería como identidad."""
    profile = profile_for(params)
    for observed in profile.observed_names:
        assert isinstance(observed, ObservedName)
        assert observed.document == REFERENCE["documento_proveedor"]
        assert observed.document_type == REFERENCE["tipodocproveedor"]


def test_inv_observed_name_one_document_keeps_all_its_written_forms(params):
    """Once escrituras para un NIT, uniones temporales incluidas.

    El perfil las conserva todas en vez de elegir la más frecuente: elegir
    sería exactamente afirmar una razón social.
    """
    profile = profile_for(params)
    names = [n.name for n in profile.observed_names]
    assert len(names) == len(CONTRACT["observed_names_sample"]["names"])
    assert any("UNIÓN TEMPORAL" in n or "UT " in n for n in names)


def test_inv_id_type_mixed_document_types_are_declared_not_merged(params):
    """Dos tipos bajo un mismo número pueden ser dos actores. Se dice.

    Verificado en vivo: 13848603 es cédula de una persona natural y NIT de
    una unión temporal.
    """

    class MixedTypeClient(FakeSecopClient):
        def _dispatch(self, soql: str) -> list[dict]:
            if "GROUP BY proveedor_adjudicado" in soql:
                self.calls.append(soql)
                return [
                    {"proveedor_adjudicado": "PERSONA NATURAL",
                     "documento_proveedor": "13848603",
                     "tipodocproveedor": "Cédula de Ciudadanía", "cnt": "191"},
                    {"proveedor_adjudicado": "UNIÓN TEMPORAL MIGRACIÓN COLOMBIA 2021",
                     "documento_proveedor": "13848603",
                     "tipodocproveedor": "NIT", "cnt": "1"},
                ]
            return super()._dispatch(soql)

    profile = profile_for(params, MixedTypeClient())
    assert profile.has_mixed_document_types is True
    assert set(profile.document_types_observed) == {"Cédula de Ciudadanía", "NIT"}


def test_a_single_document_type_is_not_flagged_as_mixed(params):
    assert profile_for(params).has_mixed_document_types is False


# ---------------------------------------------------------------------------
# El SoQL que el perfil emite.
# ---------------------------------------------------------------------------


def test_every_aggregate_filters_by_the_exact_document(params):
    """INV-EXACT-DOC extendido a los ocho aspectos, no solo a la tabla."""
    client = FakeSecopClient()
    profile_for(params, client)

    assert len(client.calls) == len(ASPECTS)
    for soql in client.calls:
        assert "documento_proveedor = '860002400'" in soql


def test_the_profile_never_projects_personal_columns(params):
    """INV-PII: lo que no se selecciona no puede filtrarse al render."""
    client = FakeSecopClient()
    profile_for(params, client)
    forbidden = (
        "nombre_representante_legal", "identificaci_n_representante_legal",
        "domicilio_representante_legal", "n_mero_de_cuenta",
    )
    for soql in client.calls:
        assert not any(column in soql for column in forbidden)


def test_recent_contracts_are_bounded(params):
    from app.core.actor_profile.aspects import RECENT_CONTRACTS_LIMIT

    client = FakeSecopClient()
    profile_for(params, client)
    recent_soql = next(s for s in client.calls if "SELECT id_contrato" in s)
    assert f"LIMIT {RECENT_CONTRACTS_LIMIT}" in recent_soql


def test_an_unsupported_aspect_is_empty_not_an_error():
    """Un dataset que no publica al proveedor no puede dar nombres observados.

    Eso es una capacidad ausente, no una avería, y confundirlas haría que
    todo perfil sobre esa fuente se declarara roto.
    """
    from app.core.actor_profile.aspects import ASPECTS as _ASPECTS

    aspect = next(a for a in _ASPECTS if a.name == "observed_names")
    assert aspect.build(SoQLBuilder(), SoQLBuilder.PROCESOS_DATASET, {"objeto": ["aseo"]}) == ""


# ---------------------------------------------------------------------------
# Concurrencia acotada.
# ---------------------------------------------------------------------------


def test_the_fan_out_is_bounded_and_reuses_the_turn_budget(params):
    from app.core.actor_profile.assembler import (
        ACTOR_PROFILE_MAX_WORKERS,
        _budget_for_task,
    )
    from app.core.deadline import begin_deadline, end_deadline, now

    assert ACTOR_PROFILE_MAX_WORKERS <= len(ASPECTS)

    tokens = begin_deadline(now())
    try:
        assert _budget_for_task() >= 1
    finally:
        end_deadline(tokens)


def test_aspect_status_reuses_the_repository_outcome_vocabulary():
    """No hay taxonomía paralela: son los mismos valores de outcome.py."""
    status = AspectStatus("buyers")
    assert status.outcome == Outcome.SUCCESS
    assert status.reason == OutcomeReason.RESULTS_FOUND
    assert status.is_available is True
