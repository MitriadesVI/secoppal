"""Selección de transporte por configuración tras el cutover SODA3.

Gates cubiertos: SODA3_DEFAULT, CLASSIC_EXPLICIT_ROLLBACK,
NO_AUTOMATIC_FALLBACK, SODAPY_RETAINED.
"""
from __future__ import annotations

import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.socrata_transport import (
    DEFAULT_TRANSPORT_KIND,
    TRANSPORT_CLASSIC,
    TRANSPORT_SODA3,
    ClassicSocrataTransport,
    TransportKind,
    build_transport,
    parse_transport_kind,
    transport_kind_from_settings,
)
from app.core.socrata_transport.soda3 import Soda3HttpTransport


# ── Default: SODA3; classic: rollback explícito ──────────────────────────

def test_default_settings_select_soda3(monkeypatch):
    monkeypatch.delenv("SECOPPAL_SOCRATA_TRANSPORT", raising=False)
    assert Settings().secoppal_socrata_transport == "soda3"
    assert transport_kind_from_settings(Settings()) is TransportKind.SODA3
    assert DEFAULT_TRANSPORT_KIND is TransportKind.SODA3


def test_explicit_classic_selects_classic(monkeypatch):
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", "classic")
    assert transport_kind_from_settings(Settings()) is TransportKind.CLASSIC


def test_explicit_soda3_selects_soda3(monkeypatch):
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", "soda3")
    assert transport_kind_from_settings(Settings()) is TransportKind.SODA3


@pytest.mark.parametrize("bad", ["shadow", "SODA4", "", "classico", "true"])
def test_invalid_value_fails_closed_with_an_explicit_error(bad, monkeypatch):
    """Fail closed: ni selecciona SODA3 ni cae al clásico en silencio."""
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", bad)
    with pytest.raises(ValueError) as caught:
        transport_kind_from_settings(Settings())
    message = str(caught.value)
    assert "SECOPPAL_SOCRATA_TRANSPORT" in message
    assert "classic" in message and "soda3" in message


def test_only_two_transports_exist_in_v1():
    """Sin shadow, sin sampling, sin cascada automática."""
    assert {kind.value for kind in TransportKind} == {TRANSPORT_CLASSIC, TRANSPORT_SODA3}


def test_parse_is_case_and_whitespace_tolerant_but_not_permissive():
    assert parse_transport_kind("  SODA3 ") is TransportKind.SODA3
    with pytest.raises(ValueError):
        parse_transport_kind("soda3 preferred")


# ── La factory construye lo que se le pide ───────────────────────────────

def test_factory_builds_classic_over_the_given_client():
    sentinel = object()
    transport = build_transport(
        TransportKind.CLASSIC,
        domain="d", app_token=None, timeout_seconds=15, classic_client=sentinel,
    )
    assert isinstance(transport, ClassicSocrataTransport)
    assert transport.raw_client is sentinel


def test_factory_builds_soda3():
    transport = build_transport(
        TransportKind.SODA3, domain="d", app_token="t", timeout_seconds=15,
    )
    assert isinstance(transport, Soda3HttpTransport)
    assert transport.name == TRANSPORT_SODA3


# ── El workflow real ─────────────────────────────────────────────────────

def _workflow(tmp_path):
    return SecopalWorkflow(Settings(
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "secoppal.db",
    ))


def test_workflow_default_wires_the_soda3_transport(tmp_path, monkeypatch):
    monkeypatch.delenv("SECOPPAL_SOCRATA_TRANSPORT", raising=False)
    workflow = _workflow(tmp_path)
    assert workflow.secop_client.transport_name == TRANSPORT_SODA3
    assert isinstance(workflow.secop_client.transport, Soda3HttpTransport)


def test_workflow_with_classic_flag_never_constructs_a_soda3_client(tmp_path, monkeypatch):
    """CERO llamadas al transporte SODA3 con el flag en clásico."""
    constructions = []
    import app.core.socrata_transport.soda3 as soda3_module

    original = soda3_module.Soda3HttpTransport

    class Tracking(original):
        def __init__(self, **kwargs):
            constructions.append(kwargs)
            super().__init__(**kwargs)

    monkeypatch.setattr(soda3_module, "Soda3HttpTransport", Tracking)
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", "classic")

    workflow = _workflow(tmp_path)

    assert constructions == []
    assert workflow.secop_client.transport_name == TRANSPORT_CLASSIC


def test_workflow_with_soda3_flag_wires_soda3(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", "soda3")
    workflow = _workflow(tmp_path)
    assert workflow.secop_client.transport_name == TRANSPORT_SODA3
    assert isinstance(workflow.secop_client.transport, Soda3HttpTransport)


def test_workflow_with_invalid_flag_refuses_to_build(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", "soda3_preferred")
    with pytest.raises(ValueError):
        _workflow(tmp_path)


def test_derived_clients_keep_the_selected_transport(tmp_path, monkeypatch):
    """Acotar el presupuesto no degrada SODA3 a clásico ni abre un Socrata."""
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", "soda3")
    client = _workflow(tmp_path).secop_client
    derived = client.derived_for_timeout(3)
    assert derived.transport_name == TRANSPORT_SODA3
    assert derived.transport is client.transport  # comparte pool, no lo duplica
    assert derived.timeout == 3


# ── sodapy sigue disponible ──────────────────────────────────────────────

def test_sodapy_is_still_installed_and_usable():
    from sodapy import Socrata  # noqa: F401

    from app.core.secop_client import SecopClient

    client = SecopClient(domain="www.datos.gov.co", app_token=None)
    assert client.transport_name == TRANSPORT_CLASSIC
    assert isinstance(client.transport, ClassicSocrataTransport)


# ── T1C — el clásico conserva su semántica de timeout derivado ───────────
#
# Regresión que cierra este bloque: T1B pasaba SIEMPRE un transporte explícito
# al SecopClient, también en clásico. Un ClassicSocrataTransport explícito se
# propaga tal cual por `_respawn`, y el timeout clásico está horneado en el
# objeto `Socrata` que ese transporte envuelve. Resultado: derived.timeout
# valía 3 mientras la llamada real seguía usando el Socrata de 60 s — y el
# contrato de deadlines/H4 depende de que ese acote sea efectivo.

class FakeSocrata:
    """Socrata de mentira que recuerda con qué timeout fue construido."""

    instances: list["FakeSocrata"] = []

    def __init__(self, domain, app_token=None, timeout=None):
        self.domain = domain
        self.app_token = app_token
        self.timeout = timeout
        self.calls: list[tuple] = []
        FakeSocrata.instances.append(self)

    def get(self, dataset_id, query=None):
        self.calls.append((dataset_id, query))
        return []


@pytest.fixture
def fake_socrata(monkeypatch):
    FakeSocrata.instances = []
    monkeypatch.setattr("app.core.secop_client.Socrata", FakeSocrata)
    return FakeSocrata


def test_classic_workflow_does_not_receive_an_explicit_transport(
    tmp_path, monkeypatch, fake_socrata,
):
    """El clásico usa el camino implícito legacy: sin transporte explícito.

    Es la condición estructural de la que dependen las dos propiedades de
    abajo; comprobarla aquí hace que el diagnóstico sea legible cuando alguna
    de ellas falle.
    """
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", "classic")
    client = _workflow(tmp_path).secop_client
    assert client._explicit_transport is None
    assert isinstance(client.client, FakeSocrata)
    assert client.transport_name == TRANSPORT_CLASSIC


def test_classic_derived_client_gets_its_own_bounded_socrata(
    tmp_path, monkeypatch, fake_socrata,
):
    """El acote por deadline debe llegar al transporte, no solo al atributo.

    No basta con ``derived.timeout == 3``: el timeout clásico vive dentro del
    objeto ``Socrata``, así que la prueba es que la llamada real salga por un
    ``Socrata`` construido con 3.
    """
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", "classic")
    original = _workflow(tmp_path).secop_client
    original_socrata = original.client
    original_timeout = original.timeout

    derived = original.derived_for_timeout(3)

    # Un transporte propio, no el compartido del original.
    assert derived.client is not original_socrata
    assert derived.transport.raw_client is not original.transport.raw_client

    # Y el timeout EFECTIVO —el del Socrata que emite— es el acotado.
    assert derived.client.timeout == 3
    derived.query("ds", "SELECT * LIMIT 1")
    emitting = [s for s in FakeSocrata.instances if s.calls]
    assert [s.timeout for s in emitting] == [3]

    # El original no se mutó: sigue con su presupuesto y sin llamadas.
    assert original.timeout == original_timeout
    assert original_socrata.timeout == original_timeout
    assert original_socrata.calls == []


def test_classic_workflow_client_can_still_be_replaced_to_cut_the_network(
    tmp_path, monkeypatch, fake_socrata,
):
    """Reasignar ``.client`` sigue redirigiendo el transporte del workflow.

    ``tests/test_audit_runner_isolation.py`` corta la red exactamente así. Con
    un transporte explícito de por medio la sustitución quedaba ignorada y el
    assert ``transport_calls == []`` pasaba trivialmente mientras la corrida
    hacía red real: un falso verde, no un fallo visible.
    """
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", "classic")
    client = _workflow(tmp_path).secop_client
    cut = FakeSocrata("offline", timeout=1)

    client.client = cut
    client.query("ds", "SELECT * LIMIT 1")

    assert cut.calls == [("ds", "SELECT * LIMIT 1")]
    assert client.transport.raw_client is cut


def test_soda3_derived_clients_reuse_the_transport_and_open_no_socrata(
    tmp_path, monkeypatch, fake_socrata,
):
    """SODA3 sí comparte transporte: su timeout viaja por llamada, no horneado."""
    monkeypatch.setenv("SECOPPAL_SOCRATA_TRANSPORT", "soda3")
    client = _workflow(tmp_path).secop_client

    derived = client.derived_for_timeout(3)

    assert derived.transport is client.transport  # mismo pool, a propósito
    assert derived.timeout == 3 and derived.transport_name == TRANSPORT_SODA3
    assert FakeSocrata.instances == []  # ningún Socrata abierto en la ruta SODA3
