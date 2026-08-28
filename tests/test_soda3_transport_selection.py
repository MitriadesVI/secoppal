"""T1B — selección de transporte por configuración.

Gates cubiertos: CLASSIC_DEFAULT, SODA3_FLAG_DEFAULT_OFF, NO_AUTOMATIC_FALLBACK,
SODAPY_RETAINED.

El veredicto ``GO_INTEGRATION`` autoriza SODA3 como candidato **apagado**. Lo
que estos tests fijan es que encenderlo requiera un acto explícito del entorno
y que una configuración rota no lo encienda —ni lo apague— por accidente.
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


# ── Default: clásico ─────────────────────────────────────────────────────

def test_default_settings_select_classic(monkeypatch):
    monkeypatch.delenv("SECOPPAL_SOCRATA_TRANSPORT", raising=False)
    assert Settings().secoppal_socrata_transport == "classic"
    assert transport_kind_from_settings(Settings()) is TransportKind.CLASSIC
    assert DEFAULT_TRANSPORT_KIND is TransportKind.CLASSIC


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


def test_workflow_default_wires_the_classic_transport(tmp_path):
    workflow = _workflow(tmp_path)
    assert workflow.secop_client.transport_name == TRANSPORT_CLASSIC


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
