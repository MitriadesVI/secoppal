"""T1A — la costura de transporte no cambia el comportamiento clásico.

Gates cubiertos: TRANSPORT_SEAM, CLASSIC_DEFAULT, CLASSIC_PARITY.

El transporte clásico es el default de construcción de ``SecopClient`` y
emite exactamente la misma llamada sodapy que antes de la costura:
``client.get(dataset_id, query=soql)``. Todo lo demás —normalización, lectura
del entero del count, retries, derivación por timeout— sigue donde estaba.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.core.secop_client import SecopClient
from app.core.socrata_transport import (
    TRANSPORT_CLASSIC,
    ClassicSocrataTransport,
    Soda3OrderingSpecifier,
)


# ── El default de construcción es clásico ────────────────────────────────

def test_injected_client_yields_classic_transport():
    secop = SecopClient("test.gov", "token", client=MagicMock())
    assert secop.transport_name == TRANSPORT_CLASSIC
    assert isinstance(secop.transport, ClassicSocrataTransport)


def test_transport_wraps_the_live_client_attribute():
    """Reasignar ``.client`` sigue redirigiendo el transporte.

    Los harnesses de auditoría cortan la red con
    ``workflow.secop_client.client = NoNetworkTransport()``. Si la costura
    congelara el cliente en el constructor, esa sustitución dejaría de surtir
    efecto y la suite haría red real sin que ningún assert se enterara.
    """
    original = MagicMock()
    original.get.return_value = []
    secop = SecopClient("test.gov", "token", client=original)

    replacement = MagicMock()
    replacement.get.return_value = [{"id": "9"}]
    secop.client = replacement

    assert secop.query("ds", "SELECT *") == [{"id": "9", "urlproceso": ""}]
    original.get.assert_not_called()


# ── Paridad: la llamada sodapy es la de siempre ──────────────────────────

@pytest.mark.parametrize("operation", ["query", "count", "aggregate"])
def test_every_operation_issues_the_same_sodapy_call(operation):
    client = MagicMock()
    client.get.return_value = [{"count": "7"}] if operation == "count" else []
    secop = SecopClient("test.gov", "token", client=client)

    getattr(secop, operation)("ds", "SELECT whatever")

    client.get.assert_called_once_with("ds", query="SELECT whatever")


def test_classic_transport_ignores_ordering_specifier():
    """El eje de ordering no existe en la API clásica: no viaja al cliente."""
    client = MagicMock()
    client.get.return_value = []
    transport = ClassicSocrataTransport(client)

    transport.query(
        "ds", "SELECT *",
        timeout_seconds=3,
        ordering_specifier=Soda3OrderingSpecifier.NO_PRESERVE,
    )

    assert client.get.call_args.kwargs == {"query": "SELECT *"}


def test_soql_reaches_the_transport_byte_for_byte():
    soql = "SELECT a, b WHERE unaccent(UPPER(x)) LIKE '%Ñ%' ORDER BY a DESC LIMIT 50"
    client = MagicMock()
    client.get.return_value = []
    SecopClient("test.gov", "token", client=client).query("ds", soql)
    assert client.get.call_args.args[0] == "ds"
    assert client.get.call_args.kwargs["query"] == soql


# ── Semántica preservada: normalización, count, retries, derivación ──────

def test_query_still_normalizes_rows():
    client = MagicMock()
    client.get.return_value = [{"id_contrato": "CO1.NTC.7", "valor_del_contrato": "1,500"}]
    rows = SecopClient("test.gov", "token", client=client).query("jbjy-vk9h", "SELECT *")
    assert rows[0]["valor_del_contrato"] == 1500.0
    assert "OpportunityDetail" in rows[0]["urlproceso"]


def test_count_still_reads_the_integer():
    client = MagicMock()
    client.get.return_value = [{"count": "42"}]
    assert SecopClient("test.gov", "token", client=client).count("ds", "SELECT count(*)") == 42


def test_count_of_empty_result_is_zero():
    client = MagicMock()
    client.get.return_value = []
    assert SecopClient("test.gov", "token", client=client).count("ds", "SELECT count(*)") == 0


def test_aggregate_has_no_retries():
    client = MagicMock()
    client.get.side_effect = TimeoutError("read timed out")
    with pytest.raises(TimeoutError):
        SecopClient("test.gov", "token", client=client).aggregate("ds", "SELECT count(*)")
    assert client.get.call_count == 1


def test_derived_clients_keep_the_classic_transport():
    client = MagicMock()
    shared = SecopClient("test.gov", "token", timeout=30, client=client)
    derived = shared.derived_for_timeout(3)
    assert derived is not shared
    assert derived.timeout == 3 and shared.timeout == 30
    assert derived.transport_name == TRANSPORT_CLASSIC
    assert derived.client is client
