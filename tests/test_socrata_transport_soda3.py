"""T1B — contrato HTTP del transporte SODA3 y sus errores tipados.

Gates cubiertos: SODA3_POST_CONTRACT, SOQL_IDENTITY, TIMEOUT_SEMANTICS.

El contrato probado aquí es el que el discovery congeló en
``scripts/audits/soda3_probe.py`` / ``soda3_runtime_canary.py`` /
``soda3_h4_policy_replay.py``. Ninguna llamada sale a la red: el cliente
``httpx`` está inyectado.
"""
from __future__ import annotations

import httpx
import pytest

from app.core.secop_client import SecopClient
from app.core.socrata_transport import TRANSPORT_SODA3, Soda3OrderingSpecifier
from app.core.socrata_transport.errors import (
    Soda3BadQueryError,
    Soda3ContractViolationError,
    Soda3DecodeError,
    Soda3NetworkError,
    Soda3NotFoundError,
    Soda3RateLimitError,
    Soda3TimeoutError,
    Soda3TransientServerError,
    TransportErrorCategory,
)
from app.core.socrata_transport.soda3 import Soda3HttpTransport
from app.core.workflow.helpers import _is_timeout_exception


class FakeHttpClient:
    """Cliente httpx de mentira: registra la request y devuelve/lanza lo pedido."""

    def __init__(self, outcome):
        self.outcome = outcome
        self.calls: list[dict] = []

    def post(self, url, *, json, timeout):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def _transport(outcome):
    fake = FakeHttpClient(outcome)
    return Soda3HttpTransport(
        domain="www.datos.gov.co", app_token="tok", http_client=fake,
    ), fake


def _rows(payload):
    return httpx.Response(200, json=payload)


SOQL = (
    "SELECT id_contrato, valor_del_contrato WHERE unaccent(UPPER(objeto)) "
    "LIKE '%AIRE ACONDICIONADO%' ORDER BY fecha_de_firma DESC LIMIT 50"
)


# ── El contrato POST exacto ──────────────────────────────────────────────

def test_query_posts_to_the_v3_query_endpoint():
    transport, fake = _transport(_rows([{"id_contrato": "1"}]))
    transport.query(
        "jbjy-vk9h", SOQL,
        timeout_seconds=8, ordering_specifier=Soda3OrderingSpecifier.TOTAL,
    )
    assert fake.calls[0]["url"] == (
        "https://www.datos.gov.co/api/v3/views/jbjy-vk9h/query.json"
    )


def test_payload_matches_the_frozen_discovery_contract():
    transport, fake = _transport(_rows([]))
    transport.query(
        "p6dx-8zbt", SOQL,
        timeout_seconds=3, ordering_specifier=Soda3OrderingSpecifier.NO_PRESERVE,
    )
    assert fake.calls[0]["json"] == {
        "query": SOQL,
        "timeout": 3,
        "includeSynthetic": False,
        "orderingSpecifier": "no_preserve",
    }
    assert fake.calls[0]["timeout"] == 3


def test_dataset_id_is_the_one_received():
    transport, fake = _transport(_rows([]))
    transport.count(
        "p6dx-8zbt", "SELECT count(*)",
        timeout_seconds=3, ordering_specifier=Soda3OrderingSpecifier.TOTAL,
    )
    assert "/views/p6dx-8zbt/query.json" in fake.calls[0]["url"]


def test_soql_travels_byte_for_byte_without_rewriting():
    weird = "SELECT * WHERE nombre LIKE '%BOGOTÁ D.C.%' AND x IS NOT NULL LIMIT 200"
    transport, fake = _transport(_rows([]))
    transport.query(
        "jbjy-vk9h", weird,
        timeout_seconds=8, ordering_specifier=Soda3OrderingSpecifier.TOTAL,
    )
    assert fake.calls[0]["json"]["query"] == weird


def test_app_token_header_is_sent_and_absent_when_unset():
    with_token = Soda3HttpTransport(domain="d", app_token="secret", http_client=object())
    without = Soda3HttpTransport(domain="d", app_token=None, http_client=object())
    assert with_token._headers()["X-App-Token"] == "secret"
    assert "X-App-Token" not in without._headers()


def test_transport_name_is_soda3():
    transport, _ = _transport(_rows([]))
    assert transport.name == TRANSPORT_SODA3


def test_valid_response_yields_rows_ready_for_normalization():
    payload = [{"id_contrato": "CO1.NTC.7", "valor_del_contrato": "1,500"}]
    transport, _ = _transport(_rows(payload))
    rows = transport.query(
        "jbjy-vk9h", SOQL,
        timeout_seconds=8, ordering_specifier=Soda3OrderingSpecifier.TOTAL,
    )
    # El transporte NO normaliza: eso es autoridad de SecopClient._normalize_row.
    assert rows == payload


def test_secop_client_normalizes_soda3_rows():
    transport, _ = _transport(
        _rows([{"id_contrato": "CO1.NTC.7", "valor_del_contrato": "1,500"}])
    )
    client = SecopClient("www.datos.gov.co", "tok", timeout=8, transport=transport)
    rows = client.query("jbjy-vk9h", SOQL)
    assert rows[0]["valor_del_contrato"] == 1500.0
    assert "OpportunityDetail" in rows[0]["urlproceso"]


def test_client_timeout_reaches_the_payload():
    transport, fake = _transport(_rows([]))
    SecopClient("d", None, timeout=5, transport=transport).query("ds", "SELECT 1")
    assert fake.calls[0]["json"]["timeout"] == 5
    assert fake.calls[0]["timeout"] == 5


# ── Errores tipados: nada se degrada a lista vacía ───────────────────────

@pytest.mark.parametrize("status,error_cls,category", [
    (400, Soda3BadQueryError, TransportErrorCategory.BAD_QUERY),
    (429, Soda3RateLimitError, TransportErrorCategory.RATE_LIMIT),
    (404, Soda3NotFoundError, TransportErrorCategory.NOT_FOUND),
    (503, Soda3TransientServerError, TransportErrorCategory.TRANSIENT_SERVER),
])
def test_http_errors_raise_their_category(status, error_cls, category):
    transport, _ = _transport(httpx.Response(status, text="nope"))
    with pytest.raises(error_cls) as caught:
        transport.query(
            "ds", SOQL,
            timeout_seconds=3, ordering_specifier=Soda3OrderingSpecifier.TOTAL,
        )
    assert caught.value.category == category
    assert caught.value.http_status == status


def test_decode_error_is_typed():
    transport, _ = _transport(httpx.Response(200, text="<html>not json</html>"))
    with pytest.raises(Soda3DecodeError):
        transport.query(
            "ds", SOQL,
            timeout_seconds=3, ordering_specifier=Soda3OrderingSpecifier.TOTAL,
        )


def test_unexpected_shape_is_a_contract_violation():
    transport, _ = _transport(_rows({"not": "a list"}))
    with pytest.raises(Soda3ContractViolationError):
        transport.query(
            "ds", SOQL,
            timeout_seconds=3, ordering_specifier=Soda3OrderingSpecifier.TOTAL,
        )


def test_network_failure_is_typed_and_keeps_its_cause():
    boom = httpx.ConnectError("dns")
    transport, _ = _transport(boom)
    with pytest.raises(Soda3NetworkError) as caught:
        transport.query(
            "ds", SOQL,
            timeout_seconds=3, ordering_specifier=Soda3OrderingSpecifier.TOTAL,
        )
    assert caught.value.__cause__ is boom


# ── Semántica de timeout: la que decide si H4 se arma ────────────────────

def test_soda3_timeout_is_a_real_timeout_error():
    cause = httpx.ReadTimeout("budget")
    transport, _ = _transport(cause)
    with pytest.raises(Soda3TimeoutError) as caught:
        transport.query(
            "ds", SOQL,
            timeout_seconds=3, ordering_specifier=Soda3OrderingSpecifier.TOTAL,
        )
    assert isinstance(caught.value, TimeoutError)
    assert caught.value.__cause__ is cause


def test_timeout_survives_the_secop_client_wrapper_as_recoverable():
    """El workflow reconoce el timeout recorriendo la cadena de excepciones.

    SecopClient envuelve todo fallo en RuntimeError; si el timeout SODA3 no
    llegara como TimeoutError por la cadena, H4 nunca se armaría y un turno
    recuperable se perdería.
    """
    transport, _ = _transport(httpx.ReadTimeout("budget"))
    client = SecopClient("d", None, timeout=3, transport=transport)
    with pytest.raises(RuntimeError) as caught:
        client.query("ds", SOQL)
    assert _is_timeout_exception(caught.value) is True


@pytest.mark.parametrize("status", [429, 400, 401, 404, 503])
def test_http_errors_are_never_mistaken_for_timeouts(status):
    """Un 429 no es un timeout: convertirlo en uno armaría un H4 indebido."""
    transport, _ = _transport(httpx.Response(status, text="nope"))
    client = SecopClient("d", None, timeout=3, transport=transport)
    with pytest.raises(RuntimeError) as caught:
        client.query("ds", SOQL)
    assert _is_timeout_exception(caught.value) is False


def test_max_retries_of_one_means_a_single_attempt():
    transport, fake = _transport(httpx.ReadTimeout("budget"))
    client = SecopClient("d", None, timeout=3, transport=transport)
    with pytest.raises(RuntimeError):
        client.query("ds", SOQL)
    assert len(fake.calls) == 1
