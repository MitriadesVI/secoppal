"""Pruebas sin red del TelegramProvider real del piloto."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.core.notify import telegram_provider as telegram_module
from app.core.notify.provider import (
    CONFIRMED_EXTERNAL_EFFECT,
    NO_EXTERNAL_EFFECT,
    ProviderRequest,
)
from app.core.notify.telegram_provider import (
    SAFE_AMBIGUOUS,
    TELEGRAM_ALLOWED_CHAT_IDS_ENV,
    TELEGRAM_BOT_TOKEN_ENV,
    TELEGRAM_TIMEOUT_SECONDS,
    TelegramAmbiguousDeliveryError,
    TelegramConfiguration,
    TelegramProvider,
)
from app.core.notify.worker import WorkerRunReport


TOKEN = "999999:super-secret-token"
CHAT_ID = "-1001234567890"
TEXT = "🔔 Radar privado: dato reservado"
NOW = datetime(2026, 7, 16, 15, 0, tzinfo=timezone.utc)


class Response:
    def __init__(self, status_code: int, data=None, *, json_error: Exception | None = None):
        self.status_code = status_code
        self._data = data
        self._json_error = json_error

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._data


class Transport:
    def __init__(self, response: Response | None = None, *, error: Exception | None = None):
        self.response = response
        self.error = error
        self.calls: list[dict] = []

    def __call__(self, path, *, json, timeout):
        self.calls.append({"path": path, "json": json, "timeout": timeout})
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response


@pytest.fixture(autouse=True)
def telegram_environment(monkeypatch):
    monkeypatch.setenv(TELEGRAM_BOT_TOKEN_ENV, TOKEN)
    monkeypatch.setenv(TELEGRAM_ALLOWED_CHAT_IDS_ENV, CHAT_ID)


def request(**overrides) -> ProviderRequest:
    fields = {
        "notification_id": "notification-1",
        "attempt_id": "attempt-1",
        "channel": "telegram",
        "external_idempotency_key": "local-idempotency-only",
        "external_recipient_id": CHAT_ID,
        "text": TEXT,
        "payload_version": "p1b-payload-v2",
    }
    fields.update(overrides)
    return ProviderRequest(**fields)


def provider(response: Response, **kwargs) -> tuple[TelegramProvider, Transport]:
    transport = Transport(response)
    return TelegramProvider(transport=transport, clock=lambda: NOW, **kwargs), transport


def install_fake_https(
    monkeypatch,
    *,
    status: int = 200,
    data=None,
    request_error: Exception | None = None,
):
    instances = []
    body = json.dumps(data if data is not None else {}).encode("utf-8")

    class RawResponse:
        def __init__(self):
            self.status = status

        def read(self):
            return body

    class FakeHTTPSConnection:
        def __init__(self, host, *, timeout, context):
            self.host = host
            self.timeout = timeout
            self.context = context
            self.debuglevel = None
            self.requests = []
            self.closed = False
            instances.append(self)

        def set_debuglevel(self, level):
            self.debuglevel = level

        def request(self, method, path, *, body, headers):
            self.requests.append(
                {"method": method, "path": path, "body": body, "headers": headers}
            )
            if request_error is not None:
                raise request_error

        def getresponse(self):
            return RawResponse()

        def close(self):
            self.closed = True

    monkeypatch.setattr(telegram_module.http.client, "HTTPSConnection", FakeHTTPSConnection)
    return instances


def test_exito_envia_json_minimo_y_confirma_message_id():
    subject, transport = provider(Response(200, {"ok": True, "result": {"message_id": 77}}))

    result = subject.send(request())

    assert result.effect == CONFIRMED_EXTERNAL_EFFECT
    assert result.retryable is False
    assert result.provider_message_id == "77"
    assert len(transport.calls) == 1
    call = transport.calls[0]
    assert call["path"] == f"/bot{TOKEN}/sendMessage"
    assert call["json"] == {"chat_id": CHAT_ID, "text": TEXT}
    assert "parse_mode" not in call["json"]
    assert "external_idempotency_key" not in call["json"]
    assert call["timeout"] == TELEGRAM_TIMEOUT_SECONDS


def test_429_retry_after_valido_usa_reloj_inyectado():
    subject, _ = provider(
        Response(429, {"ok": False, "parameters": {"retry_after": 37}})
    )

    result = subject.send(request())

    assert result.effect == NO_EXTERNAL_EFFECT
    assert result.retryable is True
    assert result.error_class == "rate_limited"
    assert result.retry_after == NOW + timedelta(seconds=37)
    assert result.retry_after.tzinfo is not None


def test_401_es_auth_error_permanente_aunque_el_json_no_sirva():
    subject, _ = provider(Response(401, json_error=ValueError("raw secret body")))
    result = subject.send(request())
    assert result.effect == NO_EXTERNAL_EFFECT
    assert result.retryable is False
    assert result.error_class == "auth_error"


@pytest.mark.parametrize(
    "status,description",
    [
        (403, "Forbidden: bot was blocked by the user"),
        (400, "Bad Request: chat not found"),
    ],
)
def test_destino_inequivocamente_bloqueado_es_invalid_address(status, description):
    subject, _ = provider(Response(status, {"ok": False, "description": description}))
    result = subject.send(request())
    assert result.effect == NO_EXTERNAL_EFFECT
    assert result.retryable is False
    assert result.error_class == "invalid_address"


def test_400_generico_no_se_clasifica_como_direccion_invalida():
    subject, _ = provider(
        Response(400, {"ok": False, "description": "Bad Request: message is too long"})
    )
    with pytest.raises(TelegramAmbiguousDeliveryError, match=f"^{SAFE_AMBIGUOUS}$"):
        subject.send(request())


def test_5xx_es_ambiguo_mediante_excepcion_propia():
    subject, _ = provider(Response(503, {"ok": False, "description": "upstream raw"}))
    with pytest.raises(TelegramAmbiguousDeliveryError, match=f"^{SAFE_AMBIGUOUS}$"):
        subject.send(request())


def test_timeout_es_ambiguo_y_no_conserva_la_excepcion_cruda():
    raw = RuntimeError(f"timeout {TOKEN} {CHAT_ID} {TEXT}")
    transport = Transport(error=raw)
    subject = TelegramProvider(transport=transport)

    with pytest.raises(TelegramAmbiguousDeliveryError) as caught:
        subject.send(request())

    rendered = repr(caught.value) + str(caught.value)
    assert rendered == f"TelegramAmbiguousDeliveryError('{SAFE_AMBIGUOUS}'){SAFE_AMBIGUOUS}"
    for secret in (TOKEN, CHAT_ID, TEXT):
        assert secret not in rendered
    assert caught.value.__context__ is None


def test_json_corrupto_es_ambiguo_sin_cuerpo_crudo():
    subject, _ = provider(
        Response(200, json_error=ValueError(f"corrupt {TOKEN} {CHAT_ID} {TEXT}"))
    )
    with pytest.raises(TelegramAmbiguousDeliveryError) as caught:
        subject.send(request())
    assert str(caught.value) == SAFE_AMBIGUOUS
    assert caught.value.__context__ is None


@pytest.mark.parametrize(
    "data",
    [
        {"ok": True, "result": {}},
        {"ok": True, "result": {"message_id": True}},
        {"ok": True, "result": {"message_id": 0}},
    ],
)
def test_ok_true_sin_message_id_valido_es_ambiguo(data):
    subject, _ = provider(Response(200, data))
    with pytest.raises(TelegramAmbiguousDeliveryError):
        subject.send(request())


def test_allowlist_bloquea_antes_del_transporte_y_sin_chat_id_en_mensaje():
    subject, transport = provider(Response(200, {"ok": True, "result": {"message_id": 1}}))
    blocked = "987654321"

    result = subject.send(request(external_recipient_id=blocked))

    assert transport.calls == []
    assert result.effect == NO_EXTERNAL_EFFECT
    assert result.retryable is False
    assert result.error_class == "invalid_address"
    assert blocked not in (result.error_message_safe or "")
    assert blocked not in repr(result)


def test_token_destinatario_y_texto_no_aparecen_en_repr_o_reporte():
    subject, _ = provider(Response(503, {}))
    configuration = TelegramConfiguration.from_environment()
    report = WorkerRunReport(ambiguous=1, provider_calls=1)
    combined = " ".join((repr(subject), repr(subject._api), repr(configuration), repr(report)))
    for secret in (TOKEN, CHAT_ID, TEXT):
        assert secret not in combined
    assert not hasattr(report, "claim_token")


def test_429_sin_retry_after_valido_es_ambiguo():
    subject, _ = provider(Response(429, {"ok": False, "parameters": {"retry_after": "37"}}))
    with pytest.raises(TelegramAmbiguousDeliveryError):
        subject.send(request())


def test_transporte_https_host_timeout_debug_ruta_body_headers_y_cierre(monkeypatch):
    instances = install_fake_https(
        monkeypatch, data={"ok": True, "result": {"message_id": 81}}
    )
    subject = TelegramProvider(timeout=7.5)

    result = subject.send(request())

    assert result.provider_message_id == "81"
    assert len(instances) == 1
    connection = instances[0]
    assert connection.host == "api.telegram.org"
    assert connection.timeout == 7.5
    assert connection.context.check_hostname is True
    assert connection.context.verify_mode != 0
    assert connection.debuglevel == 0
    assert connection.closed is True
    assert len(connection.requests) == 1
    sent = connection.requests[0]
    assert sent["method"] == "POST"
    assert sent["path"] == f"/bot{TOKEN}/sendMessage"
    assert json.loads(sent["body"].decode("utf-8")) == {
        "chat_id": CHAT_ID,
        "text": TEXT,
    }
    assert sent["headers"] == {"Content-Type": "application/json"}


@pytest.mark.parametrize(
    "status,data,expected_error_class,expected_exception",
    [
        (
            400,
            {"ok": False, "description": "Bad Request: chat not found"},
            "invalid_address",
            None,
        ),
        (401, {"ok": False}, "auth_error", None),
        (
            403,
            {"ok": False, "description": "Forbidden: bot was blocked by the user"},
            "invalid_address",
            None,
        ),
        (
            429,
            {"ok": False, "parameters": {"retry_after": 12}},
            "rate_limited",
            None,
        ),
        (503, {"ok": False}, None, TelegramAmbiguousDeliveryError),
    ],
)
def test_respuestas_http_llegan_al_provider_para_clasificacion(
    monkeypatch, status, data, expected_error_class, expected_exception
):
    instances = install_fake_https(monkeypatch, status=status, data=data)
    subject = TelegramProvider(clock=lambda: NOW)

    if expected_exception is not None:
        with pytest.raises(expected_exception):
            subject.send(request())
    else:
        result = subject.send(request())
        assert result.error_class == expected_error_class

    assert instances[0].closed is True


def test_fallo_de_conexion_cierra_y_no_sobrevive_en_error_seguro(monkeypatch):
    raw = OSError(f"connect failed {TOKEN} {CHAT_ID} {TEXT}")
    instances = install_fake_https(monkeypatch, request_error=raw)
    subject = TelegramProvider()

    with pytest.raises(TelegramAmbiguousDeliveryError) as caught:
        subject.send(request())

    assert instances[0].closed is True
    rendered = str(caught.value) + repr(caught.value)
    for secret in (TOKEN, CHAT_ID, TEXT):
        assert secret not in rendered
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


def test_modulo_no_importa_ni_menciona_httpx():
    source = Path("app/core/notify/telegram_provider.py").read_text(encoding="utf-8")
    assert "httpx" not in source
