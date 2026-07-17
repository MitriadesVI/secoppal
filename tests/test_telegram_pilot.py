"""Pruebas sin red de la herramienta local del piloto Telegram."""
from __future__ import annotations

import json

import pytest

from app.core.notify.telegram_provider import (
    TELEGRAM_ALLOWED_CHAT_IDS_ENV,
    TELEGRAM_BOT_TOKEN_ENV,
    TelegramBotAPI,
    TelegramProvider,
)
from app.core.notify.worker import WorkerRunReport
from scripts import telegram_pilot


TOKEN = "123456:bootstrap-secret"
CHAT_ID = "123456789"


class Response:
    def __init__(self, data, status_code=200):
        self.status_code = status_code
        self._data = data

    def json(self):
        return self._data


class Transport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, url, *, json, timeout):
        self.calls.append((url, json, timeout))
        return self.responses.pop(0)


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    monkeypatch.setenv(TELEGRAM_BOT_TOKEN_ENV, TOKEN)
    monkeypatch.setenv(TELEGRAM_ALLOWED_CHAT_IDS_ENV, CHAT_ID)


def test_probe_muestra_unicamente_username_e_id():
    transport = Transport([Response({"ok": True, "result": {"id": 42, "username": "secoppal_bot", "first_name": "Secret"}})])
    output = []

    telegram_pilot.probe(
        TelegramBotAPI.from_environment(transport=transport), output=output.append
    )

    assert json.loads(output[0]) == {"username": "secoppal_bot", "id": 42}
    assert "Secret" not in output[0]
    assert TOKEN not in output[0]
    assert transport.calls[0][0].endswith("/getMe")


def test_discover_chat_solo_acepta_start_en_chat_privado():
    updates = {
        "ok": True,
        "result": [
            {"message": {"text": "/start", "chat": {"id": 11, "type": "group"}}},
            {"message": {"text": "hola", "chat": {"id": 22, "type": "private"}}},
            {"message": {"text": "/start", "chat": {"id": 33, "type": "private"}}},
        ],
    }
    transport = Transport([Response(updates)])
    output = []

    found = telegram_pilot.discover_chat(
        TelegramBotAPI.from_environment(transport=transport), output=output.append
    )

    assert found == "33"
    assert output == ["33"]
    assert transport.calls[0][0].endswith("/getUpdates")


def test_discover_chat_falla_con_varios_salvo_seleccion_explicita():
    updates = {
        "ok": True,
        "result": [
            {"message": {"text": "/start", "chat": {"id": 33, "type": "private"}}},
            {"message": {"text": "/start@secoppal_bot", "chat": {"id": 44, "type": "private"}}},
        ],
    }
    api = TelegramBotAPI.from_environment(
        transport=Transport([Response(updates), Response(updates)])
    )
    with pytest.raises(telegram_pilot.TelegramPilotError, match="multiple_private_chats"):
        telegram_pilot.discover_chat(api)
    output = []
    assert telegram_pilot.discover_chat(
        api, selected_chat_id="44", output=output.append
    ) == "44"
    assert output == ["44"]


def test_send_test_exige_confirmacion_literal_y_no_llama_transporte():
    transport = Transport([Response({"ok": True, "result": {"message_id": 9}})])
    provider = TelegramProvider(transport=transport)
    output = []

    with pytest.raises(telegram_pilot.TelegramPilotError, match="literal_SEND"):
        telegram_pilot.send_test(
            provider, CHAT_ID, input_fn=lambda: "send", output=output.append
        )

    assert output == [telegram_pilot.TEST_MESSAGE]
    assert transport.calls == []


def test_send_test_con_SEND_envia_solo_el_mensaje_fijo_sin_ledger():
    transport = Transport([Response({"ok": True, "result": {"message_id": 9}})])
    provider = TelegramProvider(transport=transport)

    telegram_pilot.send_test(provider, CHAT_ID, input_fn=lambda: "SEND", output=lambda _: None)

    assert transport.calls[0][1] == {
        "chat_id": CHAT_ID,
        "text": telegram_pilot.TEST_MESSAGE,
    }


def test_send_test_rechaza_chat_no_allowlisted_antes_de_confirmar_o_enviar():
    transport = Transport([Response({"ok": True, "result": {"message_id": 9}})])
    provider = TelegramProvider(transport=transport)
    confirmations = []

    with pytest.raises(telegram_pilot.TelegramPilotError, match="recipient_not_available"):
        telegram_pilot.send_test(
            provider,
            "999",
            input_fn=lambda: confirmations.append(True) or "SEND",
            output=lambda _: None,
        )

    assert confirmations == []
    assert transport.calls == []


def test_run_worker_exige_SEND_y_limita_dispatch_a_un_batch():
    calls = []
    report = WorkerRunReport(claimed=1, authorized=1, sent=1, provider_calls=1)

    class FakeWorker:
        def __init__(self, db_path, providers):
            calls.append((db_path, providers))

        def run_once(self, **kwargs):
            calls.append(kwargs)
            return report

    output = []
    marker_provider = object()
    returned = telegram_pilot.run_worker(
        "pilot.db",
        input_fn=lambda: "SEND",
        output=output.append,
        provider=marker_provider,
        worker_factory=FakeWorker,
    )

    assert returned == report
    assert calls[0] == ("pilot.db", {"telegram": marker_provider})
    assert calls[1] == {
        "dispatch_limit": 1,
        "reconcile_limit": 0,
        "sweep_limit": 0,
    }
    assert output == [repr(report)]
    assert CHAT_ID not in output[0]
    assert TOKEN not in output[0]


def test_run_worker_sin_SEND_no_construye_worker():
    def forbidden(*args, **kwargs):
        raise AssertionError("worker must not be constructed")

    with pytest.raises(telegram_pilot.TelegramPilotError, match="literal_SEND"):
        telegram_pilot.run_worker(
            "pilot.db", input_fn=lambda: "NO", worker_factory=forbidden
        )


def test_run_worker_exige_los_limites_de_aislamiento_explicitos():
    report = WorkerRunReport()

    class StrictWorker:
        def __init__(self, db_path, providers):
            pass

        def run_once(self, *, dispatch_limit, reconcile_limit, sweep_limit):
            assert dispatch_limit == 1
            assert reconcile_limit == 0
            assert sweep_limit == 0
            return report

    assert telegram_pilot.run_worker(
        "pilot.db",
        input_fn=lambda: "SEND",
        output=lambda _: None,
        provider=object(),
        worker_factory=StrictWorker,
    ) is report


def test_cli_no_acepta_token_como_argumento():
    parser = telegram_pilot._parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["probe", "--token", TOKEN])


def test_pruebas_focales_no_usan_transporte_real(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("internet is forbidden in Telegram tests")

    monkeypatch.setattr(
        "app.core.notify.telegram_provider.http.client.HTTPSConnection", forbidden
    )
    # Construir el API con transporte inyectado no toca el transporte real.
    api = TelegramBotAPI.from_environment(
        transport=Transport([Response({"ok": True, "result": {"id": 1, "username": "bot"}})])
    )
    output = []
    telegram_pilot.probe(api, output=output.append)
    assert output
