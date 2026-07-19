"""Pruebas sin red de la herramienta local del piloto Telegram."""
from __future__ import annotations

import json

import pytest

from app.core.db import connect_sqlite
from app.core.notify.outbox_store import OutboxStore
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


def test_seed_demo_rechaza_la_base_principal():
    with pytest.raises(telegram_pilot.TelegramPilotError, match="refuses_primary_database"):
        telegram_pilot.seed_demo(telegram_pilot.DEFAULT_DB_PATH, output=lambda _: None)


def test_seed_demo_rechaza_una_base_que_ya_existe(tmp_path):
    existing = tmp_path / "not_fresh.db"
    existing.write_bytes(b"")

    with pytest.raises(telegram_pilot.TelegramPilotError, match="requires_fresh_database"):
        telegram_pilot.seed_demo(existing, output=lambda _: None)


def test_seed_demo_exige_exactamente_un_chat_id_permitido(tmp_path, monkeypatch):
    monkeypatch.setenv(TELEGRAM_ALLOWED_CHAT_IDS_ENV, f"{CHAT_ID},999")

    with pytest.raises(
        telegram_pilot.TelegramPilotError, match="requires_exactly_one_allowed_chat_id"
    ):
        telegram_pilot.seed_demo(tmp_path / "fresh.db", output=lambda _: None)


def test_seed_demo_crea_un_batch_pending_sin_llamar_al_proveedor(tmp_path):
    db_path = tmp_path / "telegram_pilot.db"
    output = []

    seed = telegram_pilot.seed_demo(db_path, output=output.append)

    assert seed.radar_id and seed.endpoint_id and seed.preference_id
    assert seed.notification_id
    assert CHAT_ID not in output[0]
    assert TOKEN not in output[0]
    assert set(json.loads(output[0])) == {
        "radar_id", "endpoint_id", "preference_id", "notification_id", "items",
    }

    batch = OutboxStore(db_path).get_batch(seed.notification_id)
    assert batch is not None
    assert batch.status == "pending"
    assert batch.radar_id == seed.radar_id
    assert batch.endpoint_id == seed.endpoint_id


def test_seed_demo_dos_veces_sobre_la_misma_base_falla_por_no_estar_fresca(tmp_path):
    db_path = tmp_path / "telegram_pilot.db"
    telegram_pilot.seed_demo(db_path, output=lambda _: None)

    with pytest.raises(telegram_pilot.TelegramPilotError, match="requires_fresh_database"):
        telegram_pilot.seed_demo(db_path, output=lambda _: None)


def test_preview_rechaza_la_base_principal():
    with pytest.raises(
        telegram_pilot.TelegramPilotError, match="preview_refuses_primary_database"
    ):
        telegram_pilot.preview(
            telegram_pilot.DEFAULT_DB_PATH, "cualquiera", output=lambda _: None
        )


def test_preview_ruta_inexistente_falla_y_no_crea_el_archivo(tmp_path):
    missing = tmp_path / "no_existe.db"

    with pytest.raises(
        telegram_pilot.TelegramPilotError, match="preview_database_not_found"
    ):
        telegram_pilot.preview(missing, "cualquiera", output=lambda _: None)

    assert not missing.exists()


def test_preview_no_construye_ningun_store(tmp_path, monkeypatch):
    db_path = tmp_path / "telegram_pilot.db"
    seed = telegram_pilot.seed_demo(db_path, output=lambda _: None)

    class Bomb:
        def __init__(self, *args, **kwargs):
            raise AssertionError("preview no debe construir ningún Store")

    # Si preview tocara OutboxStore (que corre ensure_schema y abre en RW), esto
    # explotaría. No explota porque preview abre en mode=ro con sqlite crudo.
    monkeypatch.setattr(telegram_pilot, "OutboxStore", Bomb)

    text = telegram_pilot.preview(db_path, seed.notification_id, output=lambda _: None)
    assert telegram_pilot.DEMO_RADAR_NAME in text


def test_preview_no_muta_la_base(tmp_path):
    db_path = tmp_path / "telegram_pilot.db"
    seed = telegram_pilot.seed_demo(db_path, output=lambda _: None)
    before_bytes = db_path.read_bytes()
    before_stat = db_path.stat()

    telegram_pilot.preview(db_path, seed.notification_id, output=lambda _: None)

    after_stat = db_path.stat()
    assert db_path.read_bytes() == before_bytes
    assert after_stat.st_size == before_stat.st_size
    assert after_stat.st_mtime == before_stat.st_mtime


def test_preview_batch_inexistente_falla_seguro(tmp_path):
    db_path = tmp_path / "telegram_pilot.db"
    telegram_pilot.seed_demo(db_path, output=lambda _: None)

    with pytest.raises(telegram_pilot.TelegramPilotError, match="preview_batch_not_found"):
        telegram_pilot.preview(db_path, "no-existe", output=lambda _: None)


def test_preview_renderiza_batch_valido_sin_filtrar_chat_id(tmp_path):
    db_path = tmp_path / "telegram_pilot.db"
    seed = telegram_pilot.seed_demo(db_path, output=lambda _: None)
    output = []

    text = telegram_pilot.preview(db_path, seed.notification_id, output=output.append)

    assert text == output[0]
    assert telegram_pilot.DEMO_RADAR_NAME in text
    assert telegram_pilot.DEMO_ITEM_TITLE in text
    assert CHAT_ID not in text
    assert TOKEN not in text


def test_preview_payload_corrupto_no_filtra_contenido_crudo(tmp_path):
    """Un payload_json ilegible se vuelve un error seguro, sin traceback que
    vuelque el JSON crudo (que puede llevar identificadores del destinatario)."""
    db_path = tmp_path / "telegram_pilot.db"
    seed = telegram_pilot.seed_demo(db_path, output=lambda _: None)
    # Corromper el payload con un secreto plausible, con un writer aparte.
    secret = "SECRETO-DEL-DESTINATARIO-9999"
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "UPDATE radar_notification_batches SET payload_json = ? "
            "WHERE notification_id = ?",
            (f'{{"roto": "{secret}"', seed.notification_id),
        )
    finally:
        conn.close()

    output = []
    with pytest.raises(telegram_pilot.TelegramPilotError) as exc:
        telegram_pilot.preview(db_path, seed.notification_id, output=output.append)

    assert "payload_unreadable" in str(exc.value)
    assert secret not in str(exc.value)
    assert output == []


def test_cli_seed_demo_requiere_db():
    parser = telegram_pilot._parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["seed-demo"])


def test_cli_preview_requiere_db_y_notification_id():
    parser = telegram_pilot._parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["preview", "--db", "x.db"])


def _existing_isolated_db(tmp_path):
    """Un archivo que existe y no es la base principal — basta para pasar los
    guards de aislamiento cuando el worker se inyecta como fake."""
    db_path = tmp_path / "telegram_pilot.db"
    db_path.write_bytes(b"")
    return db_path


def test_cli_run_worker_requiere_db():
    parser = telegram_pilot._parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["run-worker"])


def test_run_worker_rechaza_la_base_principal_antes_de_todo():
    """La base principal se rechaza antes de leer SEND, construir provider o
    worker. Ni el input_fn ni el factory deben tocarse."""
    def forbidden_factory(*args, **kwargs):
        raise AssertionError("no debe construirse el worker")

    def forbidden_input():
        raise AssertionError("no debe pedirse confirmación")

    with pytest.raises(
        telegram_pilot.TelegramPilotError, match="run_worker_refuses_primary_database"
    ):
        telegram_pilot.run_worker(
            telegram_pilot.DEFAULT_DB_PATH,
            input_fn=forbidden_input,
            output=lambda _: None,
            provider=object(),
            worker_factory=forbidden_factory,
        )


def test_run_worker_base_inexistente_rechazada_sin_crearla(tmp_path):
    missing = tmp_path / "no_existe.db"

    def forbidden_factory(*args, **kwargs):
        raise AssertionError("no debe construirse el worker")

    with pytest.raises(
        telegram_pilot.TelegramPilotError, match="run_worker_database_not_found"
    ):
        telegram_pilot.run_worker(
            missing,
            input_fn=lambda: "SEND",
            output=lambda _: None,
            provider=object(),
            worker_factory=forbidden_factory,
        )

    assert not missing.exists()


def test_run_worker_exige_SEND_y_limita_dispatch_a_un_batch(tmp_path):
    db_path = _existing_isolated_db(tmp_path)
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
        db_path,
        input_fn=lambda: "SEND",
        output=output.append,
        provider=marker_provider,
        worker_factory=FakeWorker,
    )

    assert returned == report
    assert calls[0] == (db_path, {"telegram": marker_provider})
    assert calls[1] == {
        "dispatch_limit": 1,
        "reconcile_limit": 0,
        "sweep_limit": 0,
    }
    assert output == [repr(report)]
    assert CHAT_ID not in output[0]
    assert TOKEN not in output[0]


def test_run_worker_sin_SEND_no_construye_worker(tmp_path):
    db_path = _existing_isolated_db(tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("worker must not be constructed")

    with pytest.raises(telegram_pilot.TelegramPilotError, match="literal_SEND"):
        telegram_pilot.run_worker(
            db_path, input_fn=lambda: "NO", worker_factory=forbidden
        )


def test_run_worker_base_aislada_pasa_exactamente_uno_cero_cero(tmp_path):
    db_path = _existing_isolated_db(tmp_path)
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
        db_path,
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
