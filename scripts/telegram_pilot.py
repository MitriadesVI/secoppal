#!/usr/bin/env python3
"""Bootstrap y ejecución manual del piloto Telegram del Radar.

Uso desde la raíz del repositorio::

    python -m scripts.telegram_pilot probe
    python -m scripts.telegram_pilot discover-chat
    python -m scripts.telegram_pilot discover-chat --select-chat-id CHAT_ID
    python -m scripts.telegram_pilot send-test --chat-id CHAT_ID
    python -m scripts.telegram_pilot seed-demo --db data/telegram_pilot.db
    python -m scripts.telegram_pilot preview --db data/telegram_pilot.db \\
        --notification-id NOTIFICATION_ID
    python -m scripts.telegram_pilot run-worker --db data/telegram_pilot.db

``send-test`` y ``run-worker`` leen una línea de stdin y solo continúan si es
exactamente ``SEND``. Ninguna operación acepta el token como argumento. TG-P0
solo intenta despachar un candidato: no reconcilia huérfanos ni barre expirados.

``seed-demo`` (TG-P1) materializa un único radar, endpoint y batch de prueba
sobre una base aislada — nunca sobre ``data/secoppal.db`` — y no envía nada.
``preview`` abre esa base en modo estrictamente de solo lectura (``mode=ro``, sin
construir ningún store) y renderiza el texto del batch sin llamar al proveedor.
``run-worker`` también exige una base aislada explícita y rechaza
``data/secoppal.db`` antes de pedir confirmación o construir nada.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.core.notify.endpoint_store import EndpointStore
from app.core.notify.identity_store import IdentityStore
from app.core.notify.masking import Masker
from app.core.notify.outbox_store import BatchEventRef, OutboxStore
from app.core.notify.preference_store import PreferenceStore
from app.core.notify.provider import (
    CONFIRMED_EXTERNAL_EFFECT,
    ProviderRequest,
)
from app.core.notify.render import PAYLOAD_VERSION_V2, render_payload
from app.core.notify.telegram_provider import (
    SAFE_INVALID_ADDRESS,
    TelegramBotAPI,
    TelegramConfiguration,
    TelegramError,
    TelegramProvider,
    decode_json_object,
)
from app.core.notify.worker import NotificationWorker, WorkerRunReport
from app.core.radar.store import RadarStore


TEST_MESSAGE = "SECOPPAL Radar · prueba de conexión"
DEFAULT_DB_PATH = Path("data/secoppal.db")

DEMO_RADAR_NAME = "SECOPPAL Radar · Piloto (demo)"
DEMO_ITEM_TITLE = "Oportunidad de prueba — base aislada, no representa una contratación real"
DEMO_ENTITY = "Piloto SECOPPAL Radar"
DEMO_OWNER_KEY = "telegram-pilot-demo"
DEMO_PRINCIPAL_EMAIL = "telegram-pilot-demo@local.invalid"
# No protege ningún secreto: solo deriva el hash de deduplicación del endpoint
# dentro de una base de usar-y-tirar que nunca es data/secoppal.db.
DEMO_MASKING_KEY = b"telegram-pilot-demo-key"
DEMO_BATCH_LIFETIME = timedelta(hours=24)

SAFE_CONFIRMATION_REQUIRED = "literal_SEND_confirmation_required"
SAFE_DISCOVERY_FAILED = "telegram_private_start_not_found"
SAFE_MULTIPLE_CHATS = "multiple_private_chats_require_explicit_selection"
SAFE_SELECTION_FAILED = "selected_chat_not_found"
SAFE_PROBE_FAILED = "telegram_probe_failed"
SAFE_TEST_FAILED = "telegram_test_not_confirmed"
SAFE_DEMO_MAIN_DB_REFUSED = "telegram_pilot_demo_refuses_primary_database"
SAFE_DEMO_DB_NOT_FRESH = "telegram_pilot_demo_requires_fresh_database"
SAFE_DEMO_CHAT_ID_INVALID = "telegram_pilot_demo_requires_exactly_one_allowed_chat_id"
SAFE_PREVIEW_NOT_FOUND = "telegram_pilot_preview_batch_not_found"
SAFE_PREVIEW_MAIN_DB_REFUSED = "telegram_pilot_preview_refuses_primary_database"
SAFE_PREVIEW_DB_NOT_FOUND = "telegram_pilot_preview_database_not_found"
SAFE_PREVIEW_PAYLOAD_UNREADABLE = "telegram_pilot_preview_payload_unreadable"
SAFE_WORKER_MAIN_DB_REFUSED = "telegram_pilot_run_worker_refuses_primary_database"
SAFE_WORKER_DB_NOT_FOUND = "telegram_pilot_run_worker_database_not_found"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TelegramPilotError(Exception):
    """Fallo operativo seguro del comando local."""


def _response_data(api: TelegramBotAPI, method: str, payload: dict | None = None) -> dict:
    response = api.request(method, payload)
    try:
        status = response.status_code
        data = decode_json_object(response)
    except Exception:
        # Ningún detalle de la respuesta remota llega a la consola.
        raise TelegramPilotError(SAFE_PROBE_FAILED) from None
    if status != 200 or data.get("ok") is not True:
        raise TelegramPilotError(SAFE_PROBE_FAILED)
    return data


def probe(
    api: TelegramBotAPI,
    *,
    output: Callable[[str], Any] = print,
) -> tuple[str, int]:
    data = _response_data(api, "getMe")
    result = data.get("result")
    if not isinstance(result, dict):
        raise TelegramPilotError(SAFE_PROBE_FAILED)
    username = result.get("username")
    bot_id = result.get("id")
    if not isinstance(username, str) or not username or type(bot_id) is not int:
        raise TelegramPilotError(SAFE_PROBE_FAILED)
    output(json.dumps({"username": username, "id": bot_id}, ensure_ascii=False))
    return username, bot_id


def _private_start_chat_ids(data: dict) -> list[str]:
    updates = data.get("result")
    if not isinstance(updates, list):
        raise TelegramPilotError(SAFE_DISCOVERY_FAILED)
    found: set[str] = set()
    for update in updates:
        if not isinstance(update, dict):
            continue
        message = update.get("message")
        if not isinstance(message, dict):
            continue
        text = message.get("text")
        chat = message.get("chat")
        if not isinstance(text, str) or not isinstance(chat, dict):
            continue
        command = text.strip().split(maxsplit=1)[0].casefold()
        if command != "/start" and not command.startswith("/start@"):
            continue
        chat_id = chat.get("id")
        if chat.get("type") == "private" and type(chat_id) is int:
            found.add(str(chat_id))
    return sorted(found)


def discover_chat(
    api: TelegramBotAPI,
    *,
    selected_chat_id: str | None = None,
    output: Callable[[str], Any] = print,
) -> str:
    data = _response_data(
        api, "getUpdates", {"allowed_updates": ["message"], "timeout": 0}
    )
    chat_ids = _private_start_chat_ids(data)
    if not chat_ids:
        raise TelegramPilotError(SAFE_DISCOVERY_FAILED)
    if selected_chat_id is not None:
        if selected_chat_id not in chat_ids:
            raise TelegramPilotError(SAFE_SELECTION_FAILED)
        chosen = selected_chat_id
    elif len(chat_ids) == 1:
        chosen = chat_ids[0]
    else:
        raise TelegramPilotError(SAFE_MULTIPLE_CHATS)
    output(chosen)
    return chosen


def _confirmed(input_fn: Callable[[], str]) -> bool:
    try:
        return input_fn() == "SEND"
    except (EOFError, KeyboardInterrupt):
        return False


def send_test(
    provider: TelegramProvider,
    chat_id: str,
    *,
    input_fn: Callable[[], str] = input,
    output: Callable[[str], Any] = print,
) -> None:
    if not provider.is_allowed_recipient(chat_id):
        raise TelegramPilotError(SAFE_INVALID_ADDRESS)
    output(TEST_MESSAGE)
    if not _confirmed(input_fn):
        raise TelegramPilotError(SAFE_CONFIRMATION_REQUIRED)
    result = provider.send(
        ProviderRequest(
            notification_id="telegram-pilot-test",
            attempt_id="telegram-pilot-test",
            channel="telegram",
            external_idempotency_key="telegram-pilot-test",
            external_recipient_id=chat_id,
            text=TEST_MESSAGE,
            payload_version="telegram-pilot-test-v1",
        )
    )
    if result.effect != CONFIRMED_EXTERNAL_EFFECT:
        raise TelegramPilotError(SAFE_TEST_FAILED)


@dataclass(frozen=True)
class DemoSeed:
    """Identificadores opacos de lo que ``seed_demo`` creó. Sin chat_id."""

    radar_id: str
    endpoint_id: str
    preference_id: str
    notification_id: str


def seed_demo(
    db_path: str | Path,
    *,
    clock: Callable[[], datetime] = _utc_now,
    output: Callable[[str], Any] = print,
) -> DemoSeed:
    """Materializa radar + endpoint + preferencia + un único batch de prueba.

    Nunca escribe en ``data/secoppal.db`` ni reutiliza una base existente: el
    piloto sintético es de usar y tirar, y mezclarlo con datos reales
    invalidaría ambos. No llama al proveedor — solo dispatch/``run-worker``
    hace eso, tras la confirmación ``SEND`` habitual.
    """
    target = Path(db_path)
    if target.resolve() == DEFAULT_DB_PATH.resolve():
        raise TelegramPilotError(SAFE_DEMO_MAIN_DB_REFUSED)
    if target.exists():
        raise TelegramPilotError(SAFE_DEMO_DB_NOT_FRESH)

    config = TelegramConfiguration.from_environment()
    if len(config.allowed_chat_ids) != 1:
        raise TelegramPilotError(SAFE_DEMO_CHAT_ID_INVALID)
    chat_id = next(iter(config.allowed_chat_ids))

    identity = IdentityStore(target)
    endpoints = EndpointStore(target, masker=Masker.from_key(DEMO_MASKING_KEY))
    preferences = PreferenceStore(target)
    radars = RadarStore(target)
    outbox = OutboxStore(target)

    principal = identity.create_principal(DEMO_PRINCIPAL_EMAIL)
    identity.link_owner_key(DEMO_OWNER_KEY, principal)
    endpoint = endpoints.create_endpoint(
        principal_id=principal, channel="telegram", address=chat_id
    )
    endpoints.mark_verified(endpoint, external_recipient_id=chat_id)
    radar = radars.create_radar(
        name=DEMO_RADAR_NAME, owner=DEMO_OWNER_KEY, query_frame_json="{}"
    )
    preference = preferences.create_preference(radar_id=radar, endpoint_id=endpoint)
    preferences.set_enabled(preference, True)

    now = clock()
    created_at = now.isoformat()
    payload = {
        "version": PAYLOAD_VERSION_V2,
        "principal_id": principal,
        "endpoint_id": endpoint,
        "radar_id": radar,
        "radar_name": DEMO_RADAR_NAME,
        "channel": "telegram",
        "timezone": "America/Bogota",
        "created_at": created_at,
        "max_items": 10,
        "items": [
            {
                "kind": "new",
                "title": DEMO_ITEM_TITLE,
                "dataset": "procesos",
                "entity": DEMO_ENTITY,
            }
        ],
        "total_items": 1,
        "links_missing": 1,
    }
    result = outbox.create_batch(
        principal_id=principal,
        endpoint_id=endpoint,
        radar_id=radar,
        payload_json=payload,
        events=[BatchEventRef("telegram-pilot-demo-event", 1)],
        created_at=created_at,
        expires_at=(now + DEMO_BATCH_LIFETIME).isoformat(),
    )

    seed = DemoSeed(
        radar_id=radar,
        endpoint_id=endpoint,
        preference_id=preference,
        notification_id=result.notification_id,
    )
    output(
        json.dumps(
            {
                "radar_id": seed.radar_id,
                "endpoint_id": seed.endpoint_id,
                "preference_id": seed.preference_id,
                "notification_id": seed.notification_id,
                "items": 1,
            },
            ensure_ascii=False,
        )
    )
    return seed


def _read_payload_json_readonly(target: Path, notification_id: str) -> str | None:
    """Lee ``payload_json`` de un batch sin poder escribir ni un byte.

    Abre la base con ``mode=ro`` (URI de SQLite): no crea el archivo si no
    existe, no aplica pragmas de escritura, no corre ``ensure_schema`` y no
    materializa esquema. No es ``OutboxStore``: construir cualquier store abriría
    la base en lectura-escritura y le tocaría el journal. La conexión se cierra
    siempre, y el render ocurre *después* de cerrarla.
    """
    uri = f"file:{target}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        row = conn.execute(
            "SELECT payload_json FROM radar_notification_batches "
            "WHERE notification_id = ?",
            (notification_id,),
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row is not None else None


def preview(
    db_path: str | Path,
    notification_id: str,
    *,
    output: Callable[[str], Any] = print,
) -> str:
    """Renderiza el texto congelado de un batch sin enviarlo ni tocar la red.

    Estrictamente de solo lectura: rechaza la base principal, no crea archivos y
    no construye ningún store. Un payload ilegible se convierte en un error
    seguro y estable —nunca en un traceback que vuelque el JSON crudo, que puede
    llevar identificadores del destinatario.
    """
    target = Path(db_path)
    if target.resolve() == DEFAULT_DB_PATH.resolve():
        raise TelegramPilotError(SAFE_PREVIEW_MAIN_DB_REFUSED)
    if not target.exists():
        raise TelegramPilotError(SAFE_PREVIEW_DB_NOT_FOUND)

    payload_json = _read_payload_json_readonly(target, notification_id)
    if payload_json is None:
        raise TelegramPilotError(SAFE_PREVIEW_NOT_FOUND)

    try:
        rendered = render_payload(json.loads(payload_json))
    except Exception:
        # ``from None`` corta el encadenamiento: el mensaje crudo de json/render
        # cita el payload, y ese payload no debe aparecer en consola.
        raise TelegramPilotError(SAFE_PREVIEW_PAYLOAD_UNREADABLE) from None

    output(rendered.text)
    return rendered.text


def run_worker(
    db_path: str | Path,
    *,
    input_fn: Callable[[], str] = input,
    output: Callable[[str], Any] = print,
    provider: TelegramProvider | None = None,
    worker_factory: Callable[..., NotificationWorker] = NotificationWorker,
) -> WorkerRunReport:
    """Despacha como máximo un candidato, sin reconciliar ni barrer el ledger.

    Exige una base aislada explícita: rechaza ``data/secoppal.db`` y una ruta
    inexistente **antes** de leer la confirmación, construir el proveedor o el
    worker. El orden importa —negarse temprano evita tanto pedir un ``SEND`` que
    nunca se honraría como instanciar ``TelegramProvider`` (que lee el entorno).
    """
    target = Path(db_path)
    if target.resolve() == DEFAULT_DB_PATH.resolve():
        raise TelegramPilotError(SAFE_WORKER_MAIN_DB_REFUSED)
    if not target.exists():
        raise TelegramPilotError(SAFE_WORKER_DB_NOT_FOUND)
    if not _confirmed(input_fn):
        raise TelegramPilotError(SAFE_CONFIRMATION_REQUIRED)
    selected_provider = provider or TelegramProvider()
    worker = worker_factory(db_path, {"telegram": selected_provider})
    report = worker.run_once(
        dispatch_limit=1,
        reconcile_limit=0,
        sweep_limit=0,
    )
    output(repr(report))
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Piloto manual Telegram de SECOPPAL Radar")
    subparsers = parser.add_subparsers(dest="operation", required=True)
    subparsers.add_parser("probe", help="valida el bot mediante getMe")

    discover = subparsers.add_parser(
        "discover-chat", help="descubre un chat privado que haya enviado /start"
    )
    discover.add_argument("--select-chat-id")

    test = subparsers.add_parser(
        "send-test", help="envía la prueba fija tras leer la confirmación SEND"
    )
    test.add_argument("--chat-id", required=True)

    seed = subparsers.add_parser(
        "seed-demo",
        help=(
            "crea radar+endpoint+batch de prueba en una base aislada nueva; "
            "nunca en data/secoppal.db"
        ),
    )
    seed.add_argument("--db", type=Path, required=True)

    preview_parser = subparsers.add_parser(
        "preview", help="muestra el texto de un batch sin enviarlo"
    )
    preview_parser.add_argument("--db", type=Path, required=True)
    preview_parser.add_argument("--notification-id", required=True)

    worker = subparsers.add_parser(
        "run-worker",
        help=(
            "intenta despachar un candidato tras SEND sobre una base aislada "
            "(p. ej. data/telegram_pilot.db); rechaza data/secoppal.db, no "
            "reconcilia huérfanos ni barre expirados"
        ),
    )
    worker.add_argument("--db", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.operation == "probe":
            probe(TelegramBotAPI.from_environment())
        elif args.operation == "discover-chat":
            discover_chat(
                TelegramBotAPI.from_environment(),
                selected_chat_id=args.select_chat_id,
            )
        elif args.operation == "send-test":
            send_test(TelegramProvider(), args.chat_id)
        elif args.operation == "seed-demo":
            seed_demo(args.db)
        elif args.operation == "preview":
            preview(args.db, args.notification_id)
        elif args.operation == "run-worker":
            run_worker(args.db)
        else:  # pragma: no cover - argparse cierra el vocabulario.
            raise AssertionError("unknown_telegram_pilot_operation")
    except (TelegramError, TelegramPilotError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
