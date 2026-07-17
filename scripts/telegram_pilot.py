#!/usr/bin/env python3
"""Bootstrap y ejecución manual del piloto Telegram del Radar.

Uso desde la raíz del repositorio::

    python -m scripts.telegram_pilot probe
    python -m scripts.telegram_pilot discover-chat
    python -m scripts.telegram_pilot discover-chat --select-chat-id CHAT_ID
    python -m scripts.telegram_pilot send-test --chat-id CHAT_ID
    python -m scripts.telegram_pilot run-worker --db data/secoppal.db

``send-test`` y ``run-worker`` leen una línea de stdin y solo continúan si es
exactamente ``SEND``. Ninguna operación acepta el token como argumento. TG-P0
solo intenta despachar un candidato: no reconcilia huérfanos ni barre expirados.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from app.core.notify.provider import (
    CONFIRMED_EXTERNAL_EFFECT,
    ProviderRequest,
)
from app.core.notify.telegram_provider import (
    SAFE_INVALID_ADDRESS,
    TelegramBotAPI,
    TelegramError,
    TelegramProvider,
    decode_json_object,
)
from app.core.notify.worker import NotificationWorker, WorkerRunReport


TEST_MESSAGE = "SECOPPAL Radar · prueba de conexión"
DEFAULT_DB_PATH = Path("data/secoppal.db")

SAFE_CONFIRMATION_REQUIRED = "literal_SEND_confirmation_required"
SAFE_DISCOVERY_FAILED = "telegram_private_start_not_found"
SAFE_MULTIPLE_CHATS = "multiple_private_chats_require_explicit_selection"
SAFE_SELECTION_FAILED = "selected_chat_not_found"
SAFE_PROBE_FAILED = "telegram_probe_failed"
SAFE_TEST_FAILED = "telegram_test_not_confirmed"


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


def run_worker(
    db_path: str | Path,
    *,
    input_fn: Callable[[], str] = input,
    output: Callable[[str], Any] = print,
    provider: TelegramProvider | None = None,
    worker_factory: Callable[..., NotificationWorker] = NotificationWorker,
) -> WorkerRunReport:
    """Despacha como máximo un candidato, sin reconciliar ni barrer el ledger."""
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

    worker = subparsers.add_parser(
        "run-worker",
        help=(
            "intenta despachar un candidato tras SEND; no reconcilia huérfanos "
            "ni barre expirados"
        ),
    )
    worker.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
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
