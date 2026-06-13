from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from html import escape

import httpx
from fastapi import FastAPI, Form, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from pydantic import BaseModel, Field

from app.config import get_settings
from app.core.conversation_store import is_reset_command
from app.service import get_workflow

settings = get_settings()
app = FastAPI(title="SECOPPAL", version="0.1.0")
logger = logging.getLogger(__name__)

# Telegram rechaza mensajes > 4096 chars; margen por code-units UTF-16 (emoji=2).
_TELEGRAM_CHUNK_LIMIT = 4000


async def _send_telegram(chat_id: int | str, text: str) -> bool:
    """Envía ``text`` a ``chat_id`` vía Bot API, troceado y revisando errores.

    Devuelve True solo si todos los fragmentos se entregaron. Sin token
    configurado → False (no se intenta).
    """
    token = settings.telegram_bot_token
    if not token:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    ok = True
    async with httpx.AsyncClient(timeout=20) as client:
        for chunk in _chunk_message(text, _TELEGRAM_CHUNK_LIMIT):
            try:
                resp = await client.post(url, json={"chat_id": chat_id, "text": chunk})
                if resp.status_code >= 400:
                    ok = False
                    logger.warning(
                        "Telegram sendMessage falló (%s): %s",
                        resp.status_code,
                        resp.text[:200],
                    )
            except httpx.HTTPError as exc:
                ok = False
                logger.warning("Telegram sendMessage error de red: %s", exc)
    return ok


class ChatRequest(BaseModel):
    query: str = Field(..., min_length=3)
    channel: str = Field(default="streamlit")
    chat_id: str | None = Field(default=None)
    user_name: str | None = Field(default=None)
    user_email: str | None = Field(default=None)
    is_anonymous: bool = Field(default=True)


class RateRequest(BaseModel):
    trace_id: str = Field(..., min_length=1)
    rating: int = Field(..., ge=0, le=1)  # 1=good, 0=bad
    comment: str | None = Field(default=None)


@app.get("/")
async def root() -> dict:
    return {
        "name": "SECOPPAL",
        "channels": ["streamlit", "telegram", "whatsapp"],
        "status": "ok",
    }


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "llm_enabled": settings.llm_enabled}


@app.post("/chat")
async def chat(request: ChatRequest) -> JSONResponse:
    identity = _build_user_identity(
        channel=request.channel,
        channel_user_key=request.chat_id or request.user_email,
        name=request.user_name,
        email=request.user_email,
        is_anonymous=request.is_anonymous and not request.user_email,
    )
    result = get_workflow().run_query(
        request.query,
        channel=request.channel,
        chat_id=request.chat_id,
        user_identity=identity,
    )
    return JSONResponse(content=result)


@app.post("/rate")
async def rate(request: RateRequest) -> JSONResponse:
    success = get_workflow().rate_query(request.trace_id, request.rating, request.comment)
    if not success:
        raise HTTPException(status_code=404, detail="Trace not found")
    return JSONResponse(content={"ok": True, "trace_id": request.trace_id})


@app.get("/feedback/stats")
async def feedback_stats() -> JSONResponse:
    stats = get_workflow().get_feedback_stats()
    return JSONResponse(content=stats)


@app.post("/webhooks/twilio/whatsapp")
async def twilio_whatsapp_webhook(
    body: str = Form(..., alias="Body"),
    sender: str | None = Form(default=None, alias="From"),
) -> Response:
    chat_id = sender or None

    if chat_id and is_reset_command(body):
        get_workflow().conv_store.clear(chat_id)
        payload = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<Response><Message>Conversacion reiniciada. Puedes empezar una nueva busqueda.</Message></Response>"
        )
        return Response(content=payload, media_type="application/xml")

    result = await run_in_threadpool(
        get_workflow().run_query,
        body,
        channel="whatsapp",
        chat_id=chat_id,
        user_identity=_build_user_identity(
            channel="whatsapp",
            channel_user_key=chat_id,
            is_anonymous=True,
        ),
    )
    payload = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        f"<Response><Message>{escape(result['response'])}</Message></Response>"
    )
    return Response(content=payload, media_type="application/xml")


@app.post("/webhooks/telegram")
async def telegram_webhook(
    update: dict,
    secret_token: str | None = Header(default=None, alias="X-Telegram-Bot-Api-Secret-Token"),
) -> JSONResponse:
    if settings.telegram_webhook_secret and secret_token != settings.telegram_webhook_secret:
        raise HTTPException(status_code=401, detail="Invalid Telegram secret")

    message = update.get("message") or update.get("edited_message") or {}
    text = message.get("text")
    chat_id = message.get("chat", {}).get("id")
    sender = message.get("from", {}) or {}

    if not text or not chat_id:
        return JSONResponse(content={"ok": True, "ignored": True})

    chat_id_str = str(chat_id)

    # Comando /start — onboarding
    if text.strip() == "/start":
        welcome = (
            "👋 ¡Hola! Soy SECOPPAL, tu asistente para buscar oportunidades "
            "y contratos en SECOP II (Colombia).\n\n"
            "Puedes preguntarme cosas como:\n"
            "• contratos de obra en Bogotá\n"
            "• procesos abiertos en INVIAS\n"
            "• contratos mayores a 500M en Antioquia\n\n"
            "¿Qué te gustaría buscar?"
        )
        await _send_telegram(chat_id, welcome)
        return JSONResponse(content={"ok": True, "start": True})

    # Comando de reset
    if is_reset_command(text):
        get_workflow().conv_store.clear(chat_id_str)
        await _send_telegram(
            chat_id, "Conversacion reiniciada. Puedes empezar una nueva busqueda."
        )
        return JSONResponse(content={"ok": True, "reset": True})

    telegram_name = " ".join(
        part for part in (sender.get("first_name"), sender.get("last_name")) if part
    ) or sender.get("username")
    # run_query es síncrono y puede tardar (red SECOP); fuera del event loop.
    result = await run_in_threadpool(
        get_workflow().run_query,
        text,
        channel="telegram",
        chat_id=chat_id_str,
        user_identity=_build_user_identity(
            channel="telegram",
            channel_user_key=chat_id_str,
            name=telegram_name,
            is_anonymous=True,
        ),
    )

    if settings.telegram_bot_token:
        delivered = await _send_telegram(chat_id, result["response"])
        return JSONResponse(content={"ok": True, "delivered": delivered})

    return JSONResponse(content={"ok": True, "response": result["response"]})


# ---------------------------------------------------------------------------
# OpenWA — gateway self-hosted de WhatsApp Web (whatsapp-web.js).
# Patrón JSON-in / REST-out, espeja Telegram (NO TwiML como Twilio).
# Swagger de la instancia: http://localhost:2785/api/docs
# ---------------------------------------------------------------------------

# WhatsApp Web admite mensajes largos; troceamos por robustez/legibilidad.
_WA_CHUNK_LIMIT = 3500


def _chunk_message(text: str, limit: int = _WA_CHUNK_LIMIT) -> list[str]:
    """Parte un texto en fragmentos <= limit respetando saltos de línea."""
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current = ""
    for line in text.split("\n"):
        # Línea individual más larga que el límite: trocear duro.
        while len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(line[:limit])
            line = line[limit:]
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > limit:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def _parse_openwa_message(update: dict) -> dict | None:
    """Normaliza un evento entrante de OpenWA, o None si no es procesable.

    El evento envuelve el mensaje en ``data`` (IncomingMessage de
    whatsapp-web.js: id/from/to/chatId/body/type/timestamp/fromMe/isGroup).
    Confirmado contra src/engine/interfaces/whatsapp-engine.interface.ts y
    src/engine/adapters/whatsapp-web-js.adapter.ts del repo OpenWA.
    """
    data = update.get("data")
    if not isinstance(data, dict):
        # Tolera que envíen el mensaje sin envoltura.
        data = update if ("body" in update or "from" in update) else {}
    chat_id = data.get("chatId") or data.get("from")
    text = data.get("body")
    if not chat_id or not text:
        return None
    return {
        "chat_id": str(chat_id),
        "text": str(text),
        "from_me": bool(data.get("fromMe")),
        "is_group": bool(data.get("isGroup")),
        "type": data.get("type") or "chat",
    }


def _verify_openwa_signature(raw_body: bytes, signature: str | None) -> bool:
    """HMAC-SHA256 del cuerpo crudo contra el secret configurado.

    OpenWA firma con ``'sha256=' + hmac_sha256(secret, body)`` y lo envía en
    el header ``X-OpenWA-Signature`` (confirmado en docs/04-security-design.md
    y docs/06-api-specification.md). Sin secret configurado → no se exige
    firma (modo prueba). Tolera el prefijo ``sha256=``.
    """
    secret = settings.openwa_webhook_secret
    if not secret:
        return True
    if not signature:
        return False
    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    provided = signature.split("=", 1)[-1].strip()
    return hmac.compare_digest(expected, provided)


# Dedup de entregas: OpenWA reintenta hasta WEBHOOK_MAX_RETRIES (3) con la
# misma idempotencyKey → sin esto, una query lenta dispararía SECOP y la
# respuesta por duplicado. TTL simple en memoria (suficiente para 1 worker).
_OPENWA_SEEN: dict[str, float] = {}
_OPENWA_SEEN_TTL = 24 * 60 * 60  # 24 h, igual que la guía de OpenWA


def _openwa_already_processed(key: str | None) -> bool:
    if not key:
        return False
    now = time.time()
    # Limpieza perezosa de entradas viejas.
    for k, ts in list(_OPENWA_SEEN.items()):
        if now - ts > _OPENWA_SEEN_TTL:
            del _OPENWA_SEEN[k]
    if key in _OPENWA_SEEN:
        return True
    _OPENWA_SEEN[key] = now
    return False


async def _send_openwa(chat_id: str, text: str) -> bool:
    """Envía ``text`` a ``chat_id`` vía REST de OpenWA, troceando si hace falta."""
    if not settings.openwa_session_id:
        return False
    url = (
        f"{settings.openwa_base_url.rstrip('/')}"
        f"/api/sessions/{settings.openwa_session_id}/messages/send-text"
    )
    headers = {}
    if settings.openwa_api_key:
        headers["X-Api-Key"] = settings.openwa_api_key
    ok = True
    async with httpx.AsyncClient(timeout=20) as client:
        for chunk in _chunk_message(text):
            try:
                resp = await client.post(
                    url, json={"chatId": chat_id, "text": chunk}, headers=headers
                )
                if resp.status_code >= 400:
                    ok = False
            except httpx.HTTPError:
                ok = False
    return ok


@app.post("/webhooks/openwa")
async def openwa_webhook(request: Request) -> JSONResponse:
    raw = await request.body()
    # Header case-insensitive en Starlette; OpenWA usa X-OpenWA-Signature.
    signature = request.headers.get("X-OpenWA-Signature")
    if not _verify_openwa_signature(raw, signature):
        raise HTTPException(status_code=401, detail="Invalid OpenWA signature")

    try:
        update = json.loads(raw or b"{}")
    except json.JSONDecodeError:
        return JSONResponse(content={"ok": True, "ignored": True})

    event = update.get("event")
    if event and event not in ("message.received", "message"):
        return JSONResponse(content={"ok": True, "ignored": True})

    # Idempotencia: ignora reintentos con la misma clave de entrega.
    idem_key = request.headers.get("X-OpenWA-Idempotency-Key") or update.get("idempotencyKey")
    if _openwa_already_processed(idem_key):
        return JSONResponse(content={"ok": True, "status": "duplicate_ignored"})

    msg = _parse_openwa_message(update)
    if msg is None or msg["from_me"] or msg["is_group"] or msg["type"] != "chat":
        return JSONResponse(content={"ok": True, "ignored": True})
    chat_id, text = msg["chat_id"], msg["text"]

    if is_reset_command(text):
        get_workflow().conv_store.clear(chat_id)
        await _send_openwa(
            chat_id, "Conversacion reiniciada. Puedes empezar una nueva busqueda."
        )
        return JSONResponse(content={"ok": True, "reset": True})

    result = await run_in_threadpool(
        get_workflow().run_query,
        text,
        channel="whatsapp",
        chat_id=chat_id,
        user_identity=_build_user_identity(
            channel="whatsapp",
            channel_user_key=chat_id,
            is_anonymous=True,
        ),
    )
    delivered = await _send_openwa(chat_id, result["response"])
    return JSONResponse(content={"ok": True, "delivered": delivered})


@app.get("/examples")
async def examples() -> PlainTextResponse:
    return PlainTextResponse(
        "\n".join(
            [
                "licitaciones de mantenimiento vial en Atlantico por mas de 500 millones abiertas",
                "procesos de la gobernacion del Atlantico",
                "contratos del SENA en Bogota mayores a 200 millones",
            ]
        )
    )


def _build_user_identity(
    channel: str,
    channel_user_key: str | None,
    name: str | None = None,
    email: str | None = None,
    is_anonymous: bool = True,
) -> dict | None:
    if not channel_user_key and not email:
        return None
    return {
        "channel": channel,
        "channel_user_key": channel_user_key or email,
        "name": name.strip() if name else None,
        "email": email.strip().lower() if email else None,
        "is_anonymous": is_anonymous,
    }
