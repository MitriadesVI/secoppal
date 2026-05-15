from __future__ import annotations

from html import escape

import httpx
from fastapi import FastAPI, Form, Header, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from pydantic import BaseModel, Field

from app.config import get_settings
from app.core.conversation_store import is_reset_command
from app.service import get_workflow

settings = get_settings()
app = FastAPI(title="SECOPAL", version="0.1.0")


class ChatRequest(BaseModel):
    query: str = Field(..., min_length=3)
    channel: str = Field(default="streamlit")


class RateRequest(BaseModel):
    trace_id: str = Field(..., min_length=1)
    rating: int = Field(..., ge=0, le=1)  # 1=good, 0=bad
    comment: str | None = Field(default=None)


@app.get("/")
async def root() -> dict:
    return {
        "name": "SECOPAL",
        "channels": ["streamlit", "telegram", "whatsapp"],
        "status": "ok",
    }


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "llm_enabled": settings.llm_enabled}


@app.post("/chat")
async def chat(request: ChatRequest) -> JSONResponse:
    result = get_workflow().run_query(request.query, channel=request.channel)
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

    result = get_workflow().run_query(body, channel="whatsapp", chat_id=chat_id)
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

    if not text or not chat_id:
        return JSONResponse(content={"ok": True, "ignored": True})

    chat_id_str = str(chat_id)

    # Comando de reset
    if is_reset_command(text):
        get_workflow().conv_store.clear(chat_id_str)
        if settings.telegram_bot_token:
            async with httpx.AsyncClient(timeout=20) as client:
                await client.post(
                    f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage",
                    json={"chat_id": chat_id, "text": "Conversacion reiniciada. Puedes empezar una nueva busqueda."},
                )
        return JSONResponse(content={"ok": True, "reset": True})

    result = get_workflow().run_query(text, channel="telegram", chat_id=chat_id_str)

    if settings.telegram_bot_token:
        async with httpx.AsyncClient(timeout=20) as client:
            await client.post(
                f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage",
                json={"chat_id": chat_id, "text": result["response"]},
            )
        return JSONResponse(content={"ok": True, "delivered": True})

    return JSONResponse(content={"ok": True, "response": result["response"]})


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

