"""Pruebas del webhook de Telegram tras robustecer: chunking a 4096,
errores de entrega no silenciados, y run_query fuera del event loop.
"""
from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

import app.main as main


class _FakeConvStore:
    def __init__(self) -> None:
        self.cleared: list[str] = []

    def clear(self, chat_id: str) -> None:
        self.cleared.append(chat_id)


class _FakeWorkflow:
    def __init__(self) -> None:
        self.conv_store = _FakeConvStore()
        self.calls: list[dict] = []

    def run_query(self, query, channel=None, chat_id=None, user_identity=None):
        self.calls.append({"query": query, "channel": channel, "chat_id": chat_id})
        return {"response": f"respuesta a: {query}"}


@pytest.fixture
def fake_workflow(monkeypatch):
    wf = _FakeWorkflow()
    monkeypatch.setattr(main, "get_workflow", lambda: wf)
    return wf


@pytest.fixture
def captured_sends(monkeypatch):
    sent: list[tuple] = []

    async def _fake_send(chat_id, text) -> bool:
        sent.append((chat_id, text))
        return True

    monkeypatch.setattr(main, "_send_telegram", _fake_send)
    return sent


@pytest.fixture(autouse=True)
def _isolate_secret(monkeypatch):
    """Aísla del .env real: sin secret salvo que el test lo fije."""
    monkeypatch.setattr(main.settings, "telegram_webhook_secret", None)


@pytest.fixture
def with_token(monkeypatch):
    monkeypatch.setattr(main.settings, "telegram_bot_token", "TESTTOKEN")


@pytest.fixture
def client():
    return TestClient(main.app)


def _update(text, chat_id=999, **chat_extra):
    return {"message": {"text": text, "chat": {"id": chat_id}, "from": {"first_name": "Ana"}}}


# --- flujo del endpoint -----------------------------------------------------


def test_start_sends_welcome(client, fake_workflow, captured_sends, with_token):
    resp = client.post("/webhooks/telegram", json=_update("/start"))
    assert resp.json() == {"ok": True, "start": True}
    assert len(captured_sends) == 1 and "SECOPPAL" in captured_sends[0][1]
    assert fake_workflow.calls == []


def test_reset_clears_and_replies(client, fake_workflow, captured_sends, with_token):
    resp = client.post("/webhooks/telegram", json=_update("/reset"))
    assert resp.json() == {"ok": True, "reset": True}
    assert fake_workflow.conv_store.cleared == ["999"]
    assert len(captured_sends) == 1


def test_query_runs_and_delivers(client, fake_workflow, captured_sends, with_token):
    resp = client.post("/webhooks/telegram", json=_update("contratos de obra"))
    assert resp.json() == {"ok": True, "delivered": True}
    assert fake_workflow.calls[0]["channel"] == "telegram"
    assert fake_workflow.calls[0]["chat_id"] == "999"
    assert captured_sends == [(999, "respuesta a: contratos de obra")]


def test_delivered_reflects_send_failure(client, fake_workflow, with_token, monkeypatch):
    async def _failing_send(chat_id, text):
        return False

    monkeypatch.setattr(main, "_send_telegram", _failing_send)
    resp = client.post("/webhooks/telegram", json=_update("algo"))
    assert resp.json() == {"ok": True, "delivered": False}


def test_no_token_returns_response_inline(client, fake_workflow, monkeypatch):
    monkeypatch.setattr(main.settings, "telegram_bot_token", None)
    resp = client.post("/webhooks/telegram", json=_update("algo"))
    assert resp.json() == {"ok": True, "response": "respuesta a: algo"}


def test_ignores_message_without_text(client, fake_workflow, captured_sends):
    resp = client.post("/webhooks/telegram", json={"message": {"chat": {"id": 1}}})
    assert resp.json() == {"ok": True, "ignored": True}
    assert fake_workflow.calls == []


def test_rejects_bad_secret(client, monkeypatch):
    monkeypatch.setattr(main.settings, "telegram_webhook_secret", "sek")
    resp = client.post(
        "/webhooks/telegram",
        json=_update("hola"),
        headers={"X-Telegram-Bot-Api-Secret-Token": "wrong"},
    )
    assert resp.status_code == 401


# --- _send_telegram: chunking y manejo de errores ---------------------------


class _FakeResp:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = "err" if status_code >= 400 else "ok"


class _FakeClient:
    """Reemplaza httpx.AsyncClient; registra los posts y devuelve status fijo."""

    posts: list[dict] = []
    status = 200

    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, json=None):
        type(self).posts.append(json)
        return _FakeResp(type(self).status)


def test_send_telegram_chunks_long_message(monkeypatch, with_token):
    _FakeClient.posts = []
    _FakeClient.status = 200
    monkeypatch.setattr(main.httpx, "AsyncClient", _FakeClient)
    long_text = "x" * 9000  # > 2 * 4000
    ok = asyncio.run(main._send_telegram(123, long_text))
    assert ok is True
    assert len(_FakeClient.posts) == 3  # 4000 + 4000 + 1000
    assert all(len(p["text"]) <= main._TELEGRAM_CHUNK_LIMIT for p in _FakeClient.posts)


def test_send_telegram_reports_failure(monkeypatch, with_token):
    _FakeClient.posts = []
    _FakeClient.status = 400
    monkeypatch.setattr(main.httpx, "AsyncClient", _FakeClient)
    ok = asyncio.run(main._send_telegram(123, "hola"))
    assert ok is False
