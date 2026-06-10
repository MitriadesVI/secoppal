"""Pruebas del adaptador OpenWA (webhook de WhatsApp Web).

Cubre el hueco "cero tests de la capa de canal". No tocan red real:
``get_workflow`` y el sender ``_send_openwa`` se monkeypatchean.
"""
from __future__ import annotations

import hashlib
import hmac
import json

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
    sent: list[tuple[str, str]] = []

    async def _fake_send(chat_id: str, text: str) -> bool:
        sent.append((chat_id, text))
        return True

    monkeypatch.setattr(main, "_send_openwa", _fake_send)
    return sent


@pytest.fixture
def client():
    return TestClient(main.app)


# --- chunker ---------------------------------------------------------------


def test_chunk_short_message_single_chunk():
    assert main._chunk_message("hola") == ["hola"]


def test_chunk_preserves_content_across_newlines():
    text = "\n".join(f"linea {i} " + "x" * 50 for i in range(200))
    chunks = main._chunk_message(text, limit=300)
    assert all(len(c) <= 300 for c in chunks)
    assert "\n".join(chunks) == text  # reconstrucción exacta


def test_chunk_hard_splits_oversized_line():
    long = "y" * 1000
    chunks = main._chunk_message(long, limit=300)
    assert all(len(c) <= 300 for c in chunks)
    assert "".join(chunks) == long


# --- parser de payload -----------------------------------------------------


def test_parse_openwa_message_real_shape():
    update = {
        "event": "message.received",
        "data": {"chatId": "57300@c.us", "from": "57300@c.us", "body": "hola", "type": "chat"},
    }
    msg = main._parse_openwa_message(update)
    assert msg == {
        "chat_id": "57300@c.us",
        "text": "hola",
        "from_me": False,
        "is_group": False,
        "type": "chat",
    }


def test_parse_openwa_message_marks_from_me_and_group():
    update = {"data": {"chatId": "x@g.us", "body": "eco", "fromMe": True, "isGroup": True}}
    msg = main._parse_openwa_message(update)
    assert msg["from_me"] is True and msg["is_group"] is True


def test_parse_openwa_message_none_without_text():
    assert main._parse_openwa_message({"data": {"from": "x@c.us"}}) is None


# --- HMAC ------------------------------------------------------------------


def test_signature_not_required_without_secret(monkeypatch):
    monkeypatch.setattr(main.settings, "openwa_webhook_secret", None)
    assert main._verify_openwa_signature(b"{}", None) is True


def test_signature_valid_and_invalid(monkeypatch):
    monkeypatch.setattr(main.settings, "openwa_webhook_secret", "s3cr3t")
    raw = b'{"a":1}'
    good = hmac.new(b"s3cr3t", raw, hashlib.sha256).hexdigest()
    assert main._verify_openwa_signature(raw, good) is True
    assert main._verify_openwa_signature(raw, "sha256=" + good) is True  # prefijo GitHub
    assert main._verify_openwa_signature(raw, "deadbeef") is False
    assert main._verify_openwa_signature(raw, None) is False


# --- endpoint end-to-end (mockeado) ----------------------------------------


def _msg(body, **extra):
    data = {"chatId": "57300@c.us", "from": "57300@c.us", "body": body, "type": "chat"}
    data.update(extra)
    return {"event": "message.received", "data": data}


def test_webhook_runs_query_and_sends_reply(client, fake_workflow, captured_sends):
    resp = client.post("/webhooks/openwa", json=_msg("contratos de obra"))
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "delivered": True}
    assert fake_workflow.calls[0]["channel"] == "whatsapp"
    assert fake_workflow.calls[0]["chat_id"] == "57300@c.us"
    assert captured_sends == [("57300@c.us", "respuesta a: contratos de obra")]


def test_webhook_ignores_outbound_echo(client, fake_workflow, captured_sends):
    resp = client.post("/webhooks/openwa", json=_msg("eco", fromMe=True))
    assert resp.json() == {"ok": True, "ignored": True}
    assert fake_workflow.calls == []
    assert captured_sends == []


def test_webhook_ignores_group_messages(client, fake_workflow, captured_sends):
    resp = client.post("/webhooks/openwa", json=_msg("hola grupo", isGroup=True))
    assert resp.json() == {"ok": True, "ignored": True}
    assert fake_workflow.calls == []


def test_webhook_ignores_non_text_type(client, fake_workflow, captured_sends):
    resp = client.post("/webhooks/openwa", json=_msg("", type="image"))
    assert resp.json() == {"ok": True, "ignored": True}
    assert fake_workflow.calls == []


def test_webhook_deduplicates_retries(client, fake_workflow, captured_sends):
    headers = {"X-OpenWA-Idempotency-Key": "msg_abc_123"}
    first = client.post("/webhooks/openwa", json=_msg("contratos"), headers=headers)
    second = client.post("/webhooks/openwa", json=_msg("contratos"), headers=headers)
    assert first.json() == {"ok": True, "delivered": True}
    assert second.json() == {"ok": True, "status": "duplicate_ignored"}
    assert len(fake_workflow.calls) == 1  # solo se procesó una vez


def test_webhook_reset_clears_conversation(client, fake_workflow, captured_sends):
    resp = client.post("/webhooks/openwa", json=_msg("/reset"))
    assert resp.json() == {"ok": True, "reset": True}
    assert fake_workflow.conv_store.cleared == ["57300@c.us"]
    assert fake_workflow.calls == []


def test_webhook_rejects_bad_signature(client, monkeypatch):
    monkeypatch.setattr(main.settings, "openwa_webhook_secret", "s3cr3t")
    body = json.dumps(_msg("hola")).encode()
    resp = client.post(
        "/webhooks/openwa",
        content=body,
        headers={"X-OpenWA-Signature": "wrong", "Content-Type": "application/json"},
    )
    assert resp.status_code == 401
