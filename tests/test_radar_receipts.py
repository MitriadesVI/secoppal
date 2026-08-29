"""R5A — receipts append-only de Inbox y feedback de relevancia."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import connect_sqlite
from app.core.radar.receipts import (
    ACTIVE,
    DISCARDED,
    NOT_RELEVANT,
    RadarReceiptStore,
    ReceiptDispositionError,
    ReceiptTargetNotFoundError,
)
from app.core.radar.store import RadarStore

NOW = datetime(2026, 8, 29, 15, 0, tzinfo=timezone.utc)
OWNER = "email:ana@example.com"


@pytest.fixture()
def env(tmp_path):
    db = tmp_path / "receipts.db"
    radars = RadarStore(db)
    frame = {
        "topic": "alimentación escolar",
        "departments": ["Atlántico"],
        "api_token": "NO-DEBE-PERSISTIR",
        "nested": {"cookie": "NO-DEBE-PERSISTIR", "safe": "sí"},
    }
    radar = radars.create_radar(
        name="PAE Caribe", owner=OWNER,
        query_frame_json=json.dumps(frame, ensure_ascii=False),
    )
    other_radar = radars.create_radar(
        name="PAE Centro", owner=OWNER, query_frame_json="{}"
    )
    alien_radar = radars.create_radar(
        name="PAE Ajeno", owner="email:beto@example.com", query_frame_json="{}"
    )
    rows = [
        _event(radar, "ev-a", "RESULT-X", title="Alimento para perros"),
        _event(radar, "ev-baseline", None, kind="baseline", title="Línea base"),
        _event(radar, "ev-no-key", None),
        _event(other_radar, "ev-other-radar", "RESULT-X"),
        _event(alien_radar, "ev-alien", "RESULT-X", owner="email:beto@example.com"),
    ]
    radars.record_events(rows)
    conn = connect_sqlite(db)
    try:
        stored = conn.execute(
            "SELECT id, owner, radar_id, kind, result_key FROM radar_events"
        ).fetchall()
    finally:
        conn.close()
    event_ids = {
        "a": next(row["id"] for row in stored
                  if row["radar_id"] == radar and row["result_key"] == "RESULT-X"),
        "baseline": next(row["id"] for row in stored if row["kind"] == "baseline"),
        "no_key": next(row["id"] for row in stored
                       if row["radar_id"] == radar and row["kind"] == "new"
                       and row["result_key"] is None),
        "other_radar": next(row["id"] for row in stored
                            if row["radar_id"] == other_radar),
        "alien": next(row["id"] for row in stored if row["radar_id"] == alien_radar),
    }
    return {
        "db": db,
        "radars": radars,
        "receipts": RadarReceiptStore(db),
        "radar": radar,
        "other_radar": other_radar,
        "alien_radar": alien_radar,
        "events": event_ids,
    }


def _event(radar_id, event_id, result_key, **overrides):
    row = {
        "id": event_id,
        "radar_id": radar_id,
        "owner": OWNER,
        "radar_name": "PAE Caribe",
        "run_id": "run-secret-runtime-state",
        "ts": "2026-08-29T14:00:00+00:00",
        "kind": "new",
        "result_key": result_key,
        "dataset": "procesos",
        "title": "Programa de alimentación escolar",
        "object_excerpt": "Suministro de alimentos para estudiantes.",
        "entity": "ALCALDÍA",
        "department": "Atlántico",
        "city": "Barranquilla",
        "value": "500000000",
        "status": "Abierto",
        "modality": "Licitación pública",
        "closing_date": "2026-09-30T00:00:00.000",
        "referencia": "PAE-2026",
        "urlproceso": "https://secop.example/proceso",
    }
    row.update(overrides)
    return row


def _rows(env):
    conn = connect_sqlite(env["db"])
    try:
        return [dict(row) for row in conn.execute(
            "SELECT * FROM radar_event_receipts ORDER BY receipt_seq"
        ).fetchall()]
    finally:
        conn.close()


def test_fresh_schema_is_append_only_ordered_and_has_no_destructive_fk(env):
    conn = connect_sqlite(env["db"])
    try:
        columns = {
            row["name"] for row in conn.execute(
                "PRAGMA table_info(radar_event_receipts)"
            )
        }
        foreign_keys = conn.execute(
            "PRAGMA foreign_key_list(radar_event_receipts)"
        ).fetchall()
    finally:
        conn.close()
    assert {
        "receipt_seq", "receipt_id", "owner", "event_id", "radar_id",
        "result_key", "action", "event_kind", "occurred_at", "evidence_json",
    } <= columns
    assert foreign_keys == []


def test_view_is_idempotent_and_scoped_to_the_owner(env):
    event_id = env["events"]["a"]
    first = env["receipts"].mark_viewed(OWNER, event_id, now=NOW)
    repeat = env["receipts"].mark_viewed(OWNER, event_id, now=NOW)

    assert first.created and not first.noop
    assert repeat.noop and not repeat.created
    assert env["receipts"].get_viewed_event_ids(
        OWNER, [event_id, "unknown"]
    ) == {event_id}
    assert env["receipts"].get_viewed_event_ids(
        "email:beto@example.com", [event_id]
    ) == set()
    assert [row["action"] for row in _rows(env)] == ["viewed"]


@pytest.mark.parametrize("action", ["discard", "mark_not_relevant"])
def test_cross_owner_event_ids_are_rejected(env, action):
    with pytest.raises(ReceiptTargetNotFoundError):
        getattr(env["receipts"], action)(OWNER, env["events"]["alien"], now=NOW)
    with pytest.raises(ReceiptTargetNotFoundError):
        getattr(env["receipts"], action)(
            "email:beto@example.com", env["events"]["a"], now=NOW
        )
    assert _rows(env) == []


@pytest.mark.parametrize("event_name", ["baseline", "no_key"])
@pytest.mark.parametrize("action", ["discard", "mark_not_relevant"])
def test_baseline_and_missing_result_identity_fail_closed(env, event_name, action):
    with pytest.raises(ReceiptDispositionError):
        getattr(env["receipts"], action)(OWNER, env["events"][event_name], now=NOW)
    assert _rows(env) == []


def test_discard_and_not_relevant_repetitions_are_noops(env):
    event_id = env["events"]["a"]
    discarded = env["receipts"].discard(OWNER, event_id, now=NOW)
    repeat_discard = env["receipts"].discard(OWNER, event_id, now=NOW)
    relevant = env["receipts"].mark_not_relevant(OWNER, event_id, now=NOW)
    repeat_relevant = env["receipts"].mark_not_relevant(OWNER, event_id, now=NOW)

    assert (discarded.previous_disposition, discarded.current_disposition) == (
        ACTIVE, DISCARDED,
    )
    assert repeat_discard.noop
    assert (relevant.previous_disposition, relevant.current_disposition) == (
        DISCARDED, NOT_RELEVANT,
    )
    assert repeat_relevant.noop
    assert [row["action"] for row in _rows(env)] == [DISCARDED, NOT_RELEVANT]


@pytest.mark.parametrize("first_action", ["discard", "mark_not_relevant"])
def test_restore_appends_and_restore_while_active_is_a_noop(env, first_action):
    active = env["receipts"].restore(OWNER, env["radar"], "RESULT-X", now=NOW)
    getattr(env["receipts"], first_action)(OWNER, env["events"]["a"], now=NOW)
    restored = env["receipts"].restore(
        OWNER, env["radar"], "RESULT-X", now=NOW
    )

    assert active.noop
    assert restored.created
    assert restored.current_disposition == ACTIVE
    assert len(_rows(env)) == 2
    assert _rows(env)[-1]["action"] == "restored"


def test_suppression_applies_to_future_events_only_in_same_scope(env):
    env["receipts"].discard(OWNER, env["events"]["a"], now=NOW)
    env["radars"].record_events([
        _event(env["radar"], "ev-future", "RESULT-X", kind="changed_state"),
    ])

    assert env["receipts"].get_suppressed_result_keys(
        OWNER, env["radar"]
    ) == {"RESULT-X"}
    assert env["receipts"].get_suppressed_result_keys(
        OWNER, env["other_radar"]
    ) == set()
    assert env["receipts"].get_suppressed_result_keys(
        "email:beto@example.com", env["alien_radar"]
    ) == set()
    assert env["receipts"].get_current_dispositions(OWNER) == {
        (env["radar"], "RESULT-X"): DISCARDED,
    }


def test_not_relevant_freezes_whitelisted_evidence_and_query_frame(env):
    env["receipts"].mark_not_relevant(OWNER, env["events"]["a"], now=NOW)
    receipt = _rows(env)[0]
    evidence = json.loads(receipt["evidence_json"])

    assert evidence["schema_version"] == 1
    assert evidence["feedback"] == NOT_RELEVANT
    assert evidence["event_id"] == env["events"]["a"]
    assert evidence["event_kind"] == "new"
    assert evidence["event_ts"] == "2026-08-29T14:00:00+00:00"
    assert evidence["radar_id"] == env["radar"]
    assert evidence["radar_name"] == "PAE Caribe"
    assert evidence["result_key"] == "RESULT-X"
    assert evidence["object_excerpt"].startswith("Suministro")
    assert evidence["query_frame"]["topic"] == "alimentación escolar"
    assert evidence["query_frame"]["nested"] == {"safe": "sí"}
    assert evidence["feedback_at"] == NOW.isoformat()
    frozen = json.dumps(evidence, ensure_ascii=False).casefold()
    for forbidden in (
        "no-debe-persistir", "api_token", "cookie", "authorization",
        "run-secret-runtime-state", "run_id", "event_seq",
    ):
        assert forbidden not in frozen


def test_restored_not_relevant_feedback_is_explicitly_retracted(env):
    env["receipts"].mark_not_relevant(OWNER, env["events"]["a"], now=NOW)
    env["receipts"].restore(OWNER, env["radar"], "RESULT-X", now=NOW)

    feedback = env["receipts"].list_not_relevant_feedback(owner=OWNER)
    assert len(feedback) == 1
    assert feedback[0].retracted
    assert feedback[0].currently_restored
    assert feedback[0].current_disposition == ACTIVE
    assert env["receipts"].list_suppressed(OWNER) == []


def test_receipt_writes_never_update_or_delete_prior_rows(env):
    env["receipts"].discard(OWNER, env["events"]["a"], now=NOW)
    before = _rows(env)[0]
    env["receipts"].restore(OWNER, env["radar"], "RESULT-X", now=NOW)
    after = _rows(env)

    assert len(after) == 2
    assert after[0] == before
    source = Path(__file__).parents[1] / "app/core/radar/receipts.py"
    text = source.read_text()
    assert "UPDATE radar_event_receipts" not in text
    assert "DELETE FROM radar_event_receipts" not in text
