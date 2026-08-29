"""R5A — la bandeja es una proyección; el feed histórico no se muta."""
from __future__ import annotations

from app.core.db import connect_sqlite
from app.core.feedback import ensure_feedback_schema
from app.core.radar.inbox import project_inbox
from app.core.radar.receipts import DISCARDED, NOT_RELEVANT, RadarReceiptStore
from app.core.radar.store import RadarStore
from app.core.workspace_store import WorkspaceStore

OWNER = "email:ana@example.com"
USER = "streamlit:email:ana@example.com"


def _event(event_id, kind="new", result_key="RESULT-X", radar_id="radar-a"):
    return {
        "id": event_id,
        "kind": kind,
        "radar_id": radar_id,
        "result_key": result_key,
        "title": event_id,
    }


def test_unseen_is_unread_viewed_is_read_and_baseline_never_counts():
    events = [_event("baseline", kind="baseline", result_key=None), _event("ev-a")]

    unseen = project_inbox(events)
    viewed = project_inbox(events, viewed_event_ids={"ev-a"})

    assert unseen.visible_events == tuple(events)
    assert unseen.unread_event_ids == {"ev-a"}
    assert unseen.unread_count == 1
    assert viewed.unread_count == 0


def test_discarded_and_not_relevant_are_hidden_then_restored_is_visible():
    events = [_event("ev-a"), _event("ev-b", result_key="RESULT-Y")]
    discarded = project_inbox(
        events,
        dispositions={
            ("radar-a", "RESULT-X"): DISCARDED,
            ("radar-a", "RESULT-Y"): NOT_RELEVANT,
        },
    )
    restored = project_inbox(
        events, dispositions={("radar-a", "RESULT-X"): "active"}
    )

    assert discarded.visible_events == ()
    assert discarded.unread_count == 0
    assert restored.visible_events == tuple(events)


def test_future_event_same_result_is_hidden_but_other_radar_is_not():
    future = _event("ev-future", kind="changed_state")
    other_radar = _event("ev-other", radar_id="radar-b")
    projection = project_inbox(
        [future, other_radar],
        dispositions={("radar-a", "RESULT-X"): DISCARDED},
    )

    assert projection.visible_events == (other_radar,)
    assert projection.unread_event_ids == {"ev-other"}


def test_discard_does_not_touch_a_saved_workspace_item(tmp_path):
    db = tmp_path / "integration.db"
    conn = connect_sqlite(db)
    try:
        ensure_feedback_schema(conn)
        conn.execute(
            """
            INSERT INTO users (
                user_id, channel, channel_user_key, email, is_anonymous,
                created_at, updated_at
            ) VALUES (?, 'streamlit', ?, 'ana@example.com', 0, ?, ?)
            """,
            (USER, OWNER, "2026-08-29T00:00:00+00:00", "2026-08-29T00:00:00+00:00"),
        )
    finally:
        conn.close()
    radars = RadarStore(db)
    radar_id = radars.create_radar(
        name="Radar A", owner=OWNER, query_frame_json="{}"
    )
    radars.record_events([{
        "radar_id": radar_id, "owner": OWNER,
        "radar_name": "Radar A", "ts": "2026-08-29T12:00:00+00:00",
        "kind": "new", "result_key": "RESULT-X", "dataset": "procesos",
        "title": "Proceso X",
    }])
    workspace = WorkspaceStore(db)
    saved = workspace.save_item(
        USER, "proceso", "p6dx-8zbt/RESULT-X", title="Proceso X",
        metadata={"source": "radar"},
    )

    event_id = radars.get_feed(OWNER, limit=1)[0]["id"]
    RadarReceiptStore(db).discard(OWNER, event_id)

    item = workspace.get_item(USER, saved.item_id)
    assert item is not None
    assert not item.archived
    assert item.title == "Proceso X"
