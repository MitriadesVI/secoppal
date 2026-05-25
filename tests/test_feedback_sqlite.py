from __future__ import annotations

import json
import sqlite3

from app.core.feedback import FeedbackStore
from app.core.feedback import ensure_feedback_schema
from scripts.migrate_feedback_sqlite import ensure_workspace_items_schema, main as migrate_main


def _workflow_result(**overrides):
    result = {
        "parsed_params": {"dataset": "procesos"},
        "resolved_params": {"dataset": "procesos"},
        "dataset_id": "p6dx-8zbt",
        "soql_query": "SELECT * LIMIT 1",
        "needs_llm": False,
        "needs_clarification": False,
        "route_reason": "heuristic_only",
        "timings_ms": {"total_ms": 12.5},
        "total_count": 1,
        "results": [{"nombre_del_procedimiento": "Mantenimiento vial"}],
        "suggestions": [{"label": "Filtrar por abiertos"}],
        "risk_flag_timeout": True,
        "total_count_approximate": True,
        "analytical_intent": "market_scan",
        "clarification_reason": "",
    }
    result.update(overrides)
    return result


def _db_rows(db_path, sql: str):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(sql).fetchall()


def test_log_trace_dual_writes_append_jsonl_and_sqlite(tmp_path):
    jsonl_path = tmp_path / "feedback.jsonl"
    db_path = tmp_path / "secoppal.db"
    store = FeedbackStore(jsonl_path, db_path=db_path)

    trace_id = store.log_trace(
        "procesos de mantenimiento",
        "streamlit",
        _workflow_result(),
        user_identity={
            "channel": "streamlit",
            "channel_user_key": "email:foo@bar.com",
            "name": "Foo",
            "email": "Foo@Bar.com",
            "is_anonymous": False,
        },
    )

    lines = jsonl_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["trace_id"] == trace_id

    traces = _db_rows(db_path, "SELECT * FROM traces")
    assert len(traces) == 1
    assert traces[0]["trace_id"] == trace_id
    assert traces[0]["has_suggestions"] == 1
    assert traces[0]["risk_flag_timeout"] == 1
    assert traces[0]["total_count_approximate"] == 1
    assert traces[0]["analytical_intent"] == "market_scan"

    users = _db_rows(db_path, "SELECT * FROM users")
    assert len(users) == 1
    assert users[0]["email"] == "foo@bar.com"
    assert users[0]["is_anonymous"] == 0


def test_rate_updates_only_sqlite_not_jsonl(tmp_path):
    jsonl_path = tmp_path / "feedback.jsonl"
    db_path = tmp_path / "secoppal.db"
    store = FeedbackStore(jsonl_path, db_path=db_path)
    trace_id = store.log_trace("consulta", "streamlit", _workflow_result())
    before = jsonl_path.read_bytes()

    assert store.rate(trace_id, 0, "no era lo que buscaba") is True

    assert jsonl_path.read_bytes() == before
    row = _db_rows(db_path, "SELECT rating, rating_comment, rated_at, trace_json FROM traces")[0]
    assert row["rating"] == 0
    assert row["rating_comment"] == "no era lo que buscaba"
    assert row["rated_at"]
    assert json.loads(row["trace_json"])["rating"] == 0


def test_rate_late_imports_jsonl_trace_when_sqlite_misses(tmp_path, caplog):
    jsonl_path = tmp_path / "feedback.jsonl"
    db_path = tmp_path / "secoppal.db"
    trace = {
        "trace_id": "late123",
        "timestamp": "2026-05-24T10:00:00+00:00",
        "user_query": "consulta antigua",
        "channel": "streamlit",
        "parsed_params": {},
        "resolved_params": {},
        "dataset_id": "",
        "soql_query": "",
        "needs_llm": False,
        "needs_clarification": False,
        "route_reason": "heuristic_only",
        "timings_ms": {},
        "total_count": 0,
        "results_count": 0,
        "first_result_title": "",
        "rating": None,
        "rating_comment": None,
        "rated_at": None,
    }
    jsonl_path.write_text(json.dumps(trace) + "\n", encoding="utf-8")
    store = FeedbackStore(jsonl_path, db_path=db_path)

    assert store.rate("late123", 1, "ok") is True

    row = _db_rows(db_path, "SELECT trace_id, rating, source FROM traces")[0]
    assert row["trace_id"] == "late123"
    assert row["rating"] == 1
    assert row["source"] == "late_import"
    assert "late import during rate" in caplog.text


def test_migration_imports_all_jsonl_traces_once_and_verifies_structured_columns(tmp_path):
    jsonl_path = tmp_path / "feedback.jsonl"
    db_path = tmp_path / "secoppal.db"
    rows = [
        {
            "trace_id": "a1",
            "timestamp": "2026-05-24 10:00:00",
            "user_query": "consulta a",
            "channel": "streamlit",
            "parsed_params": {"dataset": "procesos"},
            "resolved_params": {"dataset": "procesos"},
            "dataset_id": "p6dx-8zbt",
            "soql_query": "SELECT 1",
            "needs_llm": False,
            "needs_clarification": True,
            "route_reason": "clarify",
            "timings_ms": {"total_ms": 10},
            "total_count": 0,
            "total_count_approximate": False,
            "suggestions": [],
            "risk_flag_timeout": False,
            "analytical_intent": "",
            "clarification_reason": "faltan filtros",
            "results_count": 0,
            "first_result_title": "",
            "rating": None,
            "rating_comment": None,
            "rated_at": None,
        },
        {
            "trace_id": "b2",
            "timestamp": "2026-05-24T11:00:00Z",
            "user_query": "consulta b",
            "channel": "telegram",
            "parsed_params": {},
            "resolved_params": {},
            "dataset_id": "",
            "soql_query": "",
            "needs_llm": True,
            "needs_clarification": False,
            "route_reason": "llm_fallback",
            "timings_ms": {"total_ms": 20},
            "total_count": 3,
            "total_count_approximate": True,
            "suggestions": [{"label": "refinar"}],
            "risk_flag_timeout": True,
            "analytical_intent": "trend",
            "clarification_reason": "",
            "results_count": 1,
            "first_result_title": "Proceso",
            "rating": 1,
            "rating_comment": "bien",
            "rated_at": "2026-05-24T11:01:00Z",
        },
    ]
    jsonl_path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    assert migrate_main(["--jsonl", str(jsonl_path), "--db", str(db_path), "--sample-size", "all"]) == 0
    assert migrate_main(["--jsonl", str(jsonl_path), "--db", str(db_path), "--sample-size", "all"]) == 0

    traces = _db_rows(
        db_path,
        """
        SELECT trace_id, timestamp, clarification_reason, has_suggestions,
               risk_flag_timeout, total_count_approximate, rating
        FROM traces
        ORDER BY trace_id
        """,
    )
    assert len(traces) == 2
    assert traces[0]["timestamp"] == "2026-05-24T10:00:00+00:00"
    assert traces[0]["clarification_reason"] == "faltan filtros"
    assert traces[0]["has_suggestions"] == 0
    assert traces[1]["has_suggestions"] == 1
    assert traces[1]["risk_flag_timeout"] == 1
    assert traces[1]["total_count_approximate"] == 1
    assert traces[1]["rating"] == 1


def test_email_normalization_reuses_same_streamlit_user(tmp_path):
    jsonl_path = tmp_path / "feedback.jsonl"
    db_path = tmp_path / "secoppal.db"
    store = FeedbackStore(jsonl_path, db_path=db_path)

    for email in ("Foo@Bar.com", " foo@bar.com "):
        store.log_trace(
            "consulta",
            "streamlit",
            _workflow_result(),
            user_identity={
                "channel": "streamlit",
                "channel_user_key": f"email:{email.strip().lower()}",
                "email": email,
                "is_anonymous": False,
            },
        )

    users = _db_rows(db_path, "SELECT email FROM users")
    assert [row["email"] for row in users] == ["foo@bar.com"]


def test_workspace_items_table_exists_and_accepts_inserts(tmp_path):
    db_path = tmp_path / "secoppal.db"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        ensure_feedback_schema(conn)
        ensure_workspace_items_schema(conn, applied_at="2026-05-25T00:00:00+00:00")
        conn.execute(
            """
            INSERT INTO users (
                user_id, channel, channel_user_key, is_anonymous,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "streamlit:anon:test",
                "streamlit",
                "anon:test",
                1,
                "2026-05-25T00:00:00+00:00",
                "2026-05-25T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO workspace_items (
                item_id, user_id, item_type, reference_id, title,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "item-1",
                "streamlit:anon:test",
                "proceso",
                "PROC-1",
                "Proceso de prueba",
                "2026-05-25T00:00:00+00:00",
                "2026-05-25T00:00:00+00:00",
            ),
        )
        row = conn.execute(
            """
            SELECT item_id, user_id, item_type, reference_id, title,
                   status, archived, is_pinned
            FROM workspace_items
            WHERE item_id = ?
            """,
            ("item-1",),
        ).fetchone()

    assert dict(row) == {
        "item_id": "item-1",
        "user_id": "streamlit:anon:test",
        "item_type": "proceso",
        "reference_id": "PROC-1",
        "title": "Proceso de prueba",
        "status": "active",
        "archived": 0,
        "is_pinned": 0,
    }
