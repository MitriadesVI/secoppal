from __future__ import annotations

import json
import sqlite3

from app.core.feedback import FeedbackStore
from app.core.feedback import ensure_feedback_schema
from scripts.migrate_feedback_sqlite import ensure_workspace_items_schema, main as migrate_main


def test_natural_alpha_migration_existing_database_and_rating(tmp_path):
    from app.core.feedback import SCHEMA_SQL
    from app.core.natural_alpha.models import GroundedJob, Observation
    from app.core.natural_alpha.telemetry import TRACE_COLUMNS

    db = tmp_path / "old.db"
    with sqlite3.connect(db) as conn:
        # The pre-Alpha CREATE statement remains unchanged; additions migrate.
        conn.executescript(SCHEMA_SQL)
        assert not set(TRACE_COLUMNS) & {row[1] for row in conn.execute("PRAGMA table_info(traces)")}
        conn.execute("INSERT INTO traces (trace_id, timestamp, channel, user_query, trace_json) "
                     "VALUES ('old', '2026', 'streamlit', 'legacy', '{\"trace_id\": \"old\"}')")
    store = FeedbackStore(tmp_path / "traces.jsonl", db_path=db)
    FeedbackStore(tmp_path / "traces.jsonl", db_path=db)
    observation = Observation("alpha", "MATCHED", (GroundedJob("SEARCH", "CONTRACTS", 0, 5),), 1, 12.5)
    workflow_result = _workflow_result(**observation.metadata())
    workflow_result["natural_alpha_jobs"][0]["evidence_quote"] = "do not duplicate private text"
    trace_id = store.log_trace("consulta", "streamlit", workflow_result)
    assert store.rate(trace_id, 1)
    rows = _db_rows(db, "SELECT * FROM traces ORDER BY trace_id")
    assert len(rows) == 2
    old = next(row for row in rows if row["trace_id"] == "old")
    assert old["trace_json"] == '{"trace_id": "old"}'
    assert all(old[name] is None for name in TRACE_COLUMNS)
    alpha = next(row for row in rows if row["trace_id"] == trace_id)
    assert alpha["natural_alpha_provider_calls"] == 1
    assert alpha["rating"] == 1
    assert json.loads(alpha["natural_alpha_jobs"]) == [
        {"action": "SEARCH", "aspect": "CONTRACTS", "evidence_interval": [0, 5]},
    ]
    assert "do not duplicate" not in alpha["trace_json"]
    assert "do not duplicate" not in store.path.read_text()
    assert len(_db_rows(db, "SELECT * FROM schema_migrations WHERE name = 'natural_alpha_v1'")) == 1


def test_natural_alpha_off_has_no_trace_metadata(tmp_path):
    store = FeedbackStore(tmp_path / "traces.jsonl")
    trace_id = store.log_trace("consulta", "streamlit", _workflow_result())
    trace = next(row for row in store.get_recent() if row["trace_id"] == trace_id)
    assert not any(key.startswith("natural_alpha_") for key in trace)
    assert "natural_alpha" not in store.path.read_text()


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


# ── R2: ensure_user, la única autoridad de identidad ────────────────────────
#
# Workspace necesita una FK válida contra ``users`` y NO puede reconstruir el
# ``user_id`` por su cuenta. Estos casos fijan que la apertura delega en el
# mismo algoritmo que ya usaba log_trace, y que no lo altera.


def _store(tmp_path) -> FeedbackStore:
    return FeedbackStore(
        tmp_path / "feedback.jsonl",
        db_path=tmp_path / "secoppal.db",
        jsonl_dual_write=False,
    )


def test_ensure_user_materializes_a_stable_user_id_for_an_authenticated_email(tmp_path):
    store = _store(tmp_path)
    identity = {
        "channel": "streamlit",
        "channel_user_key": "email:foo@bar.com",
        "name": "Foo",
        "email": "foo@bar.com",
        "is_anonymous": False,
    }

    primero = store.ensure_user("streamlit", identity)
    segundo = store.ensure_user("streamlit", dict(identity))

    assert primero == "streamlit:email:foo@bar.com"
    assert segundo == primero
    users = _db_rows(tmp_path / "secoppal.db", "SELECT user_id, email FROM users")
    assert [(r["user_id"], r["email"]) for r in users] == [
        ("streamlit:email:foo@bar.com", "foo@bar.com")
    ]


def test_ensure_user_produces_the_same_id_that_log_trace_would(tmp_path):
    """Un solo algoritmo: dos autoridades no pueden divergir en la identidad."""
    store = _store(tmp_path)
    identity = {
        "channel": "streamlit",
        "channel_user_key": "email:foo@bar.com",
        "email": "foo@bar.com",
        "is_anonymous": False,
    }

    store.log_trace("consulta", "streamlit", _workflow_result(), user_identity=identity)
    desde_traces = _db_rows(tmp_path / "secoppal.db", "SELECT user_id FROM traces")[0]

    assert store.ensure_user("streamlit", dict(identity)) == desde_traces["user_id"]
    assert len(_db_rows(tmp_path / "secoppal.db", "SELECT user_id FROM users")) == 1


def test_ensure_user_gives_workspace_a_usable_foreign_key(tmp_path):
    """El id devuelto sirve como FK real, no es un string decorativo."""
    from app.core.workspace_store import WorkspaceStore

    db_path = tmp_path / "secoppal.db"
    user_id = _store(tmp_path).ensure_user(
        "streamlit",
        {
            "channel": "streamlit",
            "channel_user_key": "email:foo@bar.com",
            "email": "foo@bar.com",
            "is_anonymous": False,
        },
    )

    guardado = WorkspaceStore(db_path).save_item(
        user_id, "proceso", "p6dx-8zbt/CO1.NTC.1",
        title="Mantenimiento vial", metadata={"source": "radar"},
    )
    filas = _db_rows(db_path, "SELECT item_id, user_id FROM workspace_items")
    assert [(r["item_id"], r["user_id"]) for r in filas] == [
        (guardado.item_id, user_id)
    ]


def test_ensure_user_returns_none_when_the_identity_identifies_nobody(tmp_path):
    store = _store(tmp_path)

    assert store.ensure_user("streamlit", None) is None
    assert store.ensure_user("streamlit", {}) is None
    assert store.ensure_user("streamlit", {"name": "sin clave"}) is None
    assert _db_rows(tmp_path / "secoppal.db", "SELECT user_id FROM users") == []


def test_ensure_user_does_not_write_any_trace(tmp_path):
    """Materializar el usuario no puede fabricar actividad que no existió."""
    store = _store(tmp_path)
    store.ensure_user(
        "streamlit",
        {"channel": "streamlit", "channel_user_key": "anon:abc", "is_anonymous": True},
    )

    assert _db_rows(tmp_path / "secoppal.db", "SELECT trace_id FROM traces") == []
    assert len(_db_rows(tmp_path / "secoppal.db", "SELECT user_id FROM users")) == 1
