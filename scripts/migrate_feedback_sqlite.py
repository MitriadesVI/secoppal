#!/usr/bin/env python3
"""
Migrate SECOPPAL feedback JSONL to SQLite.

Backup & retention:
- Run a daily backup with:
    mkdir -p .backup
    sqlite3 data/secoppal.db "PRAGMA wal_checkpoint(FULL);"
    cp data/secoppal.db ".backup/secoppal-$(date +%F).db"
    find .backup -name "secoppal-*.db" -mtime +30 -delete
- After 30 dual-write days, archive JSONL read-only, do not delete it:
    mkdir -p data/archive
    cp data/feedback.jsonl "data/archive/feedback-pre-sqlite-$(date +%F).jsonl"
    chmod 0444 "data/archive/feedback-pre-sqlite-$(date +%F).jsonl"
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.db import connect_sqlite
from app.core.feedback import TRACE_COLUMNS, _trace_to_row, ensure_feedback_schema


MIGRATION_NAME = "feedback_jsonl_to_sqlite_001"
WORKSPACE_ITEMS_MIGRATION_NAME = "workspace_items_schema_001"


WORKSPACE_ITEMS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS workspace_items (
    item_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    item_type TEXT NOT NULL CHECK (
        item_type IN (
            'proceso', 'contrato', 'contratista', 'entidad',
            'busqueda', 'analisis', 'nota', 'radar_ref',
            'documento', 'paa_rubro'
        )
    ),
    reference_id TEXT,
    title TEXT NOT NULL,
    note TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    deadline TEXT,
    source_trace_id TEXT,
    metadata_json TEXT,
    save_reason TEXT,
    is_pinned INTEGER NOT NULL DEFAULT 0 CHECK (is_pinned IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
) WITHOUT ROWID
"""

WORKSPACE_ITEMS_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_workspace_user ON workspace_items(user_id, archived)",
    "CREATE INDEX IF NOT EXISTS idx_workspace_user_deadline ON workspace_items(user_id, deadline)",
    """
    CREATE INDEX IF NOT EXISTS idx_workspace_user_type
      ON workspace_items(user_id, item_type, archived)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_workspace_user_pinned
      ON workspace_items(user_id, is_pinned) WHERE is_pinned = 1
    """,
    "CREATE INDEX IF NOT EXISTS idx_workspace_reference ON workspace_items(reference_id)",
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    jsonl_path = Path(args.jsonl)
    db_path = Path(args.db)

    loaded = load_jsonl(jsonl_path)
    if loaded["errors"]:
        _print_json({"ok": False, "errors": loaded["errors"]})
        return 1
    if loaded["duplicates"]:
        _print_json({"ok": False, "duplicates": loaded["duplicates"][:20]})
        return 1

    traces = loaded["traces"]
    before_count = 0
    after_count = 0
    verification: dict[str, Any] = {}

    if args.dry_run:
        _print_json({
            "ok": True,
            "dry_run": True,
            "valid_jsonl_traces": len(traces),
            "unique_trace_ids": len({t["trace_id"] for t in traces}),
        })
        return 0

    with connect_sqlite(db_path) as conn:
        ensure_feedback_schema(conn)
        before_count = _count_traces(conn)
        migrated_at = datetime.now(timezone.utc).isoformat()
        conn.execute("BEGIN IMMEDIATE")
        try:
            for trace in traces:
                insert_trace(conn, trace, migrated_at=migrated_at)
            conn.execute(
                """
                INSERT INTO schema_migrations (name, applied_at)
                VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET applied_at = excluded.applied_at
                """,
                (MIGRATION_NAME, migrated_at),
            )
            ensure_workspace_items_schema(conn, applied_at=migrated_at)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        after_count = _count_traces(conn)
        verification = verify(conn, traces, sample_size=args.sample_size)

    ok = (
        after_count == len(traces)
        and verification["sample_discrepancies"] == []
        and verification["fk_orphans"] == 0
        and verification["bad_ratings"] == 0
    )
    _print_json({
        "ok": ok,
        "jsonl_traces": len(traces),
        "sqlite_before": before_count,
        "sqlite_after": after_count,
        "verification": verification,
    })
    return 0 if ok else 1


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jsonl", default="data/feedback.jsonl", help="Source feedback JSONL path")
    parser.add_argument("--db", default="data/secoppal.db", help="Destination SQLite DB path")
    parser.add_argument("--dry-run", action="store_true", help="Validate JSONL without writing SQLite")
    parser.add_argument(
        "--sample-size",
        default="50",
        help="Number of traces to verify against trace_json, or 'all'",
    )
    return parser.parse_args(argv)


def load_jsonl(path: Path) -> dict[str, Any]:
    traces: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    seen: set[str] = set()

    with path.open("r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            raw = line.strip()
            if not raw:
                continue
            try:
                trace = json.loads(raw)
            except json.JSONDecodeError as exc:
                errors.append({"line": line_number, "error": str(exc)})
                continue
            if not isinstance(trace, dict):
                errors.append({"line": line_number, "error": "line is not a JSON object"})
                continue
            trace_id = str(trace.get("trace_id") or "")
            if not trace_id:
                errors.append({"line": line_number, "error": "missing trace_id"})
                continue
            if trace_id in seen:
                duplicates.append({"line": line_number, "trace_id": trace_id})
                continue
            seen.add(trace_id)
            trace = dict(trace)
            trace["trace_id"] = trace_id
            trace["timestamp"] = normalize_timestamp(trace.get("timestamp"))
            if trace.get("rated_at"):
                trace["rated_at"] = normalize_timestamp(trace.get("rated_at"))
            traces.append(trace)
    return {"traces": traces, "errors": errors, "duplicates": duplicates}


def insert_trace(conn: sqlite3.Connection, trace: dict[str, Any], migrated_at: str) -> None:
    row = _trace_to_row(trace, user_id=None, source="migration", migrated_at=migrated_at)
    placeholders = ", ".join("?" for _ in TRACE_COLUMNS)
    columns = ", ".join(TRACE_COLUMNS)
    updates = ", ".join(
        f"{column} = excluded.{column}"
        for column in TRACE_COLUMNS
        if column not in {"trace_id", "user_id"}
    )
    conn.execute(
        f"""
        INSERT INTO traces ({columns})
        VALUES ({placeholders})
        ON CONFLICT(trace_id) DO UPDATE SET
            user_id = COALESCE(traces.user_id, excluded.user_id),
            {updates}
        """,
        tuple(row[column] for column in TRACE_COLUMNS),
    )


def ensure_workspace_items_schema(conn: sqlite3.Connection, applied_at: str | None = None) -> None:
    table = conn.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name = 'workspace_items'
        """
    ).fetchone()
    if table is not None:
        row_count = conn.execute("SELECT COUNT(*) FROM workspace_items").fetchone()[0]
        autoindex = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'index'
              AND tbl_name = 'workspace_items'
              AND name LIKE 'sqlite_autoindex_%'
            """
        ).fetchone()
        if row_count == 0 and autoindex is not None:
            conn.execute("DROP TABLE workspace_items")

    conn.execute(WORKSPACE_ITEMS_TABLE_SQL)
    for statement in WORKSPACE_ITEMS_INDEX_SQL:
        conn.execute(statement)
    conn.execute(
        """
        INSERT INTO schema_migrations (name, applied_at)
        VALUES (?, ?)
        ON CONFLICT(name) DO UPDATE SET applied_at = excluded.applied_at
        """,
        (
            WORKSPACE_ITEMS_MIGRATION_NAME,
            applied_at or datetime.now(timezone.utc).isoformat(),
        ),
    )


def verify(conn: sqlite3.Connection, traces: list[dict[str, Any]], sample_size: str) -> dict[str, Any]:
    by_id = {trace["trace_id"]: trace for trace in traces}
    sample_ids = _sample_trace_ids(list(by_id), sample_size)
    discrepancies = []
    for trace_id in sample_ids:
        row = conn.execute("SELECT * FROM traces WHERE trace_id = ?", (trace_id,)).fetchone()
        if row is None:
            discrepancies.append({"trace_id": trace_id, "field": "row", "expected": "present", "actual": None})
            continue
        stored_trace = json.loads(row["trace_json"])
        expected_row = _trace_to_row(stored_trace, row["user_id"], row["source"], row["migrated_at"])
        for field in _verified_fields():
            expected = expected_row[field]
            actual = row[field]
            if actual != expected:
                discrepancies.append({
                    "trace_id": trace_id,
                    "field": field,
                    "expected": expected,
                    "actual": actual,
                })

    fk_orphans = conn.execute(
        """
        SELECT COUNT(*)
        FROM traces
        WHERE user_id IS NOT NULL
          AND user_id NOT IN (SELECT user_id FROM users)
        """
    ).fetchone()[0]
    bad_ratings = conn.execute(
        """
        SELECT COUNT(*)
        FROM traces
        WHERE rating IS NOT NULL
          AND (rated_at IS NULL OR rating NOT IN (0, 1))
        """
    ).fetchone()[0]
    return {
        "sample_size": len(sample_ids),
        "sample_discrepancies": discrepancies,
        "fk_orphans": fk_orphans,
        "bad_ratings": bad_ratings,
    }


def normalize_timestamp(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return datetime.now(timezone.utc).isoformat()
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"invalid timestamp: {value!r}") from None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _sample_trace_ids(trace_ids: list[str], sample_size: str) -> list[str]:
    if sample_size == "all":
        return sorted(trace_ids)
    size = int(sample_size)
    if size >= len(trace_ids):
        return sorted(trace_ids)
    return sorted(random.Random(42).sample(trace_ids, size))


def _verified_fields() -> tuple[str, ...]:
    return tuple(
        field for field in TRACE_COLUMNS
        if field not in {"trace_id", "user_id", "trace_json", "source", "migrated_at"}
    )


def _count_traces(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM traces").fetchone()[0])


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
