"""
SECOPPAL — Query Feedback System
=================================

Stores complete query traces with user ratings.
Each trace captures the full pipeline: raw query → regex params → LLM params →
entity resolution → SoQL → results count → user rating.

Storage: JSONL (one JSON object per line) — append-only, easy to analyze.

Usage:
    feedback = FeedbackStore("data/feedback.jsonl")

    # After query execution, log the trace
    trace_id = feedback.log_trace(
        user_query="procesos de mantenimiento en atlantico",
        channel="streamlit",
        workflow_result=result,  # dict from SecopalWorkflow.run_query()
    )

    # When user rates the result
    feedback.rate(trace_id, rating=1, comment="encontró lo que buscaba")

    # Analysis
    stats = feedback.get_stats()
    failures = feedback.get_unrated()
    low_rated = feedback.get_low_rated()
"""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.db import connect_sqlite
from app.core.natural_alpha.telemetry import (
    TRACE_COLUMNS as NATURAL_ALPHA_COLUMNS, sqlite_values, trace_metadata,
)


logger = logging.getLogger(__name__)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    channel_user_key TEXT NOT NULL,
    name TEXT,
    email TEXT,
    is_anonymous INTEGER NOT NULL DEFAULT 1 CHECK (is_anonymous IN (0, 1)),
    primary_user_id TEXT NULL REFERENCES users(user_id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (channel, channel_user_key)
);

CREATE TABLE IF NOT EXISTS traces (
    trace_id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    user_id TEXT,
    channel TEXT NOT NULL,
    user_query TEXT NOT NULL,

    parsed_params_json TEXT NOT NULL DEFAULT '{}',
    resolved_params_json TEXT NOT NULL DEFAULT '{}',
    dataset_id TEXT NOT NULL DEFAULT '',
    soql_query TEXT NOT NULL DEFAULT '',
    needs_llm INTEGER NOT NULL DEFAULT 0 CHECK (needs_llm IN (0, 1)),
    needs_clarification INTEGER NOT NULL DEFAULT 0 CHECK (needs_clarification IN (0, 1)),
    route_reason TEXT NOT NULL DEFAULT '',
    timings_ms_json TEXT NOT NULL DEFAULT '{}',
    total_ms REAL,
    query_error TEXT NOT NULL DEFAULT '',
    total_count INTEGER NOT NULL DEFAULT 0,
    total_count_approximate INTEGER NOT NULL DEFAULT 0 CHECK (total_count_approximate IN (0, 1)),
    degraded INTEGER NOT NULL DEFAULT 0 CHECK (degraded IN (0, 1)),
    followup INTEGER NOT NULL DEFAULT 0 CHECK (followup IN (0, 1)),
    intent_type TEXT NOT NULL DEFAULT '',
    followup_intent_type TEXT NOT NULL DEFAULT '',
    analytical_intent TEXT NOT NULL DEFAULT '',
    clarification_reason TEXT NOT NULL DEFAULT '',
    has_suggestions INTEGER NOT NULL DEFAULT 0 CHECK (has_suggestions IN (0, 1)),
    risk_flag_timeout INTEGER NOT NULL DEFAULT 0 CHECK (risk_flag_timeout IN (0, 1)),

    results_count INTEGER NOT NULL DEFAULT 0,
    first_result_title TEXT NOT NULL DEFAULT '',

    rating INTEGER CHECK (rating IN (0, 1)),
    rating_comment TEXT,
    rated_at TEXT,

    trace_json TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'app',
    migrated_at TEXT,

    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_traces_timestamp ON traces(timestamp);
CREATE INDEX IF NOT EXISTS idx_traces_channel_timestamp ON traces(channel, timestamp);
CREATE INDEX IF NOT EXISTS idx_traces_user_timestamp ON traces(user_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_traces_rating ON traces(rating);
CREATE INDEX IF NOT EXISTS idx_users_primary_user_id ON users(primary_user_id);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    trace_id TEXT,
    user_id TEXT,
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'result_click',
            'refinement',
            'export',
            'radar_created',
            'abandon',
            'bookmark'
        )
    ),
    event_data_json TEXT NOT NULL DEFAULT '{}',
    timestamp TEXT NOT NULL,
    FOREIGN KEY (trace_id) REFERENCES traces(trace_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_events_trace_id ON events(trace_id);
CREATE INDEX IF NOT EXISTS idx_events_user_type ON events(user_id, event_type);

CREATE TABLE IF NOT EXISTS schema_migrations (
    name TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
);
"""


TRACE_COLUMNS = (
    "trace_id",
    "timestamp",
    "user_id",
    "channel",
    "user_query",
    "parsed_params_json",
    "resolved_params_json",
    "dataset_id",
    "soql_query",
    "needs_llm",
    "needs_clarification",
    "route_reason",
    "timings_ms_json",
    "total_ms",
    "query_error",
    "total_count",
    "total_count_approximate",
    "degraded",
    "followup",
    "intent_type",
    "followup_intent_type",
    "analytical_intent",
    "clarification_reason",
    "has_suggestions",
    "risk_flag_timeout",
    "results_count",
    "first_result_title",
    "rating",
    "rating_comment",
    "rated_at",
    "trace_json",
    "source",
    "migrated_at",
    *NATURAL_ALPHA_COLUMNS,
)


class FeedbackStore:
    """Dual feedback store: SQLite primary, JSONL append-only transition log."""

    def __init__(
        self,
        path: str | Path = "data/feedback.jsonl",
        db_path: str | Path | None = None,
        sqlite_enabled: bool = True,
        jsonl_dual_write: bool = True,
    ):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(db_path) if db_path is not None else self.path.parent / "secoppal.db"
        self.sqlite_enabled = sqlite_enabled
        self.jsonl_dual_write = jsonl_dual_write
        if self.sqlite_enabled:
            self._setup_sqlite()

    def log_trace(
        self,
        user_query: str,
        channel: str,
        workflow_result: dict[str, Any],
        user_identity: dict[str, Any] | None = None,
    ) -> str:
        """
        Log a complete query trace. Returns trace_id for later rating.

        workflow_result is the dict returned by SecopalWorkflow.run_query()
        """
        trace_id = uuid.uuid4().hex[:12]
        from app.core.actor_resolution_trace import actor_resolution_trace
        has_suggestions = bool(
            workflow_result.get("suggestions")
            or workflow_result.get("timeout_suggestions")
        )

        trace = {
            **trace_metadata(workflow_result),
            **actor_resolution_trace(
                workflow_result.get("resolved_params") or workflow_result.get("parsed_params") or {}
            ),
            "trace_id": trace_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_query": user_query,
            "channel": channel,
            # Pipeline trace
            "parsed_params": workflow_result.get("parsed_params", {}),
            "resolved_params": _safe_serialize(workflow_result.get("resolved_params", {})),
            "dataset_id": workflow_result.get("dataset_id", ""),
            "soql_query": workflow_result.get("soql_query", ""),
            "needs_llm": workflow_result.get("needs_llm", False),
            "needs_clarification": workflow_result.get("needs_clarification", False),
            "route_reason": workflow_result.get("route_reason", ""),
            "timings_ms": _safe_serialize(workflow_result.get("timings_ms", {})),
            "total_ms": (workflow_result.get("timings_ms", {}) or {}).get("total_ms"),
            "query_error": workflow_result.get("query_error", ""),
            "total_count": workflow_result.get("total_count", 0),
            "degraded": workflow_result.get("degraded", False),
            "followup": workflow_result.get("followup", False),
            "intent_type": workflow_result.get("intent_type", ""),
            "followup_intent_type": workflow_result.get("followup_intent_type", ""),
            "analytical_intent": workflow_result.get("analytical_intent", ""),
            "clarification_reason": workflow_result.get("clarification_reason", ""),
            "risk_flag_timeout": workflow_result.get("risk_flag_timeout", False),
            "total_count_approximate": workflow_result.get("total_count_approximate", False),
            # Results summary (not full results — too large)
            "results_count": len(workflow_result.get("results", [])),
            "first_result_title": _first_title(workflow_result),
            # Rating (filled later)
            "rating": None,  # 1=good, 0=bad, None=unrated
            "rating_comment": None,
            "rated_at": None,
        }

        if self.jsonl_dual_write:
            self._append(trace)
        if self.sqlite_enabled:
            try:
                with self._connect() as conn:
                    user_id = self._upsert_user(conn, channel, user_identity)
                    self._upsert_trace(
                        conn,
                        trace,
                        user_id=user_id,
                        source="app",
                        has_suggestions=has_suggestions,
                    )
            except sqlite3.Error:
                logger.exception("failed to write feedback trace to sqlite trace_id=%s", trace_id)
        return trace_id

    def rate(self, trace_id: str, rating: int, comment: str | None = None) -> bool:
        """
        Rate a trace. rating: 1=relevant results, 0=wrong/irrelevant.
        Returns True if trace was found and updated.
        """
        if rating not in (0, 1):
            raise ValueError("rating must be 0 or 1")

        rated_at = datetime.now(timezone.utc).isoformat()
        if self.sqlite_enabled:
            try:
                with self._connect() as conn:
                    updated = self._rate_sqlite(conn, trace_id, rating, comment, rated_at)
                    if updated:
                        return True

                    trace = self._find_jsonl_trace(trace_id)
                    if trace is None:
                        return False

                    logger.warning(
                        "trace in JSONL but not SQLite; late import during rate trace_id=%s",
                        trace_id,
                    )
                    self._upsert_trace(conn, trace, user_id=None, source="late_import")
                    return self._rate_sqlite(conn, trace_id, rating, comment, rated_at)
            except sqlite3.Error:
                logger.exception("failed to rate feedback trace in sqlite trace_id=%s", trace_id)
                return False

        trace = self._find_jsonl_trace(trace_id)
        return trace is not None

    def get_stats(self) -> dict:
        """Get summary statistics."""
        traces = self._read_sqlite_traces()
        if not traces:
            traces = self._read_all()
        total = len(traces)
        rated = [t for t in traces if t.get("rating") is not None]
        good = [t for t in rated if t["rating"] == 1]
        bad = [t for t in rated if t["rating"] == 0]
        unrated = total - len(rated)

        # Route breakdown
        heuristic_only = sum(1 for t in traces if t.get("route_reason") == "heuristic_only")
        with_llm = sum(1 for t in traces if "llm" in (t.get("route_reason") or ""))
        no_results = sum(1 for t in traces if t.get("results_count", 0) == 0)

        return {
            "total_queries": total,
            "rated": len(rated),
            "unrated": unrated,
            "good": len(good),
            "bad": len(bad),
            "precision": len(good) / len(rated) if rated else 0,
            "heuristic_only": heuristic_only,
            "with_llm": with_llm,
            "no_results": no_results,
            "no_results_pct": no_results / total if total else 0,
        }

    def get_low_rated(self) -> list[dict]:
        """Get traces rated as bad — these are your improvement targets."""
        traces = self._read_sqlite_traces()
        if not traces:
            traces = self._read_all()
        return [t for t in traces if t.get("rating") == 0]

    def get_unrated(self, limit: int = 50) -> list[dict]:
        """Get unrated traces for review."""
        traces = self._read_sqlite_traces()
        if not traces:
            traces = self._read_all()
        return [t for t in traces if t.get("rating") is None][:limit]

    def get_recent(self, limit: int = 20) -> list[dict]:
        """Get most recent traces."""
        traces = self._read_sqlite_traces(limit=limit)
        if traces:
            return traces
        traces = self._read_all()
        return traces[-limit:]

    def get_usage_by_user(self) -> list[dict]:
        """Uso agregado por usuario (para el panel admin).

        Devuelve una fila por usuario con nº de consultas, calificadas,
        positivas y última actividad. Solo SQLite; lista vacía si está
        deshabilitado o aún no hay base.
        """
        if not self.sqlite_enabled or not self.db_path.exists():
            return []
        try:
            with self._connect() as conn:
                ensure_feedback_schema(conn)
                rows = conn.execute(
                    """
                    SELECT
                        COALESCE(u.email, '(anónimo)') AS email,
                        u.name AS name,
                        t.channel AS channel,
                        COUNT(*) AS queries,
                        SUM(CASE WHEN t.rating IS NOT NULL THEN 1 ELSE 0 END) AS rated,
                        SUM(CASE WHEN t.rating = 1 THEN 1 ELSE 0 END) AS good,
                        MAX(t.timestamp) AS last_activity
                    FROM traces t
                    LEFT JOIN users u ON u.user_id = t.user_id
                    GROUP BY t.user_id
                    ORDER BY queries DESC
                    """
                ).fetchall()
            return [
                {
                    "email": r["email"],
                    "name": r["name"],
                    "channel": r["channel"],
                    "queries": int(r["queries"]),
                    "rated": int(r["rated"]),
                    "good": int(r["good"]),
                    "precision": (int(r["good"]) / int(r["rated"])) if r["rated"] else None,
                    "last_activity": r["last_activity"],
                }
                for r in rows
            ]
        except sqlite3.Error:
            logger.exception("failed to compute usage by user from sqlite")
            return []

    def ensure_user(
        self,
        channel: str,
        user_identity: dict[str, Any] | None,
    ) -> str | None:
        """Materializa ``users.user_id`` para esa identidad y lo devuelve.

        Existe porque otras autoridades —Workspace, en R2— necesitan una FK
        válida contra ``users`` y NO pueden reconstruir el ``user_id`` por su
        cuenta: la forma de la clave (``streamlit:email:...``) y la resolución
        del conflicto por ``(channel, channel_user_key)`` viven en un solo sitio
        y deben seguir viviendo en uno solo. Duplicar el algoritmo produciría
        dos usuarios para la misma persona el día que cualquiera de los dos
        cambie.

        Es la misma escritura que ya hace ``log_trace``, expuesta sin trace de
        por medio; no altera en nada la semántica de traces. Devuelve ``None``
        si la identidad no alcanza para identificar a nadie (sin
        ``channel_user_key``, sin email y sin ``user_id``) o si SQLite está
        deshabilitado: el caller decide qué hacer con esa ausencia.
        """
        if not self.sqlite_enabled:
            return None
        try:
            with self._connect() as conn:
                ensure_feedback_schema(conn)
                return self._upsert_user(conn, channel, user_identity)
        except sqlite3.Error:
            logger.exception("failed to ensure user in sqlite channel=%s", channel)
            return None

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def _setup_sqlite(self) -> None:
        with self._connect() as conn:
            ensure_feedback_schema(conn)

    def _upsert_user(
        self,
        conn: sqlite3.Connection,
        channel: str,
        user_identity: dict[str, Any] | None,
    ) -> str | None:
        if not user_identity:
            return None

        email = _normalize_email(user_identity.get("email"))
        name = _clean_optional_text(user_identity.get("name"))
        channel_user_key = _clean_optional_text(user_identity.get("channel_user_key"))
        if not channel_user_key:
            channel_user_key = email or _clean_optional_text(user_identity.get("user_id"))
        if not channel_user_key:
            return None

        identity_channel = _clean_optional_text(user_identity.get("channel")) or channel
        is_anonymous = 1 if user_identity.get("is_anonymous", email is None) else 0
        user_id = _clean_optional_text(user_identity.get("user_id")) or _build_user_id(
            identity_channel,
            channel_user_key,
            email,
            bool(is_anonymous),
        )
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """
            INSERT INTO users (
                user_id, channel, channel_user_key, name, email,
                is_anonymous, primary_user_id, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)
            ON CONFLICT(channel, channel_user_key) DO UPDATE SET
                name = COALESCE(excluded.name, users.name),
                email = COALESCE(excluded.email, users.email),
                is_anonymous = excluded.is_anonymous,
                updated_at = excluded.updated_at
            """,
            (
                user_id,
                identity_channel,
                channel_user_key,
                name,
                email,
                is_anonymous,
                now,
                now,
            ),
        )
        row = conn.execute(
            "SELECT user_id FROM users WHERE channel = ? AND channel_user_key = ?",
            (identity_channel, channel_user_key),
        ).fetchone()
        return str(row["user_id"]) if row else user_id

    def _upsert_trace(
        self,
        conn: sqlite3.Connection,
        trace: dict[str, Any],
        user_id: str | None,
        source: str,
        migrated_at: str | None = None,
        has_suggestions: bool | None = None,
    ) -> None:
        row = _trace_to_row(
            trace,
            user_id=user_id,
            source=source,
            migrated_at=migrated_at,
            has_suggestions=has_suggestions,
        )
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

    def _rate_sqlite(
        self,
        conn: sqlite3.Connection,
        trace_id: str,
        rating: int,
        comment: str | None,
        rated_at: str,
    ) -> bool:
        row = conn.execute(
            "SELECT trace_json FROM traces WHERE trace_id = ?",
            (trace_id,),
        ).fetchone()
        if row is None:
            return False

        trace = _json_loads_dict(row["trace_json"])
        trace["rating"] = rating
        trace["rating_comment"] = comment
        trace["rated_at"] = rated_at
        conn.execute(
            """
            UPDATE traces
            SET rating = ?, rating_comment = ?, rated_at = ?, trace_json = ?
            WHERE trace_id = ?
            """,
            (rating, comment, rated_at, _json_dumps(trace), trace_id),
        )
        return True

    def _read_sqlite_traces(self, limit: int | None = None) -> list[dict]:
        if not self.sqlite_enabled or not self.db_path.exists():
            return []
        try:
            with self._connect() as conn:
                ensure_feedback_schema(conn)
                if limit is None:
                    rows = conn.execute(
                        "SELECT * FROM traces ORDER BY timestamp ASC, trace_id ASC"
                    ).fetchall()
                    return [_row_to_trace(row) for row in rows]
                rows = conn.execute(
                    """
                    SELECT * FROM traces
                    ORDER BY timestamp DESC, trace_id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
                return list(reversed([_row_to_trace(row) for row in rows]))
        except sqlite3.Error:
            logger.exception("failed to read feedback traces from sqlite")
            return []

    def _append(self, trace: dict) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(_json_dumps(trace) + "\n")

    def _read_all(self) -> list[dict]:
        if not self.path.exists():
            return []
        traces = []
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        traces.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return traces

    def _find_jsonl_trace(self, trace_id: str) -> dict | None:
        for trace in self._read_all():
            if trace.get("trace_id") == trace_id:
                return trace
        return None

    def _write_all(self, traces: list[dict]) -> None:
        """Rewrite entire file (for updates like rating)."""
        with self.path.open("w", encoding="utf-8") as f:
            for trace in traces:
                f.write(_json_dumps(trace) + "\n")


def ensure_feedback_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    # executescript closes any previous transaction. Serialize the additive
    # migration so concurrent store constructors cannot both add a column.
    conn.execute("BEGIN IMMEDIATE")
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(traces)")}
        for name, declaration in NATURAL_ALPHA_COLUMNS.items():
            if name not in columns:
                conn.execute(f"ALTER TABLE traces ADD COLUMN {name} {declaration}")
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (name, applied_at) VALUES (?, ?)",
            ("natural_alpha_v1", datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    except sqlite3.Error:
        conn.rollback()
        raise


def _safe_serialize(obj: Any) -> Any:
    """Make sure nested dicts are JSON-serializable."""
    if isinstance(obj, dict):
        return {k: _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_serialize(item) for item in obj]
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    return str(obj)


def _first_title(result: dict) -> str:
    """Extract title of first result for quick trace inspection."""
    results = result.get("results", [])
    if not results:
        return ""
    first = results[0]
    return str(
        first.get("nombre_del_procedimiento")
        or first.get("objeto_del_contrato")
        or ""
    )[:100]


def _trace_to_row(
    trace: dict[str, Any],
    user_id: str | None,
    source: str,
    migrated_at: str | None,
    has_suggestions: bool | None = None,
) -> dict[str, Any]:
    if has_suggestions is None:
        has_suggestions = bool(trace.get("suggestions") or trace.get("timeout_suggestions"))
    total_ms = trace.get("total_ms")
    if total_ms is None:
        total_ms = (trace.get("timings_ms", {}) or {}).get("total_ms")
    return {
        "trace_id": str(trace.get("trace_id", "")),
        **sqlite_values(trace),
        "timestamp": str(trace.get("timestamp") or datetime.now(timezone.utc).isoformat()),
        "user_id": user_id,
        "channel": str(trace.get("channel", "")),
        "user_query": str(trace.get("user_query", "")),
        "parsed_params_json": _json_dumps(_safe_serialize(trace.get("parsed_params", {}))),
        "resolved_params_json": _json_dumps(_safe_serialize(trace.get("resolved_params", {}))),
        "dataset_id": str(trace.get("dataset_id", "")),
        "soql_query": str(trace.get("soql_query", "")),
        "needs_llm": _bool_int(trace.get("needs_llm", False)),
        "needs_clarification": _bool_int(trace.get("needs_clarification", False)),
        "route_reason": str(trace.get("route_reason", "")),
        "timings_ms_json": _json_dumps(_safe_serialize(trace.get("timings_ms", {}))),
        "total_ms": total_ms,
        "query_error": str(trace.get("query_error", "")),
        "total_count": int(trace.get("total_count") or 0),
        "total_count_approximate": _bool_int(trace.get("total_count_approximate", False)),
        "degraded": _bool_int(trace.get("degraded", False)),
        "followup": _bool_int(trace.get("followup", False)),
        "intent_type": str(trace.get("intent_type", "")),
        "followup_intent_type": str(trace.get("followup_intent_type", "")),
        "analytical_intent": str(trace.get("analytical_intent", "")),
        "clarification_reason": str(trace.get("clarification_reason", "")),
        "has_suggestions": _bool_int(has_suggestions),
        "risk_flag_timeout": _bool_int(trace.get("risk_flag_timeout", False)),
        "results_count": int(trace.get("results_count") or 0),
        "first_result_title": str(trace.get("first_result_title", "")),
        "rating": trace.get("rating"),
        "rating_comment": trace.get("rating_comment"),
        "rated_at": trace.get("rated_at"),
        "trace_json": _json_dumps(trace),
        "source": source,
        "migrated_at": migrated_at,
    }


def _row_to_trace(row: sqlite3.Row) -> dict:
    trace = _json_loads_dict(row["trace_json"])
    trace["rating"] = row["rating"]
    trace["rating_comment"] = row["rating_comment"]
    trace["rated_at"] = row["rated_at"]
    return trace


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _json_loads_dict(raw: str | bytes | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _bool_int(value: Any) -> int:
    return 1 if bool(value) else 0


def _normalize_email(value: Any) -> str | None:
    text = _clean_optional_text(value)
    return text.lower() if text else None


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _build_user_id(channel: str, channel_user_key: str, email: str | None, is_anonymous: bool) -> str:
    if email:
        return f"{channel}:email:{email}"
    prefix = "anon" if is_anonymous else "user"
    return f"{channel}:{prefix}:{channel_user_key}"
