"""Synthetic SQLite coverage oracle, with scope decoys and write witnesses."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from unittest.mock import patch

from hypothesis import given, strategies as st
import pytest

from app.core.natural_alpha.dogfood import DogfoodReportError, report_dogfood

EMAIL = "alpha@example.test"
SINCE = "2026-09-13T00:00:00+00:00"
SCRIPT = Path(__file__).resolve().parents[1] / "scripts/report_natural_alpha_dogfood.py"


def _row(trace_id, minute=0, **overrides):
    row = {"trace_id": trace_id,
           "timestamp": datetime(2026, 9, 13, 0, minute, tzinfo=timezone.utc).isoformat(),
           "user_id": "target", "channel": "streamlit", "route_reason": "heuristic_only",
           "rating": None, "natural_alpha_mode": "alpha", "natural_alpha_eligible": 1,
           "natural_alpha_status": "MATCHED", "natural_alpha_latency_ms": None,
           "total_ms": None, "user_query": "PRIVATE RAW QUERY alpha@example.test"}
    row.update(overrides)
    return row


def _database(tmp_path, rows, *, latency=True):
    db = tmp_path / "feedback sample.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE users (user_id TEXT PRIMARY KEY, email TEXT)")
        conn.executemany("INSERT INTO users VALUES (?, ?)", [
            ("target", "\t ALPHA@EXAMPLE.TEST \n"), ("other", "other@example.test"),
        ])
        columns = ["trace_id TEXT PRIMARY KEY", "timestamp TEXT", "user_id TEXT", "channel TEXT",
                   "route_reason TEXT", "rating INTEGER", "natural_alpha_mode TEXT",
                   "natural_alpha_eligible INTEGER", "natural_alpha_status TEXT", "user_query TEXT"]
        if latency:
            columns += ["natural_alpha_latency_ms REAL", "total_ms REAL"]
        conn.execute("CREATE TABLE traces (" + ",".join(columns) + ")")
        fields = [column.split()[0] for column in columns]
        conn.executemany("INSERT INTO traces (" + ",".join(fields) + ") VALUES ("
                         + ",".join("?" for _ in fields) + ")",
                         [[row[field] for field in fields] for row in rows])
    return db


@pytest.fixture
def cohort(tmp_path):
    return _database(tmp_path, [
        _row("p0", 0, route_reason="entity_profile_overview", natural_alpha_mode=None,
             natural_alpha_eligible=None, natural_alpha_status=None, rating=0),
        _row("f1", 1, route_reason="reset", natural_alpha_mode=None,
             natural_alpha_eligible=None, natural_alpha_status=None, rating=1),
        _row("a2", 2, rating=1, natural_alpha_latency_ms=100, total_ms=1000),
        _row("x3", 3, route_reason="not_eligible", natural_alpha_eligible=0),
        _row("s4", 4, route_reason="shadow_route", natural_alpha_mode="shadow"),
        _row("m5", 5, route_reason="missing_status", natural_alpha_status=None),
        _row("a6", 6, route_reason="natural_alpha_multi_job_stepwise",
             natural_alpha_status="MULTI_JOB_BLOCKED", natural_alpha_latency_ms=300),
        _row("a7", 7, natural_alpha_status="INVALID", rating=0, natural_alpha_latency_ms=200, total_ms=3000),
        _row("p8", 8, route_reason="entity_profile_suppliers", natural_alpha_mode=None,
             natural_alpha_eligible=None, natural_alpha_status=None),
        _row("a9", 9, natural_alpha_status="PROVIDER_ERROR"),
        _row("a10", 10, rating=1, natural_alpha_latency_ms=400, total_ms=5000),
        _row("decoy-email", 0, user_id="other"),
        _row("decoy-channel", 0, channel="telegram"),
        _row("decoy-before", 0, timestamp="2026-09-12T23:59:59.999999+00:00"),
    ])


def test_window_extends_to_nth_reached_seam_and_excludes_later_turns(cohort):
    result = report_dogfood(cohort, email=" ALPHA@EXAMPLE.TEST ", since=SINCE, checkpoint=3)
    assert result["checkpoint_ready"]
    assert result["window_end"] == {"trace_id": "a7", "timestamp": "2026-09-13T00:07:00+00:00"}
    assert result["eligible_turns_window"] == 8
    assert result["reached_seam"] == 3
    assert result["coverage_gap"] == 5
    assert result["coverage_rate"] == 3 / 8
    assert result["excluded_by_route"] == {
        "entity_profile_overview": 1, "reset": 1, "not_eligible": 1, "shadow_route": 1, "missing_status": 1,
    }
    assert result["alpha_status_counts"] == {"MATCHED": 1, "MULTI_JOB_BLOCKED": 1, "INVALID": 1}
    assert result["blocked"] == result["invalid"] == 1
    assert result["provider_errors"] == 0
    assert result["alpha_ratings"] == {
        "rated": 2, "positive": 1, "negative": 1, "unrated": 1, "invalid": 0,
        "utility_rate": .5, "rating_coverage": 2 / 3,
    }
    assert result["alpha_latency_ms"] == {"observed": 3, "missing": 0, "invalid": 0, "avg": 200, "p95": 300}
    assert result["turn_latency_ms"] == {"observed": 2, "missing": 1, "invalid": 0, "avg": 2000, "p95": 3000}


def test_checkpoint_not_ready_uses_all_available_scoped_turns(cohort):
    result = report_dogfood(cohort, email=EMAIL, since=SINCE)
    assert result["checkpoint_target"] == 50
    assert not result["checkpoint_ready"]
    assert result["eligible_turns_window"] == 11
    assert result["reached_seam"] == 5
    assert result["coverage_gap"] == 6
    assert result["provider_errors"] == 1
    assert result["excluded_by_route"]["entity_profile_suppliers"] == 1
    assert result["alpha_ratings"]["utility_rate"] == 2 / 3
    assert result["alpha_latency_ms"]["avg"] == 250


def test_chronological_timestamp_and_trace_id_break_ties_deterministically(tmp_path):
    db = _database(tmp_path, [
        _row("b", timestamp="2026-09-13T00:00:00+00:00"),
        _row("a", timestamp="2026-09-13T01:00:00+01:00"),
        _row("pre", timestamp="2026-09-12T23:59:59.999999+00:00"),
        _row("post", timestamp="2026-09-13T00:00:00.000001+00:00"),
    ])
    result = report_dogfood(db, email=EMAIL, since="2026-09-12T19:00:00-05:00", checkpoint=1)
    assert result["window_end"] == {"trace_id": "a", "timestamp": SINCE}
    assert result["eligible_turns_window"] == result["reached_seam"] == 1


def test_default_redaction_and_minimal_optional_review_queue(cohort):
    default = report_dogfood(cohort, email=EMAIL, since=SINCE)
    assert "review_queue" not in default
    explicit = report_dogfood(cohort, email=EMAIL, since=SINCE, checkpoint=3, review_queue=True)
    assert len(explicit["review_queue"]) == 8
    for item in explicit["review_queue"]:
        assert set(item) == {"trace_id", "timestamp", "route_reason", "natural_alpha_status", "rating"}
    for result in (default, explicit):
        encoded = json.dumps(result)
        assert EMAIL not in encoded
        assert "PRIVATE RAW QUERY" not in encoded
        assert "user_query" not in encoded


def test_read_only_uri_and_database_hash_mtime_are_preserved(cohort, monkeypatch):
    before = (hashlib.sha256(cohort.read_bytes()).hexdigest(), cohort.stat().st_mtime_ns)
    original = sqlite3.connect
    observed = []
    statements = []

    def connect(database, **kwargs):
        observed.append((database, kwargs))
        conn = original(database, **kwargs)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(sqlite3, "connect", connect)
    report_dogfood(cohort, email=EMAIL, since=SINCE)
    assert len(observed) == 1
    assert observed[0][0] == cohort.resolve().as_uri() + "?mode=ro"
    assert observed[0][1] == {"uri": True}
    assert all(statement.lstrip().split()[0] in {"PRAGMA", "BEGIN", "SELECT"} for statement in statements)
    assert not any("user_query" in statement for statement in statements)
    assert (hashlib.sha256(cohort.read_bytes()).hexdigest(), cohort.stat().st_mtime_ns) == before


def test_missing_schema_and_missing_database_fail_without_creation(tmp_path):
    db = tmp_path / "legacy.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE users (user_id TEXT, email TEXT)")
    before = db.read_bytes()
    with pytest.raises(DogfoodReportError, match="MISSING_COLUMNS:traces"):
        report_dogfood(db, email=EMAIL, since=SINCE)
    assert db.read_bytes() == before
    missing = tmp_path / "missing.sqlite"
    with pytest.raises(DogfoodReportError, match="DATABASE_READ_FAILED"):
        report_dogfood(missing, email=EMAIL, since=SINCE)
    assert not missing.exists()


def test_read_only_report_sees_committed_wal_rows_without_changing_database_files(tmp_path):
    db = _database(tmp_path, [_row("first")])
    with sqlite3.connect(db) as writer:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        row = _row("wal-only", 1)
        fields = list(row)
        writer.execute("INSERT INTO traces (" + ",".join(fields) + ") VALUES ("
                       + ",".join("?" for _ in fields) + ")", list(row.values()))
        writer.commit()
        wal = Path(str(db) + "-wal")
        assert wal.exists()
        files = {path.name for path in tmp_path.iterdir()}
        before = {path: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
                  for path in (db, wal)}
        result = report_dogfood(db, email=EMAIL, since=SINCE, checkpoint=2)
        assert result["checkpoint_ready"] and result["reached_seam"] == 2
        assert result["window_end"]["trace_id"] == "wal-only"
        assert {path.name for path in tmp_path.iterdir()} == files
        for path in (db, wal):
            assert (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns) == before[path]


def test_missing_alpha_column_is_an_explicit_schema_failure(tmp_path):
    db = _database(tmp_path, [])
    with sqlite3.connect(db) as conn:
        conn.execute("ALTER TABLE traces DROP COLUMN natural_alpha_status")
    before = db.read_bytes()
    with pytest.raises(DogfoodReportError, match="MISSING_COLUMNS:traces:natural_alpha_status"):
        report_dogfood(db, email=EMAIL, since=SINCE)
    assert db.read_bytes() == before


def test_invalid_latency_samples_are_counted_without_inventing_a_value(tmp_path):
    db = _database(tmp_path, [_row("bad-text", natural_alpha_latency_ms="unknown"),
                              _row("negative", 1, natural_alpha_latency_ms=-10),
                              _row("infinite", 2, natural_alpha_latency_ms=float("inf"))])
    result = report_dogfood(db, email=EMAIL, since=SINCE)
    assert result["alpha_latency_ms"] == {"observed": 0, "missing": 0, "invalid": 3, "avg": None, "p95": None}


def test_optional_latency_and_unrated_denominators_are_null(tmp_path):
    db = _database(tmp_path, [_row("single")], latency=False)
    result = report_dogfood(db, email=EMAIL, since=SINCE)
    assert result["alpha_ratings"]["rated"] == 0
    assert result["alpha_ratings"]["utility_rate"] is None
    assert result["alpha_latency_ms"] == {"observed": 0, "missing": 1, "invalid": 0, "avg": None, "p95": None}
    empty = report_dogfood(db, email="other@example.test", since=SINCE)
    assert empty["eligible_turns_window"] == empty["reached_seam"] == 0
    assert empty["coverage_rate"] is None
    assert empty["alpha_ratings"]["rating_coverage"] is None
    assert empty["window_end"] is None
    assert not empty["checkpoint_ready"]


@pytest.mark.parametrize("kwargs,reason", [
    ({"since": "2026-09-13"}, "TIMEZONE"), ({"since": "not a time"}, "TIMESTAMP"),
    ({"checkpoint": 0}, "CHECKPOINT"), ({"checkpoint": True}, "CHECKPOINT"),
    ({"email": " "}, "EMAIL_REQUIRED"),
])
def test_invalid_inputs_do_not_become_empty_reports(cohort, kwargs, reason):
    with pytest.raises(DogfoodReportError, match=reason):
        report_dogfood(cohort, **{"email": EMAIL, "since": SINCE, **kwargs})


def test_query_parameters_cannot_expand_email_scope(cohort):
    result = report_dogfood(cohort, email="' OR 1=1 --", since=SINCE)
    assert result["eligible_turns_window"] == 0


def test_cli_is_read_only_and_redacted(cohort, tmp_path):
    before = (cohort.read_bytes(), cohort.stat().st_mtime_ns)
    completed = subprocess.run([
        sys.executable, str(SCRIPT), "--db", str(cohort), "--email", EMAIL,
        "--since", SINCE, "--checkpoint", "3",
    ], cwd=tmp_path, check=True, capture_output=True, text=True)
    assert json.loads(completed.stdout)["reached_seam"] == 3
    assert EMAIL not in completed.stdout + completed.stderr
    assert "PRIVATE RAW QUERY" not in completed.stdout
    assert (cohort.read_bytes(), cohort.stat().st_mtime_ns) == before


@given(st.lists(st.booleans(), max_size=60), st.integers(min_value=1, max_value=50))
def test_window_partition_and_cutoff_invariants(reached_flags, checkpoint):
    rows = [_row(f"trace-{index:03}", index,
                 natural_alpha_mode="alpha" if reached else None,
                 natural_alpha_status="MATCHED" if reached else None,
                 alpha_latency=None, turn_latency=None)
            for index, reached in enumerate(reached_flags)]
    positions = [index for index, reached in enumerate(reached_flags) if reached]
    cutoff = positions[checkpoint - 1] + 1 if len(positions) >= checkpoint else len(rows)
    with patch("app.core.natural_alpha.dogfood._read", return_value=rows):
        result = report_dogfood("synthetic-unused-path", email=EMAIL, since=SINCE, checkpoint=checkpoint)
    expected_reached = sum(reached_flags[:cutoff])
    assert result["eligible_turns_window"] == cutoff
    assert result["reached_seam"] == expected_reached
    assert result["coverage_gap"] + expected_reached == cutoff
    assert result["checkpoint_ready"] is (len(positions) >= checkpoint)
    assert result["coverage_rate"] == (expected_reached / cutoff if cutoff else None)
