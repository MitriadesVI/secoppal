"""Synthetic SQLite coverage oracle, with scope decoys and write witnesses."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

from hypothesis import given, strategies as st
import pytest

from app.core.natural_alpha.dogfood import DogfoodReportError, report_dogfood
from app.core.natural_alpha import dogfood

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
    if row["natural_alpha_mode"] == "alpha" and "coverage_class" not in overrides:
        # Alpha samples carry the host turn receipt the report requires.
        row.update(coverage_class="REACHED_ALPHA", coverage_turn_id=trace_id)
    return row


_COVERAGE_JSON = {"coverage_class": "natural_alpha_coverage_class", "coverage_turn_id": "natural_alpha_coverage_turn_id",
                  "coverage_excluded_route": "natural_alpha_excluded_by_route"}


def _insert(conn, rows, fields):
    names = [*fields, "trace_json"]
    conn.executemany("INSERT INTO traces (" + ",".join(names) + ") VALUES (" + ",".join("?" for _ in names) + ")",
                     [[row[field] for field in fields] + [json.dumps({
                         key: row[alias] for alias, key in _COVERAGE_JSON.items() if row.get(alias) is not None
                     })] for row in rows])


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
        conn.execute("CREATE TABLE traces (" + ",".join(columns) + ", trace_json TEXT)")
        _insert(conn, rows, [column.split()[0] for column in columns])
    return db


def _directory_witness(directory):
    return {path.name: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_size,
                        path.stat().st_mtime_ns, path.stat().st_ctime_ns, path.stat().st_ino)
            for path in directory.iterdir()}


def _temporary_sqlite_path(uri):
    parsed = urlsplit(uri)
    assert parsed.scheme == "file" and parsed.query == "mode=ro"
    path = Path(unquote(parsed.path))
    assert path.parent.name.startswith("natural-alpha-dogfood-")
    return path


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
    # Volume is reached, but five unaccredited turns fail the accounting closed.
    assert not result["checkpoint_ready"] and not result["accounting_valid"]
    assert result["window_end"] == {"trace_id": "a7", "timestamp": "2026-09-13T00:07:00+00:00"}
    assert result["eligible_turns_window"] == 3
    assert result["reached_seam"] == 3
    assert result["coverage_gap"] is None
    assert result["coverage_rate"] is None
    assert result["traffic_turns_window"] == 8
    assert result["natural_work_turns_window"] == 3
    assert result["control_bypass"] == 0
    assert result["unaccredited_turns_window"] == 5
    assert result["excluded_by_route"] == {}
    assert result["unaccredited_by_route"] == {
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
    assert result["eligible_turns_window"] == 5
    assert result["traffic_turns_window"] == 11
    assert result["reached_seam"] == 5
    assert result["coverage_gap"] is None
    assert result["provider_errors"] == 1
    assert result["unaccredited_by_route"]["entity_profile_suppliers"] == 1
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
    temporary = _temporary_sqlite_path(observed[0][0])
    assert temporary != cohort.resolve()
    assert not temporary.parent.exists()
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


def test_read_only_report_sees_committed_wal_rows_without_changing_database_files(tmp_path, monkeypatch):
    db = _database(tmp_path, [_row("first")])
    with sqlite3.connect(db) as writer:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        row = _row("wal-only", 1)
        _insert(writer, [row], [field for field in row if not field.startswith("coverage_")])
        writer.commit()
        wal = Path(str(db) + "-wal")
        assert wal.exists()
        shm = Path(str(db) + "-shm")
        assert shm.exists()
        before = _directory_witness(tmp_path)
        original = sqlite3.connect
        connections = []

        def connect(database, **kwargs):
            connections.append(database)
            temporary = _temporary_sqlite_path(database)
            assert set(path.name for path in temporary.parent.iterdir()) == {
                "feedback.sqlite", "feedback.sqlite-wal",
            }
            return original(database, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", connect)
        result = report_dogfood(db, email=EMAIL, since=SINCE, checkpoint=2)
        assert result["checkpoint_ready"] and result["reached_seam"] == 2
        assert result["window_end"]["trace_id"] == "wal-only"
        assert _directory_witness(tmp_path) == before
        assert len(connections) == 1
        temporary = _temporary_sqlite_path(connections[0])
        assert temporary != db.resolve()
        assert not temporary.parent.exists()


def test_wal_source_without_shm_is_never_opened_or_given_new_sidecars(tmp_path):
    fixture = tmp_path / "writer"
    fixture.mkdir()
    db = _database(fixture, [_row("first")])
    source = tmp_path / "source"
    source.mkdir()
    copied_db = source / "source.sqlite"
    with sqlite3.connect(db) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("UPDATE traces SET trace_id='wal-only'")
        writer.commit()
        shutil.copyfile(db, copied_db)
        shutil.copyfile(Path(str(db) + "-wal"), Path(str(copied_db) + "-wal"))
        before = _directory_witness(source)
        assert set(before) == {"source.sqlite", "source.sqlite-wal"}
        result = report_dogfood(copied_db, email=EMAIL, since=SINCE, checkpoint=1)
        assert result["window_end"]["trace_id"] == "wal-only"
        assert _directory_witness(source) == before


def test_database_symlink_is_rejected_instead_of_resolved(tmp_path):
    db = _database(tmp_path, [_row("foreign-through-db-link")])
    alias = tmp_path / "alias.sqlite"
    alias.symlink_to(db)
    with patch.object(sqlite3, "connect", wraps=sqlite3.connect) as connect:
        with pytest.raises(DogfoodReportError, match="SNAPSHOT_SOURCE_NOT_REGULAR"):
            result = report_dogfood(alias, email=EMAIL, since=SINCE, checkpoint=1)
            pytest.fail(f"symlink admitted: {result['window_end']}")
    connect.assert_not_called()


def test_foreign_wal_symlink_cannot_inject_rows_into_the_selected_database(tmp_path):
    own = tmp_path / "own"
    own.mkdir()
    db = _database(own, [_row("OWN")])
    other = tmp_path / "foreign"
    other.mkdir()
    foreign = other / "foreign.sqlite"
    shutil.copyfile(db, foreign)
    with closing(sqlite3.connect(db)) as setup:
        setup.execute("PRAGMA journal_mode=WAL")
    with closing(sqlite3.connect(foreign)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("UPDATE traces SET trace_id='FOREIGN-DECOY'")
        writer.commit()
        assert report_dogfood(foreign, email=EMAIL, since=SINCE, checkpoint=1)["window_end"]["trace_id"] == "FOREIGN-DECOY"
        alias = Path(str(db) + "-wal")
        alias.symlink_to(Path(str(foreign) + "-wal"))
        before = _directory_witness(own), _directory_witness(other)
        with patch.object(sqlite3, "connect", wraps=sqlite3.connect) as connect:
            with pytest.raises(DogfoodReportError, match="SNAPSHOT_SOURCE_NOT_REGULAR"):
                result = report_dogfood(db, email=EMAIL, since=SINCE, checkpoint=1)
                pytest.fail(f"foreign WAL admitted: {result['window_end']}")
        connect.assert_not_called()
        assert (_directory_witness(own), _directory_witness(other)) == before


def test_source_uses_one_nofollow_descriptor_and_never_path_copyfile(cohort, monkeypatch):
    original_open = os.open
    opened = []
    copied_descriptors = []
    copy = dogfood._copy_from_descriptor

    def open_file(path, flags, *args, **kwargs):
        descriptor = original_open(path, flags, *args, **kwargs)
        if Path(path) == cohort:
            opened.append((descriptor, flags))
        return descriptor

    def copy_descriptor(path, source, destination):
        copied_descriptors.append(source.fileno())
        return copy(path, source, destination)

    monkeypatch.setattr(os, "open", open_file)
    monkeypatch.setattr(dogfood, "_copy_from_descriptor", copy_descriptor)
    with patch.object(shutil, "copyfile", side_effect=AssertionError("source path must not be reopened")):
        result = report_dogfood(cohort, email=EMAIL, since=SINCE)
    assert result["reached_seam"] == 5
    assert len(opened) == 1 and copied_descriptors == [opened[0][0]]
    if hasattr(os, "O_NOFOLLOW"):
        assert opened[0][1] & os.O_NOFOLLOW
    with pytest.raises(OSError):
        os.fstat(opened[0][0])


@pytest.mark.parametrize("without_nofollow", [False, True])
def test_symlink_inserted_between_lstat_and_open_is_rejected_before_reading(cohort, monkeypatch, without_nofollow):
    if without_nofollow:
        monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    original_open = os.open
    parked = cohort.with_suffix(".parked")
    changed = False

    def open_file(path, flags, *args, **kwargs):
        nonlocal changed
        if Path(path) == cohort and not changed:
            changed = True
            cohort.rename(parked)
            cohort.symlink_to(parked)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", open_file)
    with patch.object(dogfood, "_fingerprint", wraps=dogfood._fingerprint) as fingerprint:
        with patch.object(sqlite3, "connect") as connect:
            with pytest.raises(DogfoodReportError, match="SNAPSHOT_SOURCE_NOT_REGULAR"):
                report_dogfood(cohort, email=EMAIL, since=SINCE)
    assert changed
    fingerprint.assert_not_called()
    connect.assert_not_called()


def test_regular_inode_replacement_between_lstat_and_open_forces_retry(cohort, monkeypatch):
    original_open = os.open
    parked = cohort.with_suffix(".parked")
    opens = 0

    def open_file(path, flags, *args, **kwargs):
        nonlocal opens
        if Path(path) == cohort:
            opens += 1
            if opens == 1:
                cohort.rename(parked)
                shutil.copyfile(parked, cohort)
                assert cohort.lstat().st_ino != parked.lstat().st_ino
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", open_file)
    result = report_dogfood(cohort, email=EMAIL, since=SINCE)
    assert result["reached_seam"] == 5
    assert opens == 2


@pytest.mark.parametrize("event", ["replace", "disappear", "symlink"])
def test_path_changed_after_copy_is_not_accepted_as_the_accredited_source(cohort, monkeypatch, event):
    copy = dogfood._copy_from_descriptor
    files = dogfood._source_files
    parked = cohort.with_suffix(".parked")
    destinations = []

    def source_files(path):
        if event == "disappear" and parked.exists() and not cohort.exists():
            parked.rename(cohort)  # The fabricated source returns for the retry.
        return files(path)

    def copy_descriptor(path, source, destination):
        copied = copy(path, source, destination)
        destinations.append(destination)
        if len(destinations) == 1:
            cohort.rename(parked)
            if event == "replace":
                shutil.copyfile(parked, cohort)
            elif event == "symlink":
                cohort.symlink_to(parked)
        return copied

    monkeypatch.setattr(dogfood, "_source_files", source_files)
    monkeypatch.setattr(dogfood, "_copy_from_descriptor", copy_descriptor)
    with patch.object(sqlite3, "connect", wraps=sqlite3.connect) as connect:
        if event == "symlink":
            with pytest.raises(DogfoodReportError, match="SNAPSHOT_SOURCE_NOT_REGULAR"):
                report_dogfood(cohort, email=EMAIL, since=SINCE)
            connect.assert_not_called()
            assert len(destinations) == 1
        else:
            assert report_dogfood(cohort, email=EMAIL, since=SINCE)["reached_seam"] == 5
            connect.assert_called_once()
            assert len(destinations) == 2
    assert all(not destination.parent.exists() for destination in destinations)


@pytest.mark.parametrize("member,kind", [("db", "directory"), ("db", "fifo"), ("wal", "directory")])
def test_non_regular_source_members_fail_explicitly_before_sqlite(tmp_path, member, kind):
    if member == "wal":
        db = _database(tmp_path, [_row("own")])
        target = Path(str(db) + "-wal")
    else:
        db = target = tmp_path / "not-regular.sqlite"
    if kind == "fifo":
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO is unavailable on this platform")
        os.mkfifo(target)
    else:
        target.mkdir()
    with patch.object(sqlite3, "connect") as connect:
        with pytest.raises(DogfoodReportError, match="SNAPSHOT_SOURCE_NOT_REGULAR"):
            report_dogfood(db, email=EMAIL, since=SINCE)
    connect.assert_not_called()


@pytest.mark.parametrize("event", ["appears", "disappears"])
def test_wal_membership_change_retries_and_keeps_committed_rows(tmp_path, monkeypatch, event):
    db = _database(tmp_path, [_row("first")])
    wal = Path(str(db) + "-wal")
    with closing(sqlite3.connect(db)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        if event == "disappears":
            writer.execute("UPDATE traces SET trace_id='wal-only'")
            writer.commit()
        assert wal.exists() is (event == "disappears")
        copy_from_descriptor = dogfood._copy_from_descriptor
        destinations = []
        changed = False
        after_writer = None

        def copy(source, descriptor, destination):
            nonlocal changed, after_writer
            copied = copy_from_descriptor(source, descriptor, destination)
            destinations.append(destination)
            if not changed:
                changed = True
                if event == "appears":
                    writer.execute("UPDATE traces SET trace_id='wal-only'")
                    writer.commit()
                else:
                    writer.close()  # Checkpoints and removes this fabricated WAL.
                after_writer = _directory_witness(tmp_path)
            return copied

        monkeypatch.setattr(dogfood, "_copy_from_descriptor", copy)
        original = sqlite3.connect
        connections = []

        def connect(database, **kwargs):
            connections.append(_temporary_sqlite_path(database))
            return original(database, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", connect)
        result = report_dogfood(db, email=EMAIL, since=SINCE, checkpoint=1)
        assert result["window_end"]["trace_id"] == "wal-only"
        attempts = {path.parent for path in destinations}
        assert len(attempts) == 2
        assert all(not path.exists() for path in attempts)
        assert len(connections) == 1
        assert _directory_witness(tmp_path) == after_writer


@pytest.mark.parametrize("fault", ["source_metadata", "copied_bytes"])
def test_three_unstable_copies_fail_closed_without_sqlite_or_leaked_temporaries(cohort, monkeypatch, fault):
    copy_from_descriptor = dogfood._copy_from_descriptor
    destinations = []

    def copy(source, descriptor, destination):
        copied = copy_from_descriptor(source, descriptor, destination)
        destinations.append(destination)
        if fault == "source_metadata":
            stamp = source.stat()
            os.utime(source, ns=(stamp.st_atime_ns, stamp.st_mtime_ns + 1_000_000))
        else:
            with destination.open("ab") as output:
                output.write(b"fabricated torn copy")
        return copied

    monkeypatch.setattr(dogfood, "_copy_from_descriptor", copy)
    with patch.object(sqlite3, "connect") as connect:
        with pytest.raises(DogfoodReportError, match="SNAPSHOT_UNSTABLE"):
            report_dogfood(cohort, email=EMAIL, since=SINCE)
    connect.assert_not_called()
    assert len(destinations) == 3
    assert all(not path.parent.exists() for path in destinations)


def test_active_rollback_journal_is_rejected_without_touching_source(tmp_path):
    db = _database(tmp_path, [_row("committed")])
    with closing(sqlite3.connect(db)) as writer:
        writer.execute("UPDATE traces SET trace_id='uncommitted'")
        journal = Path(str(db) + "-journal")
        assert journal.stat().st_size > 0
        before = _directory_witness(tmp_path)
        with patch.object(sqlite3, "connect") as connect:
            with pytest.raises(DogfoodReportError, match="SNAPSHOT_ROLLBACK_JOURNAL_PRESENT"):
                report_dogfood(db, email=EMAIL, since=SINCE)
        connect.assert_not_called()
        assert _directory_witness(tmp_path) == before


def test_temporaries_are_removed_on_schema_failure_and_source_reads_are_streamed(tmp_path, monkeypatch):
    db = tmp_path / "legacy.sqlite"
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("CREATE TABLE users (user_id TEXT, email TEXT)")
        conn.commit()
    original = dogfood.TemporaryDirectory
    directories = []

    def temporary(**kwargs):
        directory = original(**kwargs)
        directories.append(Path(directory.name))
        return directory

    monkeypatch.setattr(dogfood, "TemporaryDirectory", temporary)
    with patch.object(Path, "read_bytes", side_effect=AssertionError("must stream source hashes")):
        with pytest.raises(DogfoodReportError, match="MISSING_COLUMNS:traces"):
            report_dogfood(db, email=EMAIL, since=SINCE)
    assert len(directories) == 1
    assert all(not path.exists() for path in directories)


@given(st.lists(st.booleans(), min_size=3, max_size=3))
def test_snapshot_retry_budget_queries_only_the_first_stable_copy(stable_attempts):
    effects = [None if stable else dogfood._SnapshotChanged() for stable in stable_attempts]
    with patch.object(dogfood, "_stable_snapshot", side_effect=effects) as snapshot:
        with patch.object(dogfood, "_read_snapshot", return_value=[]) as query:
            if any(stable_attempts):
                assert dogfood._read(Path(__file__), EMAIL) == []
                expected = stable_attempts.index(True) + 1
                query.assert_called_once()
            else:
                with pytest.raises(DogfoodReportError, match="SNAPSHOT_UNSTABLE"):
                    dogfood._read(Path(__file__), EMAIL)
                expected = 3
                query.assert_not_called()
    assert snapshot.call_count == expected
    assert all(not call.args[1].parent.exists() for call in snapshot.call_args_list)


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
    ], cwd=tmp_path, capture_output=True, text=True)
    # Unaccredited turns: the report still prints, but the command fails closed.
    assert completed.returncode == 3
    assert json.loads(completed.stdout)["reached_seam"] == 3
    (tmp_path / "clean").mkdir()
    clean = _database(tmp_path / "clean", [_row("only-alpha")])
    assert subprocess.run([sys.executable, str(SCRIPT), "--db", str(clean), "--email", EMAIL, "--since", SINCE],
                          cwd=tmp_path, capture_output=True, text=True).returncode == 0
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
    assert result["traffic_turns_window"] == cutoff
    assert result["eligible_turns_window"] == expected_reached
    assert result["unaccredited_turns_window"] == cutoff - expected_reached
    assert result["reached_seam"] == expected_reached
    valid = cutoff == expected_reached
    assert result["accounting_valid"] is valid
    assert result["coverage_gap"] == (0 if valid else None)
    assert result["checkpoint_ready"] is (len(positions) >= checkpoint and valid)
    assert result["coverage_rate"] == (1 if expected_reached and valid else None)
