"""Read-only scoped dogfood coverage; host trace metadata is the only oracle."""
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import math
import os
from pathlib import Path
import shutil
import sqlite3
import stat
from statistics import fmean
from tempfile import TemporaryDirectory


class DogfoodReportError(ValueError):
    """Explicit input/schema/data failure, never a fabricated zero report."""


class _SnapshotChanged(Exception):
    """A source/copy witness changed; retry without querying the copy."""


_REQUIRED = {
    "users": {"user_id", "email"},
    "traces": {"trace_id", "timestamp", "user_id", "channel", "route_reason", "rating",
               "natural_alpha_mode", "natural_alpha_eligible", "natural_alpha_status"},
}


def _email(value):
    return value.strip().lower() if type(value) is str else None


def _instant(value, field):
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise DogfoodReportError(f"INVALID_{field}_TIMESTAMP") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DogfoodReportError(f"{field}_TIMESTAMP_REQUIRES_TIMEZONE")
    return parsed.astimezone(timezone.utc)


def _columns(conn):
    columns = {}
    for table, required in _REQUIRED.items():
        columns[table] = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        missing = required - columns[table]
        if missing:
            raise DogfoodReportError(f"MISSING_COLUMNS:{table}:" + ",".join(sorted(missing)))
    return columns["traces"]


def _stamp(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _fingerprint(path):
    before = path.stat()
    if not stat.S_ISREG(before.st_mode):
        raise DogfoodReportError("SNAPSHOT_SOURCE_NOT_REGULAR")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        if _stamp(os.fstat(source.fileno())) != _stamp(before):
            raise _SnapshotChanged
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
        if _stamp(os.fstat(source.fileno())) != _stamp(before):
            raise _SnapshotChanged
    if _stamp(path.stat()) != _stamp(before):
        raise _SnapshotChanged
    return _stamp(before), digest.digest()


def _source_files(path):
    # Conservatively reject even an apparently idle rollback journal: recovery
    # needs SQLite authority that this source reader deliberately never acquires.
    try:
        Path(str(path) + "-journal").lstat()
    except FileNotFoundError:
        pass
    else:
        raise DogfoodReportError("SNAPSHOT_ROLLBACK_JOURNAL_PRESENT")
    files = {"": path}
    wal = Path(str(path) + "-wal")
    try:
        wal.lstat()
    except FileNotFoundError:
        pass
    else:
        files["-wal"] = wal
    return files


def _source_fingerprints(path):
    files = _source_files(path)
    fingerprints = {suffix: _fingerprint(source) for suffix, source in files.items()}
    if files != _source_files(path):
        raise _SnapshotChanged
    return fingerprints


def _stable_snapshot(path, destination):
    before = _source_fingerprints(path)
    for suffix, expected in before.items():
        copied = Path(str(destination) + suffix)
        shutil.copyfile(Path(str(path) + suffix), copied)
        actual = _fingerprint(copied)
        if actual[1] != expected[1]:
            raise _SnapshotChanged
    if _source_fingerprints(path) != before:
        raise _SnapshotChanged


def _read_snapshot(path, email):
    # SQLite may create/update SHM in this disposable directory, never at source.
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        columns = _columns(conn)
        conn.create_function("normalized_email", 1, _email, deterministic=True)
        latency = "t.natural_alpha_latency_ms" if "natural_alpha_latency_ms" in columns else "NULL"
        total = "t.total_ms" if "total_ms" in columns else "NULL"
        rows = conn.execute(
            f"""SELECT t.trace_id, t.timestamp, t.route_reason, t.rating,
                           t.natural_alpha_mode, t.natural_alpha_eligible, t.natural_alpha_status,
                           {latency} AS alpha_latency, {total} AS turn_latency
                    FROM traces AS t JOIN users AS u ON u.user_id = t.user_id
                    WHERE t.channel = ? AND normalized_email(u.email) = ?
                    ORDER BY t.timestamp, t.trace_id""", ("streamlit", email),
        ).fetchall()
        return [dict(row) for row in rows]


def _read(db: Path, email: str):
    try:
        path = db.expanduser().resolve(strict=True)
        for _ in range(3):
            with TemporaryDirectory(prefix="natural-alpha-dogfood-") as directory:
                snapshot = Path(directory) / "feedback.sqlite"
                try:
                    _stable_snapshot(path, snapshot)
                except (_SnapshotChanged, FileNotFoundError):
                    continue
                return _read_snapshot(snapshot, email)
        raise DogfoodReportError("SNAPSHOT_UNSTABLE")
    except (OSError, sqlite3.Error) as exc:
        raise DogfoodReportError("DATABASE_READ_FAILED") from exc


def _reached(row):
    return (row["natural_alpha_mode"] == "alpha" and row["natural_alpha_eligible"] == 1
            and type(row["natural_alpha_status"]) is str and bool(row["natural_alpha_status"]))


def _latency(rows, field):
    values = [row[field] for row in rows if type(row[field]) in (int, float)
              and math.isfinite(row[field]) and row[field] >= 0]
    values.sort()
    missing = sum(row[field] is None for row in rows)
    return {"observed": len(values), "missing": missing,
            "invalid": len(rows) - len(values) - missing,
            "avg": fmean(values) if values else None,
            "p95": values[math.ceil(.95 * len(values)) - 1] if values else None}


def report_dogfood(db: Path | str, *, email: str, since: str, checkpoint: int = 50,
                   review_queue: bool = False) -> dict:
    """Freeze the window inclusively at the Nth Alpha-seam turn, if reached.

    Eligibility for this report means the observed Streamlit/email trace cohort;
    only the recorded Alpha metadata proves that a turn reached the Alpha seam.
    Timestamps are compared chronologically in UTC, then by trace_id. No query
    text is read. All utility and latency metrics below concern reached turns.
    """
    normalized = _email(email)
    if not normalized:
        raise DogfoodReportError("EMAIL_REQUIRED")
    if type(checkpoint) is not int or checkpoint < 1:
        raise DogfoodReportError("CHECKPOINT_MUST_BE_POSITIVE_INTEGER")
    start = _instant(since, "SINCE")
    rows = []
    for row in _read(Path(db), normalized):
        moment = _instant(row["timestamp"], "TRACE")
        if moment < start:
            continue
        if type(row["trace_id"]) is not str or type(row["route_reason"]) is not str:
            raise DogfoodReportError("INVALID_TRACE_IDENTIFIER_OR_ROUTE")
        rows.append((moment, row["trace_id"], row))
    rows.sort(key=lambda entry: entry[:2])
    window, scoped = [], []
    for moment, _, row in rows:
        row["timestamp"] = moment.isoformat()
        window.append(row)
        if _reached(row):
            scoped.append(row)
        if len(scoped) == checkpoint:
            break
    excluded = Counter(row["route_reason"] for row in window if not _reached(row))
    statuses = Counter(row["natural_alpha_status"] for row in scoped)
    positive = sum(row["rating"] == 1 for row in scoped)
    negative = sum(row["rating"] == 0 for row in scoped)
    unrated = sum(row["rating"] is None for row in scoped)
    rated = positive + negative
    total, reached = len(window), len(scoped)
    result = {
        "since": start.isoformat(), "checkpoint_target": checkpoint,
        "checkpoint_ready": reached == checkpoint,
        "window_end": {key: window[-1][key] for key in ("trace_id", "timestamp")} if window else None,
        "eligible_turns_window": total, "reached_seam": reached, "coverage_gap": total - reached,
        "coverage_rate": reached / total if total else None,
        "excluded_by_route": dict(sorted(excluded.items())), "alpha_status_counts": dict(sorted(statuses.items())),
        "blocked": statuses["MULTI_JOB_BLOCKED"], "invalid": statuses["INVALID"],
        "provider_errors": statuses["PROVIDER_ERROR"],
        "alpha_ratings": {"rated": rated, "positive": positive, "negative": negative,
                          "unrated": unrated, "invalid": reached - rated - unrated,
                          "utility_rate": positive / rated if rated else None,
                          "rating_coverage": rated / reached if reached else None},
        "alpha_latency_ms": _latency(scoped, "alpha_latency"),
        "turn_latency_ms": _latency(scoped, "turn_latency"),
    }
    if review_queue:
        result["review_queue"] = [
            {key: row[key] for key in ("trace_id", "timestamp", "route_reason", "natural_alpha_status", "rating")}
            for row in window
        ]
    return result
