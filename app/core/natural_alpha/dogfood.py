"""Read-only scoped dogfood coverage; host trace metadata is the only oracle."""
from collections import Counter
from contextlib import ExitStack, closing, contextmanager
from datetime import datetime, timezone
import errno
import hashlib
import math
import os
from pathlib import Path
import sqlite3
import stat
from statistics import fmean
from tempfile import TemporaryDirectory

from .coverage_report import coverage_summary, is_reached_alpha as _reached


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
    return info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _regular_stat(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise DogfoodReportError("SNAPSHOT_SOURCE_NOT_REGULAR")
    return info


def _check_descriptor(path, source):
    current = _regular_stat(path)
    if _stamp(os.fstat(source.fileno())) != _stamp(current):
        raise _SnapshotChanged
    return _stamp(current)


@contextmanager
def _open_regular(path):
    before = _stamp(_regular_stat(path))
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise DogfoodReportError("SNAPSHOT_SOURCE_NOT_REGULAR") from exc
        raise
    with os.fdopen(descriptor, "rb") as source:
        if _check_descriptor(path, source) != before:
            raise _SnapshotChanged
        yield source
        if _check_descriptor(path, source) != before:
            raise _SnapshotChanged


def _fingerprint(path, source, output=None):
    """Hash an accredited descriptor, optionally copying those exact bytes."""
    before = _check_descriptor(path, source)
    digest = hashlib.sha256()
    source.seek(0)
    while chunk := source.read(1024 * 1024):
        if _check_descriptor(path, source) != before:
            raise _SnapshotChanged
        digest.update(chunk)
        if output is not None:
            output.write(chunk)
    if _check_descriptor(path, source) != before:
        raise _SnapshotChanged
    return before, digest.digest()


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


def _copy_from_descriptor(path, source, destination):
    with destination.open("wb") as output:
        return _fingerprint(path, source, output)


def _stable_snapshot(path, destination):
    files = _source_files(path)
    with ExitStack() as stack:
        sources = {suffix: stack.enter_context(_open_regular(source)) for suffix, source in files.items()}
        before = {suffix: _fingerprint(files[suffix], source) for suffix, source in sources.items()}
        if files != _source_files(path):
            raise _SnapshotChanged
        for suffix, expected in before.items():
            copied = Path(str(destination) + suffix)
            if _copy_from_descriptor(files[suffix], sources[suffix], copied) != expected:
                raise _SnapshotChanged
            with _open_regular(copied) as copy:
                if _fingerprint(copied, copy)[1] != expected[1]:
                    raise _SnapshotChanged
        after = {suffix: _fingerprint(files[suffix], source) for suffix, source in sources.items()}
        if after != before or files != _source_files(path):
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
        # Extract only explicit host coverage fields, never raw trace JSON.
        fields = {
            "coverage_class": "$.natural_alpha_coverage_class",
            "coverage_excluded_route": "$.natural_alpha_excluded_by_route",
            "coverage_turn_id": "$.natural_alpha_coverage_turn_id",
        }
        projection = [
            (f"json_extract(t.trace_json, '{json_path}')" if "trace_json" in columns else "NULL") + f" AS {alias}"
            for alias, json_path in fields.items()
        ]
        rows = conn.execute(
            f"""SELECT t.trace_id, t.timestamp, t.route_reason, t.rating,
                           t.natural_alpha_mode, t.natural_alpha_eligible, t.natural_alpha_status,
                           {latency} AS alpha_latency, {total} AS turn_latency, {', '.join(projection)}
                    FROM traces AS t JOIN users AS u ON u.user_id = t.user_id
                    WHERE t.channel = ? AND normalized_email(u.email) = ?
                    ORDER BY t.timestamp, t.trace_id""", ("streamlit", email),
        ).fetchall()
        return [dict(row) for row in rows]


def _read(db: Path, email: str):
    try:
        path = db.expanduser().absolute()
        _regular_stat(path)
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

    The Streamlit/email cohort is traffic, not evidence of dogfood eligibility.
    Eligibility requires a host turn receipt; a reached turn also needs valid
    recorded Alpha metadata agreeing with it. Unaccredited turns fail closed.
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
    statuses = Counter(row["natural_alpha_status"] for row in scoped)
    positive = sum(row["rating"] == 1 for row in scoped)
    negative = sum(row["rating"] == 0 for row in scoped)
    unrated = sum(row["rating"] is None for row in scoped)
    rated = positive + negative
    reached = len(scoped)
    coverage = coverage_summary(window)
    result = {
        "since": start.isoformat(), "checkpoint_target": checkpoint,
        # Volume alone is not readiness: unaccredited turns block the checkpoint.
        "checkpoint_ready": reached == checkpoint and coverage["accounting_valid"],
        "window_end": {key: window[-1][key] for key in ("trace_id", "timestamp")} if window else None,
        "reached_seam": reached, **coverage,
        "alpha_status_counts": dict(sorted(statuses.items())),
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
