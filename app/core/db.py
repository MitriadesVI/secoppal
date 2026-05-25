from __future__ import annotations

import sqlite3
from pathlib import Path


def connect_sqlite(path: str | Path) -> sqlite3.Connection:
    """Open a SECOPPAL SQLite connection with production pragmas applied."""
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        timeout=30,
        isolation_level=None,
        # FastAPI handlers and Streamlit callbacks may cross worker threads.
        # We keep connections short-lived per operation, so sharing is limited,
        # but disabling this guard avoids false failures in those runtimes.
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn
