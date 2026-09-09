"""Typed floating timestamp emulation for offline SoQL transport fixtures.

Invalid fixture text maps to missing typed timestamp; no eligibility policy lives here.
"""
from datetime import datetime


def register_dates(conn):
    def truncate_year(value):
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return f'{parsed.year:04d}-01-01T00:00:00.000'
    conn.create_function('date_trunc_y', 1, truncate_year)
    conn.create_function('date_extract_y', 1, lambda value: int(truncate_year(value)[:4])
                         if truncate_year(value) is not None else None)


# Explicit opt-in plugin for frozen SQL fixtures which predate date_extract_y.
# Only the SQL dialect is extended; all rows, assertions and runtime are unchanged.
import sqlite3
import pytest


@pytest.fixture(autouse=True)
def support_soql_dates(monkeypatch):
    original = sqlite3.connect
    def connect(*args, **kwargs):
        connection = original(*args, **kwargs)
        register_dates(connection)
        return connection
    monkeypatch.setattr(sqlite3, 'connect', connect)
