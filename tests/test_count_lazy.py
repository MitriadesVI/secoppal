"""Tests COUNT-QUERY-LATENCY-001 — query-first, count-lazy behaviour."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

from burr.core import State

from app.core.orchestrator import _query_first_then_count


def test_skips_count_when_under_limit():
    """When query returns < limit rows, count is skipped and total = len(rows)."""
    client = MagicMock()
    client.query.return_value = [{"id": i} for i in range(10)]
    client.count.return_value = 999  # should never be called

    total, results, timings, approx = _query_first_then_count(
        client, "ds", "count_soql", "soql", limit=50,
    )

    assert total == 10
    assert len(results) == 10
    assert approx is False
    assert timings["secop_count_ms"] == 0
    client.count.assert_not_called()


def test_runs_count_when_at_limit():
    """When query returns exactly limit rows, count runs and total comes from count."""
    client = MagicMock()
    client.query.return_value = [{"id": i} for i in range(50)]
    client.count.return_value = 347

    total, results, timings, approx = _query_first_then_count(
        client, "ds", "count_soql", "soql", limit=50,
    )

    assert total == 347
    assert approx is False
    client.count.assert_called_once()


def test_approximate_when_count_fails():
    """When count fails after query returned limit rows, total = limit and approximate=True."""
    client = MagicMock()
    client.query.return_value = [{"id": i} for i in range(50)]
    client.count.side_effect = Exception("timeout")

    total, results, timings, approx = _query_first_then_count(
        client, "ds", "count_soql", "soql", limit=50,
    )

    assert total == 50
    assert approx is True
    assert len(results) == 50


def test_query_failure_propagates():
    """If query itself fails, the exception propagates without calling count."""
    client = MagicMock()
    client.query.side_effect = Exception("query error")

    try:
        _query_first_then_count(client, "ds", "count_soql", "soql", limit=50)
        raise AssertionError("expected exception")
    except Exception as exc:
        assert str(exc) == "query error"
    client.count.assert_not_called()
