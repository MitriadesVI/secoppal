"""Etapa 6 H1 — el count no bloquea la entrega de filas; nunca fabrica EMPTY/0."""
from __future__ import annotations

import pytest

from app.core.instrumentation import attach_telemetry
from app.core.outcome import Outcome, OutcomeReason, classify_outcome
from app.core.workflow.helpers import _query_first_then_count


class FakeClient:
    """Cliente con query/count controlables. with_timeout falla → fuerza el fallback
    a self (como en entornos sin sodapy real)."""

    def __init__(self, rows, count_value=0, count_exc=None):
        self.rows = rows
        self.count_value = count_value
        self.count_exc = count_exc
        self.count_calls = 0

    def query(self, dataset_id, soql):
        return list(self.rows)

    def count(self, dataset_id, soql):
        self.count_calls += 1
        if self.count_exc is not None:
            raise self.count_exc
        return self.count_value

    def with_timeout(self, timeout):
        raise RuntimeError("sin sodapy real")


# ── _query_first_then_count: el count nunca borra las filas ──────────────────

def test_below_limit_skips_count_exact_total():
    c = FakeClient(rows=[{}] * 3)
    total, res, t, approx = _query_first_then_count(c, "ds", "cnt", "q", limit=50)
    assert total == 3 and approx is False
    assert t["secop_count_ms"] == 0  # no se consultó count
    assert c.count_calls == 0
    assert "rows_ready_ms" in t


def test_full_page_with_fast_count_is_exact():
    c = FakeClient(rows=[{}] * 50, count_value=1234)
    total, res, t, approx = _query_first_then_count(c, "ds", "cnt", "q", limit=50)
    assert total == 1234 and approx is False and len(res) == 50


def test_count_timeout_keeps_rows_as_approximate():
    c = FakeClient(rows=[{}] * 50, count_exc=TimeoutError("read timed out"))
    total, res, t, approx = _query_first_then_count(c, "ds", "cnt", "q", limit=50)
    assert len(res) == 50            # filas conservadas
    assert total == 50               # X+ aproximado, NUNCA 0
    assert approx is True


def test_count_connection_error_keeps_rows():
    c = FakeClient(rows=[{}] * 50, count_exc=ConnectionError("boom"))
    total, res, t, approx = _query_first_then_count(c, "ds", "cnt", "q", limit=50)
    assert len(res) == 50 and total == 50 and approx is True


def test_empty_fetch_does_not_invent_total():
    c = FakeClient(rows=[])
    total, res, t, approx = _query_first_then_count(c, "ds", "cnt", "q", limit=50)
    assert res == [] and total == 0 and approx is False
    assert c.count_calls == 0  # 0 real (sin filas), no un count fallido inventado


def test_temporary_timeout_mutates_and_restores_same_instance():
    # El acote NO crea cliente nuevo: muta el timeout del transporte y lo restaura,
    # preservando la instancia (y sus métodos mockeados).
    from app.core.secop_client import SecopClient

    class FakeSocrata:
        def __init__(self):
            self.timeout = 30

        def get(self, dataset_id, query=None):
            return []

    sc = SecopClient("d", None, client=FakeSocrata())
    assert sc.client.timeout == 30
    with sc.temporary_timeout(3):
        assert sc.client.timeout == 3  # acotado durante el bloque
    assert sc.client.timeout == 30     # restaurado al salir


# ── count_status derivado (attach_telemetry) ─────────────────────────────────

def test_count_status_exact():
    r = {"results": [{}] * 5}
    attach_telemetry(r, [{"operation": "query", "duration_ms": 1},
                         {"operation": "count", "status": "completed", "duration_ms": 1}])
    assert r["count_status"] == "exact"
    assert r["count_used_in_response"] is True
    assert r["result_row_count"] == 5


def test_count_status_timed_out_not_used():
    r = {"results": [{}] * 50}
    attach_telemetry(r, [{"operation": "query", "duration_ms": 1},
                         {"operation": "count", "status": "failed", "timeout": True,
                          "duration_ms": 3000, "exception_type": "Timeout"}])
    assert r["count_status"] == "timed_out"
    assert r["count_used_in_response"] is False
    assert r["count_error_type"] == "Timeout"


def test_count_status_skipped_when_no_count():
    r = {"results": [{}] * 3}
    attach_telemetry(r, [{"operation": "query", "duration_ms": 1}])
    assert r["count_status"] == "skipped"
    assert r["count_used_in_response"] is True  # total = filas, exacto


# ── Invariante: filas + count fallido NUNCA es EMPTY (ya en Etapa 4) ─────────

def test_rows_with_count_failure_is_partial_never_empty():
    out, reason = classify_outcome(rows_returned=50, count_approximate=True)
    assert out == Outcome.PARTIAL and reason == OutcomeReason.COUNT_TIMEOUT
    assert out != Outcome.EMPTY
