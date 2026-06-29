"""Etapa 6 H2 — deadline total del turno + fetch acotado por presupuesto restante."""
from __future__ import annotations

import threading
import time

import pytest

from app.core.deadline import (
    CLOSING_RESERVE_SECONDS,
    TURN_DEADLINE_SECONDS,
    Deadline,
    attach_deadline_telemetry,
    begin_deadline,
    collected_deadline,
    current_deadline,
    end_deadline,
    record_deadline,
)
from app.core.workflow.helpers import _query_first_then_count


class FakeClient:
    def __init__(self, rows, count_value=0, count_exc=None, query_exc=None, timeout=15):
        self.rows = rows
        self.count_value = count_value
        self.count_exc = count_exc
        self.query_exc = query_exc
        self.timeout = timeout

    def query(self, dataset_id, soql):
        if self.query_exc is not None:
            raise self.query_exc
        return list(self.rows)

    def count(self, dataset_id, soql):
        if self.count_exc is not None:
            raise self.count_exc
        return self.count_value

    def with_timeout(self, timeout):
        return FakeClient(self.rows, self.count_value, self.count_exc, self.query_exc, timeout)


def _run(started_at, client, **kw):
    tokens = begin_deadline(started_at)
    try:
        out = _query_first_then_count(client, "ds", "cnt", "q", **kw)
        return out, collected_deadline()
    finally:
        end_deadline(tokens)


# ── Deadline puro (reloj monotónico, presupuestos) ───────────────────────────

def test_deadline_remaining_and_fetch_budget():
    dl = Deadline(started_at=time.perf_counter())
    assert 14.0 < dl.remaining_s() <= 15.0
    # min(source, remaining − reserve)
    assert dl.fetch_budget_s(60) == pytest.approx(dl.remaining_s() - CLOSING_RESERVE_SECONDS, abs=0.3)
    assert dl.fetch_budget_s(2) == pytest.approx(2.0, abs=0.01)


def test_monotonic_clock_never_decreases():
    dl = Deadline(started_at=time.perf_counter())
    assert dl.elapsed_s() >= 0
    assert dl.elapsed_s() >= dl.elapsed_s() - 0.001  # monotónico


# ── Fetch acotado ────────────────────────────────────────────────────────────

def test_fetch_budget_caps_source_timeout():
    # source 60s, pero el turno solo deja ~11s → budget acotado (no 60).
    (total, res, t, approx), tel = _run(time.perf_counter(), FakeClient([{}] * 3, timeout=60))
    assert tel["request_attempted"] is True
    assert tel["fetch_budget_ms"] <= 11_500  # ~ (15 − 4) s
    assert tel["fetch_status"] == "completed"


def test_fast_query_not_penalized():
    (total, res, t, approx), tel = _run(time.perf_counter(), FakeClient([{}] * 3, timeout=15))
    assert len(res) == 3 and approx is False and tel["fetch_status"] == "completed"


def test_no_deadline_means_h1_behavior_unchanged():
    # Sin begin_deadline: comportamiento H1 puro (sin recorte de deadline).
    total, res, t, approx = _query_first_then_count(FakeClient([{}] * 3), "ds", "cnt", "q", limit=50)
    assert len(res) == 3 and approx is False


# ── Sin presupuesto: no iniciar llamadas que no terminarán ───────────────────

def test_fetch_skipped_when_budget_exhausted():
    # started_at en el pasado → casi agotado → no se inicia el fetch.
    (total, res, t, approx), tel = _run(time.perf_counter() - 14.8, FakeClient([{}] * 50))
    assert res == [] and total == 0
    assert tel["request_skipped_due_to_budget"] is True
    assert tel["deadline_exhausted"] is True and tel["deadline_stage"] == "fetch"


def test_count_skipped_when_low_budget_keeps_rows():
    # El query corre (hay presupuesto) pero CONSUME 12s; al llegar al count ya no queda
    # presupuesto → se omite el count y se devuelven las filas con total aproximado.
    from app.core import deadline as d

    clock = [100.0]
    d.set_clock(lambda: clock[0])

    class SlowQuery(FakeClient):
        def query(self, dataset_id, soql):
            clock[0] += 12.0  # el fetch consume 12s del presupuesto de 15s
            return list(self.rows)

        def with_timeout(self, timeout):
            return self  # el cliente acotado preserva el comportamiento lento

    tokens = begin_deadline(100.0)
    try:
        total, res, t, approx = _query_first_then_count(
            SlowQuery([{}] * 50, count_value=999), "ds", "cnt", "q", limit=50,
        )
        tel = collected_deadline()
    finally:
        end_deadline(tokens)
        d.reset_clock()

    assert len(res) == 50 and approx is True          # filas conservadas, NUNCA EMPTY
    assert tel.get("count_skipped_due_to_budget") is True


# ── Timeout sin filas → inconcluso (ERROR), nunca EMPTY ──────────────────────

def test_query_timeout_propagates_for_inconclusive_not_empty():
    with pytest.raises(TimeoutError):
        _run(time.perf_counter(), FakeClient([], query_exc=TimeoutError("read timed out")))
    # fetch_status quedó registrado como timed_out (el caller lo mapea a ERROR/FETCH_TIMEOUT)
    # nota: collected_deadline ya se cerró en _run; verificamos el contrato vía outcome:
    from app.core.outcome import Outcome, classify_outcome
    out, _ = classify_outcome(query_failed=True, query_timed_out=True, rows_returned=0)
    assert out == Outcome.ERROR and out != Outcome.EMPTY


# ── Dual: deadline compartido secuencial ─────────────────────────────────────

def test_dual_shares_deadline_sequentially():
    tokens = begin_deadline(time.perf_counter())
    try:
        dl = current_deadline()
        r1 = dl.remaining_s()
        _query_first_then_count(FakeClient([{}] * 3), "ds", "cnt", "q", limit=50)
        r2 = dl.remaining_s()
        assert r2 <= r1  # el segundo hijo ve menos presupuesto; no se reinicia
    finally:
        end_deadline(tokens)


# ── No threads huérfanos ─────────────────────────────────────────────────────

def test_no_threads_spawned():
    before = threading.active_count()
    _run(time.perf_counter(), FakeClient([{}] * 50, count_value=5))
    assert threading.active_count() == before


# ── Telemetría + anidación ───────────────────────────────────────────────────

def test_attach_deadline_telemetry_fields():
    r = {}
    attach_deadline_telemetry(r, {"fetch_status": "completed", "fetch_budget_ms": 11000.0},
                              time.perf_counter() - 1.0)
    assert r["turn_deadline_ms"] == TURN_DEADLINE_SECONDS * 1000
    assert r["turn_elapsed_ms"] >= 900
    assert r["fetch_status"] == "completed"


def test_nested_deadline_restores_outer():
    outer = begin_deadline(time.perf_counter())
    record_deadline(marker="outer")
    inner = begin_deadline(time.perf_counter())
    record_deadline(marker="inner")
    assert collected_deadline()["marker"] == "inner"
    end_deadline(inner)
    assert collected_deadline()["marker"] == "outer"
    end_deadline(outer)
    assert collected_deadline() == {}
