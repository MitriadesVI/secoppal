"""Etapa 5 — telemetría observacional (timings + HTTP), sin cambiar comportamiento."""
from __future__ import annotations

import pytest

from app.core import instrumentation as ins
from app.core.instrumentation import (
    begin_collection,
    collected_http,
    end_collection,
    http_span,
    is_timeout_exception,
    query_hash,
    record_http,
)
from app.core.secop_client import SecopClient


@pytest.fixture(autouse=True)
def _clean_state():
    # Aísla el contextvar de recolección entre tests (otro test puede dejarlo activo).
    end_collection()
    yield
    ins.reset_clock()
    end_collection()


class FakeSocrata:
    """Cliente Socrata falso: devuelve rows fijas o lanza."""

    def __init__(self, rows=None, raise_exc=None):
        self.rows = rows if rows is not None else []
        self.raise_exc = raise_exc
        self.calls = 0

    def get(self, dataset_id, query=None):
        self.calls += 1
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.rows


def _fixed_clock(ticks):
    it = iter(ticks)
    return lambda: next(it)


# ── Recolección pasiva ───────────────────────────────────────────────────────

def test_record_http_is_noop_without_collection():
    # Sin begin_collection, registrar no acumula nada (overhead nulo).
    record_http({"operation": "query"})
    assert collected_http() == []


def test_http_span_records_duration_and_rows():
    begin_collection()
    ins.set_clock(_fixed_clock([10.0, 10.25]))  # 0.25s → 250ms
    with http_span("query", "p6dx-8zbt", "SELECT 1") as span:
        span["rows_received"] = 3
    metrics = collected_http()
    assert len(metrics) == 1
    m = metrics[0]
    assert m["operation"] == "query" and m["dataset"] == "p6dx-8zbt"
    assert m["rows_received"] == 3
    assert m["duration_ms"] == 250.0
    assert m["status"] == "completed" and m["timeout"] is False


def test_http_span_captures_timeout_in_finally_and_reraises():
    begin_collection()
    ins.set_clock(_fixed_clock([0.0, 60.0]))
    with pytest.raises(TimeoutError):
        with http_span("count", "jbjy-vk9h", "SELECT count(*)") as span:
            raise TimeoutError("read timed out")
    m = collected_http()[0]
    assert m["status"] == "failed"
    assert m["timeout"] is True
    assert m["timeout_stage"] == "read"
    assert m["duration_ms"] == 60000.0  # el dato se captura aunque haya excepción


def test_query_hash_short_and_deterministic():
    h = query_hash("SELECT * WHERE x=1")
    assert len(h) == 16
    assert h == query_hash("SELECT * WHERE x=1")
    assert h != query_hash("SELECT * WHERE x=2")


def test_is_timeout_walks_cause_chain():
    inner = TimeoutError("read timed out")
    outer = RuntimeError("wrapped")
    outer.__cause__ = inner
    assert is_timeout_exception(outer) is True
    assert is_timeout_exception(ValueError("nope")) is False


# ── Gate observacional: misma salida con o sin recolección ───────────────────

def test_client_query_same_result_with_or_without_collection():
    fake = FakeSocrata(rows=[{"id_del_proceso": "CO1.NTC.1", "precio_base": "100"}])
    client = SecopClient(domain="d", app_token=None, client=fake)

    without = client.query("p6dx-8zbt", "SELECT 1")
    begin_collection()
    with_collection = client.query("p6dx-8zbt", "SELECT 1")

    assert without == with_collection  # comportamiento idéntico
    metrics = collected_http()
    assert len(metrics) == 1 and metrics[0]["operation"] == "query"
    assert metrics[0]["rows_received"] == 1


def test_client_count_records_metric_and_returns_value():
    fake = FakeSocrata(rows=[{"count": "42"}])
    client = SecopClient(domain="d", app_token=None, client=fake)
    begin_collection()
    assert client.count("p6dx-8zbt", "SELECT count(*)") == 42
    assert collected_http()[0]["operation"] == "count"


def test_client_query_timeout_records_and_raises():
    fake = FakeSocrata(raise_exc=TimeoutError("read timed out"))
    client = SecopClient(domain="d", app_token=None, client=fake, max_retries=1)
    begin_collection()
    with pytest.raises(RuntimeError):
        client.query("p6dx-8zbt", "SELECT 1")
    m = collected_http()[0]
    assert m["status"] == "failed" and m["timeout"] is True


def test_retries_produce_independent_metrics(monkeypatch):
    # Cada intento es una métrica propia; el backoff aparece como retry_delay.
    monkeypatch.setattr("app.core.secop_client.time.sleep", lambda s: None)
    fake = FakeSocrata(raise_exc=TimeoutError("read timed out"))
    client = SecopClient(domain="d", app_token=None, client=fake, max_retries=2)
    begin_collection()
    with pytest.raises(RuntimeError):
        client.query("p6dx-8zbt", "SELECT 1")
    metrics = collected_http()
    attempts = [m for m in metrics if m["operation"] == "query"]
    delays = [m for m in metrics if m["operation"] == "retry_delay"]
    assert [m["attempt"] for m in attempts] == [1, 2]  # dos intentos independientes
    assert all(m["timeout"] for m in attempts)
    assert len(delays) == 1  # un backoff entre los dos intentos


# ── Lifecycle del collector en run_query (cierre en TODA salida) ─────────────

def test_run_query_closes_collector_even_on_exception(monkeypatch, caplog):
    import logging

    from app.config import Settings
    from app.core.orchestrator import SecopalWorkflow

    wf = SecopalWorkflow(Settings())

    def boom(deps, ctx):
        record_http({"operation": "query", "duration_ms": 1.0})  # métrica antes de fallar
        raise RuntimeError("boom en el flujo")

    monkeypatch.setattr("app.core.orchestrator.handle_normal_flow", boom)
    with caplog.at_level(logging.WARNING, logger="app.core.orchestrator"):
        with pytest.raises(RuntimeError):
            wf.run_query("una consulta cualquiera", chat_id="lifecycle-1")

    # Collector cerrado pese a la excepción → siguiente turno no hereda métricas.
    assert collected_http() == []
    # Punto A del asesor: la evidencia del error NO se pierde (queda en el log).
    assert any("abortado tras capturar" in r.message for r in caplog.records)


def test_run_query_attaches_metrics_to_alternate_handler(monkeypatch):
    # No solo normal_flow: un handler alterno (lookup) también expone http_metrics,
    # porque el attach es centralizado en run_query.
    from app.config import Settings
    from app.core.orchestrator import SecopalWorkflow

    wf = SecopalWorkflow(Settings())

    def fake_lookup(deps, ctx):
        record_http({"operation": "query", "duration_ms": 5.0, "status": "completed"})
        return {"response": "x", "timings_ms": {"total_ms": 12.0}}

    monkeypatch.setattr("app.core.orchestrator.handle_direct_lookup", fake_lookup)
    res = wf.run_query("CO1.NTC.123", chat_id="alt-1")

    assert res.get("http_metrics") and res["http_metrics"][0]["operation"] == "query"
    assert res["timings_ms"]["fetch_results_ms"] == 5.0
    assert res["timings_ms"]["pipeline_self_ms"] == 7.0  # total 12 − http 5


def test_nested_collection_preserves_outer():
    from app.core.instrumentation import end_collection as end_c

    outer = begin_collection()
    record_http({"operation": "outer"})
    inner = begin_collection()
    record_http({"operation": "inner"})
    assert [m["operation"] for m in collected_http()] == ["inner"]

    end_c(inner)  # restaura el buffer externo, no lo deja inactivo
    assert [m["operation"] for m in collected_http()] == ["outer"]

    end_c(outer)
    assert collected_http() == []
