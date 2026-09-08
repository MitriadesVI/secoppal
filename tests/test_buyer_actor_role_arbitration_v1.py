"""Buyer role regression with real parser, directory, workflow and SQL compiler.

All provider identities and source contracts below are synthetic fixtures.
"""
import json
import os
import socket
import re
import threading
from datetime import date
import sqlite3
from pathlib import Path

import pytest

from app.config import Settings, get_settings
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from app.core.buyer_source_names import project_buyer_names
from app.data.actor_alias_index import build_actor_alias_index

CARTAGENA = "ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS"
BARRANQUILLA = "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"
SEEDS = [
    ("contratos de alcaldia de cartagena por mas de 500 millones", "alcaldia de cartagena", CARTAGENA, 500000000),
    ("que ha contratado alcaldia de barranquilla por mas de 2000 millones", "alcaldia de barranquilla", BARRANQUILLA, 2000000000),
]


def row(name, number="70000001", kind="CC", frequency=5):
    return {"tipodocproveedor": kind, "documento_proveedor": number,
            "proveedor_adjudicado": name, "occurrences": frequency,
            "first_seen": "2024-01-01", "last_seen": "2026-01-01"}


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("External I/O forbidden in buyer-role regression")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)


def directory(path, rows, mode="built"):
    if mode == "missing":
        return path
    if mode == "incompatible":
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE incompatible(value TEXT)")
        return path
    build_actor_alias_index(rows, path, source_dataset="synthetic-buyer-role-v1",
                           built_at="2026-09-08T12:00:00+00:00")
    return path


def flow_for(tmp_path, monkeypatch, index, frame, trace):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", str(frame).lower())
    get_settings.cache_clear()
    settings = Settings(
        deepseek_api_key=None, secop_actor_alias_index_path=index,
        secoppal_entity_profile_frame_enabled=frame,
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.sqlite",
        secop_feedback_sqlite_enabled=False,
    )
    flow = SecopalWorkflow(settings)
    trace["effective_config"] = {"actor": True, "frame": settings.secoppal_entity_profile_frame_enabled,
                                 "index": str(index), "persistence": str(tmp_path)}
    resolver = flow.actor_name_resolver
    for name in ("resolve", "resolve_exact"):
        original = getattr(resolver, name)
        def record(mention, *args, _original=original, _name=name, **kwargs):
            result = _original(mention, *args, **kwargs)
            trace.setdefault("resolver", []).append({"method": _name, "mention": mention,
                                                    "status": str(result.status), "resolution": result.method})
            return result
        monkeypatch.setattr(resolver, name, record)
    monkeypatch.setattr(flow.narrator, "narrate_with_grounding", lambda **kw: None)
    return flow


def source_for(flow, monkeypatch, trace, buyer, amount):
    # SQL is actually evaluated, including pagination predicates. Decoy rows
    # would appear if either the buyer or the monetary predicate disappeared.
    columns = ("id_contrato", "referencia_del_contrato", "nombre_entidad", "departamento",
               "ciudad", "objeto_del_contrato", "descripcion_del_proceso", "tipo_de_contrato",
               "modalidad_de_contratacion", "valor_del_contrato", "valor_facturado", "estado_contrato",
               "fecha_de_firma", "proveedor_adjudicado", "documento_proveedor", "urlproceso")
    # `check_same_thread=False` + lock porque el observer agrega en 5 hilos
    # (ThreadPoolExecutor en observer.py); una conexión ligada a un hilo daría
    # ProgrammingError en vez de evaluar el SQL bajo prueba.
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    lock = threading.RLock()
    conn.row_factory = sqlite3.Row
    conn.create_function("unaccent", 1, lambda value: value)
    conn.create_function("date_trunc_y", 1, lambda value: str(value)[:4] + "-01-01T00:00:00.000")
    conn.execute("CREATE TABLE contracts (" + ",".join(
        f"{name} {'INTEGER' if name.startswith('valor_') else 'TEXT'}" for name in columns) + ")")
    physical_buyer = (project_buyer_names(buyer, "jbjy-vk9h") or (buyer,))[0]
    rows = []
    for n in range(62):
        rows.append((f"SYNTH-{n:03d}", f"FIXTURE-{n}", physical_buyer if n < 61 else "DECOY BUYER",
                     "Atlantico", "Fixture City", "software fixture", "software fixture", "Prestacion de servicios",
                     "Contratacion directa", amount + 1000 if n != 60 else 1, 0, "Firmado",
                     "2026-06-01T00:00:00.000", "SYNTHETIC SUPPLIER", "70000999", "https://example.invalid/fixture"))
    conn.executemany("INSERT INTO contracts VALUES (" + ",".join("?" for _ in columns) + ")", rows)

    def query(dataset, soql, *args, **kwargs):
        trace.setdefault("sql", []).append({"dataset": dataset, "soql": soql})
        where = soql.upper().split(" WHERE ", 1)[-1].split(" ORDER BY ", 1)[0]
        violations = []
        if dataset not in {"jbjy-vk9h", "contratos"}:
            violations.append("wrong dataset")
        if physical_buyer.upper() not in where:
            violations.append("wrong buyer")
        if "DOCUMENTO_PROVEEDOR" in where:
            violations.append("unexpected contractor")
        if "VALOR_DEL_CONTRATO" not in where or str(amount) not in where:
            violations.append("missing amount")
        if "CONTRATADO" in where or "ALCALDIA" in where.split("AND", 1)[-1]:
            violations.append("request language as topic")
        if violations:
            trace.setdefault("violations", []).extend(violations)
            raise AssertionError(violations)
        sql = re.sub(r"\bWHERE\b", "FROM contracts WHERE", soql, count=1, flags=re.I)
        with lock:
            result = [dict(r) for r in conn.execute(sql)]
        trace.setdefault("source_rows", []).append([r.get("id_contrato") for r in result])
        return result
    monkeypatch.setattr(flow.secop_client, "query", query)
    monkeypatch.setattr(flow.secop_client, "aggregate", query)
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **kw: 60)
    # Sustituir métodos en la instancia no alcanza: el observer agrega sobre
    # `secop_client.with_timeout(...)`, que construye un SecopClient nuevo y sale
    # a la red de verdad. `derived_for_timeout` ya devuelve `self` cuando detecta
    # métodos mockeados; `with_timeout` no, así que se ata aquí explícitamente.
    monkeypatch.setattr(flow.secop_client, "with_timeout", lambda *a, **kw: flow.secop_client)
    monkeypatch.setattr(flow.secop_client, "derived_for_timeout", lambda *a, **kw: flow.secop_client)


def write_trace(trace, name):
    destination = os.environ.get("BUYER_ACTOR_EVIDENCE")
    if destination:
        path = Path(destination)
        path.mkdir(parents=True, exist_ok=True)
        (path / f"{name}.json").write_text(json.dumps(trace, ensure_ascii=False, indent=2, default=str))


@pytest.mark.parametrize("frame", [False, True], ids=["off", "on"])
@pytest.mark.parametrize("cardinality", [0, 1, 3, "missing", "incompatible"])
@pytest.mark.parametrize("query,mention,buyer,amount", SEEDS, ids=["cartagena", "barranquilla"])
def test_seeds(tmp_path, monkeypatch, frame, cardinality, query, mention, buyer, amount):
    rows = [row(mention, str(70000001 + n)) for n in range(cardinality)] if isinstance(cardinality, int) else []
    index = directory(tmp_path / "actors.sqlite", rows, str(cardinality))
    trace = {"query": query, "initial_params": dict(QueryRouter().parse(query).params)}
    flow = flow_for(tmp_path, monkeypatch, index, frame, trace)
    source_for(flow, monkeypatch, trace, buyer, amount)
    try:
        result = flow.run_query(query, chat_id="fresh", today=date(2026, 9, 8))
        trace["result"] = result
        trace["published"] = [p.read_text() for p in (tmp_path / "conversations").glob("*") if p.is_file()]
        params = result["resolved_params"]
        assert not trace.get("violations"), trace
        assert trace.get("sql"), result
        assert trace.get("source_rows"), result
        assert params["dataset"] == "contratos"
        assert params["entidad_resolved"] == buyer
        assert params["valor_min"] == amount
        assert not any(params.get(k) for k in ("actor_scope", "actor_pending", "contratista", "actor_resolution", "actor_resolution_method"))
        assert not trace.get("resolver")
        assert not any(params.get(k) for k in ("fecha_desde", "fecha_hasta", "estado", "ciudad", "departamento_resolved", "region"))
    finally:
        write_trace(trace, f"{mention.split()[-1]}-{cardinality}-{frame}")
