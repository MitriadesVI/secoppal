"""Buyer role regression with real parser, directory, workflow and SQL compiler.

All provider identities and source contracts below are synthetic fixtures.
"""
import json
import os
import socket
import sqlite3
from pathlib import Path

import pytest

from app.config import Settings, get_settings
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from app.data.actor_alias_index import build_actor_alias_index
from app.data.actor_alias_resolver import ActorNameResolver

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
    def query(dataset, soql, *args, **kwargs):
        trace.setdefault("sql", []).append({"dataset": dataset, "soql": soql})
        where = soql.upper().split(" WHERE ", 1)[-1].split(" ORDER BY ", 1)[0]
        assert dataset in {"jbjy-vk9h", "contratos"}
        assert buyer.upper() in where
        assert "DOCUMENTO_PROVEEDOR" not in where
        assert "VALOR_DEL_CONTRATO" in where and str(amount) in where
        assert "DESCRIPCION_DEL_PROCESO LIKE" not in where
        return []
    monkeypatch.setattr(flow.secop_client, "query", query)
    monkeypatch.setattr(flow.secop_client, "aggregate", query)
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **kw: 0)


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
        result = flow.run_query(query, chat_id="fresh")
        trace["result"] = result
        trace["published"] = [p.read_text() for p in (tmp_path / "conversations").glob("*") if p.is_file()]
        params = result["resolved_params"]
        assert trace.get("sql"), result
        assert params["dataset"] == "contratos"
        assert params["entidad_resolved"] == buyer
        assert params["valor_min"] == amount
        assert not any(params.get(k) for k in ("actor_scope", "actor_pending", "contratista", "actor_resolution", "actor_resolution_method"))
        assert not trace.get("resolver")
        assert not any(params.get(k) for k in ("fecha_desde", "fecha_hasta", "estado", "ciudad", "departamento_resolved", "region"))
    finally:
        write_trace(trace, f"{mention.split()[-1]}-{cardinality}-{frame}")
