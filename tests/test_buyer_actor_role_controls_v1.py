"""Mention/role contrasts and persisted continuity for Buyer-Actor V1."""
from datetime import date

import pytest
from burr.core import State

from app.core.query_router import QueryRouter
from app.core.workflow.actions.actor_resolution import resolve_actor_name
from app.data.actor_alias_resolver import ActorNameResolver
from tests.test_buyer_actor_role_arbitration_v1 import offline  # noqa: F401
from tests.test_buyer_actor_role_arbitration_v1 import (
    CARTAGENA, SEEDS, directory, flow_for, row, source_for, write_trace,
)


@pytest.mark.parametrize("frame", [False, True])
@pytest.mark.parametrize("query,mention,buyer,amount", SEEDS, ids=["cartagena", "barranquilla"])
def test_reload_buyer_year_more(tmp_path, monkeypatch, frame, query, mention, buyer, amount):
    index = directory(tmp_path / "actors.sqlite", [row(mention), row(mention, "70000002")])
    turns = []
    for text in (query, "solo 2026", "muéstrame más"):
        trace = {"query": text}
        flow = flow_for(tmp_path, monkeypatch, index, frame, trace)
        source_for(flow, monkeypatch, trace, buyer, amount)
        result = flow.run_query(text, chat_id="reload", today=date(2026, 9, 8))
        trace["result"] = result
        turns.append(trace)
        params = result["resolved_params"]
        assert not trace.get("violations"), trace
        assert params.get("entidad_resolved") == buyer, result
        assert params.get("valor_min") == amount, result
        assert not any(params.get(k) for k in ("actor_scope", "actor_pending", "contratista")), result
        assert not trace.get("resolver"), trace
        assert not result.get("needs_clarification"), result
        assert buyer in result["response"], result
        if text != query:
            assert params.get("fecha_desde") == "2026-01-01", result
        if text == "muéstrame más":
            previous_ids = {r["id_contrato"] for r in turns[1]["result"]["rows"]}
            current_ids = {r["id_contrato"] for r in result["rows"]}
            assert current_ids and not previous_ids & current_ids
    write_trace({"turns": turns}, f"continuity-{mention.split()[-1]}-{frame}")


@pytest.mark.parametrize("frame", [False, True])
@pytest.mark.parametrize("seed", SEEDS, ids=["cartagena", "barranquilla"])
def test_new_buyer_after_legitimate_actor(tmp_path, monkeypatch, frame, seed):
    query, mention, buyer, amount = seed
    index = directory(tmp_path / "actors.sqlite", [row("FUNDACARIBE"), row(mention, "70000002")])
    first_trace = {}
    flow = flow_for(tmp_path, monkeypatch, index, frame, first_trace)
    monkeypatch.setattr(flow.secop_client, "query", lambda *a, **k: [])
    monkeypatch.setattr(flow.secop_client, "aggregate", lambda *a, **k: [])
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **k: 0)
    first = flow.run_query("contratos del proveedor FUNDACARIBE", chat_id="replace", today=date(2026, 9, 8))
    assert first["resolved_params"]["actor_scope"]["base_number"] == "70000001"
    trace = {"previous": first}
    flow = flow_for(tmp_path, monkeypatch, index, frame, trace)
    source_for(flow, monkeypatch, trace, buyer, amount)
    result = flow.run_query(query, chat_id="replace", today=date(2026, 9, 8))
    trace["result"] = result
    write_trace(trace, f"new-after-actor-{mention.split()[-1]}-{frame}")
    assert not trace.get("violations"), trace
    assert trace.get("sql"), result
    assert result["resolved_params"]["entidad_resolved"] == buyer
    assert not result["resolved_params"].get("actor_scope"), result
    assert not trace.get("resolver"), trace


@pytest.mark.parametrize("frame", [False, True])
@pytest.mark.parametrize("query", [
    "CONTRATOS DE LA ALCALDÍA DE CARTAGENA por mas de 500 millones",
    "qué ha contratado la alcaldía de Cartagena por mas de 500 millones",
    "contratos de alcaldia de cartagena en 2026 por mas de 500 millones",
    "contratos de software de alcaldia de cartagena por mas de 500 millones",
    "contratos de alcaldia de cartagena por mas de 500 millones por mayor valor",
])
def test_orthogonal_variants(tmp_path, monkeypatch, frame, query):
    index = directory(tmp_path / "actors.sqlite", [row("alcaldia de cartagena"), row("alcaldia de cartagena", "70000002")])
    trace = {"query": query}
    flow = flow_for(tmp_path, monkeypatch, index, frame, trace)
    source_for(flow, monkeypatch, trace, CARTAGENA, 500000000)
    result = flow.run_query(query, chat_id="variant", today=date(2026, 9, 8))
    trace["result"] = result
    assert not trace.get("violations"), trace
    assert trace.get("source_rows"), result
    assert not result["resolved_params"].get("actor_scope"), result
    assert not result.get("needs_clarification"), result
    if "software" in query:
        assert "software" in str(result["resolved_params"].get("objeto"))
    if "mayor valor" in query:
        assert result["resolved_params"].get("ordering_signal") == "valor_desc"


@pytest.mark.parametrize("mention", ["alcaldia de galapa", "alcaldia de baranoa", "universidad del atlantico"])
@pytest.mark.parametrize("head", ["contratos de ", "qué ha contratado "])
@pytest.mark.parametrize("frame", [False, True])
def test_existing_catalog_families(tmp_path, monkeypatch, mention, head, frame):
    query = head + mention + " por mas de 500 millones"
    initial = QueryRouter().parse(query).params
    buyer = initial.get("entidad_resolved")
    assert buyer, initial
    index = directory(tmp_path / "actors.sqlite", [row(mention), row(mention, "70000002")])
    trace = {"query": query, "initial": initial}
    flow = flow_for(tmp_path, monkeypatch, index, frame, trace)
    source_for(flow, monkeypatch, trace, buyer, 500000000)
    result = flow.run_query(query, chat_id="catalog", today=date(2026, 9, 8))
    assert not trace.get("violations"), trace
    assert trace.get("source_rows"), result
    assert not result["resolved_params"].get("actor_scope"), result
    assert not result.get("needs_clarification"), result


@pytest.mark.parametrize("query,name", [
    ("contratos de FUNDACARIBE", "FUNDACARIBE"),
    ("qué ha contratado FUNDACARIBE", "FUNDACARIBE"),
    ("contratos del proveedor FUNDACARIBE", "FUNDACARIBE"),
    ("contratos del proveedor CC 70000001", "FUNDACARIBE"),
    ("contratos del proveedor FUNDACARIBE CC 70000001", "FUNDACARIBE"),
    ("contratos adjudicados a alcaldia de cartagena", "alcaldia de cartagena"),
    ("contratos del proveedor alcaldia de cartagena", "alcaldia de cartagena"),
    ("contratos del proveedor UNIVERSIDAD DEL ATLANTICO", "UNIVERSIDAD DEL ATLANTICO"),
    ("contratos del proveedor alcaldia de cartagena servicios integrados", "alcaldia de cartagena servicios integrados"),
    ("contratos de alcaldia de cartagena servicios integrados", "alcaldia de cartagena servicios integrados"),
    ("contratos del proveedor FUNDACARIBE con la Alcaldía de Cartagena", "FUNDACARIBE"),
])
@pytest.mark.parametrize("cardinality", [1, 2])
@pytest.mark.parametrize("frame", [False, True])
def test_legitimate_provider(tmp_path, monkeypatch, query, name, cardinality, frame):
    index = directory(tmp_path / "actors.sqlite", [row(name, str(70000001 + n)) for n in range(cardinality)])
    trace = {"query": query}
    flow = flow_for(tmp_path, monkeypatch, index, frame, trace)
    def record(dataset, soql, *args, **kwargs):
        trace.setdefault("sql", []).append(soql)
        return []
    monkeypatch.setattr(flow.secop_client, "query", record)
    monkeypatch.setattr(flow.secop_client, "aggregate", record)
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **k: 0)
    result = flow.run_query(query, chat_id="provider", today=date(2026, 9, 8))
    params = result["resolved_params"]
    if query == "contratos de alcaldia de cartagena servicios integrados":
        assert params.get("actor_intent_conflict"), result
        assert not trace.get("sql"), trace
        return
    if cardinality == 1 or "CC 70000001" in query:
        assert params.get("actor_scope"), result
        assert params["actor_scope"]["base_number"] == "70000001", result
        assert trace.get("sql"), result
        assert all("70000001" in sql for sql in trace["sql"])
        if "con la" in query:
            assert params["entidad_resolved"] == CARTAGENA
            assert all(CARTAGENA in sql for sql in trace["sql"])
        else:
            assert not params.get("entidad_resolved"), result
    else:
        assert params.get("actor_pending"), result
        assert not trace.get("sql"), trace


@pytest.mark.parametrize("query", [
    "contratos del proveedor CC 70000001",
    "contratos del proveedor FUNDACARIBE CC 70000001",
])
def test_explicit_documents(tmp_path, query):
    index = directory(tmp_path / "actors.sqlite", [row("FUNDACARIBE")])
    initial = QueryRouter().parse(query).params
    final = resolve_actor_name(State({"parsed_params": initial, "raw_query": query}), ActorNameResolver(index))["parsed_params"]
    assert final["actor_scope"]["base_number"] == "70000001"


@pytest.mark.parametrize("query", ["contratos de bogota", "contratos de software"])
def test_geography_and_topic_do_not_become_buyer_or_actor(tmp_path, query):
    name = query.removeprefix("contratos de ")
    index = directory(tmp_path / "actors.sqlite", [row(name)])
    initial = QueryRouter().parse(query).params
    final = resolve_actor_name(State({"parsed_params": initial, "raw_query": query}), ActorNameResolver(index))["parsed_params"]
    assert not final.get("entidad_resolved"), final
    if name == "bogota":
        assert not final.get("actor_scope"), final
    else:
        # Exact topic homonym keeps the existing intent clarification policy.
        assert final.get("actor_intent_conflict"), final


@pytest.mark.parametrize("query", [
    "contratos donde el proveedor no sea FUNDACARIBE",
    "contratos no de alcaldia de cartagena",
])
def test_negation_keeps_policy(tmp_path, monkeypatch, query):
    index = directory(tmp_path / "actors.sqlite", [row("FUNDACARIBE"), row("alcaldia de cartagena")])
    trace = {}
    flow = flow_for(tmp_path, monkeypatch, index, False, trace)
    def forbidden(*args, **kwargs):
        pytest.fail("Negated role cannot execute")
    monkeypatch.setattr(flow.secop_client, "query", forbidden)
    monkeypatch.setattr(flow.secop_client, "aggregate", forbidden)
    result = flow.run_query(query, chat_id="negative", today=date(2026, 9, 8))
    assert result.get("needs_clarification"), result
    assert not trace.get("resolver"), trace
