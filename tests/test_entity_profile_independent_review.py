"""Independent audit: desired contracts, deliberately red on candidate 991a02f.

All source data are synthetic; observations capture actual SQL and product text.
Set ENTITY_AUDIT_OUTPUT to save evidence outside transient pytest directories.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import date
import json
import os
from pathlib import Path
import socket

from hypothesis import given, settings, strategies as st
import pytest

from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.effective_scope import build_effective_scope
from app.core.entity_profile.render import render_entity_profile
from app.core.entity_profile.source_request import resolve_requested_sources
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from test_entity_profile_queries import FakeClient
from test_entity_profile_workflow import _workflow

TODAY = date(2026, 9, 7)
BUYER = "alcaldia de baranoa"
ROUTER = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("independent audit forbids external network")
    monkeypatch.setattr(socket.socket, "connect", blocked)


def record(name, value):
    destination = os.environ.get("ENTITY_AUDIT_OUTPUT")
    if destination:
        path = Path(destination) / f"{name}.json"
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")


def observe(query, injected=None):
    params = ROUTER.parse(query, today=TODAY).params
    params.update(injected or {})
    turn = classify_entity_profile_turn(query, params)
    assert turn.detected and turn.plan is not None, (query, params)
    client = FakeClient()
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=turn.focus, secop_client=client, soql_builder=SoQLBuilder(), plan=turn.plan,
    )
    return {
        "query": query, "injected": injected, "parsed": params,
        "request": turn.request.to_dict(), "clarification": turn.clarification,
        "effective": turn.plan.effective_sources,
        "process_plan": asdict(turn.plan.process_plan),
        "contract_plan": asdict(turn.plan.contract_plan),
        "calls": client.calls, "snapshot": asdict(build_effective_scope(profile)),
        "render": render_entity_profile(profile),
    }


TEMPORAL_CASES = [
    "procesos publicados y contratos firmados en agosto",
    "contratos firmados y procesos publicados en agosto",
    "procesos y contratos publicados en agosto",
    "procesos y contratos firmados en agosto",
    "contratos terminados en agosto y actualmente activos",
    "contratos liquidados en agosto y actualmente activos",
    "contratos iniciados en agosto y actualmente terminados",
    "procesos cerrados en agosto y todavia publicados",
    "contratos firmados y liquidados en agosto",
    "procesos actualmente publicados en agosto",
]


@pytest.mark.parametrize("index,phrase", enumerate(TEMPORAL_CASES))
def test_temporal_request_is_fully_honored_or_clarified(index, phrase):
    observation = observe(f"analiza {phrase} de {BUYER}")
    record(f"temporal-{index}", observation)
    if index < 2 and observation["effective"] == "BOTH":
        assert observation["process_plan"]["event"] == "PROCESS_PUBLICATION"
        assert observation["contract_plan"]["event"] == "CONTRACT_SIGNATURE"
    elif index == 9:
        assert observation["process_plan"]["params"].get("estado_del_procedimiento")
    else:
        assert observation["clarification"] and not observation["calls"]


@pytest.mark.parametrize("index,phrase,expected", [
    (0, "contratos de gestion de procesos industriales", "CONTRACTS"),
    (1, "procesos de administracion de contratos", "PROCESSES"),
    (2, "contratos de publicacion de convocatorias", "CONTRACTS"),
])
def test_topic_source_words_do_not_choose_an_extra_source(index, phrase, expected):
    observation = observe(f"analiza {phrase} de {BUYER}")
    record(f"source-topic-{index}", observation)
    assert observation["request"]["sources"] == expected


def test_accredited_buyer_surface_masks_source_word():
    buyer = "Departamento Administrativo de Procesos X"
    source = resolve_requested_sources(
        f"analiza contratos del {buyer}",
        {"entidad_resolved": buyer, "entidad": buyer},
    )
    assert source.sources == "CONTRACTS"


@pytest.mark.parametrize("focus", ["proveedores", "montos"])
def test_explicit_both_capability_discloses_limitation(focus):
    observation = observe(f"analiza {focus} de procesos y contratos de {BUYER}")
    record(f"capability-{focus}", observation)
    assert observation["request"]["sources"] == "BOTH"
    assert observation["clarification"] or (
        "sólo se puede responder sobre contratos" in observation["render"]
    )


FILTER_CASES = [
    ("state", "contratos actualmente terminados", None, "estado_contrato", "terminado"),
    ("exclude", "contratos de mantenimiento excepto interventoria", None,
     "NOT (coalesce", "excepto"),
    ("modality", "contratos por contratacion directa", None,
     "modalidad_de_contratacion", "directa"),
    ("value", "procesos y contratos entre 100 y 500 millones", None,
     "100000000", "100"),
    ("type", "contratos de obra", None, "tipo_de_contrato", "obra"),
    ("or", "contratos", {"objeto_or": ["mantenimiento", "vias"]}, " OR ", " o "),
    ("and", "contratos", {"objeto": ["mantenimiento", "vias"]}, " AND ", " y "),
    ("phrase", "contratos de aire acondicionado", None, "acondicion", "aire acondicionado"),
]


@pytest.mark.parametrize("name,phrase,injected,sql_marker,visible", FILTER_CASES)
def test_material_sql_filter_is_visible(name, phrase, injected, sql_marker, visible):
    observation = observe(f"analiza {phrase} de {BUYER}", injected)
    record(f"filter-{name}", observation)
    sql = "\n".join(call[2].split(" WHERE ", 1)[1] for call in observation["calls"])
    assert sql_marker.casefold() in sql.casefold(), ("filter not exercised", sql)
    if name in ("and", "or"):
        opposite = "objeto" if name == "or" else "objeto_or"
        other = observe(f"analiza contratos de {BUYER}", {
            opposite: ["mantenimiento", "vias"],
        })
        assert observation["calls"][0][2] != other["calls"][0][2]
        assert observation["snapshot"] != other["snapshot"]
        return
    # Scope header only: a recent record title does not declare an applied filter.
    header = observation["render"].split("•")[0].casefold()
    assert visible in header
    if name == "value":
        assert "presupuesto" in header and "valor" in header


def test_baranoa_incident_and_listing_boundary():
    query = f"analizame que contratos firmo {BUYER} en agosto"
    observation = observe(query)
    record("baranoa", observation)
    assert observation["request"]["sources"] == "CONTRACTS"
    assert observation["request"]["sources_provenance"] == "EXPLICIT"
    assert observation["request"]["event"] == "CONTRACT_SIGNATURE"
    assert observation["effective"] == "CONTRACTS"
    assert observation["calls"] and all(c[1] == "jbjy-vk9h" for c in observation["calls"])
    assert "firmo" not in str(observation["request"]["topic"])
    for text in (f"que contratos firmo {BUYER} en agosto", "muestrame contratos firmados"):
        assert not classify_entity_profile_turn(text, ROUTER.parse(text, today=TODAY).params).detected


SEQUENCES = {
    "default": [f"analiza {BUYER}", "proveedores", "recientes"],
    "contracts": [f"analiza contratos firmados de {BUYER} en agosto", "proveedores", "en 2025"],
    "clarification": [f"analiza procesos de {BUYER}", "en agosto", "proveedores", "si"],
    "operators": [f"analiza contratos de mantenimiento o vias excepto interventoria de {BUYER}",
                  "en agosto", "proveedores", "en 2025"],
    "phrases": [f"analiza contratos de aire acondicionado de {BUYER}",
                "en agosto", "proveedores", "en 2025"],
}


@pytest.mark.parametrize("name,queries", SEQUENCES.items())
def test_workflow_disk_reload_preserves_semantics(name, queries):
    observations = []
    for query in queries:
        # Recreate the entire workflow at each turn; ConversationStore reads disk.
        workflow, client = _workflow()
        result = workflow.run_query(query, chat_id=f"independent-{name}", today=TODAY)
        observations.append({
            "query": query, "params": result.get("resolved_params"),
            "parsed": result.get("parsed_params"), "response": result.get("response"),
            "clarify": result.get("needs_clarification"), "route": result.get("route_reason"),
            "calls": client.calls,
            "disk_history": [asdict(turn) for turn in workflow.conv_store.get_history(
                f"independent-{name}", last_n=8,
            )],
        })
    record(f"conversation-{name}", observations)
    requests = [x["params"].get("_entity_profile_request") for x in observations]
    if name == "contracts":
        assert all(r and r["sources"] == "CONTRACTS" for r in requests)
        assert all(r["event"] == "CONTRACT_SIGNATURE" for r in requests)
        assert requests[-1]["focus"] == "suppliers"
    elif name == "default":
        assert all(r and r["sources"] == "BOTH" for r in requests)
        assert requests[-1]["focus"] == "recent"
    elif name == "clarification":
        assert observations[2]["clarify"] and not observations[2]["calls"]
        assert requests[2] == requests[1]
    else:
        fields = ("objeto", "objeto_or", "objeto_phrases", "objeto_exclude")
        first = {k: observations[0]["params"].get(k) for k in fields}
        for observation in observations[1:]:
            assert {k: observation["params"].get(k) for k in fields} == first


@settings(max_examples=12, derandomize=True, deadline=None)
@given(event=st.sampled_from(["terminados", "liquidados", "iniciados"]),
       current=st.sampled_from(["actualmente activos", "hoy activos", "todavia activos"]))
def test_property_current_clause_cannot_erase_historical_event(event, current):
    observation = observe(f"analiza contratos {event} en agosto y {current} de {BUYER}")
    assert observation["clarification"] and not observation["calls"]
