"""Dogfood regressions through the real parser, Burr graph and persisted turns."""
from __future__ import annotations

import json
import sqlite3
import unicodedata

import pytest

from app.config import Settings
from app.core.entity_profile.turn import PROFILE_ACTIVE_PARAM
from app.core.orchestrator import SecopalWorkflow
from app.core.soql_builder import SoQLBuilder
from test_actor_conversation import _row, _workflow as actor_workflow
from test_entity_profile_workflow import WorkflowClient, _workflow


GALAPA = "MUNICIPIO DE GALAPA."


class SqlProfileClient(WorkflowClient):
    """Evaluate emitted SoQL predicates, including the leaked actor predicate.

    Tiny synthetic source, not a SECOP total: two Galapa contracts and a
    different buyer's contract for the previously selected actor. SQLite
    executes the real WHERE; no canned answer keyed to the expected route.
    """

    def _execute(self, dataset, soql):
        self._record("source", dataset, soql)
        spec = SoQLBuilder.SPECS[dataset]
        fields = set(spec.select_fields) | {spec.contractor_document_type}
        fields.discard(None)
        with sqlite3.connect(":memory:") as conn:
            conn.row_factory = sqlite3.Row
            conn.create_function("unaccent", 1, lambda value: "".join(
                c for c in unicodedata.normalize("NFD", value or "")
                if not unicodedata.combining(c)
            ))
            conn.execute("CREATE TABLE records (" + ",".join(
                f'"{field}" {"REAL" if field == spec.value else "TEXT"}'
                for field in sorted(fields)
            ) + ")")
            for index, buyer in enumerate([GALAPA, GALAPA, "OTRO COMPRADOR"]):
                row = {
                    spec.entity: buyer, spec.object_name: "OBRA PÚBLICA",
                    spec.date: "2026-08-01T00:00:00.000", spec.value: 100,
                    spec.modality: "Contratación directa",
                    ("id_contrato" if dataset == "jbjy-vk9h" else "id_del_proceso"): str(index),
                }
                if spec.contractor_document:
                    row.update({
                        spec.contractor_document: "900333444" if index == 2 else "900111222",
                        spec.contractor_document_type: "NIT",
                        spec.contractor: "ACTOR ALFA" if index == 2 else "PROVEEDOR GALAPA",
                    })
                conn.execute(
                    "INSERT INTO records (" + ",".join(row) + ") VALUES ("
                    + ",".join("?" for _ in row) + ")", tuple(row.values()),
                )
            select, separator, rest = soql.partition(" WHERE ")
            assert separator, soql
            return [dict(row) for row in conn.execute(select + " FROM records WHERE " + rest)]

    aggregate = _execute
    query = _execute


def test_entity_profile_galapa_does_not_report_false_zero(tmp_path):
    workflow = actor_workflow(tmp_path, [
        _row("NIT", "900111222", "ACTOR ALFA", 9),
        _row("NIT", "900333444", "ACTOR ALFA", 8),
    ])
    workflow.run_query("busca proveedor ACTOR ALFA", chat_id="galapa")
    selected = workflow.run_query("2", chat_id="galapa")
    assert selected["resolved_params"]["actor_resolution_method"] == "user_selected_candidate"
    client = SqlProfileClient()
    workflow.secop_client = client
    result = workflow.run_query("analizame la alcaldia de galapa", chat_id="galapa")
    assert "Se observan 0 contratos" not in result["response"]
    assert "Se observan 2 contratos" in result["response"]
    assert result["intent_type"] == "new_search"
    assert result["resolved_params"]["entidad_resolved"] == GALAPA
    assert not result["resolved_params"].get("contratista")
    assert not result["resolved_params"].get("actor_scope")
    assert len(client.calls) == 6
    assert all("900333444" not in soql for _, _, soql in client.calls)


@pytest.mark.parametrize("previous_dataset", ["procesos", "contratos"])
@pytest.mark.parametrize("query", [
    "cuales son sus mayores proveedores?",
    "quienes son sus principales proveedores",
    "quien tiene mas contratos",
    "quienes contratan mas con esa entidad",
])
def test_entity_profile_supplier_followup_keeps_buyer(previous_dataset, query):
    workflow, client = _workflow()
    first = workflow.run_query("analiza la alcaldia de galapa", chat_id="suppliers")
    # Historical persisted profiles can be in either dataset; both must work.
    params = {**first["resolved_params"], "dataset": previous_dataset}
    workflow.conv_store.append_turn(
        chat_id="suppliers", user_query="analiza la alcaldia de galapa",
        response=first["response"], parsed_params=params,
    )
    client.calls.clear()
    result = workflow.run_query(query, chat_id="suppliers")
    assert result["route_reason"] == "entity_profile_suppliers"
    assert result["resolved_params"]["entidad_resolved"] == GALAPA
    assert result["resolved_params"][PROFILE_ACTIVE_PARAM] is True
    assert "Principales contratistas por número" in result["response"]
    assert "Principales contratistas por valor" in result["response"]
    assert len(client.calls) == 2
    assert all(dataset == "jbjy-vk9h" for _, dataset, _ in client.calls)
    assert all(GALAPA in soql for _, _, soql in client.calls)


def test_entity_profile_supplier_followup_by_value_keeps_both_rankings():
    workflow, client = _workflow()
    workflow.run_query("analiza la alcaldia de galapa", chat_id="value")
    workflow.run_query("y sus proveedores", chat_id="value")
    client.calls.clear()
    result = workflow.run_query("y por valor", chat_id="value")
    assert result["route_reason"] == "entity_profile_suppliers"
    assert "Principales contratistas por número" in result["response"]
    assert "Principales contratistas por valor" in result["response"]
    assert len(client.calls) == 2


@pytest.mark.parametrize("name, rows, status, reason", [
    ("rodrigo ortiz", [_row("CC", "10000001", "RODRIGO ORTIZ"),
                       _row("CC", "10000002", "RODRIGO ORTIZ")],
     "AMBIGUOUS", "actor_name_ambiguous"),
    ("camila araujo", [_row("CC", "10000001", "CAMILA ANDREA ARAUJO")],
     "CANDIDATES", "actor_name_candidates"),
    ("actor inexistente", [], "UNRESOLVED", "actor_name_requires_resolution"),
    ("actor unico", [_row("NIT", "900111222", "ACTOR UNICO")], "RESOLVED", None),
])
def test_strong_actor_new_search_reaches_resolution(tmp_path, name, rows, status, reason):
    workflow = actor_workflow(tmp_path, rows)
    workflow.run_query("reset", chat_id="actor")
    workflow.run_query("busca proveedor codetec", chat_id="actor")
    client = WorkflowClient()
    workflow.secop_client = client
    result = workflow.run_query(f"busca proveedor {name}", chat_id="actor")
    assert "perdi el contexto" not in result["response"].lower()
    assert result["resolved_params"]["actor_scope"]["observed_name"] == name
    assert result["resolved_params"]["actor_resolution_status"] == status
    if reason:
        assert result["query_plan"]["reason"] == reason
        assert client.calls == []
    else:
        assert result["query_plan"]["should_execute"] is True
    if status in {"AMBIGUOUS", "CANDIDATES"}:
        candidates = result["resolved_params"]["actor_pending"]["candidates"]
        assert candidates
        assert all(c["observed_name"] in result["response"] for c in candidates)


def test_strong_actor_new_search_discards_previous_topic_and_scope(tmp_path):
    workflow = actor_workflow(tmp_path, [_row("NIT", "900111222", "ACTOR X")])
    workflow.secop_client = WorkflowClient()
    workflow.run_query("contratos de alimentacion de la alcaldia de galapa en 2025", chat_id="new")
    result = workflow.run_query("busca proveedor ACTOR X", chat_id="new")
    params = result["resolved_params"]
    assert params["actor_scope"]["observed_name"] == "actor x"
    for key in ["entidad_resolved", "objeto", "fecha_desde", PROFILE_ACTIVE_PARAM]:
        assert not params.get(key)


def test_strong_actor_missing_index_reaches_dedicated_policy(tmp_path):
    workflow = SecopalWorkflow(Settings(secop_actor_alias_index_path=tmp_path / "missing.sqlite"))
    client = WorkflowClient()
    workflow.secop_client = client
    workflow.run_query("busca proveedor codetec", chat_id="missing")
    result = workflow.run_query("busca proveedor rodrigo ortiz", chat_id="missing")
    assert result["query_plan"]["reason"] == "actor_index_unavailable"
    assert client.calls == []


def test_truly_unscoped_search_stays_blocked():
    workflow, client = _workflow()
    workflow.run_query("analiza la alcaldia de galapa", chat_id="empty")
    client.calls.clear()
    result = workflow.run_query("busca contratos", chat_id="empty")
    assert result["needs_clarification"] is True
    assert client.calls == []


def test_actor_candidate_summary_is_persisted_in_trace(tmp_path):
    workflow = actor_workflow(tmp_path, [_row("CC", "10000001", "CAMILA ANDREA ARAUJO")])
    result = workflow.run_query("busca proveedor camila araujo")
    trace = json.loads((tmp_path / "feedback.jsonl").read_text().splitlines()[-1])
    assert trace["actor_resolution_status"] == "CANDIDATES"
    assert trace["actor_resolution_method"] == "fts5_trigram_bm25"
    assert trace["candidates_count"] == len(result["resolved_params"]["actor_pending"]["candidates"])
