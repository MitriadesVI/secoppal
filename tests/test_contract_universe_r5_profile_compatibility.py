"""Counter-proofs and persistence compatibility for Entity Profile under CU V1.1.

Adjudicated in
docs/audits/contract_universe_v1_1_integration_r5_profile_compatibility/.

Two fixtures were closed there, and this module keeps both adjudications
observable:

* C03 evidenced membership in the celebrated universe on its contract rows.
  The counter-proofs pin what that state does and does not buy: excluded
  states never enter the count, another buyer never does, and the previously
  selected actor never reaches the emitted SoQL.
* C04 built a contractual profile by editing ``dataset`` on a process profile.
  The persistence tests pin the real boundary: a stamped profile continues, an
  old or unstamped one is sent to review, and no path silently migrates it.
"""
from __future__ import annotations

import sqlite3
import unicodedata

import pytest

from app.core.contract_universe import POLICY_VERSION, ContractUniverse
from app.core.entity_profile.turn import PROFILE_ACTIVE_PARAM
from app.core.soql_builder import SoQLBuilder
from test_actor_conversation import _row, _workflow as actor_workflow
from test_beta_entity_actor_conversation import CELEBRATED_STATE, GALAPA, PROFILE_OPENERS
from test_entity_profile_workflow import WorkflowClient, _workflow
from tests.contract_universe_sql_dates import register_dates


SUPPLIER_QUERY = "cuales son sus mayores proveedores?"
RECORDS_OPENER = "analiza la alcaldia de galapa y muestrame todos los registros del dataset"


class StatefulProfileClient(WorkflowClient):
    """The C03 source, with the contract rows declared by the caller."""

    def __init__(self, contract_rows):
        super().__init__()
        self.contract_rows = contract_rows

    def _execute(self, dataset, soql):
        self._record("source", dataset, soql)
        spec = SoQLBuilder.SPECS[dataset]
        fields = set(spec.select_fields) | {spec.contractor_document_type}
        fields.discard(None)
        contracts = dataset == "jbjy-vk9h"
        with sqlite3.connect(":memory:") as conn:
            conn.row_factory = sqlite3.Row
            conn.create_function("unaccent", 1, lambda value: "".join(
                c for c in unicodedata.normalize("NFD", value or "")
                if not unicodedata.combining(c)
            ))
            register_dates(conn)
            conn.execute("CREATE TABLE records (" + ",".join(
                f'"{field}" {"REAL" if field == spec.value else "TEXT"}'
                for field in sorted(fields)
            ) + ")")
            rows = self.contract_rows if contracts else [
                (buyer, None) for buyer, _ in self.contract_rows
            ]
            for index, (buyer, state) in enumerate(rows):
                row = {
                    spec.entity: buyer, spec.object_name: "OBRA PÚBLICA",
                    spec.date: "2026-08-01T00:00:00.000", spec.value: 100,
                    spec.modality: "Contratación directa",
                    ("id_contrato" if contracts else "id_del_proceso"): str(index),
                }
                if contracts and state is not None:
                    row[spec.state] = state
                if spec.contractor_document:
                    actor = buyer != GALAPA
                    row.update({
                        spec.contractor_document: "900333444" if actor else "900111222",
                        spec.contractor_document_type: "NIT",
                        spec.contractor: "ACTOR ALFA" if actor else "PROVEEDOR GALAPA",
                    })
                conn.execute(
                    "INSERT INTO records (" + ",".join(f'"{key}"' for key in row)
                    + ") VALUES (" + ",".join("?" for _ in row) + ")", tuple(row.values()),
                )
            select, separator, rest = soql.partition(" WHERE ")
            assert separator, soql
            return [dict(row) for row in conn.execute(select + " FROM records WHERE " + rest)]

    aggregate = _execute
    query = _execute


def _galapa_profile(tmp_path, contract_rows):
    workflow = actor_workflow(tmp_path, [
        _row("NIT", "900111222", "ACTOR ALFA", 9),
        _row("NIT", "900333444", "ACTOR ALFA", 8),
    ])
    workflow.run_query("busca proveedor ACTOR ALFA", chat_id="galapa")
    workflow.run_query("2", chat_id="galapa")
    client = StatefulProfileClient(contract_rows)
    workflow.secop_client = client
    return workflow.run_query("analizame la alcaldia de galapa", chat_id="galapa"), client


# ── C03 counter-proofs ────────────────────────────────────────────────────

def test_two_celebrated_galapa_contracts_are_counted(tmp_path):
    result, _ = _galapa_profile(tmp_path, [
        (GALAPA, CELEBRATED_STATE), (GALAPA, CELEBRATED_STATE),
    ])
    assert "Se observan 2 contratos" in result["response"]


def test_another_buyer_does_not_reach_the_count(tmp_path):
    """The third row is celebrated too; only the buyer keeps it out."""
    result, client = _galapa_profile(tmp_path, [
        (GALAPA, CELEBRATED_STATE), (GALAPA, CELEBRATED_STATE),
        ("OTRO COMPRADOR", CELEBRATED_STATE),
    ])
    assert "Se observan 2 contratos" in result["response"]
    assert "OTRO COMPRADOR" not in result["response"]
    assert all("OTRO COMPRADOR" not in soql for _, _, soql in client.calls)


def test_the_previously_selected_actor_does_not_reach_the_filters(tmp_path):
    result, client = _galapa_profile(tmp_path, [
        (GALAPA, CELEBRATED_STATE), (GALAPA, CELEBRATED_STATE),
        ("OTRO COMPRADOR", CELEBRATED_STATE),
    ])
    assert all("900333444" not in soql for _, _, soql in client.calls)
    assert not result["resolved_params"].get("contratista")
    assert not result["resolved_params"].get("actor_scope")


@pytest.mark.parametrize("excluded", ["Cancelado", "Borrador"])
def test_an_excluded_state_does_not_grow_the_celebrated_total(tmp_path, excluded):
    result, _ = _galapa_profile(tmp_path, [
        (GALAPA, CELEBRATED_STATE), (GALAPA, CELEBRATED_STATE), (GALAPA, excluded),
    ])
    assert "Se observan 2 contratos" in result["response"]


def test_contract_rows_without_a_state_are_not_evidence_of_celebration(tmp_path):
    """The pre-V1.1 fixture shape: reachable rows, no membership evidence."""
    result, _ = _galapa_profile(tmp_path, [(GALAPA, None), (GALAPA, None)])
    assert "Se observan 2 contratos" not in result["response"]


# ── C04 persistence compatibility ─────────────────────────────────────────

@pytest.mark.parametrize("previous_dataset", ["procesos", "contratos"])
def test_a_current_profile_continues_into_the_supplier_followup(previous_dataset):
    workflow, client = _workflow()
    first = workflow.run_query(PROFILE_OPENERS[previous_dataset], chat_id="persist")
    assert first["resolved_params"]["dataset"] == previous_dataset
    client.calls.clear()
    result = workflow.run_query(SUPPLIER_QUERY, chat_id="persist")
    assert result["route_reason"] == "entity_profile_suppliers"
    assert result["resolved_params"]["entidad_resolved"] == GALAPA
    assert result["resolved_params"][PROFILE_ACTIVE_PARAM] is True


def test_a_stamped_celebrated_profile_keeps_its_universe():
    workflow, _ = _workflow()
    first = workflow.run_query(PROFILE_OPENERS["contratos"], chat_id="celebrated")
    assert first["resolved_params"]["contract_universe"] == ContractUniverse.CELEBRATED.value
    assert first["resolved_params"]["contract_universe_version"] == POLICY_VERSION
    result = workflow.run_query(SUPPLIER_QUERY, chat_id="celebrated")
    assert result["route_reason"] == "entity_profile_suppliers"
    assert result["resolved_params"]["contract_universe"] == ContractUniverse.CELEBRATED.value
    assert result["resolved_params"]["contract_universe_version"] == POLICY_VERSION


def test_an_explicit_source_records_profile_executes_under_its_own_universe():
    """The explicit request is honoured and named; it is not review material."""
    workflow, client = _workflow()
    first = workflow.run_query(RECORDS_OPENER, chat_id="records")
    params = first["resolved_params"]
    assert params["contract_universe"] == ContractUniverse.SOURCE_RECORDS.value
    assert params["contract_universe_version"] == POLICY_VERSION
    assert first["needs_clarification"] is False
    assert "Registros de la fuente solicitados explícitamente." in first["response"]
    assert client.calls
    assert all("estado_contrato" not in soql.split(" WHERE ")[-1]
               for _, _, soql in client.calls)


def test_a_source_records_profile_continues_and_names_the_universe_it_uses():
    """Whichever universe the next view runs under, the answer states it.

    The supplier view goes back to the celebrated default rather than carrying
    the record request forward. That asymmetry against the tabular route is
    recorded as ENTITY_PROFILE_SOURCE_RECORDS_INHERITANCE_DEBT in the R5
    adjudication and is not decided here; what this test refuses is a silent
    switch — the turn continues, and the response names the universe applied.
    """
    workflow, client = _workflow()
    workflow.run_query(RECORDS_OPENER, chat_id="records-followup")
    client.calls.clear()
    result = workflow.run_query(SUPPLIER_QUERY, chat_id="records-followup")
    assert result["route_reason"] == "entity_profile_suppliers"
    assert result["needs_clarification"] is False
    from app.core.contract_universe import label, universe_from_params
    universe = universe_from_params(result["resolved_params"])
    assert label(universe) in result["response"]
    assert result["resolved_params"]["contract_universe_version"] == POLICY_VERSION


@pytest.mark.parametrize("stored", [
    {"contract_universe": "celebrated", "contract_universe_version": "contract-universe-1"},
    {"contract_universe": "celebrated"},
])
def test_an_old_or_unstamped_contractual_profile_is_sent_to_review(stored):
    """Neither shape migrates silently: the answer is review, not an assumed universe."""
    workflow, client = _workflow()
    first = workflow.run_query(PROFILE_OPENERS["contratos"], chat_id="legacy")
    params = {**first["resolved_params"], "dataset": "contratos"}
    params.pop("contract_universe_version", None)
    params.update(stored)
    workflow.conv_store.append_turn(
        chat_id="legacy", user_query=PROFILE_OPENERS["contratos"],
        response=first["response"], parsed_params=params,
    )
    client.calls.clear()
    result = workflow.run_query(SUPPLIER_QUERY, chat_id="legacy")
    assert result["needs_clarification"] is True
    assert "revisión de universo" in result["response"]
    assert client.calls == []


def test_no_profile_turn_persists_a_contract_dataset_without_its_universe():
    """The C04 fixture's shape is unreachable, so it is not historical evidence."""
    openers = [*PROFILE_OPENERS.values(), RECORDS_OPENER, "analiza la alcaldia de galapa en 2026"]
    followups = ["y sus proveedores", "solo contratos", "y por valor", "y en 2026"]
    observed = 0
    for index, opener in enumerate(openers):
        workflow, _ = _workflow()
        chat = f"shape-{index}"
        turns = [workflow.run_query(opener, chat_id=chat)]
        turns += [workflow.run_query(text, chat_id=chat) for text in followups]
        for turn in turns:
            params = turn["resolved_params"]
            if params.get("dataset") != "contratos":
                continue
            observed += 1
            assert params.get("contract_universe_version") == POLICY_VERSION
            assert params.get("contract_universe") in {u.value for u in ContractUniverse}
    assert observed >= len(openers)
