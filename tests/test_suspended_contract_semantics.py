"""INV-STATE-SUSPENDED-IS-DATASET-AWARE.

Under Contract Universe V1.1 a suspended contract is CELEBRATED, while a
cancelled record sits outside that universe. ``no_procede`` held both for
contracts, so the same word answered with two incompatible meanings and
Suspendido appeared in two contractual families at once — the duplicate
test_catalog_no_unexpected_duplicates reported.

Suspension now resolves per lifecycle through DATASET_AWARE_FAMILY, the same
infrastructure that already splits "vigente". Adjudicated in
docs/audits/contract_universe_v1_1_final_closeout/.
"""
from __future__ import annotations

import pytest

from app.core.estado_families import resolve_estado
from app.core.query_router import QueryRouter
from test_entity_profile_workflow import _workflow


def parse(text: str) -> dict:
    return dict(QueryRouter().parse(text).params)


@pytest.mark.parametrize("token", ["suspendido", "suspendidos", "suspendida", "suspendidas"])
def test_a_suspended_contract_is_exactly_suspendido(token):
    assert resolve_estado(token, "contratos") == {
        "family": "contrato_suspendido",
        "filters": {"estado_contrato": ["Suspendido"]},
        "consumed_span": token,
        "kind": "family",
    }


@pytest.mark.parametrize("query", ["contratos suspendidos", "contratos firmados suspendidos"])
def test_a_suspended_contract_query_never_widens_to_cancelado(query):
    params = parse(query)
    assert params["dataset"] == "contratos"
    assert params["estado_contrato"] == ["Suspendido"]
    assert params["estado_family"] == "contrato_suspendido"
    assert params.get("objeto") is None


def test_the_process_reading_of_suspension_is_unchanged():
    params = parse("procesos suspendidos")
    assert params["dataset"] == "procesos"
    # PROCESS_STATE_REQUEST_INTEGRITY_V1: the process axis is unchanged
    # (no_procede), but a named literal no longer widens to its family.
    assert params["estado_del_procedimiento"] == ["Suspendido"]
    assert params["estado_family"] == "no_procede"


def test_a_bare_suspended_followup_reaches_the_contract_reading():
    """R4's inherited limit: the isolated parse resolved it on the wrong axis.

    Before this, "y los suspendidos" over a contract search answered with
    Cancelado — the state the user did not ask for, and the one Contract
    Universe excludes.
    """
    workflow, _ = _workflow()
    workflow.run_query("contratos de la alcaldia de galapa", chat_id="susp")
    params = workflow.run_query("y los suspendidos", chat_id="susp")["resolved_params"]
    assert params["estado_contrato"] == ["Suspendido"]
    assert "Cancelado" not in params["estado_contrato"]
    assert params["estado_family"] == "contrato_suspendido"


def test_a_bare_cancelled_followup_still_means_cancelled():
    """The re-read must not turn every no_procede word into a suspension."""
    workflow, _ = _workflow()
    workflow.run_query("contratos de la alcaldia de galapa", chat_id="canc")
    params = workflow.run_query("y los cancelados", chat_id="canc")["resolved_params"]
    assert params["estado_contrato"] == ["Cancelado"]


def test_cancelled_contracts_still_come_through_contract_universe():
    params = parse("contratos cancelados")
    assert params["contract_universe"] == "source_records"
    assert params["estado_contrato"] == ["Cancelado"]


@pytest.mark.parametrize("token", ["desierto", "desierta", "desiertos", "desiertas", "revocado"])
def test_a_process_only_outcome_invents_no_contractual_state(token):
    """A contract is never void for lack of bidders; it projects nothing."""
    assert resolve_estado(token, "contratos") is None
    assert resolve_estado(token, "procesos")["family"] == "no_procede"


def test_a_void_process_query_keeps_its_process_reading():
    params = parse("procesos desiertos")
    assert params["estado_del_procedimiento"] == ["Cancelado", "Suspendido"]
    assert params["estado_family"] == "no_procede"


def test_suspendido_belongs_to_exactly_one_contractual_family():
    from app.core.estado_families import ESTADO_FAMILIES

    owners = [
        name for name, family in ESTADO_FAMILIES.items()
        if "Suspendido" in family.get("contratos", {}).get("estado_contrato", [])
    ]
    assert owners == ["contrato_suspendido"]
