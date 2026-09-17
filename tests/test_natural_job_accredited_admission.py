"""C4A: the accredited assembly inside the real turn, still with zero source calls.

Plan V2 9-17. A valid credential claims the turn, the independent authorities read
the raw question, and the turn ends with accredited params plus receipts — never in
QueryRouter, never in the dispatcher, never in a source. READY still stops (C4B
owns execution); CLARIFY and OTHER_OWNER report their own reason.
"""
import json

import pytest

from app.config import get_settings
from app.core.accreditation import SUPPLIERS
from app.core.natural_alpha.provider import ProviderResult
from tests.test_natural_alpha_contract import payload
from tests.test_natural_alpha_runtime import make_flow, run  # noqa: F401
from tests.test_natural_job_credentialed_admission import counting_dispatch, counting_router

GALAPA = "con quien ha contratado la alcaldia de galapa en 2025"
OBRAS = "proveedores de galapa en obras civiles"
NEGATION = "no me muestres con quien ha contratado Galapa"
ACTOR = "contratos del proveedor Fundacaribe"


def suppliers_payload(quote):
    return json.dumps({"jobs": [{
        "action": "ANALYZE", "aspect": "SUPPLIERS", "evidence_quote": quote, "occurrence": 1,
    }]})


@pytest.fixture
def frame_on(monkeypatch):
    def enable():
        monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "true")
        get_settings.cache_clear()

    yield enable
    get_settings.cache_clear()


def accredited_flow(make_flow, frame_on, quote, *, binding=True):
    flow = make_flow("alpha", ProviderResult(suppliers_payload(quote), provider_calls=1))
    flow.settings.secoppal_a2_suppliers_binding_enabled = binding
    frame_on()
    return flow


def test_ready_transports_accredited_params_and_receipts(frame_on, make_flow):
    flow = accredited_flow(make_flow, frame_on, "con quien ha contratado")
    router_calls = counting_router(flow)
    dispatch_calls = counting_dispatch(flow)

    result = run(flow, GALAPA, chat_id="c4a")

    assert result["route_reason"] == "admission_accredited_execution_pending"
    assert result["needs_clarification"] is True
    params = result["accredited_params"]
    assert params["entidad_resolved"].startswith("MUNICIPIO DE GALAPA")
    assert params["fecha_desde"] == "2025-01-01" and params["fecha_hasta"] == "2025-12-31"
    assert params["dataset"] == "contratos"
    assert result["authority_status"] == "READY" and result["authority_reason"] == "accredited"
    assert result["buyer_authority"].startswith("MUNICIPIO DE GALAPA")
    assert result["temporal_authority"] == "2025-01-01..2025-12-31"
    assert result["residual_text"] == "" and result["residual_material"] == []
    assert result["credential_overreach"] is False
    assert result["credential_capability"] == SUPPLIERS
    assert result["query_router_calls"] == 0 and result["legacy_bypassed"] is True
    assert [receipt["dimension"] for receipt in result["authority_receipts"]] == [
        "BUYER", "TIME", "ACTOR", "RANKING", "CARDINALITY", "SOURCE"]
    # The real witnesses: no parser, no dispatcher, no source, one trace.
    assert router_calls == [] and dispatch_calls == []
    assert flow.secop_client.calls == []
    assert len(flow.feedback.get_recent()) == 1


def test_overbroad_evidence_still_clarifies_never_executes(frame_on, make_flow):
    flow = accredited_flow(make_flow, frame_on, OBRAS)
    router_calls = counting_router(flow)

    result = run(flow, OBRAS, chat_id="c4a-obras")

    assert result["route_reason"] == "admission_UNACCREDITED_RESIDUAL"
    assert result["authority_status"] == "CLARIFY"
    assert result["credential_overreach"] is True
    assert result["evidence_consumes_residual"] is False
    assert "obras" in result["residual_text"]
    assert result["accredited_params"] == {}
    assert router_calls == [] and flow.secop_client.calls == []


def test_named_actor_stops_as_other_owner(frame_on, make_flow):
    flow = accredited_flow(make_flow, frame_on, "contratos del proveedor")
    dispatch_calls = counting_dispatch(flow)

    result = run(flow, ACTOR, chat_id="c4a-actor")

    assert result["route_reason"] == "admission_other_owner_pending"
    assert result["authority_status"] == "OTHER_OWNER"
    assert result["actor_owner"] == "ACTOR"
    assert result["accredited_params"] == {}
    assert dispatch_calls == [] and flow.secop_client.calls == []


def test_negated_scope_stops_with_its_own_reason(frame_on, make_flow):
    flow = accredited_flow(make_flow, frame_on, "con quien ha contratado")

    result = run(flow, NEGATION, chat_id="c4a-negation")

    assert result["route_reason"] == "admission_negation_scope_unaccredited"
    assert result["authority_status"] == "CLARIFY"
    assert result["accredited_params"] == {}
    assert flow.secop_client.calls == []


def test_without_rollout_the_turn_never_assembles(frame_on, make_flow):
    baseline = make_flow("off")
    candidate = accredited_flow(make_flow, frame_on, "con quien ha contratado", binding=False)
    router_calls = counting_router(candidate)

    before = run(baseline, GALAPA, chat_id="no-flag-base")
    after = run(candidate, GALAPA, chat_id="no-flag-cand")

    for field in ("response", "route_reason", "parsed_params", "resolved_params", "soql_query",
                  "dataset_id", "results", "total_count", "needs_clarification"):
        assert after[field] == before[field], field
    assert "accredited_params" not in after and "authority_status" not in after
    assert router_calls, "the legacy parser must still run without a credential"


def test_invalid_evidence_keeps_the_turn_legacy(frame_on, make_flow):
    baseline = make_flow("off")
    candidate = accredited_flow(make_flow, frame_on, "NOT IN RAW")
    router_calls = counting_router(candidate)

    before = run(baseline, GALAPA, chat_id="bad-evidence-base")
    after = run(candidate, GALAPA, chat_id="bad-evidence-cand")

    for field in ("response", "route_reason", "parsed_params", "resolved_params", "soql_query",
                  "dataset_id", "results", "total_count", "needs_clarification"):
        assert after[field] == before[field], field
    assert "accredited_params" not in after
    assert router_calls
