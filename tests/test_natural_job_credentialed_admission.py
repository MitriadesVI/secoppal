"""C3: a valid Observation claims Admission and ends the turn without legacy.

Plan V2 9. The credentialed lane is scaffolding: it claims, records provenance and
stops honestly — zero QueryRouter calls, zero dispatcher, zero source calls. Buyer,
time, ranking and actor authorities are C4 and are deliberately absent here.
"""
import json
import sqlite3

import pytest

from app.config import get_settings
from app.core.accreditation import SUPPLIERS, capability_spec, credential_from_observation
from app.core.conversation_envelope import EnvelopeStatus
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.natural_alpha.models import GroundedJob, Observation
from app.core.natural_alpha.provider import ProviderResult
from app.core.workflow.admission import AdmittedTurn, admit
from app.core.workflow.admission_rules import AdmissionDecision, AdmissionRoute, InputShape
from app.core.workflow.continuation import ContinuationOwner
from tests.test_natural_alpha_contract import IDENTITY, payload
from tests.test_natural_alpha_runtime import MULTI, make_flow, run  # noqa: F401

GALAPA = "con quien ha contratado la alcaldia de galapa en 2025"
CARTAGENA = "con quien ha contratado mas la alcaldia de cartagena en 2026"
QUOTE = "con quien ha contratado"
SUPPLIERS_PAYLOAD = json.dumps({"jobs": [{
    "action": "ANALYZE", "aspect": "SUPPLIERS", "evidence_quote": QUOTE, "occurrence": 1,
}]})
PARITY_FIELDS = ("response", "route_reason", "parsed_params", "resolved_params", "query_plan",
                 "soql_query", "dataset_id", "results", "total_count", "needs_clarification")


@pytest.fixture
def frame_on(monkeypatch):
    def enable():
        monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "true")
        get_settings.cache_clear()

    yield enable
    get_settings.cache_clear()


def settings_with_rollout(**overrides):
    from types import SimpleNamespace

    fields = {"secoppal_a2_suppliers_binding_enabled": True}
    fields.update(overrides)
    return SimpleNamespace(**fields)


def observation(*jobs, mode="alpha", status="MATCHED"):
    return Observation(mode, status, tuple(jobs), provider_calls=1)


def grounded(quote=QUOTE, action="ANALYZE", aspect="SUPPLIERS"):
    return GroundedJob(action, aspect, 0, len(quote))


def counting_router(flow):
    """Real call counter: wraps the router and delegates, so a reinserted call shows."""
    calls, real = [], flow.query_router.parse

    def parse(query, **kwargs):
        calls.append(query)
        return real(query, **kwargs)

    flow.query_router.parse = parse
    return calls


def counting_dispatch(flow):
    """Real call counter for the legacy chain (any handler, including normal flow)."""
    calls, real = [], flow._dispatch

    def dispatch(ctx, seed=None, **kwargs):
        calls.append(ctx.user_query)
        return real(ctx, seed, **kwargs)

    flow._dispatch = dispatch
    return calls


def credentialed_flow(make_flow, *, binding=True, mode="alpha", response=None, frame_on=None):
    flow = make_flow(mode, response if response is not None
                     else ProviderResult(SUPPLIERS_PAYLOAD, provider_calls=1))
    flow.settings.secoppal_a2_suppliers_binding_enabled = binding
    if frame_on is not None:
        frame_on()
    return flow


# ── C3.1: the seam ────────────────────────────────────────────────────────────

def test_seam_requires_alpha_matched_single_declared_job_and_rollout():
    settings = settings_with_rollout()
    good = observation(grounded())
    built = credential_from_observation(good, GALAPA, settings=settings)
    assert built is not None and built.capability == SUPPLIERS
    assert built.source == "NATURAL_ALPHA"
    assert credential_from_observation(None, GALAPA, settings=settings) is None
    assert credential_from_observation(
        observation(grounded(), mode="shadow"), GALAPA, settings=settings) is None
    assert credential_from_observation(
        observation(grounded(), status="SKIPPED"), GALAPA, settings=settings) is None
    assert credential_from_observation(
        observation(grounded(), grounded()), GALAPA, settings=settings) is None
    assert credential_from_observation(
        observation(grounded(action="SEARCH", aspect="CONTRACTS")), GALAPA,
        settings=settings) is None
    assert credential_from_observation(
        good, GALAPA, settings=settings_with_rollout(
            secoppal_a2_suppliers_binding_enabled=False)) is None


def test_seam_refuses_evidence_that_does_not_land_in_this_raw():
    settings = settings_with_rollout()
    beyond = GroundedJob("ANALYZE", "SUPPLIERS", 0, len(GALAPA) + 3)
    assert credential_from_observation(observation(beyond), GALAPA, settings=settings) is None


def test_seam_credential_only_corresponds_to_the_turn_it_came_from():
    built = credential_from_observation(
        observation(grounded()), GALAPA, settings=settings_with_rollout())
    assert built.corresponds_to(GALAPA)
    assert built.corresponds_to(CARTAGENA) is False
    same_length = GALAPA[:-1] + ("X" if GALAPA[-1] != "X" else "Y")
    assert built.corresponds_to(same_length) is False
    assert built.corresponds_to(GALAPA + " extra") is False


# ── C3.3: typed authority travels, or not at all ──────────────────────────────

def test_admitted_turn_carries_credential_and_spec_together():
    decision = AdmissionDecision(AdmissionRoute.CLARIFY, "x", InputShape.TEXT,
                                 ContinuationOwner.ENTITY_PROFILE)
    credential = credential_from_observation(
        observation(grounded()), GALAPA, settings=settings_with_rollout())
    base = dict(decision=decision, profile_state=ProfileConversationState(), contract=None,
                envelope_status=EnvelopeStatus.EMPTY, conversation_revision=0, params={},
                effective_user_query=GALAPA)
    together = AdmittedTurn(**base, credential=credential,
                            capability_spec=capability_spec(SUPPLIERS))
    assert together.credential is credential
    assert together.capability_spec is capability_spec(SUPPLIERS)
    with pytest.raises(ValueError, match="CREDENTIAL_SPEC_MISMATCH"):
        AdmittedTurn(**base, credential=credential)
    with pytest.raises(ValueError, match="CREDENTIAL_SPEC_MISMATCH"):
        AdmittedTurn(**base, capability_spec=capability_spec(SUPPLIERS))


# ── C3.4 / C3.5 / C3.6 / C3.7: the credentialed lane ──────────────────────────

@pytest.mark.parametrize("raw", [GALAPA, CARTAGENA])
def test_credentialed_turn_claims_admission_without_any_legacy_call(frame_on, make_flow, raw):
    flow = credentialed_flow(make_flow, frame_on=frame_on)
    router_calls = counting_router(flow)
    dispatch_calls = counting_dispatch(flow)

    result = run(flow, raw, chat_id="c3")

    # C3.4 — the claim, with the scaffolding clarification and nothing else.
    assert result["route_reason"] == "admission_accredited_authorities_pending"
    assert result["needs_clarification"] is True
    assert "todavía no ha acreditado" in result["response"]
    assert result["parsed_params"] == {} and result["resolved_params"] == {}
    # C3.6 — provenance rides the one trace this turn writes.
    assert result["credential_capability"] == SUPPLIERS
    assert result["credential_source"] == "NATURAL_ALPHA"
    assert result["admission_owner"] == "ENTITY_PROFILE"
    assert result["admission_reason"] == "accredited_authorities_pending"
    assert result["legacy_bypassed"] is True
    assert result["natural_alpha_status"] == "MATCHED"
    assert result["natural_alpha_jobs"] == [
        {"action": "ANALYZE", "aspect": "SUPPLIERS",
         "evidence_interval": [0, len(QUOTE)]}]
    assert result["natural_alpha_provider_calls"] == 1
    traces = flow.feedback.get_recent()
    assert len(traces) == 1
    assert traces[0]["trace_id"] == result["trace_id"]
    assert traces[0]["natural_alpha_status"] == "MATCHED"
    # One classified row, and the credentialed claim counts as reached alpha.
    with sqlite3.connect(flow.feedback.db_path) as conn:
        classes = [row[0] for row in conn.execute(
            "SELECT json_extract(trace_json, '$.natural_alpha_coverage_class') FROM traces")]
    assert classes == ["REACHED_ALPHA"]
    # C3.5 — real witnesses, not route_reason inference.
    assert router_calls == []
    assert dispatch_calls == []
    assert flow.secop_client.calls == []
    assert flow.natural_alpha_provider.observe.call_count == 1


def test_admit_itself_never_parses_a_credentialed_turn(frame_on, make_flow):
    flow = credentialed_flow(make_flow, frame_on=frame_on)
    router_calls = counting_router(flow)
    deps = flow.deps
    turned = admit(
        deps, user_query=GALAPA, chat_id=None, seed_resolved_params=None,
        observation=observation(grounded()),
    )
    assert turned.route is AdmissionRoute.CLARIFY
    assert turned.decision.reason == "accredited_authorities_pending"
    assert turned.decision.owner is ContinuationOwner.ENTITY_PROFILE
    assert turned.params == {}
    assert turned.credential is not None and turned.capability_spec is not None
    assert router_calls == []


# ── C3.8: negative controls ───────────────────────────────────────────────────

@pytest.mark.parametrize("mode,response,binding", [
    ("off", ProviderResult(SUPPLIERS_PAYLOAD, provider_calls=1), True),
    ("shadow", ProviderResult(SUPPLIERS_PAYLOAD, provider_calls=1), True),
    ("alpha", ProviderResult(payload(action="OTHER", aspect="OTHER"), provider_calls=1), True),
    ("alpha", ProviderResult(payload("NOT IN RAW"), provider_calls=1), True),
    ("alpha", ProviderResult(SUPPLIERS_PAYLOAD, provider_calls=1), False),
])
def test_without_a_valid_credential_the_turn_stays_legacy(frame_on, make_flow, mode, response,
                                                          binding):
    baseline = make_flow("off")
    candidate = make_flow(mode, response)
    candidate.settings.secoppal_a2_suppliers_binding_enabled = binding
    frame_on()
    router_calls = counting_router(candidate)

    before = run(baseline, GALAPA, chat_id="legacy-base")
    after = run(candidate, GALAPA, chat_id="legacy-cand")

    for field in PARITY_FIELDS:
        assert after[field] == before[field], (mode, binding, field)
    assert "credential_capability" not in after
    assert router_calls, "the legacy parser must still run without a credential"


def test_multi_job_observation_keeps_its_existing_terminal_behaviour(frame_on, make_flow):
    flow = credentialed_flow(make_flow, frame_on=frame_on,
                             response=ProviderResult(MULTI, provider_calls=1))
    dispatch_calls = counting_dispatch(flow)

    result = run(flow, "busca contratos de parques y dime cuánto suman", chat_id="multi")

    assert result["route_reason"] == "natural_alpha_multi_job_stepwise"
    assert result["needs_clarification"] is True
    assert "credential_capability" not in result
    assert result["_continuation_owner"] == "NONE"
    assert flow.secop_client.calls == []
    # The multi-job terminal is still produced by the dispatcher seam: existing
    # behaviour, one pass, no credential.
    assert len(dispatch_calls) == 1
    assert len(flow.feedback.get_recent()) == 1
