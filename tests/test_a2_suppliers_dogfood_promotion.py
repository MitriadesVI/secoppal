"""Gates for promoting A2 SUPPLIERS to Rodrigo's personal dogfood.

This file is the promotion contract, not a unit-test collection. Each section
is one of the properties the pilot is allowed to assume, and every one of them
is written as "the turn does NOT do X", because the risk being managed is a
silent semantic loss, not a missing feature.

The x2 experiment adjudicated NO_GO on ``UNKNOWN_AS_EXACT_5``. The human
decision for dogfood is to block those surfaces rather than interpret them, so
they appear here as regressions that must clarify with zero source calls --
they are not reopened as defects of the sidecar.
"""
from __future__ import annotations

import json
import pathlib
import socket
from collections import Counter
from datetime import date
from unittest.mock import Mock

import pytest

from app.config import Settings, get_settings
from app.core.capability_binding import supplier_semantics_wiring as wiring
from app.core.capability_binding.dogfood_telemetry import (
    TRACE_KEYS,
    a2_trace_metadata,
    attach_a2_telemetry,
)
from app.core.capability_binding.registry import SUPPLIERS, capability_for
from app.core.capability_binding.supplier_contract import Outcome, decide
from app.core.capability_binding.supplier_dogfood_guard import (
    ambiguous_cardinal_surface,
)
from app.core.capability_binding.supplier_request_authority import assess_supplier_request
from app.core.capability_binding.supplier_request_types import (
    Authorization,
    CardinalityRelation,
)
from app.core.capability_binding.supplier_semantics import VetoOutcome, VetoVerdict
from app.core.conversation_store import ConversationStore
from app.core.entity_resolver import EntityResolver
from app.core.natural_alpha.models import GroundedJob, Observation
from app.core.natural_alpha.provider import ProviderResult
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from tests.test_natural_alpha_contract import IDENTITY

TODAY = date(2026, 9, 15)
X2 = pathlib.Path(__file__).parent / "fixtures" / "a2_supplier_semantics" / "x2"

# The six surfaces the human decision names, plus the two the corpus showed
# reaching an executable route by the same mechanism.
AMBIGUOUS_CARDINALS = (
    "diferente de 5 proveedores de la alcaldía de Barranquilla",
    "distinto de 5 proveedores de la alcaldía de Barranquilla",
    "unos 5 proveedores de la alcaldía de Barranquilla",
    "alrededor de 5 proveedores de la alcaldía de Barranquilla",
    "casi 5 proveedores de la alcaldía de Barranquilla",
    "5 proveedores más o menos de la alcaldía de Barranquilla",
    "un total distinto de 5 proveedores de la alcaldía de Barranquilla",
    "5 proveedores en adelante de la alcaldía de Barranquilla",
)
# An operator on the far side of a hard break is not absent, only unreachable.
SEGMENT_BOUNDARY_CARDINALS = (
    "superior a. 5 proveedores de la alcaldía de Barranquilla",
    "máx. 5 proveedores de la alcaldía de Barranquilla",
)


class RecordingSource:
    timeout = 15

    def __init__(self):
        self.calls = []

    def with_timeout(self, timeout):
        return self

    derived_for_timeout = with_timeout

    def aggregate(self, dataset, soql, **kwargs):
        self.calls.append((dataset, soql))
        return []

    def query(self, dataset, soql, **kwargs):
        self.calls.append((dataset, soql))
        return []

    def count(self, dataset, soql, **kwargs):
        self.calls.append((dataset, soql))
        return 0


class CountingSidecar:
    """A veto that records every call. It must stay at zero on a blocked turn."""

    def __init__(self, verdict=None):
        self.calls = 0
        self.verdict = verdict or VetoVerdict(VetoOutcome.CONFIRMED, "POSITIVE")

    def __call__(self, user_query, refs, quantities=()):
        self.calls += 1
        return self.verdict


@pytest.fixture(scope="module")
def router():
    return QueryRouter(entity_resolver=EntityResolver(pathlib.Path("app/data/aliases_db.json")))


@pytest.fixture
def params(router):
    def build(text):
        return router.parse(text, today=TODAY).params

    return build


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _payload(quote):
    return json.dumps({"jobs": [{"action": "ANALYZE", "aspect": "SUPPLIERS",
                                 "evidence_quote": quote, "occurrence": 1}]})


@pytest.fixture
def make_flow(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "false")
    get_settings.cache_clear()
    import burr.telemetry
    monkeypatch.setattr(burr.telemetry, "g_telemetry_enabled", False)

    def forbidden(*args, **kwargs):
        raise AssertionError("no network in this test")

    monkeypatch.setattr(socket.socket, "connect", forbidden)

    def build(text, enabled=True, email="alpha@example.test"):
        root = tmp_path / f"{enabled}-{email}-{abs(hash(text))}"
        settings = Settings(
            _env_file=None, deepseek_api_key=None, secoppal_natural_alpha_mode="alpha",
            secoppal_natural_alpha_email=email,
            secoppal_a2_suppliers_binding_enabled=enabled,
            secop_feedback_path=root / "traces.jsonl",
            secop_feedback_db_path=root / "feedback.db",
        )
        flow = SecopalWorkflow(settings)
        flow.conv_store = ConversationStore(root / "conversations")
        flow.secop_client = RecordingSource()
        # The quote must occur literally in the text: Alpha's grounding checks
        # it, and a mock that fails grounding would silently test nothing.
        quote = "proveedores" if "proveedores" in text else text[:12]
        flow.natural_alpha_provider = Mock(observe=Mock(
            return_value=ProviderResult(_payload(quote), provider_calls=1)))
        return flow

    yield build
    get_settings.cache_clear()


def _run(flow, text):
    return flow.run_query(text, channel="streamlit", user_identity=IDENTITY, today=TODAY)


# ── DOGFOOD_CARDINAL_AMBIGUITY_GUARD ──────────────────────────────────────────

@pytest.mark.parametrize("text", AMBIGUOUS_CARDINALS)
def test_an_ambiguous_cardinal_clarifies_without_asking_the_sidecar(params, text):
    """It blocks before the sidecar, so the turn costs no provider call at all."""
    sidecar = CountingSidecar()
    decision = decide(text, params(text), polarity_veto=sidecar)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "DOGFOOD_CARDINAL_AMBIGUITY"
    assert sidecar.calls == 0


@pytest.mark.parametrize("text", AMBIGUOUS_CARDINALS)
def test_an_ambiguous_cardinal_reaches_no_source(make_flow, text):
    """Zero source calls is the gate; the route is asserted where A2 owns it.

    Some of these phrasings never reach the contract at all -- the legacy
    pipeline stops them earlier, for its own reasons -- so the receipt is
    checked only when there is one.
    """
    flow = make_flow(text)
    result = _run(flow, text)
    assert flow.secop_client.calls == []
    assert result["route_reason"] != "entity_profile_suppliers"
    receipt = result.get("a2_capability_telemetry")
    if receipt is not None:
        assert receipt["a2_route"] == "DOGFOOD_CARDINAL_AMBIGUITY"
        assert result["route_reason"] == "a2_suppliers_dogfood_cardinal_ambiguity"
        assert result["needs_clarification"] is True


@pytest.mark.parametrize("text", SEGMENT_BOUNDARY_CARDINALS)
def test_an_operator_across_a_hard_break_is_not_read_as_exact(make_flow, text):
    """x2 K47/K48: the period hid the operator and the turn executed EXACT(5)."""
    evidence = assess_supplier_request(text)
    assert evidence.authorization is Authorization.NOT_ACCREDITED
    assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


def test_the_guard_can_only_block(params):
    """Recognizing nothing must leave the decision exactly as it was."""
    text = "cuales son los 5 proveedores de la alcaldía de Barranquilla"
    assert ambiguous_cardinal_surface(text, assess_supplier_request(text)) is None
    assert decide(text, params(text)).outcome is Outcome.EXECUTE


@pytest.mark.parametrize("text", [
    "5 proveedores de 2020 en adelante de la alcaldía de Barranquilla",
    "cuales son los 5 proveedores con mas contratos en la alcaldia de barranquilla en 2025?",
    "proveedores de la alcaldía de Galapa",
    "los 5 proveedores principales de la alcaldía de Galapa",
])
def test_the_guard_is_bound_to_the_quantitys_own_noun_group(text):
    """A trailing phrase that modifies another dimension is not this guard's.

    "de 2020 en adelante" opens a period; reading it as an open-ended count
    would cost coverage on a turn the capability can actually serve.
    """
    assert ambiguous_cardinal_surface(text, assess_supplier_request(text)) is None


def test_the_guard_never_fires_without_a_located_quantity(params):
    """A bare determiner is not a cardinal: "unos proveedores" is not its business.

    That turn still clarifies, on the pre-existing residual-topic authority --
    the point here is that the guard is not the one that stopped it.
    """
    text = "unos proveedores de la alcaldía de Barranquilla"
    assert ambiguous_cardinal_surface(text, assess_supplier_request(text)) is None
    assert decide(text, params(text)).reason != "DOGFOOD_CARDINAL_AMBIGUITY"


def test_the_guard_leaves_the_authority_untouched(params):
    """It classifies nothing. Where the grammar read EXACT, it still reads EXACT.

    "5 proveedores en adelante" is EXECUTABLE to the deterministic authority
    (x2 K21, blind oracle LOWER_BOUND), which is why the guard sits in the
    contract and not only on the deferred route: on that turn the sidecar is
    never consulted, so a guard living there would never see it.
    """
    text = "5 proveedores en adelante de la alcaldía de Barranquilla"
    assert assess_supplier_request(text).authorization is Authorization.EXECUTABLE
    sidecar = CountingSidecar()
    decision = decide(text, params(text), polarity_veto=sidecar)
    assert decision.reason == "DOGFOOD_CARDINAL_AMBIGUITY"
    assert sidecar.calls == 0


# ── x2 corpus replayed against this runtime ───────────────────────────────────

@pytest.fixture(scope="module")
def x2_cases():
    path = X2 / "corpus_cardinal_v1.json"
    assert path.exists(), f"vendored x2 corpus missing: {path}"
    return json.loads(path.read_text(encoding="utf-8"))["cases"]


def test_the_x2_corpus_is_the_measured_one(x2_cases):
    assert len(x2_cases) == 50
    verdicts = json.loads((X2 / "verdicts.json").read_text(encoding="utf-8"))
    # The adjudicated gate this promotion has to answer for.
    assert verdicts["hard_gates"]["UNKNOWN_AS_EXACT_5"] == 11
    assert verdicts["deferred_route_openings"]["UNKNOWN_AS_EXACT_5__ON_DEFERRED_ROUTE"] == 8


def test_a_lying_sidecar_can_only_reach_the_deferred_route(x2_cases, params):
    """Bound the damage a maximally wrong model could do.

    The sidecar is replaced by a stub that confirms an exact five for every
    single turn. By design it *is* the authority on the relation once the local
    grammar defers, so some of those turns execute -- that is the trade the
    pilot is measuring. What must hold is that it cannot open a door the host
    already closed: a refusal by the grammar, by the segment-boundary rule, or
    by the ambiguity guard stays closed no matter what the model says.
    """
    closed = []
    for case in x2_cases:
        text = case["query"].replace("alcaldía de Galapa", "alcaldía de Barranquilla")
        evidence = assess_supplier_request(text)
        guarded = ambiguous_cardinal_surface(text, evidence) is not None
        deferred = (
            not guarded
            and evidence.cardinality.residue_reason is not None
            and evidence.cardinality.residue_reason.value == "UNACCREDITED_CONTEXT"
        )
        executable = not guarded and evidence.authorization is Authorization.EXECUTABLE
        if deferred or executable:
            continue
        sidecar = CountingSidecar(
            VetoVerdict(VetoOutcome.CONFIRMED, "POSITIVE", relation="EXACT", relation_value=5),
        )
        decision = decide(text, params(text), polarity_veto=sidecar)
        if decision.outcome is Outcome.EXECUTE:
            closed.append((case["id"], case["query"]))
    assert closed == [], closed


def test_every_guarded_surface_is_closed_to_a_lying_sidecar(params):
    for text in (*AMBIGUOUS_CARDINALS, *SEGMENT_BOUNDARY_CARDINALS):
        sidecar = CountingSidecar(
            VetoVerdict(VetoOutcome.CONFIRMED, "POSITIVE", relation="EXACT", relation_value=5),
        )
        decision = decide(text, params(text), polarity_veto=sidecar)
        assert decision.outcome is not Outcome.EXECUTE, text


def _oracle() -> dict[str, str]:
    import csv

    path = X2 / "oracle_cardinal_v1.csv"
    assert path.exists(), f"vendored x2 oracle missing: {path}"
    with path.open(encoding="utf-8") as handle:
        return {row["id"]: row["expected_relation"]
                for row in csv.DictReader(handle) if row.get("id")}


@pytest.fixture(scope="module")
def x2_runs():
    path = X2 / "runs.jsonl"
    assert path.exists(), f"vendored x2 runs missing: {path}"
    runs = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    cardinal = [run for run in runs if run["suite"] == "CARDINAL"]
    assert len(runs) == 360 and len(cardinal) == 150
    return cardinal


def test_the_recorded_x2_answers_open_no_executable_route(x2_runs, params):
    """The gate x2 adjudicated NO_GO on, replayed against this runtime.

    Every recorded answer of the real model is fed back through the runtime's
    own adjudicator and contract. A turn whose blind oracle is not EXACT must
    not execute -- which is what ``UNKNOWN_AS_EXACT_5`` measured it doing.
    """
    from app.core.capability_binding.supplier_semantics import (
        adjudicate,
        mention_refs,
        quantity_refs,
    )

    oracle = _oracle()
    openings = []
    for run in x2_runs:
        text = run["query"].replace("alcaldía de Galapa", "alcaldía de Barranquilla")
        evidence = assess_supplier_request(text)

        def veto(query, refs, quantities=(), _raw=run["raw"]):
            return adjudicate(refs, json.loads(_raw), quantities)

        decision = decide(text, params(text), polarity_veto=veto)
        if decision.outcome is Outcome.EXECUTE and oracle[run["id"]] != "EXACT":
            openings.append((run["id"], run["run"], oracle[run["id"]], run["query"]))
        # The host never hands over a quantity it did not locate itself.
        for quantity in quantity_refs(text, evidence):
            assert quantity.quote in text
        for ref in mention_refs(text, evidence):
            assert ref.quote in text
    assert openings == [], openings


def test_the_recorded_x2_answers_still_execute_the_exact_ones(x2_runs, params):
    """The guard must not have closed the door on the cardinality we serve."""
    oracle = _oracle()
    executed = {
        run["id"] for run in x2_runs
        if oracle[run["id"]] == "EXACT"
        and decide(
            run["query"].replace("alcaldía de Galapa", "alcaldía de Barranquilla"),
            params(run["query"].replace("alcaldía de Galapa", "alcaldía de Barranquilla")),
            polarity_veto=lambda q, r, qt=(), _raw=run["raw"]: __import__(
                "app.core.capability_binding.supplier_semantics", fromlist=["adjudicate"],
            ).adjudicate(r, json.loads(_raw), qt),
        ).outcome is Outcome.EXECUTE
    }
    # Not all fourteen: some EXACT phrasings are refused for reasons of their
    # own (a residual term the buyer authority cannot account for). What the
    # pilot needs is that the plain ones still work.
    assert {"K10", "K50"} <= executed


# ── the central incident ──────────────────────────────────────────────────────

INCIDENT = "cuales son los 5 proveedores con mas contratos en la alcaldia de barranquilla en 2025?"


def test_the_central_incident_still_binds_and_answers(make_flow):
    flow = make_flow(INCIDENT)
    result = _run(flow, INCIDENT)
    assert result["route_reason"] == "entity_profile_suppliers"
    assert flow.secop_client.calls, "the capability must query the source"
    assert all("contratos%" not in soql.lower() for _, soql in flow.secop_client.calls)


def test_the_central_incident_preserves_buyer_period_and_cardinality(params):
    decision = decide(INCIDENT, params(INCIDENT))
    assert decision.outcome is Outcome.EXECUTE
    admitted = decision.admitted_params
    assert "barranquilla" in str(admitted.get("entidad", "")).lower()
    assert (admitted.get("fecha_desde"), admitted.get("fecha_hasta")) == ("2025-01-01", "2025-12-31")
    assert not any(admitted.get(key) for key in ("objeto", "objeto_or", "objeto_phrases"))
    evidence = assess_supplier_request(INCIDENT)
    assert evidence.cardinality.relation is CardinalityRelation.EXACT
    assert evidence.cardinality.values == (5,)
    assert evidence.polarity.value == "POSITIVE"


# ── eligibility: the flag may not reach anyone outside the Alpha lane ─────────

def test_a_non_eligible_user_binds_nothing_even_with_the_flag_on(make_flow):
    """Alpha's own identity receipt is the gate; the A2 flag does not widen it."""
    flow = make_flow(INCIDENT, email="someone-else@example.test")
    result = _run(flow, INCIDENT)
    assert not result["route_reason"].startswith("a2_suppliers")
    assert result.get("a2_capability_telemetry") is None


def test_shadow_mode_never_binds():
    settings = Settings(_env_file=None, secoppal_a2_suppliers_binding_enabled=True)
    observation = Observation("shadow", "MATCHED", (GroundedJob("ANALYZE", "SUPPLIERS", 0, 5),))
    assert capability_for(observation, settings) is None


def test_a_multi_job_proposal_never_binds():
    settings = Settings(_env_file=None, secoppal_a2_suppliers_binding_enabled=True)
    observation = Observation("alpha", "MATCHED", (
        GroundedJob("ANALYZE", "SUPPLIERS", 0, 5),
        GroundedJob("ANALYZE", "SUPPLIERS", 6, 9),
    ))
    assert capability_for(observation, settings) is None


def test_the_binding_flag_is_off_by_default():
    assert Settings(_env_file=None).secoppal_a2_suppliers_binding_enabled is False
    assert Settings(_env_file=None).secoppal_a2_supplier_polarity_sidecar_enabled is False


def test_binding_requires_the_flag():
    observation = Observation("alpha", "MATCHED", (GroundedJob("ANALYZE", "SUPPLIERS", 0, 5),))
    assert capability_for(observation, Settings(_env_file=None)) is None
    assert capability_for(observation, Settings(
        _env_file=None, secoppal_a2_suppliers_binding_enabled=True)) == SUPPLIERS


# ── the provider may never let a turn through ────────────────────────────────

def test_the_sidecar_client_never_retries():
    from app.core.capability_binding import supplier_semantics_provider as provider

    captured = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)
            raise RuntimeError("stop after construction")

    import sys
    import types
    module = types.ModuleType("openai")
    module.OpenAI = FakeClient
    sys.modules["openai"] = module
    try:
        assert provider.classify("q", (), api_key="k", model="m", timeout_seconds=3.0) is None
    finally:
        sys.modules.pop("openai", None)
    assert captured["max_retries"] == 0
    assert captured["timeout"] == 3.0


@pytest.mark.parametrize("failure", [
    RuntimeError("provider down"),
    TimeoutError("timed out"),
])
def test_a_failed_sidecar_clarifies_instead_of_executing(params, monkeypatch, failure):
    monkeypatch.setattr(wiring, "supplier_polarity_sidecar_enabled", lambda: True)
    monkeypatch.setattr(wiring, "get_settings", lambda: Settings(
        _env_file=None, deepseek_api_key="k"))

    def boom(*args, **kwargs):
        raise failure

    monkeypatch.setattr(
        "app.core.capability_binding.supplier_semantics_provider.OpenAI", boom, raising=False,
    )
    monkeypatch.setattr(wiring, "classify", lambda *a, **k: None)
    veto = wiring.configured_polarity_veto()
    text = "proveedores de la alcaldía de Barranquilla"
    decision = decide(text, params(text), polarity_veto=veto)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "POLARITY_SIDECAR_UNAVAILABLE"


@pytest.mark.parametrize("payload", [
    None, {}, {"mentions": "not a list"}, {"mentions": []},
    {"mentions": [{"quote": "proveedores", "occurrence": 1, "polarity": "SI"}]},
    {"mentions": [{"quote": "inventado", "occurrence": 1, "polarity": "POSITIVE"}]},
])
def test_a_malformed_sidecar_answer_blocks_like_a_veto(params, monkeypatch, payload):
    monkeypatch.setattr(wiring, "supplier_polarity_sidecar_enabled", lambda: True)
    monkeypatch.setattr(wiring, "get_settings", lambda: Settings(
        _env_file=None, deepseek_api_key="k"))
    monkeypatch.setattr(wiring, "classify", lambda *a, **k: payload)
    veto = wiring.configured_polarity_veto()
    text = "proveedores de la alcaldía de Barranquilla"
    decision = decide(text, params(text), polarity_veto=veto)
    assert decision.outcome is Outcome.CLARIFY


@pytest.mark.parametrize("turn_polarity", ["NEGATED", "MIXED", "UNKNOWN"])
def test_a_non_positive_turn_never_executes(params, turn_polarity):
    outcome = VetoOutcome.VETOED if turn_polarity != "UNKNOWN" else VetoOutcome.UNAVAILABLE
    sidecar = CountingSidecar(VetoVerdict(outcome, turn_polarity))
    text = "proveedores de la alcaldía de Barranquilla"
    decision = decide(text, params(text), polarity_veto=sidecar)
    assert decision.outcome is Outcome.CLARIFY


@pytest.mark.parametrize("relation,value", [
    ("LOWER_BOUND", 5), ("UPPER_BOUND", 5), ("RANGE", 5),
    ("UNKNOWN", 5), ("EXACT", 7), ("NONE", None),
])
def test_a_rescue_only_survives_an_exact_five(params, relation, value):
    """The deferred route may be revived by nothing but an exact five."""
    text = "sus 5 proveedores de la alcaldía de Barranquilla"
    assert assess_supplier_request(text).authorization is Authorization.NOT_ACCREDITED
    sidecar = CountingSidecar(
        VetoVerdict(VetoOutcome.CONFIRMED, "POSITIVE", relation=relation, relation_value=value),
    )
    decision = decide(text, params(text), polarity_veto=sidecar)
    assert decision.outcome is Outcome.CLARIFY
    assert sidecar.calls == 1


def test_a_rescue_of_exactly_five_executes(params):
    text = "sus 5 proveedores de la alcaldía de Barranquilla"
    sidecar = CountingSidecar(
        VetoVerdict(VetoOutcome.CONFIRMED, "POSITIVE", relation="EXACT", relation_value=5),
    )
    record: dict = {}
    decision = decide(text, params(text), polarity_veto=sidecar, record=record)
    assert decision.outcome is Outcome.EXECUTE
    assert record["a2_rescued_by_sidecar"] is True


# ── preconditions that contained the bad bindings ────────────────────────────

def test_a_resolved_actor_subject_is_never_a_supplier_binding(params):
    """The precondition that contained the bad bindings in A2 X0."""
    text = "proveedores de la alcaldía de Barranquilla"
    for key in ("contratista", "actor_scope", "actor_pending"):
        tainted = {**params(text), key: "CAMILA ARAUJO"}
        assert decide(text, tainted).outcome is Outcome.NOT_BOUND, key
    tainted = {**params(text), "actor_conversation_operation": "SET_ACTOR"}
    assert decide(text, tainted).outcome is Outcome.NOT_BOUND


def test_a_person_named_as_the_supplier_source_never_executes(params):
    """A personal name the buyer authority cannot accredit must not reach SECOP."""
    text = "proveedores de Camila Araujo"
    decision = decide(text, params(text))
    assert decision.outcome is not Outcome.EXECUTE


def test_a_buyer_with_geography_clarifies(params):
    text = "proveedores de la alcaldía de Barranquilla en Galapa"
    assert decide(text, params(text)).reason == "GEOGRAPHY_WITH_BUYER"


def test_a_missing_buyer_clarifies(params):
    text = "cuales son los 5 proveedores con mas contratos"
    decision = decide(text, params(text))
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason in ("BUYER_REQUIRED", "TERRITORY_NOT_BUYER")


def test_a_residual_topic_clarifies(params):
    text = "proveedores de obras viales de la alcaldía de Galapa"
    assert decide(text, params(text)).reason == "UNACCREDITED_RESIDUAL"


# ── the receipt ───────────────────────────────────────────────────────────────

def test_a_bound_turn_leaves_a_validated_receipt(make_flow):
    flow = make_flow(INCIDENT)
    result = _run(flow, INCIDENT)
    receipt = result["a2_capability_telemetry"]
    assert receipt["a2_route"] == "ACCREDITED"
    assert receipt["a2_outcome"] == "EXECUTE"
    assert receipt["a2_buyer_accredited"] is True
    assert receipt["a2_period_accredited"] is True
    assert receipt["a2_sidecar_called"] is False


def test_a_blocked_turn_leaves_a_receipt_naming_the_class(make_flow):
    text = AMBIGUOUS_CARDINALS[0]
    flow = make_flow(text)
    result = _run(flow, text)
    receipt = result["a2_capability_telemetry"]
    assert receipt["a2_route"] == "DOGFOOD_CARDINAL_AMBIGUITY"
    assert receipt["a2_ambiguity_class"] == "NOT_EQUAL"
    assert receipt["a2_source_calls_expected"] == 0


def test_the_receipt_never_carries_the_query_or_the_buyer(make_flow):
    flow = make_flow(INCIDENT)
    result = _run(flow, INCIDENT)
    serialized = json.dumps(result["a2_capability_telemetry"], ensure_ascii=False).lower()
    for secret in ("barranquilla", "proveedores", "2025", "cuales"):
        assert secret not in serialized


def test_an_unrecognized_receipt_is_dropped_rather_than_shipped():
    result: dict = {}
    attach_a2_telemetry(result, {"a2_route": "MADE_UP", "a2_outcome": "EXECUTE"})
    assert "a2_capability_telemetry" not in result


def test_receipt_fields_are_typed_not_coerced():
    result = {"a2_capability_telemetry": {
        "a2_route": "ACCREDITED",
        "a2_sidecar_called": "yes",          # not a bool
        "a2_sidecar_latency_ms": float("inf"),
        "a2_mention_witnesses": -1,
        "a2_turn_polarity": "MAYBE",
    }}
    clean = a2_trace_metadata(result)
    assert set(clean) == {"a2_route"}
    assert set(TRACE_KEYS) >= set(clean)


def test_a_turn_the_contract_hands_back_still_leaves_a_receipt(make_flow):
    """"Alpha proposed SUPPLIERS and the host refused" is the measurement."""
    text = "analiza la alcaldía de Barranquilla"
    flow = make_flow(text)
    result = _run(flow, text)
    receipt = result["a2_capability_telemetry"]
    assert receipt["a2_outcome"] == "NOT_BOUND"
    assert receipt["a2_route"] == "INTENT_WITNESS_MISSING"
    assert receipt["a2_mention_witnesses"] == 0


# ── the legacy path must be untouched where the contract does not own it ─────

@pytest.mark.parametrize("text", [
    "analiza la alcaldía de Barranquilla",
    "sin proveedores de la alcaldía de Barranquilla",
    "no quiero proveedores de la alcaldía de Barranquilla",
])
def test_an_unbound_turn_matches_the_flag_off_baseline(make_flow, text):
    flow = make_flow(text)
    baseline = make_flow(text, enabled=False)
    result = _run(flow, text)
    baseline_result = _run(baseline, text)
    assert result["route_reason"] == baseline_result["route_reason"]
    assert Counter(flow.secop_client.calls) == Counter(baseline.secop_client.calls)
