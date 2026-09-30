"""ADR-102: Foundation claims only first-turn SEARCH that every existing owner declined.

Each owner case runs twice, with Foundation ON and OFF, with Natural Alpha in
alpha mode and the relevant capability bindings enabled. The ON run must keep
the owner's route, calls and answer, and must never call the Foundation producer.
"""

from __future__ import annotations

import json
import socket
from collections import Counter
from datetime import date
from functools import lru_cache
from unittest.mock import Mock

import pytest

from app import config
from app.config import Settings, SupplierSemanticsSettings, get_settings, get_supplier_semantics_settings
from app.core.conversation_store import ConversationStore
from app.core.foundation_producers import decode_proposal
from app.core.natural_alpha.provider import ProviderResult
from app.core.orchestrator import SecopalWorkflow
from tests.test_natural_alpha_contract import IDENTITY

TODAY = date(2026, 9, 15)
SEARCH = "busca contratos de aseo"


class RecordingSource:
    """The real SecopClient over a frozen, empty source; records every effective call."""

    def __init__(self):
        from app.core.soql_builder import SoQLBuilder
        from tests.product_contract.frozen_source import FrozenTransport, frozen_client

        from app.core.foundation_projection import SOURCE_SELECT_FIELDS

        # One all-NULL row per dataset only declares the columns; no filter matches it.
        rows = {dataset: [dict.fromkeys({*SoQLBuilder.SPECS[dataset].select_fields, *SOURCE_SELECT_FIELDS[dataset]})]
                for dataset in (SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET)}
        self.transport = FrozenTransport(rows)
        self.client = frozen_client(self.transport)

    @property
    def calls(self):
        return [(call["operation"], call["dataset"], call["soql"]) for call in self.transport.snapshot()]


def search_proposal(raw):
    """A grounded proposal for «busca contratos de aseo» (records, TOPIC aseo)."""
    noun, topic = raw.index("contratos"), raw.index("aseo")
    payload = {"requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": "contracts",
                             "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                             "universe_evidence": {"start": noun, "end": noun + 9, "quote": "contratos"},
                             "condition_expression": {"operator": "ATOM", "condition_id": "c1"}}],
               "conditions": [{"condition_id": "c1", "request_ids": ["r1"], "dimension": "TOPIC",
                               "operator": "contains", "polarity": "POSITIVE",
                               "evidence": [{"start": topic, "end": topic + 4, "quote": "aseo"}]}]}
    return decode_proposal(payload, raw, arm="C")


def producer_stub(raw):
    """The simple-search proposal; any other text is an unexpected call, never a crash."""
    if raw == SEARCH:
        return search_proposal(raw)
    from app.core.interpretation_contract import AcceptanceStatus, InterpretationAcceptance
    return InterpretationAcceptance(None, AcceptanceStatus.INVALID_PROPOSAL, reason="UNEXPECTED_PRODUCER_CALL")


def jobs(*pairs, quote=None):
    return json.dumps({"jobs": [{"action": action, "aspect": aspect, "evidence_quote": quote_,
                                 "occurrence": 1} for action, aspect, quote_ in pairs]})


@pytest.fixture
def make_flow(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "false")
    monkeypatch.setenv("SECOPPAL_ACTOR_PROFILE_ENABLED", "true")
    get_settings.cache_clear()
    get_supplier_semantics_settings.cache_clear()

    @lru_cache(maxsize=1)
    def isolated_sidecar_settings():
        return SupplierSemanticsSettings(_env_file=None)

    monkeypatch.setattr(config, "get_supplier_semantics_settings", isolated_sidecar_settings)
    import burr.telemetry
    monkeypatch.setattr(burr.telemetry, "g_telemetry_enabled", False)

    def forbidden(*args, **kwargs):
        raise AssertionError("no network in this test")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    built = []

    def build(mode, alpha_payload=None):
        root = tmp_path / f"{mode}-{len(built)}"
        settings = Settings(
            _env_file=None, deepseek_api_key=None,
            secoppal_natural_alpha_mode="alpha" if alpha_payload else "off",
            secoppal_natural_alpha_email="alpha@example.test",
            secoppal_a2_suppliers_binding_enabled=True,
            secoppal_contract_activity_binding_enabled=True,
            secoppal_natural_follow_binding_enabled=True,
            secoppal_foundation_mode=mode,
            secop_feedback_path=root / "traces.jsonl", secop_feedback_db_path=root / "feedback.db",
            secop_actor_alias_index_path=root / "no_actor_index.sqlite",
        )
        flow = SecopalWorkflow(settings)
        flow.conv_store = ConversationStore(root / "conversations")
        source = RecordingSource()
        flow.secop_client, flow.source = source.client, source
        flow.foundation_producer = Mock(side_effect=producer_stub)
        if alpha_payload:
            flow.natural_alpha_provider = Mock(observe=Mock(
                return_value=ProviderResult(alpha_payload, provider_calls=1)))
        built.append(flow)
        return flow

    yield build
    get_settings.cache_clear()
    get_supplier_semantics_settings.cache_clear()


def run(flow, text, chat_id=None):
    return flow.run_query(text, channel="streamlit", user_identity=IDENTITY, today=TODAY, chat_id=chat_id)


OBSERVABLE = ("route_reason", "response", "needs_clarification", "dataset_id", "soql_query", "total_count")


def observable(result):
    return {key: result.get(key) for key in OBSERVABLE}


OWNER_CASES = [
    pytest.param("quiénes son los principales proveedores de la Alcaldía de Galapa",
                 jobs(("ANALYZE", "SUPPLIERS", "proveedores")), "a2_suppliers_", id="entity-profile-suppliers"),
    # Passes the frozen lexical gate: only the capability binding keeps it from Foundation.
    pytest.param("muéstrame los proveedores con más contratos de la alcaldía de Barranquilla en 2025",
                 jobs(("ANALYZE", "SUPPLIERS", "proveedores")), "entity_profile_suppliers",
                 id="entity-profile-suppliers-gate-shaped"),
    pytest.param("cuántos contratos celebró la Alcaldía de Soledad en 2026",
                 jobs(("ANALYZE", "CONTRACT_COUNT", "cuántos contratos")), "analytics_contract_activity",
                 id="contract-activity-count"),
    pytest.param("busca proveedor FUNDACARIBE", None, "ACTOR", id="actor-profile-name"),
    pytest.param("contratos del proveedor FUNDACARIBE", None, "ACTOR", id="actor-profile-contracts-noun"),
    # The legacy parse reads this buyer phrasing as an unresolved contractor name; the
    # actor owner keeps it and Foundation declines (coverage debt, FOUNDATION_ENTRY_ADMISSION_V1).
    pytest.param("busca licitaciones de alcaldía de barranquilla en 2024", None, "ACTOR",
                 id="legacy-actor-parse-keeps-the-turn"),
    pytest.param("revisa Radar PAE Caribe", None, "RADAR", id="radar-run"),
    pytest.param("mis radares", None, "RADAR", id="radar-list"),
    pytest.param("busca contratos de aseo y exporta los resultados a excel",
                 jobs(("SEARCH", "CONTRACTS", "busca contratos de aseo"),
                      ("RESULT_ACTION", "EXPORT_XLSX", "exporta los resultados")),
                 "natural_alpha_multi_job_stepwise", id="natural-alpha-multi-job"),
    pytest.param("CO1.PCCNTR.1234567", None, "direct_lookup", id="direct-lookup"),
    pytest.param("/reset", None, "reset", id="reset"),
    pytest.param("contratos del contratista con NIT 900373913", None, "ACTOR", id="actor-profile-nit"),
    pytest.param("analiza la alcaldía de Barranquilla", None, "entity_profile", id="entity-profile-overview"),
    pytest.param("contratos de aseo cuál es el valor total por año", None, "analytics", id="aggregate-analytics"),
    pytest.param("contratos de aseo de la alcaldía de Soledad en 2025",
                 jobs(("ANALYZE", "CONTRACTS_BY_MODALITY", "contratos")), "analytics_contract_activity",
                 id="alpha-non-search-job"),
]


@pytest.mark.parametrize(("text", "alpha", "route"), OWNER_CASES)
def test_existing_owners_keep_their_turn_with_foundation_on(make_flow, text, alpha, route):
    on, off = make_flow("foundation_v3", alpha), make_flow("legacy", alpha)
    result_on, result_off = run(on, text, chat_id="owner"), run(off, text, chat_id="owner")
    owner = route if route.isupper() else None
    if owner:  # these owners answer with a generic route_reason; their continuation owner names them
        assert result_on.get("_continuation_owner") == owner, result_on.get("_continuation_owner")
    else:
        assert str(result_on.get("route_reason", "")).startswith(route), result_on.get("route_reason")
    assert on.foundation_producer.call_count == 0
    assert "foundation_mode" not in result_on and "foundation_telemetry" not in result_on
    assert observable(result_on) == observable(result_off)
    assert Counter(on.source.calls) == Counter(off.source.calls)


def test_pagination_keeps_its_owner_with_foundation_on(make_flow):
    first = "aseo en Cali"  # not a Foundation-shaped first turn: legacy owns it in both runs
    on, off = make_flow("foundation_v3"), make_flow("legacy")
    for flow in (on, off):
        run(flow, first, chat_id="c1")
    result_on, result_off = run(on, "muéstrame más", chat_id="c1"), run(off, "muéstrame más", chat_id="c1")
    assert on.foundation_producer.call_count == 0
    assert observable(result_on) == observable(result_off)
    assert Counter(on.source.calls) == Counter(off.source.calls)


@pytest.mark.parametrize("alpha", [None, jobs(("SEARCH", "CONTRACTS", SEARCH))])
def test_a_simple_first_turn_search_is_foundation_only_when_on(make_flow, alpha):
    on, off = make_flow("foundation_v3", alpha), make_flow("legacy", alpha)
    result_on, result_off = run(on, SEARCH), run(off, SEARCH)
    assert on.foundation_producer.call_count == 1 and off.foundation_producer.call_count == 0
    assert result_on["foundation_mode"] == "foundation_v3" and result_on["foundation_terminal"] == "READY"
    assert result_on["foundation_telemetry"]["route"] == "accepted"
    assert result_on["foundation_telemetry"]["h7_authority"] == ["PRODUCER"]
    assert "foundation_mode" not in result_off and "source_call_receipts" not in result_off
    assert result_on["trace_id"]
    assert all(dataset == "jbjy-vk9h" for _, dataset, _ in on.source.calls)


def test_a_follow_up_with_history_is_never_foundation(make_flow):
    flow = make_flow("foundation_v3")
    run(flow, "aseo en Cali", chat_id="c1")
    run(flow, SEARCH, chat_id="c1")
    assert flow.foundation_producer.call_count == 0


def test_a_fragment_after_a_foundation_turn_clarifies_instead_of_losing_the_topic(make_flow):
    flow = make_flow("foundation_v3")
    first = run(flow, SEARCH, chat_id="c1")
    assert first["foundation_terminal"] == "READY"
    calls = len(flow.source.calls)
    follow = run(flow, "y de Cali?", chat_id="c1")
    assert follow["needs_clarification"] is True
    assert follow["route_reason"] == "foundation_continuation_unsupported"
    assert len(flow.source.calls) == calls and flow.foundation_producer.call_count == 1
    assert "consulta completa" in follow["response"]
    fresh = run(flow, "busca contratos de vigilancia en Cali", chat_id="c1")
    assert fresh.get("route_reason") != "foundation_continuation_unsupported"
    assert flow.foundation_producer.call_count == 1  # legacy owns a turn with history


def test_the_foundation_history_turn_carries_no_legacy_params(make_flow):
    flow = make_flow("foundation_v3")
    run(flow, SEARCH, chat_id="c1")
    (turn,) = flow.conv_store.get_history("c1", last_n=5)
    assert turn.parsed_params == {} and turn.outcome_reason == "foundation_v3"


@pytest.fixture
def frame_on(monkeypatch):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_frame_admission_keeps_the_entity_profile_and_delegates_search(make_flow, frame_on):
    profile = "analiza la alcaldía de Barranquilla"
    on, off = make_flow("foundation_v3"), make_flow("legacy")
    result_on, result_off = run(on, profile, chat_id="f1"), run(off, profile, chat_id="f1")
    assert str(result_on["route_reason"]).startswith("entity_profile"), result_on["route_reason"]
    assert on.foundation_producer.call_count == 0
    assert observable(result_on) == observable(result_off)
    assert Counter(on.source.calls) == Counter(off.source.calls)
    searched = run(make_flow("foundation_v3"), SEARCH, chat_id="f2")
    assert searched["foundation_mode"] == "foundation_v3" and searched["foundation_terminal"] == "READY"


def test_natural_alpha_owner_keeps_capabilities_and_non_search_jobs():
    from types import SimpleNamespace

    from app.core.foundation_route_owner import natural_alpha_owner

    def observed(*actions, mode="alpha", status="MATCHED"):
        return SimpleNamespace(mode=mode, status=status, jobs=tuple(SimpleNamespace(action=a) for a in actions))

    assert natural_alpha_owner(None, object()) == "CAPABILITY_BINDING"
    assert natural_alpha_owner(observed("SEARCH"), object()) == "CAPABILITY_BINDING"
    assert natural_alpha_owner(observed("ANALYZE"), None) == "NATURAL_ALPHA_JOB"
    assert natural_alpha_owner(observed("SEARCH", "SEARCH"), None) == "NATURAL_ALPHA_JOB"
    assert natural_alpha_owner(observed("SEARCH"), None) is None
    assert natural_alpha_owner(observed("ANALYZE", mode="shadow"), None) is None
    assert natural_alpha_owner(observed("ANALYZE", status="NO_MATCH"), None) is None
    assert natural_alpha_owner(None, None) is None
