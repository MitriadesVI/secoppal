"""End-to-end SUPPLIERS binding through the real workflow, with a recording source."""
import json
import socket
from collections import Counter
from datetime import date
from unittest.mock import Mock

import pytest

from app.config import Settings, get_settings
from app.core.conversation_store import ConversationStore
from app.core.natural_alpha.provider import ProviderResult
from app.core.orchestrator import SecopalWorkflow
from tests.test_natural_alpha_contract import IDENTITY


class RecordingSource:
    timeout = 15

    def __init__(self, rows=None):
        self.calls = []
        self.rows = rows or []

    def with_timeout(self, timeout):
        return self

    derived_for_timeout = with_timeout

    def aggregate(self, dataset, soql, **kwargs):
        self.calls.append((dataset, soql))
        return list(self.rows) if "GROUP BY" in soql else []

    def query(self, dataset, soql, **kwargs):
        self.calls.append((dataset, soql))
        return []

    def count(self, dataset, soql, **kwargs):
        self.calls.append((dataset, soql))
        return 0


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

    def build(text, enabled=True):
        root = tmp_path / str(enabled)
        settings = Settings(
            _env_file=None, deepseek_api_key=None, secoppal_natural_alpha_mode="alpha",
            secoppal_natural_alpha_email="alpha@example.test",
            secoppal_a2_suppliers_binding_enabled=enabled,
            secop_feedback_path=root / "traces.jsonl", secop_feedback_db_path=root / "feedback.db",
        )
        flow = SecopalWorkflow(settings)
        flow.conv_store = ConversationStore(root / "conversations")
        flow.secop_client = RecordingSource()
        flow.natural_alpha_provider = Mock(observe=Mock(
            return_value=ProviderResult(_payload(text[:20]), provider_calls=1)))
        return flow

    yield build
    get_settings.cache_clear()


def _run(flow, text):
    return flow.run_query(text, channel="streamlit", user_identity=IDENTITY,
                          today=date(2026, 9, 15))


def test_incident_opens_suppliers_without_the_spurious_topic(make_flow):
    text = "cuales son los 5 proveedores con mas contratos en la alcaldia de barranquilla en 2025?"
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "entity_profile_suppliers"
    assert flow.secop_client.calls, "the capability must query the source"
    assert all(dataset == "jbjy-vk9h" for dataset, _ in flow.secop_client.calls)
    assert all("contratos%" not in soql.lower() for _, soql in flow.secop_client.calls)
    assert "No encontré proveedores" in result["response"]


def test_injected_supplier_job_without_witness_keeps_baseline_route(make_flow):
    text = "analiza la alcaldía de Barranquilla"
    flow = make_flow(text)
    baseline = make_flow(text, enabled=False)
    result = _run(flow, text)
    baseline_result = _run(baseline, text)
    assert result["route_reason"] == "entity_profile_overview"
    assert result["route_reason"] == baseline_result["route_reason"]
    assert Counter(flow.secop_client.calls) == Counter(baseline.secop_client.calls)


@pytest.mark.parametrize("text", [
    "sin proveedores de la alcaldía de Barranquilla",
    "no sus proveedores de la alcaldía de Barranquilla",
    "proveedores no de la alcaldía de Barranquilla",
    "excluye los 5 proveedores de la alcaldía de Barranquilla",
])
def test_non_positive_supplier_witness_keeps_baseline_route_and_calls(make_flow, text):
    flow = make_flow(text)
    baseline = make_flow(text, enabled=False)
    result = _run(flow, text)
    baseline_result = _run(baseline, text)
    assert result["route_reason"] == baseline_result["route_reason"]
    assert Counter(flow.secop_client.calls) == Counter(baseline.secop_client.calls)


def test_buyer_plus_geography_clarifies_before_supplier_calls(make_flow):
    text = "proveedores de la alcaldía de Barranquilla en Galapa"
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_geography_with_buyer"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


@pytest.mark.parametrize("text", [
    "más de 5 proveedores de la alcaldía de Barranquilla",
    "menos de 5 proveedores de la alcaldía de Barranquilla",
    "entre 5 y 10 proveedores de la alcaldía de Barranquilla",
    "5 proveedores o más de la alcaldía de Barranquilla",
    "5 proveedores como mínimo de la alcaldía de Barranquilla",
    "al menos, 5 proveedores de la alcaldía de Barranquilla",
    ">= 5 proveedores de la alcaldía de Barranquilla",
    "X proveedores de la alcaldía de Barranquilla",
    "77777 proveedores de la alcaldía de Barranquilla",
])
def test_unsupported_cardinality_clarifies_before_supplier_calls(make_flow, text):
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_unsupported_cardinality"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


def test_residual_topic_clarifies_with_zero_source_calls(make_flow):
    text = "proveedores de obras viales de la alcaldía de Galapa"
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_unaccredited_residual"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []
    # The clarification names what it could not accredit instead of dropping it.
    assert "«obras»" in result["response"]


def test_flag_off_keeps_legacy_ownership(make_flow):
    text = "proveedores de obras viales de la alcaldía de Galapa"
    flow = make_flow(text, enabled=False)
    result = _run(flow, text)
    assert not result["route_reason"].startswith("a2_suppliers")
