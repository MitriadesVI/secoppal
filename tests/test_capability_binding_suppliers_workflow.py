"""End-to-end SUPPLIERS binding through the real workflow, with a recording source."""
import json
import socket
from collections import Counter
from datetime import date
from functools import lru_cache
from unittest.mock import Mock

import pytest

from app import config
from app.config import (
    Settings,
    SupplierSemanticsSettings,
    get_settings,
    get_supplier_semantics_settings,
)
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
        quote = "proveedores" if "proveedores" in text else text[:20]
        flow.natural_alpha_provider = Mock(observe=Mock(
            return_value=ProviderResult(_payload(quote), provider_calls=1)))
        return flow

    yield build
    get_settings.cache_clear()
    get_supplier_semantics_settings.cache_clear()


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
    "excluye los 5 proveedores de la alcaldía de Barranquilla",
])
def test_non_positive_supplier_witness_clarifies_without_source_calls(make_flow, text):
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_intent_witness_not_positive"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


def test_mixed_supplier_witness_clarifies_without_source_calls(make_flow):
    text = "proveedores y sin contratistas de la alcaldía de Barranquilla"
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_intent_witness_not_positive"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


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


@pytest.mark.parametrize("text", [
    "proveedores de educación de la Secretaría de Educación de Cali",
    "proveedores de educación de educación de la Secretaría de Educación de Cali",
])
def test_repeated_buyer_word_does_not_accredit_topic_occurrence(make_flow, text):
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_unaccredited_residual"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


def test_or_topic_group_requires_every_alternative_in_real_workflow(
    make_flow, monkeypatch,
):
    text = "proveedores de la Secretaría de Educación de Cali"
    flow = make_flow(text)
    real_parse = flow.query_router.parse

    def parse_with_or_group(*args, **kwargs):
        parsed = real_parse(*args, **kwargs)
        parsed.params["objeto"] = [["obras viales", "educacion"]]
        return parsed

    monkeypatch.setattr(flow.query_router, "parse", parse_with_or_group)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_unaccredited_residual"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


def test_sidecar_config_failure_while_sidecar_is_on_is_unavailable(
    make_flow, monkeypatch,
):
    monkeypatch.setenv("SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED", "true")
    config.get_supplier_semantics_settings.cache_clear()
    text = "proveedores de la alcaldía de Barranquilla"
    flow = make_flow(text)

    def broken_settings():
        raise RuntimeError("broken configuration")

    monkeypatch.setattr(config, "get_supplier_semantics_settings", broken_settings)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_polarity_sidecar_unavailable"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


def test_lowercase_sidecar_on_with_unrelated_global_error_can_still_veto(
    make_flow, monkeypatch,
):
    from app.core.capability_binding import supplier_semantics_wiring as wiring

    text = "proveedores de la alcaldía de Barranquilla"
    flow = make_flow(text)
    monkeypatch.delenv("SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED", raising=False)
    monkeypatch.setenv("secoppal_a2_supplier_polarity_sidecar_enabled", "true")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "key")
    monkeypatch.setenv("SECOP_TIMEOUT_SECONDS", "not-an-integer")
    config.get_supplier_semantics_settings.cache_clear()
    monkeypatch.setattr(wiring, "classify", lambda *a, **k: None)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_polarity_sidecar_unavailable"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"DEEPSEEK_API_KEY": "key", "DEEPSEEK_MODEL": ""},
        {
            "DEEPSEEK_API_KEY": "key",
            "SECOPPAL_A2_SUPPLIER_POLARITY_TIMEOUT_SECONDS": "invalid",
        },
    ],
)
def test_invalid_sidecar_prerequisite_clarifies_without_source_calls(
    make_flow, monkeypatch, tmp_path, values,
):
    env_file = tmp_path / "invalid-sidecar.env"
    env_file.write_text("".join(
        f"{key}={value}\n"
        for key, value in {
            "SECOPPAL_A2_SUPPLIER_POLARITY_SIDECAR_ENABLED": "true",
            **values,
        }.items()
    ))
    monkeypatch.setattr(
        config,
        "get_supplier_semantics_settings",
        lambda: SupplierSemanticsSettings(_env_file=env_file),
    )
    text = "proveedores de la alcaldía de Barranquilla"
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_polarity_sidecar_unavailable"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


@pytest.mark.parametrize("failure", ["missing_credential", "config_error", "provider_unavailable"])
def test_sidecar_on_but_unavailable_clarifies_without_source_calls(
    make_flow, monkeypatch, failure,
):
    from app.config import SupplierPolaritySidecarConfig, SupplierPolaritySidecarState
    from app.core.capability_binding import supplier_semantics_wiring as wiring

    if failure == "provider_unavailable":
        configuration = SupplierPolaritySidecarConfig(
            SupplierPolaritySidecarState.ENABLED,
            api_key="test-key",
            model="deepseek-chat",
            timeout_seconds=8.0,
        )
        monkeypatch.setattr(wiring, "classify", lambda *args, **kwargs: None)
    else:
        detail = "MISSING_CREDENTIAL" if failure == "missing_credential" else "CONFIGURATION_UNAVAILABLE"
        configuration = SupplierPolaritySidecarConfig(
            SupplierPolaritySidecarState.ENABLED_BUT_UNAVAILABLE,
            detail=detail,
        )
    monkeypatch.setattr(
        wiring, "supplier_polarity_sidecar_config", lambda: configuration,
    )
    text = "proveedores de la alcaldía de Barranquilla"
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "a2_suppliers_polarity_sidecar_unavailable"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


def test_actor_cardinal_turn_keeps_actor_profile_ownership(make_flow):
    text = "5 proveedores en adelante del proveedor NIT 900711074"
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "actor_profile"
    assert result["resolved_params"]["actor_scope"]["base_number"] == "900711074"
    assert flow.secop_client.calls


def test_actor_document_turn_keeps_actor_profile_ownership(make_flow):
    text = "proveedor NIT 900711074"
    flow = make_flow(text)
    result = _run(flow, text)
    assert result["route_reason"] == "actor_profile"
    assert result["resolved_params"]["actor_scope"]["base_number"] == "900711074"
    assert flow.secop_client.calls


def test_named_and_fuzzy_actor_turns_remain_actor_owned(make_flow, tmp_path):
    from tests.actor_prefix_fixtures import resolver_for

    exact = "proveedor Fundacaribe"
    exact_flow = make_flow(exact)
    exact_flow.actor_name_resolver = resolver_for(
        tmp_path / "exact-actors.sqlite",
        [("NIT", "900711074", "FUNDACARIBE")],
    )
    exact_result = _run(exact_flow, exact)
    assert exact_result["route_reason"] == "actor_profile"
    assert exact_flow.secop_client.calls

    fuzzy = "proveedor Fundacarib"
    fuzzy_flow = make_flow(fuzzy)
    fuzzy_flow.actor_name_resolver = resolver_for(
        tmp_path / "fuzzy-actors.sqlite",
        [("NIT", "900711074", "FUNDACARIBE")],
    )
    fuzzy_result = _run(fuzzy_flow, fuzzy)
    assert fuzzy_result["needs_clarification"] is True
    assert fuzzy_result["resolved_params"]["actor_resolution_status"] == "CANDIDATES"
    assert not fuzzy_result["route_reason"].startswith("a2_suppliers")
    assert fuzzy_flow.secop_client.calls == []


def test_flag_off_keeps_legacy_ownership(make_flow):
    text = "proveedores de obras viales de la alcaldía de Galapa"
    flow = make_flow(text, enabled=False)
    result = _run(flow, text)
    assert not result["route_reason"].startswith("a2_suppliers")
