"""ADR-102 §12: a provider outage may run a labeled basic search, only inside its contract.

Contract: docs/audits/foundation_v3_promotion/BASIC_FALLBACK_CONTRACT.md.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import Mock

import pytest

from app.core.foundation_basic_fallback import LABEL, assess
from app.core.foundation_producers import LinguisticProducer
from app.core.interpretation_contract import AcceptanceStatus, InterpretationAcceptance
from tests.test_foundation_messages import assert_public
from tests.test_foundation_silent_paths import NOW, proposal, span


class HTTPStatusError(Exception):
    """Stands for an HTTP failure raised by the provider SDK."""


def _harness(tmp_path, monkeypatch, producer, rows=None):
    from app.core.foundation_projection import SOURCE_SELECT_FIELDS
    from app.core.soql_builder import SoQLBuilder
    from tests.product_contract.harness import ProductHarness

    declared = {dataset: [dict.fromkeys({*SoQLBuilder.SPECS[dataset].select_fields, *SOURCE_SELECT_FIELDS[dataset]})]
                for dataset in (SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET)}
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows or declared, reference_instant=NOW,
                             today=NOW.date(), foundation_producer=producer,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    harness.flow.llm_handler = Mock(name="llm_handler")  # any second LLM call would be recorded here
    return harness


def _down(exc):
    def provider(system, payload):
        raise exc
    return LinguisticProducer(provider)


OUTAGES = [TimeoutError("provider timed out"), RuntimeError("PROVIDER_UNAVAILABLE"), HTTPStatusError("503")]


@pytest.mark.parametrize("exc", OUTAGES, ids=["timeout", "unavailable", "http"])
def test_an_outage_on_a_safe_search_runs_a_labeled_basic_search(tmp_path, monkeypatch, exc):
    harness = _harness(tmp_path, monkeypatch, _down(exc))
    turn = harness.say("contratos de aseo")
    result = turn.result
    assert result["foundation_terminal"] == "PROVIDER_ERROR"
    assert result["foundation_internal_reason"] == f"PROVIDER_ERROR:{type(exc).__name__}"
    assert result["route_reason"] == "foundation_basic_fallback"
    assert result["response"].splitlines()[0] == LABEL
    assert "- dataset: contratos" in result["response"] and "- tema: aseo" in result["response"]
    assert "Puedes reintentar" in result["response"]
    assert_public(result["response"])
    telemetry = result["foundation_telemetry"]
    assert telemetry["degraded"] is True and telemetry["fallback_outcome"] == "EXECUTED"
    assert telemetry["degradation_source"] == "foundation_provider_error"
    assert telemetry["fallback_mode"] == "legacy_deterministic_basic"
    assert telemetry["advanced_provider_calls"] == 1 and telemetry["second_llm_calls"] == 0
    assert telemetry["provider_calls"] == 1 and telemetry["provider_error"] == type(exc).__name__
    assert telemetry["fallback_label_shown"] is True
    assert telemetry["fallback_source_calls"] == len(turn.transport_calls) >= 1
    reasons = {receipt["reason"] for receipt in result["source_call_receipts"]}
    assert reasons <= {"foundation_basic_fallback:query", "foundation_basic_fallback:count"}
    assert harness.flow.llm_handler.mock_calls == []


def test_first_infancy_basic_search_keeps_buyer_and_topic_without_the_universe_word(tmp_path, monkeypatch):
    harness = _harness(tmp_path, monkeypatch, _down(TimeoutError()))
    turn = harness.say("contratos de primera infancia de la alcaldía de barranquilla")
    response = turn.result["response"]
    assert response.startswith(LABEL)
    assert "- entidad: DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA" in response
    assert "- tema: primera, infancia" in response
    (query,) = [call for call in turn.transport_calls if call["operation"] == "query"]
    assert query["dataset"] == "jbjy-vk9h" and "%contratos%" not in query["soql"].lower()


@pytest.mark.parametrize("text,detail", [
    ("contratos", "NO_ANCHOR"),
    ("contratos de 2025", "NO_ANCHOR"),
    ("contratos de aseo sin vigilancia", "NEGATION"),
    ("contratos de aseo que no sean de mínima cuantía", "NEGATION"),
    ("procesos y contratos de aseo", "UNSUPPORTED_KEYS:dataset_scope"),
    ("contratos de más de 100 millones de aseo", "UNSUPPORTED_KEYS:valor_min"),
    ("procesos abiertos de aseo", "UNSUPPORTED_KEYS:estado,estado_del_procedimiento,estado_family,estado_field,intent_type"),
])
def test_an_outage_outside_the_contract_makes_no_call(tmp_path, monkeypatch, text, detail):
    harness = _harness(tmp_path, monkeypatch, _down(TimeoutError()))
    turn = harness.say(text)
    result = turn.result
    assert turn.transport_calls == ()
    assert result["foundation_terminal"] == "PROVIDER_ERROR" and result["total_count"] is None
    assert result["foundation_fallback"]["fallback_outcome"] == "BASIC_FALLBACK_NOT_SAFE"
    assert result["foundation_fallback"]["fallback_unsafe_detail"] == detail
    assert result["foundation_telemetry"]["fallback_label_shown"] is False
    assert not result["response"].startswith(LABEL) and "reintentar" in result["response"]
    assert_public(result["response"])


def test_an_analytic_parse_is_never_a_basic_search(tmp_path, monkeypatch):
    harness = _harness(tmp_path, monkeypatch, _down(TimeoutError()))
    parsed = harness.flow.deps.query_router.parse("cuántos contratos de aseo hay en 2025", today=date(2026, 9, 26))
    params, detail = assess(parsed)
    assert params is None and detail is not None


def test_a_malformed_answer_is_invalid_and_never_falls_back(tmp_path, monkeypatch):
    harness = _harness(tmp_path, monkeypatch, LinguisticProducer(lambda system, payload: "not json"))
    turn = harness.say("contratos de aseo")
    assert turn.result["foundation_terminal"] == "INVALID_PROPOSAL"
    assert "foundation_fallback" not in turn.result and turn.transport_calls == ()


def _conflict():
    raw = "licitaciones para presentarme de 2021"
    from tests.test_foundation_universe_policy import proposal as universe_proposal
    return raw, universe_proposal(raw, "licitaciones para presentarme", [("TIME", "during_year", "2021")])


@pytest.mark.parametrize("case", ["invalid", "clarify", "unsupported", "h7_conflict"])
def test_no_other_terminal_ever_runs_the_fallback(tmp_path, monkeypatch, case):
    raw = "contratos de aseo y de vigilancia"
    produced = {
        "invalid": InterpretationAcceptance(None, AcceptanceStatus.INVALID_PROPOSAL, reason="RAW_HASH_MISMATCH"),
        "clarify": proposal(raw, [("TOPIC", "contains", [span(raw, "aseo"), span(raw, "vigilancia")], "POSITIVE")]),
        "unsupported": proposal(raw, [("TOPIC", "contains", [span(raw, "aseo")], "NEGATIVE")]),
    }.get(case)
    if case == "h7_conflict":
        raw, produced = _conflict()
    harness = _harness(tmp_path, monkeypatch, lambda _: produced)
    turn = harness.say(raw)
    assert turn.result["foundation_terminal"] != "PROVIDER_ERROR"
    assert "foundation_fallback" not in turn.result
    assert turn.result["route_reason"] != "foundation_basic_fallback"
    assert turn.transport_calls == ()


def test_actor_and_supplier_owners_are_never_reached_by_the_fallback(tmp_path, monkeypatch):
    producer = Mock(side_effect=TimeoutError("must never be called"))
    harness = _harness(tmp_path, monkeypatch, producer)
    for text in ("busca proveedor FUNDACARIBE", "contratos del proveedor FUNDACARIBE"):
        result = harness.say(text).result
        assert "foundation_fallback" not in result and "foundation_mode" not in result
    assert producer.call_count == 0
