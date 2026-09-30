"""ADR-102 §11: the user never reads a Foundation reason code, an exception name or a traceback."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.core.foundation_messages import GENERIC, KNOWN_CODES, user_message
from app.core.interpretation_contract import AcceptanceStatus, InterpretationAcceptance
from tests.test_foundation_silent_paths import NOW, harness_run, proposal, span

CORE = Path(__file__).resolve().parents[1] / "app" / "core"
RUNTIME_FILES = [*CORE.glob("foundation_*.py"), CORE / "interpretation_contract.py"]
FORBIDDEN = (
    re.compile(r"^[A-Z0-9_]+:", re.MULTILINE),          # a raw «CODE:detail» line
    re.compile(r"\b[A-Z][A-Z0-9]*_[A-Z0-9_]+\b"),       # any SCREAMING_SNAKE code
    re.compile(r"\b\w*(?:Error|Exception)\b"),          # exception class names
    re.compile(r"Traceback|File \""),
)


def assert_public(text: str) -> None:
    assert text and text.strip()
    for pattern in FORBIDDEN:
        assert not pattern.search(text), (pattern.pattern, text)


def _source_codes() -> set[str]:
    """Every code literal the Foundation runtime can raise or return."""
    codes = set()
    for path in RUNTIME_FILES:
        for literal in re.findall(r'"([A-Z][A-Z0-9_]{3,}(?::[A-Za-z_{}.]*)?)"', path.read_text(encoding="utf-8")):
            codes.add(literal.replace("{dimension}", "TOPIC"))
    return codes


SOURCE_CODES = sorted(_source_codes())


def test_the_source_scan_finds_the_codes_the_fixes_introduced():
    assert {"MULTI_EVIDENCE_TOPIC_UNSUPPORTED", "TOPIC_OVERLAPS_REQUEST_UNIVERSE",
            "STATE_COMPOSITION_UNSUPPORTED", "VALUE_OPERATOR_EVIDENCE_MISMATCH",
            "HOST_UNIVERSE_RULE_CONFLICT", "NONCONJUNCTIVE_EXPRESSION"} <= set(SOURCE_CODES) | KNOWN_CODES


@pytest.mark.parametrize("terminal", [status.value for status in AcceptanceStatus if status.value != "READY"])
@pytest.mark.parametrize("code", [*SOURCE_CODES, *sorted(KNOWN_CODES),
                                  "PROVIDER_ERROR:TimeoutError", "MALFORMED_PROPOSAL:JSONDecodeError",
                                  "SOQL_PROJECTION_BLOCKED:KeyError('fecha_desde')", "UNKNOWN_FUTURE_CODE:x", "", None])
def test_every_code_and_terminal_renders_public_text(terminal, code):
    assert_public(user_message(terminal, code).text)


@pytest.mark.parametrize("code", sorted(KNOWN_CODES))
def test_every_registered_code_has_its_own_message(code):
    assert user_message("NEEDS_CLARIFICATION", code) != GENERIC


def test_multi_topic_message_offers_the_cited_topics():
    raw = "contratos de aseo y de vigilancia"
    produced = proposal(raw, [("TOPIC", "contains", [span(raw, "aseo"), span(raw, "vigilancia")], "POSITIVE")])
    message = user_message("NEEDS_CLARIFICATION", "MULTI_EVIDENCE_TOPIC_UNSUPPORTED", produced)
    assert "«aseo»" in message.text and "«vigilancia»" in message.text
    assert message.choices[:2] == ("aseo", "vigilancia")


@pytest.mark.parametrize("name,produced,terminal,code", [
    ("invalid", InterpretationAcceptance(None, AcceptanceStatus.INVALID_PROPOSAL, reason="RAW_HASH_MISMATCH"),
     "INVALID_PROPOSAL", "RAW_HASH_MISMATCH"),
    ("no_action", InterpretationAcceptance(None, AcceptanceStatus.NO_ACTION, reason="NO_REQUEST"),
     "NO_ACTION", "NO_REQUEST"),
])
def test_terminals_without_a_proposal_answer_in_plain_spanish(tmp_path, monkeypatch, name, produced, terminal, code):
    turn, harness = harness_run(tmp_path, monkeypatch, "contratos de aseo", produced)
    result = turn.result
    assert result["foundation_terminal"] == terminal
    assert result["foundation_internal_reason"] == code
    assert result["foundation_telemetry"]["internal_reason_code"] == code
    assert result["route_reason"] == f"foundation_{terminal.lower()}"
    assert_public(result["response"])
    assert harness.transport.snapshot() == []


def test_a_clarification_keeps_the_code_internal(tmp_path, monkeypatch):
    raw = "procesos publicados o abiertos de aseo"
    produced = proposal(raw, [("STATE", "is", [span(raw, "publicados"), span(raw, "abiertos")], "POSITIVE"),
                              ("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE")],
                        subject="processes", noun="procesos")
    turn, _ = harness_run(tmp_path, monkeypatch, raw, produced)
    result = turn.result
    assert result["foundation_internal_reason"] == "STATE_COMPOSITION_UNSUPPORTED"
    assert result["route_reason"] == "foundation_needs_clarification"
    assert result["foundation_choices"] == ["Publicados", "Abiertos"]
    assert_public(result["response"])


def test_a_projection_block_keeps_the_code_internal(tmp_path, monkeypatch):
    raw = "procesos no publicados de aseo"
    produced = proposal(raw, [("STATE", "is", [span(raw, "publicados")], "NEGATIVE"),
                              ("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE")],
                        subject="processes", noun="procesos")
    turn, _ = harness_run(tmp_path, monkeypatch, raw, produced)
    assert turn.result["foundation_internal_reason"] == "UNSUPPORTED_PROCESS_STATE_POLARITY"
    assert_public(turn.result["response"])


def test_a_source_timeout_never_reads_as_zero_or_as_a_code(tmp_path, monkeypatch):
    from tests.product_contract.frozen_source import InjectedFailure
    from tests.product_contract.harness import ProductHarness

    raw = "contratos de aseo"
    produced = proposal(raw, [("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE")])
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset={"jbjy-vk9h": []},
                             failures=[InjectedFailure(lambda: ValueError("boom"), operation="query")],
                             reference_instant=NOW, today=NOW.date(), foundation_producer=lambda _: produced,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    result = harness.say(raw).result
    assert result["foundation_terminal"] == "SOURCE_ERROR" and result["total_count"] is None
    assert "No afirmo que no existan resultados" in result["response"]
    assert "0 filas" not in result["response"]
    assert_public(result["response"])


def test_a_ready_answer_is_public_text_too(tmp_path, monkeypatch):
    raw = "contratos de aseo"
    produced = proposal(raw, [("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE")])
    turn, _ = harness_run(tmp_path, monkeypatch, raw, produced)
    assert turn.result["foundation_terminal"] == "READY" and turn.result["route_reason"] == "foundation_search"
    assert turn.result["foundation_internal_reason"] is None
    assert_public(turn.result["response"])
