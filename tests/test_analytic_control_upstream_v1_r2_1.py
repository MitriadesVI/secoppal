"""Frozen R2.1 sobre/entity oracle and parser integrity regressions."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from burr.core import State

from app.core.analytics.contract_activity import handle_contract_activity
from app.core.analytics.contract_activity.models import DecisionKind
from app.core.analytics.registry import maybe_handle_analytical_query
from app.core.entity_resolver import EntityResolver
from app.core.material_residual import (
    MaterialEvidence,
    MaterialOccurrence,
    material_evidence,
    material_residual,
)
from app.core.query_router import QueryRouter
from app.core.semantic_turn_evidence import SemanticTurnEvidence, raw_fingerprint
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions.execution import execute_query
from scripts.audits.analytic_control_upstream_v1_r2_1 import _unexplained_ids
from tests.test_contract_activity_narrow_corpus import CorpusClient
from tests.test_contract_activity_admission_v2 import RecordingContractSource


FIXTURE = json.loads(Path(
    "tests/fixtures/analytic_control_upstream_v1_r2_1/sobre_entity_oracle.json"
).read_text())
ROUTER = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _frozen_raws() -> list[str]:
    manifest = json.loads(Path(
        "tests/fixtures/analytic_control_upstream_v1_r2_1/blast_manifest.json"
    ).read_text())
    raws: list[str] = []
    for source in manifest["sources"]:
        path = Path(source["path"])
        if source["format"] == "jsonl":
            records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        else:
            records = json.loads(path.read_text())
            if source["format"] == "nested_json":
                records = records[source["section"]]
        raws.extend(
            record[source["raw_key"]] for record in records
            if isinstance(record.get(source["raw_key"]), str)
        )
    assert len(raws) == 1275
    return raws


@pytest.mark.parametrize("case", FIXTURE["rows"], ids=lambda case: case["id"])
def test_sobre_entity_historical_oracle(case: dict) -> None:
    parsed = ROUTER.parse(case["raw"], today=date(2026, 9, 22))
    assert parsed.params.get("entidad_resolved") == case["entity_resolved"]
    source = RecordingContractSource()
    decision = handle_contract_activity(
        case["raw"], parsed.params, source,
        semantic_evidence=parsed.semantic_evidence,
    )
    expected_kind = {
        "READY": DecisionKind.ANSWER,
        "HANDOFF": DecisionKind.HANDOFF,
        "CLARIFY": DecisionKind.CLARIFY,
    }[case["ca_status"]]
    assert decision.kind is expected_kind
    if decision.request is not None:
        assert decision.request.operation.value == case["ca_operation"]
    assert len(source.calls) == case["ca_calls"]


@pytest.mark.parametrize("case", FIXTURE["strong_content"], ids=lambda case: case["id"])
def test_strong_content_does_not_accredit_its_own_buyer(case: dict) -> None:
    parsed = ROUTER.parse(case["raw"], today=date(2026, 9, 22))
    if case["id"] in {"S01", "S02", "S03"}:
        assert parsed.params.get("entidad_resolved") is None
    else:
        assert parsed.params.get("entidad_resolved") == case["historical_entity_resolved"]
    source = RecordingContractSource()
    decision = handle_contract_activity(
        case["raw"], parsed.params, source,
        semantic_evidence=parsed.semantic_evidence,
    )
    assert decision.kind is not DecisionKind.ANSWER
    assert source.calls == []


def test_parser_never_throws_on_frozen_blast_and_new_family() -> None:
    for raw in _frozen_raws():
        parsed = ROUTER.parse(raw, today=date(2026, 9, 22))
        assert parsed.semantic_evidence is not None
        assert parsed.semantic_evidence.valid_for(raw)


def test_material_occurrences_imply_residual_over_frozen_corpus() -> None:
    for raw in _frozen_raws():
        residual = material_residual(raw)
        for occurrence in material_evidence(raw).occurrences:
            assert raw[occurrence.start:occurrence.end] == occurrence.surface
            assert occurrence.surface.lower() in residual, raw


def test_incoherent_material_keeps_receipt_and_fails_at_ca_material_gate(monkeypatch) -> None:
    import app.core.query_router as query_router

    real_from_parse = SemanticTurnEvidence.from_parse

    class IncoherentProducer:
        @staticmethod
        def from_parse(raw, controls, _residual, _material):
            return real_from_parse(
                raw, controls, (),
                MaterialEvidence("x", (MaterialOccurrence(0, 1, "x"),)),
            )

    monkeypatch.setattr(query_router, "SemanticTurnEvidence", IncoherentProducer)
    count_raw = "¿Cuántos contratos firmó la Alcaldía de Galapa?"
    count = ROUTER.parse(count_raw, today=date(2026, 9, 22))
    assert count.semantic_evidence is not None
    assert count.semantic_evidence.material_accredited is False
    assert count.semantic_evidence.analytic_controls
    assert SemanticTurnEvidence.from_state(count.semantic_evidence.to_state()) == count.semantic_evidence
    source = RecordingContractSource()
    decision = handle_contract_activity(
        count_raw, count.params, source, semantic_evidence=count.semantic_evidence,
    )
    assert decision.kind is DecisionKind.CLARIFY
    assert decision.reason == "UNSUPPORTED_MATERIAL_FILTER"
    assert source.calls == []

    search_raw = "contratos de la Alcaldía de Galapa"
    search = ROUTER.parse(search_raw, today=date(2026, 9, 22))
    handed = handle_contract_activity(
        search_raw, search.params, source, semantic_evidence=search.semantic_evidence,
    )
    assert handed.kind is DecisionKind.HANDOFF

    process_raw = "¿Cuántos procesos de la Alcaldía de Galapa?"
    process = ROUTER.parse(process_raw, today=date(2026, 9, 22))
    state = State({
        "user_query": process_raw, "resolved_params": process.params,
        "dataset_id": "jbjy-vk9h", "soql_query": "", "today": date(2026, 9, 22),
        "semantic_evidence": process.semantic_evidence.to_state(), "timings_ms": {},
    })
    result = execute_query(state, CorpusClient(), SoQLBuilder())
    assert "No tengo un conteo certificado" in result.get("analytic_limitation")


def test_receipt_extra_key_and_surrogate_fingerprint_are_strict() -> None:
    raw = "¿Cuántos contratos firmó la Alcaldía de Galapa?"
    receipt = ROUTER.parse(raw, today=date(2026, 9, 22)).semantic_evidence
    assert receipt is not None
    state = receipt.to_state()
    state["unexpected"] = True
    assert SemanticTurnEvidence.from_state(state) is None
    assert raw_fingerprint("x\ud800") != raw_fingerprint("x\udc00")
    assert raw_fingerprint("x\ud800") == raw_fingerprint("x\ud800")


def test_blast_new_error_cannot_be_whitelisted_by_expected_reason() -> None:
    changed = [{
        "id": "sobre_entity/T01B01", "expected_reason": "known semantic repair",
        "before": {"error": "ValueError:old"},
        "after": {"error": "RuntimeError:new"},
    }]
    assert _unexplained_ids(changed) == (
        ["sobre_entity/T01B01"], ["sobre_entity/T01B01"],
    )


def test_registry_requires_receipt_before_direct_ca_claim() -> None:
    raw = "¿Cuántos contratos firmó la Alcaldía de Galapa?"
    parsed = ROUTER.parse(raw, today=date(2026, 9, 22))
    source = RecordingContractSource()
    assert maybe_handle_analytical_query(
        raw, parsed.params, "jbjy-vk9h", source, SoQLBuilder(),
        semantic_evidence=None,
    ) is None
    assert source.calls == []
