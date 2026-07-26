"""Tests focales del analizador de la compuerta de producto M3C-C -> M3C-D.

Cubre la bateria SS15 del contrato M3C-CG-I: esquema V1, exclusiones (consulta
inicial, follow-up sin modalidad, duplicados, malformados, muestra sintetica),
conteo separado de ``target_missing`` conversacional vs estados juridicos M2,
procedencia explicita y la aritmetica exacta de la compuerta.

El analizador es read-only: estos tests no escriben en ``data/``.
"""
from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scripts.analyze_modality_followup_gate import (  # noqa: E402
    CLARIFICATION_CANDIDATE_CODES,
    KNOWN_CODES,
    aggregate,
    render_markdown,
)


def _record(event_id: str, **overrides) -> str:
    """Construye un evento V1 valido. ``event_id`` se normaliza a UUID determinista."""
    try:
        uuid.UUID(event_id)
        stable_id = event_id
    except ValueError:
        stable_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, event_id))
    base = {
        "schema_version": 1,
        "event_id": stable_id,
        "recorded_at": "2026-07-25T12:00:00+00:00",
        "source": "runtime",
        "is_followup": True,
        "had_previous_modality_filter": False,
        "has_final_modality_filter": True,
        "has_modality_operation": True,
        "operation": "replace",
        "instruction_status": "resolved",
        "resolution_status": "applied",
        "clarification_code": None,
        "blocked": False,
        "no_op": False,
        "clear": False,
        "implicit_replace": False,
    }
    base.update(overrides)
    return json.dumps(base, ensure_ascii=False)


def _blocked(event_id: str, code: str = "target_missing", **overrides) -> str:
    return _record(
        event_id,
        has_final_modality_filter=False,
        operation="add_include",
        resolution_status="blocked",
        blocked=True,
        clarification_code=code,
        **overrides,
    )


def _sample(n_total: int, n_blocked: int, *, source: str = "runtime") -> list[str]:
    """``n_total`` turnos con senal de modalidad, de los cuales ``n_blocked`` bloqueados."""
    lines = [_blocked(f"b{i}", "ambiguous_target", source=source) for i in range(n_blocked)]
    lines += [_record(f"a{i}", source=source) for i in range(n_total - n_blocked)]
    return lines


# -- Exclusiones y conteo basico --------------------------------------------------

def test_zero_records_is_insufficient_sample():
    report = aggregate([], sample_provenance="real")
    assert report["modality_followup_turns_total"] == 0
    assert report["modality_followup_blocked_total"] == 0
    assert report["blocked_rate"] == 0.0
    assert report["sample_is_real"] is True
    assert report["sample_provenance"] == "real"
    assert report["gate_minimum_sample_met"] is False
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"


def test_malformed_record_is_excluded_and_counted():
    report = aggregate(["{esto no es json", "", _record("ok1")], sample_provenance="real")
    assert report["invalid_records_excluded"] == 1
    assert report["modality_followup_turns_total"] == 1


def test_structurally_invalid_record_is_excluded():
    sin_id = json.loads(_record("x"))
    sin_id["event_id"] = ""
    report = aggregate([json.dumps(sin_id), _record("y", schema_version=2)], sample_provenance="real")
    assert report["invalid_records_excluded"] == 2
    assert report["modality_followup_turns_total"] == 0


def test_invalid_event_id_is_excluded():
    bad = json.dumps({
        "schema_version": 1,
        "event_id": "not-a-uuid",
        "recorded_at": "2026-07-25T12:00:00+00:00",
        "source": "runtime",
        "is_followup": True,
        "had_previous_modality_filter": False,
        "has_final_modality_filter": True,
        "has_modality_operation": True,
        "operation": "replace",
        "instruction_status": "resolved",
        "resolution_status": "applied",
        "clarification_code": None,
        "blocked": False,
        "no_op": False,
        "clear": False,
        "implicit_replace": False,
    })
    report = aggregate([bad], sample_provenance="real")
    assert report["invalid_records_excluded"] == 1
    assert report["modality_followup_turns_total"] == 0


def test_arbitrary_recorded_at_is_excluded():
    bad = json.dumps({
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "recorded_at": "texto-del-usuario-/ruta/privada",
        "source": "runtime",
        "is_followup": True,
        "had_previous_modality_filter": False,
        "has_final_modality_filter": True,
        "has_modality_operation": True,
        "operation": "replace",
        "instruction_status": "resolved",
        "resolution_status": "applied",
        "clarification_code": None,
        "blocked": False,
        "no_op": False,
        "clear": False,
        "implicit_replace": False,
    })
    report = aggregate([bad], sample_provenance="real")
    assert report["invalid_records_excluded"] == 1
    assert report["modality_followup_turns_total"] == 0


def test_non_utc_recorded_at_is_excluded():
    bad = json.dumps({
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "recorded_at": "2026-07-25T12:00:00+05:00",
        "source": "runtime",
        "is_followup": True,
        "had_previous_modality_filter": False,
        "has_final_modality_filter": True,
        "has_modality_operation": True,
        "operation": "replace",
        "instruction_status": "resolved",
        "resolution_status": "applied",
        "clarification_code": None,
        "blocked": False,
        "no_op": False,
        "clear": False,
        "implicit_replace": False,
    })
    report = aggregate([bad], sample_provenance="real")
    assert report["invalid_records_excluded"] == 1
    assert report["modality_followup_turns_total"] == 0


def test_naive_recorded_at_is_excluded():
    bad = json.dumps({
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "recorded_at": "2026-07-25T12:00:00",
        "source": "runtime",
        "is_followup": True,
        "had_previous_modality_filter": False,
        "has_final_modality_filter": True,
        "has_modality_operation": True,
        "operation": "replace",
        "instruction_status": "resolved",
        "resolution_status": "applied",
        "clarification_code": None,
        "blocked": False,
        "no_op": False,
        "clear": False,
        "implicit_replace": False,
    })
    report = aggregate([bad], sample_provenance="real")
    assert report["invalid_records_excluded"] == 1
    assert report["modality_followup_turns_total"] == 0


def test_recorded_at_is_canonicalized_to_utc():
    ok = json.dumps({
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "recorded_at": "2026-07-25T12:00:00+00:00",
        "source": "runtime",
        "is_followup": True,
        "had_previous_modality_filter": False,
        "has_final_modality_filter": True,
        "has_modality_operation": True,
        "operation": "replace",
        "instruction_status": "resolved",
        "resolution_status": "applied",
        "clarification_code": None,
        "blocked": False,
        "no_op": False,
        "clear": False,
        "implicit_replace": False,
    })
    report = aggregate([ok], sample_provenance="real")
    assert report["invalid_records_excluded"] == 0
    assert report["sample_start"] == "2026-07-25T12:00:00.000000Z"
    assert report["sample_end"] == "2026-07-25T12:00:00.000000Z"


def test_sample_window_orders_mixed_fractional_precision():
    lines = [
        _record("earlier", recorded_at="2026-07-25T12:00:00Z"),
        _record("later", recorded_at="2026-07-25T12:00:00.100000Z"),
    ]
    report = aggregate(lines, sample_provenance="real")
    assert report["sample_start"] == "2026-07-25T12:00:00.000000Z"
    assert report["sample_end"] == "2026-07-25T12:00:00.100000Z"


def test_synthetic_provenance_is_preserved():
    report = aggregate(_sample(100, 10), sample_provenance="synthetic")
    assert report["sample_is_real"] is False
    assert report["sample_provenance"] == "synthetic"
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"


def test_initial_query_is_excluded():
    report = aggregate([_record("i1", is_followup=False)], sample_provenance="real")
    assert report["modality_followup_turns_total"] == 0
    assert report["non_followup_records_excluded"] == 1


def test_followup_without_modality_signal_is_excluded():
    report = aggregate([_record(
        "n1",
        has_modality_operation=False,
        has_final_modality_filter=False,
        had_previous_modality_filter=False,
        blocked=False,
        operation=None,
        resolution_status=None,
    )], sample_provenance="real")
    assert report["modality_followup_turns_total"] == 0
    assert report["no_modality_signal_records_excluded"] == 1


def test_inherited_modality_followup_is_counted():
    report = aggregate([_record(
        "h1",
        has_modality_operation=False,
        has_final_modality_filter=False,
        had_previous_modality_filter=True,
        operation=None,
        resolution_status=None,
        instruction_status="none",
    )], sample_provenance="real")
    assert report["modality_followup_turns_total"] == 1
    assert report["modality_followup_blocked_total"] == 0
    assert report["sample_is_real"] is True


def test_resolved_modality_followup_is_counted():
    report = aggregate([_record("r1")], sample_provenance="real")
    assert report["modality_followup_turns_total"] == 1
    assert report["modality_followup_blocked_total"] == 0
    assert report["sample_is_real"] is True


def test_blocked_counted_exactly_once():
    report = aggregate([_blocked("b1", "partial_target")], sample_provenance="real")
    assert report["modality_followup_turns_total"] == 1
    assert report["modality_followup_blocked_total"] == 1
    assert report["blocked_by_status"]["PARTIAL"] == 1
    assert sum(report["blocked_by_status"].values()) == 1
    assert sum(report["blocked_by_code"].values()) == 1


def test_control_conflict_is_reported_separately():
    report = aggregate([_blocked("c1", "control_conflict")], sample_provenance="real")
    assert report["control_conflict_total"] == 1
    assert report["target_missing_total"] == 0
    assert sum(report["blocked_by_status"].values()) == 0
    assert report["modality_followup_blocked_total"] == 1


def test_target_missing_is_not_mixed_with_m2_statuses():
    report = aggregate(
        [_blocked("t1", "target_missing"), _blocked("t2", "conflicting_target")],
        sample_provenance="real",
    )
    assert report["target_missing_total"] == 1
    assert report["control_conflict_total"] == 0
    assert report["blocked_by_status"]["CONFLICT"] == 1
    assert sum(report["blocked_by_status"].values()) == 1
    assert report["modality_followup_blocked_total"] == 2


def test_each_m2_status_has_its_own_bucket():
    codes = {
        "partial_target": "PARTIAL",
        "ambiguous_target": "AMBIGUOUS",
        "conflicting_target": "CONFLICT",
        "non_searchable_target": "NOT_SEARCHABLE",
        "dataset_inapplicable_target": "DATASET_INAPPLICABLE",
    }
    report = aggregate([_blocked(f"s{i}", code) for i, code in enumerate(codes)], sample_provenance="real")
    assert report["blocked_by_status"] == {status: 1 for status in codes.values()}


def test_noop_is_counted():
    report = aggregate([_record("o1", operation="remove_member", resolution_status="no_op", no_op=True)], sample_provenance="real")
    assert report["noop_total"] == 1
    assert report["modality_followup_turns_total"] == 1
    assert report["modality_followup_blocked_total"] == 0


def test_clear_is_counted():
    report = aggregate([
        _record("cl1", operation="clear", resolution_status="applied", clear=True),
        _record("cl2", operation="clear", resolution_status="no_op", no_op=True, clear=True),
    ], sample_provenance="real")
    assert report["clear_total"] == 2
    assert report["noop_total"] == 1


def test_duplicate_record_is_excluded():
    report = aggregate([_record("dup"), _record("dup"), _record("otro")], sample_provenance="real")
    assert report["duplicate_records_excluded"] == 1
    assert report["modality_followup_turns_total"] == 2


def test_duplicate_blocked_never_inflates_blocked_total():
    report = aggregate([_blocked("dupb", "ambiguous_target")] * 3, sample_provenance="real")
    assert report["modality_followup_blocked_total"] == 1
    assert report["duplicate_records_excluded"] == 2


def test_clarification_candidate_is_a_subset_of_blocked():
    report = aggregate([
        _blocked("k1", "target_missing"),
        _blocked("k2", "non_searchable_target"),
    ], sample_provenance="real")
    assert report["modality_followup_blocked_total"] == 2
    assert report["clarification_candidate_total"] == 1
    assert CLARIFICATION_CANDIDATE_CODES <= KNOWN_CODES


# -- Procedencia: lo sintetico jamas autoriza -------------------------------------

def test_synthetic_sample_never_authorizes_without_explicit_real():
    report = aggregate(_sample(200, 50), sample_provenance="unknown")
    assert report["modality_followup_turns_total"] == 200
    assert report["sample_is_real"] is False
    assert report["sample_provenance"] == "unknown"
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"


def test_synthetic_source_never_authorizes():
    report = aggregate(_sample(200, 50, source="corpus"), sample_provenance="real")
    assert report["modality_followup_turns_total"] == 0
    assert report["non_real_source_records_excluded"] == 200
    assert report["sample_is_real"] is True
    assert report["sample_provenance"] == "real"
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"


def test_explicit_real_sample_is_required_to_authorize():
    report = aggregate(_sample(100, 10), sample_provenance="real")
    assert report["sample_is_real"] is True
    assert report["sample_provenance"] == "real"
    assert report["m3c_d_recommendation"] == "AUTHORIZED"


def test_default_without_manifest_is_not_real():
    report = aggregate(_sample(100, 10))
    assert report["sample_is_real"] is False
    assert report["sample_provenance"] == "unknown"
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"


def test_source_runtime_does_not_imply_real():
    """El analizador no infiere sample_is_real solo porque source sea 'runtime'."""
    report = aggregate(_sample(100, 10, source="runtime"))
    assert report["modality_followup_turns_total"] == 100
    assert report["sample_is_real"] is False
    assert report["sample_provenance"] == "unknown"
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"


# -- Aritmetica exacta de la compuerta ---------------------------------------

def test_99_real_records_never_meet_minimum():
    report = aggregate(_sample(99, 99), sample_provenance="real")
    assert report["modality_followup_turns_total"] == 99
    assert report["gate_minimum_sample_met"] is False
    assert report["gate_threshold_met"] is True
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"


def test_100_real_with_4_blocked_is_no_go():
    report = aggregate(_sample(100, 4), sample_provenance="real")
    assert report["blocked_rate"] == 0.04
    assert report["gate_minimum_sample_met"] is True
    assert report["gate_threshold_met"] is False
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"


def test_100_real_with_5_blocked_is_go_by_rate():
    report = aggregate(_sample(100, 5), sample_provenance="real")
    assert report["blocked_rate"] == 0.05
    assert report["gate_threshold_met"] is True
    assert report["m3c_d_recommendation"] == "AUTHORIZED"


def test_100_real_with_10_blocked_is_go():
    report = aggregate(_sample(100, 10), sample_provenance="real")
    assert report["modality_followup_blocked_total"] == 10
    assert report["m3c_d_recommendation"] == "AUTHORIZED"


def test_200_real_with_9_blocked_is_no_go():
    report = aggregate(_sample(200, 9), sample_provenance="real")
    assert report["blocked_rate"] == 0.045
    assert report["gate_minimum_sample_met"] is True
    assert report["gate_threshold_met"] is False
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"


def test_rate_clause_still_requires_five_blocked():
    report = aggregate(_sample(80, 4), sample_provenance="real")
    assert report["blocked_rate"] == 0.05
    assert report["gate_threshold_met"] is False
    assert report["gate_minimum_sample_met"] is False
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"


def test_200_real_with_10_blocked_is_go_by_absolute():
    report = aggregate(_sample(200, 10), sample_provenance="real")
    assert report["blocked_rate"] == 0.05
    assert report["m3c_d_recommendation"] == "AUTHORIZED"


# -- Privacidad: la salida es codigos y conteos ----------------------------------

_SENSITIVE = (
    "licitacion_publica",
    "contratacion_directa",
    "alimentacion para el hospital de barranquilla",
    "st_email_ortizrodrigo39",
    "12345678",
)


def test_procedure_ids_and_user_text_never_reach_the_output():
    poisoned = {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "recorded_at": "2026-07-25T12:00:00+00:00",
        "source": "runtime",
        "is_followup": True,
        "had_previous_modality_filter": False,
        "has_final_modality_filter": False,
        "has_modality_operation": True,
        "operation": "add_include",
        "instruction_status": "resolved",
        "resolution_status": "blocked",
        "clarification_code": "target_missing",
        "blocked": True,
        "no_op": False,
        "clear": False,
        "implicit_replace": False,
        "user_query": "alimentacion para el hospital de barranquilla",
        "chat_id": "st_email_ortizrodrigo39",
        "procedure_ids": ["licitacion_publica", "contratacion_directa"],
        "nit": "12345678",
    }
    report = aggregate([json.dumps(poisoned)], sample_provenance="real")
    assert report["modality_followup_blocked_total"] == 1
    blob = json.dumps(report, ensure_ascii=False) + render_markdown(report)
    for needle in _SENSITIVE:
        assert needle not in blob


def test_unknown_clarification_code_is_bucketed_not_echoed():
    report = aggregate([_blocked("u1", "codigo_inventado_con_texto_de_usuario")], sample_provenance="real")
    assert report["modality_followup_blocked_total"] == 1
    assert report["blocked_by_code"] == {"unknown_code": 1}
    assert "codigo_inventado" not in json.dumps(report, ensure_ascii=False)


# -- Guardia de drift contra el motor real ---------------------------------------

def test_code_taxonomy_matches_the_engine():
    from app.core.modality_followup_wiring import KNOWN_CLARIFICATION_CODES

    assert KNOWN_CLARIFICATION_CODES <= KNOWN_CODES
    assert "ambiguous_control" in KNOWN_CODES


def test_markdown_render_is_deterministic_and_has_no_ids():
    report = aggregate(_sample(100, 10), sample_provenance="real")
    first = render_markdown(report)
    assert first == render_markdown(aggregate(_sample(100, 10), sample_provenance="real"))
    assert "NOT_AUTHORIZED" not in first
    assert "AUTHORIZED" in first


# -- CLI / manifest ----------------------------------------------------------------


def test_manifest_declares_real_provenance(tmp_path):
    from scripts.analyze_modality_followup_gate import _load_manifest

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"sample_provenance": "real", "sample_source": "prod"}))
    provenance, source = _load_manifest(manifest)
    assert provenance == "real"
    assert source == "prod"


def test_manifest_declares_synthetic_provenance(tmp_path):
    from scripts.analyze_modality_followup_gate import _load_manifest

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"sample_provenance": "synthetic", "sample_source": "test"}))
    provenance, source = _load_manifest(manifest)
    assert provenance == "synthetic"
    assert source == "test"


def test_manifest_default_is_unknown(tmp_path):
    from scripts.analyze_modality_followup_gate import _load_manifest

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"sample_source": "prod"}))
    provenance, source = _load_manifest(manifest)
    assert provenance == "unknown"
    assert source == "prod"


def test_main_cli_real_provenance(tmp_path, capsys):
    from scripts.analyze_modality_followup_gate import main

    log = tmp_path / "telemetry.jsonl"
    log.write_text(_record("r1", source="runtime"))
    main(["--input", str(log), "--sample-provenance", "real", "--format", "json"])
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert report["sample_is_real"] is True
    assert report["sample_provenance"] == "real"


def test_main_cli_unknown_provenance_does_not_authorize(tmp_path, capsys):
    from scripts.analyze_modality_followup_gate import main

    log = tmp_path / "telemetry.jsonl"
    log.write_text(
        "\n".join(
            [_record(f"r{i}", source="runtime") for i in range(100)]
            + [_blocked(f"b{i}", "ambiguous_target") for i in range(10)]
        )
    )
    main(["--input", str(log), "--format", "json"])
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert report["sample_is_real"] is False
    assert report["sample_provenance"] == "unknown"
    assert report["m3c_d_recommendation"] == "NOT_AUTHORIZED"
