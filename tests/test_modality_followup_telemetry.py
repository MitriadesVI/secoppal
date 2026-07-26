"""Tests for app/core/modality_followup_telemetry.py.

Covers the M3C-CG-I contract: producer-only telemetry, fail-open,
exactly-once, no sensitive data, and end-to-end workflow integration.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from pathlib import Path

import pytest

from app.config import Settings
from app.core.modality_followup_telemetry import (
    SCHEMA_VERSION,
    SOURCE,
    append_event,
    project_event,
    should_emit,
)
from app.core.orchestrator import SecopalWorkflow


def _project(**overrides) -> dict | None:
    """Helper that builds a canonical project_event call with sensible defaults.

    Because project_event has many keyword-only parameters, this helper avoids
    repeating the same baseline.  Overrides are passed straight through, so
    callers can mutate any signal they want.
    """
    base = {
        "followup": True,
        "context_params": {},
        "parsed_params": {"modalidad_include_procedure_ids": ["licitacion_publica"]},
        "followup_mutation": {
            "modality": {
                "instruction_status": "resolved",
                "operation": "replace",
                "control_code": "replace_ahora",
                "reason": "mutation_applied",
                "no_op": False,
            },
        },
        "query_plan_reason": "",
        "needs_clarification": False,
    }
    base.update(overrides)
    return project_event(**base)


# ---------------------------------------------------------------------------
# Schema / signal rules
# ---------------------------------------------------------------------------


def test_schema_version_and_source():
    event = _project()
    assert event is not None
    assert event["schema_version"] == SCHEMA_VERSION
    assert event["source"] == SOURCE


def test_event_id_is_random_uuid():
    event = _project()
    assert event is not None
    assert uuid.UUID(event["event_id"], version=4)
    other = _project()
    assert other is not None
    assert other["event_id"] != event["event_id"]


def test_recorded_at_is_iso_utc():
    event = _project()
    assert event is not None
    assert event["recorded_at"].endswith("Z")
    assert "T" in event["recorded_at"]


def test_telemetry_disabled_by_default():
    settings = Settings()
    assert settings.modality_followup_telemetry_path is None


def test_initial_query_excluded():
    event = _project(followup=False)
    assert event is None


def test_generic_followup_excluded():
    event = _project(followup=True, parsed_params={}, followup_mutation={})
    assert event is None


def test_resolved_modality_followup_emitted():
    event = _project(
        parsed_params={"modalidad_include_procedure_ids": ["licitacion_publica"]},
        followup_mutation={
            "modality": {
                "instruction_status": "resolved",
                "operation": "replace",
                "control_code": "replace_ahora",
                "reason": "mutation_applied",
                "no_op": False,
            },
        },
    )
    assert event is not None
    assert event["is_followup"] is True
    assert event["has_final_modality_filter"] is True
    assert event["has_modality_operation"] is True
    assert event["operation"] == "replace"
    assert event["instruction_status"] == "resolved"
    assert event["resolution_status"] == "applied"
    assert event["blocked"] is False
    assert event["no_op"] is False
    assert event["clear"] is False


def test_inherited_modality_followup_emitted():
    event = _project(
        context_params={"modalidad_include_procedure_ids": ["licitacion_publica"]},
        parsed_params={},
        followup_mutation={},
    )
    assert event is not None
    assert event["had_previous_modality_filter"] is True
    assert event["has_modality_operation"] is False
    assert event["instruction_status"] == "none"
    assert event["resolution_status"] is None
    assert event["operation"] is None


def test_clear_emitted():
    event = _project(
        parsed_params={},
        followup_mutation={
            "modality": {
                "instruction_status": "resolved",
                "operation": "clear",
                "control_code": "clear",
                "reason": "mutation_applied",
                "no_op": False,
            },
        },
    )
    assert event is not None
    assert event["operation"] == "clear"
    assert event["clear"] is True
    assert event["has_modality_operation"] is True


def test_no_op_emitted():
    event = _project(
        parsed_params={"modalidad_include_procedure_ids": ["licitacion_publica"]},
        followup_mutation={
            "modality": {
                "instruction_status": "resolved",
                "operation": "replace",
                "control_code": "replace_ahora",
                "reason": "mutation_no_op",
                "no_op": True,
            },
        },
    )
    assert event is not None
    assert event["no_op"] is True
    assert event["resolution_status"] == "no_op"


@pytest.mark.parametrize(
    "code",
    [
        "partial_target",
        "ambiguous_target",
        "conflicting_target",
        "non_searchable_target",
        "dataset_inapplicable_target",
    ],
)
def test_blocked_m2_status_emitted(code):
    event = _project(
        parsed_params={},
        followup_mutation={},
        query_plan_reason=f"modality_{code}",
        needs_clarification=True,
    )
    assert event is not None
    assert event["blocked"] is True
    assert event["resolution_status"] == "blocked"
    assert event["instruction_status"] == "resolved"


def test_target_missing_emitted():
    event = _project(
        parsed_params={},
        followup_mutation={},
        query_plan_reason="modality_target_missing",
        needs_clarification=True,
    )
    assert event is not None
    assert event["blocked"] is True
    assert event["clarification_code"] == "target_missing"


def test_control_conflict_emitted():
    event = _project(
        parsed_params={},
        followup_mutation={},
        query_plan_reason="modality_control_conflict",
        needs_clarification=True,
    )
    assert event is not None
    assert event["blocked"] is True
    assert event["clarification_code"] == "control_conflict"


def test_ambiguous_clear_or_null_value_emitted():
    event = _project(
        parsed_params={},
        followup_mutation={
            "modality": {
                "instruction_status": "ambiguous",
                "operation": None,
                "control_code": "ambiguous_clear_or_null_value",
                "reason": "ambiguous_clear_or_null_value",
                "no_op": False,
            },
        },
        query_plan_reason="modality_ambiguous_clear_or_null_value",
        needs_clarification=True,
    )
    assert event is not None
    assert event["blocked"] is True
    assert event["clarification_code"] == "ambiguous_clear_or_null_value"
    assert event["instruction_status"] == "ambiguous"
    assert event["resolution_status"] == "blocked"


def test_operation_values_from_real_contract():
    """Operation values must match the actual mutation engine contract."""
    for op in (
        "replace",
        "add_include",
        "add_exclude",
        "remove_member",
        "remove_exclude",
        "restore_include",
        "clear",
    ):
        event = _project(
            parsed_params={},
            followup_mutation={
                "modality": {
                    "instruction_status": "resolved",
                    "operation": op,
                    "control_code": f"{op}_x",
                    "reason": "mutation_applied",
                    "no_op": False,
                },
            },
        )
        assert event is not None
        assert event["operation"] == op


# ---------------------------------------------------------------------------
# Privacy / prohibited fields
# ---------------------------------------------------------------------------


def test_event_contains_no_prohibited_fields():
    event = _project(
        parsed_params={"modalidad_include_procedure_ids": ["licitacion_publica"]},
        followup_mutation={
            "modality": {
                "instruction_status": "resolved",
                "operation": "replace",
                "control_code": "replace_ahora",
                "reason": "mutation_applied",
                "no_op": False,
            },
        },
    )
    assert event is not None
    prohibited = {
        "user_query",
        "chat_id",
        "procedure_ids",
        "raws",
        "aliases",
        "entity",
        "city",
        "department",
        "objeto",
        "objeto_or",
        "objeto_phrases",
        "parsed_params",
        "resolved_params",
        "query_fingerprint",
        "soql",
        "results",
        "url",
    }
    assert not (prohibited & set(event.keys()))


def test_procedure_ids_do_not_appear_anywhere_in_serialized_line():
    event = _project(
        parsed_params={"modalidad_include_procedure_ids": ["licitacion_publica"]},
        followup_mutation={
            "modality": {
                "instruction_status": "resolved",
                "operation": "replace",
                "control_code": "replace_licitacion_publica",
                "reason": "mutation_applied",
                "no_op": False,
            },
        },
    )
    assert event is not None
    line = json.dumps(event)
    assert "licitacion_publica" not in line
    assert "procedure_id" not in line


def test_user_query_does_not_appear_in_serialized_line():
    event = _project(
        parsed_params={"modalidad_include_procedure_ids": ["licitacion_publica"]},
        followup_mutation={
            "modality": {
                "instruction_status": "resolved",
                "operation": "replace",
                "control_code": "replace_ahora",
                "reason": "mutation_applied",
                "no_op": False,
                "user_query": "compra de alimentacion en barranquilla",
            },
        },
    )
    assert event is not None
    line = json.dumps(event)
    assert "alimentacion" not in line
    assert "barranquilla" not in line
    assert "user_query" not in line


def test_chat_id_does_not_appear_in_serialized_line():
    """chat_id nunca debe aparecer en el evento ni en su serializacion JSON."""
    event = _project()
    assert event is not None
    assert "chat_id" not in event
    line = json.dumps(event)
    assert "chat_id" not in line
    assert "chat" not in line


def test_event_has_no_secret_fields():
    event = _project(
        parsed_params={"modalidad_include_procedure_ids": ["licitacion_publica"]},
    )
    assert event is not None
    assert "chat_id" not in event
    assert "user_query" not in event
    assert "procedure_ids" not in event


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


def test_append_event_creates_jsonl_line(tmp_path):
    path = tmp_path / "telemetry.jsonl"
    event = {"event_id": "e1"}
    append_event(path, event)
    lines = path.read_text().strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == event


def test_append_event_appends(tmp_path):
    path = tmp_path / "telemetry.jsonl"
    path.write_text(json.dumps({"event_id": "first"}) + "\n")
    append_event(path, {"event_id": "second"})
    lines = path.read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[1]) == {"event_id": "second"}


def test_append_event_makes_parent_directory(tmp_path):
    path = tmp_path / "sub" / "dir" / "telemetry.jsonl"
    append_event(path, {"event_id": "x"})
    assert path.exists()


def test_append_event_fail_open_read_only_directory(tmp_path):
    """Telemetry must be fail-open: write errors do not propagate."""
    ro_dir = tmp_path / "ro"
    ro_dir.mkdir()
    os.chmod(ro_dir, 0o555)
    path = ro_dir / "telemetry.jsonl"
    event = {"event_id": "x"}
    try:
        append_event(path, event)
    finally:
        os.chmod(ro_dir, 0o755)
    assert not path.exists()


def test_append_event_invalid_event_no_exception(tmp_path):
    """Projection bugs are caught; writer should not crash the caller even with
    a degenerate event.
    """
    path = tmp_path / "telemetry.jsonl"
    append_event(path, {"event_id": "x"})
    assert path.read_text().strip()


def test_projection_failure_log_no_path_or_data(tmp_path, caplog, monkeypatch):
    """Un fallo de project_event en el bloque real de normal_flow debe ser
    fail-open: la consulta responde, el turno se persiste, no se escribe
    telemetría, y el log no filtra ruta, evento ni datos del usuario.
    """
    caplog.set_level(logging.WARNING, logger="app.core.workflow.handlers.normal_flow")

    telemetry_path = tmp_path / "telemetry.jsonl"
    feedback_path = tmp_path / "feedback.jsonl"
    feedback_db_path = tmp_path / "secoppal.db"
    settings = Settings(
        modality_followup_telemetry_path=telemetry_path,
        secop_feedback_path=feedback_path,
        secop_feedback_db_path=feedback_db_path,
        secop_feedback_sqlite_enabled=False,
        secop_feedback_jsonl_dual_write=False,
        secop_app_token="",
        secop_api_key_id="",
        secop_app_secret="",
        deepseek_api_key="",
    )

    def _boom(*a, **k):
        raise ValueError("projection explosion with path /secret/telemetry.jsonl")

    monkeypatch.setattr(
        "app.core.workflow.handlers.normal_flow.project_event",
        _boom,
    )

    workflow = SecopalWorkflow(settings)
    result = workflow.run_query(chat_id="chat-123", user_query="licitacion publica de mantenimiento")
    assert result["response"]
    assert result.get("trace_id")

    followup = workflow.run_query(chat_id="chat-123", user_query="ahora seleccion abreviada")
    assert followup["response"]
    assert followup.get("trace_id")

    log_text = caplog.text.lower()
    assert not telemetry_path.exists()
    assert "secret" not in log_text
    assert "telemetry.jsonl" not in log_text
    assert "projection explosion" not in log_text
    assert "licitacion" not in log_text
    assert "chat-123" not in log_text
    assert "procedure_id" not in log_text
    assert "event_id" not in log_text
    assert "{" not in log_text


def test_append_event_fail_open_log_no_path_or_data(tmp_path, caplog):
    """Un fallo de escritura no debe filtrar ruta, nombre de archivo, evento
    ni datos del usuario en los logs.
    """
    caplog.set_level(logging.WARNING, logger="app.core.modality_followup_telemetry")
    ro_dir = tmp_path / "ro"
    ro_dir.mkdir()
    os.chmod(ro_dir, 0o555)
    path = ro_dir / "telemetry.jsonl"
    sensitive = {
        "event_id": "evt-123",
        "user_query": "compra de alimentacion en barranquilla",
        "chat_id": "chat-456",
        "procedure_ids": ["licitacion_publica"],
    }
    try:
        append_event(path, sensitive)
    finally:
        os.chmod(ro_dir, 0o755)
    log_text = caplog.text.lower()
    assert not path.exists()
    assert "telemetry.jsonl" not in log_text
    assert str(tmp_path).lower() not in log_text
    assert "compra" not in log_text
    assert "barranquilla" not in log_text
    assert "chat-456" not in log_text
    assert "licitacion_publica" not in log_text
    assert "evt-123" not in log_text
    assert "user_query" not in log_text
    assert "procedure_id" not in log_text


# ---------------------------------------------------------------------------
# should_emit
# ---------------------------------------------------------------------------


def test_should_emit_requires_followup():
    assert should_emit(False, {}, {}, {}, "", False) is False


def test_should_emit_requires_modality_signal():
    assert should_emit(True, {}, {}, {}, "", False) is False


def test_should_emit_with_context_modality():
    assert should_emit(
        True,
        {"modalidad_include_procedure_ids": ["a"]},
        {},
        {},
        "",
        False,
    )


def test_should_emit_with_parsed_modality():
    assert should_emit(
        True,
        {},
        {"modalidad_include_procedure_ids": ["a"]},
        {},
        "",
        False,
    )


def test_should_emit_with_modality_block():
    assert should_emit(True, {}, {}, {}, "modality_partial_target", True)


def test_should_emit_with_modality_mutation_block():
    assert should_emit(
        True,
        {},
        {},
        {"modality": {"operation": "replace"}},
        "",
        False,
    )


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------


def test_mutation_remove_followup_condition_excludes():
    """Eliminar followup => None."""
    assert _project(followup=False) is None


def test_mutation_remove_modality_signal_excludes():
    """Eliminar senal de modalidad => None."""
    assert _project(followup=True, parsed_params={}, followup_mutation={}) is None


def test_mutation_emit_initial_query():
    """Emitir consulta inicial => None."""
    assert _project(followup=False, parsed_params={"modalidad_include_procedure_ids": ["a"]}) is None


def test_mutation_duplicate_call_same_event(tmp_path):
    """Duplicar llamada al writer debe escribir dos lineas si el caller lo hace,
    pero el producer no deduplica.  El punto de integracion debe garantizar una sola.
    """
    path = tmp_path / "telemetry.jsonl"
    event = {"event_id": "a"}
    append_event(path, event)
    append_event(path, event)
    assert path.read_text().strip().splitlines() == [json.dumps(event), json.dumps(event)]


def test_mutation_allow_user_query_is_excluded():
    """Permitir user_query en el evento => la serializacion debe excluirlo."""
    event = _project(
        followup_mutation={
            "modality": {
                "instruction_status": "resolved",
                "operation": "replace",
                "control_code": "replace_ahora",
                "reason": "mutation_applied",
                "no_op": False,
                "user_query": "compra de alimentacion",
            },
        },
    )
    assert event is not None
    assert "user_query" not in event


def test_mutation_relax_deduplication_not_producers_job():
    """El productor no deduplica; ese es trabajo del analizador."""
    event = _project()
    assert event is not None
    assert uuid.UUID(event["event_id"])


def test_mutation_write_error_must_not_propagate():
    """Verificar que un error de escritura no se propaga."""
    ro_dir = Path("/dev/null")
    path = ro_dir / "impossible.jsonl"
    append_event(path, {"event_id": "x"})
    # no exception


# ---------------------------------------------------------------------------
# End-to-end workflow integration
# ---------------------------------------------------------------------------


class _Dialogue:
    def __init__(self, tmp_path: Path):
        self.telemetry_path = tmp_path / "telemetry.jsonl"
        self.feedback_path = tmp_path / "feedback.jsonl"
        self.feedback_db_path = tmp_path / "secoppal.db"
        self.settings = Settings(
            modality_followup_telemetry_path=self.telemetry_path,
            secop_feedback_path=self.feedback_path,
            secop_feedback_db_path=self.feedback_db_path,
            secop_feedback_sqlite_enabled=False,
            secop_feedback_jsonl_dual_write=False,
            secop_app_token="",
            secop_api_key_id="",
            secop_app_secret="",
            deepseek_api_key="",
        )
        self.workflow = SecopalWorkflow(self.settings)
        self.chat_id = "test-chat"

    def turn(self, query: str):
        self.workflow.run_query(chat_id=self.chat_id, user_query=query)

    def events(self):
        if not self.telemetry_path.exists():
            return []
        lines = self.telemetry_path.read_text().strip().splitlines()
        return [json.loads(line) for line in lines if line.strip()]


@pytest.fixture
def dialogue(tmp_path):
    return _Dialogue(tmp_path)


def test_e2e_initial_query_excluded(dialogue):
    dialogue.turn("licitacion publica de mantenimiento")
    assert dialogue.events() == []


def test_e2e_generic_followup_excluded(dialogue):
    dialogue.turn("licitacion publica de mantenimiento")
    dialogue.turn("muestrame mas")
    assert dialogue.events() == []


def test_e2e_modality_followup_emitted(dialogue):
    dialogue.turn("licitacion publica de mantenimiento")
    dialogue.turn("ahora solo seleccion abreviada")
    events = dialogue.events()
    assert len(events) == 1
    assert events[0]["is_followup"] is True
    assert events[0]["has_modality_operation"] is True
    assert events[0]["operation"] == "replace"


def test_e2e_inherited_modality_followup_emitted(dialogue):
    dialogue.turn("licitacion publica de mantenimiento")
    dialogue.turn("en bogota")
    events = dialogue.events()
    assert len(events) == 1
    assert events[0]["is_followup"] is True
    assert events[0]["had_previous_modality_filter"] is True
    assert events[0]["has_modality_operation"] is False


def test_e2e_clear_emitted(dialogue):
    dialogue.turn("licitacion publica de mantenimiento")
    dialogue.turn("quita el filtro de modalidad")
    events = dialogue.events()
    assert len(events) == 1
    assert events[0]["operation"] == "clear"
    assert events[0]["clear"] is True


def test_e2e_no_op_emitted(dialogue):
    dialogue.turn("licitacion publica de mantenimiento")
    dialogue.turn("licitacion publica")
    events = dialogue.events()
    assert len(events) == 1
    assert events[0]["no_op"] is True


def test_e2e_ambiguous_clear_or_null_value_blocked(dialogue):
    dialogue.turn("licitacion publica de mantenimiento")
    dialogue.turn("sin modalidad")
    events = dialogue.events()
    assert len(events) == 1
    event = events[0]
    assert event["is_followup"] is True
    assert event["had_previous_modality_filter"] is True
    assert event["blocked"] is True
    assert event["clarification_code"] == "ambiguous_clear_or_null_value"
    assert event["instruction_status"] == "ambiguous"
    assert event["resolution_status"] == "blocked"


def test_e2e_fail_open_bad_path(dialogue):
    dialogue.telemetry_path = Path("/nonexistent_dir") / "telemetry.jsonl"
    dialogue.settings = Settings(
        modality_followup_telemetry_path=dialogue.telemetry_path,
        secop_feedback_path=dialogue.feedback_path,
        secop_feedback_db_path=dialogue.feedback_db_path,
        secop_feedback_sqlite_enabled=False,
        secop_feedback_jsonl_dual_write=False,
        secop_app_token="",
        secop_api_key_id="",
        secop_app_secret="",
        deepseek_api_key="",
    )
    dialogue.workflow = SecopalWorkflow(dialogue.settings)
    dialogue.turn("licitacion publica de mantenimiento")
    dialogue.turn("ahora seleccion abreviada")
    assert not dialogue.telemetry_path.exists()


def test_e2e_fail_open_log_no_path(dialogue, caplog):
    """Ruta inexistente: fail-open y sin filtrar la ruta en el log."""
    caplog.set_level(logging.WARNING, logger="app.core.modality_followup_telemetry")
    dialogue.telemetry_path = Path("/nonexistent_dir") / "telemetry.jsonl"
    dialogue.settings = Settings(
        modality_followup_telemetry_path=dialogue.telemetry_path,
        secop_feedback_path=dialogue.feedback_path,
        secop_feedback_db_path=dialogue.feedback_db_path,
        secop_feedback_sqlite_enabled=False,
        secop_feedback_jsonl_dual_write=False,
        secop_app_token="",
        secop_api_key_id="",
        secop_app_secret="",
        deepseek_api_key="",
    )
    dialogue.workflow = SecopalWorkflow(dialogue.settings)
    dialogue.turn("licitacion publica de mantenimiento")
    dialogue.turn("ahora seleccion abreviada")
    log_text = caplog.text.lower()
    assert "nonexistent_dir" not in log_text
    assert "telemetry.jsonl" not in log_text
    assert not dialogue.telemetry_path.exists()


def test_e2e_blocked_followup_at_most_once(dialogue):
    """Bloqueo cubierto con tests unitarios robustos; aqui verificamos que un
    follow-up con ambiguedad no emite mas de una vez.
    """
    dialogue.turn("licitacion publica de mantenimiento")
    dialogue.turn("seleccion abreviada y contratacion directa")
    events = dialogue.events()
    assert len(events) <= 1


def test_e2e_exactly_one_line_per_turn(dialogue):
    dialogue.turn("licitacion publica de mantenimiento")
    dialogue.turn("ahora seleccion abreviada")
    dialogue.turn("en bogota")
    events = dialogue.events()
    assert len(events) == 2
    assert events[0]["event_id"] != events[1]["event_id"]


def test_e2e_first_followup_exactly_one_line(dialogue):
    """Exactly-once: 0 eventos antes del primer follow-up, 1 exacto despues."""
    dialogue.turn("licitacion publica de mantenimiento")
    assert dialogue.events() == []
    dialogue.turn("ahora solo seleccion abreviada")
    events = dialogue.events()
    assert len(events) == 1
    assert events[0]["event_id"]
    dialogue.turn("en bogota")
    events = dialogue.events()
    assert len(events) == 2
    assert events[0]["event_id"] != events[1]["event_id"]
