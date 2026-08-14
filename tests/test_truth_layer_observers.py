"""Product-foundation tests for the two pure, dormant observer seams."""
from __future__ import annotations

import ast
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from app.core.truth_layer.observers import (
    observe_contract_truth_axes,
    observe_process_truth_axes,
)

REFERENCE = datetime(2026, 8, 3, 12, tzinfo=ZoneInfo("America/Bogota"))


def _process(**overrides):
    row = {
        "id_del_proceso": "P-1",
        "estado_del_procedimiento": "Publicado",
        "id_estado_del_procedimiento": "published",
        "estado_de_apertura_del_proceso": "Abierto",
        "fase": "Presentación de oferta",
        "estado_resumen": "Presentación de ofertas",
        "adjudicado": "No",
        "fecha_adjudicacion": None,
        "id_adjudicacion": None,
        "id_del_portafolio": None,
        "modalidad_de_contratacion": "Licitación pública",
        "fecha_de_recepcion_de": "2026-08-20",
        "urlproceso": (
            "https://community.secop.gov.co/Public/Tendering/NoticeDetail/"
            "Index?noticeUID=CO1.NTC.1"
        ),
    }
    row.update(overrides)
    return row


def _contract(**overrides):
    row = {
        "estado_contrato": "En ejecución",
        "fecha_de_firma": "2026-06-05",
        "fecha_de_inicio_del_contrato": "2026-06-16",
        "fecha_de_fin_del_contrato": "2026-12-31",
        "liquidaci_n": "Si",
        "fecha_inicio_liquidacion": "2026-08-01",
        "fecha_fin_liquidacion": "2027-04-30",
    }
    row.update(overrides)
    return row


def test_process_observer_exposes_only_neutral_axes_and_does_not_mutate():
    row = _process()
    snapshot = dict(row)
    payload = observe_process_truth_axes(row, reference_instant=REFERENCE)

    assert row == snapshot
    assert set(payload) == {
        "process_lifecycle",
        "process_resolution",
        "temporal",
        "visibility",
        "quality_summary",
        "reason_codes",
    }
    assert payload["process_lifecycle"] == "offer_phase_observed"
    assert payload["temporal"]["relation"] == "future"
    assert payload["visibility"] == "public_view_available"
    forbidden = {"role", "offerability", "bid_ready", "query_context"}
    assert forbidden.isdisjoint(payload)


def test_observers_require_an_explicit_timezone_aware_reference():
    with pytest.raises(ValueError, match="timezone-aware"):
        observe_process_truth_axes(_process(), reference_instant=datetime(2026, 8, 3))
    with pytest.raises(ValueError, match="timezone-aware"):
        observe_contract_truth_axes(_contract(), reference_instant=datetime(2026, 8, 3))


def test_future_liquidation_end_never_proves_finished():
    payload = observe_contract_truth_axes(_contract(), reference_instant=REFERENCE)
    assert payload["liquidation"] == {
        "status": "in_progress",
        "completion": "unknown",
    }
    assert payload["liquidation_event"]["relation"] == "future"
    assert "LIQUIDATION_STARTED_END_DATE_NOT_YET_REACHED" in payload["reason_codes"]
    assert payload["quality_summary"]["completeness"] != "complete"
    assert payload["quality_summary"]["confidence"] != "high"


def test_past_liquidation_end_preserves_supported_completion():
    payload = observe_contract_truth_axes(
        _contract(
            fecha_inicio_liquidacion="2026-01-01",
            fecha_fin_liquidacion="2026-02-26",
        ),
        reference_instant=REFERENCE,
    )
    assert payload["liquidation"] == {"status": "yes", "completion": "finished"}
    assert payload["liquidation_event"]["relation"] == "past"


def test_bare_date_today_does_not_prove_finished():
    payload = observe_contract_truth_axes(
        _contract(fecha_fin_liquidacion="2026-08-03"),
        reference_instant=REFERENCE,
    )
    assert payload["liquidation"]["completion"] == "unknown"
    assert payload["liquidation_event"]["relation"] == "today"


def test_real_flag_date_conflict_remains_explicit():
    payload = observe_contract_truth_axes(
        _contract(
            liquidaci_n="No",
            fecha_inicio_liquidacion="2027-01-01",
            fecha_fin_liquidacion="2027-04-30",
        ),
        reference_instant=REFERENCE,
    )
    assert payload["liquidation"] == {
        "status": "conflicting",
        "completion": "conflicting",
    }
    assert "LIQUIDATION_FLAG_VS_DATES" in payload["quality_summary"]["conflict_codes"]


def test_observer_source_has_no_clock_io_or_product_dependencies():
    path = Path("app/core/truth_layer/observers.py")
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    forbidden_import_tokens = {
        "soql",
        "modality",
        "workflow",
        "socrata",
        "warehouse",
        "streamlit",
        "feedback",
        "generic_enrichment",
    }
    assert not {
        module
        for module in imported
        if any(token in module.casefold() for token in forbidden_import_tokens)
    }
    forbidden_calls = {"now", "utcnow", "today", "open", "read_text", "write_text"}
    called = {
        node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, (ast.Attribute, ast.Name))
    }
    assert forbidden_calls.isdisjoint(called)
    assert len(source.splitlines()) < 500
