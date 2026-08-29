"""R2 — el puente Radar → Workspace.

Lo que aquí se protege es la IDENTIDAD de lo guardado y la honestidad del
snapshot. Un item cuya identidad se derive del título se pierde en cuanto SECOP
renombra el proceso; un snapshot que contenga algo que nadie observó le miente
al usuario meses después, cuando ya no puede contrastarlo.
"""
from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.radar.detail import build_opportunity_detail
from app.core.radar.workspace import (
    WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
    build_workspace_snapshot,
    is_saveable,
    workspace_item_type,
    workspace_reference_id,
    workspace_title,
)
from app.core.soql_builder import SoQLBuilder
from app.core.workspace_store import WORKSPACE_ITEM_TYPES

NOW = datetime(2026, 8, 29, 15, 0, tzinfo=timezone.utc)  # 10:00 en Bogotá
SAVED_AT = "2026-08-29T15:30:00+00:00"
PROC = SoQLBuilder.PROCESOS_DATASET
CONT = SoQLBuilder.CONTRATOS_DATASET


def _process_event(**overrides) -> dict:
    base = {
        "id": "ev-1",
        "radar_id": "rad-1",
        "kind": "new",
        "radar_name": "Mantenimiento Costa",
        "result_key": "CO1.NTC.111",
        "dataset": "procesos",
        "ts": "2026-08-28T15:00:00+00:00",
        "title": "Mantenimiento preventivo y correctivo de vías",
        "object_excerpt": "Mantenimiento de la malla vial urbana del municipio.",
        "entity": "ALCALDÍA DE SOLEDAD",
        "department": "Atlántico",
        "city": "Soledad",
        "value": "1250000000",
        "status": "Abierto",
        "modality": "Licitación pública",
        "closing_date": "2026-09-02T00:00:00.000",
        "referencia": "LP-004-2026",
        "urlproceso": "https://community.secop.gov.co/Public/Tendering/x",
    }
    base.update(overrides)
    return base


def _contract_event(**overrides) -> dict:
    base = {
        "id": "ev-2",
        "radar_id": "rad-2",
        "kind": "new",
        "radar_name": "Proveedores",
        "result_key": "CO1.PCCNTR.999",
        "dataset": "contratos",
        "ts": "2026-08-28T15:00:00+00:00",
        "title": "Suministro de dotación escolar",
        "entity": "SECRETARÍA DE EDUCACIÓN",
        "value": "480000000",
        "status": "En ejecución",
        "contractor": "DOTACIONES SAS",
        "signature_date": "2026-03-11T00:00:00.000",
        "referencia": "CTO-2026-77",
    }
    base.update(overrides)
    return base


def _detail(event: dict):
    detalle = build_opportunity_detail(event, now=NOW)
    assert detalle is not None
    return detalle


# ── Tipo e identidad ────────────────────────────────────────────────────────


def test_process_maps_to_proceso_with_a_namespaced_reference():
    detalle = _detail(_process_event())

    assert workspace_item_type(detalle) == "proceso"
    assert workspace_reference_id(detalle) == f"{PROC}/CO1.NTC.111"
    assert is_saveable(detalle)


def test_contract_maps_to_contrato_with_a_namespaced_reference():
    detalle = _detail(_contract_event())

    assert workspace_item_type(detalle) == "contrato"
    assert workspace_reference_id(detalle) == f"{CONT}/CO1.PCCNTR.999"
    assert is_saveable(detalle)


def test_both_item_types_exist_in_the_deployed_schema():
    """Los strings son los de la tabla real, no los del plan histórico."""
    assert {"proceso", "contrato"} <= set(WORKSPACE_ITEM_TYPES)
    assert "process" not in WORKSPACE_ITEM_TYPES
    assert "contract" not in WORKSPACE_ITEM_TYPES


def test_an_unknown_dataset_is_not_saveable():
    """Falla cerrado: clasificarlo como proceso le afirmaría un cierre ajeno."""
    detalle = _detail(_process_event(dataset="otra-cosa"))

    assert detalle.dataset_id is None
    assert workspace_item_type(detalle) is None
    assert workspace_reference_id(detalle) is None
    assert not is_saveable(detalle)


def test_an_event_without_result_key_never_reaches_the_workspace():
    """Sin clave estable no hay nada que volver a reconocer."""
    assert build_opportunity_detail(_process_event(result_key=None), now=NOW) is None
    assert build_opportunity_detail(_process_event(result_key="   "), now=NOW) is None
    assert not is_saveable(None)


def test_identity_survives_a_title_change():
    """El título es display; la identidad no puede depender de él."""
    original = _detail(_process_event())
    renombrado = _detail(_process_event(title="OTRO NOMBRE COMPLETAMENTE DISTINTO"))

    assert workspace_reference_id(original) == workspace_reference_id(renombrado)


def test_title_falls_back_to_observed_facts_never_to_an_invention():
    """``title`` es NOT NULL, pero el relleno sale del propio evento."""
    assert workspace_title(_detail(_process_event())).startswith("Mantenimiento")
    sin_nombre = _detail(_process_event(title=None))
    assert workspace_title(sin_nombre) == "LP-004-2026"
    sin_nada = _detail(_process_event(title=None, referencia=None))
    assert workspace_title(sin_nada) == "CO1.NTC.111"


# ── Snapshot ────────────────────────────────────────────────────────────────


def test_snapshot_carries_the_observed_facts_of_a_process():
    evento = _process_event()
    snapshot = build_workspace_snapshot(evento, _detail(evento), SAVED_AT)

    assert snapshot["schema_version"] == WORKSPACE_SNAPSHOT_SCHEMA_VERSION
    assert snapshot["source"] == "radar"
    assert snapshot["dataset_id"] == PROC
    assert snapshot["result_key"] == "CO1.NTC.111"
    assert snapshot["radar_id"] == "rad-1"
    assert snapshot["radar_name"] == "Mantenimiento Costa"
    assert snapshot["radar_event_id"] == "ev-1"
    assert snapshot["observed_at"] == "2026-08-28T15:00:00+00:00"
    assert snapshot["saved_at"] == SAVED_AT
    assert snapshot["entity"] == "ALCALDÍA DE SOLEDAD"
    assert snapshot["location"] == "Soledad, Atlántico"
    assert snapshot["modality"] == "Licitación pública"
    assert snapshot["source_status"] == "Abierto"
    assert snapshot["reference"] == "LP-004-2026"
    assert snapshot["secop_url"].startswith("https://")
    assert snapshot["value_display"] == "$1,250,000,000"


def test_snapshot_stores_the_closing_day_without_a_countdown():
    """El día se observó; "faltan 4 días" caduca mañana y sería falso."""
    evento = _process_event()
    snapshot = build_workspace_snapshot(evento, _detail(evento), SAVED_AT)

    assert snapshot["closing_date"] == "2026-09-02"
    assert "T00:00" not in snapshot["closing_date"]
    for prohibido in ("days_remaining", "relation", "closing_relation", "dias_restantes"):
        assert prohibido not in snapshot


def test_snapshot_never_invents_a_field_the_event_did_not_carry():
    """Ausencia se preserva: la clave no aparece, en vez de salir en null."""
    evento = _process_event(
        modality=None, status=None, urlproceso=None, referencia=None,
        department=None, city=None, value=None, closing_date=None,
    )
    snapshot = build_workspace_snapshot(evento, _detail(evento), SAVED_AT)

    for ausente in ("modality", "source_status", "secop_url", "reference",
                    "location", "value_display", "closing_date"):
        assert ausente not in snapshot
    assert None not in snapshot.values()
    # Y nunca aparecen campos derivados que ninguna capa observó.
    for inventado in ("operational_status", "publication_date", "eligibility",
                      "relevance_score", "deadline_hour"):
        assert inventado not in snapshot


def test_contract_snapshot_shows_signature_not_a_bidding_deadline():
    evento = _contract_event()
    snapshot = build_workspace_snapshot(evento, _detail(evento), SAVED_AT)

    assert snapshot["contractor"] == "DOTACIONES SAS"
    assert snapshot["signature_date"] == "2026-03-11"
    # Un contrato no tiene cierre de ofertas: el proceso ya terminó.
    assert "closing_date" not in snapshot


def test_snapshot_is_self_contained_and_json_serializable():
    """"Mi escritorio" debe poder pintarse sin releer radar ni SECOP."""
    evento = _process_event()
    snapshot = build_workspace_snapshot(evento, _detail(evento), SAVED_AT)

    recuperado = json.loads(json.dumps(snapshot, ensure_ascii=False))
    assert recuperado == snapshot
    # Todo lo que la tarjeta necesita ya está en el snapshot.
    for clave in ("title", "entity", "location", "value_display",
                  "source_status", "modality", "closing_date", "reference",
                  "secop_url", "saved_at"):
        assert clave in recuperado


def test_snapshot_survives_an_event_without_radar_id():
    evento = _process_event()
    evento.pop("radar_id")
    snapshot = build_workspace_snapshot(evento, _detail(evento), SAVED_AT)

    assert "radar_id" not in snapshot
    assert snapshot["radar_event_id"] == "ev-1"


# ── Alcance del módulo ──────────────────────────────────────────────────────


def test_the_bridge_is_pure_and_never_touches_sqlite_or_secop():
    """La escritura tiene dueño; el radar no la duplica."""
    fuente = (Path(__file__).parents[1] / "app/core/radar/workspace.py").read_text()
    arbol = ast.parse(fuente)
    importados = {
        node.module
        for node in ast.walk(arbol)
        if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(arbol)
        if isinstance(node, ast.Import) for alias in node.names
    }
    assert "sqlite3" not in importados
    assert not any("workspace_store" in m for m in importados)
    assert not any("client" in m or "requests" in m for m in importados)

    # Ninguna llamada de persistencia ni de red: no basta con no importar el
    # módulo, hay que no ejercer el verbo.
    llamadas = {
        node.func.attr
        for node in ast.walk(arbol)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert not ({"execute", "executescript", "commit", "connect", "get", "post"}
                & (llamadas - {"get"}))
    # ``dict.get`` sí se usa (lectura del evento); ``requests.get`` no existe
    # porque no hay import de red, ya comprobado arriba.


@pytest.mark.parametrize("detalle", [None])
def test_missing_detail_is_never_saveable(detalle):
    assert workspace_item_type(detalle) is None
    assert workspace_reference_id(detalle) is None
    assert not is_saveable(detalle)
