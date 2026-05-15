"""Test E2E: follow-up contextual preserva dataset y objeto."""
from __future__ import annotations

from pathlib import Path

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.conversation_store import ConversationStore


def _mock_workflow(tmp_path):
    """Workflow con mocks de SECOP + conv_store aislado."""
    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = lambda dataset_id, soql_query: [  # type: ignore[method-assign]
        {
            "nombre_del_procedimiento": "Atencion integral primera infancia",
            "entidad": "ALCALDIA DE BARRANQUILLA",
            "precio_base": "500000000",
            "estado_de_apertura_del_proceso": "Celebrado",
            "fecha_de_publicacion_del": "2025-06-01T00:00:00.000",
            "urlproceso": "https://secop.gov.co/test",
            "referencia_del_proceso": "REF-TEST-1",
        }
    ]
    workflow.secop_client.count = lambda dataset_id, soql_query: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    return workflow


def test_followup_preserves_dataset_and_objeto(tmp_path):
    """
    Turno 1: "contratos de primera infancia alcaldia de barranquilla"
    → dataset=contratos, objeto=["primera","infancia"]

    Turno 2: "muestrame los de mayor valor de 2026"
    → follow-up preserva dataset=contratos y objeto=["primera","infancia"]
    → agrega ordering_signal=valor_desc y fecha_desde=2026-01-01
    """
    workflow = _mock_workflow(tmp_path)

    # ── Turno 1 ───────────────────────────────────────────────────────────
    r1 = workflow.run_query(
        "contratos de primera infancia alcaldia de barranquilla",
        channel="streamlit",
        chat_id="st_test",
    )

    assert r1["parsed_params"].get("dataset") == "contratos", (
        f"Expected dataset=contratos, got {r1['parsed_params'].get('dataset')}"
    )
    obj1 = r1["parsed_params"].get("objeto", [])
    assert "primera" in obj1, f"Expected 'primera' in objeto, got {obj1}"
    assert "infancia" in obj1, f"Expected 'infancia' in objeto, got {obj1}"

    # ── Turno 2 (follow-up) ───────────────────────────────────────────────
    r2 = workflow.run_query(
        "muestrame los de mayor valor de 2026",
        channel="streamlit",
        chat_id="st_test",
    )

    # Dataset debe preservarse de Turno 1 (dataset_explicit=False → keep_previous)
    ds2 = r2.get("resolved_params", {}).get("dataset", r2["parsed_params"].get("dataset"))
    assert ds2 == "contratos", f"Expected dataset=contratos (preserved), got {ds2}"

    # Objeto debe preservarse (follow-up objeto vacio → keep_previous)
    obj2 = r2.get("resolved_params", {}).get("objeto", []) or r2["parsed_params"].get("objeto", [])
    assert "primera" in obj2, f"Expected 'primera' preserved in objeto, got {obj2}"
    assert "infancia" in obj2, f"Expected 'infancia' preserved in objeto, got {obj2}"

    # Debe tener ordering_signal
    assert r2["parsed_params"].get("ordering_signal") == "valor_desc", (
        f"Expected ordering_signal=valor_desc, got {r2['parsed_params'].get('ordering_signal')}"
    )

    # Debe tener fechas para 2026
    pp2 = r2["parsed_params"]
    assert pp2.get("fecha_desde") == "2026-01-01", f"fecha_desde={pp2.get('fecha_desde')}"
    assert pp2.get("fecha_hasta") == "2026-12-31", f"fecha_hasta={pp2.get('fecha_hasta')}"

    # El SoQL debe usar contratos dataset y ORDER BY valor DESC
    soql = r2.get("soql_query", "")
    assert "jbjy-vk9h" in soql or "valor_del_contrato" in soql, (
        f"SoQL no usa dataset contratos: {soql[:200]}"
    )
    assert "ORDER BY valor_del_contrato DESC" in soql or (
        "valor_del_contrato DESC" in soql
    ), f"SoQL no ordena por valor DESC: {soql[:200]}"


def test_no_dataset_override_when_not_explicit(tmp_path):
    """
    Si el turno 1 tiene dataset=procesos (default, sin keyword explicito),
    y turno 2 tampoco tiene keyword, dataset se preserva.
    """
    workflow = _mock_workflow(tmp_path)

    r1 = workflow.run_query("mantenimiento vial", channel="streamlit", chat_id="st_test2")
    assert r1["parsed_params"].get("dataset") == "procesos"

    r2 = workflow.run_query("los mas grandes", channel="streamlit", chat_id="st_test2")
    ds2 = r2.get("resolved_params", {}).get("dataset", r2["parsed_params"].get("dataset"))
    assert ds2 == "procesos", f"dataset should stay 'procesos', got {ds2}"
