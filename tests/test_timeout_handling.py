"""Tests para manejo de timeout en consultas SECOP pesadas."""
from __future__ import annotations

from pathlib import Path

import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def test_timeout_with_ordering_retries_without_ordering(tmp_path):
    """
    Input: "muestrame los contratos mas altos del distrito de barranquilla de primera infancia"
    Mock: secop_client.query lanza TimeoutError en el primer intento, funciona en el segundo.
    Verify: response dice "SECOP se demoro", timeout_suggestions contiene sugerencias.
    """
    call_count = {"n": 0}

    def _query(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise TimeoutError("SECOP timeout")
        return [
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

    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = _query  # type: ignore[method-assign]
    workflow.secop_client.count = lambda dataset_id, soql: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]

    result = workflow.run_query(
        "muestrame los contratos mas altos del distrito de barranquilla de primera infancia",
        channel="streamlit",
    )

    # Debió reintentar (2 llamadas a query: 1 timeout + 1 exitosa)
    assert call_count["n"] == 2, f"Expected 2 calls (timeout + retry), got {call_count['n']}"

    # Response debe mencionar que se cambió la estrategia
    response = result.get("response", "")
    assert "SECOP se demoro" in response or "se demoro" in response.lower() or "cambie" in response.lower(), (
        f"Response should mention timeout retry: {response[:200]}"
    )

    # Debe tener timeout_suggestions
    ts = result.get("timeout_suggestions", [])
    assert len(ts) > 0, f"Expected timeout_suggestions, got {ts}"

    # Debe incluir sugerencia de filtrar por 2026
    labels = [s.get("label", "") for s in ts]
    assert any("2026" in l for l in labels), f"Expected '2026' suggestion, got {labels}"
    assert any("3" in l for l in labels) or any("anos" in l.lower() for l in labels), f"Expected time range suggestion, got {labels}"


def test_normal_query_no_timeout_has_no_timeout_suggestions(tmp_path):
    """Query normal sin timeout no debe tener timeout_suggestions."""
    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = lambda dataset_id, soql: [  # type: ignore[method-assign]
        {
            "nombre_del_procedimiento": "Mantenimiento vial",
            "entidad": "DEPARTAMENTO DE ATLANTICO",
            "precio_base": "500000000",
            "estado_de_apertura_del_proceso": "Abierto",
            "fecha_de_publicacion_del": "2026-04-01T00:00:00.000",
            "urlproceso": "https://secop.gov.co/test",
            "referencia_del_proceso": "REF-TEST-1",
        }
    ]
    workflow.secop_client.count = lambda dataset_id, soql: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]

    result = workflow.run_query("contratos de mantenimiento en antioquia", channel="streamlit")
    ts = result.get("timeout_suggestions", [])
    assert ts == [], f"Expected empty timeout_suggestions, got {ts}"


def test_timeout_without_ordering_no_retry(tmp_path):
    """Timeout sin ordering_signal no debe reintentar."""
    call_count = {"n": 0}

    def _query(*args, **kwargs):
        call_count["n"] += 1
        raise TimeoutError("SECOP timeout")

    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = _query  # type: ignore[method-assign]
    workflow.secop_client.count = lambda dataset_id, soql: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]

    result = workflow.run_query("contratos de mantenimiento", channel="streamlit")
    # Solo 1 llamada (sin retry)
    assert call_count["n"] == 1, f"Expected 1 call (no retry), got {call_count['n']}"
    assert "no respondio" in result.get("response", "").lower() or "se demoro" in result.get("response", "").lower()
