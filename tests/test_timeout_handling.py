"""Tests para manejo de timeout en consultas SECOP pesadas."""
from __future__ import annotations

from pathlib import Path

import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def test_timeout_with_semantic_order_does_not_fallback(tmp_path):
    """H4: orden SEMÁNTICO ("mas altos" → valor_desc) NO degrada a muestra sin orden.

    Devolver 50 filas arbitrarias para "los más altos" respondería otra pregunta. El
    contrato correcto: una sola llamada (sin fallback) y timeout honesto, SIN afirmar
    ranking. (Reemplaza el contrato viejo que sí reintentaba sin ordenar.)
    """
    call_count = {"n": 0}

    def _query(*args, **kwargs):
        call_count["n"] += 1
        raise TimeoutError("SECOP timeout")

    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = _query  # type: ignore[method-assign]
    workflow.secop_client.count = lambda dataset_id, soql: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]

    result = workflow.run_query(
        "muestrame los contratos mas altos del distrito de barranquilla de primera infancia",
        channel="streamlit",
    )

    # Orden semántico → SIN fallback: una sola llamada (no se intenta sin ORDER BY).
    assert call_count["n"] == 1, f"Expected 1 call (no fallback for semantic order), got {call_count['n']}"
    assert result.get("result_order_guarantee") == "unavailable"
    # Timeout honesto, sin muestra ni afirmación de "los más altos".
    resp = result.get("response", "").lower()
    assert "no respondio" in resp or "se demoro" in resp or "demoró" in resp


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


def test_timeout_presentational_attempts_one_fallback(tmp_path):
    """H4: orden PRESENTACIONAL (sin ordering_signal) SÍ intenta UN fallback sin ORDER BY.

    Si el fallback también hace timeout, cae a un timeout honesto. Verifica que se intentó
    exactamente una vez más (2 llamadas), no una cascada. (Reemplaza el contrato viejo de
    "sin retry": antes no se recuperaba nada; ahora se intenta una variante más barata.)
    """
    call_count = {"n": 0}

    def _query(*args, **kwargs):
        call_count["n"] += 1
        raise TimeoutError("SECOP timeout")

    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = _query  # type: ignore[method-assign]
    workflow.secop_client.count = lambda dataset_id, soql: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]

    result = workflow.run_query("contratos de mantenimiento", channel="streamlit")
    # 2 llamadas: principal (con ORDER BY) + UN fallback (sin ORDER BY). Máximo uno.
    assert call_count["n"] == 2, f"Expected 2 calls (primary + 1 fallback), got {call_count['n']}"
    # Ambas fallaron → timeout honesto, sin falso EMPTY ni ranking.
    assert result.get("result_order_guarantee") == "unavailable"
    resp = result.get("response", "").lower()
    assert "no respondio" in resp or "se demoro" in resp or "demoró" in resp
