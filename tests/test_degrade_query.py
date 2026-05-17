"""Tests tarea 1.4.5 — degradacion elegante (relax_params + degrade_query action)."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.core.orchestrator import _relax_params, degrade_query


class TestRelaxParams:
    def test_drops_estado_first(self):
        p = {"estado": "Abierto", "departamento_resolved": "Antioquia", "objeto": ["obra"]}
        relaxed, hint = _relax_params(p)
        assert "estado" not in relaxed
        assert "departamento_resolved" in relaxed
        assert hint == "sin filtro de estado"

    def test_drops_fecha_when_no_estado(self):
        p = {"fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31", "objeto": ["vias"]}
        relaxed, hint = _relax_params(p)
        assert "fecha_desde" not in relaxed
        assert "fecha_hasta" not in relaxed
        assert hint == "sin filtro de fechas"

    def test_drops_valor_when_no_estado_fecha(self):
        p = {"valor_min": 50_000_000, "valor_max": 200_000_000, "objeto": ["dotacion"]}
        relaxed, hint = _relax_params(p)
        assert "valor_min" not in relaxed
        assert "valor_max" not in relaxed
        assert hint == "sin filtro de valor"

    def test_drops_departamento_last(self):
        p = {"departamento_resolved": "Choco", "objeto": ["mantenimiento"]}
        relaxed, hint = _relax_params(p)
        assert "departamento_resolved" not in relaxed
        assert hint == "en todo el pais"

    def test_nothing_to_relax_returns_none(self):
        p = {"objeto": ["obra"]}
        relaxed, hint = _relax_params(p)
        assert relaxed is None
        assert hint == ""

    def test_empty_params_returns_none(self):
        relaxed, hint = _relax_params({})
        assert relaxed is None
        assert hint == ""

    def test_original_params_not_mutated(self):
        p = {"estado": "Abierto", "objeto": ["obra"]}
        original_keys = set(p.keys())
        _relax_params(p)
        assert set(p.keys()) == original_keys

    def test_only_valor_min_dropped(self):
        p = {"valor_min": 100_000_000}
        relaxed, hint = _relax_params(p)
        assert relaxed == {}
        assert hint == "sin filtro de valor"

    def test_departamento_resolution_metadata_removed(self):
        p = {"departamento_resolved": "Valle del Cauca", "departamento_resolution": {"confidence": 0.9}}
        relaxed, hint = _relax_params(p)
        assert "departamento_resolution" not in relaxed
        assert hint == "en todo el pais"


class TestDegradeQueryAction:
    """Verifica que degrade_query action persiste total_count."""

    def test_degraded_query_sets_total_count(self):
        """N1: degrade_query con resultados debe escribir total_count > 0."""
        import time
        from burr.core import State

        params = {"estado": "Abierto", "objeto": ["obra"]}
        state = State({
            "results": [],
            "query_error": "",
            "resolved_params": params,
            "dataset_id": "p6dx-8zbt",
            "followup": False,
            "timings_ms": {},
        })
        soql_builder = MagicMock()
        soql_builder.build.return_value = "SELECT ... WHERE ..."
        soql_builder.build_count.return_value = "SELECT count(*) WHERE ..."

        secop_client = MagicMock()
        secop_client.query.return_value = [{"nombre": "test"}]
        secop_client.count.return_value = 5

        new_state = degrade_query(state, secop_client, soql_builder)

        assert new_state["degraded"] is True
        assert new_state["total_count"] == 5
        assert new_state["degraded_hint"] == "sin filtro de estado"
