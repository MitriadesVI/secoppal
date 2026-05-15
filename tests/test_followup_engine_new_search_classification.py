"""Tests de equivalencia de responsabilidad H3/H8.

Verifica que detect_and_merge clasifica como new_search (descarta contexto)
las queries que empiezan con verbos de busqueda nueva, aunque haya turno
reciente con dataset y objeto previos.

El viejo is_followup() interceptaba estos casos. Ahora debe hacerlo
detect_and_merge en apply_context.
"""

from __future__ import annotations

import pytest

from app.core.followup_engine import detect_and_merge
from app.core.query_frame import QueryFrame, frame_from_params, params_from_frame
from app.core._followup_constants import NEW_SEARCH_VERBS


def _prev_frame(**extra) -> QueryFrame:
    """Turno previo con objeto y departamento."""
    return frame_from_params({
        "dataset": "contratos",
        "objeto": ["mantenimiento"],
        "departamento_resolved": "ANTIOQUIA",
        "entidad_resolved": "GOBERNACION DE ANTIOQUIA",
        "fecha_desde": "2026-01-01",
        **extra,
    })


def _assert_no_context_inherited(intent, merged):
    """Verifica que el merge NO conserva objeto/departamento/entidad del previo."""
    assert intent == "new_search", f"expected new_search, got {intent}"
    # El objeto del turno actual debe ser el que vino en la query, no el heredado
    if "mantenimiento" in str(merged.get("objeto", [])):
        raise AssertionError(
            f"'mantenimiento' del turno previo contaminó el objeto del merge: {merged.get('objeto')}"
        )
    # El departamento del turno previo no debe aparecer
    if merged.get("departamento_resolved") == "ANTIOQUIA":
        raise AssertionError(
            f"departamento ANTIOQUIA del turno previo contaminó el merge"
        )
    # La entidad del turno previo no debe aparecer
    if merged.get("entidad_resolved") == "GOBERNACION DE ANTIOQUIA":
        raise AssertionError(
            f"entidad del turno previo contaminó el merge"
        )


class TestNewSearchVerbsDiscardContext:
    """Cada query con verbo de nueva busqueda debe clasificarse como new_search
    y NO heredar objeto/departamento/entidad del turno previo."""

    prev = _prev_frame()

    @pytest.mark.parametrize("query,current_params", [
        (
            "busca contratos de salud en bogota",
            {"objeto": ["salud"], "ciudad": "Bogotá", "dataset": "contratos"},
        ),
        (
            "necesito licitaciones de agua",
            {"objeto": ["agua"], "dataset": "procesos"},
        ),
        (
            "encuentra procesos de obra",
            {"objeto": ["obra"], "dataset": "procesos"},
        ),
        (
            "lista los contratos de barranquilla",
            {"objeto": [], "ciudad": "Barranquilla", "dataset": "contratos"},
        ),
        (
            "consulta procesos de educacion",
            {"objeto": ["educacion"], "dataset": "procesos"},
        ),
        (
            "muestra contratos de la alcaldia",
            {"objeto": [], "entidad": "alcaldia", "dataset": "contratos"},
        ),
    ])
    def test_verbos_busqueda_nueva_descartan_contexto(self, query, current_params):
        intent, merged = detect_and_merge(
            text=query,
            current_params=current_params,
            previous_frame=self.prev,
        )
        _assert_no_context_inherited(intent, merged)

    def test_todos_los_new_search_verbs_estan_cubiertos(self):
        """Sanity: verifica que NEW_SEARCH_VERBS no esta vacio y tiene los valores esperados."""
        assert len(NEW_SEARCH_VERBS) >= 4
        assert "busca " in NEW_SEARCH_VERBS
        assert "necesito " in NEW_SEARCH_VERBS
        assert "encuentra " in NEW_SEARCH_VERBS
        assert "consulta " in NEW_SEARCH_VERBS
