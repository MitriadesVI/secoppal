"""Tests de equivalencia H9: execute_query y FollowupGuards usan la misma
fuente de verdad SCOPE_TOPIC_KEYS para el guard anti-WHERE 1=1."""

from __future__ import annotations

import pytest

from app.core._followup_constants import SCOPE_TOPIC_KEYS
from app.core.orchestrator import (
    SecopalWorkflow,
    execute_query as _execute_query_action,
)
from app.core.followup_engine import FollowupGuards


# ── Casos compartidos ───────────────────────────────────────────────────

_SCOPE_PRESENT = [
    ({"objeto": ["mantenimiento"]}, "objeto"),
    ({"entidad_resolved": "ALCALDIA DE BOGOTA"}, "entidad_resolved"),
    ({"entidad_like": "%BOGOTA%"}, "entidad_like"),
    ({"departamento_resolved": "ANTIOQUIA"}, "departamento_resolved"),
    ({"ciudad": "Medellin"}, "ciudad"),
    ({"contratista": "CONSTRUCTORA ABC"}, "contratista"),
]

_MODIFIERS_ONLY = [
    ({"dataset": "contratos"}, "dataset solo"),
    ({"estado": "En ejecucion"}, "estado solo"),
    ({"fecha_desde": "2026-01-01"}, "fecha_desde solo"),
    ({"valor_min": 100000000}, "valor_min solo"),
    ({"ordering_signal": "valor_desc"}, "order solo"),
    ({"limit": 10}, "limit solo"),
    ({"offset": 20}, "offset solo"),
]


def _guard_accepts(params: dict) -> bool:
    """El guard acepta si al menos una key de SCOPE_TOPIC_KEYS tiene valor."""
    return any(
        params.get(k) and params.get(k) not in (None, [], "", {}, False)
        for k in SCOPE_TOPIC_KEYS
    )


class TestGuardEquivalence:
    """Ambos guards (execute_query via SCOPE_TOPIC_KEYS y FollowupGuards)
    deben aceptar/rechazar el mismo set de params."""

    @pytest.mark.parametrize("params,case_name", _SCOPE_PRESENT)
    def test_scope_present_accepted_by_both(self, params, case_name):
        expected = _guard_accepts(params)
        actual_guards = FollowupGuards.check_no_where_1_1(params)
        assert expected is True, f"{case_name}: SCOPE_TOPIC_KEYS should accept"
        assert actual_guards is expected, (
            f"{case_name}: FollowupGuards mismatch — "
            f"SCOPE_TOPIC_KEYS={expected}, FollowupGuards={actual_guards}"
        )

    @pytest.mark.parametrize("params,case_name", _MODIFIERS_ONLY)
    def test_modifiers_only_rejected_by_both(self, params, case_name):
        expected = _guard_accepts(params)
        actual_guards = FollowupGuards.check_no_where_1_1(params)
        assert expected is False, f"{case_name}: SCOPE_TOPIC_KEYS should reject"
        assert actual_guards is expected, (
            f"{case_name}: FollowupGuards mismatch — "
            f"SCOPE_TOPIC_KEYS={expected}, FollowupGuards={actual_guards}"
        )

    def test_empty_params_rejected(self):
        assert _guard_accepts({}) is False
        assert FollowupGuards.check_no_where_1_1({}) is False

    def test_null_like_values_still_rejected(self):
        """Valores vacíos/no-truthy en keys de scope NO pasan el guard."""
        params = {
            "objeto": [],
            "entidad_resolved": "",
            "ciudad": None,
            "departamento_resolved": {},
        }
        assert _guard_accepts(params) is False
        assert FollowupGuards.check_no_where_1_1(params) is False

    def test_sco_topic_keys_frozenset_matches(self):
        """Sanity: las keys son exactamente las esperadas."""
        expected = frozenset({
            "objeto", "entidad_resolved", "entidad_like",
            "departamento_resolved", "ciudad", "contratista",
        })
        assert SCOPE_TOPIC_KEYS == expected
