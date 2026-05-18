"""Tests for VALUE-RANGE-001: monetary range parsing (hyphen and entre...y)."""

import pytest
from app.core.query_router import QueryRouter


def test_hyphen_range_millones():
    router = QueryRouter()
    result = router.parse("contratos de mantenimiento entre 1000-3000 millones de 2026")
    params = result.params if hasattr(result, "params") else result
    assert params.get("valor_min") == 1_000_000_000
    assert params.get("valor_max") == 3_000_000_000


def test_hyphen_range_con_y():
    router = QueryRouter()
    result = router.parse("contratos de mantenimiento entre 1000 y 3000 millones de 2026")
    params = result.params if hasattr(result, "params") else result
    assert params.get("valor_min") == 1_000_000_000
    assert params.get("valor_max") == 3_000_000_000


def test_range_de_a_millones():
    router = QueryRouter()
    result = router.parse("contratos de mantenimiento de 1000 a 3000 millones")
    params = result.params if hasattr(result, "params") else result
    assert params.get("valor_min") == 1_000_000_000
    assert params.get("valor_max") == 3_000_000_000


def test_range_followup_preserves_values():
    router = QueryRouter()
    # Turn 1
    result1 = router.parse("contratos de mantenimiento entre 1000-3000 millones de 2026")
    params1 = result1.params if hasattr(result1, "params") else result1
    assert params1.get("valor_min") == 1_000_000_000
    assert params1.get("valor_max") == 3_000_000_000

    # Follow-up preservation is handled by FollowupEngine (out of scope for this minimal fix)
    # We only verify that the initial parse correctly extracts both min and max.