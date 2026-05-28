"""Unit tests for app.core.workflow.results.

These constructors are staged in Paso 3 but not consumed by run_query
yet (Paso 5 will do that). They need their own tests to stay honest in
the meantime.
"""
from __future__ import annotations

import pytest

from app.core.workflow.results import (
    CANONICAL_KEYS,
    empty_result,
    tabular_result,
)


# ─── empty_result ────────────────────────────────────────────────────────────

class TestEmptyResult:
    def test_required_fields_only_works(self):
        r = empty_result(response="hola", route_reason="reset")
        assert r["response"] == "hola"
        assert r["route_reason"] == "reset"

    def test_missing_required_raises(self):
        with pytest.raises(TypeError):
            empty_result(route_reason="reset")  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            empty_result(response="hola")  # type: ignore[call-arg]

    def test_defaults_match_legacy_shape(self):
        r = empty_result(response="hola", route_reason="reset")
        assert r["rows"] == []
        assert r["results"] == []
        assert r["dataset_id"] == ""
        assert r["soql_query"] == ""
        assert r["parsed_params"] == {}
        assert r["resolved_params"] == {}
        assert r["needs_llm"] is False
        assert r["needs_clarification"] is False
        assert r["degraded"] is False
        assert r["degraded_hint"] == ""
        assert r["total_count"] == 0
        assert r["universe_insights"] is None
        assert r["suggestions"] == []
        assert r["timeout_suggestions"] == []
        assert r["intent_type"] == ""
        assert r["followup"] is False
        assert r["trace_id"] == ""
        assert r["narrated"] is False

    def test_overrides_take_effect(self):
        r = empty_result(
            response="?",
            route_reason="pagination_no_context",
            needs_clarification=True,
            intent_type="pagination_more",
        )
        assert r["needs_clarification"] is True
        assert r["intent_type"] == "pagination_more"

    def test_total_count_can_be_none(self):
        r = empty_result(response="x", route_reason="reset", total_count=None)
        assert r["total_count"] is None


# ─── tabular_result ──────────────────────────────────────────────────────────

class TestTabularResult:
    MIN_KWARGS = dict(
        response="ok",
        rows=[{"a": 1}],
        results=[{"a": 1}],
        dataset_id="p6dx-8zbt",
        soql_query="SELECT *",
        parsed_params={"objeto": ["x"]},
        resolved_params={"objeto": ["x"]},
        route_reason="normal_flow",
        total_count=42,
    )

    def test_min_kwargs_work(self):
        r = tabular_result(**self.MIN_KWARGS)
        assert r["response"] == "ok"
        assert r["total_count"] == 42
        assert r["route_reason"] == "normal_flow"

    @pytest.mark.parametrize(
        "missing",
        list(MIN_KWARGS.keys()),
    )
    def test_each_required_is_actually_required(self, missing):
        kwargs = {k: v for k, v in self.MIN_KWARGS.items() if k != missing}
        with pytest.raises(TypeError):
            tabular_result(**kwargs)  # type: ignore[arg-type]

    def test_optional_extras_omitted_by_default(self):
        r = tabular_result(**self.MIN_KWARGS)
        assert "contextual_filters" not in r
        assert "total_count_approximate" not in r
        assert "risk_flag_timeout" not in r

    def test_optional_extras_when_set(self):
        r = tabular_result(
            **self.MIN_KWARGS,
            contextual_filters={"by_entity": []},
            total_count_approximate=True,
            risk_flag_timeout=False,
        )
        assert r["contextual_filters"] == {"by_entity": []}
        assert r["total_count_approximate"] is True
        assert r["risk_flag_timeout"] is False

    def test_trace_id_starts_empty(self):
        """trace_id is filled by persist_trace_and_turn, not at construction."""
        r = tabular_result(**self.MIN_KWARGS)
        assert r["trace_id"] == ""

    def test_narrated_starts_false(self):
        r = tabular_result(**self.MIN_KWARGS)
        assert r["narrated"] is False


# ─── canonical contract ──────────────────────────────────────────────────────

class TestCanonicalKeys:
    """Both constructors must only produce keys from CANONICAL_KEYS."""

    def test_empty_result_keys_are_canonical(self):
        r = empty_result(response="x", route_reason="y")
        assert set(r.keys()) <= CANONICAL_KEYS, (
            f"empty_result leaked non-canonical keys: {set(r.keys()) - CANONICAL_KEYS}"
        )

    def test_tabular_result_keys_are_canonical(self):
        r = tabular_result(**TestTabularResult.MIN_KWARGS)
        assert set(r.keys()) <= CANONICAL_KEYS

    def test_tabular_result_with_all_optionals_keys_are_canonical(self):
        r = tabular_result(
            **TestTabularResult.MIN_KWARGS,
            contextual_filters={},
            total_count_approximate=False,
            risk_flag_timeout=False,
        )
        assert set(r.keys()) <= CANONICAL_KEYS
