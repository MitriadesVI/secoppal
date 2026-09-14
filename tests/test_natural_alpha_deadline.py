from unittest.mock import Mock

import pytest

from app.core import deadline
from app.core.natural_alpha.provider import ProviderResult
from tests.test_natural_alpha_contract import payload
from tests.test_natural_alpha_runtime import make_flow, run  # noqa: F401


@pytest.mark.parametrize("response", [
    ProviderResult(payload(), provider_calls=1),
    ProviderResult(error="PROVIDER_TIMEOUT", provider_calls=1),
    ProviderResult(error="PROVIDER_FAILURE", provider_calls=1),
    ProviderResult("bad JSON", provider_calls=1),
])
def test_alpha_latency_never_consumes_legacy_fifteen_seconds(make_flow, monkeypatch, response):
    from app.core import orchestrator

    clock = [100.0]
    monkeypatch.setattr(orchestrator.time, "perf_counter", lambda: clock[0])
    monkeypatch.setitem(deadline._clock, "now", lambda: clock[0])
    flow = make_flow(response=response)

    def slow_provider(*args):
        clock[0] += 12
        return response

    flow.natural_alpha_provider.observe.side_effect = slow_provider
    observed = []

    def legacy(deps, ctx):
        observed.append((deadline.current_deadline().remaining_s(), ctx.user_query, ctx.run_started_at))
        clock[0] += 3
        deadline.record_deadline(fetch_status="completed")
        result = {"response": "legacy", "resolved_params": {}, "route_reason": "legacy", "results": []}
        result["trace_id"] = deps.feedback.log_trace(ctx.user_query, ctx.channel, result)
        return result

    monkeypatch.setattr(orchestrator, "handle_normal_flow", legacy)
    result = run(flow)
    assert observed == [(15.0, "busca oportunidades de mantenimiento", 100.0)]
    assert result["turn_elapsed_ms"] == 15_000
    assert result["natural_alpha_latency_ms"] == 12_000
    assert result["natural_alpha_legacy_elapsed_ms"] == 3_000
    assert result["natural_alpha_legacy_budget_ms"] == 15_000
    assert result["fetch_status"] == "completed"
    assert deadline.current_deadline() is None


@pytest.mark.parametrize("mode,eligible", [("off", True), ("alpha", False)])
def test_off_and_ineligible_keep_original_clock(make_flow, monkeypatch, mode, eligible):
    from app.core import orchestrator

    clock = [100.0]
    monkeypatch.setattr(orchestrator.time, "perf_counter", lambda: clock[0])
    monkeypatch.setitem(deadline._clock, "now", lambda: clock[0])
    flow = make_flow(mode)
    if not eligible:
        flow.settings.secoppal_auth_enabled = False

    def earlier_handler(*args):
        clock[0] += 3

    monkeypatch.setattr(orchestrator, "handle_direct_lookup", earlier_handler)
    remaining = []

    def legacy(deps, ctx):
        remaining.append(deadline.current_deadline().remaining_s())
        return {"response": "legacy", "resolved_params": {}}

    monkeypatch.setattr(orchestrator, "handle_normal_flow", legacy)
    result = run(flow)
    assert remaining == [12.0]
    flow.natural_alpha_provider.observe.assert_not_called()
    assert not any(key.startswith("natural_alpha_") for key in result)


def test_legacy_exception_restores_enclosing_deadline(make_flow, monkeypatch):
    from app.core import orchestrator

    flow = make_flow()
    monkeypatch.setattr(orchestrator, "handle_normal_flow", Mock(side_effect=RuntimeError("legacy")))
    tokens = deadline.begin_deadline(deadline.now(), budget_s=77)
    outer = deadline.current_deadline()
    try:
        with pytest.raises(RuntimeError, match="legacy"):
            run(flow)
        assert deadline.current_deadline() is outer
        assert deadline.current_deadline().budget_s == 77
    finally:
        deadline.end_deadline(tokens)
