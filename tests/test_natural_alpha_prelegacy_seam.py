"""C2: one observation before admission, consumed by the seam and never repeated.

Plan V2 8: the structured controls keep their precedence, Natural Alpha observes
once, and the observation travels to admission as inert input. C2 changes where
the observation is made — not who executes, not what the legacy parser sees.
"""
from datetime import date
from unittest.mock import patch

import pytest

from app.config import get_settings
from app.core.natural_alpha.models import GroundedJob, Observation
from app.core.workflow.admission import admit
from app.core.workflow.handlers import natural_alpha as seam
from tests.test_natural_alpha_contract import IDENTITY
from tests.test_natural_alpha_runtime import RAW, make_flow, run  # noqa: F401

PARITY_FIELDS = ("response", "route_reason", "parsed_params", "resolved_params", "query_plan",
                 "soql_query", "dataset_id", "results", "total_count", "needs_clarification")


@pytest.fixture
def frame_on(monkeypatch):
    """The Frame lane is where C2 installs the pre-admission observation.

    ``make_flow`` forces the flag off while it builds, so tests must switch it on
    after the flow exists — the same order the turn oracle uses.
    """
    def enable():
        monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "true")
        get_settings.cache_clear()

    yield enable
    get_settings.cache_clear()


def _observation():
    return Observation("alpha", "MATCHED", (GroundedJob("ANALYZE", "SUPPLIERS", 0, 4),),
                       provider_calls=1)


def test_text_turn_is_observed_once_and_the_seam_does_not_repeat(frame_on, make_flow):
    flow = make_flow()
    frame_on()
    with patch.object(seam, "observe", wraps=seam.observe) as spy:
        result = run(flow, RAW, chat_id="once")
    # One observation for the whole turn: made before admission, consumed at the
    # seam. A second observe() call here would be the duplicated call C2 forbids.
    assert spy.call_count == 1
    flow.natural_alpha_provider.observe.assert_called_once()
    assert result["natural_alpha_status"] == "MATCHED"
    assert len(flow.feedback.get_recent()) == 1


def test_admission_receives_the_observation_as_inert_input(frame_on, make_flow):
    flow = make_flow()
    frame_on()
    with patch("app.core.workflow.admission.admit", wraps=admit) as spy:
        run(flow, RAW, chat_id="carried")
    observation = spy.call_args.kwargs["observation"]
    assert isinstance(observation, Observation)
    assert observation.status == "MATCHED"
    assert flow.natural_alpha_provider.observe.call_count == 1


def test_dispatch_alone_still_observes_exactly_once(make_flow):
    # Lanes that never reach admission (frame disabled, reset, rollback) keep
    # observing at the seam, exactly once.
    flow = make_flow()
    ctx = flow._make_ctx(RAW, channel="streamlit", user_identity=IDENTITY,
                         today=date(2026, 9, 13), run_started_at=0.0)
    with patch.object(seam, "observe", wraps=seam.observe) as spy:
        flow._dispatch(ctx, None)
    assert spy.call_count == 1
    flow.natural_alpha_provider.observe.assert_called_once()


def test_pre_observation_is_free_when_the_flag_is_off(make_flow):
    flow = make_flow("off")
    ctx = flow._make_ctx(RAW, channel="streamlit", user_identity=IDENTITY)
    assert seam.pre_observe(flow.deps, ctx, None, flow.natural_alpha_provider) is None
    flow.natural_alpha_provider.observe.assert_not_called()


def test_frame_lane_parity_between_flag_off_and_alpha(frame_on, make_flow):
    off, alpha = make_flow("off"), make_flow("alpha")
    frame_on()
    before = run(off, RAW, chat_id="lane-off")
    after = run(alpha, RAW, chat_id="lane-on")
    for field in PARITY_FIELDS:
        assert after[field] == before[field], field
    off.natural_alpha_provider.observe.assert_not_called()
    alpha.natural_alpha_provider.observe.assert_called_once()


@pytest.mark.parametrize("raw", [RAW, "muestrame mas", "/reset", "CO1.BDOS.123456",
                                 "analiza la alcaldía de Baranoa"])
def test_admission_decision_is_identical_with_and_without_observation(make_flow, raw):
    deps = make_flow().deps
    plain = admit(deps, user_query=raw, chat_id=None, seed_resolved_params=None)
    carried = admit(deps, user_query=raw, chat_id=None, seed_resolved_params=None,
                    observation=_observation())
    assert (carried.route, carried.decision, carried.params) == (
        plain.route, plain.decision, plain.params)
    assert carried.effective_user_query == plain.effective_user_query


@pytest.mark.parametrize("raw,kwargs", [
    ("/reset", {}),
    ("mis radares", {}),
    ("muestrame mas", {}),
    ("CO1.BDOS.123456", {}),
    ("1", {}),
    ("opción 1", {"actor_candidate_selection": {"operation": "SELECT_ACTOR_CANDIDATE",
                                                "candidate_rank": 1, "pending_token": "stale"}}),
    ("refinar", {"seed_resolved_params": {"dataset": "procesos", "objeto": ["mantenimiento"]}}),
])
def test_structured_controls_keep_their_precedence_with_zero_calls(frame_on, make_flow, raw, kwargs):
    off, alpha = make_flow("off"), make_flow("alpha")
    frame_on()
    before = run(off, raw, chat_id="fast-off", **kwargs)
    after = run(alpha, raw, chat_id="fast-on", **kwargs)
    assert after["route_reason"] == before["route_reason"]
    alpha.natural_alpha_provider.observe.assert_not_called()
