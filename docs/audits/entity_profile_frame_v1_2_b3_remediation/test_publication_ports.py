"""Ports keep historical functional assertions; only the recovery suffix changes."""
import pytest
import test_publication_sequences as historical
from entity_profile_frame_b_harness import frame_v12  # noqa: F401
from publication_port import fail_confirmation_and_rearm


@pytest.mark.parametrize('profile', [False, True])
@pytest.mark.parametrize('failed_prefix', [False, True])
def test_failed_prefix_port(frame_v12, monkeypatch, profile, failed_prefix):
    with monkeypatch.context() as patch:
        patch.setattr(historical, 'fail_confirmation_and_rearm', fail_confirmation_and_rearm)
        historical.test_rollback_of_retry_never_promotes_an_earlier_failed_turn(
            frame_v12, monkeypatch, profile, failed_prefix,
        )
