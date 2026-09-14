"""Private Alpha terminal receipt; publish NONE without returning a reference."""
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.workflow.publication_io import PublicationRecoveryError
from app.core.workflow.turn_publication import delegated_contract, publish

_SIGNAL_KEY = "_natural_alpha_terminal_publication"
_SIGNAL = object()
_ROUTE = "natural_alpha_multi_job_stepwise"


class NaturalAlphaPublicationError(RuntimeError):
    """The prior continuation could not be replaced; no successful Alpha reply."""


def mark_terminal_publication(result: dict) -> None:
    result[_SIGNAL_KEY] = _SIGNAL


def publish_if_terminal(deps, ctx, reservation, result: dict) -> bool:
    marker = result.pop(_SIGNAL_KEY, None)
    if marker is not _SIGNAL or result.get("route_reason") != _ROUTE:
        return False
    if reservation is None:
        return True
    # publish() attaches identifiers to its result argument. Keep that working
    # copy private while the existing envelope replaces the previous owner.
    scratch = dict(result)
    scratch["_continuation_owner"] = "NONE"
    try:
        published = publish(
            deps, reservation, scratch, user_query=ctx.user_query,
            profile_state=ProfileConversationState(),
            contract=delegated_contract(reservation, scratch, deps.conv_store),
            execution_record=None,
        )
    except PublicationRecoveryError as exc:
        raise NaturalAlphaPublicationError("NATURAL_ALPHA_TERMINAL_PUBLICATION_FAILED") from exc
    if not published.published:
        raise NaturalAlphaPublicationError("NATURAL_ALPHA_TERMINAL_PUBLICATION_FAILED")
    return True
