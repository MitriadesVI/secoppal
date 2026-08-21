"""Transport-neutral presentation model for Actor Profile candidates.

The model includes full user-visible identity text, but its selection payload
contains only the canonical rank and opaque pending-list token.  A UI therefore
cannot turn a displayed document into backend authority.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from app.core.actor_identity import IdentityKey
from app.core.actor_name import ActorResolutionStatus, format_actor_identity_for_user

__all__ = ["ActorCandidateOption", "actor_candidate_options"]


@dataclass(frozen=True, slots=True)
class ActorCandidateOption:
    rank: int
    observed_name: str
    identity_text: str
    pending_token: str

    def as_dict(self) -> dict:
        return asdict(self)


def actor_candidate_options(params: dict) -> tuple[ActorCandidateOption, ...]:
    """Project the current backend order into safe, clickable UI options."""
    pending = params.get("actor_pending")
    if not isinstance(pending, dict):
        return ()
    if pending.get("resolution_status") not in {
        ActorResolutionStatus.AMBIGUOUS,
        ActorResolutionStatus.CANDIDATES,
    }:
        return ()
    token = pending.get("selection_token")
    candidates = pending.get("candidates")
    if not isinstance(token, str) or not token or not isinstance(candidates, list):
        return ()

    options: list[ActorCandidateOption] = []
    for candidate in candidates[:10]:
        if not isinstance(candidate, dict):
            continue
        try:
            rank = int(candidate["rank"])
            identity = IdentityKey(
                str(candidate["document_type"]),  # type: ignore[arg-type]
                str(candidate["base_number"]),
            )
        except (KeyError, TypeError, ValueError):
            continue
        if rank < 1:
            continue
        options.append(
            ActorCandidateOption(
                rank=rank,
                observed_name=str(
                    candidate.get("observed_name") or "Nombre observado"
                ).upper(),
                identity_text=format_actor_identity_for_user(identity),
                pending_token=token,
            )
        )
    return tuple(options)
