"""Frame-envelope-only admission for result-bound Entity Profile actions."""
from __future__ import annotations

from dataclasses import dataclass

from app.core.conversation_envelope import EnvelopeRead
from app.core.entity_profile.executive_actions import (
    ExecutiveAction,
    action_for_id,
    actions_for_sources,
)
from app.core.entity_profile.execution_record import ExecutionRecord, ExecutionRecordError
from app.core.entity_profile.frame import EntityProfileFrame
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.entity_profile.record_validation import validate_record
from app.core.workflow.continuation import (
    ContinuationContract,
    ContinuationOwner,
    ResultKind,
)


PROFILE_ACTION_EVENT_VERSION = 1
FRAME_REFERENCE_KIND = "FRAME_ENVELOPE"
_CLARIFICATION = (
    "Esta acción ya no pertenece al perfil vigente. Abre un perfil ejecutivo "
    "actual y vuelve a elegir la opción."
)


@dataclass(frozen=True, slots=True)
class ProfileActionReference:
    chat_id: str
    result_id: str
    conversation_revision: int
    kind: str = FRAME_REFERENCE_KIND


@dataclass(frozen=True, slots=True)
class TrustedProfileAction:
    action: ExecutiveAction
    phrase: str
    reference: ProfileActionReference
    confirmed_frame: EntityProfileFrame
    owner: ContinuationOwner


@dataclass(frozen=True, slots=True)
class ProfileActionAdmission:
    trusted: TrustedProfileAction | None
    reason: str
    response: str = _CLARIFICATION

    @property
    def admitted(self) -> bool:
        return self.trusted is not None


def admit_profile_action(
    event: object,
    *,
    chat_id: str | None,
    read: EnvelopeRead | None,
    state: ProfileConversationState,
    contract: ContinuationContract | None,
) -> ProfileActionAdmission:
    """Trust only an exact current published Frame envelope."""
    parsed = _parse_event(event)
    if parsed is None:
        return _reject("malformed_profile_action")
    action_id, reference = parsed
    action = action_for_id(action_id)
    if action is None:
        return _reject("unknown_profile_action")
    if type(chat_id) is not str or not chat_id or reference.chat_id != chat_id:
        return _reject("foreign_profile_action")
    if read is None or not read.usable:
        return _reject("unpublished_profile_action")
    envelope = read.envelope
    if (
        envelope.chat_id != reference.chat_id
        or envelope.result_id != reference.result_id
        or envelope.revision != reference.conversation_revision
    ):
        return _reject("stale_profile_action")
    frame = state.confirmed_frame
    if state.has_pending or frame is None:
        return _reject("pending_profile_action")
    if (
        contract is None
        or contract.owner is not ContinuationOwner.ENTITY_PROFILE
        or contract.result_kind is not ResultKind.ENTITY_PROFILE
        or contract.confirmed_profile is None
        or contract.result_id != envelope.result_id
        or contract.conversation_revision != envelope.revision
    ):
        return _reject("foreign_profile_owner")
    try:
        record = ExecutionRecord.from_dict(envelope.execution_record)
        validate_record(frame, contract.confirmed_profile, record)
    except (ExecutionRecordError, ValueError, KeyError, TypeError, IndexError):
        return _reject("invalid_profile_publication")
    if record.focus != "overview":
        return _reject("replaced_profile_action")
    sources = {call.dataset_id for call in record.calls}
    allowed = {candidate.action_id for candidate in actions_for_sources(sources)}
    if action.action_id not in allowed:
        return _reject("incompatible_profile_action")
    return ProfileActionAdmission(
        TrustedProfileAction(
            action=action,
            phrase=action.phrase,
            reference=reference,
            confirmed_frame=frame,
            owner=contract.owner,
        ),
        "profile_action",
        response="",
    )


def _parse_event(event: object) -> tuple[str, ProfileActionReference] | None:
    if type(event) is not dict or set(event) != {"schema_version", "action_id", "reference"}:
        return None
    version = event.get("schema_version")
    action_id, raw = event.get("action_id"), event.get("reference")
    if type(version) is not int or version != PROFILE_ACTION_EVENT_VERSION:
        return None
    if type(action_id) is not str or type(raw) is not dict:
        return None
    if set(raw) != {"kind", "chat_id", "result_id", "conversation_revision"}:
        return None
    if raw.get("kind") != FRAME_REFERENCE_KIND:
        return None
    chat_id, result_id, revision = (
        raw.get("chat_id"), raw.get("result_id"), raw.get("conversation_revision"),
    )
    if type(chat_id) is not str or not chat_id or type(result_id) is not str or not result_id:
        return None
    if type(revision) is not int or revision < 1:
        return None
    return action_id, ProfileActionReference(chat_id, result_id, revision)


def _reject(reason: str) -> ProfileActionAdmission:
    return ProfileActionAdmission(None, reason)


__all__ = [
    "FRAME_REFERENCE_KIND",
    "PROFILE_ACTION_EVENT_VERSION",
    "ProfileActionAdmission",
    "ProfileActionReference",
    "TrustedProfileAction",
    "admit_profile_action",
]
