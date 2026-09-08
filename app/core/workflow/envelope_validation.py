"""Validate the current publication as one object before admitting continuity."""
from app.core.entity_profile.execution_record import ExecutionRecord
from app.core.entity_profile.record_validation import validate_record
from app.core.workflow.continuation import ContinuationOwner, ResultKind


def validate_envelope(envelope, state, contract):
    if contract is None:
        raise ValueError('published continuation absent')
    if (contract.conversation_revision != envelope.revision
            or contract.visible_turn_id != envelope.turn_id
            or contract.result_id != envelope.result_id
            or not envelope.turn_id or not envelope.result_id):
        raise ValueError('foreign publication reference')
    generation = envelope.continuity_generation
    if generation is not None and (not isinstance(generation, str) or not generation.isalnum()):
        raise ValueError('invalid generation')
    if contract.owner is not ContinuationOwner.ENTITY_PROFILE:
        if state.confirmed_frame or state.pending or envelope.execution_record:
            raise ValueError('profile state has foreign owner')
        return
    frame, pending = state.confirmed_frame, state.pending
    if frame:
        if contract.confirmed_profile is None:
            raise ValueError('confirmed execution reference absent')
        record = ExecutionRecord.from_dict(envelope.execution_record)
        validate_record(frame, contract.confirmed_profile, record)
    elif contract.confirmed_profile or envelope.execution_record:
        raise ValueError('execution without confirmed Frame')
    if pending:
        ref = contract.pending_profile
        base = (frame.frame_id, frame.revision) if frame else None
        if (ref is None or ref.pending_ref != pending.pending_id
                or ref.base_frame_ref != pending.base_frame_ref
                or pending.base_frame_ref != base or pending.patch.base_frame_ref != base):
            raise ValueError('foreign pending reference')
    elif contract.pending_profile:
        raise ValueError('reference without pending')
    if contract.result_kind is ResultKind.ENTITY_PROFILE and not frame:
        raise ValueError('executed profile without Frame')
