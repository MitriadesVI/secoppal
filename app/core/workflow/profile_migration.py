"""Activation uses the existing equivalence checker before legacy can execute."""
from app.core.entity_profile.frame_migration import migrate_legacy_context, MigrationStatus
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.entity_profile.turn import PROFILE_ACTIVE_PARAM, PROFILE_REQUEST_PARAM


def migrate_current_legacy(store, chat_id):
    history = store.get_history(chat_id, last_n=1) if chat_id else []
    if not history:
        return None
    params = history[0].parsed_params
    if not params.get(PROFILE_ACTIVE_PARAM):
        return None
    request = params.get(PROFILE_REQUEST_PARAM)
    state = params.get('_entity_profile_state') or {}
    if state.get('pending') or (state.get('confirmed') and state['confirmed'] != request):
        return ProfileConversationState(), 'legacy pending or inconsistent confirmed request'
    migrated = migrate_legacy_context(request, params)
    if migrated.status is not MigrationStatus.MIGRATED:
        return ProfileConversationState(), migrated.reason or migrated.status.value
    return ProfileConversationState(confirmed_frame=migrated.frame), None
