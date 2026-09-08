"""Reuse existing command and profile detectors; pending does not own commands."""
from app.core.direct_lookup import detect_direct_lookup
from app.core.actor_arbitration import is_complete_buyer_search
from app.core.radar.commands import parse_radar_command
from app.core.entity_profile.turn import detect_entity_profile_turn


def external_owner(query, params, actor_selection=None):
    if actor_selection:
        return 'ACTOR'
    if parse_radar_command(query):
        return 'RADAR'
    if detect_direct_lookup(query):
        return 'NONE'
    if params.get('actor_scope') or params.get('actor_pending'):
        return 'ACTOR'
    profile, _ = detect_entity_profile_turn(query, params)
    from app.core.query_frame import frame_from_params, is_complete_new_search
    from app.core._followup_constants import NEW_SEARCH_VERBS
    from app.core.followup_engine import TextNormalizer

    normalized = TextNormalizer.strip_noise(query)
    explicit_search = any(normalized.startswith(verb) and normalized[len(verb):].strip()
                          for verb in NEW_SEARCH_VERBS)
    if not profile and is_complete_buyer_search(query, params):
        return 'TABULAR'
    if not profile and (is_complete_new_search(frame_from_params(params)) or explicit_search):
        return 'TABULAR'
    return None
