"""Reuse existing command and profile detectors; pending does not own commands."""
from app.core.direct_lookup import detect_direct_lookup
from app.core.radar.commands import parse_radar_command
from app.core.entity_profile.turn import detect_entity_profile_turn, PROFILE_ACTIVE_PARAM


def external_owner(query, params, actor_selection=None):
    if actor_selection:
        return 'ACTOR'
    if parse_radar_command(query):
        return 'RADAR'
    if detect_direct_lookup(query):
        return 'NONE'
    if params.get('actor_scope') or params.get('actor_pending'):
        return 'ACTOR'
    profile, _ = detect_entity_profile_turn(query, {**params, PROFILE_ACTIVE_PARAM: True})
    if not profile and params.get('dataset_explicit') and params.get('objeto') and any(params.get(key) for key in (
            'ciudad', 'ciudad_resolved', 'departamento', 'departamento_resolved',
            'entidad', 'entidad_resolved')):
        return 'TABULAR'
    return None
