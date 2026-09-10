import json

from app.core.entity_profile.executive_actions import (
    action_for_id,
    actions_for_sources,
    presented_action_ids,
    project_actions,
)
from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET


def _ids(sources):
    return tuple(action.action_id for action in actions_for_sources(sources))


def test_actions_follow_the_accredited_source_scope_exactly():
    assert _ids({PROCESS_DATASET, CONTRACT_DATASET}) == (
        "suppliers", "modalities", "temporal", "recent_contracts",
    )
    assert _ids({CONTRACT_DATASET}) == ("suppliers", "temporal", "recent_contracts")
    assert _ids({PROCESS_DATASET}) == ("modalities", "temporal")
    assert _ids(set()) == ()


def test_catalogue_owns_exact_human_phrases_and_json_projection():
    expected = {
        "suppliers": "Muéstrame sus principales proveedores",
        "modalities": "Muéstrame sus modalidades",
        "temporal": "Muéstrame su evolución",
        "recent_contracts": "Muéstrame sus contratos recientes",
    }
    for action_id, phrase in expected.items():
        assert action_for_id(action_id).phrase == phrase
    projected = project_actions({PROCESS_DATASET, CONTRACT_DATASET})
    assert json.loads(json.dumps(projected, ensure_ascii=False)) == projected
    assert presented_action_ids(projected) == tuple(expected)


def test_unknown_or_malformed_presentation_hides_every_control():
    assert action_for_id("SUPPLIERS") is None
    assert presented_action_ids([{"id": "suppliers", "label": "Otro"}]) == ()
    assert presented_action_ids([{"id": "suppliers", "label": "Proveedores", "x": 1}]) == ()
