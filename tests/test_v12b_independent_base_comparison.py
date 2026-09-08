"""Identical observational probes runnable on BASE and candidate (no B imports)."""
import json

import pytest

from app.config import Settings, get_settings
from app.core.orchestrator import SecopalWorkflow
from test_entity_profile_workflow import WorkflowClient


@pytest.mark.parametrize('sequence', [
    ['analiza contratos de alcaldia de baranoa en agosto', 'mas', 'proveedores'],
    ['analiza contratos publicados en agosto de alcaldia de baranoa', 'analiza contratos firmados en agosto de alcaldia de baranoa'],
    ['analiza contratos publicados en agosto de alcaldia de baranoa', 'mis radares'],
    ['analiza contratos publicados en agosto de alcaldia de baranoa', 'CO1.PCCNTR.12345'],
    ['contratos de obra en baranoa', 'analiza contratos de alcaldia de baranoa en agosto', 'guardar esta búsqueda como Radar Sintetico'],
])
def test_observe_same_product_sequence_on_base_and_candidate(monkeypatch, sequence):
    from app.core.workflow.handlers import radar

    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', 'true')
    effects = []

    def save(store, **kwargs):
        effects.append(kwargs['params'])
        return 'synthetic-only'

    monkeypatch.setattr(radar, 'save_radar_from_params', save)
    observations = []
    for query in sequence:
        get_settings.cache_clear()
        workflow = SecopalWorkflow(Settings())
        client = WorkflowClient()
        workflow.secop_client = client
        workflow.narrator.narrate_with_grounding = lambda **kwargs: None
        result = workflow.run_query(query, chat_id='ind-base-comparison')
        observations.append({'query': query, 'route': result['route_reason'], 'response': result['response'],
                             'calls': len(client.calls), 'published': result.get('continuation_published')})
    print('BASE_COMPARISON', json.dumps({'turns': observations, 'save_effects': effects}, ensure_ascii=False))
    assert len(observations) == len(sequence)
