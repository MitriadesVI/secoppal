"""Run ONLY inside frozen BASE; serialize real historical structures."""
import json
import os
from datetime import date
from pathlib import Path


def test_generate_base_artifacts(tmp_path, monkeypatch):
    from app.core.conversation_store import ConversationStore
    from app.core.pagination_buffer import PaginationBuffer, pagination_fingerprint
    from app.core.radar.builder import save_radar_from_params
    from app.core.radar.store import RadarStore
    from app.core.entity_profile.record_validation import snapshot
    from entity_profile_frame_harness import observe
    from tests.buyer_actor_independent_support import workflow, index_at, attach_source
    from app.core.entity_profile.frame_turn import run_profile_turn
    from app.core.entity_profile.profile_state import ProfileConversationState
    params = dict(dataset='contratos', entidad_resolved='ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS',
                  valor_min=500_000_000)
    store = ConversationStore(tmp_path / 'conversations')
    store.append_turn(chat_id='audit', user_query='contratos de alcaldia de cartagena por mas de 500 millones',
                      response='Historical BASE search', parsed_params=params, result_ids=['BASE-ROW'])
    buffer = PaginationBuffer.create(query_fingerprint=pagination_fingerprint(params), group='contratos',
                                      dataset_id='jbjy-vk9h', query_params=params,
                                      candidate_buffer=[{'id_contrato': 'BASE-ROW'}],
                                      validated_ranked_buffer=[{'id_contrato': 'BASE-ROW'}],
                                      display_cursor=1, source_cursor=50, source_candidate_total=63)
    radar_store = RadarStore(tmp_path / 'radar.sqlite')
    radar_id = save_radar_from_params(radar_store, owner='independent', name='BASE', params=params)
    frame = observe('analiza contratos de alcaldia de cartagena por mas de 500 millones')
    assert frame.ready
    assert frame.frame.schema_version == 1
    flow = workflow(tmp_path / 'execution', monkeypatch, index_at(tmp_path, []), True)
    conn = attach_source(flow, monkeypatch)
    text = 'analiza contratos de alcaldia de cartagena por mas de 500 millones'
    try:
        turn = run_profile_turn(user_query=text, params=flow.query_router.parse(text).params,
                                state=ProfileConversationState(), turn_id='BASE',
                                soql_builder=flow.soql_builder, secop_client=flow.secop_client,
                                today=date(2026, 9, 8))
        assert turn.answered and turn.record and not flow.audit_trace['errors']
    finally:
        conn.close()
    output = dict(base='c52d7eda355f44278164d4cb9803a199eec83486', params=params,
                  conversation=(tmp_path / 'conversations/audit.jsonl').read_text(),
                  buffer=buffer.to_dict(), frame=frame.frame.to_dict(), plan=snapshot(frame.decision.plan),
                  execution_record=turn.record.to_dict(), execution_source=flow.audit_trace,
                  radar_query_frame_json=radar_store.get_radar(radar_id).query_frame_json)
    Path(os.environ['CU_BASE_FIXTURE']).write_text(json.dumps(output, ensure_ascii=False, indent=2) + '\n')
