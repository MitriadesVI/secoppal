"""Independent persistence/version checks against a REAL V1 artifact.

docs/audits/contract_universe_v1_1_independent/v1_independent_artifacts.json was
emitted by running the V1 candidate's own serializers (067b3977) in a throwaway
worktree, not by stripping keys off a V1.1 dict.
"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.buyer_actor_independent_support import SEEDS, say
from tests.contract_universe_independent_support import setup

FIXTURE = Path('docs/audits/contract_universe_v1_1_independent/v1_independent_artifacts.json')


@pytest.fixture(scope='module')
def v1():
    return json.loads(FIXTURE.read_text())


def test_the_artifact_really_comes_from_v1(v1):
    assert v1['candidate'] == '067b39770592640b5aef3898cbc99d8f82c59e0f'
    assert v1['policy_version'] == 'contract-universe-1'
    assert v1['catalog_version'] == 'entity-profile-frame-2-contract-universe-1'
    assert v1['params']['contract_universe_version'] == 'contract-universe-1'


def test_v11_versions_moved(v1):
    from app.core.contract_universe import POLICY_VERSION
    from app.core.entity_profile.frame_compiler import CATALOG_VERSION
    assert POLICY_VERSION == 'contract-universe-1.1'
    assert CATALOG_VERSION == 'entity-profile-frame-2-contract-universe-1.1'
    assert POLICY_VERSION != v1['policy_version']
    assert CATALOG_VERSION != v1['catalog_version']


def test_v1_params_are_not_silently_reinterpreted(v1):
    from app.core.contract_universe import compatible, universe_from_params
    assert compatible(v1['params']) is False
    with pytest.raises(ValueError, match='revisión'):
        universe_from_params(dict(v1['params']))


def test_v1_frame_is_rejected_at_construction_not_silently_migrated(v1):
    """A V1 frame cannot even be deserialized: the universe version stops it."""
    from app.core.entity_profile.frame import EntityProfileFrame
    assert v1['frame']['schema_version'] == 2
    with pytest.raises(ValueError, match='revisión'):
        EntityProfileFrame.from_dict(v1['frame'])


def test_the_only_frame_incompatibility_is_the_universe_version(v1):
    """Schema 2 itself is still valid: field names and types did not change."""
    import copy
    from app.core.entity_profile.frame import EntityProfileFrame
    repinned = copy.deepcopy(v1['frame'])
    for predicates in repinned['source_predicates'].values():
        if predicates.get('contract_universe_version') == 'contract-universe-1':
            predicates['contract_universe_version'] = 'contract-universe-1.1'
    frame = EntityProfileFrame.from_dict(repinned)
    assert frame.to_dict() == repinned          # same fields, same types, round-trip
    assert frame.schema_version == 2
    assert frame.fingerprint()
    assert set(repinned.keys()) == set(v1['frame'].keys())


def test_v1_plan_and_execution_record_require_review(v1):
    from app.core.entity_profile.frame_compiler import CATALOG_VERSION
    from app.core.entity_profile.execution_record import ExecutionRecord
    from app.core.entity_profile.record_validation import validate_record
    assert v1['plan']['catalog_version'] != CATALOG_VERSION
    record = ExecutionRecord.from_dict(v1['execution_record'])
    assert record.to_dict() == v1['execution_record']   # bytes preserved
    with pytest.raises(ValueError, match='incompatible execution catalog'):
        validate_record(None, None, record)


def test_v1_buffer_format_is_preserved_and_continuation_is_blocked(v1):
    from app.core.pagination_buffer import PaginationBuffer
    from app.core.workflow.tabular_capability import capability, validate_tabular
    raw = v1['buffer']
    buffer = PaginationBuffer.from_dict(raw)
    assert buffer.to_dict() == raw          # container format unchanged
    store = SimpleNamespace(_pagination_path_for=lambda chat: 'unused',
                            _read_pagination_payload=lambda path: {'contratos': raw})
    reservation = SimpleNamespace(chat_id='audit', previous=SimpleNamespace(
        envelope=SimpleNamespace(continuity_generation='g')))
    contract = SimpleNamespace(tabular_groups={'contratos': capability(buffer, 'g')})
    with pytest.raises(ValueError, match='universe'):
        validate_tabular(store, reservation, contract, 'más')


def test_v1_radar_is_preserved_and_blocked(tmp_path, monkeypatch, v1):
    from app.core.radar.store import RadarStore
    from app.core.radar.runner import run_radar
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3])
    store = RadarStore(tmp_path / 'v1-radar.sqlite')
    raw = v1['radar_query_frame_json']
    rid = store.create_radar(name='V1', owner='independent', query_frame_json=raw,
                             dataset='contratos', channel='web', frequency='manual')
    try:
        result = run_radar(flow, store, store.get_radar(rid))
        assert result.reason == 'contract_universe_review_required'
        assert not flow.audit_trace['sql']
        assert store.get_radar(rid).query_frame_json == raw
    finally:
        conn.close()


def test_v1_conversation_is_preserved_and_a_fresh_search_still_works(tmp_path, monkeypatch, v1):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3])
    path = tmp_path / 'conversations/audit.jsonl'
    path.write_text(v1['conversation'])
    original = path.read_bytes()
    try:
        follow = say(flow, 'solo 2026')
        assert follow['needs_clarification']
        assert not flow.audit_trace['sql']
        assert path.read_bytes().startswith(original)
        fresh = say(flow, SEEDS[0][0])
        assert fresh['total_count'] == 22
        assert fresh['resolved_params']['contract_universe_version'] == 'contract-universe-1.1'
        assert path.read_bytes().startswith(original)
    finally:
        conn.close()


def test_v1_export_is_blocked_but_the_file_is_not_rewritten(v1):
    from app.core.export_xlsx import build_results_xlsx
    with pytest.raises(ValueError):
        build_results_xlsx(rows=[{'id_contrato': 'V1-ROW'}], query='exportar',
                           resolved_params=dict(v1['params']), dataset_id='jbjy-vk9h',
                           total_count=1)


def test_universe_change_moves_the_frame_fingerprint(tmp_path, monkeypatch):
    from dataclasses import replace
    from datetime import date
    from app.core.entity_profile.frame import EntityProfileFrame
    from app.core.entity_profile.frame_turn import run_profile_turn
    from app.core.entity_profile.frame_values import SourceKind
    from app.core.entity_profile.profile_state import ProfileConversationState
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3])
    try:
        text = 'analiza contratos de alcaldia de cartagena por mas de 500 millones'
        turn = run_profile_turn(user_query=text, params=flow.query_router.parse(text).params,
                                state=ProfileConversationState(), turn_id='independent',
                                soql_builder=flow.soql_builder, secop_client=flow.secop_client,
                                today=date(2026, 9, 8))
        frame = EntityProfileFrame.from_dict(turn.frame.to_dict())
        assert frame.fingerprint() == turn.frame.fingerprint()
        changed = replace(frame, source_predicates={
            **frame.source_predicates,
            SourceKind.CONTRACTS: replace(frame.predicates_for(SourceKind.CONTRACTS),
                                          contract_universe='source_records')})
        assert changed.fingerprint() != frame.fingerprint()
    finally:
        conn.close()
