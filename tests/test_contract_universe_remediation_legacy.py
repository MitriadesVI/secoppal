"""Read artifacts emitted by the real frozen BASE serializers, never strip keys."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.buyer_actor_independent_support import SEEDS, say
from tests.contract_universe_independent_support import capture, setup


@pytest.fixture
def historical():
    raw = Path('docs/audits/contract_universe_v1_1_remediation/v1_serialized.json').read_text()
    return json.loads(raw)


def test_real_base_conversation_requires_review(tmp_path, monkeypatch, historical):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3])
    path = tmp_path / 'conversations/audit.jsonl'
    path.write_text(historical['conversation'])
    original = path.read_bytes()
    try:
        result = say(flow, 'solo 2026')
        capture(flow, 'BASE-conversation', result)
        assert result['needs_clarification']
        assert not flow.audit_trace['sql']
        assert path.read_bytes().startswith(original)
        fresh = say(flow, SEEDS[0][0])
        assert fresh['total_count'] == 22
        assert path.read_bytes().startswith(original)
    finally:
        conn.close()


def test_real_base_buffer_requires_review(historical):
    from app.core.pagination_buffer import PaginationBuffer
    from app.core.workflow.tabular_capability import capability, validate_tabular
    raw = historical['buffer']
    buffer = PaginationBuffer.from_dict(raw)
    assert buffer.to_dict() == raw
    store = SimpleNamespace(_pagination_path_for=lambda chat: 'unused',
                            _read_pagination_payload=lambda path: {'contratos': raw})
    reservation = SimpleNamespace(chat_id='audit', previous=SimpleNamespace(envelope=SimpleNamespace(continuity_generation='g')))
    contract = SimpleNamespace(tabular_groups={'contratos': capability(buffer, 'g')})
    with pytest.raises(ValueError, match='universe'):
        validate_tabular(store, reservation, contract, 'más')


def test_real_base_frame_and_authorized_plan_require_review(historical):
    from app.core.entity_profile.frame import EntityProfileFrame
    from app.core.entity_profile.frame_compiler import CATALOG_VERSION
    assert historical['frame']['schema_version'] == 2
    with pytest.raises(ValueError, match='revisión'):
        EntityProfileFrame.from_dict(historical['frame'])
    assert historical['plan']['catalog_version'] != CATALOG_VERSION
    assert historical['plan']['jobs']
    assert all('upper(unaccent(trim(estado_contrato)))' in j['sql'] for j in historical['plan']['jobs'])
    from app.core.entity_profile.execution_record import ExecutionRecord
    from app.core.entity_profile.record_validation import validate_record
    record = ExecutionRecord.from_dict(historical['execution_record'])
    assert record.to_dict() == historical['execution_record']
    # Catalog rejection precedes dereferencing any frame or continuation ref.
    with pytest.raises(ValueError, match='incompatible execution catalog'):
        validate_record(None, None, record)


def test_real_base_radar_preserved_and_blocked(tmp_path, monkeypatch, historical):
    from app.core.radar.store import RadarStore
    from app.core.radar.runner import run_radar
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3])
    store = RadarStore(tmp_path / 'old-radar.sqlite')
    raw = historical['radar_query_frame_json']
    rid = store.create_radar(name='BASE', owner='independent', query_frame_json=raw,
                             dataset='contratos', channel='web', frequency='manual')
    try:
        result = run_radar(flow, store, store.get_radar(rid))
        assert result.reason == 'contract_universe_review_required'
        assert not flow.audit_trace['sql']
        assert store.get_radar(rid).query_frame_json == raw
    finally:
        conn.close()


def test_v1_export_requires_review(historical):
    from app.core.export_xlsx import build_results_xlsx
    before = json.dumps(historical, sort_keys=True)
    with pytest.raises(ValueError, match='revisión'):
        build_results_xlsx(rows=[], query='histórico', resolved_params=historical['params'],
                           dataset_id='jbjy-vk9h', total_count=0)
    assert json.dumps(historical, sort_keys=True) == before


def test_main_base_conversation_adjudicated(tmp_path, monkeypatch):
    historical = json.loads(Path('docs/audits/contract_universe_v1_independent/base_serialized.json').read_text())
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3])
    path = tmp_path / 'conversations/audit.jsonl'
    path.write_text(historical['conversation'])
    original = path.read_bytes()
    try:
        result = say(flow, 'solo 2026')
        capture(flow, 'BASE-conversation', result)
        assert result['needs_clarification']
        assert not flow.audit_trace['sql']
        assert path.read_bytes().startswith(original)
        fresh = say(flow, SEEDS[0][0])
        assert fresh['total_count'] == 22
        assert path.read_bytes().startswith(original)
    finally:
        conn.close()
