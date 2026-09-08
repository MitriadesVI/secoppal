"""Bounded no-promotion model checked against real files and complete workflows."""
from itertools import product
import json
from pathlib import Path
import tempfile

from hypothesis import given, settings, strategies as st
import pytest
from audit_output import emit
from file_faults import file_fault
from publication_port import process_read

from app.core.conversation_store import ConversationStore, StaleRevisionError
from app.core.workflow.continuation import ContinuationContract
from app.core.workflow.publication_io import barrier_path
from app.core.workflow.store_epoch import read_epoch
from app.core.workflow.tabular_capability import export_params
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_v12b_independent_boundaries import Pages


FAILURES = ('partial', 'complete', 'fsync', 'before_unlink', 'after_unlink', 'rearm')


def check_sequence(store, sequence, *, external=False):
    chat_id = 'bounded'
    first = store.publish_turn(store.reserve_turn(chat_id), user_query='A', response='A')
    target = store._envelope_path_for(chat_id)
    confirmed_bytes = target.read_bytes()
    failed = set()
    for i, kind in enumerate(sequence):
        reservation = store.reserve_turn(chat_id)
        failed.add(reservation.result_id)
        with pytest.MonkeyPatch.context() as patch, file_fault(patch, target, kind):
            with pytest.raises(OSError):
                store.publish_turn(reservation, user_query=f'FAILED-{i}', response='failed')
        read = ConversationStore(store.base_path).load_envelope(chat_id)
        assert not read.usable or read.envelope.result_id == first.result_id
        assert target.read_bytes().startswith(confirmed_bytes)
        if external:
            observed = process_read(store, chat_id)
            assert not observed['usable'] or observed['envelope']['result_id'] == first.result_id
    valid = store.publish_turn(store.reserve_turn(chat_id), user_query='NEW-EXPLICIT', response='new')
    read = ConversationStore(store.base_path).load_envelope(chat_id)
    assert read.usable and read.envelope == valid and read.envelope.result_id not in failed
    assert target.read_bytes().startswith(confirmed_bytes)
    stale = store.reserve_turn(chat_id)
    epoch = read_epoch(target)
    store.clear(chat_id)
    assert read_epoch(target) == epoch + 1
    with pytest.raises(StaleRevisionError):
        store.publish_turn(stale, user_query='stale', response='stale')
    reset = store.publish_turn(store.reserve_turn(chat_id), user_query='RESET', response='reset')
    assert store.load_envelope(chat_id).envelope == reset


@pytest.mark.parametrize('sequence', list(product(FAILURES, repeat=3)))
def test_three_failures_exhaustive(sequence, tmp_path):
    check_sequence(ConversationStore(tmp_path / 'bounded'), sequence)


@settings(max_examples=40, derandomize=True, deadline=None)
@given(st.lists(st.sampled_from(FAILURES), min_size=2, max_size=5))
def test_no_promotion_property(sequence):
    # A fresh directory per example; model oracle is only A/blocked, while all
    # writes, reads, serialization, CAS and reset are the actual implementation.
    with tempfile.TemporaryDirectory(prefix='b3-property-') as directory:
        check_sequence(ConversationStore(Path(directory)), sequence)


@pytest.mark.parametrize('profile', [False, True])
@pytest.mark.parametrize('prefix_fault', ['partial', 'complete', 'fsync', 'after_unlink'])
def test_integrated_three_failures_reload_capabilities_and_recovery(frame_v12, monkeypatch, profile, prefix_fault):
    chat = Conversation('b3-integrated-chain', client_factory=Pages)
    other = Conversation('b3-integrated-other', client_factory=Pages)
    query = 'analiza contratos de alcaldia de baranoa en agosto' if profile else 'contratos de obra en baranoa'
    bad_query = 'analiza contratos de alcaldia de galapa en julio' if profile else 'procesos de alimentacion en bogota'
    first = chat.say(query)
    old = chat.published().envelope
    store = ConversationStore()
    target = store._envelope_path_for(chat.chat_id)
    confirmed_bytes = target.read_bytes()
    failed_raws = []

    def other_publish():
        assert other.say('procesos de alimentacion en bogota').result['continuation_published']

    for kind in (prefix_fault, 'rearm', 'rearm'):
        with file_fault(monkeypatch, target, kind, other=other_publish) as (reached, visible):
            result = chat.say(bad_query)
        assert result.result['continuation_published'] is False
        assert 'No pude guardar el contexto' in result.response
        failed_raws.extend(visible)
        read = process_read(store, chat.chat_id)
        assert not read['usable'] and not read['history']
        assert store.get_history(chat.chat_id) == []
        assert not store._pagination_path_for(chat.chat_id).exists()
        assert target.read_bytes().startswith(confirmed_bytes)
        for raw in failed_raws:
            contract = ContinuationContract.from_dict(raw['continuation'])
            assert export_params(store, contract, chat.chat_id) is None
        emit('B3_CHAIN', profile, kind, reached, read)
    assert other.say('más').result['dataset_id'] == 'p6dx-8zbt'
    # Check inert orphan generations before a new valid turn can create one.
    generations = set((store.base_path / '.generations' / chat.chat_id).iterdir())
    assert len(generations) >= 4
    blocked = chat.say('proveedores' if profile else 'más')
    assert blocked.clarifies and not blocked.calls
    # A new explicit profile can succeed without reset and must use its own
    # state. Tabular invalid admission remains conservative (F5); reset below.
    if profile:
        valid = chat.say(query)
        assert valid.result['continuation_published'] and not valid.clarifies
        after = process_read(store, chat.chat_id)
        assert after['envelope']['result_id'] == valid.result['result_id']
        assert not after['history']
        follow = chat.say('proveedores')
        assert len(follow.calls) == 2 and all('BARANOA' in sql and '2026-08' in sql for sql in follow.sql)
        assert all('GALAPA' not in sql and '2026-07' not in sql for sql in follow.sql)
    epoch = read_epoch(target)
    with file_fault(monkeypatch, target, 'fsync'):
        assert chat.say(bad_query).result['continuation_published'] is False
    old_barrier = barrier_path(target)
    chat.say('/reset')
    assert read_epoch(target) == epoch + 1 and old_barrier.exists()
    assert generations.issubset(set((store.base_path / '.generations' / chat.chat_id).iterdir()))
    valid = chat.say(query)
    assert valid.result['continuation_published'] and valid.result['result_id'] != old.result_id
    follow = chat.say('proveedores' if profile else 'más')
    assert not follow.clarifies
    if profile:
        assert follow.calls and all('BARANOA' in sql for sql in follow.sql)
    else:
        assert follow.result['rows'] and follow.result['dataset_id'] == 'jbjy-vk9h'
        exported = export_params(store, chat.contract(), chat.chat_id)
        assert exported and 'alimentacion' not in json.dumps(exported)
    emit('B3_RECOVERY', profile, first.response, valid.response, follow.response)


@pytest.mark.parametrize('sequence', [('fsync', 'rearm', 'rearm'), ('partial', 'rearm', 'rearm')])
def test_new_process_at_every_critical_boundary(sequence, tmp_path):
    check_sequence(ConversationStore(tmp_path / 'process-sequence'), sequence, external=True)
