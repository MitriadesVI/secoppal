"""Independent sequential failures at real file operations; no publication mocks."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from audit_output import emit

from app.core.conversation_store import ConversationStore
from app.core.workflow.continuation import ContinuationContract
from app.core.workflow.publication_io import barrier_path, PublicationRecoveryError
from app.core.workflow.tabular_capability import export_params
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_v12b_independent_boundaries import Pages


def process_read(store, chat_id):
    code = ('import json,sys; from app.core.conversation_store import ConversationStore; '
            's=ConversationStore(sys.argv[1]); r=s.load_envelope(sys.argv[2]); '
            'print(json.dumps(dict(usable=r.usable, envelope=r.envelope.to_dict() if r.usable else None, '
            'history=[t.to_dict() for t in s.get_history(sys.argv[2])])) )')
    return json.loads(subprocess.check_output([sys.executable, '-c', code, str(store.base_path), chat_id], text=True))


def fail_envelope_fsync(chat, query, monkeypatch):
    store = ConversationStore()
    target = store._envelope_path_for(chat.chat_id)
    original = os.fsync
    reached = []

    def fsync(fd):
        if target.exists() and not reached and os.fstat(fd).st_ino == target.stat().st_ino:
            reached.append('complete_envelope_fsync')
            raise OSError('audit: complete append fsync failed')
        return original(fd)

    with monkeypatch.context() as patch:
        patch.setattr(os, 'fsync', fsync)
        failed = chat.say(query)
    assert reached == ['complete_envelope_fsync']
    assert failed.result['continuation_published'] is False
    assert 'No pude guardar el contexto' in failed.response
    assert not store.load_envelope(chat.chat_id).usable
    raw = json.loads(target.read_text().splitlines()[-1])
    return raw


def fail_confirmation_and_rearm(chat, query, monkeypatch, *, rollback='ok', other=None):
    store = ConversationStore()
    target = store._envelope_path_for(chat.chat_id)
    barrier = barrier_path(target)
    original_open, original_unlink = Path.open, Path.unlink
    before = target.read_bytes()
    reached = []

    def unlink(path, *args, **kwargs):
        if path == barrier and not reached:
            original_unlink(path, *args, **kwargs)
            assert not barrier.exists()
            reached.append('unlink_visible')
            if other:
                assert other.say('procesos de alimentacion en bogota').result['continuation_published']
            raise OSError('audit: unlink reported failure after visibility')
        return original_unlink(path, *args, **kwargs)

    def open_path(path, mode='r', *args, **kwargs):
        if path == barrier and mode == 'w' and reached:
            reached.append('rearm_open_failed')
            raise OSError('audit: cannot reopen barrier')
        if path == target and mode == 'r+b':
            reached.append('rollback_open')
            if rollback == 'open_failure':
                raise OSError('audit: cannot open rollback file')
        return original_open(path, mode, *args, **kwargs)

    error, failed = None, None
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'unlink', unlink)
        patch.setattr(Path, 'open', open_path)
        try:
            failed = chat.say(query)
        except PublicationRecoveryError as exc:
            error = exc
    assert reached == ['unlink_visible', 'rearm_open_failed', 'rollback_open']
    if rollback == 'ok':
        assert error is None and failed.result['continuation_published'] is False
        assert target.read_bytes() == before, 'only own append may be removed'
    else:
        assert isinstance(error, PublicationRecoveryError) and failed is None
        assert target.read_bytes().startswith(before) and len(target.read_bytes()) > len(before)
    return failed, error


@pytest.mark.parametrize('profile', [False, True])
@pytest.mark.parametrize('failed_prefix', [False, True])
def test_rollback_of_retry_never_promotes_an_earlier_failed_turn(frame_v12, monkeypatch, profile, failed_prefix):
    chat = Conversation('audit-repeated-fault', client_factory=Pages)
    other = Conversation('audit-other-writer', client_factory=Pages)
    first = chat.say('analiza contratos de alcaldia de baranoa en agosto' if profile else 'contratos de obra en baranoa')
    old = chat.published().envelope
    failed_raw = None
    if failed_prefix:
        failed_raw = fail_envelope_fsync(chat, 'analiza contratos de alcaldia de galapa en julio' if profile
                                         else 'procesos de alimentacion en bogota', monkeypatch)
        blocked = process_read(ConversationStore(), chat.chat_id)
        assert not blocked['usable'] and not blocked['history']
    fail_confirmation_and_rearm(chat, 'analiza contratos de alcaldia de baranoa en 2025' if profile
                               else 'contratos de obra en baranoa', monkeypatch, other=other)
    store = ConversationStore()
    read = store.load_envelope(chat.chat_id)
    external = process_read(store, chat.chat_id)
    history = store.get_history(chat.chat_id)
    exported = export_params(store, ContinuationContract.from_dict(read.envelope.continuation), chat.chat_id) if read.usable else None
    after = chat.say('proveedores' if profile else 'más')
    emit('SEQUENTIAL_FAULT', json.dumps(dict(profile=profile, failed_prefix=failed_prefix,
          first=first.result['result_id'], failed_id=failed_raw['result_id'] if failed_raw else None,
          reload=external, history=[t.to_dict() for t in history], export=exported,
          next_route=after.route, next_calls=after.calls, next_rows=after.result['rows']), ensure_ascii=False))
    assert other.say('más').result['dataset_id'] == 'p6dx-8zbt'
    assert not read.usable or read.envelope.result_id == old.result_id, 'An earlier explicitly unpublished turn became current after rollback of the retry'
    assert not external['usable'] or external['envelope']['result_id'] == old.result_id
    if failed_prefix:
        assert all(t.user_query != failed_raw['user_query'] for t in history)
        assert after.clarifies or all('GALAPA' not in sql and 'alimentacion' not in sql for sql in after.sql)
    else:
        assert not after.clarifies


@pytest.mark.parametrize('rollback', ['ok', 'open_failure'])
def test_recovery_error_observes_physical_state_and_next_turn(frame_v12, monkeypatch, rollback):
    """Observation of declared multi-failure limit, not a passing safety guarantee."""
    chat = Conversation('audit-recovery-error')
    first = chat.say('analiza contratos de alcaldia de baranoa en agosto')
    fail_confirmation_and_rearm(chat, 'analiza contratos de alcaldia de galapa en julio', monkeypatch, rollback=rollback)
    store = ConversationStore()
    read = process_read(store, chat.chat_id)
    next_turn = chat.say('proveedores')
    emit('RECOVERY_ERROR_OBSERVATION', rollback, read, next_turn.response, next_turn.calls)
    assert read['usable']
    if rollback == 'ok':
        assert read['envelope']['result_id'] == first.result['result_id']
        assert all('BARANOA' in sql for sql in next_turn.sql)
    else:
        assert read['envelope']['user_query'] == 'analiza contratos de alcaldia de galapa en julio'
        assert all('GALAPA' in sql and '2026-07' in sql for sql in next_turn.sql)
    assert len(next_turn.calls) == 2


@pytest.mark.parametrize('profile', [False, True])
@pytest.mark.parametrize('failure', ['before_barrier_visible', 'complete_envelope'])
def test_explicit_valid_retry_and_reset_never_copy_failed_history(frame_v12, monkeypatch, profile, failure):
    chat = Conversation('audit-valid-recovery', client_factory=Pages)
    first_query = 'analiza contratos de alcaldia de baranoa en agosto' if profile else 'contratos de obra en baranoa'
    bad_query = 'analiza contratos de alcaldia de galapa en julio' if profile else 'procesos de alimentacion en bogota'
    first = chat.say(first_query)
    store = ConversationStore()
    barrier = barrier_path(store._envelope_path_for(chat.chat_id))
    if failure == 'complete_envelope':
        fail_envelope_fsync(chat, bad_query, monkeypatch)
        assert store.get_history(chat.chat_id) == []
        assert not store._pagination_path_for(chat.chat_id).exists()
    else:
        original = Path.open
        reached = []

        def open_path(path, mode='r', *args, **kwargs):
            if path == barrier and mode == 'w':
                reached.append('arm_before_visible')
                raise OSError('audit: barrier creation failed before visible')
            return original(path, mode, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(Path, 'open', open_path)
            failed = chat.say(bad_query)
        assert reached == ['arm_before_visible'] and not failed.result['continuation_published']
        assert process_read(store, chat.chat_id)['envelope']['result_id'] == first.result['result_id']
    valid = chat.say(first_query)
    if not profile and failure == 'complete_envelope':
        # Existing invalid-publication admission blocks tabular continuation; reset is supported.
        assert valid.clarifies and not valid.calls
        chat.say('/reset')
        valid = chat.say(first_query)
    assert not valid.clarifies
    assert valid.result['continuation_published']
    external = process_read(store, chat.chat_id)
    assert external['envelope']['result_id'] == valid.result['result_id']
    assert all(t['user_query'] != bad_query for t in external['history'])
    later = chat.say('proveedores' if profile else 'más')
    assert not later.clarifies
    if profile:
        assert len(later.calls) == 2 and all('BARANOA' in sql for sql in later.sql)
    else:
        assert later.result['rows'] and later.result['dataset_id'] == 'jbjy-vk9h'
    fail_envelope_fsync(chat, bad_query, monkeypatch)
    old_barrier = barrier_path(store._envelope_path_for(chat.chat_id))
    assert old_barrier.exists()
    chat.say('/reset')
    assert old_barrier.exists(), 'this is an old-epoch barrier, not a deletion-based test'
    assert chat.say(first_query).result['continuation_published']
    assert not chat.say('proveedores' if profile else 'más').clarifies
