"""B2-I-F4: no later failed attempt may promote an earlier unpublished record."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from app.core.conversation_store import ConversationStore
from app.core.workflow.publication_io import barrier_path
from app.core.workflow.store_epoch import advance_epoch, read_epoch
from storage_faults import fail_after_visible_unlink, publish

CHAT = 'audit-independent-f4'


def leave_barrier(store, chat_id, query, monkeypatch):
    """Fail the append fsync: the durable barrier survives and blocks readers."""
    path = store._envelope_path_for(chat_id)
    original, reached = os.fsync, []

    def fsync(fd):
        if path.exists() and not reached and os.fstat(fd).st_ino == path.stat().st_ino:
            reached.append('append_fsync')
            raise OSError('audit: append fsync failed')
        return original(fd)

    with monkeypatch.context() as patch:
        patch.setattr(os, 'fsync', fsync)
        with pytest.raises(OSError):
            publish(store, chat_id, query)
    assert reached == ['append_fsync']
    assert barrier_path(path).exists(), 'the barrier must survive the failure'
    return path.read_bytes()


def read_in_new_process(store, chat_id):
    code = ('import json,sys;from app.core.conversation_store import ConversationStore;'
            's=ConversationStore(sys.argv[1]);r=s.load_envelope(sys.argv[2]);'
            'print(json.dumps(dict(usable=r.usable,status=r.status.name,'
            'query=r.envelope.user_query if r.usable else None,'
            'revision=r.revision,history=[t.user_query for t in s.get_history(sys.argv[2])])))')
    out = subprocess.check_output([sys.executable, '-c', code, str(store.base_path), chat_id],
                                  text=True, env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'))
    return json.loads(out)


def observe(store, chat_id):
    read = store.load_envelope(chat_id)
    external = read_in_new_process(store, chat_id)
    return dict(usable=read.usable, status=read.status.name,
                query=read.envelope.user_query if read.usable else None,
                history=[t.user_query for t in store.get_history(chat_id)],
                external=external)


# --------------------------------------------------------------------------
# The finding itself: a barrier-blocked prefix followed by a failed retry.
# --------------------------------------------------------------------------

def test_failed_retry_never_promotes_a_barrier_blocked_prefix(monkeypatch):
    store = ConversationStore()
    a = publish(store, CHAT, 'consulta A confirmada')
    leave_barrier(store, CHAT, 'consulta B NO publicada', monkeypatch)
    blocked = observe(store, CHAT)
    assert not blocked['usable'] and not blocked['external']['usable']

    outcome = fail_after_visible_unlink(store, CHAT, 'consulta C fallida', monkeypatch)
    assert outcome['reached'] == ['unlink_visible', 'rearm_failed', 'rollback_open']
    assert outcome['blocked_before'] is True
    assert not barrier_path(store._envelope_path_for(CHAT)).exists(), \
        'no barrier survives: the marker is the only remaining fence'

    after = observe(store, CHAT)
    assert not after['usable'], 'B was promoted to current authority'
    assert not after['external']['usable'], 'another process admits the unpublished B'
    assert after['history'] == [] or all('NO publicada' not in q for q in after['history'])
    assert 'consulta B NO publicada' not in json.dumps(after)
    assert a.user_query == 'consulta A confirmada'


def test_a_confirmed_prefix_keeps_legitimate_continuity(monkeypatch):
    """Positive control: without a blocked prefix nothing is written and A continues."""
    store = ConversationStore()
    publish(store, CHAT, 'consulta A confirmada')
    outcome = fail_after_visible_unlink(store, CHAT, 'consulta B fallida', monkeypatch)
    assert outcome['blocked_before'] is False
    assert outcome['after'] == outcome['before'], 'a confirmed prefix gets no suffix'
    after = observe(store, CHAT)
    assert after['usable'] and after['query'] == 'consulta A confirmada'
    assert after['external']['usable'] and after['external']['query'] == 'consulta A confirmada'


# --------------------------------------------------------------------------
# The marker under further failures, a legitimate publication, and reset.
# --------------------------------------------------------------------------

def test_the_marker_stays_inert_across_further_failures(monkeypatch):
    store = ConversationStore()
    publish(store, CHAT, 'consulta A confirmada')
    leave_barrier(store, CHAT, 'consulta B NO publicada', monkeypatch)
    first = fail_after_visible_unlink(store, CHAT, 'consulta C fallida', monkeypatch)
    marked = first['after']
    for query in ('consulta D fallida', 'consulta E fallida'):
        outcome = fail_after_visible_unlink(store, CHAT, query, monkeypatch)
        assert outcome['blocked_before'] is False, 'no barrier remains after the first repair'
        assert outcome['after'] == marked, 'a later failure must neither drop nor duplicate the marker'
        state = observe(store, CHAT)
        assert not state['usable'] and not state['external']['usable']


def test_a_new_legitimate_publication_recovers_without_reset(monkeypatch):
    store = ConversationStore()
    publish(store, CHAT, 'consulta A confirmada')
    leave_barrier(store, CHAT, 'consulta B NO publicada', monkeypatch)
    fail_after_visible_unlink(store, CHAT, 'consulta C fallida', monkeypatch)
    assert not observe(store, CHAT)['usable']

    published = publish(store, CHAT, 'consulta D nueva y legitima')
    state = observe(store, CHAT)
    assert state['usable'] and state['query'] == 'consulta D nueva y legitima'
    assert state['external']['usable'] and state['external']['query'] == 'consulta D nueva y legitima'
    assert published.user_query == 'consulta D nueva y legitima'
    assert 'consulta B NO publicada' not in json.dumps(state), 'D must not inherit from the failed B'


def test_reset_recovers_and_the_marker_does_not_survive_the_epoch(monkeypatch):
    store = ConversationStore()
    path = store._envelope_path_for(CHAT)
    publish(store, CHAT, 'consulta A confirmada')
    leave_barrier(store, CHAT, 'consulta B NO publicada', monkeypatch)
    fail_after_visible_unlink(store, CHAT, 'consulta C fallida', monkeypatch)
    before_epoch = read_epoch(path)
    advance_epoch(path)
    assert read_epoch(path) == before_epoch + 1
    state = observe(store, CHAT)
    assert not state['usable'], 'the terminal marker is not cleared by the epoch alone'
    published = publish(store, CHAT, 'consulta E tras reset')
    after = observe(store, CHAT)
    assert after['usable'] and after['query'] == 'consulta E tras reset'
    assert published.user_query == 'consulta E tras reset'


# --------------------------------------------------------------------------
# Distinguish the three ways a prefix can be blocked.
# --------------------------------------------------------------------------

def test_a_partial_tail_without_barrier_or_marker_still_blocks(monkeypatch):
    """'No barrier' never means 'confirmed': a truncated tail blocks on its own."""
    store = ConversationStore()
    path = store._envelope_path_for(CHAT)
    publish(store, CHAT, 'consulta A confirmada')
    with path.open('ab') as handle:
        handle.write(b'{"revision": 2, "user_query": "cola parcial"')
    assert not barrier_path(path).exists()
    assert not observe(store, CHAT)['usable']

    outcome = fail_after_visible_unlink(store, CHAT, 'consulta C fallida', monkeypatch)
    assert outcome['blocked_before'] is False
    assert outcome['after'] == outcome['before']
    state = observe(store, CHAT)
    assert not state['usable'] and not state['external']['usable'], \
        'a partial tail was promoted by the rollback'


def test_the_marker_is_rejected_by_the_existing_strict_reader(monkeypatch):
    store = ConversationStore()
    path = store._envelope_path_for(CHAT)
    publish(store, CHAT, 'consulta A confirmada')
    leave_barrier(store, CHAT, 'consulta B NO publicada', monkeypatch)
    fail_after_visible_unlink(store, CHAT, 'consulta C fallida', monkeypatch)
    lines = [l for l in path.read_text().splitlines() if l.strip()]
    assert json.loads(lines[-1]) == {'publication_incomplete': True}
    assert len(lines) == 3, 'A, the unpublished B and the marker'
    read = store.load_envelope(CHAT)
    assert not read.usable and read.status.name == 'CORRUPT'
