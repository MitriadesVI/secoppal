"""Observations of the declared recovery limit. Not safety guarantees."""
import json
from pathlib import Path

import pytest

from app.core.conversation_store import ConversationStore
from app.core.workflow.publication_io import barrier_path, PublicationRecoveryError
from storage_faults import publish
from test_f4_chained_failures import leave_barrier, observe

CHAT = 'audit-independent-limits'


def fail_with_broken_marker(store, chat_id, query, monkeypatch, *, fault):
    """The rollback opens the file but cannot write or sync the marker."""
    path = store._envelope_path_for(chat_id)
    barrier = barrier_path(path)
    original_open, original_unlink = Path.open, Path.unlink
    reached = []

    class Broken:
        def __init__(self, handle):
            self._handle = handle

        def truncate(self, size):
            return self._handle.truncate(size)

        def seek(self, offset):
            return self._handle.seek(offset)

        def write(self, data):
            if fault == 'write':
                raise OSError('audit: cannot write the recovery marker')
            return self._handle.write(data)

        def flush(self):
            if fault == 'flush':
                raise OSError('audit: cannot flush the recovery marker')
            return self._handle.flush()

        def fileno(self):
            return self._handle.fileno()

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return self._handle.__exit__(*exc)

    def unlink(self, *args, **kwargs):
        if self == barrier and not reached:
            original_unlink(self, *args, **kwargs)
            reached.append('unlink_visible')
            raise OSError('audit: unlink reported failure after visibility')
        return original_unlink(self, *args, **kwargs)

    def open_path(self, mode='r', *args, **kwargs):
        if self == barrier and mode == 'w' and reached:
            reached.append('rearm_failed')
            raise OSError('audit: cannot recreate barrier')
        handle = original_open(self, mode, *args, **kwargs)
        if self == path and mode == 'r+b':
            reached.append('rollback_open')
            return Broken(handle)
        return handle

    error = None
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'unlink', unlink)
        patch.setattr(Path, 'open', open_path)
        try:
            publish(store, chat_id, query)
        except PublicationRecoveryError as exc:
            error = exc
        except OSError as exc:
            error = exc
    return reached, error


@pytest.mark.parametrize('fault', ['write', 'flush'])
def test_what_a_failed_marker_leaves_admitted(monkeypatch, fault):
    """OBSERVATION of a third simultaneous fault, not a safety guarantee.

    The finding B2-I-F4 was reported with recovery and fsync SUCCEEDING and an
    explicit False. Here the recovery itself fails and the call raises, so this
    is the declared limit, not the repaired sequence. It is recorded because
    what stays admitted differs from what REMEDIATION documents for that limit.
    """
    store = ConversationStore()
    path = store._envelope_path_for(CHAT)
    publish(store, CHAT, 'consulta A confirmada')
    leave_barrier(store, CHAT, 'consulta B NO publicada', monkeypatch)
    reached, error = fail_with_broken_marker(store, CHAT, 'consulta C fallida',
                                             monkeypatch, fault=fault)
    assert reached == ['unlink_visible', 'rearm_failed', 'rollback_open']
    assert isinstance(error, PublicationRecoveryError), \
        'a failed marker write surfaces as the declared recovery limit'
    state = observe(store, CHAT)
    lines = [l for l in path.read_text().splitlines() if l.strip()]
    admitted = dict(fault=fault, usable=state['usable'], query=state['query'],
                    external=state['external']['usable'], lines=len(lines))
    (Path(__file__).parent / f'limit_marker_{fault}.json').write_text(
        json.dumps(admitted, ensure_ascii=False, indent=2) + '\n')
    if fault == 'flush':
        # The marker reached the file before the flush failed: still fenced.
        assert not state['usable'] and not state['external']['usable']
    else:
        # The truncate ran before the write, so the blocking tail is gone and
        # an earlier, explicitly unpublished record is admitted. Reported to
        # the coordinator as a widened limit, not as the repaired sequence.
        assert state['usable'] and state['query'] == 'consulta B NO publicada'
        assert state['external']['usable']


def test_rollback_open_failure_is_an_observation_not_a_fence(monkeypatch):
    """Declared limit: with no rollback at all, the new envelope may be admitted."""
    store = ConversationStore()
    path = store._envelope_path_for(CHAT)
    publish(store, CHAT, 'consulta A confirmada')
    barrier = barrier_path(path)
    original_open, original_unlink = Path.open, Path.unlink
    reached = []

    def unlink(self, *args, **kwargs):
        if self == barrier and not reached:
            original_unlink(self, *args, **kwargs)
            reached.append('unlink_visible')
            raise OSError('audit: unlink reported failure after visibility')
        return original_unlink(self, *args, **kwargs)

    def open_path(self, mode='r', *args, **kwargs):
        if self == barrier and mode == 'w' and reached:
            reached.append('rearm_failed')
            raise OSError('audit: cannot recreate barrier')
        if self == path and mode == 'r+b':
            reached.append('rollback_open')
            raise OSError('audit: cannot open file for rollback')
        return original_open(self, mode, *args, **kwargs)

    error = None
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'unlink', unlink)
        patch.setattr(Path, 'open', open_path)
        try:
            publish(store, CHAT, 'consulta C sin rollback')
        except PublicationRecoveryError as exc:
            error = exc
    assert isinstance(error, PublicationRecoveryError)
    state = observe(store, CHAT)
    # OBSERVATION, recorded for the coordinator: the exception did not fence.
    assert state['usable'] and state['query'] == 'consulta C sin rollback', (
        'documented limit: an un-rolled-back complete envelope is admitted '
        'even though the call raised')
