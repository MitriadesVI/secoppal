"""Real file-operation faults against the real ConversationStore. No mocks."""
import os
from pathlib import Path

from app.core.conversation_store import ConversationStore
from app.core.workflow.publication_io import barrier_path, PublicationRecoveryError


def publish(store, chat_id, query):
    """One ordinary confirmed publication through the real API."""
    reservation = store.reserve_turn(chat_id)
    return store.publish_turn(reservation, user_query=query, response='r:' + query,
                              profile_state=None, continuation=None)


def fail_after_visible_unlink(store, chat_id, query, monkeypatch, *,
                              rollback='ok', marker='ok'):
    """Unlink reports failure AFTER visibility, then re-arm fails.

    This is the exact B2-I-F4 shape: recovery and fsync succeed and the caller
    gets an explicit failure, not a device-level catastrophe.
    """
    path = store._envelope_path_for(chat_id)
    barrier = barrier_path(path)
    original_open, original_unlink = Path.open, Path.unlink
    before = path.read_bytes() if path.exists() else b''
    blocked_before = barrier.exists()
    reached = []

    def unlink(self, *args, **kwargs):
        if self == barrier and not reached:
            original_unlink(self, *args, **kwargs)
            assert not barrier.exists(), 'the unlink must really be visible'
            reached.append('unlink_visible')
            raise OSError('audit: unlink reported failure after visibility')
        return original_unlink(self, *args, **kwargs)

    def open_path(self, mode='r', *args, **kwargs):
        if self == barrier and mode == 'w' and reached:
            reached.append('rearm_failed')
            raise OSError('audit: cannot recreate barrier')
        if self == path and mode == 'r+b':
            reached.append('rollback_open')
            if rollback == 'open_failure':
                raise OSError('audit: cannot open file for rollback')
        return original_open(self, mode, *args, **kwargs)

    original_write = os.write
    error = None
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'unlink', unlink)
        patch.setattr(Path, 'open', open_path)
        if marker == 'write_failure':
            def truncate_then_fail(self, size):
                raise OSError('audit: cannot write the recovery marker')
            patch.setattr('io.BufferedRandom.truncate', truncate_then_fail)
        try:
            publish(store, chat_id, query)
        except PublicationRecoveryError as exc:
            error = ('recovery', exc)
        except OSError as exc:
            error = ('oserror', exc)
    assert original_write is os.write
    return dict(reached=reached, error=error, blocked_before=blocked_before,
                before=before, after=path.read_bytes() if path.exists() else b'')
