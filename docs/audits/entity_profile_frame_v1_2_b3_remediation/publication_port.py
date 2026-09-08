"""Independent sequential failures at real file operations; no publication mocks."""
import json
import os
from pathlib import Path
import subprocess
import sys

from audit_output import emit

from app.core.conversation_store import ConversationStore
from app.core.workflow.publication_io import barrier_path, PublicationRecoveryError


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
    blocked_before = barrier.exists()
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
        after = target.read_bytes()
        expected = before + b'\n{"publication_incomplete":true}\n' if blocked_before else before
        assert after == expected, 'preserve prefix bytes and its nonconfirmation'
        emit('RECOVERY_PORT', dict(blocked_before=blocked_before, before_bytes=len(before),
                                  after_bytes=len(after), reached=reached))
    else:
        assert isinstance(error, PublicationRecoveryError) and failed is None
        assert target.read_bytes().startswith(before) and len(target.read_bytes()) > len(before)
    return failed, error
