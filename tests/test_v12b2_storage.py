"""Real I/O faults on the new barrier, including errors AFTER visibility."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from app.core.conversation_store import ConversationStore
from app.core.workflow.publication_io import barrier_path
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_v12b_independent_boundaries import Pages


@pytest.mark.parametrize('profile', [False, True])
@pytest.mark.parametrize('boundary', ['arm_write', 'arm_fsync', 'arm_directory',
                                     'confirm_before', 'confirm_after', 'confirm_rearm_failure'])
def test_barrier_failures_never_authorize_failed_turn(frame_v12, monkeypatch, profile, boundary):
    chat = Conversation('b2-barrier', client_factory=Pages)
    other = Conversation('b2-other', client_factory=Pages)
    first_query = ('analiza contratos de alcaldia de baranoa en agosto' if profile
                   else 'contratos de obra en baranoa')
    next_query = 'proveedores' if profile else 'procesos de alimentacion en bogota'
    chat.say(first_query)
    other.say('contratos de obra en baranoa')
    store = ConversationStore()
    target = store._envelope_path_for(chat.chat_id)
    old = store.load_envelope(chat.chat_id).envelope.result_id
    barrier = barrier_path(target)
    other_before = other.published().envelope.to_json()
    original_open, original_unlink, original_fsync = Path.open, Path.unlink, os.fsync
    reached = []

    class Writer:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def write(self, text):
            self.handle.write(text)
            self.handle.flush()
            reached.append('arm_write')
            raise OSError('barrier write AFTER visibility')

    def open_path(path, mode='r', *args, **kwargs):
        if path == barrier and mode == 'w' and reached and boundary == 'confirm_rearm_failure':
            raise OSError('cannot rearm; must roll back own append')
        handle = original_open(path, mode, *args, **kwargs)
        if path == barrier and mode == 'w' and boundary == 'arm_write' and not reached:
            return Writer(handle)
        return handle

    def unlink(path, *args, **kwargs):
        if path == barrier and boundary.startswith('confirm') and not reached:
            reached.append(boundary)
            if boundary != 'confirm_before':
                original_unlink(path, *args, **kwargs)
                assert not barrier.exists()
            raise OSError('confirmation failure')
        return original_unlink(path, *args, **kwargs)

    def fsync(fd):
        if not reached and barrier.exists():
            selected = barrier if boundary == 'arm_fsync' else store.base_path
            if boundary in ('arm_fsync', 'arm_directory') and os.fstat(fd).st_ino == selected.stat().st_ino:
                reached.append(boundary)
                raise OSError('barrier fsync failure AFTER visibility')
        return original_fsync(fd)

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'open', open_path)
        patch.setattr(Path, 'unlink', unlink)
        patch.setattr(os, 'fsync', fsync)
        failed = chat.say(next_query)
    assert reached == [boundary]
    assert failed.result['continuation_published'] is False
    read = store.load_envelope(chat.chat_id)
    assert not read.usable or read.envelope.result_id == old
    assert other.published().envelope.to_json() == other_before
    # Fresh process reads physical storage; no process-local blacklist can pass.
    code = ('import json,sys; from app.core.conversation_store import ConversationStore; '
            'r=ConversationStore(sys.argv[1]).load_envelope(sys.argv[2]); '
            'print(json.dumps([r.usable,r.envelope.result_id if r.usable else None]))')
    observed = json.loads(subprocess.check_output(
        [sys.executable, '-c', code, str(store.base_path), chat.chat_id], text=True))
    assert not observed[0] or observed[1] == old
    later = chat.say('montos' if profile else 'más')
    assert later.clarifies or (profile and later.route == 'entity_profile_values') or (
        not profile and later.result.get('dataset_id') == 'jbjy-vk9h')
    assert all('alimentacion' not in sql for sql in later.sql)
    assert other.say('más').result['dataset_id'] == 'jbjy-vk9h'
    # Reset is the existing supported recovery path, and old barriers are inert.
    chat.say('/reset')
    recovered = chat.say(first_query)
    assert recovered.result['continuation_published']
    followup = chat.say('proveedores' if profile else 'más')
    assert not followup.clarifies
    if profile:
        assert followup.calls and followup.route == 'entity_profile_suppliers'
    else:
        assert followup.result['rows'] and followup.result['dataset_id'] == 'jbjy-vk9h'
        assert followup.result['rows'] != recovered.result['rows']


def test_success_after_unconfirmed_complete_append_without_reset(frame_v12, monkeypatch):
    store = ConversationStore()
    first = store.reserve_turn('b2-recovery')
    store.publish_turn(first, user_query='first', response='first')
    next_turn = store.reserve_turn('b2-recovery')
    target = store._envelope_path_for('b2-recovery')
    original = os.fsync
    reached = []

    def fsync(fd):
        if target.exists() and os.fstat(fd).st_ino == target.stat().st_ino and not reached:
            reached.append(True)
            raise OSError('envelope fsync')
        return original(fd)

    with monkeypatch.context() as patch:
        patch.setattr(os, 'fsync', fsync)
        with pytest.raises(OSError):
            store.publish_turn(next_turn, user_query='failed', response='failed')
    assert reached and not store.load_envelope('b2-recovery').usable
    recovered = store.reserve_turn('b2-recovery')
    valid = store.publish_turn(recovered, user_query='new explicit result', response='new')
    assert ConversationStore().load_envelope('b2-recovery').envelope == valid
