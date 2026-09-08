"""Publication failures at actual file boundaries, after private writes exist."""
import json
import os
from pathlib import Path

import pytest

from app.core.conversation_store import ConversationStore
from app.core.workflow.continuity_staging import active_generation, generation_path
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_v12b_independent_boundaries import Pages


@pytest.mark.parametrize('boundary', ['history_fsync', 'buffer_fsync', 'envelope_fsync', 'directory_fsync'])
def test_failed_io_never_authorizes_the_unpublished_generation(frame_v12, monkeypatch, boundary):
    chat = Conversation('b1-independent-io', client_factory=Pages)
    first = chat.say('contratos de obra en baranoa')
    store = ConversationStore()
    old = chat.published().envelope
    history = store.get_history(chat.chat_id, 1)[0].to_dict()
    original = os.fsync
    injected = []
    private = {}
    envelope_path = store._envelope_path_for(chat.chat_id)
    old_size = envelope_path.stat().st_size

    def fsync(fd):
        generation = active_generation(store, chat.chat_id)
        if generation and not injected:
            private_path = generation_path(store, chat.chat_id, generation, 'history.jsonl')
            targets = {
                'history_fsync': private_path,
                'buffer_fsync': private_path.with_name('pagination.json'),
                'envelope_fsync': envelope_path,
                'directory_fsync': store.base_path,
            }
            target = targets[boundary]
            at_boundary = target.exists() and os.fstat(fd).st_ino == target.stat().st_ino
            if boundary == 'directory_fsync':
                at_boundary = at_boundary and envelope_path.stat().st_size > old_size
            if at_boundary:
                # The real handler has already written a complete private history.
                private.update(generation=generation, history=private_path.read_text())
                assert 'alimentacion' in private['history']
                injected.append(boundary)
                raise OSError('independent injected I/O failure at ' + boundary)
        return original(fd)

    with monkeypatch.context() as patch:
        patch.setattr(os, 'fsync', fsync)
        failed = chat.say('procesos de alimentacion en bogota')
    assert injected == [boundary], 'the real I/O boundary must have been reached'
    assert failed.result['continuation_published'] is False
    assert 'No pude guardar el contexto' in failed.response
    read = chat.published()
    followup = chat.say('muéstrame más')
    print('IO_BOUNDARY', boundary, 'old_result', first.result['result_id'],
          'private_generation', private['generation'], 'reloaded', read,
          'FOLLOWUP', followup.route, followup.result.get('resolved_params'), followup.calls)
    assert not read.usable or read.envelope.result_id == old.result_id, (
        'A turn declared unpublished became the durable current authority')
    assert followup.clarifies or followup.result.get('dataset_id') == 'jbjy-vk9h'
    if read.usable:
        assert json.loads(private['history'].splitlines()[-1])['user_query'] != history['user_query']


def test_preparation_failure_keeps_other_chat_and_orphan_inert(frame_v12, monkeypatch):
    chat = Conversation('b1-independent-orphan', client_factory=Pages)
    other = Conversation('b1-independent-other', client_factory=Pages)
    chat.say('contratos de obra en baranoa')
    old = chat.published().envelope
    store = ConversationStore()
    original = os.fsync
    orphan = []
    other_result = []

    def fsync(fd):
        generation = active_generation(store, chat.chat_id)
        if generation and not orphan:
            path = generation_path(store, chat.chat_id, generation, 'history.jsonl')
            if path.exists() and os.fstat(fd).st_ino == path.stat().st_ino:
                orphan.append(path)
                other_result.append(other.say('procesos de alimentacion en bogota'))
                raise OSError('failure after the other chat committed')
        return original(fd)

    with monkeypatch.context() as patch:
        patch.setattr(os, 'fsync', fsync)
        failed = chat.say('procesos de alimentacion en bogota')
    assert failed.result['continuation_published'] is False
    assert orphan and orphan[0].exists() and 'alimentacion' in orphan[0].read_text()
    assert chat.published().envelope.result_id == old.result_id
    assert store._path_for(chat.chat_id).parent.name == old.continuity_generation
    assert other.published().envelope.result_id == other_result[0].result['result_id']
    assert other.say('más').result['dataset_id'] == 'p6dx-8zbt'
    assert chat.say('más').result['dataset_id'] == 'jbjy-vk9h'


@pytest.mark.parametrize('truncate', [False, True])
def test_append_boundary_is_fail_closed_after_write(frame_v12, monkeypatch, truncate):
    chat = Conversation('b1-independent-append')
    chat.say('analiza contratos de alcaldia de baranoa en agosto')
    old = chat.published().envelope
    target = ConversationStore()._envelope_path_for(chat.chat_id)
    original_open = Path.open
    reached = []

    class Writer:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def write(self, text):
            reached.append(text)
            self.handle.write(text[:len(text) // 2] if truncate else text)
            self.handle.flush()
            raise OSError('write completed partially or fully before I/O error')

    def open_path(path, mode='r', *args, **kwargs):
        handle = original_open(path, mode, *args, **kwargs)
        return Writer(handle) if path == target and mode == 'a' else handle

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'open', open_path)
        failed = chat.say('proveedores')
    assert len(reached) == 1 and failed.result['continuation_published'] is False
    read = chat.published()
    later = chat.say('montos')
    print('APPEND', truncate, read, later.route, later.calls)
    assert not read.usable or read.envelope.result_id == old.result_id, 'Unpublished Frame gained authority'
    if truncate:
        assert later.clarifies and not later.calls
