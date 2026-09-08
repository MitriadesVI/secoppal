"""Controlled interleavings at actual reservation, execution and publish boundaries."""
from concurrent.futures import ThreadPoolExecutor
import sys
import threading

import pytest

from app.core.conversation_store import ConversationStore
from app.core.store_lock import conversation_lock
from entity_profile_frame_b_harness import BUYER, TODAY, Conversation, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient


@pytest.mark.parametrize('same_chat', [True, False])
@pytest.mark.parametrize('winner', [0, 1])
def test_reservation_serializes_execution_not_only_append(frame_v12, monkeypatch, same_chat, winner):
    chats = [Conversation('ind-race'), Conversation('ind-race' if same_chat else 'ind-other')]
    first_executed = threading.Event()
    execution_order = []

    class OrderedClient(WorkflowClient):
        def __init__(self, index):
            super().__init__()
            self.index = index

        def _record(self, operation, dataset, soql):
            if self.index == 1:
                assert first_executed.wait(5)
            super()._record(operation, dataset, soql)
            execution_order.append(self.index)
            if self.index == 0 and len(self.calls) == 4:
                first_executed.set()

    workflows = [c.workflow(OrderedClient(i))[0] for i, c in enumerate(chats)]
    rendezvous = threading.Barrier(2)
    released = threading.Event()
    revisions = []
    original_reserve = ConversationStore.reserve_turn
    original_publish = ConversationStore.publish_turn

    def reserve(store, chat_id):
        reservation = original_reserve(store, chat_id)
        revisions.append(reservation.expected_revision)
        rendezvous.wait(timeout=5)
        return reservation

    def publish(store, reservation, **kwargs):
        is_winner = kwargs['user_query'].endswith(str(winner))
        if not is_winner:
            assert released.wait(5)
        try:
            return original_publish(store, reservation, **kwargs)
        finally:
            if is_winner:
                released.set()

    monkeypatch.setattr(ConversationStore, 'reserve_turn', reserve)
    monkeypatch.setattr(ConversationStore, 'publish_turn', publish)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(w.run_query, f'analiza contratos de {BUYER} en 202{i}', chat_id=c.chat_id, today=TODAY)
                   for i, (w, c) in enumerate(zip(workflows, chats))]
        results = [f.result(timeout=15) for f in futures]
    print('RACE', execution_order, revisions, [len(w.secop_client.calls) for w in workflows],
          [(r.get('continuation_published'), r.get('conversation_revision')) for r in results])
    assert execution_order == [0] * 4 + [1] * 4
    assert all(w.secop_client.calls for w in workflows)
    assert all(r.get('continuation_published') for r in results), 'Both turns executed from the same revision; one cannot publish'


class PausedClient(WorkflowClient):
    def __init__(self, entered, released):
        super().__init__()
        self.entered, self.released = entered, released

    def aggregate(self, dataset, soql):
        self.entered.set()
        assert self.released.wait(5)
        return super().aggregate(dataset, soql)


def test_reset_invalidates_an_already_reserved_initial_turn(frame_v12):
    entered, released = threading.Event(), threading.Event()
    chat = Conversation('ind-reset-race')
    client = PausedClient(entered, released)
    workflow, _ = chat.workflow(client)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(workflow.run_query, f'analiza contratos de {BUYER} en agosto', chat_id=chat.chat_id, today=TODAY)
        assert entered.wait(5)
        reset = chat.say('/reset')
        assert reset.route == 'reset'
        assert not chat.published().usable
        released.set()
        result = future.result(timeout=10)
    print('RESET_RACE', result.get('continuation_published'), chat.published())
    assert not chat.published().usable, 'A pre-reset reservation recreated a confirmed profile'


def test_missing_fcntl_does_not_silently_admit_writers(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, 'fcntl', None)
    entered = False
    try:
        with conversation_lock(tmp_path / 'chat'):
            entered = True
    except (ImportError, RuntimeError):
        pass
    assert not entered, 'No cross-process guarantee, but execution was admitted without an error'


def test_failed_delegated_publication_cannot_replace_legacy_authority(frame_v12, monkeypatch):
    chat = Conversation('ind-failed-delegated')
    chat.say('contratos de obra en baranoa')
    store = ConversationStore()
    before = store.get_history(chat.chat_id, last_n=1)[-1].to_dict()
    visible = chat.published().envelope.to_dict()
    workflow, _ = chat.workflow()

    def fail(*args, **kwargs):
        raise OSError('synthetic disk full')

    monkeypatch.setattr(workflow.conv_store, 'publish_turn', fail)
    result = workflow.run_query('procesos de alimentacion en bogota', chat_id=chat.chat_id, today=TODAY)
    after = store.get_history(chat.chat_id, last_n=1)[-1].to_dict()
    print('FAILED_PUBLISH', result, 'BEFORE', before, 'AFTER', after)
    assert result['continuation_published'] is False
    assert chat.published().envelope.to_dict() == visible
    followup = chat.say('muestrame mas')
    print('UNPUBLISHED_FOLLOWUP', followup.route, followup.result.get('resolved_params'))
    assert followup.result.get('resolved_params', {}).get('dataset') == before['parsed_params']['dataset'], 'Pagination followed the unpublished history'
    assert after == before, 'Unpublished delegated turn changed the history consumed by pagination'


def test_two_processes_both_enter_when_fcntl_is_missing(tmp_path):
    import subprocess

    script = '''
import sys
from pathlib import Path
sys.modules['fcntl'] = None
from app.core.store_lock import conversation_lock
with conversation_lock(Path(sys.argv[1])):
    print('HELD', flush=True)
    sys.stdin.readline()
'''
    first = subprocess.Popen([sys.executable, '-c', script, str(tmp_path / 'same')],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert first.stdout.readline().strip() == 'HELD'
        second = subprocess.run([sys.executable, '-c', script, str(tmp_path / 'same')],
                                input='release\n', capture_output=True, text=True, timeout=5, check=False)
        print('NO_FCNTL_TWO_PROCESSES', second.returncode, second.stdout)
        assert 'HELD' not in second.stdout, 'Both processes held the same conversation lock concurrently'
    finally:
        first.communicate(input='release\n', timeout=5)
