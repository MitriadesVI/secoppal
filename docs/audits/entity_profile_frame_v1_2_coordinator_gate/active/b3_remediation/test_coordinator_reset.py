"""Command contract, independently of the epoch primitive's readable output."""
import pytest

from app.core.conversation_store import ConversationStore
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from publication_port import fail_envelope_fsync, process_read
from test_v12b_independent_boundaries import Pages


@pytest.mark.parametrize('profile', [False, True])
def test_reset_discards_confirmed_and_failed_continuity(frame_v12, monkeypatch, profile):
    chat = Conversation('coordinator-reset', client_factory=Pages)
    first_query = ('analiza contratos de alcaldia de baranoa en agosto' if profile
                   else 'contratos de obra en baranoa')
    bad_query = ('analiza contratos de alcaldia de galapa en julio' if profile
                 else 'procesos de alimentacion en bogota')
    first = chat.say(first_query)
    assert first.result['continuation_published']
    failed = fail_envelope_fsync(chat, bad_query, monkeypatch)
    store = ConversationStore()
    assert not process_read(store, chat.chat_id)['usable']
    cleared = []
    original = ConversationStore.clear

    def observe_clear(self, chat_id):
        cleared.append(chat_id)
        return original(self, chat_id)

    with monkeypatch.context() as patch:
        patch.setattr(ConversationStore, 'clear', observe_clear)
        reset = chat.say('/reset')
    assert reset.route == 'reset' and not reset.calls
    assert cleared == [chat.chat_id]
    reloaded = process_read(store, chat.chat_id)
    assert not reloaded['usable'] and not reloaded['history']
    assert not store._pagination_path_for(chat.chat_id).exists()
    current = chat.say(first_query)
    assert current.result['continuation_published'] and not current.clarifies
    assert current.result['result_id'] not in (first.result['result_id'], failed['result_id'])
    published = process_read(store, chat.chat_id)
    assert published['usable']
    assert published['envelope']['result_id'] == current.result['result_id']
    assert published['envelope']['user_query'] == first_query
    assert all(turn['user_query'] != bad_query for turn in published['history'])
    more = chat.say('proveedores' if profile else 'más')
    assert not more.clarifies
    if profile:
        assert len(more.calls) == 2
        assert all('BARANOA' in sql and '2026-08' in sql and 'GALAPA' not in sql for sql in more.sql)
    else:
        assert more.result['rows'] and more.result['dataset_id'] == 'jbjy-vk9h'
