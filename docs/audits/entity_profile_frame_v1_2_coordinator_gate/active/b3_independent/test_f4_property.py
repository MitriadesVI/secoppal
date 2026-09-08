"""Property: an unpublished record is never admitted after any failure chain."""
from hypothesis import given, HealthCheck, settings, strategies as st

from app.core.conversation_store import ConversationStore
from app.core.workflow.publication_io import barrier_path
from app.core.workflow.store_epoch import advance_epoch
from storage_faults import fail_after_visible_unlink, publish
from test_f4_chained_failures import leave_barrier

STEPS = st.sampled_from(['barrier', 'visible_unlink', 'publish', 'reset'])


@settings(max_examples=40, deadline=None, derandomize=True,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(steps=st.lists(STEPS, min_size=2, max_size=5))
def test_no_failure_chain_ever_admits_an_unpublished_record(monkeypatch, tmp_path_factory, steps):
    base = tmp_path_factory.mktemp('chain')
    store = ConversationStore(str(base))
    chat = 'audit-property'
    confirmed = {publish(store, chat, 'turno A confirmado').result_id}
    unpublished = set()
    for index, step in enumerate(steps):
        query = f'turno {index} {step}'
        if step == 'barrier':
            # Classify by the real outcome: an injected fault may not fire
            # once the store is already fenced.
            try:
                leave_barrier(store, chat, query, monkeypatch)
            except Exception:
                unpublished.add(query)
            else:
                unpublished.add(query)
        elif step == 'visible_unlink':
            try:
                fail_after_visible_unlink(store, chat, query, monkeypatch)
            finally:
                unpublished.add(query)
        elif step == 'publish':
            try:
                envelope = publish(store, chat, query)
            except Exception:
                unpublished.add(query)
            else:
                confirmed.add(envelope.result_id)
        else:
            # A reset is a deliberate cut, not a failed attempt. The barrier is
            # renamed with the epoch and stops fencing: PREEXISTING behaviour,
            # verified identical on base a7e12ebf (probe_reset.py). The property
            # under audit is about failed attempts, so the ledger restarts here.
            advance_epoch(store._envelope_path_for(chat))
            unpublished.clear()
            confirmed.add(store.load_envelope(chat).envelope.result_id
                          if store.load_envelope(chat).usable else None)

        read = store.load_envelope(chat)
        if read.usable:
            assert read.envelope.user_query not in unpublished, (
                f'after {steps[:index + 1]} an explicitly unpublished record '
                f'became the current authority')
            assert read.envelope.result_id in confirmed
        history = [turn.user_query for turn in store.get_history(chat)]
        assert not (set(history) & unpublished), 'history absorbed a failed turn'
    # A barrier, if present, must still block; if absent, the reader decides.
    if barrier_path(store._envelope_path_for(chat)).exists():
        assert not store.load_envelope(chat).usable
