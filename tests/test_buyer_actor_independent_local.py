"""Opt-in local index: only two authorized mentions, no identity export."""
import os
from pathlib import Path

import pytest

from app.data.actor_alias_resolver import ActorNameResolver
from tests.buyer_actor_independent_support import (
    SEEDS, workflow, attach_source, say, persist, assert_buyer,
)


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('seed', SEEDS, ids=['cartagena', 'barranquilla'])
def test_local_effective_flags_and_product(tmp_path, monkeypatch, frame, seed):
    configured = os.environ.get('BUYER_ACTOR_LOCAL_INDEX')
    if not configured or not Path(configured).is_file():
        pytest.skip('Opt-in requires BUYER_ACTOR_LOCAL_INDEX')
    path = Path(configured)
    before = path.stat()
    query, mention, buyer, minimum = seed
    lookup = ActorNameResolver(path).resolve_exact(mention)
    flow = workflow(tmp_path, monkeypatch, path, frame)
    conn = attach_source(flow, monkeypatch, buyer, minimum)
    try:
        result = say(flow, query)
        assert_buyer(flow, result, buyer, minimum)
        assert not flow.audit_trace['resolver']
        flow.audit_trace['bounded_lookup'] = {
            'mention': mention, 'status': str(lookup.status),
            'method': lookup.method, 'returned_candidates': len(lookup.candidates),
        }
        after = path.stat()
        old = (before.st_size, before.st_mtime_ns, before.st_ino)
        new = (after.st_size, after.st_mtime_ns, after.st_ino)
        assert old == new
        flow.audit_trace['artifact_stat_before_after'] = [old, new]
    finally:
        persist(f'local-flags-{mention.split()[-1]}-{frame}', flow.audit_trace)
        conn.close()
