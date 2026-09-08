"""Opt-in read-only verification of the two reported local mentions only."""
import os
import sqlite3
from datetime import date
from pathlib import Path

import pytest

from app.data.actor_alias_resolver import ActorNameResolver
from tests.test_buyer_actor_role_arbitration_v1 import offline  # noqa: F401
from tests.test_buyer_actor_role_arbitration_v1 import SEEDS, flow_for, source_for, write_trace


@pytest.mark.parametrize("frame", [False, True])
@pytest.mark.parametrize("seed", SEEDS, ids=["cartagena", "barranquilla"])
def test_local_index_read_only(tmp_path, monkeypatch, frame, seed):
    configured = os.environ.get("BUYER_ACTOR_LOCAL_INDEX")
    if not configured or not Path(configured).is_file():
        pytest.skip("Local index check requires explicit BUYER_ACTOR_LOCAL_INDEX")
    index = Path(configured)
    before = index.stat()
    with sqlite3.connect(index.resolve().as_uri() + "?mode=ro&immutable=1", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        metadata = dict(conn.execute("SELECT * FROM actor_index_metadata WHERE singleton=1").fetchone())
    query, mention, buyer, amount = seed
    # Bounded exact lookup; never export identities or the provider directory.
    direct = ActorNameResolver(index).resolve_exact(mention)
    trace = {"query": query, "metadata": metadata,
             "artifact": {"bytes": before.st_size, "mtime_ns": before.st_mtime_ns},
             "bounded_lookup": {"mention": mention, "status": str(direct.status),
                                "method": direct.method, "candidate_count": len(direct.candidates)}}
    flow = flow_for(tmp_path, monkeypatch, index, frame, trace)
    source_for(flow, monkeypatch, trace, buyer, amount)
    result = flow.run_query(query, chat_id="local-fresh", today=date(2026, 9, 8))
    trace["result"] = result
    write_trace(trace, f"local-{mention.split()[-1]}-{frame}")
    assert not trace.get("violations"), trace
    assert trace.get("source_rows"), result
    assert not trace.get("resolver"), trace
    assert not result["resolved_params"].get("actor_scope"), result
    after = index.stat()
    assert (before.st_size, before.st_mtime_ns, before.st_ino) == (after.st_size, after.st_mtime_ns, after.st_ino)
