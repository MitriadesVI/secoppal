"""E8B.2: contrato persistido y ciclo de vida de PaginationBuffer."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core.conversation_store import ConversationStore
from app.core.pagination_buffer import PaginationBuffer


def _row(i: int, *, dataset: str = "procesos") -> dict:
    if dataset == "contratos":
        return {"id_contrato": f"CID-{i}", "objeto_del_contrato": f"Contrato {i}"}
    return {"id_del_proceso": f"PID-{i}", "nombre_del_procedimiento": f"Proceso {i}"}


def _buffer(*, now: datetime | None = None, ttl_seconds: int = 900) -> PaginationBuffer:
    now = now or datetime.now(timezone.utc)
    candidates = [_row(i) for i in range(5)]
    return PaginationBuffer.create(
        query_fingerprint="fp-1",
        group="procesos",
        dataset_id="p6dx-8zbt",
        query_params={"dataset": "procesos", "objeto": ["aseo"], "limit": 5},
        candidate_buffer=candidates,
        validated_ranked_buffer=candidates[:4],
        display_cursor=2,
        source_cursor=5,
        source_candidate_total=9,
        now=now,
        ttl_seconds=ttl_seconds,
    )


def test_create_read_advance_and_restart(tmp_path):
    store = ConversationStore(base_path=tmp_path / "conv")
    original = _buffer()
    store.save_pagination_buffer("chat-1", original)

    # Instancia nueva = reanudacion tras reinicio del proceso.
    restarted = ConversationStore(base_path=tmp_path / "conv")
    loaded = restarted.get_pagination_buffer("chat-1", "procesos", "fp-1")
    assert loaded == original
    assert loaded.validated_total_known == 4
    assert loaded.displayed_so_far == 2

    page, advanced = loaded.take(2)
    assert [row["id_del_proceso"] for row in page] == ["PID-2", "PID-3"]
    assert advanced.display_cursor == 4
    assert original.display_cursor == 2  # modelo inmutable

    restarted.save_pagination_buffer("chat-1", advanced)
    assert restarted.get_pagination_buffer("chat-1", "procesos", "fp-1").display_cursor == 4


def test_expand_deduplicates_candidates_and_validated_rows():
    original = _buffer()
    expanded = original.extend(
        candidate_rows=[_row(4), _row(5), _row(6)],
        validated_rows=[_row(3), _row(5), _row(6)],
        next_source_cursor=8,
        source_candidate_total=9,
    )
    assert [r["id_del_proceso"] for r in expanded.candidate_buffer] == [
        "PID-0", "PID-1", "PID-2", "PID-3", "PID-4", "PID-5", "PID-6",
    ]
    assert [r["id_del_proceso"] for r in expanded.validated_ranked_buffer] == [
        "PID-0", "PID-1", "PID-2", "PID-3", "PID-5", "PID-6",
    ]
    assert expanded.validated_total_known == 6
    assert expanded.source_cursor == 8


def test_fingerprint_mismatch_invalidates_persisted_buffer(tmp_path):
    store = ConversationStore(base_path=tmp_path / "conv")
    store.save_pagination_buffer("chat-1", _buffer())

    assert store.get_pagination_buffer("chat-1", "procesos", "other-fp") is None
    assert store.get_pagination_buffer("chat-1", "procesos", "fp-1") is None


def test_expired_buffer_is_evicted(tmp_path):
    now = datetime(2026, 6, 29, 12, 0, tzinfo=timezone.utc)
    store = ConversationStore(base_path=tmp_path / "conv")
    store.save_pagination_buffer("chat-1", _buffer(now=now, ttl_seconds=60))

    assert store.get_pagination_buffer(
        "chat-1", "procesos", "fp-1", now=now + timedelta(seconds=61),
    ) is None
    assert not store._pagination_path_for("chat-1").exists()


def test_group_isolation_delete_and_clear(tmp_path):
    store = ConversationStore(base_path=tmp_path / "conv")
    procesos = _buffer()
    contratos = PaginationBuffer.create(
        query_fingerprint="fp-2", group="contratos", dataset_id="jbjy-vk9h",
        query_params={"dataset": "contratos", "objeto": ["aseo"], "limit": 5},
        candidate_buffer=[_row(1, dataset="contratos")],
        validated_ranked_buffer=[_row(1, dataset="contratos")],
        display_cursor=1, source_cursor=1, source_candidate_total=1,
        now=datetime.now(timezone.utc),
    )
    store.save_pagination_buffer("chat-1", procesos)
    store.save_pagination_buffer("chat-1", contratos)

    store.delete_pagination_buffer("chat-1", "procesos")
    assert store.get_pagination_buffer("chat-1", "procesos", "fp-1") is None
    assert store.get_pagination_buffer("chat-1", "contratos", "fp-2") == contratos

    store.clear("chat-1")
    assert store.get_pagination_buffer("chat-1", "contratos", "fp-2") is None
