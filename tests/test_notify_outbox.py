"""Tests de app/core/notify/outbox_store.py — outbox, idempotencia y
asociación batch-evento (P1B-2)."""
from __future__ import annotations

import sqlite3
import threading

import pytest

from app.core.db import connect_sqlite
from app.core.notify.idempotency import canonical_event_set, idempotency_key
from app.core.notify.outbox_store import (
    PHASE_AFTER_ASSOC_INSERT,
    PHASE_AFTER_BATCH_INSERT,
    PHASE_BEFORE_COMMIT,
    BatchEventRef,
    DuplicateEventAssociationError,
    OutboxError,
    OutboxStore,
)


@pytest.fixture
def store(tmp_path):
    return OutboxStore(tmp_path / "outbox_test.db")


def _ref(event_id: str, seq: int) -> BatchEventRef:
    return BatchEventRef(event_id=event_id, event_seq=seq)


def _make(
    store,
    *,
    endpoint_id="e1",
    radar_id="r1",
    events,
    payload_json=None,
    created_at="2026-07-15T00:00:00+00:00",
    expires_at="2099-01-01T00:00:00+00:00",
    **over,
):
    return store.create_batch(
        principal_id="p1",
        endpoint_id=endpoint_id,
        radar_id=radar_id,
        payload_json=payload_json if payload_json is not None else {},
        events=events,
        created_at=created_at,
        expires_at=expires_at,
        **over,
    )


# --- B · identidad del batch (idempotency_key) ------------------------------

def test_different_event_sets_produce_different_keys(store):
    """9 · {1,5} y {1,2,3,4,5} → claves distintas.

    Mismo endpoint/radar a propósito: si la fórmula colapsara a min/max en vez
    del conjunto completo, ambos conjuntos comparten min=1 y max=5 y
    producirían la misma clave pese a ser conjuntos distintos — justo lo que
    esta prueba debe delatar (mutación 1).
    """
    res1 = _make(store, events=[_ref("a", 1), _ref("b", 5)])
    res2 = _make(store, events=[_ref("c", 1), _ref("d", 2),
                                _ref("e", 3), _ref("f", 4), _ref("g", 5)])
    b1 = store.get_batch(res1.notification_id)
    b2 = store.get_batch(res2.notification_id)
    assert b1.idempotency_key != b2.idempotency_key
    assert res1.notification_id != res2.notification_id
    assert res2.created is True


def test_same_set_different_order_same_key(store):
    """10 · mismo conjunto, orden de entrada distinto → misma clave."""
    k1 = idempotency_key(endpoint_id="e1", radar_id="r1", event_seqs=[1, 5])
    k2 = idempotency_key(endpoint_id="e1", radar_id="r1", event_seqs=[5, 1])
    assert k1 == k2


def test_same_set_different_payload_same_key_and_no_overwrite(store):
    """11 · mismo conjunto, payload/texto distinto → misma clave, y el batch
    existente no se pisa."""
    res1 = _make(store, events=[_ref("a", 1), _ref("b", 2)], payload_json={"text": "v1"})
    res2 = _make(store, events=[_ref("b", 2), _ref("a", 1)], payload_json={"text": "v2 distinto"})
    assert res1.notification_id == res2.notification_id
    assert res2.created is False
    batch = store.get_batch(res1.notification_id)
    assert batch.payload_json == '{"text": "v1"}'  # el primero, intacto


def test_exact_duplicate_creates_one_row(store):
    """12 · mismo conjunto exacto creado dos veces → una fila."""
    _make(store, events=[_ref("a", 1)])
    _make(store, events=[_ref("a", 1)])
    assert store.count_batches(radar_id="r1", endpoint_id="e1") == 1


def test_batch_key_shared_across_different_event_sets(store):
    """13 · batch_key coincide para mismo radar/endpoint con eventos distintos."""
    res1 = _make(store, events=[_ref("a", 1)])
    res2 = _make(store, events=[_ref("b", 2)])
    b1 = store.get_batch(res1.notification_id)
    b2 = store.get_batch(res2.notification_id)
    assert b1.batch_key == b2.batch_key == "r1:e1"
    assert b1.notification_id != b2.notification_id  # son batches distintos


def test_empty_event_set_rejected(store):
    """14 · conjunto vacío rechazado."""
    with pytest.raises(OutboxError):
        _make(store, events=[])


def test_non_positive_seq_rejected():
    """15 · secuencia cero o negativa rechazada (canonical_event_set puro)."""
    with pytest.raises(ValueError):
        canonical_event_set([0])
    with pytest.raises(ValueError):
        canonical_event_set([-1, 3])


@pytest.mark.parametrize("value", [True, False, 1.5, "1", None])
def test_non_integer_event_seq_is_rejected(value):
    """event_seq debe ser int genuino: bool/float/str/None se rechazan en vez
    de coaccionarse silenciosamente a un entero (hallazgo de revisión externa
    — ``int(True)==1`` haría que un caller que pasa un bool por error termine
    con una idempotency_key indistinguible de un event_seq=1 real)."""
    with pytest.raises(ValueError):
        canonical_event_set([value])


def test_create_batch_rejects_non_integer_event_seq(store):
    """Lo mismo pero a través de la API pública de create_batch."""
    with pytest.raises(ValueError):
        _make(store, events=[_ref("ev", "1")])
    assert store.count_batches() == 0


# --- C · asociación batch-evento --------------------------------------------

def test_duplicate_event_id_within_batch_rejected(store):
    """16 · mismo event_id dos veces dentro del batch → rechazo."""
    with pytest.raises(DuplicateEventAssociationError):
        _make(store, events=[_ref("dup", 1), _ref("dup", 2)])
    assert store.count_batches() == 0


def test_same_event_and_endpoint_in_two_batches_rejected(store):
    """17 · mismo event_id y endpoint en dos batches → rechazo, sin huérfano."""
    _make(store, events=[_ref("shared", 1)])
    with pytest.raises(DuplicateEventAssociationError):
        _make(store, events=[_ref("shared", 1), _ref("other", 2)])
    # el segundo batch no quedó huérfano: solo existe el primero
    assert store.count_batches(endpoint_id="e1") == 1


def test_same_event_different_endpoints_allowed(store):
    """18 · mismo event_id en endpoints distintos → permitido."""
    res1 = _make(store, endpoint_id="e1", events=[_ref("shared", 1)])
    res2 = _make(store, endpoint_id="e2", events=[_ref("shared", 1)])
    assert res1.created and res2.created
    assert res1.notification_id != res2.notification_id


def test_association_endpoint_mismatch_fails_compound_fk(tmp_path):
    """19 · endpoint_id de asociación distinto al batch → FK compuesta falla.

    No es alcanzable desde la API pública de create_batch (siempre deriva el
    endpoint_id de la asociación del propio batch); se prueba a nivel SQL para
    demostrar que la FK compuesta existe y protege el invariante igual.
    """
    store = OutboxStore(tmp_path / "fk.db")
    res = _make(store, events=[_ref("a", 1)])
    conn = connect_sqlite(tmp_path / "fk.db")
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO radar_notification_batch_events "
                "(notification_id, endpoint_id, event_id, event_seq) "
                "VALUES (?, ?, ?, ?)",
                (res.notification_id, "endpoint-que-no-es-el-del-batch", "z", 9),
            )
    finally:
        conn.close()


@pytest.mark.parametrize("phase", [
    PHASE_AFTER_BATCH_INSERT, PHASE_AFTER_ASSOC_INSERT, PHASE_BEFORE_COMMIT,
])
def test_fault_in_any_phase_rolls_back_batch_and_associations(store, phase):
    """20 · conflicto/fallo en cualquier fase revierte el batch completo y
    todas las asociaciones ya insertadas, sin dejar huérfanos."""

    class _Boom(RuntimeError):
        pass

    def hook(current):
        if current == phase:
            raise _Boom(phase)

    with pytest.raises(_Boom):
        _make(store, events=[_ref("a", 1), _ref("b", 2)], _fault_hook=hook)

    assert store.count_batches() == 0


def test_two_concurrent_creates_same_event_endpoint_exactly_one_wins(tmp_path):
    """21 · dos threads crean batches distintos con el mismo evento y
    endpoint: exactamente uno gana; el otro no deja batch huérfano.

    ``only-a``/``only-b`` llevan event_seq **distintos** (2 y 3): son eventos
    reales distintos. Si compartieran seq, los dos batches calcularían la
    misma idempotency_key (que depende de event_seqs, no de event_id) y el
    caso de prueba dejaría de ser el conflicto evento-endpoint que se quiere
    ejercitar.
    """
    db_path = tmp_path / "race.db"
    seed = OutboxStore(db_path)  # asegura el schema antes de las conexiones concurrentes

    barrier = threading.Barrier(2)
    results: dict = {}
    errors: dict = {}
    lock = threading.Lock()

    def create(label: str, own_event: str, own_seq: int) -> None:
        store = OutboxStore(db_path)
        barrier.wait()
        try:
            res = store.create_batch(
                principal_id="p1", endpoint_id="e1", radar_id="r1",
                payload_json={}, events=[_ref("shared", 1), _ref(own_event, own_seq)],
                created_at="a", expires_at="2099-01-01T00:00:00+00:00",
            )
            with lock:
                results[label] = res
        except DuplicateEventAssociationError as exc:
            with lock:
                errors[label] = exc

    threads = [
        threading.Thread(target=create, args=("A", "only-a", 2)),
        threading.Thread(target=create, args=("B", "only-b", 3)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads)

    assert len(results) + len(errors) == 2
    assert len(results) == 1  # exactamente un ganador
    assert len(errors) == 1
    winner = next(iter(results.values()))
    assert seed.count_batches(endpoint_id="e1") == 1  # el perdedor no quedó huérfano
    winner_events = {r.event_id for r in seed.list_batch_events(winner.notification_id)}
    assert "shared" in winner_events


def test_fifty_iterations_concurrent_event_endpoint_conflict(tmp_path):
    """Repetición de 21: exactamente un ganador en cada corrida, sin excepción."""
    for i in range(50):
        db_path = tmp_path / f"race_{i}.db"
        seed = OutboxStore(db_path)
        barrier = threading.Barrier(2)
        results: dict = {}
        lock = threading.Lock()

        def create(label: str, own_event: str, own_seq: int) -> None:
            store = OutboxStore(db_path)
            barrier.wait()
            try:
                res = store.create_batch(
                    principal_id="p1", endpoint_id="e1", radar_id="r1",
                    payload_json={}, events=[_ref("shared", 1), _ref(own_event, own_seq)],
                    created_at="a", expires_at="2099-01-01T00:00:00+00:00",
                )
                with lock:
                    results[label] = ("ok", res)
            except DuplicateEventAssociationError as exc:
                with lock:
                    results[label] = ("err", exc)

        threads = [
            threading.Thread(target=create, args=("A", "only-a", 2)),
            threading.Thread(target=create, args=("B", "only-b", 3)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert not any(t.is_alive() for t in threads)

        outcomes = [v[0] for v in results.values()]
        assert outcomes.count("ok") == 1, f"iteración {i}: {outcomes}"
        assert outcomes.count("err") == 1, f"iteración {i}: {outcomes}"
        assert seed.count_batches(endpoint_id="e1") == 1, f"iteración {i}"


# --- F · schema y migración (parte de outbox) -------------------------------

def test_repeated_init_is_idempotent(tmp_path):
    """47 · inicialización repetida es idempotente."""
    db_path = tmp_path / "idem.db"
    OutboxStore(db_path)
    OutboxStore(db_path)  # segunda vez no debe fallar
    store = OutboxStore(db_path)
    _make(store, events=[_ref("a", 1)])
    assert store.count_batches() == 1


def test_notify_stores_can_init_schema_in_any_order(tmp_path):
    """49 · los stores de outbox pueden inicializar el schema en cualquier
    orden entre ellos."""
    from app.core.notify.ledger_store import LedgerStore

    db_path = tmp_path / "order.db"
    LedgerStore(db_path)   # ledger primero
    OutboxStore(db_path)   # outbox después, misma base
    store = OutboxStore(db_path)
    res = _make(store, events=[_ref("a", 1)])
    assert res.created is True
