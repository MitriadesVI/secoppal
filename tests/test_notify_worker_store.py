"""Tests de app/core/notify/worker_store.py — las primitivas transaccionales del
worker (P1B-4, R2A).

Convenciones: conexiones separadas por hilo, ``threading.Barrier``/``Event``, cero
sleeps coordinadores, reloj inyectado, joins con timeout, 50 iteraciones en las
carreras. Ningún test importa proveedor, worker ni red — no existen todavía, y que
sigan sin existir es parte de lo que se prueba.
"""
from __future__ import annotations

import itertools
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

import pytest

from app.core.db import connect_sqlite
from app.core.notify import claim
from app.core.notify.endpoint_store import EndpointStore
from app.core.notify.identity_store import IdentityStore
from app.core.notify.ledger_store import LedgerStore
from app.core.notify.masking import Masker
from app.core.notify.outbox_store import BatchEventRef, OutboxStore
from app.core.notify.preference_store import PreferenceStore
from app.core.notify.worker_store import (
    DEFAULT_HOLD_DURATION,
    OUTCOME_AUTHORIZED,
    OUTCOME_CLAIM_LOST,
    OUTCOME_DEAD,
    OUTCOME_HOLD,
    OUTCOME_SECURITY_BLOCKED,
    PHASE_AFTER_ATTEMPT_COUNT,
    PHASE_AFTER_ATTEMPT_INSERT,
    PHASE_AFTER_AUTHORIZATION_READ,
    PHASE_AFTER_BEGIN,
    PHASE_AFTER_CLAIM_VALIDATION,
    PHASE_AFTER_COMMIT,
    PHASE_AFTER_EXTERNAL_KEY,
    PHASE_BEFORE_COMMIT,
    DispatchAuthorization,
    WorkerStore,
    WorkerStoreConsistencyError,
    WorkerStoreError,
    external_idempotency_key,
)
from app.core.radar.store import RadarStore

NOW = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
LATER = NOW + timedelta(hours=1)
DURATION = timedelta(seconds=120)
RECON_DURATION = timedelta(seconds=70)
FOREVER = "2099-01-01T00:00:00+00:00"
JOIN_TIMEOUT = 30
RACE_ITERATIONS = 50

_seq = itertools.count(1)


class _Clock:
    """Reloj inyectado que registra cada invocación.

    Devuelve los instantes en orden y repite el último: así un test que espera una
    sola llamada falla ruidosamente si el código empieza a llamar dos veces, en vez
    de reventar con StopIteration en un sitio que no explica nada.
    """

    def __init__(self, *moments: datetime) -> None:
        self.moments = list(moments) or [NOW]
        self.calls: list[datetime] = []

    def __call__(self) -> datetime:
        moment = self.moments[min(len(self.calls), len(self.moments) - 1)]
        self.calls.append(moment)
        return moment


@pytest.fixture
def env(tmp_path):
    db = tmp_path / "worker_store.db"
    radars = RadarStore(db)
    identity = IdentityStore(db)
    endpoints = EndpointStore(db, masker=Masker.from_key("worker-test-key"))
    preferences = PreferenceStore(db)
    owner = "email:ana@example.com"
    principal = identity.create_principal("ana@example.com")
    identity.link_owner_key(owner, principal)
    endpoint = endpoints.create_endpoint(
        principal_id=principal, channel="telegram", address="ana-chat"
    )
    endpoints.mark_verified(endpoint, external_recipient_id="chat-ana")
    radar = radars.create_radar(name="Radar PAE", owner=owner, query_frame_json="{}")
    preference = preferences.create_preference(radar_id=radar, endpoint_id=endpoint)
    preferences.set_enabled(preference, True)
    return {
        "db": db, "radars": radars, "identity": identity, "endpoints": endpoints,
        "preferences": preferences, "principal": principal, "endpoint": endpoint,
        "radar": radar, "preference": preference, "owner": owner,
        "store": WorkerStore(db), "outbox": OutboxStore(db), "ledger": LedgerStore(db),
    }


def _sql(env, statement, params=()):
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(statement, params)
    finally:
        conn.close()


def _row(env, nid):
    conn = connect_sqlite(env["db"])
    try:
        return conn.execute(
            "SELECT * FROM radar_notification_batches WHERE notification_id=?", (nid,)
        ).fetchone()
    finally:
        conn.close()


def _attempts(env, nid):
    conn = connect_sqlite(env["db"])
    try:
        return conn.execute(
            "SELECT * FROM radar_delivery_attempts WHERE notification_id=? "
            "ORDER BY attempt_number", (nid,)
        ).fetchall()
    finally:
        conn.close()


def _batch(env, *, created_at="2026-07-15T00:00:00+00:00", expires_at=FOREVER,
           not_before=None, endpoint=None):
    number = next(_seq)
    result = env["outbox"].create_batch(
        principal_id=env["principal"], endpoint_id=endpoint or env["endpoint"],
        radar_id=env["radar"], payload_json={"v": 1, "n": number},
        events=[BatchEventRef(f"ev-{number}", number)],
        created_at=created_at, expires_at=expires_at, not_before=not_before,
    )
    return result.notification_id


def _p1b2_claim(env, nid, *, now=NOW, duration=DURATION):
    """Reclama por el camino de P1B-2, que no excluye dispatching: es como se
    fabrican los estados que el worker tiene que saber rechazar."""
    conn = connect_sqlite(env["db"])
    try:
        return claim.try_claim(conn, nid, now, duration)
    finally:
        conn.close()


def _open_dispatching(env, nid, *, now=NOW, duration=DURATION):
    """Deja el batch 'claimed' con un intento 'dispatching' abierto."""
    token = _p1b2_claim(env, nid, now=now, duration=duration)
    assert token is not None
    attempt_id = env["ledger"].begin_attempt(nid, token, now=now)
    return token, attempt_id


def _force_status(env, nid, status):
    """Simula lo que INV-W25 prohíbe al worker pero un operador puede hacer."""
    _sql(env, "UPDATE radar_notification_batches SET status=?, claim_token=NULL, "
              "claim_until=NULL WHERE notification_id=?", (status, nid))


def _authorized_claim(env, nid, *, now=NOW):
    claimed = env["store"].claim_candidate(
        clock=_Clock(now), duration=DURATION, notification_id=nid
    )
    assert claimed is not None
    return claimed


# ══ C · Schema aditivo (22–27) ═══════════════════════════════════════════════════

def test_22_partial_unique_index_on_external_key_exists(env):
    """22 · La unicidad de la clave externa la hace cumplir la base, no el worker."""
    conn = connect_sqlite(env["db"])
    try:
        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND name='idx_batches_external_idem'"
        ).fetchone()
    finally:
        conn.close()
    assert sql is not None, "no existe idx_batches_external_idem"
    text = " ".join(sql["sql"].split()).lower()
    assert "unique" in text
    assert "external_idempotency_key is not null" in text


def test_23_multiple_null_external_keys_are_allowed(env):
    """23 · Todo batch nace sin clave externa: el índice único parcial no puede
    convertir "aún no autorizado" en un conflicto."""
    ids = [_batch(env) for _ in range(5)]
    conn = connect_sqlite(env["db"])
    try:
        nulls = conn.execute(
            "SELECT COUNT(*) AS n FROM radar_notification_batches "
            "WHERE external_idempotency_key IS NULL"
        ).fetchone()["n"]
    finally:
        conn.close()
    assert nulls == len(ids) == 5


def test_24_duplicate_non_null_external_keys_are_rejected(env):
    """24 · Dos batches con la misma clave externa serían dos mensajes que el
    proveedor creería el mismo. La base lo impide."""
    first, second = _batch(env), _batch(env)
    key = external_idempotency_key(first)
    _sql(env, "UPDATE radar_notification_batches SET external_idempotency_key=? "
              "WHERE notification_id=?", (key, first))
    with pytest.raises(sqlite3.IntegrityError):
        _sql(env, "UPDATE radar_notification_batches SET external_idempotency_key=? "
                  "WHERE notification_id=?", (key, second))


def test_25_repeated_initialization_is_idempotent(env):
    """25 · ensure_schema corre en cada constructor de cada store."""
    before = _index_names(env)
    for _ in range(5):
        WorkerStore(env["db"])
        LedgerStore(env["db"])
        OutboxStore(env["db"])
    assert _index_names(env) == before
    assert "idx_batches_external_idem" in before
    assert "idx_batches_expiry" in before


def _index_names(env):
    conn = connect_sqlite(env["db"])
    try:
        return {
            r["name"] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' "
                "AND tbl_name='radar_notification_batches'"
            ).fetchall()
        }
    finally:
        conn.close()


def test_26_preexisting_database_opens_without_recreation(env):
    """26 · Los índices son aditivos: una base con datos previos los gana sin ALTER,
    sin recrear tablas y sin backfill. Las filas viejas conservan su clave NULL."""
    legacy = _batch(env)
    _p1b2_claim(env, legacy)
    conn = connect_sqlite(env["db"])
    try:
        conn.execute("DROP INDEX IF EXISTS idx_batches_external_idem")
        conn.execute("DROP INDEX IF EXISTS idx_batches_expiry")
        count_before = conn.execute(
            "SELECT COUNT(*) AS n FROM radar_notification_batches"
        ).fetchone()["n"]
    finally:
        conn.close()

    WorkerStore(env["db"])  # abre una base "vieja"

    assert {"idx_batches_external_idem", "idx_batches_expiry"} <= _index_names(env)
    row = _row(env, legacy)
    assert row["external_idempotency_key"] is None
    assert row["status"] == "claimed"
    conn = connect_sqlite(env["db"])
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM radar_notification_batches"
        ).fetchone()["n"] == count_before
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def test_27_expiry_index_is_used_by_the_sweep_query_plan(env):
    """27 · El índice de expiración no es especulativo: el plan del sweep lo usa.
    ``idx_batches_claimable(status, not_before)`` no sirve — su segunda columna es la
    equivocada."""
    for _ in range(30):
        _batch(env, expires_at="2026-01-01T00:00:00+00:00")
    conn = connect_sqlite(env["db"])
    try:
        conn.execute("ANALYZE")
        plan = conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT b.notification_id FROM radar_notification_batches b "
            " WHERE b.status='pending' AND b.expires_at <= ? "
            " ORDER BY b.expires_at, b.created_at, b.notification_id LIMIT 100",
            (NOW.isoformat(),),
        ).fetchall()
    finally:
        conn.close()
    text = " ".join(" ".join(str(c) for c in row) for row in plan)
    assert "idx_batches_expiry" in text, f"el plan no usa el índice: {text}"


# ══ D · claim_candidate (28–41) ══════════════════════════════════════════════════

def test_28_due_pending_batch_is_claimable(env):
    """28 · El caso normal: pending, vivo, sin cuarentena."""
    nid = _batch(env)
    clock = _Clock(NOW)
    claimed = env["store"].claim_candidate(clock=clock, duration=DURATION)
    assert claimed is not None
    assert claimed.notification_id == nid
    assert claimed.claimed_at == NOW.isoformat()
    assert claimed.claim_until == (NOW + DURATION).isoformat()
    row = _row(env, nid)
    assert row["status"] == "claimed"
    assert row["claim_token"] == claimed.claim_token
    assert row["claim_until"] == claimed.claim_until


def test_29_quiet_hours_batch_is_not_claimable(env):
    """29 · not_before futuro = cuarentena: quiet hours, hold o security. Reclamarlo
    sería enviar a las 3 de la mañana."""
    nid = _batch(env, not_before=(NOW + timedelta(hours=5)).isoformat())
    assert env["store"].claim_candidate(clock=_Clock(NOW), duration=DURATION) is None
    assert _row(env, nid)["status"] == "pending"


def test_30_expired_batch_is_not_claimable(env):
    """30 · Vencido no se reclama: no se empieza un envío que ya no vale. Ese es del
    sweep."""
    nid = _batch(env, expires_at=(NOW - timedelta(seconds=1)).isoformat())
    assert env["store"].claim_candidate(clock=_Clock(NOW), duration=DURATION) is None
    assert _row(env, nid)["status"] == "pending"


def test_31_batch_with_open_dispatching_is_not_claimable(env):
    """31 · Un intento abierto significa que alguien ya llamó (o iba a llamar) al
    proveedor. Reclamarlo por el camino normal produciría un segundo envío del mismo
    mensaje: esos batches son de la reconciliación, no del camino normal."""
    nid = _batch(env)
    _open_dispatching(env, nid, now=NOW, duration=timedelta(seconds=10))
    # El claim del dueño anterior ya venció: sin la exclusión, éste sería reclamable.
    assert env["store"].claim_candidate(
        clock=_Clock(NOW + timedelta(seconds=60)), duration=DURATION
    ) is None
    assert env["store"].claim_candidate(
        clock=_Clock(NOW + timedelta(seconds=60)), duration=DURATION,
        notification_id=nid,
    ) is None


def test_31b_pending_with_open_dispatching_is_not_claimable(env):
    """31b · Y tampoco el huérfano heredado (pending + dispatching)."""
    nid = _batch(env)
    _open_dispatching(env, nid)
    _force_status(env, nid, "pending")
    assert env["store"].claim_candidate(clock=_Clock(NOW), duration=DURATION) is None


def test_32_expired_claim_without_dispatching_is_recoverable(env):
    """32 · El dueño anterior murió sin escribir nada: no hay intento, así que
    reclamar y reintentar es seguro."""
    nid = _batch(env)
    old_token = _p1b2_claim(env, nid, now=NOW, duration=timedelta(seconds=10))
    later = NOW + timedelta(seconds=60)
    claimed = env["store"].claim_candidate(clock=_Clock(later), duration=DURATION)
    assert claimed is not None and claimed.notification_id == nid
    assert claimed.claim_token != old_token
    assert _row(env, nid)["claim_token"] == claimed.claim_token


def test_33_live_claim_is_not_stealable(env):
    """33 · Un claim vigente es la promesa de que alguien está trabajando."""
    nid = _batch(env)
    token = _p1b2_claim(env, nid, now=NOW, duration=timedelta(seconds=120))
    assert env["store"].claim_candidate(
        clock=_Clock(NOW + timedelta(seconds=30)), duration=DURATION
    ) is None
    assert _row(env, nid)["claim_token"] == token


def test_34_candidate_order_is_deterministic(env):
    """34 · Orden total: COALESCE(not_before, created_at), created_at,
    notification_id. Sin el tercer desempate el orden dependería del plan de SQLite y
    un drain no sería reproducible."""
    base = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    early = _batch(env, created_at=base.isoformat())
    late = _batch(env, created_at=(base + timedelta(hours=2)).isoformat())
    # Creado tarde pero con not_before temprano: manda el efectivo, no el nacimiento.
    quarantined = _batch(
        env, created_at=(base + timedelta(hours=3)).isoformat(),
        not_before=(base - timedelta(hours=1)).isoformat(),
    )
    order = env["store"].list_candidates(now=NOW, limit=10)
    assert order == [quarantined, early, late]
    for _ in range(5):
        assert env["store"].list_candidates(now=NOW, limit=10) == order
    claimed = env["store"].claim_candidate(clock=_Clock(NOW), duration=DURATION)
    assert claimed.notification_id == quarantined


def test_34b_candidate_order_breaks_ties_by_notification_id(env):
    """34b · Mismo created_at y mismo not_before: decide notification_id."""
    stamp = "2026-07-15T08:00:00+00:00"
    ids = sorted(_batch(env, created_at=stamp) for _ in range(6))
    assert env["store"].list_candidates(now=NOW, limit=10) == ids


@pytest.mark.parametrize("maker,label", [
    (lambda env: _batch(env, expires_at=(NOW - timedelta(seconds=1)).isoformat()),
     "expirado"),
    (lambda env: _batch(env, not_before=(NOW + timedelta(hours=5)).isoformat()),
     "cuarentena"),
])
def test_35_targeted_claim_keeps_every_predicate(env, maker, label):
    """35 · Pedir un batch por id no es una puerta trasera: se aplican los mismos
    predicados que en la selección automática."""
    nid = maker(env)
    assert env["store"].claim_candidate(
        clock=_Clock(NOW), duration=DURATION, notification_id=nid
    ) is None, f"un claim dirigido se saltó el predicado: {label}"
    assert _row(env, nid)["status"] == "pending"


def test_35b_targeted_claim_rejects_terminal_states(env):
    """35b · sent/ambiguous/dead no se reclaman ni pidiéndolos por id."""
    for status in ("sent", "ambiguous", "dead"):
        nid = _batch(env)
        _force_status(env, nid, status)
        assert env["store"].claim_candidate(
            clock=_Clock(NOW), duration=DURATION, notification_id=nid
        ) is None
        assert _row(env, nid)["status"] == status


def test_35c_targeted_claim_rejects_live_claim_and_unknown_id(env):
    """35c · Un id inexistente devuelve None, no una excepción ni un claim fantasma."""
    nid = _batch(env)
    _p1b2_claim(env, nid, now=NOW, duration=timedelta(seconds=120))
    assert env["store"].claim_candidate(
        clock=_Clock(NOW), duration=DURATION, notification_id=nid
    ) is None
    assert env["store"].claim_candidate(
        clock=_Clock(NOW), duration=DURATION, notification_id="no-existe"
    ) is None


def test_36_two_claimers_exactly_one_wins(env):
    """36 · La exclusión mutua la decide el UPDATE condicional por rowcount."""
    nid = _batch(env)
    results, barrier = [], threading.Barrier(2)
    lock = threading.Lock()

    def attempt():
        store = WorkerStore(env["db"])  # conexión propia por hilo
        barrier.wait(timeout=JOIN_TIMEOUT)
        claimed = store.claim_candidate(clock=_Clock(NOW), duration=DURATION)
        with lock:
            results.append(claimed)

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=JOIN_TIMEOUT)
        assert not t.is_alive(), "hilo colgado"
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert _row(env, nid)["claim_token"] == winners[0].claim_token


def test_37_claim_race_repeated_50_times(env):
    """37 · 50 iteraciones: exactamente un ganador cada vez, ningún hilo vivo."""
    for iteration in range(RACE_ITERATIONS):
        nid = _batch(env)
        results, barrier = [], threading.Barrier(4)
        lock = threading.Lock()

        def attempt():
            store = WorkerStore(env["db"])
            barrier.wait(timeout=JOIN_TIMEOUT)
            claimed = store.claim_candidate(
                clock=_Clock(NOW), duration=DURATION, notification_id=nid
            )
            with lock:
                results.append(claimed)

        threads = [threading.Thread(target=attempt) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=JOIN_TIMEOUT)
            assert not t.is_alive(), f"hilo colgado en iteración {iteration}"
        winners = [r for r in results if r is not None]
        assert len(winners) == 1, f"iteración {iteration}: {len(winners)} ganadores"
        assert _row(env, nid)["claim_token"] == winners[0].claim_token
        _force_status(env, nid, "sent")  # sácalo del camino de la siguiente vuelta


def test_38_clock_is_called_after_the_lock_is_acquired(env):
    """38 · **INV-W26**, medido por orden, no por carrera.

    ``after_begin`` se dispara en el instante exacto en que el lock de escritor ya es
    nuestro. El reloj se registra cuando lo invocan. Si la captura vive tras el
    ``BEGIN IMMEDIATE``, el orden es ["lock", "clock"]; si alguien la mueve antes, es
    ["clock", "lock"]. Un solo hilo, sin sincronización y sin nada que ganar: el orden
    es una propiedad del código, no del planificador.

    (La primera versión de este test medía la carrera con un holder externo y **no
    mataba** al mutante: abrir la conexión hace tres PRAGMA, así que el hilo principal
    casi siempre liberaba el lock antes de que el mutante llegara a su reloj. Pasaba
    por suerte, no por el invariante.)
    """
    nid = _batch(env)
    order: list[str] = []

    def hook(phase):
        if phase == PHASE_AFTER_BEGIN:
            order.append("lock")

    def clock():
        order.append("clock")
        return NOW

    claimed = env["store"].claim_candidate(
        clock=clock, duration=DURATION, notification_id=nid, _fault_hook=hook
    )
    assert claimed is not None
    assert order == ["lock", "clock"], (
        f"el reloj se capturó en el orden equivocado: {order}. Capturarlo antes del "
        f"lock haría que la espera por el lock consumiera la ventana del claim."
    )


def test_38b_reconciliation_clock_is_called_after_the_lock(env):
    """38b · El mismo orden en el claim de reconciliación."""
    nid, _ = _orphan_for_clock_order(env)
    order: list[str] = []

    def hook(phase):
        if phase == PHASE_AFTER_BEGIN:
            order.append("lock")

    def clock():
        order.append("clock")
        return NOW + timedelta(seconds=60)

    recon = env["store"].claim_for_reconciliation(
        clock=clock, duration=RECON_DURATION, notification_id=nid, _fault_hook=hook
    )
    assert recon is not None
    assert order == ["lock", "clock"]


def _orphan_for_clock_order(env):
    nid = _batch(env)
    _, attempt_id = _open_dispatching(env, nid, now=NOW, duration=timedelta(seconds=10))
    return nid, attempt_id


def test_38c_claim_until_reflects_the_post_lock_instant_under_contention(env):
    """38c · Y el efecto observable bajo contención real: mientras otra conexión
    retiene el lock, el reloj avanza; cuando el worker por fin es dueño, ``claim_until``
    mide desde ese instante, no desde antes de pedirlo. Complementa a test_38: aquel
    fija el orden, éste comprueba que la ventana no se consume esperando.
    """
    for iteration in range(RACE_ITERATIONS):
        nid = _batch(env)
        released = threading.Event()
        about_to_claim = threading.Barrier(2)
        observed: list[bool] = []

        def clock():
            observed.append(released.is_set())
            return LATER if released.is_set() else NOW

        holder = connect_sqlite(env["db"])
        holder.execute("BEGIN IMMEDIATE")
        outcome = {}

        def worker():
            store = WorkerStore(env["db"])
            about_to_claim.wait(timeout=JOIN_TIMEOUT)
            outcome["claim"] = store.claim_candidate(
                clock=clock, duration=DURATION, notification_id=nid
            )

        thread = threading.Thread(target=worker)
        thread.start()
        try:
            about_to_claim.wait(timeout=JOIN_TIMEOUT)
            released.set()
            holder.execute("COMMIT")
        finally:
            holder.close()
        thread.join(timeout=JOIN_TIMEOUT)
        assert not thread.is_alive(), f"hilo colgado en iteración {iteration}"

        assert observed == [True], (
            f"iteración {iteration}: el reloj se invocó con el lock ajeno todavía "
            f"retenido ({observed})"
        )
        claimed = outcome["claim"]
        assert claimed is not None
        assert claimed.claimed_at == LATER.isoformat()
        assert claimed.claim_until == (LATER + DURATION).isoformat()
        _force_status(env, nid, "sent")


def test_39_claim_until_is_computed_from_the_post_lock_clock(env):
    """39 · claim_until = (instante capturado tras el lock) + duration, y el reloj se
    invoca exactamente una vez: dos capturas darían dos verdades sobre cuándo empezó
    la propiedad."""
    nid = _batch(env)
    clock = _Clock(LATER)
    claimed = env["store"].claim_candidate(clock=clock, duration=DURATION)
    assert len(clock.calls) == 1
    assert claimed.claimed_at == LATER.isoformat()
    assert claimed.claim_until == (LATER + DURATION).isoformat()
    assert _row(env, nid)["claim_until"] == (LATER + DURATION).isoformat()


def test_40_naive_clock_is_rejected_and_rolls_back(env):
    """40 · Un instante naive comparado contra marcas UTC ordena mal: el claim
    vencería cuando no debe. Se rechaza sin dejar rastro."""
    nid = _batch(env)
    naive = datetime(2026, 7, 15, 12, 0)
    with pytest.raises(WorkerStoreError, match="zona horaria"):
        env["store"].claim_candidate(clock=lambda: naive, duration=DURATION)
    row = _row(env, nid)
    assert row["status"] == "pending"
    assert row["claim_token"] is None and row["claim_until"] is None
    # Y la base sigue escribible: el ROLLBACK cerró la transacción de verdad.
    assert env["store"].claim_candidate(
        clock=_Clock(NOW), duration=DURATION
    ) is not None


def test_40b_non_datetime_clock_is_rejected(env):
    """40b · Un reloj que devuelve cualquier otra cosa falla cerrado."""
    _batch(env)
    with pytest.raises(WorkerStoreError):
        env["store"].claim_candidate(clock=lambda: "2026-07-15", duration=DURATION)


@pytest.mark.parametrize("bad", [
    timedelta(0), timedelta(seconds=-1), 120, True, "120s", None,
])
def test_41_invalid_duration_is_rejected(env, bad):
    """41 · Duración cero o negativa daría un claim vencido al nacer; un int no dice
    su unidad; bool es el error de copiar una firma con keywords."""
    nid = _batch(env)
    with pytest.raises(WorkerStoreError):
        env["store"].claim_candidate(clock=_Clock(NOW), duration=bad)
    assert _row(env, nid)["status"] == "pending"


# ══ E · authorize_and_begin_dispatch (42–66) ═════════════════════════════════════

def test_42_authorized_cycle_creates_exactly_one_dispatching(env):
    """42 · La salida autorizada: un intento 'dispatching' abierto, con outcome NULL
    porque todavía no ha pasado nada."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert result.outcome == OUTCOME_AUTHORIZED
    assert result.reason is None and result.dead_reason is None
    assert result.channel == "telegram"
    assert result.external_recipient_id == "chat-ana"
    assert result.payload_json is not None
    assert result.claim_token == claimed.claim_token

    attempts = _attempts(env, nid)
    assert len(attempts) == 1
    assert attempts[0]["attempt_id"] == result.attempt_id
    assert attempts[0]["phase"] == "dispatching"
    assert attempts[0]["outcome"] is None
    assert attempts[0]["started_at"] == NOW.isoformat()
    assert env["store"].count_open_dispatching(nid) == 1
    # El batch sigue reclamado por el mismo token: autorizar no libera nada.
    row = _row(env, nid)
    assert row["status"] == "claimed"
    assert row["claim_token"] == claimed.claim_token


def test_43_authorization_increments_attempt_count(env):
    """43 · attempt_count cuenta llamadas al proveedor, nada más."""
    nid = _batch(env)
    assert _row(env, nid)["attempt_count"] == 0
    claimed = _authorized_claim(env, nid)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert _row(env, nid)["attempt_count"] == 1
    assert _attempts(env, nid)[0]["attempt_number"] == 1
    assert result.attempt_id is not None


def test_44_external_key_matches_the_formula(env):
    """44 · sha256("p1b-extid-v1|" + notification_id): sin PII, sin el texto, sin el
    intento."""
    import hashlib

    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    expected = hashlib.sha256(f"p1b-extid-v1|{nid}".encode()).hexdigest()
    assert result.external_idempotency_key == expected
    assert _row(env, nid)["external_idempotency_key"] == expected
    # No depende del destinatario: la misma fórmula sobre el mismo id, sin base.
    assert external_idempotency_key(nid) == expected
    assert "chat-ana" not in expected


def test_45_external_key_is_stable_across_retries(env):
    """45 · **INV-W8.** Write-once: el segundo intento reutiliza la clave del
    primero. Si cambiara, la idempotencia externa no serviría para nada — que es
    justo para lo que existe."""
    nid = _batch(env)
    first_claim = _authorized_claim(env, nid)
    first = env["store"].authorize_and_begin_dispatch(
        nid, first_claim.claim_token, clock=_Clock(NOW)
    )
    env["ledger"].finish_attempt(
        first.attempt_id, nid, first_claim.claim_token, outcome="retry",
        next_not_before=None, now=NOW,
    )
    assert _row(env, nid)["status"] == "pending"

    second_claim = _authorized_claim(env, nid, now=LATER)
    second = env["store"].authorize_and_begin_dispatch(
        nid, second_claim.claim_token, clock=_Clock(LATER)
    )
    assert second.external_idempotency_key == first.external_idempotency_key
    assert _row(env, nid)["external_idempotency_key"] == first.external_idempotency_key
    assert _row(env, nid)["attempt_count"] == 2
    assert len(_attempts(env, nid)) == 2


def test_45b_preexisting_foreign_key_fails_closed(env):
    """45b · Si la clave almacenada no es la que la fórmula produce, algo la cambió:
    enviar con una clave ajena es peor que no enviar."""
    nid = _batch(env)
    _sql(env, "UPDATE radar_notification_batches SET external_idempotency_key=? "
              "WHERE notification_id=?", ("clave-ajena", nid))
    claimed = _authorized_claim(env, nid)
    with pytest.raises(WorkerStoreConsistencyError, match="write-once"):
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token, clock=_Clock(NOW)
        )
    assert _row(env, nid)["external_idempotency_key"] == "clave-ajena"
    assert _attempts(env, nid) == []


@pytest.mark.parametrize("phase", [
    PHASE_AFTER_BEGIN, PHASE_AFTER_CLAIM_VALIDATION, PHASE_AFTER_AUTHORIZATION_READ,
    PHASE_AFTER_EXTERNAL_KEY, PHASE_AFTER_ATTEMPT_COUNT, PHASE_AFTER_ATTEMPT_INSERT,
    PHASE_BEFORE_COMMIT,
])
def test_46_key_and_attempt_roll_back_together(env, phase):
    """46 · **INV-W27.** Un fallo en cualquier punto anterior al COMMIT deja el mundo
    exactamente como estaba: sin clave, sin intento, sin contador. Si la clave se
    persistiera en una transacción aparte, las fases posteriores a ella dejarían una
    clave sin intento — el estado que §A.4 declara irrepresentable."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)

    def hook(fired):
        if fired == phase:
            raise RuntimeError(f"fallo inyectado en {phase}")

    with pytest.raises(RuntimeError):
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token, clock=_Clock(NOW), _fault_hook=hook
        )
    row = _row(env, nid)
    assert row["external_idempotency_key"] is None, f"clave persistida tras {phase}"
    assert row["attempt_count"] == 0, f"attempt_count incrementado tras {phase}"
    assert _attempts(env, nid) == [], f"intento creado tras {phase}"
    assert env["store"].count_open_dispatching(nid) == 0
    # El batch conserva su claim previo: el fallo no se lo robó a nadie.
    assert row["status"] == "claimed"
    assert row["claim_token"] == claimed.claim_token
    assert row["claim_until"] == claimed.claim_until


def test_47_failure_after_key_leaves_both_untouched(env):
    """47 · El caso que separa una transacción de dos: si la clave viviera en su
    propia transacción, aquí quedaría persistida y sin intento."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)

    def hook(fired):
        if fired == PHASE_AFTER_EXTERNAL_KEY:
            raise RuntimeError("muere justo después de fijar la clave")

    with pytest.raises(RuntimeError):
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token, clock=_Clock(NOW), _fault_hook=hook
        )
    assert _row(env, nid)["external_idempotency_key"] is None
    assert _attempts(env, nid) == []


def test_48_failure_after_attempt_count_rolls_back(env):
    """48 · attempt_count incrementado sin intento diría que llamamos al proveedor."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)

    def hook(fired):
        if fired == PHASE_AFTER_ATTEMPT_COUNT:
            raise RuntimeError("muere entre el contador y el INSERT")

    with pytest.raises(RuntimeError):
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token, clock=_Clock(NOW), _fault_hook=hook
        )
    assert _row(env, nid)["attempt_count"] == 0
    assert _attempts(env, nid) == []


def test_49_failure_after_attempt_insert_rolls_back(env):
    """49 · Ya insertado pero sin COMMIT: no existe. La frontera es el COMMIT."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)

    def hook(fired):
        if fired == PHASE_AFTER_ATTEMPT_INSERT:
            raise RuntimeError("muere con el INSERT hecho y sin commitear")

    with pytest.raises(RuntimeError):
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token, clock=_Clock(NOW), _fault_hook=hook
        )
    assert _attempts(env, nid) == []
    assert _row(env, nid)["external_idempotency_key"] is None
    assert env["store"].count_open_dispatching(nid) == 0


def test_49b_after_commit_hook_runs_outside_the_transaction(env):
    """49b · after_commit es para orquestar tests, y por eso corre cuando el trabajo
    ya es durable: si se disparara dentro, la fase mentiría sobre dónde está."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    seen = {}

    def hook(fired):
        if fired == PHASE_AFTER_COMMIT:
            seen["attempts"] = len(_attempts(env, nid))
            seen["key"] = _row(env, nid)["external_idempotency_key"]

    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW), _fault_hook=hook
    )
    assert result.outcome == OUTCOME_AUTHORIZED
    # Visible desde otra conexión ⇒ commiteado de verdad, y sin bloquear.
    assert seen == {"attempts": 1, "key": result.external_idempotency_key}


def test_50_expired_claim_changes_nothing(env):
    """50 · **INV-W5.** El claim venció durante el trabajo: no somos dueños. Cero
    cambios, y otro worker lo recuperará."""
    nid = _batch(env)
    claimed = env["store"].claim_candidate(
        clock=_Clock(NOW), duration=timedelta(seconds=10), notification_id=nid
    )
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW + timedelta(seconds=60))
    )
    assert result.outcome == OUTCOME_CLAIM_LOST
    assert result.attempt_id is None
    assert result.external_idempotency_key is None
    row = _row(env, nid)
    assert row["status"] == "claimed"           # intacto
    assert row["claim_token"] == claimed.claim_token
    assert row["external_idempotency_key"] is None
    assert row["attempt_count"] == 0
    assert _attempts(env, nid) == []


def test_51_stale_token_changes_nothing(env):
    """51 · Otro worker se llevó el claim: nuestro token viejo no escribe sobre el
    trabajo de su sucesor."""
    nid = _batch(env)
    stale = _authorized_claim(env, nid)
    _sql(env, "UPDATE radar_notification_batches SET claim_token='token-del-sucesor' "
              "WHERE notification_id=?", (nid,))
    result = env["store"].authorize_and_begin_dispatch(
        nid, stale.claim_token, clock=_Clock(NOW)
    )
    assert result.outcome == OUTCOME_CLAIM_LOST
    row = _row(env, nid)
    assert row["claim_token"] == "token-del-sucesor"
    assert row["external_idempotency_key"] is None
    assert _attempts(env, nid) == []


def test_51b_unknown_batch_is_claim_lost(env):
    """51b · Un id inexistente no revienta: es un claim que no tenemos."""
    result = env["store"].authorize_and_begin_dispatch(
        "no-existe", "tok", clock=_Clock(NOW)
    )
    assert result.outcome == OUTCOME_CLAIM_LOST


@pytest.mark.parametrize("mutate,reason,dead_reason", [
    ("UPDATE radar_notification_preferences SET enabled=0", "preference_disabled",
     "opted_out"),
    ("DELETE FROM radar_notification_preferences", "preference_not_found",
     "opted_out"),
    ("UPDATE notification_endpoints SET revoked_at='2026-07-15T11:00:00+00:00'",
     "endpoint_revoked", "endpoint_revoked"),
    ("UPDATE radars SET archived_at='2026-07-15T11:00:00+00:00'", "radar_archived",
     "radar_archived"),
])
def test_52_53_54_terminal_causes_kill_without_attempt(env, mutate, reason,
                                                        dead_reason):
    """52/53/54 · **INV-W17.** Las causas terminales matan el batch sin crear
    intento: "intento ⇔ llamada al proveedor" es lo que hace legible el ledger."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    _sql(env, mutate)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert result.outcome == OUTCOME_DEAD
    assert (result.reason, result.dead_reason) == (reason, dead_reason)
    assert result.attempt_id is None
    row = _row(env, nid)
    assert row["status"] == "dead"
    assert row["dead_reason"] == dead_reason
    assert row["claim_token"] is None and row["claim_until"] is None
    assert row["external_idempotency_key"] is None
    assert row["attempt_count"] == 0
    assert _attempts(env, nid) == []


@pytest.mark.parametrize("mutate,reason", [
    ("UPDATE notification_endpoints SET enabled=0", "endpoint_disabled"),
    ("UPDATE notification_endpoints SET verified_at=NULL", "endpoint_unverified"),
    ("UPDATE notification_endpoints SET external_recipient_id=''",
     "endpoint_missing_recipient"),
    ("UPDATE principals SET status='suspended'", "principal_suspended"),
    ("DELETE FROM principal_owner_keys", "owner_not_linked"),
])
def test_55_56_57_58_59_hold_causes_wait_without_attempt(env, mutate, reason):
    """55–59 · **INV-W18.** Las causas reversibles devuelven el batch a pending con
    cuarentena. Nunca matan (la condición se resuelve) y nunca crean intento."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    _sql(env, mutate)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert result.outcome == OUTCOME_HOLD
    assert result.reason == reason
    assert result.dead_reason is None
    assert result.attempt_id is None
    row = _row(env, nid)
    assert row["status"] == "pending"
    assert row["dead_reason"] is None
    assert row["claim_token"] is None and row["claim_until"] is None
    assert row["not_before"] == (NOW + DEFAULT_HOLD_DURATION).isoformat()
    assert row["external_idempotency_key"] is None
    assert row["attempt_count"] == 0
    assert _attempts(env, nid) == []
    # Y el claim liberado deja el batch en cuarentena, no reclamable ya mismo.
    assert env["store"].claim_candidate(clock=_Clock(NOW), duration=DURATION) is None


def test_60_principal_mismatch_is_security_blocked_without_attempt(env):
    """60 · **INV-W22.** El endpoint es de otra persona que la dueña del radar: cero
    llamada, y una sola evaluación. No es un hold de quince minutos porque la
    condición no se resuelve sola — reevaluarla en bucle sería martillear un fallo de
    aislamiento en vez de señalarlo."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    otra = env["identity"].create_principal("beto@example.com")
    _sql(env, "UPDATE principal_owner_keys SET principal_id=? WHERE owner_key=?",
         (otra, env["owner"]))

    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert result.outcome == OUTCOME_SECURITY_BLOCKED
    assert result.reason == "principal_mismatch"
    assert result.attempt_id is None
    row = _row(env, nid)
    assert row["status"] == "pending"
    assert row["not_before"] == row["expires_at"]      # una evaluación, no un bucle
    assert row["not_before"] != (NOW + DEFAULT_HOLD_DURATION).isoformat()
    assert row["claim_token"] is None
    assert row["external_idempotency_key"] is None
    assert _attempts(env, nid) == []


def test_61_hold_never_schedules_past_expiration(env):
    """61 · Si la espera desbordara el TTL, el batch parecería "en espera" cuando en
    realidad ya murió. Se duerme hasta el TTL y decide el sweep."""
    expires = (NOW + timedelta(minutes=5)).isoformat()   # menos que HOLD_INTERVAL
    nid = _batch(env, expires_at=expires)
    claimed = _authorized_claim(env, nid)
    _sql(env, "UPDATE notification_endpoints SET enabled=0")
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert result.outcome == OUTCOME_HOLD
    row = _row(env, nid)
    assert row["not_before"] == expires
    assert row["not_before"] < (NOW + DEFAULT_HOLD_DURATION).isoformat()
    # Y en el instante en que despierta, ya está vencido: lo barre el sweep.
    assert env["store"].list_candidates(
        now=NOW + timedelta(minutes=10), limit=10
    ) == []
    assert env["store"].list_sweepable(
        now=NOW + timedelta(minutes=10), limit=10
    ) == [nid]


def test_61b_hold_within_ttl_uses_the_hold_interval(env):
    """61b · El clamp solo actúa cuando desborda: con TTL de sobra, la espera es la
    normal."""
    nid = _batch(env, expires_at=(NOW + timedelta(hours=12)).isoformat())
    claimed = _authorized_claim(env, nid)
    _sql(env, "UPDATE notification_endpoints SET enabled=0")
    env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert _row(env, nid)["not_before"] == (NOW + DEFAULT_HOLD_DURATION).isoformat()


def test_62_revocation_wins_linearization_zero_attempts(env):
    """62 · **INV-W11.** La revocación commitea antes de que la autorización adquiera
    el lock: la autorización la lee y sale por dead/endpoint_revoked. Cero intentos."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    env["endpoints"].revoke(env["endpoint"])
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert result.outcome == OUTCOME_DEAD
    assert result.dead_reason == "endpoint_revoked"
    assert _attempts(env, nid) == []
    assert env["store"].count_open_dispatching(nid) == 0


def test_63_authorization_wins_linearization_exactly_one_attempt(env):
    """63 · El otro lado del orden total: si la autorización commitea primero, el
    intento queda autorizado y en vuelo. Una revocación posterior bloquea intentos
    *futuros* pero no puede cancelar con certeza el ya autorizado — fingir que sí
    sería la promesa imposible que R1.2 retiró."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert result.outcome == OUTCOME_AUTHORIZED
    env["endpoints"].revoke(env["endpoint"])
    assert len(_attempts(env, nid)) == 1        # sigue exactamente uno
    assert env["store"].count_open_dispatching(nid) == 1
    assert _row(env, nid)["status"] == "claimed"

    # Y ningún intento *nuevo* se autoriza tras la revocación.
    env["ledger"].finish_attempt(
        result.attempt_id, nid, claimed.claim_token, outcome="retry",
        next_not_before=None, now=NOW,
    )
    again = _authorized_claim(env, nid, now=LATER)
    second = env["store"].authorize_and_begin_dispatch(
        nid, again.claim_token, clock=_Clock(LATER)
    )
    assert second.outcome == OUTCOME_DEAD
    assert second.dead_reason == "endpoint_revoked"
    assert len(_attempts(env, nid)) == 1


def test_64_linearization_race_repeated_50_times(env):
    """64 · Revocar y autorizar en paralelo, 50 veces. SQLite en WAL tiene un solo
    escritor, así que hay orden total entre los dos COMMIT y el resultado siempre es
    explicable por quién ganó. Lo que jamás puede ocurrir: un intento creado sobre un
    endpoint que ya constaba revocado."""
    outcomes = {OUTCOME_AUTHORIZED: 0, OUTCOME_DEAD: 0}
    for iteration in range(RACE_ITERATIONS):
        endpoint = env["endpoints"].create_endpoint(
            principal_id=env["principal"], channel="telegram",
            address=f"ana-chat-{iteration}",
        )
        env["endpoints"].mark_verified(endpoint, external_recipient_id="chat-ana")
        preference = env["preferences"].create_preference(
            radar_id=env["radar"], endpoint_id=endpoint
        )
        env["preferences"].set_enabled(preference, True)
        nid = _batch(env, endpoint=endpoint)
        claimed = _authorized_claim(env, nid)

        barrier = threading.Barrier(2)
        box = {}

        def authorize():
            store = WorkerStore(env["db"])
            barrier.wait(timeout=JOIN_TIMEOUT)
            box["result"] = store.authorize_and_begin_dispatch(
                nid, claimed.claim_token, clock=_Clock(NOW)
            )

        def revoke():
            endpoints = EndpointStore(
                env["db"], masker=Masker.from_key("worker-test-key")
            )
            barrier.wait(timeout=JOIN_TIMEOUT)
            endpoints.revoke(endpoint)

        threads = [threading.Thread(target=authorize), threading.Thread(target=revoke)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=JOIN_TIMEOUT)
            assert not t.is_alive(), f"hilo colgado en iteración {iteration}"

        result = box["result"]
        attempts = _attempts(env, nid)
        assert result.outcome in outcomes, f"salida inesperada: {result.outcome}"
        outcomes[result.outcome] += 1
        if result.outcome == OUTCOME_AUTHORIZED:
            # Autorizamos primero: exactamente un intento, nunca dos.
            assert len(attempts) == 1
            assert _row(env, nid)["external_idempotency_key"] is not None
        else:
            # La revocación ganó: ni un intento, ni una clave.
            assert result.dead_reason == "endpoint_revoked"
            assert attempts == []
            assert _row(env, nid)["external_idempotency_key"] is None
    assert sum(outcomes.values()) == RACE_ITERATIONS


def test_65_recipient_never_appears_in_repr_or_errors(env):
    """65 · **INV-W13.** El resultado de la autorización es justo lo que alguien
    loguea entero al depurar el worker. El destino real no puede viajar en un repr ni
    en el mensaje de una excepción."""
    nid = _batch(env)
    _sql(env, "UPDATE notification_endpoints SET external_recipient_id=? "
              "WHERE endpoint_id=?", ("chat-super-secreto-42", env["endpoint"]))
    claimed = _authorized_claim(env, nid)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert result.external_recipient_id == "chat-super-secreto-42"
    assert "chat-super-secreto-42" not in repr(result)
    assert "external_recipient_id" not in repr(result)
    assert "chat-super-secreto-42" not in str(result)

    # Tampoco en el mensaje de un error del propio módulo.
    _sql(env, "UPDATE radar_notification_batches SET external_idempotency_key='x' "
              "WHERE notification_id=?", (nid,))
    otro = _batch(env)
    claim_otro = _authorized_claim(env, otro)
    _sql(env, "UPDATE radar_notification_batches SET external_idempotency_key='y' "
              "WHERE notification_id=?", (otro,))
    with pytest.raises(WorkerStoreConsistencyError) as exc:
        env["store"].authorize_and_begin_dispatch(
            otro, claim_otro.claim_token, clock=_Clock(NOW)
        )
    assert "chat-super-secreto-42" not in str(exc.value)


def test_66_ledger_helper_and_public_api_produce_the_same_attempt_shape(env):
    """66 · La única definición del INSERT: el intento que abre begin_attempt (P1B-2)
    y el que abre la autorización (P1B-4) son indistinguibles salvo por sus ids. Dos
    INSERT distintos del mismo hecho acabarían numerando distinto y attempt_count
    dejaría de contar llamadas."""
    via_ledger = _batch(env)
    token = _p1b2_claim(env, via_ledger, now=NOW)
    ledger_attempt = env["ledger"].begin_attempt(
        via_ledger, token, started_at=NOW.isoformat(), now=NOW
    )

    via_worker = _batch(env)
    claimed = _authorized_claim(env, via_worker)
    worker_result = env["store"].authorize_and_begin_dispatch(
        via_worker, claimed.claim_token, clock=_Clock(NOW)
    )

    a = dict(_attempts(env, via_ledger)[0])
    b = dict(_attempts(env, via_worker)[0])
    assert a.pop("attempt_id") == ledger_attempt
    assert b.pop("attempt_id") == worker_result.attempt_id
    assert a.pop("notification_id") == via_ledger
    assert b.pop("notification_id") == via_worker
    assert a == b
    assert a["attempt_number"] == 1
    assert a["phase"] == "dispatching"
    assert a["outcome"] is None
    assert _row(env, via_ledger)["attempt_count"] == 1
    assert _row(env, via_worker)["attempt_count"] == 1


def test_66b_ledger_helper_is_not_a_public_door(env):
    """66b · El helper no puede ser público: una puerta que inserte intentos sin que
    nadie haya comprobado el claim sería exactamente el atajo que el ledger existe
    para no tener."""
    import app.core.notify.ledger_store as ledger_module

    assert not hasattr(ledger_module.LedgerStore,
                       "insert_dispatching_attempt_on_connection")
    assert "_insert_dispatching_attempt_on_connection" not in (
        getattr(ledger_module, "__all__", []) or []
    )
    assert ledger_module._insert_dispatching_attempt_on_connection.__name__.startswith("_")


# ══ F · claim_for_reconciliation (67–80) ═════════════════════════════════════════

def _orphan(env, *, expired=False, inherited=False):
    """Un huérfano: intento abierto y claim no vigente. `inherited` fabrica la forma
    pending+dispatching que INV-W25 prohíbe al worker pero un operador puede crear."""
    expires = (NOW - timedelta(hours=1)).isoformat() if expired else FOREVER
    nid = _batch(env, expires_at=FOREVER)
    _, attempt_id = _open_dispatching(env, nid, now=NOW, duration=timedelta(seconds=10))
    if inherited:
        _force_status(env, nid, "pending")
    if expired:
        _sql(env, "UPDATE radar_notification_batches SET expires_at=? "
                  "WHERE notification_id=?", (expires, nid))
    return nid, attempt_id


def test_67_reconciles_expired_claim_with_dispatching(env):
    """67 · Forma A: claimed con claim vencido + intento abierto. El worker murió
    entre autorizar y cerrar; su destino es desconocido y lo seguirá siendo."""
    nid, attempt_id = _orphan(env)
    later = NOW + timedelta(seconds=60)
    recon = env["store"].claim_for_reconciliation(
        clock=_Clock(later), duration=RECON_DURATION
    )
    assert recon is not None
    assert recon.notification_id == nid
    assert recon.attempt_id == attempt_id
    assert recon.claim_until == (later + RECON_DURATION).isoformat()
    row = _row(env, nid)
    assert row["status"] == "claimed"
    assert row["claim_token"] == recon.claim_token


def test_68_reconciles_inherited_pending_with_dispatching(env):
    """68 · Forma B (C-INHERIT): pending + intento abierto. Ni el sweep ni el camino
    normal lo tocan; sin esta forma quedaría varado para siempre."""
    nid, attempt_id = _orphan(env, inherited=True)
    assert _row(env, nid)["status"] == "pending"
    recon = env["store"].claim_for_reconciliation(
        clock=_Clock(NOW), duration=RECON_DURATION
    )
    assert recon is not None
    assert (recon.notification_id, recon.attempt_id) == (nid, attempt_id)
    assert _row(env, nid)["status"] == "claimed"


def test_69_reconciles_expired_orphan(env):
    """69 · **INV-W16.** El huérfano cuyo batch además venció: es justo el que un
    sweep ingenuo marcaría 'expired' —afirmando que nada salió sobre un mensaje que
    puede estar entregado—. La reconciliación lo alcanza."""
    nid, attempt_id = _orphan(env, expired=True)
    later = NOW + timedelta(seconds=60)
    assert env["store"].list_sweepable(now=later, limit=10) == []   # el sweep no lo ve
    recon = env["store"].claim_for_reconciliation(
        clock=_Clock(later), duration=RECON_DURATION
    )
    assert recon is not None and recon.notification_id == nid
    assert _row(env, nid)["dead_reason"] is None


def test_70_reconciles_non_expired_orphan(env):
    """70 · Y el huérfano de batch vivo también: la política es única, no depende de
    si el TTL sigue en pie."""
    nid, _ = _orphan(env, expired=False)
    later = NOW + timedelta(seconds=60)
    assert _row(env, nid)["expires_at"] == FOREVER
    recon = env["store"].claim_for_reconciliation(
        clock=_Clock(later), duration=RECON_DURATION
    )
    assert recon is not None and recon.notification_id == nid


def test_71_live_claim_with_dispatching_is_not_stolen(env):
    """71 · Un claim vigente con intento abierto no es un huérfano: es un worker
    trabajando. Robárselo produciría dos llamadas al proveedor."""
    nid = _batch(env)
    token, _ = _open_dispatching(env, nid, now=NOW, duration=timedelta(seconds=120))
    assert env["store"].claim_for_reconciliation(
        clock=_Clock(NOW + timedelta(seconds=30)), duration=RECON_DURATION
    ) is None
    assert env["store"].claim_for_reconciliation(
        clock=_Clock(NOW + timedelta(seconds=30)), duration=RECON_DURATION,
        notification_id=nid,
    ) is None
    assert _row(env, nid)["claim_token"] == token
    assert env["store"].list_orphans(now=NOW + timedelta(seconds=30), limit=10) == []


def test_72_batch_without_dispatching_is_rejected(env):
    """72 · Sin intento abierto no hay ambigüedad que cerrar: ese batch es del sweep
    o del camino normal, no de aquí."""
    nid = _batch(env)
    _p1b2_claim(env, nid, now=NOW, duration=timedelta(seconds=10))
    later = NOW + timedelta(seconds=60)
    assert env["store"].claim_for_reconciliation(
        clock=_Clock(later), duration=RECON_DURATION, notification_id=nid
    ) is None
    assert env["store"].claim_for_reconciliation(
        clock=_Clock(later), duration=RECON_DURATION
    ) is None
    assert _row(env, nid)["status"] == "claimed"


@pytest.mark.parametrize("status", ["sent", "ambiguous", "dead"])
def test_73_terminal_states_are_rejected(env, status):
    """73 · Los terminales no vuelven: reabrirlos sería reescribir un veredicto."""
    nid = _batch(env)
    _open_dispatching(env, nid, now=NOW, duration=timedelta(seconds=10))
    _force_status(env, nid, status)
    later = NOW + timedelta(seconds=60)
    assert env["store"].claim_for_reconciliation(
        clock=_Clock(later), duration=RECON_DURATION, notification_id=nid
    ) is None
    assert env["store"].list_orphans(now=later, limit=10) == []
    assert _row(env, nid)["status"] == status


def test_74_multiple_open_dispatching_fails_closed_without_changes(env):
    """74 · Dos intentos abiertos: cada uno pudo llegar al proveedor. Escoger "el
    último" inventaría una respuesta que el estado no da, y cerrar uno dejaría el otro
    afirmando en silencio que sigue en vuelo. Se falla cerrado, sin tocar nada."""
    nid = _batch(env)
    _open_dispatching(env, nid, now=NOW, duration=timedelta(seconds=10))
    _open_dispatching(env, nid, now=NOW + timedelta(seconds=20),
                      duration=timedelta(seconds=10))
    assert env["store"].count_open_dispatching(nid) == 2
    before = dict(_row(env, nid))
    later = NOW + timedelta(seconds=60)

    with pytest.raises(WorkerStoreConsistencyError, match="dispatching"):
        env["store"].claim_for_reconciliation(
            clock=_Clock(later), duration=RECON_DURATION, notification_id=nid
        )
    with pytest.raises(WorkerStoreConsistencyError):
        env["store"].claim_for_reconciliation(clock=_Clock(later),
                                              duration=RECON_DURATION)
    assert dict(_row(env, nid)) == before
    assert env["store"].count_open_dispatching(nid) == 2
    assert len(_attempts(env, nid)) == 2
    # Y la base sigue escribible: el fallo cerró su transacción.
    assert env["store"].claim_candidate(
        clock=_Clock(later), duration=DURATION, notification_id=_batch(env)
    ) is not None


def test_75_two_reconcilers_exactly_one_wins(env):
    """75 · La exclusión la decide el UPDATE condicional, igual que en el claim."""
    nid, attempt_id = _orphan(env)
    later = NOW + timedelta(seconds=60)
    results, barrier, lock = [], threading.Barrier(2), threading.Lock()

    def attempt():
        store = WorkerStore(env["db"])
        barrier.wait(timeout=JOIN_TIMEOUT)
        recon = store.claim_for_reconciliation(
            clock=_Clock(later), duration=RECON_DURATION, notification_id=nid
        )
        with lock:
            results.append(recon)

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=JOIN_TIMEOUT)
        assert not t.is_alive(), "hilo colgado"
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert winners[0].attempt_id == attempt_id
    assert _row(env, nid)["claim_token"] == winners[0].claim_token


def test_76_reconciliation_race_repeated_50_times(env):
    """76 · 50 iteraciones, cuatro reconciliadores: un ganador, un intento cerrado,
    ningún hilo vivo."""
    later = NOW + timedelta(seconds=60)
    for iteration in range(RACE_ITERATIONS):
        nid, attempt_id = _orphan(env)
        results, barrier, lock = [], threading.Barrier(4), threading.Lock()

        def attempt():
            store = WorkerStore(env["db"])
            barrier.wait(timeout=JOIN_TIMEOUT)
            recon = store.claim_for_reconciliation(
                clock=_Clock(later), duration=RECON_DURATION, notification_id=nid
            )
            with lock:
                results.append(recon)

        threads = [threading.Thread(target=attempt) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=JOIN_TIMEOUT)
            assert not t.is_alive(), f"hilo colgado en iteración {iteration}"
        winners = [r for r in results if r is not None]
        assert len(winners) == 1, f"iteración {iteration}: {len(winners)} ganadores"

        # El ganador cierra el huérfano; nadie más pudo abrir un segundo intento.
        env["ledger"].finish_attempt(
            attempt_id, nid, winners[0].claim_token, outcome="ambiguous",
            error_class="timeout_after_send", now=later,
        )
        assert _row(env, nid)["status"] == "ambiguous"
        assert len(_attempts(env, nid)) == 1


def test_77_reconciliation_claim_until_reflects_the_post_lock_instant(env):
    """77 · El efecto bajo contención real: el claim de reconciliación mide desde que
    somos dueños, no desde antes de pedirlo. Quien fija el invariante es test_38b
    (orden observable); éste comprueba que la ventana no se consume esperando."""
    for iteration in range(RACE_ITERATIONS):
        nid, _ = _orphan(env)
        released = threading.Event()
        about_to = threading.Barrier(2)
        observed: list[bool] = []
        late = NOW + timedelta(hours=2)

        def clock():
            observed.append(released.is_set())
            return late if released.is_set() else NOW + timedelta(seconds=60)

        holder = connect_sqlite(env["db"])
        holder.execute("BEGIN IMMEDIATE")
        box = {}

        def worker():
            store = WorkerStore(env["db"])
            about_to.wait(timeout=JOIN_TIMEOUT)
            box["recon"] = store.claim_for_reconciliation(
                clock=clock, duration=RECON_DURATION, notification_id=nid
            )

        thread = threading.Thread(target=worker)
        thread.start()
        try:
            about_to.wait(timeout=JOIN_TIMEOUT)
            released.set()
            holder.execute("COMMIT")
        finally:
            holder.close()
        thread.join(timeout=JOIN_TIMEOUT)
        assert not thread.is_alive(), f"hilo colgado en iteración {iteration}"
        assert observed == [True], (
            f"iteración {iteration}: reloj invocado con el lock ajeno retenido"
        )
        assert box["recon"].claim_until == (late + RECON_DURATION).isoformat()
        _force_status(env, nid, "ambiguous")


def test_78_reconciliation_is_the_only_operation_that_ignores_expiration(env):
    """78 · El contraste completo: el mismo batch expirado con intento abierto es
    invisible para el claim normal y para el sweep, y visible solo aquí."""
    nid, _ = _orphan(env, expired=True)
    later = NOW + timedelta(seconds=60)
    assert env["store"].list_candidates(now=later, limit=10) == []
    assert env["store"].claim_candidate(
        clock=_Clock(later), duration=DURATION, notification_id=nid
    ) is None
    assert env["store"].list_sweepable(now=later, limit=10) == []
    assert env["store"].sweep_expired(clock=_Clock(later)) == []
    assert env["store"].list_orphans(now=later, limit=10) == [nid]
    assert env["store"].claim_for_reconciliation(
        clock=_Clock(later), duration=RECON_DURATION
    ) is not None


def test_79_reconciliation_claim_enables_finish_attempt_ambiguous(env):
    """79 · Por qué se renueva el claim en vez de escribir 'ambiguous' a mano: para
    poder usar finish_attempt, que cierra intento **y** batch en una transacción ya
    probada. Un UPDATE directo dejaría el dispatching abierto para siempre."""
    nid, attempt_id = _orphan(env)
    later = NOW + timedelta(seconds=60)
    recon = env["store"].claim_for_reconciliation(
        clock=_Clock(later), duration=RECON_DURATION
    )
    env["ledger"].finish_attempt(
        recon.attempt_id, nid, recon.claim_token, outcome="ambiguous",
        error_class="timeout_after_send",
        error_message_safe="orphan dispatching reconciled", now=later,
    )
    row = _row(env, nid)
    assert row["status"] == "ambiguous"
    assert row["dead_reason"] is None        # nunca 'expired': sería una mentira
    assert row["claim_token"] is None
    attempt = _attempts(env, nid)[0]
    assert attempt["attempt_id"] == attempt_id
    assert attempt["phase"] == "finished"
    assert attempt["outcome"] == "ambiguous"
    assert env["store"].count_open_dispatching(nid) == 0


def test_80_second_reconciliation_is_a_noop(env):
    """80 · Idempotente: tras cerrar, el batch es terminal y ya no hay intento
    abierto. La segunda llamada no encuentra nada que hacer."""
    nid, _ = _orphan(env)
    later = NOW + timedelta(seconds=60)
    recon = env["store"].claim_for_reconciliation(
        clock=_Clock(later), duration=RECON_DURATION
    )
    env["ledger"].finish_attempt(
        recon.attempt_id, nid, recon.claim_token, outcome="ambiguous", now=later,
    )
    before = dict(_row(env, nid))
    assert env["store"].claim_for_reconciliation(
        clock=_Clock(later + timedelta(minutes=5)), duration=RECON_DURATION
    ) is None
    assert env["store"].claim_for_reconciliation(
        clock=_Clock(later + timedelta(minutes=5)), duration=RECON_DURATION,
        notification_id=nid,
    ) is None
    assert dict(_row(env, nid)) == before
    assert len(_attempts(env, nid)) == 1


def test_80b_reconciliation_rejects_invalid_duration_and_naive_clock(env):
    """80b · Las mismas guardas que el claim normal: no hay una puerta más laxa."""
    nid, _ = _orphan(env)
    with pytest.raises(WorkerStoreError):
        env["store"].claim_for_reconciliation(clock=_Clock(NOW), duration=timedelta(0))
    with pytest.raises(WorkerStoreError, match="zona horaria"):
        env["store"].claim_for_reconciliation(
            clock=lambda: datetime(2026, 7, 15, 12, 0), duration=RECON_DURATION
        )
    assert _row(env, nid)["status"] == "claimed"


def test_80c_list_orphans_finds_every_form_and_no_live_work(env):
    """80c · list_orphans ve las cuatro formas —vivo, expirado, heredado, heredado y
    expirado— y ninguna con claim vigente."""
    later = NOW + timedelta(seconds=60)
    alive, _ = _orphan(env)
    expired, _ = _orphan(env, expired=True)
    inherited, _ = _orphan(env, inherited=True)
    inherited_expired, _ = _orphan(env, expired=True, inherited=True)

    live = _batch(env)
    _open_dispatching(env, live, now=NOW, duration=timedelta(seconds=600))
    clean = _batch(env)

    full = env["store"].list_orphans(now=later, limit=50)
    assert set(full) == {alive, expired, inherited, inherited_expired}
    assert live not in full and clean not in full
    # Acotado y con el mismo orden: un limit no reordena, solo trunca.
    assert env["store"].list_orphans(now=later, limit=2) == full[:2]


# ══ G · sweep_expired (81–94) ════════════════════════════════════════════════════

EXPIRED_AT = (NOW - timedelta(hours=1)).isoformat()


def test_81_expired_pending_is_swept(env):
    """81 · **INV-W10.** Sin sweep, un vencido es 'pending' eterno y silencioso: el
    estado varado. 'expired' es literalmente cierto aquí — no hay intento que lo
    contradiga."""
    nid = _batch(env, expires_at=EXPIRED_AT)
    assert env["store"].sweep_expired(clock=_Clock(NOW)) == [nid]
    row = _row(env, nid)
    assert row["status"] == "dead"
    assert row["dead_reason"] == "expired"
    assert row["claim_token"] is None and row["claim_until"] is None


def test_82_expired_claim_without_dispatching_is_swept(env):
    """82 · El dueño anterior murió sin escribir nada: no hay intento, así que nadie
    puede contradecir el 'nada salió'."""
    nid = _batch(env, expires_at=EXPIRED_AT)
    _p1b2_claim(env, nid, now=NOW - timedelta(hours=2), duration=timedelta(seconds=10))
    assert _row(env, nid)["status"] == "claimed"
    assert env["store"].sweep_expired(clock=_Clock(NOW)) == [nid]
    row = _row(env, nid)
    assert row["status"] == "dead" and row["dead_reason"] == "expired"
    assert row["claim_token"] is None


def test_83_live_claim_is_never_swept(env):
    """83 · **INV-W15**, fencing temporal: alguien está trabajando en él ahora mismo.
    Matarlo bajo sus pies le quitaría el batch a mitad de un envío.

    El estado se fabrica como ocurre de verdad: se reclama un batch vivo y el TTL
    vence *durante* el trabajo. Un batch ya expirado no se puede reclamar —``try_claim``
    lo rechaza—, así que esta es la única forma en que "expirado + claim vigente"
    existe en producción."""
    nid = _batch(env, expires_at=(NOW + timedelta(minutes=1)).isoformat())
    token = _p1b2_claim(env, nid, now=NOW - timedelta(seconds=1),
                        duration=timedelta(hours=1))
    assert token is not None
    _sql(env, "UPDATE radar_notification_batches SET expires_at=? "
              "WHERE notification_id=?", (EXPIRED_AT, nid))
    assert _row(env, nid)["claim_until"] > NOW.isoformat()
    assert env["store"].sweep_expired(clock=_Clock(NOW)) == []
    row = _row(env, nid)
    assert row["status"] == "claimed" and row["claim_token"] == token
    assert row["dead_reason"] is None


def test_84_pending_with_dispatching_is_never_swept(env):
    """84 · **INV-W16**, fencing por evidencia, sobre el huérfano heredado."""
    nid, _ = _orphan(env, inherited=True, expired=True)
    assert _row(env, nid)["status"] == "pending"
    assert env["store"].sweep_expired(clock=_Clock(NOW + timedelta(seconds=60))) == []
    row = _row(env, nid)
    assert row["status"] == "pending"
    assert row["dead_reason"] is None
    assert env["store"].count_open_dispatching(nid) == 1


def test_85_expired_claim_with_dispatching_is_never_swept(env):
    """85 · **La cláusula más importante del bloque.** El batch pasa el fencing
    temporal (su claim venció) y muere en el de evidencia: hay un intento abierto, así
    que el proveedor pudo haber aceptado el mensaje. Marcarlo 'expired' afirmaría
    "nada salió" sobre algo que puede estar en el teléfono de alguien, y borraría la
    única prueba de la ambigüedad."""
    nid, _ = _orphan(env, expired=True)
    later = NOW + timedelta(seconds=60)
    assert _row(env, nid)["claim_until"] <= later.isoformat()   # claim vencido
    assert _row(env, nid)["expires_at"] <= later.isoformat()    # batch vencido
    assert env["store"].list_sweepable(now=later, limit=10) == []
    assert env["store"].sweep_expired(clock=_Clock(later)) == []
    row = _row(env, nid)
    assert row["status"] != "dead"
    assert row["dead_reason"] is None
    # Y sigue siendo alcanzable por quien sí puede darle un veredicto honesto.
    assert env["store"].list_orphans(now=later, limit=10) == [nid]


@pytest.mark.parametrize("status", ["sent", "ambiguous", "dead"])
def test_86_87_88_terminal_states_are_never_swept(env, status):
    """86/87/88 · Un terminal ya tiene veredicto. Reescribirlo como 'expired' borraría
    lo que de verdad pasó — y en el caso de 'sent', negaría un envío confirmado."""
    nid = _batch(env, expires_at=EXPIRED_AT)
    _force_status(env, nid, status)
    if status == "dead":
        _sql(env, "UPDATE radar_notification_batches SET dead_reason='opted_out' "
                  "WHERE notification_id=?", (nid,))
    before = dict(_row(env, nid))
    assert env["store"].sweep_expired(clock=_Clock(NOW)) == []
    assert dict(_row(env, nid)) == before


def test_89_sweep_respects_limit(env):
    """89 · Acotado: un sweep ilimitado sobre una base grande mantendría el lock de
    escritor durante todo el barrido y bloquearía a todos los workers."""
    ids = [_batch(env, expires_at=EXPIRED_AT) for _ in range(10)]
    swept = env["store"].sweep_expired(clock=_Clock(NOW), limit=3)
    assert len(swept) == 3
    remaining = env["store"].list_sweepable(now=NOW, limit=50)
    assert len(remaining) == 7
    assert set(swept) & set(remaining) == set()
    assert set(swept) | set(remaining) == set(ids)


@pytest.mark.parametrize("bad", [0, -1, True, False, 1.5, "10", None])
def test_89b_sweep_rejects_invalid_limit(env, bad):
    """89b · `limit=True` pasaría como 1 y barrería un batch en silencio: bool es int
    en Python, así que hay que rechazarlo explícitamente."""
    nid = _batch(env, expires_at=EXPIRED_AT)
    with pytest.raises(WorkerStoreError):
        env["store"].sweep_expired(clock=_Clock(NOW), limit=bad)
    assert _row(env, nid)["status"] == "pending"


def test_90_sweep_order_is_deterministic(env):
    """90 · expires_at, created_at, notification_id: orden total y reproducible."""
    base = NOW - timedelta(hours=5)
    oldest = _batch(env, created_at=base.isoformat(),
                    expires_at=(base + timedelta(hours=1)).isoformat())
    middle = _batch(env, created_at=base.isoformat(),
                    expires_at=(base + timedelta(hours=2)).isoformat())
    newest = _batch(env, created_at=base.isoformat(),
                    expires_at=(base + timedelta(hours=3)).isoformat())
    assert env["store"].list_sweepable(now=NOW, limit=10) == [oldest, middle, newest]
    assert env["store"].sweep_expired(clock=_Clock(NOW), limit=2) == [oldest, middle]
    assert env["store"].sweep_expired(clock=_Clock(NOW), limit=2) == [newest]


def test_90b_sweep_order_breaks_ties_by_notification_id(env):
    """90b · Empate total en expires_at y created_at: decide el id."""
    ids = sorted(_batch(env, expires_at=EXPIRED_AT) for _ in range(6))
    assert env["store"].sweep_expired(clock=_Clock(NOW)) == ids


def test_91_sweep_is_idempotent(env):
    """91 · Repetirlo no cambia nada: lo ya barrido es 'dead' y sale del predicado."""
    ids = [_batch(env, expires_at=EXPIRED_AT) for _ in range(4)]
    first = env["store"].sweep_expired(clock=_Clock(NOW))
    assert sorted(first) == sorted(ids)
    snapshot = {nid: dict(_row(env, nid)) for nid in ids}
    for _ in range(3):
        assert env["store"].sweep_expired(clock=_Clock(NOW)) == []
    assert {nid: dict(_row(env, nid)) for nid in ids} == snapshot


def test_92_sweep_failure_before_commit_reverts_everything(env):
    """92 · Todo o nada: un barrido a medias dejaría unos batches muertos y otros
    vivos sin que nada explicara la diferencia."""
    ids = [_batch(env, expires_at=EXPIRED_AT) for _ in range(5)]

    def hook(phase):
        if phase == PHASE_BEFORE_COMMIT:
            raise RuntimeError("muere justo antes del commit")

    with pytest.raises(RuntimeError):
        env["store"].sweep_expired(clock=_Clock(NOW), _fault_hook=hook)
    for nid in ids:
        row = _row(env, nid)
        assert row["status"] == "pending"
        assert row["dead_reason"] is None
    assert sorted(env["store"].sweep_expired(clock=_Clock(NOW))) == sorted(ids)


def test_92b_sweep_rejects_naive_clock_without_writing(env):
    """92b · La misma guarda de reloj que el resto: un instante naive compara mal y
    barrería lo que no toca."""
    nid = _batch(env, expires_at=EXPIRED_AT)
    with pytest.raises(WorkerStoreError, match="zona horaria"):
        env["store"].sweep_expired(clock=lambda: datetime(2026, 7, 15, 12, 0))
    assert _row(env, nid)["status"] == "pending"


def test_93_sweep_and_reconciliation_never_produce_expired_over_dispatching(env):
    """93 · **La cadena completa, en paralelo.** El proveedor aceptó, la respuesta se
    perdió, el proceso murió, el claim venció y el batch expiró. Sweep y reconciliación
    corren a la vez sobre él. El resultado debe ser 'ambiguous' — jamás 'expired', que
    afirmaría que nada salió sobre un mensaje entregado."""
    nid, attempt_id = _orphan(env, expired=True)
    later = NOW + timedelta(seconds=60)
    barrier, box, lock = threading.Barrier(2), {}, threading.Lock()

    def sweeper():
        store = WorkerStore(env["db"])
        barrier.wait(timeout=JOIN_TIMEOUT)
        with lock:
            box["swept"] = store.sweep_expired(clock=_Clock(later))

    def reconciler():
        store = WorkerStore(env["db"])
        barrier.wait(timeout=JOIN_TIMEOUT)
        recon = store.claim_for_reconciliation(
            clock=_Clock(later), duration=RECON_DURATION, notification_id=nid
        )
        with lock:
            box["recon"] = recon

    threads = [threading.Thread(target=sweeper), threading.Thread(target=reconciler)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=JOIN_TIMEOUT)
        assert not t.is_alive(), "hilo colgado"

    assert box["swept"] == [], "el sweep tocó un batch con dispatching abierto"
    assert box["recon"] is not None
    env["ledger"].finish_attempt(
        attempt_id, nid, box["recon"].claim_token, outcome="ambiguous",
        error_class="timeout_after_send", now=later,
    )
    row = _row(env, nid)
    assert row["status"] == "ambiguous"
    assert row["dead_reason"] is None


def test_94_sweep_reconciliation_race_repeated_50_times(env):
    """94 · 50 iteraciones de la carrera. Un solo 'expired' sobre un intento abierto
    sería NO-GO del bloque entero."""
    later = NOW + timedelta(seconds=60)
    for iteration in range(RACE_ITERATIONS):
        nid, attempt_id = _orphan(env, expired=True)
        barrier, box, lock = threading.Barrier(2), {}, threading.Lock()

        def sweeper():
            store = WorkerStore(env["db"])
            barrier.wait(timeout=JOIN_TIMEOUT)
            with lock:
                box["swept"] = store.sweep_expired(clock=_Clock(later))

        def reconciler():
            store = WorkerStore(env["db"])
            barrier.wait(timeout=JOIN_TIMEOUT)
            with lock:
                box["recon"] = store.claim_for_reconciliation(
                    clock=_Clock(later), duration=RECON_DURATION, notification_id=nid
                )

        threads = [threading.Thread(target=sweeper),
                   threading.Thread(target=reconciler)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=JOIN_TIMEOUT)
            assert not t.is_alive(), f"hilo colgado en iteración {iteration}"

        assert nid not in box["swept"], f"iteración {iteration}: barrido con intento"
        row = _row(env, nid)
        assert row["dead_reason"] != "expired", (
            f"iteración {iteration}: 'expired' sobre un batch con dispatching abierto"
        )
        assert box["recon"] is not None
        env["ledger"].finish_attempt(
            attempt_id, nid, box["recon"].claim_token, outcome="ambiguous", now=later,
        )
        assert _row(env, nid)["status"] == "ambiguous"


# ══ H · Integración focal (95–100) ═══════════════════════════════════════════════

def _selector_batch(env, *, kinds=("new", "new")):
    """Un batch nacido del selector real (P1B-3), no fabricado a mano."""
    from app.core.notify.selector import NotificationSelector

    rows = [
        {
            "radar_id": env["radar"], "owner": env["owner"], "radar_name": "Radar PAE",
            "run_id": "run", "ts": "2026-07-15T10:00:00+00:00", "kind": kind,
            "result_key": f"K-{next(_seq)}", "dataset": "procesos",
            "title": "Proceso", "entity": "Entidad", "value": "100",
            "status": "S1", "prev_status": "S0", "prev_value": "90",
            "urlproceso": "https://secop/1", "referencia": "REF-1",
        }
        for kind in kinds
    ]
    env["radars"].record_events(rows)
    selector = NotificationSelector(env["db"])
    result = selector.select_pair(env["endpoint"], env["radar"], now=NOW)
    assert result.notification_id is not None, result.skipped_reason
    return result.notification_id


def test_95_selector_batch_flows_through_claim_and_authorize(env):
    """95 · El camino completo con un batch real: el selector materializa, el claim lo
    toma, la autorización lo revalida y abre el intento. Las piezas encajan sobre el
    mismo estado, no sobre uno inventado por los tests."""
    nid = _selector_batch(env)
    assert _row(env, nid)["status"] == "pending"

    assert env["store"].list_candidates(now=NOW, limit=10) == [nid]
    claimed = env["store"].claim_candidate(clock=_Clock(NOW), duration=DURATION)
    assert claimed.notification_id == nid

    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert result.outcome == OUTCOME_AUTHORIZED
    assert result.channel == "telegram"
    assert result.external_recipient_id == "chat-ana"
    assert result.external_idempotency_key == external_idempotency_key(nid)

    import json
    payload = json.loads(result.payload_json)
    assert payload["radar_id"] == env["radar"]
    assert payload["items"]
    # Ya no es candidato: tiene un intento abierto.
    assert env["store"].list_candidates(now=NOW, limit=10) == []


def test_96_attempt_is_compatible_with_finish_attempt(env):
    """96 · El intento que abre la autorización lo cierra el ledger de P1B-2 sin
    adaptadores: es el mismo intento."""
    nid = _selector_batch(env)
    claimed = _authorized_claim(env, nid)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    attempt = env["ledger"].get_attempt(result.attempt_id)
    assert attempt is not None
    assert attempt.notification_id == nid
    assert attempt.phase == "dispatching"
    assert attempt.outcome is None
    assert env["ledger"].get_open_dispatching_attempt(nid).attempt_id == (
        result.attempt_id
    )


def test_97_sent_through_the_ledger_closes_correctly(env):
    """97 · 'sent' = el proveedor lo aceptó. Cierra intento y batch, y el batch deja
    de ser candidato para siempre."""
    nid = _selector_batch(env)
    claimed = _authorized_claim(env, nid)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    env["ledger"].finish_attempt(
        result.attempt_id, nid, claimed.claim_token, outcome="sent",
        provider_message_id="msg-1", provider_status="200", now=NOW,
    )
    row = _row(env, nid)
    assert row["status"] == "sent"
    assert row["claim_token"] is None and row["dead_reason"] is None
    attempt = env["ledger"].get_attempt(result.attempt_id)
    assert attempt.phase == "finished" and attempt.outcome == "sent"
    assert attempt.provider_message_id == "msg-1"
    assert env["store"].count_open_dispatching(nid) == 0
    assert env["store"].list_candidates(now=LATER, limit=10) == []
    assert env["store"].list_orphans(now=LATER, limit=10) == []


def test_98_retry_through_the_ledger_returns_to_pending(env):
    """98 · 'retry' devuelve el batch al camino normal: cierra el intento, así que
    vuelve a ser candidato — y sin intento abierto, reintentarlo es seguro."""
    nid = _selector_batch(env)
    claimed = _authorized_claim(env, nid)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    next_at = (NOW + timedelta(minutes=1)).isoformat()
    env["ledger"].finish_attempt(
        result.attempt_id, nid, claimed.claim_token, outcome="retry",
        error_class="rate_limited", retry_after=60, next_not_before=next_at, now=NOW,
    )
    row = _row(env, nid)
    assert row["status"] == "pending"
    assert row["not_before"] == next_at
    assert row["attempt_count"] == 1
    assert env["store"].count_open_dispatching(nid) == 0
    assert env["store"].list_candidates(now=NOW, limit=10) == []          # en espera
    assert env["store"].list_candidates(now=LATER, limit=10) == [nid]     # ya due
    # Y la clave externa sobrevive al reintento.
    assert row["external_idempotency_key"] == result.external_idempotency_key


def test_99_ambiguous_through_the_ledger_is_terminal(env):
    """99 · **INV-W6.** 'ambiguous' no vuelve a pending: reintentar un envío que pudo
    haber llegado sería elegir duplicar. Ni candidato, ni huérfano, ni barrible."""
    nid = _selector_batch(env)
    claimed = _authorized_claim(env, nid)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    env["ledger"].finish_attempt(
        result.attempt_id, nid, claimed.claim_token, outcome="ambiguous",
        error_class="timeout_after_send", now=NOW,
    )
    assert _row(env, nid)["status"] == "ambiguous"
    _sql(env, "UPDATE radar_notification_batches SET expires_at=? "
              "WHERE notification_id=?", (EXPIRED_AT, nid))
    assert env["store"].list_candidates(now=LATER, limit=10) == []
    assert env["store"].list_orphans(now=LATER, limit=10) == []
    assert env["store"].sweep_expired(clock=_Clock(LATER)) == []
    assert _row(env, nid)["status"] == "ambiguous"
    assert _row(env, nid)["dead_reason"] is None


def test_100_r2a_imports_no_provider_no_worker_no_network(env):
    """100 · **INV-W14.** R2A no envía nada, y eso se prueba estructuralmente: los
    módulos nuevos no importan red ni proveedores, y esos módulos no existen todavía.
    Si alguien los introduce por la puerta de atrás, este test lo dice."""
    import ast
    import pathlib
    import sys

    forbidden = {
        "requests", "httpx", "urllib", "urllib3", "http", "socket", "aiohttp",
        "telegram", "telebot", "smtplib", "ssl", "asyncio",
    }
    modules = [
        "app/core/notify/authorization.py",
        "app/core/notify/retry.py",
        "app/core/notify/worker_store.py",
    ]
    for path in modules:
        tree = ast.parse(pathlib.Path(path).read_text())
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            for name in names:
                root = name.split(".")[0]
                assert root not in forbidden, f"{path} importa {name}"
                assert "provider" not in name, f"{path} importa {name}"
                assert not name.endswith("worker"), f"{path} importa {name}"

    # Y los módulos de envío no existen: R2A no los adelanta.
    for absent in ("provider.py", "fake_provider.py", "worker.py"):
        assert not pathlib.Path("app/core/notify", absent).exists(), (
            f"app/core/notify/{absent} no debería existir en R2A"
        )
    # Ninguna de las tres primitivas dejó un módulo de red cargado.
    assert "app.core.notify.provider" not in sys.modules
    assert "app.core.notify.worker" not in sys.modules


# ══ E-bis · expiración entre el claim y la autorización (INV-W9) ═════════════════

def test_61c_batch_expiring_between_claim_and_authorize_dies_expired(env):
    """**INV-W9.** El batch se reclama vivo y el TTL vence antes de autorizar. Ningún
    `provider.send` se *inicia* sobre un batch vencido: la revalidación lo mata dentro
    de su propia transacción, sin clave y sin intento.

    La ventana es estrecha —`claim_candidate` exige `expires_at > now`— pero es real:
    un lock disputado o un proveedor lento la abren, y sin este chequeo el worker
    enviaría una alerta que ya no vale."""
    expires = (NOW + timedelta(minutes=5)).isoformat()
    nid = _batch(env, expires_at=expires)
    claimed = env["store"].claim_candidate(          # 1. reclamar un batch válido
        clock=_Clock(NOW), duration=timedelta(hours=2), notification_id=nid
    )
    assert claimed is not None

    at_expiry = NOW + timedelta(minutes=5)           # 2. avanzar el reloj hasta expires_at
    assert claimed.claim_until > at_expiry.isoformat()   # el claim sigue vigente
    result = env["store"].authorize_and_begin_dispatch(  # 3. autorizar
        nid, claimed.claim_token, clock=_Clock(at_expiry)
    )

    assert result.outcome == OUTCOME_DEAD            # 4. outcome = dead
    assert result.dead_reason == "expired"           # 5. dead_reason = expired
    assert result.reason == "expired"
    assert result.external_idempotency_key is None   # 6. cero clave externa
    assert result.attempt_id is None
    row = _row(env, nid)
    assert row["external_idempotency_key"] is None
    assert _attempts(env, nid) == []                 # 7. cero attempt
    assert env["store"].count_open_dispatching(nid) == 0
    assert row["attempt_count"] == 0                 # 8. attempt_count intacto
    assert row["claim_token"] is None                # 9. claim limpio
    assert row["claim_until"] is None
    assert row["status"] == "dead"


def test_61d_expiry_check_uses_strict_boundary(env):
    """El límite es `expires_at <= now`: en el instante exacto de la expiración el
    batch ya no vale (mismo criterio que el sweep, que barre con `expires_at <= now`).
    Un microsegundo antes, sí vale — y entonces se autoriza."""
    expires = (NOW + timedelta(minutes=5)).isoformat()
    nid = _batch(env, expires_at=expires)
    claimed = env["store"].claim_candidate(
        clock=_Clock(NOW), duration=timedelta(hours=2), notification_id=nid
    )
    just_before = NOW + timedelta(minutes=5) - timedelta(microseconds=1)
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(just_before)
    )
    assert result.outcome == OUTCOME_AUTHORIZED
    assert len(_attempts(env, nid)) == 1


def test_61e_expired_batch_never_reaches_the_provider_path(env):
    """El batch vencido muere antes de que exista nada que enviar: sin canal, sin
    recipient, sin payload en el resultado. No hay ProviderRequest que construir."""
    nid = _batch(env, expires_at=(NOW + timedelta(minutes=1)).isoformat())
    claimed = env["store"].claim_candidate(
        clock=_Clock(NOW), duration=timedelta(hours=2), notification_id=nid
    )
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW + timedelta(hours=1))
    )
    assert result.outcome == OUTCOME_DEAD
    assert result.channel is None
    assert result.external_recipient_id is None
    assert result.payload_json is None


def test_61f_expiry_wins_over_consent_causes_and_both_refuse_to_send(env):
    """Vencido *y* sin consentimiento: ambas causas son terminales y ninguna envía, así
    que la precedencia solo elige la etiqueta. Se declara `expired` porque el reloj se
    evalúa antes de leer el join — y el consentimiento sigue respetado: cero intento."""
    nid = _batch(env, expires_at=(NOW + timedelta(minutes=1)).isoformat())
    claimed = env["store"].claim_candidate(
        clock=_Clock(NOW), duration=timedelta(hours=2), notification_id=nid
    )
    _sql(env, "UPDATE radar_notification_preferences SET enabled=0")
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW + timedelta(hours=1))
    )
    assert result.outcome == OUTCOME_DEAD
    assert result.dead_reason == "expired"
    assert _attempts(env, nid) == []
    assert _row(env, nid)["status"] == "dead"


@pytest.mark.parametrize("phase", [
    PHASE_AFTER_AUTHORIZATION_READ, PHASE_BEFORE_COMMIT,
])
def test_61g_failure_after_deciding_expired_leaves_no_partial_death(env, phase):
    """Fallo después de decidir `expired` y antes del commit: el batch conserva su
    claim original y no queda un `dead` a medias. La muerte por tiempo es tan atómica
    como la autorización — un batch medio muerto sería un veredicto que nadie escribió."""
    expires = (NOW + timedelta(minutes=5)).isoformat()
    nid = _batch(env, expires_at=expires)
    claimed = env["store"].claim_candidate(
        clock=_Clock(NOW), duration=timedelta(hours=2), notification_id=nid
    )

    def hook(fired):
        if fired == phase:
            raise RuntimeError(f"fallo inyectado en {phase} de la rama expired")

    with pytest.raises(RuntimeError):
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token, clock=_Clock(NOW + timedelta(hours=1)),
            _fault_hook=hook,
        )
    row = _row(env, nid)
    assert row["status"] == "claimed", "quedó un dead parcial"
    assert row["dead_reason"] is None
    assert row["claim_token"] == claimed.claim_token      # claim original intacto
    assert row["claim_until"] == claimed.claim_until
    assert row["external_idempotency_key"] is None
    assert row["attempt_count"] == 0
    assert _attempts(env, nid) == []
    # Y reintentarlo produce el veredicto correcto, una sola vez.
    result = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW + timedelta(hours=1))
    )
    assert result.outcome == OUTCOME_DEAD and result.dead_reason == "expired"
    assert _row(env, nid)["status"] == "dead"


def test_61h_expired_death_is_fenced_by_token_and_liveness(env):
    """La transición va cercada como todas: un token viejo no puede matar el batch de
    su sucesor ni siquiera invocando la razón cierta de que venció."""
    nid = _batch(env, expires_at=(NOW + timedelta(minutes=1)).isoformat())
    stale = env["store"].claim_candidate(
        clock=_Clock(NOW), duration=timedelta(hours=2), notification_id=nid
    )
    _sql(env, "UPDATE radar_notification_batches SET claim_token='token-del-sucesor' "
              "WHERE notification_id=?", (nid,))
    result = env["store"].authorize_and_begin_dispatch(
        nid, stale.claim_token, clock=_Clock(NOW + timedelta(hours=1))
    )
    assert result.outcome == OUTCOME_CLAIM_LOST
    row = _row(env, nid)
    assert row["status"] == "claimed"
    assert row["dead_reason"] is None
    assert row["claim_token"] == "token-del-sucesor"


def test_61i_no_dispatching_ever_opens_over_an_expired_batch(env):
    """La afirmación en su forma más directa, y la que la mutación de INV-W9 rompe
    primero: **ningún batch vencido puede tener un intento abierto** creado por la
    autorización. Un `dispatching` sobre un batch vencido es prueba de que se llamó (o
    se iba a llamar) al proveedor con una alerta que ya no valía — y además arrastra al
    batch a la reconciliación, donde acabará `ambiguous`: el sistema declarando "no
    sabemos si llegó" sobre algo que nunca debió salir.

    Se comprueba sobre varios instantes posteriores al TTL para que la mutación no
    pueda sobrevivir por un caso límite."""
    for offset in (timedelta(0), timedelta(seconds=1), timedelta(hours=3)):
        nid = _batch(env, expires_at=(NOW + timedelta(minutes=5)).isoformat())
        claimed = env["store"].claim_candidate(
            clock=_Clock(NOW), duration=timedelta(days=1), notification_id=nid
        )
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token,
            clock=_Clock(NOW + timedelta(minutes=5) + offset),
        )
        assert env["store"].count_open_dispatching(nid) == 0, (
            f"dispatching indebido sobre un batch vencido (+{offset}): la "
            f"autorización dejó pasar un envío que ya no valía"
        )
        assert _attempts(env, nid) == []
        assert env["store"].list_orphans(now=NOW + timedelta(days=2), limit=10) == []


# ══ E-ter · endurecimientos: fail-closed y privacidad del repr ═══════════════════

def test_101_unknown_authorization_category_fails_closed(env, monkeypatch):
    """Autorizar es la única rama que llama al proveedor, así que no puede ser el caso
    por defecto. Una categoría nueva —o un typo— no puede convertirse en permiso de
    envío: ése es el fallo silencioso más caro posible."""
    import app.core.notify.authorization as authz

    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    monkeypatch.setattr(
        "app.core.notify.worker_store.authorization.authorize_row",
        lambda row, notification_id=None: authz.AuthorizationDecision(
            "categoria_del_futuro", "algo_nuevo"
        ),
    )
    with pytest.raises(WorkerStoreConsistencyError, match="desconocida"):
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token, clock=_Clock(NOW)
        )
    row = _row(env, nid)
    assert row["external_idempotency_key"] is None
    assert row["attempt_count"] == 0
    assert _attempts(env, nid) == []
    assert env["store"].count_open_dispatching(nid) == 0
    assert row["status"] == "claimed"                    # claim intacto
    assert row["claim_token"] == claimed.claim_token
    assert row["claim_until"] == claimed.claim_until


def test_102_second_authorize_with_the_same_claim_fails_closed(env):
    """**INV-W20.** Tener el claim vigente no autoriza a abrir un segundo intento: el
    mismo dueño puede llamar dos veces (un reintento de orquestación, un bug), y
    `UNIQUE(notification_id, attempt_number)` no lo detiene porque el número avanza.
    La puerta que crea intentos es la que debe cerrarse — detectarlo luego, en la
    reconciliación, ya es tarde: el proveedor habría recibido dos llamadas."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    first = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    assert first.outcome == OUTCOME_AUTHORIZED
    before = dict(_row(env, nid))

    with pytest.raises(WorkerStoreConsistencyError, match="dispatching"):
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token, clock=_Clock(NOW)
        )

    assert dict(_row(env, nid)) == before        # ni clave, ni contador, ni claim
    assert _row(env, nid)["attempt_count"] == 1
    attempts = _attempts(env, nid)
    assert len(attempts) == 1
    assert attempts[0]["attempt_id"] == first.attempt_id   # el original, intacto
    assert attempts[0]["phase"] == "dispatching"
    assert env["store"].count_open_dispatching(nid) == 1


def test_102b_dispatching_opened_by_the_ledger_also_blocks_authorization(env):
    """El intento abierto puede venir de `begin_attempt` (P1B-2), no solo de una
    autorización previa. La puerta no distingue quién lo abrió: lo que importa es que
    ya existe evidencia de una llamada en vuelo."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    attempt_id = env["ledger"].begin_attempt(nid, claimed.claim_token, now=NOW)
    with pytest.raises(WorkerStoreConsistencyError):
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token, clock=_Clock(NOW)
        )
    assert [a["attempt_id"] for a in _attempts(env, nid)] == [attempt_id]
    assert _row(env, nid)["external_idempotency_key"] is None


def test_103_expired_batch_with_open_dispatching_is_never_marked_expired(env):
    """**INV-W16**, en la puerta de autorización. El chequeo de expiración que cierra
    INV-W9 no puede matar un batch que ya tiene evidencia de una llamada: `expired`
    afirmaría "nada salió" sobre un mensaje que pudo entregarse, y dejaría el intento
    huérfano sobre un batch terminal, donde la reconciliación ya no lo alcanza (rechaza
    terminales) — el `dispatching` quedaría abierto para siempre.

    Es la fila 14 de la crash matrix: el sistema afirmando con certeza algo falso."""
    nid = _batch(env, expires_at=(NOW + timedelta(minutes=5)).isoformat())
    claimed = env["store"].claim_candidate(
        clock=_Clock(NOW), duration=timedelta(days=1), notification_id=nid
    )
    env["ledger"].begin_attempt(nid, claimed.claim_token, now=NOW)
    assert env["store"].count_open_dispatching(nid) == 1

    with pytest.raises(WorkerStoreConsistencyError):
        env["store"].authorize_and_begin_dispatch(
            nid, claimed.claim_token, clock=_Clock(NOW + timedelta(hours=1)),
        )
    row = _row(env, nid)
    assert row["dead_reason"] != "expired", "'expired' sobre un batch con dispatching"
    assert row["dead_reason"] is None
    assert row["status"] == "claimed"
    assert env["store"].count_open_dispatching(nid) == 1   # la evidencia sobrevive

    # Y el camino honesto sigue abierto: cuando el claim venza, va a reconciliación.
    much_later = NOW + timedelta(days=2)
    assert env["store"].list_orphans(now=much_later, limit=10) == [nid]
    assert env["store"].sweep_expired(clock=_Clock(much_later)) == []
    recon = env["store"].claim_for_reconciliation(
        clock=_Clock(much_later), duration=RECON_DURATION, notification_id=nid
    )
    assert recon is not None
    env["ledger"].finish_attempt(
        recon.attempt_id, nid, recon.claim_token, outcome="ambiguous", now=much_later
    )
    assert _row(env, nid)["status"] == "ambiguous"
    assert _row(env, nid)["dead_reason"] is None


def test_104_concurrent_authorize_same_token_opens_exactly_one_attempt(env):
    """Dos llamadas concurrentes con el mismo token: SQLite serializa las
    transacciones, así que la segunda ve el intento de la primera y falla cerrado.
    Exactamente un intento — nunca dos llamadas al proveedor por el mismo mensaje."""
    nid = _batch(env)
    claimed = _authorized_claim(env, nid)
    barrier, results, lock = threading.Barrier(2), [], threading.Lock()

    def attempt():
        store = WorkerStore(env["db"])
        barrier.wait(timeout=JOIN_TIMEOUT)
        try:
            outcome = store.authorize_and_begin_dispatch(
                nid, claimed.claim_token, clock=_Clock(NOW)
            )
        except WorkerStoreConsistencyError as exc:
            outcome = exc
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=JOIN_TIMEOUT)
        assert not t.is_alive(), "hilo colgado"

    authorized = [r for r in results if isinstance(r, DispatchAuthorization)]
    refused = [r for r in results if isinstance(r, WorkerStoreConsistencyError)]
    assert len(authorized) == 1 and len(refused) == 1
    assert env["store"].count_open_dispatching(nid) == 1
    assert len(_attempts(env, nid)) == 1
    assert _row(env, nid)["attempt_count"] == 1


def test_105_concurrent_authorize_race_repeated_50_times(env):
    """50 iteraciones, cuatro hilos, mismo token. Un solo intento cada vez."""
    for iteration in range(RACE_ITERATIONS):
        nid = _batch(env)
        claimed = _authorized_claim(env, nid)
        barrier, results, lock = threading.Barrier(4), [], threading.Lock()

        def attempt():
            store = WorkerStore(env["db"])
            barrier.wait(timeout=JOIN_TIMEOUT)
            try:
                outcome = store.authorize_and_begin_dispatch(
                    nid, claimed.claim_token, clock=_Clock(NOW)
                )
            except WorkerStoreConsistencyError as exc:
                outcome = exc
            with lock:
                results.append(outcome)

        threads = [threading.Thread(target=attempt) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=JOIN_TIMEOUT)
            assert not t.is_alive(), f"hilo colgado en iteración {iteration}"

        authorized = [r for r in results if isinstance(r, DispatchAuthorization)]
        assert len(authorized) == 1, f"iteración {iteration}: {len(authorized)} autorizadas"
        assert env["store"].count_open_dispatching(nid) == 1, (
            f"iteración {iteration}: {env['store'].count_open_dispatching(nid)} intentos"
        )
        assert _row(env, nid)["attempt_count"] == 1
        _force_status(env, nid, "sent")


def test_106_no_repr_ever_exposes_token_or_payload(env):
    """La política prohíbe loguear recipient, `claim_token` y `payload_json`. Estos
    objetos son lo que alguien vuelca entero al depurar: lo que quede en el repr acaba
    en un log. Se comprueba con marcadores únicos sobre los cuatro dataclasses."""
    marker_payload = "PAYLOAD-MARCADOR-UNICO-9f3a"
    marker_recipient = "RECIPIENT-MARCADOR-UNICO-7b1c"
    _sql(env, "UPDATE notification_endpoints SET external_recipient_id=? "
              "WHERE endpoint_id=?", (marker_recipient, env["endpoint"]))
    nid = env["outbox"].create_batch(
        principal_id=env["principal"], endpoint_id=env["endpoint"],
        radar_id=env["radar"], payload_json={"secreto": marker_payload},
        events=[BatchEventRef(f"ev-marker-{next(_seq)}", next(_seq))],
        created_at="2026-07-15T00:00:00+00:00", expires_at=FOREVER,
    ).notification_id

    claimed = env["store"].claim_candidate(
        clock=_Clock(NOW), duration=DURATION, notification_id=nid
    )
    for text in (repr(claimed), str(claimed)):
        assert claimed.claim_token not in text, "ClaimedCandidate expone el token"
    assert claimed.notification_id in repr(claimed)   # los ids opacos sí explican

    auth = env["store"].authorize_and_begin_dispatch(
        nid, claimed.claim_token, clock=_Clock(NOW)
    )
    for text in (repr(auth), str(auth)):
        assert marker_payload not in text, "DispatchAuthorization expone el payload"
        assert marker_recipient not in text, "DispatchAuthorization expone el recipient"
        assert claimed.claim_token not in text, "DispatchAuthorization expone el token"
    # Y sigue siendo legible para quien lo pide: ocultar no es perder.
    assert auth.payload_json is not None and marker_payload in auth.payload_json
    assert auth.external_recipient_id == marker_recipient
    assert auth.claim_token == claimed.claim_token
    assert auth.outcome in repr(auth) and str(auth.attempt_id) in repr(auth)

    env["ledger"].finish_attempt(
        auth.attempt_id, nid, claimed.claim_token, outcome="retry",
        next_not_before=None, now=NOW,
    )
    orphan_nid = _batch(env)
    _open_dispatching(env, orphan_nid, now=NOW, duration=timedelta(seconds=10))
    recon = env["store"].claim_for_reconciliation(
        clock=_Clock(NOW + timedelta(seconds=60)), duration=RECON_DURATION,
        notification_id=orphan_nid,
    )
    for text in (repr(recon), str(recon)):
        assert recon.claim_token not in text, "ReconciliationClaim expone el token"
    assert recon.attempt_id in repr(recon)
