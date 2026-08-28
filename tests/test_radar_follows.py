"""Tests de app/core/radar/follows.py — watchlist de procesos individuales.

SQLite temporal + SecopClient fake (no red), patrón de test_radar_runner.py.

Los tests de concurrencia sincronizan con ``threading.Event``/``Barrier``, nunca
con sleeps: la ventana que reproducen es "el otro proceso actúa **mientras** este
está en la red", y eso se expresa con un cliente fake que bloquea dentro de
``query`` hasta que la otra parte hizo lo suyo. Un sleep solo adivinaría.
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.core.formatter import Formatter
from app.core.radar import follows, scheduler
from app.core.radar.lease import LEASE_DURATION
from app.core.radar.store import RadarReservedNameError, RadarStore
from app.core.soql_builder import SoQLBuilder

PROCESOS = SoQLBuilder.PROCESOS_DATASET  # p6dx-8zbt
OWNER = "email:ana@x.com"
OTHER = "email:beto@x.com"
_WAIT = 15  # segundos: techo de seguridad de los Event, no una ventana adivinada


class FakeClient:
    def __init__(self, rows):
        self.rows = rows

    def query(self, dataset_id, soql):
        return list(self.rows)

    def count(self, dataset_id, soql):
        return len(self.rows)


def _deps(rows):
    return SimpleNamespace(
        soql_builder=SoQLBuilder(), secop_client=FakeClient(rows), formatter=Formatter(),
    )


@pytest.fixture
def store(tmp_path):
    return RadarStore(tmp_path / "follows.db")


def _card(pid="P-1", estado="Abierto", valor="$1.000.000"):
    """Fila de tarjeta enriquecida (lo que produce el formatter)."""
    return {
        "titulo": "Suministro PAE escolar", "entidad": "ALCALDIA DE BARRANQUILLA",
        "valor": valor, "estado": estado, "url": f"http://secop/{pid}",
        "id_del_proceso": pid, "referencia_del_proceso": f"REF-{pid}",
        "dataset_id": PROCESOS,
    }


def _raw(pid="P-1", estado="Abierto", precio="1000000"):
    """Fila cruda SECOP (lo que devolvería el cliente al refrescar)."""
    return {
        "id_del_proceso": pid, "referencia_del_proceso": f"REF-{pid}",
        "nombre_del_procedimiento": "Suministro PAE escolar",
        "entidad": "ALCALDIA DE BARRANQUILLA", "precio_base": precio,
        "estado_de_apertura_del_proceso": estado, "urlproceso": f"http://secop/{pid}",
        "fecha_de_publicacion_del": "2026-05-01",
    }


# ── follow / unfollow / list ────────────────────────────────────────────────
def test_follow_persists_and_is_idempotent(store):
    assert follows.follow(store, OWNER, _card("P-1")) is True
    assert follows.is_following(store, OWNER, "P-1") is True
    seguidos = follows.list_follows(store, OWNER)
    assert len(seguidos) == 1 and seguidos[0]["result_key"] == "P-1"

    # Re-seguir no duplica ni emite otro evento "followed".
    assert follows.follow(store, OWNER, _card("P-1")) is True
    assert len(follows.list_follows(store, OWNER)) == 1
    radar = follows.get_or_create_follows_radar(store, OWNER)
    feed = store.get_feed(OWNER)
    assert sum(1 for e in feed if e["kind"] == "followed") == 1


def test_follow_without_key_returns_false(store):
    row = {"titulo": "sin id", "dataset_id": PROCESOS}  # sin id ni referencia
    assert follows.follow(store, OWNER, row) is False
    assert follows.list_follows(store, OWNER) == []


def test_unfollow_removes(store):
    follows.follow(store, OWNER, _card("P-1"))
    follows.unfollow(store, OWNER, "P-1")
    assert follows.is_following(store, OWNER, "P-1") is False
    assert follows.list_follows(store, OWNER) == []


def test_follow_isolated_by_owner(store):
    follows.follow(store, OWNER, _card("P-1"))
    assert follows.list_follows(store, "email:beto@x.com") == []


# ── refresh_follows ──────────────────────────────────────────────────────────
def test_refresh_establishes_baseline_silently(store):
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    deps = _deps([_raw("P-1", estado="Abierto")])
    summary = follows.refresh_follows(deps, store, OWNER)
    assert summary["baseline"] == 1
    assert summary["changed_state"] == 0 and summary["changed_value"] == 0
    # No emite evento de cambio en la línea base.
    feed = store.get_feed(OWNER)
    assert all(e["kind"] == "followed" for e in feed)


def test_refresh_detects_state_change(store):
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)  # baseline
    summary = follows.refresh_follows(_deps([_raw("P-1", estado="Adjudicado")]), store, OWNER)
    assert summary["changed_state"] == 1
    feed = store.get_feed(OWNER)
    ev = [e for e in feed if e["kind"] == "changed_state"]
    assert len(ev) == 1 and ev[0]["prev_status"] == "Abierto" and ev[0]["status"] == "Adjudicado"


def test_refresh_detects_value_change(store):
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw("P-1", precio="1000000")]), store, OWNER)  # baseline
    summary = follows.refresh_follows(_deps([_raw("P-1", precio="5000000")]), store, OWNER)
    assert summary["changed_value"] == 1


def test_refresh_no_change_no_events(store):
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw("P-1")]), store, OWNER)  # baseline
    summary = follows.refresh_follows(_deps([_raw("P-1")]), store, OWNER)  # igual
    assert summary["changed_state"] == 0 and summary["changed_value"] == 0


def test_refresh_marks_missing(store):
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw("P-1")]), store, OWNER)
    summary = follows.refresh_follows(_deps([]), store, OWNER)  # SECOP no lo devuelve
    assert summary["missing"] == 1


# ── integración con el scheduler ──────────────────────────────────────────────
def test_scheduler_skips_follows_radar_on_query_side(store, monkeypatch):
    # Un radar de búsqueda + un proceso seguido (crea el radar sintético).
    store.create_radar(name="Búsqueda PAE", owner=OWNER, query_frame_json="{}",
                       frequency="diaria")
    follows.follow(store, OWNER, _card("P-1"))

    ran = []
    monkeypatch.setattr(scheduler, "run_radar",
                        lambda deps, st, radar, **kwargs:
                        ran.append(radar.name) or SimpleNamespace(
                            status="ok",
                            diff=SimpleNamespace(new_count=0, changed_count=0),
                            digest={}, error=None))

    summary = scheduler.run_due_radars(_deps([_raw("P-1")]), store, force=True)

    # run_radar solo para la búsqueda; el radar de seguidos va por refresh_follows.
    assert ran == ["Búsqueda PAE"]
    assert summary.follows == 1
    assert follows.FOLLOWS_NAME not in ran


# ══════════════════════════════════════════════════════════════════════════
# P1B-Preflight 2 · identidad, lease, atomicidad e idempotencia
# ══════════════════════════════════════════════════════════════════════════

def _events(db_path, kind: str | None = None) -> list[dict]:
    """Eventos leídos por fuera del store (estado real en disco)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        sql = "SELECT * FROM radar_events"
        args: tuple = ()
        if kind:
            sql += " WHERE kind = ?"
            args = (kind,)
        return [dict(r) for r in conn.execute(sql, args)]
    finally:
        conn.close()


def _deps_client(client):
    return SimpleNamespace(soql_builder=SoQLBuilder(), secop_client=client,
                           formatter=Formatter())


class _RecordingClient:
    """Cliente fake que cuenta sus llamadas a la red."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = 0

    def query(self, dataset_id, soql):
        self.calls += 1
        return list(self.rows)


# ── 1-2 · identidad y creación concurrente ────────────────────────────────

def test_concurrent_get_or_create_yields_one_radar_and_one_id(tmp_path):
    """Ocho hilos, ocho conexiones, misma Barrier, radar inexistente.

    El ganador lo decide el UNIQUE(owner,name); los perdedores re-leen su fila.
    Nunca INSERT OR REPLACE, nunca borrar y recrear.
    """
    db_path = tmp_path / "race.db"
    RadarStore(db_path)  # crea el esquema

    workers = 8
    barrier = threading.Barrier(workers)
    ids: list = []
    errors: list = []
    lock = threading.Lock()

    def contend() -> None:
        store = RadarStore(db_path)          # su propia conexión
        try:
            barrier.wait()
            radar = follows.get_or_create_follows_radar(store, OWNER)
        except Exception as exc:
            with lock:
                errors.append(exc)
            return
        with lock:
            ids.append(radar.id)

    threads = [threading.Thread(target=contend) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert errors == [], f"la carrera levantó excepciones: {errors}"
    assert len(ids) == workers
    assert len(set(ids)) == 1, "los callers recibieron radares distintos"

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT id, frequency FROM radars WHERE owner = ?", (OWNER,)
    ).fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0][0] == ids[0]
    assert rows[0][1] == follows.FOLLOWS_FREQUENCY


def test_follows_radars_of_different_owners_are_isolated(store):
    a = follows.get_or_create_follows_radar(store, OWNER)
    b = follows.get_or_create_follows_radar(store, OTHER)

    assert a.id != b.id
    follows.follow(store, OWNER, _card("P-1"))
    assert [r["result_key"] for r in follows.list_follows(store, OWNER)] == ["P-1"]
    assert follows.list_follows(store, OTHER) == []


# ── 3 · nombre reservado ──────────────────────────────────────────────────

def test_search_radar_cannot_take_the_reserved_name(store):
    """El nombre reservado se rechaza: la colisión no puede nacer."""
    with pytest.raises(RadarReservedNameError):
        store.create_radar(name=follows.FOLLOWS_NAME, owner=OWNER,
                           query_frame_json="{}", frequency="diaria")


def test_reserved_name_collision_is_never_mistaken_for_the_watchlist(store):
    """Base preexistente (pre-reserva): el radar de búsqueda no se secuestra.

    Antes, ``get_or_create_follows_radar`` resolvía por nombre y devolvía el
    radar de búsqueda: los seguidos del owner acababan dentro de sus
    radar_seen_results. Ahora se detecta y se informa, sin convertir ni borrar.
    """
    # Se salta la reserva escribiendo directo, como haría una base vieja.
    conn = sqlite3.connect(store.db_path)
    conn.execute(
        "INSERT INTO radars (id, name, description, query_frame_json, frequency,"
        " channel, enabled, owner, created_at) VALUES (?,?,?,?,?,?,1,?,?)",
        ("legacy-1", follows.FOLLOWS_NAME, "", '{"objeto":"PAE"}', "diaria",
         "web", OWNER, "2026-01-01T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    with pytest.raises(follows.FollowsRadarNameConflictError):
        follows.get_or_create_follows_radar(store, OWNER)

    # el radar del usuario sigue intacto: ni convertido, ni sobrescrito, ni borrado
    legacy = store.get_radar("legacy-1")
    assert legacy is not None
    assert legacy.frequency == "diaria"
    assert legacy.query_frame_json == '{"objeto":"PAE"}'


def test_follows_radar_cannot_be_created_with_an_alternative_name(store):
    """Un follows con otro nombre pasaría el UNIQUE y sería un segundo follows."""
    with pytest.raises(RadarReservedNameError):
        store.create_radar(name="Mis procesos", owner=OWNER,
                           query_frame_json="{}",
                           frequency=follows.FOLLOWS_FREQUENCY)


def _insert_raw_radar(store, rid, name, owner, frequency, created_at):
    """Escribe una fila saltándose las reglas, como una base heredada."""
    conn = sqlite3.connect(store.db_path)
    conn.execute(
        "INSERT INTO radars (id, name, description, query_frame_json, frequency,"
        " channel, enabled, owner, created_at) VALUES (?,?,?,?,?,?,1,?,?)",
        (rid, name, "", "{}", frequency, "web", owner, created_at),
    )
    conn.commit()
    conn.close()


def test_two_legacy_follows_radars_raise_instead_of_picking_one(store):
    """Dos follows heredados: error explícito, no elegir uno en silencio.

    Elegir partiría la watchlist: unos seguidos en una fila, otros en la otra,
    y cada refresco vería medio mundo sin que nadie lo note.
    """
    _insert_raw_radar(store, "dup-1", follows.FOLLOWS_NAME, OWNER,
                      follows.FOLLOWS_FREQUENCY, "2026-01-01T00:00:00+00:00")
    _insert_raw_radar(store, "dup-2", "seguidos alt", OWNER,
                      follows.FOLLOWS_FREQUENCY, "2026-01-02T00:00:00+00:00")

    with pytest.raises(follows.FollowsRadarCardinalityError):
        follows.get_or_create_follows_radar(store, OWNER)

    # ninguna fila tocada: reparar es decisión humana
    assert store.get_radar("dup-1").name == follows.FOLLOWS_NAME
    assert store.get_radar("dup-2").name == "seguidos alt"
    assert store.get_radar("dup-1").frequency == follows.FOLLOWS_FREQUENCY
    assert store.get_radar("dup-2").frequency == follows.FOLLOWS_FREQUENCY


def test_single_non_canonical_follows_radar_is_an_integrity_error(store):
    """Una sola fila follows, pero con nombre no canónico: su unicidad no está
    garantizada por el UNIQUE, así que no se opera sobre ella."""
    _insert_raw_radar(store, "legacy-1", "seguidos viejos", OWNER,
                      follows.FOLLOWS_FREQUENCY, "2026-01-01T00:00:00+00:00")

    with pytest.raises(follows.FollowsRadarIntegrityError):
        follows.get_or_create_follows_radar(store, OWNER)

    assert store.get_radar("legacy-1").name == "seguidos viejos"


def test_broken_follows_identity_does_not_stop_other_owners(store, monkeypatch):
    """El scheduler aísla al owner roto y sigue con los demás."""
    _insert_raw_radar(store, "dup-1", follows.FOLLOWS_NAME, OWNER,
                      follows.FOLLOWS_FREQUENCY, "2026-01-01T00:00:00+00:00")
    _insert_raw_radar(store, "dup-2", "seguidos alt", OWNER,
                      follows.FOLLOWS_FREQUENCY, "2026-01-02T00:00:00+00:00")
    follows.follow(store, OTHER, _card("P-2"))     # owner sano

    summary = scheduler.run_due_radars(_deps([_raw("P-2")]), store, force=True)

    assert summary.errors >= 1                       # el owner roto se reporta
    assert summary.follows == 1                      # el sano se refrescó igual
    sanos = [o for o in summary.outcomes if o.status == "follows"]
    assert [o.radar.owner for o in sanos] == [OTHER]


def test_follow_does_not_report_success_if_the_radar_was_deleted(store, monkeypatch):
    """El radar se borra entre la resolución y la materialización.

    Devolver True diría al usuario que sigue un proceso que nadie vigila.
    """
    radar = follows.get_or_create_follows_radar(store, OWNER)
    real = store.materialize_follow_if_absent

    def delete_then_materialize(**kwargs):
        store.delete_radar(radar.id)          # carrera: desaparece justo antes
        return real(**kwargs)

    monkeypatch.setattr(store, "materialize_follow_if_absent", delete_then_materialize)

    with pytest.raises(follows.FollowsRadarIntegrityError):
        follows.follow(store, OWNER, _card("P-1"))


def test_follow_does_not_report_success_if_the_radar_stopped_being_follows(
    store, monkeypatch
):
    """El radar cambia de frequency bajo la transacción: nada se escribe."""
    radar = follows.get_or_create_follows_radar(store, OWNER)
    real = store.materialize_follow_if_absent

    def mutate_then_materialize(**kwargs):
        conn = sqlite3.connect(store.db_path)
        conn.execute("UPDATE radars SET frequency='diaria' WHERE id=?", (radar.id,))
        conn.commit()
        conn.close()
        return real(**kwargs)

    monkeypatch.setattr(store, "materialize_follow_if_absent", mutate_then_materialize)

    with pytest.raises(follows.FollowsRadarIntegrityError):
        follows.follow(store, OWNER, _card("P-1"))

    assert store.get_seen(radar.id) == {}
    assert _events(store.db_path) == []


def test_follow_does_not_report_success_if_the_radar_lost_its_canonical_name(
    store, monkeypatch
):
    """Renombrado bajo la transacción: su unicidad ya no está garantizada."""
    radar = follows.get_or_create_follows_radar(store, OWNER)
    real = store.materialize_follow_if_absent

    def rename_then_materialize(**kwargs):
        conn = sqlite3.connect(store.db_path)
        conn.execute("UPDATE radars SET name='otro nombre' WHERE id=?", (radar.id,))
        conn.commit()
        conn.close()
        return real(**kwargs)

    monkeypatch.setattr(store, "materialize_follow_if_absent", rename_then_materialize)

    with pytest.raises(follows.FollowsRadarIntegrityError):
        follows.follow(store, OWNER, _card("P-1"))

    assert store.get_seen(radar.id) == {}


def test_case_variant_of_reserved_name_does_not_hijack_the_watchlist(store):
    """El UNIQUE es binario y el lookup NOCASE: la variante no puede crearse."""
    variant = follows.FOLLOWS_NAME.replace("Procesos", "procesos")
    with pytest.raises(RadarReservedNameError):
        store.create_radar(name=variant, owner=OWNER, query_frame_json="{}",
                           frequency="diaria")

    radar = follows.get_or_create_follows_radar(store, OWNER)
    assert radar.frequency == follows.FOLLOWS_FREQUENCY


# ── 4-5 · exclusión mutua del refresco ────────────────────────────────────

def test_second_refresh_of_same_owner_skips_without_touching_secop(tmp_path):
    """B llega mientras A está en la red: B no consulta, no escribe, no falla.

    Determinista: el cliente de A bloquea dentro de ``query`` hasta que B ya
    intentó, así la ventana existe de verdad y no depende del scheduler del SO.
    """
    db_path = tmp_path / "double.db"
    seed = RadarStore(db_path)
    follows.follow(seed, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), seed, OWNER)

    a_in_network = threading.Event()
    b_finished = threading.Event()
    b_client = _RecordingClient([_raw("P-1", estado="Adjudicado")])
    results: dict = {}

    class GateClient:
        calls = 0

        def query(self, dataset_id, soql):
            GateClient.calls += 1
            a_in_network.set()            # A ya tiene el lease y está en la red
            b_finished.wait(timeout=_WAIT)
            return [_raw("P-1", estado="Adjudicado")]

    def run_a():
        results["a"] = follows.refresh_follows(
            _deps_client(GateClient()), RadarStore(db_path), OWNER)

    def run_b():
        assert a_in_network.wait(timeout=_WAIT), "A nunca llegó a la red"
        results["b"] = follows.refresh_follows(
            _deps_client(b_client), RadarStore(db_path), OWNER)
        b_finished.set()

    ta, tb = threading.Thread(target=run_a), threading.Thread(target=run_b)
    ta.start(); tb.start()
    ta.join(timeout=60); tb.join(timeout=60)

    assert results["b"]["status"] == follows.REFRESH_ALREADY_RUNNING
    assert b_client.calls == 0, "el perdedor consultó SECOP"
    assert results["a"]["status"] == follows.REFRESH_REFRESHED
    # un solo evento: exactamente el bug N15 que este bloque cierra
    assert len(_events(db_path, "changed_state")) == 1


def test_refresh_of_different_owners_runs_in_parallel(tmp_path):
    """El lease de A no bloquea a B: son radares distintos.

    La Barrier(2) dentro de ``query`` sólo se libera si ambos owners llegan a la
    red a la vez. Si uno bloqueara al otro, esto revienta por BrokenBarrier.
    """
    db_path = tmp_path / "parallel.db"
    seed = RadarStore(db_path)
    follows.follow(seed, OWNER, _card("P-1"))
    follows.follow(seed, OTHER, _card("P-2"))
    for owner in (OWNER, OTHER):
        follows.refresh_follows(
            _deps_client(_RecordingClient([_raw("P-1"), _raw("P-2")])),
            seed, owner)  # baseline

    barrier = threading.Barrier(2, timeout=_WAIT)
    results: dict = {}
    errors: list = []
    lock = threading.Lock()

    class ParallelClient:
        def __init__(self, pid):
            self.pid = pid

        def query(self, dataset_id, soql):
            barrier.wait()      # exige concurrencia real
            return [_raw(self.pid, estado="Adjudicado")]

    def run(owner, pid):
        try:
            res = follows.refresh_follows(
                _deps_client(ParallelClient(pid)), RadarStore(db_path), owner)
            with lock:
                results[owner] = res
        except Exception as exc:
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=run, args=(OWNER, "P-1")),
               threading.Thread(target=run, args=(OTHER, "P-2"))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert errors == [], f"los owners se bloquearon entre sí: {errors}"
    assert results[OWNER]["status"] == follows.REFRESH_REFRESHED
    assert results[OTHER]["status"] == follows.REFRESH_REFRESHED
    assert results[OWNER]["changed_state"] == 1
    assert results[OTHER]["changed_state"] == 1


# ── 6 · lease robado durante la red ───────────────────────────────────────

def test_lease_stolen_during_network_materializes_nothing(store):
    """A pierde el lease mientras consulta: no escribe ni pisa al sucesor."""
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw("P-1")]), store, OWNER)  # baseline
    radar = follows.get_or_create_follows_radar(store, OWNER)
    before = store.get_seen(radar.id)["P-1"]["snapshot_hash"]
    stolen: dict = {}

    class StealingClient:
        def query(self, dataset_id, soql):
            # El tiempo avanza y B recupera el lease vencido de A.
            future = datetime.now(timezone.utc) + LEASE_DURATION + timedelta(seconds=1)
            claim = store.try_acquire_run_lease(radar.id, now=future)
            assert claim.acquired
            stolen["token"] = claim.token
            return [_raw("P-1", estado="Adjudicado")]

    res = follows.refresh_follows(_deps_client(StealingClient()), store, OWNER)

    assert res["status"] == follows.REFRESH_LEASE_LOST
    assert store.get_seen(radar.id)["P-1"]["snapshot_hash"] == before  # 0 seen tocados
    assert _events(store.db_path, "changed_state") == []               # 0 eventos
    # el finally de A no libera el lease de B
    assert store.get_radar(radar.id).run_lease_token == stolen["token"]


# ── 10-11 · follow / unfollow durante el fetch ────────────────────────────

def test_unfollow_during_fetch_is_not_resurrected(store):
    """Bloqueo confirmado en la investigación (§E): el UPSERT revivía la fila.

    El usuario deja de seguir P-1 mientras el refresco está en la red. Al
    materializar, P-1 no debe volver ni emitir evento.
    """
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)

    class UnfollowingClient:
        def query(self, dataset_id, soql):
            follows.unfollow(store, OWNER, "P-1")     # ocurre DURANTE la red
            return [_raw("P-1", estado="Adjudicado")]

    res = follows.refresh_follows(_deps_client(UnfollowingClient()), store, OWNER)

    assert res["status"] == follows.REFRESH_REFRESHED
    assert follows.is_following(store, OWNER, "P-1") is False, "P-1 resucitó"
    assert follows.list_follows(store, OWNER) == []
    assert _events(store.db_path, "changed_state") == []


def test_refollow_after_baseline_does_not_silence_the_next_change(store):
    """Re-seguir algo ya seguido no puede borrar su línea base.

    Con el UPSERT anterior, ``follow`` reescribía snapshot_hash a '' y el
    refresco siguiente trataba un cambio real como baseline: lo silenciaba.
    """
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)
    radar = follows.get_or_create_follows_radar(store, OWNER)
    base = store.get_seen(radar.id)["P-1"]["snapshot_hash"]
    assert base != ""

    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))   # re-follow

    assert store.get_seen(radar.id)["P-1"]["snapshot_hash"] == base
    assert len([e for e in store.get_feed(OWNER) if e["kind"] == "followed"]) == 1

    # y el cambio siguiente SÍ se anuncia: la baseline no se perdió
    res = follows.refresh_follows(_deps([_raw("P-1", estado="Adjudicado")]), store, OWNER)
    assert res["changed_state"] == 1
    assert res["baseline"] == 0
    assert len(_events(store.db_path, "changed_state")) == 1


def test_concurrent_follows_of_same_key_create_one_row_and_one_event(tmp_path):
    """Ocho clics simultáneos sobre la misma tarjeta: una fila, un evento."""
    db_path = tmp_path / "follow_race.db"
    RadarStore(db_path)
    follows.get_or_create_follows_radar(RadarStore(db_path), OWNER)

    workers = 8
    barrier = threading.Barrier(workers)
    errors: list = []
    lock = threading.Lock()

    def contend() -> None:
        store = RadarStore(db_path)
        try:
            barrier.wait()
            follows.follow(store, OWNER, _card("P-1"))
        except Exception as exc:
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=contend) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert errors == [], f"la carrera levantó excepciones: {errors}"
    store = RadarStore(db_path)
    assert len(follows.list_follows(store, OWNER)) == 1
    assert len([e for e in _events(db_path) if e["kind"] == "followed"]) == 1


def test_unfollow_during_fetch_reports_no_change(store):
    """Conteos materializados: la clave descartada no se anuncia como cambio.

    El conteo construido diría changed_state=1 y el scheduler —y mañana el
    notifier— anunciarían un cambio que nunca entró al feed.
    """
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)

    class UnfollowingClient:
        def query(self, dataset_id, soql):
            follows.unfollow(store, OWNER, "P-1")
            return [_raw("P-1", estado="Adjudicado")]     # cambio remoto real

    res = follows.refresh_follows(_deps_client(UnfollowingClient()), store, OWNER)

    assert res["changed_state"] == 0
    assert res["events"] == 0
    assert _events(store.db_path, "changed_state") == []


def test_scheduler_reports_no_change_when_unfollow_wins(tmp_path):
    """El conteo mentiroso no llega al scheduler."""
    db_path = tmp_path / "counts.db"
    store = RadarStore(db_path)
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)

    class UnfollowingClient:
        def query(self, dataset_id, soql):
            follows.unfollow(store, OWNER, "P-1")
            return [_raw("P-1", estado="Adjudicado")]

    summary = scheduler.run_due_radars(
        _deps_client(UnfollowingClient()), store, force=True)

    assert summary.changed_total == 0
    assert summary.errors == 0


def test_unfollow_then_refollow_during_fetch_does_not_touch_the_new_membership(store):
    """La carrera fina: la clave sobrevive, pero la **membresía** es otra.

    ``unfollow`` + ``follow`` de la misma clave durante la red deja una fila
    nueva con la misma (radar_id, result_key). Sin el fence de versión, el
    UPDATE acertaría con rowcount 1 y el refresco le pisaría el
    ``snapshot_hash=''`` —matándole la línea base— y emitiría un evento que
    describe la membresía muerta. ``first_seen_at`` distingue encarnaciones.
    """
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)
    radar = follows.get_or_create_follows_radar(store, OWNER)
    old_first_seen = store.get_seen(radar.id)["P-1"]["first_seen_at"]

    class ReFollowingClient:
        def query(self, dataset_id, soql):
            # el owner deja de seguirlo y lo vuelve a seguir mientras hay red
            follows.unfollow(store, OWNER, "P-1")
            follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
            return [_raw("P-1", estado="Adjudicado")]     # cambio remoto real

    res = follows.refresh_follows(_deps_client(ReFollowingClient()), store, OWNER)

    assert res["changed_state"] == 0
    assert res["events"] == 0
    assert _events(store.db_path, "changed_state") == []

    seen = store.get_seen(radar.id)["P-1"]
    assert seen["first_seen_at"] != old_first_seen, "la membresía no se recreó"
    # la membresía nueva conserva su baseline pendiente: el refresco no la tocó
    assert seen["snapshot_hash"] == ""

    # y en el ciclo siguiente fija baseline en silencio, como cualquier alta
    res2 = follows.refresh_follows(
        _deps([_raw("P-1", estado="Adjudicado")]), store, OWNER)
    assert res2["baseline"] == 1
    assert res2["changed_state"] == 0
    assert _events(store.db_path, "changed_state") == []
    assert store.get_seen(radar.id)["P-1"]["snapshot_hash"] != ""


def test_follow_during_fetch_is_left_for_the_next_cycle(store):
    """K2 entra mientras A está en la red: A no lo pisa ni lo trata como cambio."""
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)
    radar = follows.get_or_create_follows_radar(store, OWNER)

    class FollowingClient:
        def query(self, dataset_id, soql):
            follows.follow(store, OWNER, _card("P-2", estado="Abierto"))
            return [_raw("P-1", estado="Adjudicado"), _raw("P-2", estado="Abierto")]

    res = follows.refresh_follows(_deps_client(FollowingClient()), store, OWNER)

    assert res["status"] == follows.REFRESH_REFRESHED
    assert res["changed_state"] == 1                    # solo P-1
    seen = store.get_seen(radar.id)
    assert set(seen) == {"P-1", "P-2"}
    # P-2 conserva su snapshot vacío: su baseline es del ciclo siguiente
    assert seen["P-2"]["snapshot_hash"] == ""
    assert {e["result_key"] for e in _events(store.db_path, "changed_state")} == {"P-1"}

    # y en el ciclo siguiente P-2 fija baseline en silencio
    res2 = follows.refresh_follows(
        _deps([_raw("P-1", estado="Adjudicado"), _raw("P-2", estado="Abierto")]),
        store, OWNER)
    assert res2["baseline"] == 1
    assert store.get_seen(radar.id)["P-2"]["snapshot_hash"] != ""


# ── 12 · baseline silencioso y atómico ────────────────────────────────────

def test_baseline_is_silent_then_emits_exactly_one_event(store):
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))

    first = follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)
    assert first["baseline"] == 1
    assert first["changed_state"] == 0
    assert _events(store.db_path, "changed_state") == []

    second = follows.refresh_follows(
        _deps([_raw("P-1", estado="Adjudicado")]), store, OWNER)
    assert second["changed_state"] == 1
    evs = _events(store.db_path, "changed_state")
    assert len(evs) == 1
    assert evs[0]["prev_status"] == "Abierto" and evs[0]["status"] == "Adjudicado"


# ── 13 · errores remotos ──────────────────────────────────────────────────

def test_lookup_error_keeps_membership_and_releases_lease(store):
    """Un lookup que falla no confirma desaparición ni emite cambio falso."""
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw("P-1")]), store, OWNER)  # baseline
    radar = follows.get_or_create_follows_radar(store, OWNER)
    before = store.get_seen(radar.id)["P-1"]

    class BoomClient:
        def query(self, dataset_id, soql):
            raise RuntimeError("socrata 503")

    res = follows.refresh_follows(_deps_client(BoomClient()), store, OWNER)

    assert res["status"] == follows.REFRESH_REFRESHED
    assert res["lookup_errors"] == 1
    assert res["changed_state"] == 0 and res["changed_value"] == 0
    assert follows.is_following(store, OWNER, "P-1") is True   # no borra membresía
    assert store.get_seen(radar.id)["P-1"] == before           # ni la toca
    assert _events(store.db_path, "changed_state") == []       # sin evento falso
    assert store.get_radar(radar.id).run_lease_token is None   # lease liberado


def test_empty_lookup_does_not_delete_followed_items(store):
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw("P-1")]), store, OWNER)

    res = follows.refresh_follows(_deps([]), store, OWNER)   # SECOP no lo devuelve

    assert res["missing"] == 1
    assert follows.is_following(store, OWNER, "P-1") is True
    assert _events(store.db_path, "changed_state") == []


def test_duplicate_rows_from_secop_emit_one_event(store):
    """El SoQL busca `id in(...) OR referencia in(...)`: SECOP puede duplicar."""
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)

    dup = _raw("P-1", estado="Adjudicado")
    res = follows.refresh_follows(_deps([dup, dict(dup)]), store, OWNER)

    assert res["changed_state"] == 1
    assert len(_events(store.db_path, "changed_state")) == 1


# ── 14 · identidad de los eventos ─────────────────────────────────────────

def test_refresh_events_carry_the_identity_of_the_protected_radar(store):
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)
    radar = follows.get_or_create_follows_radar(store, OWNER)

    follows.refresh_follows(_deps([_raw("P-1", estado="Adjudicado")]), store, OWNER)

    ev = _events(store.db_path, "changed_state")[0]
    assert ev["radar_id"] == radar.id
    assert ev["owner"] == OWNER
    assert ev["radar_name"] == follows.FOLLOWS_NAME
    assert ev["run_id"] is None          # no hay corrida; ver investigación §F.2
    assert store.get_seen(radar.id)["P-1"]["last_run_id"] == "follows_refresh"


# ── 17 · regresión del feed visible ───────────────────────────────────────

def test_feed_keeps_its_items_kinds_and_semantics(store):
    """La UI no cambia: mismos items, mismas clases de evento, mismo render."""
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)
    follows.refresh_follows(_deps([_raw("P-1", estado="Adjudicado")]), store, OWNER)
    follows.refresh_follows(
        _deps([_raw("P-1", estado="Adjudicado", precio="9000000")]), store, OWNER)

    feed = store.get_feed(OWNER)
    assert [e["kind"] for e in feed] == ["changed_value", "changed_state", "followed"]
    for ev in feed:
        assert ev["owner"] == OWNER
        assert ev["radar_name"] == follows.FOLLOWS_NAME
        assert ev["result_key"] == "P-1"
        assert ev["title"] == "Suministro PAE escolar"
        assert ev["urlproceso"] == "http://secop/P-1"

    seguidos = follows.list_follows(store, OWNER)
    assert len(seguidos) == 1 and seguidos[0]["result_key"] == "P-1"
    assert seguidos[0]["status"] == "Adjudicado"
    follows.unfollow(store, OWNER, "P-1")
    assert follows.list_follows(store, OWNER) == []


# ── proyección inmutable del evento (R2B-1B1) ──────────────────────────────
# Los seguidos usan la MISMA proyección que el radar normal: un evento se
# describe con lo observado al emitirlo, nunca releyendo el estado guardado.

_MODALIDAD = "Licitación pública"
_CIERRE = "2026-08-20T17:00:00.000"
CONTRATOS = SoQLBuilder.CONTRATOS_DATASET


def _raw_meta(pid="P-1", estado="Abierto", precio="1000000",
              modalidad=_MODALIDAD, cierre=_CIERRE):
    row = _raw(pid, estado=estado, precio=precio)
    row["modalidad_de_contratacion"] = modalidad
    row["fecha_de_recepcion_de"] = cierre
    return row


def _raw_cont(cid="C-1", estado="En ejecución", valor="5000000",
              contratista="UNION TEMPORAL PAE", firma="2026-03-11T00:00:00.000"):
    return {
        "id_contrato": cid, "referencia_del_contrato": f"RC-{cid}",
        "objeto_del_contrato": "Suministro PAE escolar",
        "nombre_entidad": "ALCALDIA DE BARRANQUILLA",
        "valor_del_contrato": valor, "estado_contrato": estado,
        "modalidad_de_contratacion": "Contratación directa",
        "proveedor_adjudicado": contratista, "fecha_de_firma": firma,
        "urlproceso": f"http://secop/{cid}",
    }


def _card_cont(cid="C-1", estado="En ejecución"):
    return {
        "titulo": "Suministro PAE escolar", "entidad": "ALCALDIA DE BARRANQUILLA",
        "valor": "$5.000.000", "estado": estado, "url": f"http://secop/{cid}",
        "id_contrato": cid, "referencia_del_contrato": f"RC-{cid}",
        "contratista": "UNION TEMPORAL PAE", "dataset_id": CONTRATOS,
    }


def _only(feed, kind):
    hits = [e for e in feed if e["kind"] == kind]
    assert len(hits) == 1, f"esperaba 1 evento {kind}, hubo {len(hits)}"
    return hits[0]


def test_followed_event_has_the_metadata_shape(store):
    """19 · el alta persiste los campos disponibles en la fila que trae el caller.

    Una tarjeta de PROCESO no transporta ninguno de los cuatro (el formatter solo
    le pone identidad y display), así que salen en NULL. La forma del evento sí
    está completa, y no se rellena leyendo radar_seen_results.
    """
    follows.follow(store, OWNER, _card("P-1"))
    ev = _only(store.get_feed(OWNER), "followed")
    assert set(ev.keys()) >= {"modality", "closing_date", "contractor", "signature_date"}
    assert (ev["modality"], ev["closing_date"],
            ev["contractor"], ev["signature_date"]) == (None, None, None, None)


def test_followed_contract_card_keeps_the_contractor(store):
    """19c · la tarjeta de CONTRATO sí trae contratista, bajo su alias visible.

    La membresía ya lo conservaba; el evento —que es lo inmutable, y lo que leerá
    la notificación— lo perdía porque la proyección busca el nombre crudo
    ``proveedor_adjudicado`` y la tarjeta lo llama ``contratista``.
    """
    follows.follow(store, OWNER, _card_cont("C-1"))
    ev = _only(store.get_feed(OWNER), "followed")
    assert ev["contractor"] == "UNION TEMPORAL PAE"
    # La tarjeta trae `fecha`, pero puede ser publicación/apertura/recepción:
    # promoverla a cierre afirmaría un hecho que nadie fijó.
    assert ev["closing_date"] is None


def test_followed_contract_card_contractor_matches_the_membership(store):
    """El evento y la membresía cuentan lo mismo del alta: si divergen, la
    notificación y la lista de seguidos se contradicen sobre el mismo hecho."""
    card = _card_cont("C-2")
    card["contratista"] = "CONSORCIO PAE"
    follows.follow(store, OWNER, card)

    ev = _only(store.get_feed(OWNER), "followed")
    seguido = [s for s in follows.list_follows(store, OWNER)
               if s["result_key"] == "C-2"][0]
    assert ev["contractor"] == "CONSORCIO PAE" == seguido["contractor"]


def test_followed_blank_contractor_stays_null(store):
    """Un contratista en blanco en la tarjeta no se guarda como cadena vacía:
    'no sabemos' y 'sabemos que está vacío' no deben distinguirse aguas abajo."""
    card = _card_cont("C-3")
    card["contratista"] = "   "
    follows.follow(store, OWNER, card)
    assert _only(store.get_feed(OWNER), "followed")["contractor"] is None


def test_followed_event_projects_metadata_when_the_row_carries_it(store):
    """19b · misma proyección que el resto del feed: si la fila trae las columnas
    crudas, el alta las congela sin que haya que tocar follows.py."""
    card = _card("P-9")
    card["modalidad_de_contratacion"] = _MODALIDAD
    card["fecha_de_recepcion_de"] = _CIERRE

    follows.follow(store, OWNER, card)
    ev = _only(store.get_feed(OWNER), "followed")
    assert ev["modality"] == _MODALIDAD
    assert ev["closing_date"] == _CIERRE


def test_refresh_state_change_persists_current_metadata(store):
    """20 · refresh con cambio de estado congela la metadata observada ahora."""
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw_meta("P-1", estado="Abierto")]), store, OWNER)
    follows.refresh_follows(
        _deps([_raw_meta("P-1", estado="Adjudicado", cierre="2026-09-30T17:00:00.000")]),
        store, OWNER,
    )
    ev = _only(store.get_feed(OWNER), "changed_state")
    assert ev["prev_status"] == "Abierto"
    assert ev["modality"] == _MODALIDAD
    assert ev["closing_date"] == "2026-09-30T17:00:00.000"


def test_refresh_value_change_persists_current_metadata(store):
    """21 · refresh con cambio de valor congela la metadata observada ahora."""
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw_meta("P-1", precio="1000000")]), store, OWNER)
    follows.refresh_follows(_deps([_raw_meta("P-1", precio="5000000")]), store, OWNER)

    ev = _only(store.get_feed(OWNER), "changed_value")
    assert ev["prev_value"] == "1000000" and ev["value"] == "5000000"
    assert ev["modality"] == _MODALIDAD
    assert ev["closing_date"] == _CIERRE


def test_refresh_contratos_persists_contractor_and_signature(store):
    """La misma proyección aplica a contratos seguidos."""
    follows.follow(store, OWNER, _card_cont("C-1"))
    follows.refresh_follows(_deps([_raw_cont("C-1", valor="5000000")]), store, OWNER)
    follows.refresh_follows(
        _deps([_raw_cont("C-1", valor="8000000", contratista="CONSORCIO ANDINO",
                         firma="2026-04-02T00:00:00.000")]),
        store, OWNER,
    )
    ev = _only(store.get_feed(OWNER), "changed_value")
    assert ev["contractor"] == "CONSORCIO ANDINO"
    assert ev["signature_date"] == "2026-04-02T00:00:00.000"
    assert ev["modality"] == "Contratación directa"
    assert ev["closing_date"] is None


def test_refresh_metadata_only_change_emits_no_event(store):
    """La metadata no decide el cambio tampoco en follows: cambiar modalidad y
    cierre, con estado y valor intactos, no debe mover el feed."""
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw_meta("P-1")]), store, OWNER)  # baseline

    summary = follows.refresh_follows(
        _deps([_raw_meta("P-1", modalidad="Selección abreviada", cierre="2027-01-01")]),
        store, OWNER,
    )
    assert summary["changed_state"] == 0 and summary["changed_value"] == 0
    assert [e["kind"] for e in store.get_feed(OWNER)] == ["followed"]
