"""Tests de app/core/notify/schema.py — el preflight de bases heredadas.

``CREATE TABLE IF NOT EXISTS`` no toca una tabla que ya existe. Sin preflight, una
base creada cuando ``daily``/``weekly`` eran legales conservaría en silencio un CHECK
que sigue aceptándolas, y el invariante de R2B-1A sería falso exactamente donde nadie
mira: en la base vieja de alguien, por SQL directo.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from app.core.db import connect_sqlite
from app.core.notify.endpoint_store import EndpointStore
from app.core.notify.identity_store import IdentityStore
from app.core.notify.masking import Masker
from app.core.notify.preference_store import PreferenceStore
from app.core.notify.schema import (
    _EXPECTED_FREQUENCY_CHECK,
    _normalized_ddl,
    _preferences_ddl,
    SchemaIncompatibleError,
    ensure_schema,
)
from app.core.radar.store import RadarStore

_OWNER = "email:ana@example.com"

# El CHECK que tenía la tabla cuando el sistema aceptaba tres frecuencias y no
# implementaba ninguna cadencia. Es el estado exacto del que hay que rescatar —o del
# que hay que negarse a rescatar— una base real.
_LEGACY_CHECK = "frequency IN ('per_run','daily','weekly')"
_CURRENT_CHECK = "frequency = 'per_run'"


@pytest.fixture
def db(tmp_path):
    return tmp_path / "schema.db"


@pytest.fixture
def seeded(db):
    """Base vigente y completa, con una preferencia real de Ana."""
    radars = RadarStore(db)
    identity = IdentityStore(db)
    endpoints = EndpointStore(db, masker=Masker.from_key("schema-test-key"))
    preferences = PreferenceStore(db)
    principal = identity.create_principal("ana@example.com")
    identity.link_owner_key(_OWNER, principal)
    endpoint = endpoints.create_endpoint(
        principal_id=principal, channel="telegram", address="ana-chat"
    )
    endpoints.mark_verified(endpoint, external_recipient_id="chat-ana")
    radar = radars.create_radar(
        name="Radar PAE", owner=_OWNER,
        query_frame_json=json.dumps({"dataset": "procesos"}),
    )
    pref = preferences.create_preference(radar_id=radar, endpoint_id=endpoint)
    return {"db": db, "radar": radar, "endpoint": endpoint, "preference": pref}


def _downgrade_to_legacy_ddl(db_path):
    """Reescribe el DDL persistido al del vocabulario viejo, sin tocar las filas.

    Es la única forma de fabricar la base que este preflight defiende: el código
    vigente no puede crearla, así que sin esta puerta trasera el caso sería
    inconstruible y el preflight no tendría prueba.
    """
    conn = connect_sqlite(db_path)
    try:
        conn.execute("PRAGMA writable_schema=ON")
        conn.execute(
            "UPDATE sqlite_master SET sql = REPLACE(sql, ?, ?) "
            "WHERE type='table' AND name='radar_notification_preferences'",
            (_CURRENT_CHECK, _LEGACY_CHECK),
        )
        conn.execute("PRAGMA writable_schema=OFF")
    finally:
        conn.close()


def _table_sql(db_path):
    conn = connect_sqlite(db_path)
    try:
        return conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' "
            "AND name='radar_notification_preferences'"
        ).fetchone()["sql"]
    finally:
        conn.close()


def _reopen(db_path):
    """Vuelve a abrir la base como lo haría cualquier Store: dispara el preflight."""
    conn = connect_sqlite(db_path)
    try:
        ensure_schema(conn)
    finally:
        conn.close()


# ── el CHECK vigente ──────────────────────────────────────────────────────

def test_fresh_database_only_accepts_per_run(seeded):
    assert _CURRENT_CHECK in _table_sql(seeded["db"])
    assert "daily" not in _table_sql(seeded["db"])


def test_expected_check_fingerprint_matches_the_real_ddl():
    """La huella del preflight se compara contra el DDL real, no contra sí misma.

    ``_EXPECTED_FREQUENCY_CHECK`` es texto normalizado a mano: si alguien reescribe
    el CHECK como ``IN ('per_run')`` sin tocar la constante, toda base vigente
    parecería heredada y el preflight reconstruiría en cada apertura. Este test es lo
    que hace que ese desliz salga aquí y no en producción.
    """
    assert _EXPECTED_FREQUENCY_CHECK in _normalized_ddl(
        _preferences_ddl("radar_notification_preferences")
    )


def test_ensure_schema_is_idempotent(seeded):
    _reopen(seeded["db"])
    _reopen(seeded["db"])

    assert _CURRENT_CHECK in _table_sql(seeded["db"])
    store = PreferenceStore(seeded["db"])
    assert store.get_preference(seeded["preference"]) is not None


# ── base heredada compatible → se estrecha la regla, no se tocan los datos ─

def test_legacy_ddl_with_only_per_run_rows_is_rebuilt_preserving_them(seeded):
    """Negarse a abrir una base cuyas preferencias son todas ``per_run`` obligaría a
    destruir registros de consentimiento para arrancar. Se estrecha el CHECK y se
    conserva cada fila."""
    _downgrade_to_legacy_ddl(seeded["db"])
    assert "daily" in _table_sql(seeded["db"])          # la base es heredada de verdad
    before = PreferenceStore(seeded["db"]).get_preference(seeded["preference"])

    _reopen(seeded["db"])

    assert _CURRENT_CHECK in _table_sql(seeded["db"])
    assert "daily" not in _table_sql(seeded["db"])
    after = PreferenceStore(seeded["db"]).get_preference(seeded["preference"])
    assert after == before


def test_rebuilt_legacy_table_actually_enforces_the_new_check(seeded):
    """La reconstrucción no es cosmética: el CHECK nuevo tiene que morder."""
    _downgrade_to_legacy_ddl(seeded["db"])
    conn = connect_sqlite(seeded["db"])
    try:  # antes del preflight, la base heredada sí acepta 'daily'
        conn.execute(
            "UPDATE radar_notification_preferences SET frequency='daily' "
            "WHERE preference_id=?", (seeded["preference"],),
        )
        conn.execute(
            "UPDATE radar_notification_preferences SET frequency='per_run' "
            "WHERE preference_id=?", (seeded["preference"],),
        )
    finally:
        conn.close()

    _reopen(seeded["db"])

    conn = connect_sqlite(seeded["db"])
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE radar_notification_preferences SET frequency='daily' "
                "WHERE preference_id=?", (seeded["preference"],),
            )
    finally:
        conn.close()


def test_rebuild_preserves_foreign_keys(seeded):
    """El rebuild apaga ``foreign_keys`` para poder soltar la tabla; el
    ``foreign_key_check`` de dentro es lo que devuelve esa garantía."""
    _downgrade_to_legacy_ddl(seeded["db"])

    _reopen(seeded["db"])

    conn = connect_sqlite(seeded["db"])
    try:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO radar_notification_preferences "
                "(preference_id, radar_id, endpoint_id, enabled, frequency, "
                " timezone, max_items, include_new, include_state_changes, "
                " include_value_changes, include_disappeared, created_at, updated_at) "
                "VALUES ('x','radar-fantasma',?,0,'per_run','America/Bogota',10,"
                " 1,1,1,0,'t','t')",
                (seeded["endpoint"],),
            )
    finally:
        conn.close()


# ── base heredada incompatible → se para y se reporta ─────────────────────

@pytest.mark.parametrize("deferred", ["daily", "weekly"])
def test_legacy_row_with_deferred_frequency_refuses_to_open(seeded, deferred):
    """Quien eligió un resumen diario no consintió recibir uno por corrida.

    Convertir su fila a ``per_run`` en silencio sería inventarle un consentimiento
    para cumplir el invariante: el invariante no vale ese precio. Se para y se
    reporta.
    """
    _downgrade_to_legacy_ddl(seeded["db"])
    conn = connect_sqlite(seeded["db"])
    try:
        conn.execute(
            "UPDATE radar_notification_preferences SET frequency=? "
            "WHERE preference_id=?", (deferred, seeded["preference"]),
        )
    finally:
        conn.close()

    with pytest.raises(SchemaIncompatibleError) as exc:
        _reopen(seeded["db"])

    assert deferred in str(exc.value)
    assert seeded["preference"] in str(exc.value)


@pytest.mark.parametrize("deferred", ["daily", "weekly"])
def test_refused_database_is_left_exactly_as_it_was(seeded, deferred):
    """Fallar sin escribir: la fila incompatible sigue ahí para que un humano decida."""
    _downgrade_to_legacy_ddl(seeded["db"])
    conn = connect_sqlite(seeded["db"])
    try:
        conn.execute(
            "UPDATE radar_notification_preferences SET frequency=? "
            "WHERE preference_id=?", (deferred, seeded["preference"]),
        )
    finally:
        conn.close()
    sql_before = _table_sql(seeded["db"])

    with pytest.raises(SchemaIncompatibleError):
        _reopen(seeded["db"])

    assert _table_sql(seeded["db"]) == sql_before
    conn = connect_sqlite(seeded["db"])
    try:
        assert conn.execute(
            "SELECT frequency FROM radar_notification_preferences "
            "WHERE preference_id=?", (seeded["preference"],),
        ).fetchone()["frequency"] == deferred
    finally:
        conn.close()


def test_stores_cannot_open_an_incompatible_database(seeded):
    """El preflight vive en ``ensure_schema``, así que ningún Store lo esquiva."""
    _downgrade_to_legacy_ddl(seeded["db"])
    conn = connect_sqlite(seeded["db"])
    try:
        conn.execute(
            "UPDATE radar_notification_preferences SET frequency='weekly' "
            "WHERE preference_id=?", (seeded["preference"],),
        )
    finally:
        conn.close()

    with pytest.raises(SchemaIncompatibleError):
        PreferenceStore(seeded["db"])


# ── el vocabulario es la fuente de verdad ─────────────────────────────────

def test_frequencies_contains_only_per_run():
    """``FREQUENCIES`` es lo que el sistema promete saber cumplir.

    Anclado directamente y no solo a través de sus efectos: es la constante que hay
    que tocar para volver a ofrecer una cadencia, y el punto donde alguien debe
    tropezar con la pregunta "¿y quién la implementa?".
    """
    from app.core.notify.schema import FREQUENCIES, FREQUENCY_PER_RUN

    assert FREQUENCIES == (FREQUENCY_PER_RUN,)


def test_deferred_frequencies_are_not_offered():
    """Diferido y soportado son conjuntos disjuntos.

    ``DEFERRED_FREQUENCIES`` solo elige el mensaje de error; si una frecuencia
    apareciera en las dos listas, habría dos definiciones de "permitido" y ganaría la
    que se consultara primero.
    """
    from app.core.notify.schema import DEFERRED_FREQUENCIES, FREQUENCIES

    assert set(DEFERRED_FREQUENCIES) == {"daily", "weekly"}
    assert set(DEFERRED_FREQUENCIES).isdisjoint(FREQUENCIES)


# ── R2B-1C-A · idx_attempts_one_open ──────────────────────────────────────────
#
# El índice parcial es lo único que impide dos intentos abiertos por batch a un
# caller que no pase por ``worker_store``: un script, ``LedgerStore.begin_attempt``
# o SQL directo. Dos abiertos = dos llamadas posibles al proveedor por el mismo
# mensaje, y ya no hay forma de distinguirlo de un reintento seguro.

_ONE_OPEN_INDEX = "idx_attempts_one_open"


def _batch_row(db_path, seeded, notification_id):
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "INSERT INTO radar_notification_batches "
            "(notification_id, principal_id, endpoint_id, radar_id, batch_key, "
            " idempotency_key, payload_json, created_at, expires_at, status) "
            "SELECT ?, e.principal_id, ?, ?, ?, ?, '{}', "
            "       '2026-07-16T00:00:00+00:00', '2026-07-17T00:00:00+00:00', 'claimed' "
            "  FROM notification_endpoints e WHERE e.endpoint_id = ?",
            (notification_id, seeded["endpoint"], seeded["radar"],
             f"key-{notification_id}", f"idem-{notification_id}", seeded["endpoint"]),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_attempt(db_path, notification_id, number, phase, outcome=None):
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "INSERT INTO radar_delivery_attempts "
            "(attempt_id, notification_id, attempt_number, phase, started_at, "
            " finished_at, outcome) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (f"att-{notification_id}-{number}", notification_id, number, phase,
             "2026-07-16T00:00:00+00:00",
             "2026-07-16T00:01:00+00:00" if phase == "finished" else None, outcome),
        )
        conn.commit()
    finally:
        conn.close()


def _indexes(db_path):
    conn = connect_sqlite(db_path)
    try:
        return {
            row["name"]: row["sql"]
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
    finally:
        conn.close()


def _drop_one_open_index(db_path):
    """Fabrica la base heredada: escrita antes de que el índice existiera."""
    conn = connect_sqlite(db_path)
    try:
        conn.execute(f"DROP INDEX IF EXISTS {_ONE_OPEN_INDEX}")
        conn.commit()
    finally:
        conn.close()


def test_fresh_database_creates_the_partial_unique_index(seeded):
    sql = _indexes(seeded["db"])[_ONE_OPEN_INDEX]
    normalized = _normalized_ddl(sql)
    assert "unique" in normalized
    assert "wherephase='dispatching'" in normalized, "el índice debe ser parcial"


def test_ensure_schema_is_idempotent_for_the_index(seeded):
    before = _indexes(seeded["db"])
    for _ in range(3):
        conn = connect_sqlite(seeded["db"])
        try:
            ensure_schema(conn)
        finally:
            conn.close()
    assert _indexes(seeded["db"]) == before


def test_query_plan_confirms_the_index_is_partial_and_used(seeded):
    conn = connect_sqlite(seeded["db"])
    try:
        plan = " ".join(
            str(row[3]) for row in conn.execute(
                "EXPLAIN QUERY PLAN SELECT notification_id FROM radar_delivery_attempts "
                "WHERE phase = 'dispatching'"
            ).fetchall()
        )
    finally:
        conn.close()
    assert _ONE_OPEN_INDEX in plan


def test_one_open_attempt_per_batch_is_allowed(seeded):
    _batch_row(seeded["db"], seeded, "n-1")
    _insert_attempt(seeded["db"], "n-1", 1, "dispatching")
    conn = connect_sqlite(seeded["db"])
    try:
        ensure_schema(conn)          # no rompe nada: una fila abierta es legal
    finally:
        conn.close()


def test_second_open_attempt_is_physically_rejected(seeded):
    _batch_row(seeded["db"], seeded, "n-1")
    _insert_attempt(seeded["db"], "n-1", 1, "dispatching")
    with pytest.raises(sqlite3.IntegrityError):
        _insert_attempt(seeded["db"], "n-1", 2, "dispatching")


def test_many_finished_attempts_for_the_same_batch_are_valid(seeded):
    _batch_row(seeded["db"], seeded, "n-1")
    for number in range(1, 5):
        _insert_attempt(seeded["db"], "n-1", number, "finished", outcome="retry")
    _insert_attempt(seeded["db"], "n-1", 5, "dispatching")   # una abierta + historial
    conn = connect_sqlite(seeded["db"])
    try:
        ensure_schema(conn)
    finally:
        conn.close()


def test_two_batches_can_each_have_one_open_attempt(seeded):
    for nid in ("n-1", "n-2"):
        _batch_row(seeded["db"], seeded, nid)
        _insert_attempt(seeded["db"], nid, 1, "dispatching")


def test_legacy_database_with_duplicate_open_attempts_refuses_to_open(seeded):
    _drop_one_open_index(seeded["db"])
    for nid in ("n-1", "n-2"):
        _batch_row(seeded["db"], seeded, nid)
        _insert_attempt(seeded["db"], nid, 1, "dispatching")
        _insert_attempt(seeded["db"], nid, 2, "dispatching")
    _batch_row(seeded["db"], seeded, "n-ok")
    _insert_attempt(seeded["db"], "n-ok", 1, "dispatching")

    conn = connect_sqlite(seeded["db"])
    try:
        with pytest.raises(SchemaIncompatibleError) as exc:
            ensure_schema(conn)
    finally:
        conn.close()
    message = str(exc.value)
    assert "2 batch(es)" in message
    assert "n-1=2 intentos" in message and "n-2=2 intentos" in message
    assert "n-ok" not in message, "un batch sano no se denuncia"


def test_refused_database_is_not_repaired_and_keeps_every_row(seeded):
    _drop_one_open_index(seeded["db"])
    _batch_row(seeded["db"], seeded, "n-1")
    _insert_attempt(seeded["db"], "n-1", 1, "dispatching")
    _insert_attempt(seeded["db"], "n-1", 2, "dispatching")

    def snapshot():
        conn = connect_sqlite(seeded["db"])
        try:
            return [
                dict(row) for row in conn.execute(
                    "SELECT * FROM radar_delivery_attempts ORDER BY attempt_id"
                ).fetchall()
            ]
        finally:
            conn.close()

    before = snapshot()
    conn = connect_sqlite(seeded["db"])
    try:
        with pytest.raises(SchemaIncompatibleError):
            ensure_schema(conn)
    finally:
        conn.close()
    # Cerrar uno inventaría cuál llegó al proveedor. No se toca ninguno.
    assert snapshot() == before
    assert len(before) == 2
    assert _ONE_OPEN_INDEX not in _indexes(seeded["db"])


def test_after_a_human_resolves_the_duplicate_the_index_is_created(seeded):
    _drop_one_open_index(seeded["db"])
    _batch_row(seeded["db"], seeded, "n-1")
    _insert_attempt(seeded["db"], "n-1", 1, "dispatching")
    _insert_attempt(seeded["db"], "n-1", 2, "dispatching")

    conn = connect_sqlite(seeded["db"])
    try:
        conn.execute(
            "UPDATE radar_delivery_attempts SET phase='finished', outcome='ambiguous', "
            "finished_at='2026-07-16T00:05:00+00:00' WHERE attempt_id='att-n-1-2'"
        )
        conn.commit()
        ensure_schema(conn)
    finally:
        conn.close()
    assert _ONE_OPEN_INDEX in _indexes(seeded["db"])
