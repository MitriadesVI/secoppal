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
    _ATTEMPTS_TABLE,
    _BATCHES_TABLE,
    _EXPECTED_FREQUENCY_CHECK,
    _LEGACY_DEAD_REASONS,
    _LEGACY_ERROR_CLASSES,
    _PREFERENCES_TABLE,
    _SCHEMA,
    _attempts_ddl,
    _attempts_indexes_ddl,
    _batches_ddl,
    _batches_indexes_ddl,
    _canonical_form,
    _joined_indexes,
    _normalized_ddl,
    _preferences_ddl,
    DEAD_REASONS,
    ERROR_CLASSES,
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


# ══ ADR-053 · Migración del vocabulario del ledger ═══════════════════════════════
#
# La base heredada se fabrica con el **mismo generador** que escribe el DDL vigente,
# parametrizado con el vocabulario viejo. Escribir el DDL legacy a mano en el test
# dejaría dos definiciones que divergen: el día que el DDL real cambiara, el test
# seguiría migrando una tabla que ya no se parece a ninguna base real.

def _schema_script(*, dead_reasons=_LEGACY_DEAD_REASONS, error_classes=_LEGACY_ERROR_CLASSES):
    """El script de apertura con los vocabularios que el test decida.

    Un solo sitio que rellena los cinco placeholders: los tests que fabrican bases
    heredadas no tienen por qué saber cuántos hay, y si mañana aparece uno nuevo, esto
    es lo único que hay que tocar.
    """
    return (
        _SCHEMA
        .replace("{preferences_table}", _preferences_ddl(_PREFERENCES_TABLE))
        .replace("{batches_table}", _batches_ddl(_BATCHES_TABLE, dead_reasons))
        .replace("{batches_indexes}", _joined_indexes(_batches_indexes_ddl(_BATCHES_TABLE)))
        .replace("{attempts_table}", _attempts_ddl(_ATTEMPTS_TABLE, error_classes))
        .replace("{attempts_indexes}", _joined_indexes(_attempts_indexes_ddl(_ATTEMPTS_TABLE)))
    )


_LEDGER_INDEXES = (
    "idx_batches_claimable", "idx_batches_key", "idx_batches_expiry",
    "idx_batches_external_idem", "idx_attempts_notification", "idx_attempts_one_open",
)


def _index_names(conn):
    return {row["name"] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    )}


def _table_sql_of(conn, name):
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?", (name,)
    ).fetchone()
    return None if row is None else row["sql"]


def _legacy_ledger_db(path):
    conn = connect_sqlite(path)
    conn.executescript(_schema_script())
    conn.commit()
    return conn


def _seed_ledger(conn):
    conn.execute(
        "INSERT INTO radar_notification_batches (notification_id, principal_id, "
        "endpoint_id, radar_id, batch_key, idempotency_key, external_idempotency_key, "
        "payload_json, status, dead_reason, not_before, claim_token, claim_until, "
        "attempt_count, created_at, expires_at) VALUES "
        "('n1','p1','e1','r1','bk1','ik1','xk1','{\"a\":1}','dead','opted_out',NULL,"
        "NULL,NULL,2,'2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO radar_notification_batches (notification_id, principal_id, "
        "endpoint_id, radar_id, batch_key, idempotency_key, payload_json, status, "
        "attempt_count, created_at, expires_at) VALUES "
        "('n2','p1','e2','r1','bk2','ik2','{\"b\":2}','pending',0,"
        "'2026-01-01T00:00:00+00:00','2099-01-02T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO radar_notification_batch_events VALUES ('n1','e1','ev-1',5)"
    )
    conn.execute(
        "INSERT INTO radar_delivery_attempts (attempt_id, notification_id, "
        "attempt_number, phase, started_at, finished_at, provider_message_id, "
        "provider_status, error_class, error_message_safe, retry_after, outcome) "
        "VALUES ('a1','n1',1,'finished','2026-01-01T00:00:00+00:00',"
        "'2026-01-01T00:00:01+00:00','mid-1','ok','network_error','safe',30,'retry')"
    )
    conn.execute(
        "INSERT INTO radar_delivery_attempts (attempt_id, notification_id, "
        "attempt_number, phase, started_at) VALUES "
        "('a2','n2',1,'dispatching','2026-01-01T00:00:00+00:00')"
    )
    conn.commit()


class _SpyConn:
    """Proxy sobre la conexión: ``sqlite3.Connection`` es de tipo inmutable y no se
    deja parchear. Se envuelve en vez de monkeypatchear la clase, que además afectaría
    a cualquier otro test que corriera en paralelo."""

    def __init__(self, conn, on_execute):
        self._conn = conn
        self._on_execute = on_execute

    def execute(self, sql, *args):
        self._on_execute(str(sql))
        return self._conn.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _snapshot(conn):
    return {
        "batches": [dict(r) for r in conn.execute(
            "SELECT * FROM radar_notification_batches ORDER BY notification_id")],
        "events": [dict(r) for r in conn.execute(
            "SELECT * FROM radar_notification_batch_events ORDER BY event_id")],
        "attempts": [dict(r) for r in conn.execute(
            "SELECT * FROM radar_delivery_attempts ORDER BY attempt_id")],
    }


def test_53a_new_database_has_the_extended_vocabulary(db):
    conn = connect_sqlite(db)
    ensure_schema(conn)
    batches = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_notification_batches'"
    ).fetchone()["sql"]
    attempts = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_delivery_attempts'"
    ).fetchone()["sql"]
    for reason in ("render_error", "retries_exhausted", "permanent_provider_error"):
        assert reason in batches
    for klass in ("render_error", "provider_unavailable"):
        assert klass in attempts


def test_53b_empty_legacy_database_migrates(db):
    conn = _legacy_ledger_db(db)
    ensure_schema(conn)
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_notification_batches'"
    ).fetchone()["sql"]
    assert "retries_exhausted" in sql


def test_53c_legacy_database_with_rows_migrates_without_losing_any(db):
    """La migración no convierte datos: solo ensancha la regla que los admite. Cada
    fila que entra sale idéntica, campo por campo."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    before = _snapshot(conn)

    ensure_schema(conn)

    assert _snapshot(conn) == before


def test_53d_migration_preserves_foreign_keys_and_integrity(db):
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    ensure_schema(conn)

    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    fks = conn.execute("PRAGMA foreign_key_list(radar_delivery_attempts)").fetchall()
    assert [fk["table"] for fk in fks] == ["radar_notification_batches"]
    # El FK compuesto de la asociación sigue apuntando a la tabla reconstruida. Son dos
    # filas —una por columna del par (notification_id, endpoint_id)—, no dos FK.
    event_fks = conn.execute(
        "PRAGMA foreign_key_list(radar_notification_batch_events)"
    ).fetchall()
    assert {fk["table"] for fk in event_fks} == {"radar_notification_batches"}
    assert {fk["from"] for fk in event_fks} == {"notification_id", "endpoint_id"}


def test_53e_migration_recreates_the_indexes(db):
    """El DROP se lleva los índices por delante; el executescript posterior los
    recrea. Sin ellos, el sweep y la búsqueda de candidatos harían table scan y la
    unicidad de la clave externa dejaría de estar garantizada por la base."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    ensure_schema(conn)

    names = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    )}
    for index in ("idx_batches_claimable", "idx_batches_key", "idx_batches_expiry",
                  "idx_batches_external_idem", "idx_attempts_notification",
                  "idx_attempts_one_open"):
        assert index in names, f"falta {index}"


def test_53f_new_vocabulary_persists_after_migration(db):
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    ensure_schema(conn)

    for reason in ("render_error", "retries_exhausted", "permanent_provider_error"):
        conn.execute(
            "UPDATE radar_notification_batches SET dead_reason=? "
            "WHERE notification_id='n1'", (reason,)
        )
    for klass in ("render_error", "provider_unavailable"):
        conn.execute(
            "UPDATE radar_delivery_attempts SET error_class=? WHERE attempt_id='a1'",
            (klass,),
        )
    conn.commit()
    assert conn.execute(
        "SELECT error_class FROM radar_delivery_attempts WHERE attempt_id='a1'"
    ).fetchone()["error_class"] == "provider_unavailable"


def test_53g_invented_values_are_still_rejected(db):
    """Ensanchar el vocabulario no es abrirlo: el CHECK sigue siendo cerrado."""
    conn = connect_sqlite(db)
    ensure_schema(conn)
    _seed_ledger(conn)

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "UPDATE radar_notification_batches SET dead_reason='inventado' "
            "WHERE notification_id='n1'"
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "UPDATE radar_delivery_attempts SET error_class='inventado' "
            "WHERE attempt_id='a1'"
        )


def test_53h_second_open_does_not_migrate_again(db):
    """Idempotente: sobre una base ya vigente no se reconstruye nada."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    ensure_schema(conn)
    after_first = _snapshot(conn)

    rebuilt: list[str] = []

    def watch(sql):
        if "RENAME TO" in sql:
            rebuilt.append(sql)

    ensure_schema(_SpyConn(conn, watch))

    assert rebuilt == [], "la segunda apertura reconstruyó una tabla ya vigente"
    assert _snapshot(conn) == after_first


def test_53i_unknown_ddl_fails_closed(db):
    """Fail-closed: un CHECK que no es ni el vigente ni el heredado significa que
    alguien editó la tabla a mano o viene de una versión desconocida. Reconstruirla a
    ciegas copiaría filas hacia un vocabulario que quizá no las admita."""
    conn = connect_sqlite(db)
    conn.executescript(
        _schema_script(dead_reasons=("opted_out", "cosa_rara"), error_classes=ERROR_CLASSES)
    )
    conn.commit()

    with pytest.raises(SchemaIncompatibleError, match="no coincide, completo"):
        ensure_schema(conn)


def test_53j_values_outside_the_vocabulary_fail_closed(db):
    """Una fila con un dead_reason que ningún CHECK admitiría solo puede haber entrado
    por un camino que se saltó la base. Copiarla reventaría a mitad de la
    reconstrucción; descartarla borraría el registro de por qué no se entregó."""
    # Se fabrica con el CHECK ensanchado a mano —única forma de meter la fila— y
    # después se restaura el DDL heredado **exacto**: la fila queda dentro de una tabla
    # cuyo CHECK ya no la admitiría. Es el estado que deja un INSERT por un camino que
    # se saltó la base.
    conn = connect_sqlite(db)
    conn.executescript(
        _schema_script(dead_reasons=_LEGACY_DEAD_REASONS + ("fantasma",))
    )
    _seed_ledger(conn)
    conn.execute(
        "UPDATE radar_notification_batches SET dead_reason='fantasma' "
        "WHERE notification_id='n1'"
    )
    conn.commit()
    conn.execute("PRAGMA writable_schema=ON")
    conn.execute(
        "UPDATE sqlite_master SET sql=? WHERE type='table' AND name=?",
        (_batches_ddl(_BATCHES_TABLE, _LEGACY_DEAD_REASONS)
         .strip().replace("IF NOT EXISTS ", ""), _BATCHES_TABLE),
    )
    conn.execute("PRAGMA writable_schema=OFF")
    conn.commit()
    conn.close()

    conn = connect_sqlite(db)
    with pytest.raises(SchemaIncompatibleError, match="fuera del vocabulario"):
        ensure_schema(conn)


def test_53k_failure_during_migration_rolls_back_completely(db):
    """Si algo revienta a mitad, la base queda exactamente como estaba: con su
    vocabulario viejo y todas sus filas. Una migración a medias —outbox nuevo,
    intentos viejos— sería un estado que ningún preflight posterior sabría nombrar."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    before = _snapshot(conn)
    conn.close()

    conn = connect_sqlite(db)

    def boom(sql):
        if "RENAME TO" in sql:
            raise sqlite3.OperationalError("disco lleno a mitad del rename")

    with pytest.raises(sqlite3.OperationalError):
        ensure_schema(_SpyConn(conn, boom))
    conn.close()

    conn = connect_sqlite(db)
    assert _snapshot(conn) == before, "la migración fallida dejó datos alterados"
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_notification_batches'"
    ).fetchone()["sql"]
    assert "retries_exhausted" not in sql, "quedó a medias: el DDL cambió pese al fallo"
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_53l_the_python_vocabulary_matches_the_check(db):
    """``ledger_store`` valida contra ``DEAD_REASONS`` y la base contra su CHECK. Si
    divergieran, el caller recibiría un IntegrityError de SQLite en vez de una
    explicación —o peor, el ledger aceptaría algo que la base rechaza."""
    from app.core.notify.ledger_store import _VALID_DEAD_REASONS

    assert _VALID_DEAD_REASONS == frozenset(DEAD_REASONS)
    conn = connect_sqlite(db)
    ensure_schema(conn)
    sql = _normalized_ddl(conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_notification_batches'"
    ).fetchone()["sql"])
    for reason in DEAD_REASONS:
        assert f"'{reason}'" in sql


def test_53m_a_copy_that_loses_rows_is_rejected(db):
    """La guardia de recuento no es decorativa: una copia que pierde filas en silencio
    es exactamente el fallo que una migración no puede permitirse, y es el único que
    ningún ``foreign_key_check`` detectaría —una tabla con menos filas es
    perfectamente consistente—. Se fuerza tragándose el INSERT de copia."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    before = _snapshot(conn)
    conn.close()

    real = connect_sqlite(db)

    class _LosingConn(_SpyConn):
        def execute(self, sql, *args):
            if "INSERT INTO radar_notification_batches_rebuild" in str(sql):
                return self._conn.execute("SELECT 1")  # la copia se pierde
            return self._conn.execute(sql, *args)

    with pytest.raises(SchemaIncompatibleError, match="No se acepta una migración"):
        ensure_schema(_LosingConn(real, lambda _sql: None))
    real.close()

    conn = connect_sqlite(db)
    assert _snapshot(conn) == before, "la migración abortada perdió datos"
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_notification_batches'"
    ).fetchone()["sql"]
    assert "retries_exhausted" not in sql, "quedó a medias pese al fallo"


def test_53n_migration_refuses_a_database_with_broken_foreign_keys(db):
    """El rebuild apaga ``foreign_keys`` para poder soltar la tabla referenciada; el
    ``foreign_key_check`` de dentro es lo que devuelve esa garantía, y va **antes** del
    COMMIT para que una violación revierta en vez de quedar registrada. Sin él, migrar
    una base ya inconsistente la dejaría inconsistente y encima migrada."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute(
        "INSERT INTO radar_delivery_attempts (attempt_id, notification_id, "
        "attempt_number, phase, started_at, outcome) VALUES "
        "('huerfano','batch-que-no-existe',1,'finished','2026-01-01T00:00:00+00:00',"
        "'sent')"
    )
    conn.commit()
    before = _snapshot(conn)
    conn.close()

    conn = connect_sqlite(db)
    with pytest.raises(SchemaIncompatibleError, match="clave foránea"):
        ensure_schema(conn)
    conn.close()

    conn = connect_sqlite(db)
    assert _snapshot(conn) == before, "la migración rechazada alteró datos"
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_notification_batches'"
    ).fetchone()["sql"]
    assert "retries_exhausted" not in sql, "migró pese a la base inconsistente"


# ══ ADR-053-corrección · Detección por DDL completo, no por fragmento ═══════════
#
# El vocabulario (dead_reason/error_class) es necesario pero no suficiente: una
# tabla puede conservarlo intacto y aun así haber perdido un UNIQUE, un
# ON DELETE RESTRICT o el CHECK compuesto de phase/outcome. Detectar solo el
# fragmento del vocabulario clasificaría esa tabla como 'current' y
# ``CREATE TABLE IF NOT EXISTS`` no repararía nada: la base quedaría
# estructuralmente rota y el sistema la aceptaría como válida. Estos tests fabrican
# exactamente esa tabla —vocabulario correcto, otra invariante perdida— y exigen
# que el preflight pare, no que la deje pasar.

def test_53o_batches_current_vocabulary_without_the_pair_unique_fails_closed(db):
    """a · El vocabulario de dead_reason es el vigente; falta
    UNIQUE(notification_id, endpoint_id). Sin esa UNIQUE, el mismo batch podría
    asociarse a un endpoint dos veces sin que la base lo impida — exactamente el
    doble notificador que esa restricción existe para cerrar."""
    ddl = _batches_ddl(_BATCHES_TABLE, DEAD_REASONS).replace(
        ",\n    UNIQUE (notification_id, endpoint_id)", ""
    )
    assert "UNIQUE (notification_id, endpoint_id)" not in ddl  # la mutilación surtió efecto
    conn = connect_sqlite(db)
    conn.executescript(ddl)

    with pytest.raises(SchemaIncompatibleError, match="no coincide, completo"):
        ensure_schema(conn)


def test_53p_attempts_legacy_vocabulary_without_the_compound_check_fails_closed(db):
    """b · El vocabulario de error_class es el heredado (el que sí sabemos migrar);
    falta el CHECK compuesto que impide un intento 'finished' sin outcome o uno
    'dispatching' que ya afirme uno. Perder ese CHECK es perder la garantía central
    del ledger, y ningún vocabulario lo delata."""
    batches = _batches_ddl(_BATCHES_TABLE, DEAD_REASONS)
    attempts = _attempts_ddl(_ATTEMPTS_TABLE, _LEGACY_ERROR_CLASSES).replace(
        """    UNIQUE (notification_id, attempt_number),
    CHECK (
        (phase = 'dispatching' AND outcome IS NULL)
        OR (phase = 'finished' AND outcome IS NOT NULL)
    )
);""",
        "    UNIQUE (notification_id, attempt_number)\n);",
    )
    assert "CHECK (\n        (phase" not in attempts  # la mutilación surtió efecto
    conn = connect_sqlite(db)
    conn.executescript(batches + attempts)

    with pytest.raises(SchemaIncompatibleError, match="no coincide, completo"):
        ensure_schema(conn)


def test_53q_attempts_current_vocabulary_with_altered_fk_fails_closed(db):
    """c · El vocabulario de error_class es el vigente; la FK hacia el batch perdió
    ``ON DELETE RESTRICT``. Sin ese RESTRICT, borrar un batch se llevaría en
    silencio la evidencia de que se intentó entregarlo."""
    batches = _batches_ddl(_BATCHES_TABLE, DEAD_REASONS)
    attempts = _attempts_ddl(_ATTEMPTS_TABLE, ERROR_CLASSES).replace(
        "REFERENCES radar_notification_batches(notification_id)\n"
        "                        ON DELETE RESTRICT,",
        "REFERENCES radar_notification_batches(notification_id),",
    )
    assert "ON DELETE RESTRICT" not in attempts  # la mutilación surtió efecto
    conn = connect_sqlite(db)
    conn.executescript(batches + attempts)

    with pytest.raises(SchemaIncompatibleError, match="no coincide, completo"):
        ensure_schema(conn)


def test_53r_exact_current_and_legacy_databases_still_open_and_migrate(db):
    """d · Las dos formas que el preflight sí reconoce siguen funcionando: una base
    vigente exacta no se toca, una heredada exacta migra. La detección más estricta
    no puede convertirse en una que rechace lo que antes aceptaba."""
    conn = connect_sqlite(db)
    ensure_schema(conn)
    ensure_schema(conn)  # exacta y vigente: idempotente, no revienta
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_notification_batches'"
    ).fetchone()["sql"]
    assert "retries_exhausted" in sql
    conn.close()

    legacy_db = db.with_name("legacy.db")
    conn = _legacy_ledger_db(legacy_db)
    _seed_ledger(conn)
    before = _snapshot(conn)
    ensure_schema(conn)  # exacta y heredada: migra sin perder filas
    assert _snapshot(conn) == before
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_notification_batches'"
    ).fetchone()["sql"]
    assert "retries_exhausted" in sql


def test_53s_canonical_forms_ignore_only_the_two_known_sqlite_differences(db):
    """La forma canónica generada tiene que coincidir con lo que SQLite persiste de
    verdad para una tabla recién creada por este mismo código — si no coincidiera,
    ``ensure_schema`` fallaría siempre consigo mismo, incluso sin ninguna base
    heredada de por medio."""
    conn = connect_sqlite(db)
    ensure_schema(conn)
    real_batches = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_notification_batches'"
    ).fetchone()["sql"]
    real_attempts = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_delivery_attempts'"
    ).fetchone()["sql"]

    assert _canonical_form(real_batches) == _canonical_form(
        _batches_ddl(_BATCHES_TABLE, DEAD_REASONS)
    )
    assert _canonical_form(real_attempts) == _canonical_form(
        _attempts_ddl(_ATTEMPTS_TABLE, ERROR_CLASSES)
    )


# ══ ADR-053-corrección 2 · Índices dentro de la transacción de la migración ═════
#
# ``DROP TABLE`` se lleva los índices de la tabla que reemplaza. Si su recreación
# se dejara al ``executescript`` de ``ensure_schema`` —que solo corre cuando la
# transacción ya commiteó—, existiría un instante con las tablas nuevas
# confirmadas y ``idx_attempts_one_open`` ausente. Un crash o un segundo proceso
# ahí encontrarían una base sin la única restricción que impide físicamente dos
# intentos ``dispatching`` sobre el mismo batch. No es rendimiento: es la
# integridad que ADR-053 afirma preservar.


class _CommitProbe(_SpyConn):
    """Observa el estado de la base en el instante exacto anterior al COMMIT.

    Es la única forma de probar la atomicidad desde fuera: después del COMMIT los
    índices existen igual —los crearía el ``executescript``—, así que un test que
    mire al final no distingue una migración atómica de una que no lo es."""

    def __init__(self, conn, probe):
        super().__init__(conn, lambda _sql: None)
        self._probe = probe

    def execute(self, sql, *args):
        if str(sql).strip() == "COMMIT":
            self._probe(self._conn)
        return self._conn.execute(sql, *args)


def test_53t_the_six_ledger_indexes_exist_before_the_commit(db):
    """a · En el instante inmediatamente anterior al COMMIT, los seis existen."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    seen = {}

    def probe(real):
        seen["indexes"] = _index_names(real)

    ensure_schema(_CommitProbe(conn, probe))

    faltan = set(_LEDGER_INDEXES) - seen["indexes"]
    assert not faltan, f"índices ausentes en el COMMIT de la migración: {sorted(faltan)}"


def test_53u_one_open_index_already_bites_before_the_commit(db):
    """b · ``idx_attempts_one_open`` ya impide un segundo ``dispatching`` antes del
    COMMIT. Que el índice *exista* no basta: lo que importa es que muerda."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)  # deja 'a2' dispatching sobre 'n2'
    verdict = {}

    def probe(real):
        try:
            real.execute(
                "INSERT INTO radar_delivery_attempts (attempt_id, notification_id, "
                "attempt_number, phase, started_at) VALUES "
                "('segundo','n2',2,'dispatching','2026-01-01T00:00:00+00:00')"
            )
        except sqlite3.IntegrityError:
            verdict["rechazado"] = True
        else:
            verdict["rechazado"] = False

    ensure_schema(_CommitProbe(conn, probe))

    assert verdict["rechazado"], (
        "antes del COMMIT se pudo abrir un segundo intento 'dispatching' sobre el "
        "mismo batch: el índice único parcial no estaba activo"
    )


def test_53v_failure_while_recreating_indexes_rolls_back_to_legacy(db):
    """c · Un fallo recreando índices revierte tablas, filas e índices al estado
    heredado completo. La migración es atómica o no lo es."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    before_rows = _snapshot(conn)
    before_indexes = _index_names(conn)
    conn.close()

    real = connect_sqlite(db)

    def boom(sql):
        if "idx_attempts_one_open" in sql:
            raise sqlite3.OperationalError("disco lleno al crear el índice")

    with pytest.raises(sqlite3.OperationalError):
        ensure_schema(_SpyConn(real, boom))
    real.close()

    conn = connect_sqlite(db)
    assert _snapshot(conn) == before_rows, "el fallo alteró datos"
    assert _index_names(conn) == before_indexes, "el fallo dejó los índices a medias"
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='radar_notification_batches'"
    ).fetchone()["sql"]
    assert "retries_exhausted" not in sql, "quedó migrada a medias pese al fallo"


def test_53w_reopening_after_the_migration_is_still_idempotent(db):
    """d · Segunda apertura: ni reconstruye ni duplica índices."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    ensure_schema(conn)
    after_first = _snapshot(conn)
    indexes_first = _index_names(conn)

    rebuilt: list[str] = []

    def watch(sql):
        if "RENAME TO" in sql:
            rebuilt.append(sql)

    ensure_schema(_SpyConn(conn, watch))

    assert rebuilt == [], "la segunda apertura reconstruyó una tabla ya vigente"
    assert _snapshot(conn) == after_first
    assert _index_names(conn) == indexes_first
    assert set(_LEDGER_INDEXES).issubset(indexes_first)


def test_53x_index_ddl_has_a_single_definition():
    """Los índices que la migración recrea son **los mismos** que crea el DDL de
    apertura, no una copia parecida: ``_SCHEMA`` los interpola desde las mismas
    funciones. Dos literales del mismo índice divergen, y el que divergiera sería
    justo el que no protege nada."""
    generated = _joined_indexes(
        _batches_indexes_ddl(_BATCHES_TABLE)
    ) + _joined_indexes(_attempts_indexes_ddl(_ATTEMPTS_TABLE))
    for name in _LEDGER_INDEXES:
        assert name in generated, f"{name} no sale de los generadores"
        # El nombre no puede estar además escrito a mano dentro de _SCHEMA: si lo
        # estuviera, habría dos definiciones y esta prueba dejaría de significar nada.
        assert name not in _SCHEMA, f"{name} está duplicado como literal en _SCHEMA"
    assert "{batches_indexes}" in _SCHEMA and "{attempts_indexes}" in _SCHEMA


# ══ ADR-053-corrección 3 · Preflight de los índices de corrección ═══════════════
#
# El preflight de tablas compara el ``CREATE TABLE``, y un índice no vive ahí: una
# base puede conservar el DDL de tabla vigente y tener ``idx_attempts_one_open``
# degradado a un índice no-único. ``CREATE UNIQUE INDEX IF NOT EXISTS`` no lo
# repara —el nombre ya existe—, así que la base quedaría aparentemente sana y
# aceptando dos intentos ``dispatching`` abiertos sobre el mismo batch, es decir,
# dos llamadas posibles al proveedor por el mismo mensaje.


def _current_db(db):
    conn = connect_sqlite(db)
    ensure_schema(conn)
    conn.commit()
    return conn


def _replace_index(conn, name, ddl):
    """Sustituye un índice por otra definición con el mismo nombre. Es lo que deja
    una edición manual, una versión anterior del schema o un script de mantenimiento
    bienintencionado."""
    conn.execute(f"DROP INDEX {name}")
    conn.execute(ddl)
    conn.commit()


@pytest.mark.parametrize("name, ddl, porque", [
    (
        "idx_attempts_one_open",
        "CREATE INDEX idx_attempts_one_open "
        "ON radar_delivery_attempts(notification_id)",
        "a · sin UNIQUE: dos intentos abiertos dejan de ser imposibles",
    ),
    (
        "idx_attempts_one_open",
        "CREATE UNIQUE INDEX idx_attempts_one_open "
        "ON radar_delivery_attempts(notification_id)",
        "b · UNIQUE pero sin WHERE parcial: ahora prohíbe también los 'finished', "
        "que es justo el historial que hay que conservar",
    ),
    (
        "idx_batches_external_idem",
        "CREATE INDEX idx_batches_external_idem "
        "ON radar_notification_batches(external_idempotency_key) "
        "WHERE external_idempotency_key IS NOT NULL",
        "c · sin UNIQUE: dos batches podrían llevar la misma clave al proveedor",
    ),
    (
        "idx_batches_external_idem",
        "CREATE UNIQUE INDEX idx_batches_external_idem "
        "ON radar_notification_batches(batch_key) "
        "WHERE external_idempotency_key IS NOT NULL",
        "d · columna distinta: el índice ya no habla de la clave que protege",
    ),
    (
        "idx_batches_external_idem",
        "CREATE UNIQUE INDEX idx_batches_external_idem "
        "ON radar_notification_batches(external_idempotency_key)",
        "d · sin WHERE parcial: carga con todas las filas aún no autorizadas",
    ),
])
def test_53y_critical_index_with_wrong_definition_fails_closed(db, name, ddl, porque):
    conn = _current_db(db)
    _replace_index(conn, name, ddl)

    with pytest.raises(SchemaIncompatibleError, match="no es la vigente") as exc:
        ensure_schema(conn)
    assert name in str(exc.value), porque


def test_53z_missing_critical_index_is_simply_created(db):
    """e · Ausente se permite: el ``executescript`` lo crea bien. Lo que no se
    permite es el nombre ocupado por otra cosa."""
    conn = _current_db(db)
    conn.execute("DROP INDEX idx_attempts_one_open")
    conn.commit()
    assert "idx_attempts_one_open" not in _index_names(conn)

    ensure_schema(conn)

    recreated = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='idx_attempts_one_open'"
    ).fetchone()["sql"]
    assert "UNIQUE" in recreated.upper()
    assert "dispatching" in recreated


def test_53aa_exact_canonical_indexes_open_idempotently(db):
    """f · Las definiciones que este código escribe se reconocen a sí mismas. Si no,
    ``ensure_schema`` fallaría siempre consigo mismo desde la segunda apertura."""
    conn = _current_db(db)
    ensure_schema(conn)
    ensure_schema(conn)

    assert set(_LEDGER_INDEXES).issubset(_index_names(conn))


def test_53ab_the_degraded_index_check_does_not_touch_anything(db):
    """g · El preflight solo mira. No repara el índice —recrearlo sobre datos que ya
    violan la restricción reventaría sin decir sobre qué filas—, no toca tablas y no
    toca filas."""
    conn = _legacy_ledger_db(db)
    _seed_ledger(conn)
    ensure_schema(conn)          # migra: deja el ledger vigente y los índices buenos
    rows_before = _snapshot(conn)
    _replace_index(
        conn, "idx_attempts_one_open",
        "CREATE INDEX idx_attempts_one_open ON radar_delivery_attempts(notification_id)",
    )
    indexes_before = _index_names(conn)
    tables_before = {
        name: _table_sql_of(conn, name)
        for name in ("radar_notification_batches", "radar_delivery_attempts")
    }

    with pytest.raises(SchemaIncompatibleError, match="no es la vigente"):
        ensure_schema(conn)

    assert _snapshot(conn) == rows_before, "el preflight tocó filas"
    assert _index_names(conn) == indexes_before, "el preflight tocó índices"
    assert {
        name: _table_sql_of(conn, name)
        for name in ("radar_notification_batches", "radar_delivery_attempts")
    } == tables_before, "el preflight tocó tablas"
    # Y el índice degradado sigue exactamente como estaba: no se reparó a escondidas.
    degraded = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='idx_attempts_one_open'"
    ).fetchone()["sql"]
    assert "UNIQUE" not in degraded.upper()


def test_53ac_critical_indexes_come_from_the_generators():
    """Las formas canónicas contra las que se compara son las mismas que crean los
    índices, no una copia. Una segunda transcripción divergiría y el preflight
    empezaría a rechazar bases sanas — o a aceptar las degradadas."""
    from app.core.notify.schema import _CRITICAL_INDEXES

    assert set(_CRITICAL_INDEXES) == {
        "idx_attempts_one_open", "idx_batches_external_idem"
    }
    assert _CRITICAL_INDEXES["idx_attempts_one_open"] in _attempts_indexes_ddl(
        _ATTEMPTS_TABLE
    )
    assert _CRITICAL_INDEXES["idx_batches_external_idem"] in _batches_indexes_ddl(
        _BATCHES_TABLE
    )
