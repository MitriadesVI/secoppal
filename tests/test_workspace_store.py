"""WorkspaceStore — lo que el usuario decidió conservar (R2).

Estos tests fijan cuatro cosas que no se pueden romper sin que el escritorio
deje de ser confiable:

1. El schema desplegado es la autoridad (10 ``item_type`` en español, ninguna
   tabla paralela).
2. Guardar es idempotente por identidad natural, y el re-guardado NO destruye
   lo que el usuario pudo haber añadido.
3. Quitar archiva, jamás borra.
4. Los escritorios de dos personas son impermeables entre sí.
"""
from __future__ import annotations

import json
import sqlite3
import threading

import pytest

from app.core.db import connect_sqlite
from app.core.feedback import ensure_feedback_schema
from app.core.workspace_store import (
    WORKSPACE_ITEM_TYPES,
    WorkspaceIntegrityError,
    WorkspaceStore,
)


USER_A = "streamlit:email:a@example.com"
USER_B = "streamlit:email:b@example.com"
REFERENCE = "p6dx-8zbt/CO1.BDOS.123456"


@pytest.fixture()
def db_path(tmp_path):
    """Base aislada con los dos usuarios que las FK exigen."""
    path = tmp_path / "secoppal.db"
    conn = connect_sqlite(path)
    try:
        ensure_feedback_schema(conn)
        for user_id, email in ((USER_A, "a@example.com"), (USER_B, "b@example.com")):
            conn.execute(
                """
                INSERT INTO users (
                    user_id, channel, channel_user_key, email,
                    is_anonymous, created_at, updated_at
                )
                VALUES (?, 'streamlit', ?, ?, 0, ?, ?)
                """,
                (user_id, f"email:{email}", email,
                 "2026-08-29T00:00:00+00:00", "2026-08-29T00:00:00+00:00"),
            )
    finally:
        conn.close()
    return path


@pytest.fixture()
def store(db_path):
    return WorkspaceStore(db_path)


def _rows(db_path, sql, params=()):
    conn = connect_sqlite(db_path)
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def _save(store, **overrides):
    payload = {
        "user_id": USER_A,
        "item_type": "proceso",
        "reference_id": REFERENCE,
        "title": "Mantenimiento vial",
        "metadata": {"source": "radar"},
    }
    payload.update(overrides)
    user_id = payload.pop("user_id")
    item_type = payload.pop("item_type")
    reference_id = payload.pop("reference_id")
    return store.save_item(user_id, item_type, reference_id, **payload)


# ── Schema ──────────────────────────────────────────────────────────────────


def test_fresh_db_gets_workspace_items_with_the_deployed_item_types(tmp_path):
    """La store inicializa la tabla real, no una versión propia."""
    path = tmp_path / "nueva.db"
    WorkspaceStore(path)

    tablas = {r["name"] for r in _rows(
        path, "SELECT name FROM sqlite_master WHERE type = 'table'"
    )}
    assert "workspace_items" in tablas
    # Ninguna tabla paralela: la autoridad persistente ya existía.
    assert not {t for t in tablas if "saved" in t or "favorit" in t or "bookmark" in t}

    ddl = _rows(
        path,
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'workspace_items'",
    )[0]["sql"]
    for item_type in WORKSPACE_ITEM_TYPES:
        assert f"'{item_type}'" in ddl
    assert len(WORKSPACE_ITEM_TYPES) == 10


def test_store_does_not_add_a_unique_constraint_to_the_deployed_table(tmp_path):
    """R2 no cambia el schema: la unicidad la sostiene la transacción."""
    path = tmp_path / "nueva.db"
    WorkspaceStore(path)
    indices = _rows(
        path,
        "SELECT name, sql FROM sqlite_master "
        "WHERE type = 'index' AND tbl_name = 'workspace_items'",
    )
    assert {i["name"] for i in indices} == {
        "idx_workspace_user",
        "idx_workspace_user_deadline",
        "idx_workspace_user_type",
        "idx_workspace_user_pinned",
        "idx_workspace_reference",
    }
    assert not any("UNIQUE" in (i["sql"] or "") for i in indices)


# ── Guardar ─────────────────────────────────────────────────────────────────


def test_first_save_creates_exactly_one_row(store, db_path):
    resultado = _save(store)

    assert resultado.created and not resultado.refreshed and not resultado.restored
    filas = _rows(db_path, "SELECT * FROM workspace_items")
    assert len(filas) == 1
    assert filas[0]["item_id"] == resultado.item_id
    assert filas[0]["item_type"] == "proceso"
    assert filas[0]["reference_id"] == REFERENCE
    assert filas[0]["archived"] == 0
    assert json.loads(filas[0]["metadata_json"]) == {"source": "radar"}


def test_saving_twice_keeps_one_row_with_the_same_id(store, db_path):
    primero = _save(store)
    segundo = _save(store, title="Mantenimiento vial (actualizado)")

    assert segundo.item_id == primero.item_id
    assert segundo.refreshed and not segundo.created
    assert len(_rows(db_path, "SELECT item_id FROM workspace_items")) == 1


def test_resave_refreshes_the_description_and_preserves_user_data(store, db_path):
    """Pulsar Guardar otra vez no puede ser una forma de perder una nota."""
    primero = _save(store)
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            """
            UPDATE workspace_items
            SET note = ?, deadline = ?, is_pinned = 1, status = ?,
                save_reason = ?, source_trace_id = ?
            WHERE item_id = ?
            """,
            ("llamar al ingeniero", "2026-09-30", "en_revision",
             "competencia", "trace-1", primero.item_id),
        )
    finally:
        conn.close()
    antes = _rows(db_path, "SELECT * FROM workspace_items")[0]

    _save(store, title="Título nuevo", metadata={"source": "radar", "entity": "IDU"})
    despues = _rows(db_path, "SELECT * FROM workspace_items")[0]

    assert despues["title"] == "Título nuevo"
    assert json.loads(despues["metadata_json"]) == {"source": "radar", "entity": "IDU"}
    assert despues["updated_at"] >= antes["updated_at"]
    for campo in ("note", "deadline", "is_pinned", "status",
                  "save_reason", "source_trace_id", "created_at"):
        assert despues[campo] == antes[campo], campo


# ── Archivar y restaurar ────────────────────────────────────────────────────


def test_archive_hides_from_the_default_list_without_deleting(store, db_path):
    guardado = _save(store)

    assert store.archive_item(USER_A, guardado.item_id)
    assert store.list_items(USER_A) == []
    # La fila sigue ahí: quitar del escritorio no es borrar.
    assert len(_rows(db_path, "SELECT item_id FROM workspace_items")) == 1
    assert [i.item_id for i in store.list_items(USER_A, include_archived=True)] == [
        guardado.item_id
    ]

    assert store.restore_item(USER_A, guardado.item_id)
    assert [i.item_id for i in store.list_items(USER_A)] == [guardado.item_id]


def test_saving_an_archived_item_restores_the_same_id(store, db_path):
    guardado = _save(store)
    store.archive_item(USER_A, guardado.item_id)

    resultado = _save(store)

    assert resultado.item_id == guardado.item_id
    assert resultado.restored and resultado.refreshed and not resultado.created
    filas = _rows(db_path, "SELECT item_id, archived FROM workspace_items")
    assert filas == [{"item_id": guardado.item_id, "archived": 0}]


# ── Aislamiento por usuario ─────────────────────────────────────────────────


def test_one_user_cannot_read_archive_or_modify_another_users_items(store, db_path):
    de_a = _save(store)

    assert store.get_item(USER_B, de_a.item_id) is None
    assert store.get_by_reference(USER_B, "proceso", REFERENCE) is None
    assert store.list_items(USER_B) == []
    assert store.archive_item(USER_B, de_a.item_id) is False
    assert store.restore_item(USER_B, de_a.item_id) is False

    # Guardar la MISMA referencia como B crea su propia fila, no toca la de A.
    de_b = _save(store, user_id=USER_B)
    assert de_b.item_id != de_a.item_id
    assert de_b.created
    assert _rows(db_path, "SELECT archived FROM workspace_items WHERE item_id = ?",
                 (de_a.item_id,)) == [{"archived": 0}]


# ── Validación de entrada ───────────────────────────────────────────────────


@pytest.mark.parametrize("item_type", ["process", "contract", "opportunity", "", "PROCESO"])
def test_english_and_unknown_item_types_are_rejected(store, item_type):
    """El schema desplegado usa español; el plan viejo proponía inglés."""
    with pytest.raises(ValueError):
        _save(store, item_type=item_type)


@pytest.mark.parametrize("item_type", ["proceso", "contrato"])
def test_the_two_radar_item_types_are_accepted(store, item_type):
    referencia = f"dataset/{item_type}"
    assert _save(store, item_type=item_type, reference_id=referencia).created


@pytest.mark.parametrize(
    "overrides",
    [
        {"user_id": "  "},
        {"reference_id": ""},
        {"title": "   "},
        {"metadata": ["no", "es", "dict"]},
    ],
)
def test_incomplete_saves_are_rejected_before_touching_sqlite(store, db_path, overrides):
    with pytest.raises(ValueError):
        _save(store, **overrides)
    assert _rows(db_path, "SELECT item_id FROM workspace_items") == []


# ── Corrupción preexistente ─────────────────────────────────────────────────


def _inject_duplicate(db_path, item_id: str) -> None:
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            """
            INSERT INTO workspace_items (
                item_id, user_id, item_type, reference_id, title,
                created_at, updated_at
            )
            VALUES (?, ?, 'proceso', ?, 'Duplicado', ?, ?)
            """,
            (item_id, USER_A, REFERENCE,
             "2026-08-29T00:00:00+00:00", "2026-08-29T00:00:00+00:00"),
        )
    finally:
        conn.close()


def test_duplicate_rows_fail_closed_instead_of_picking_one(store, db_path):
    """Con dos filas para la misma tripleta, la store no adivina ni borra."""
    _inject_duplicate(db_path, "dup-1")
    _inject_duplicate(db_path, "dup-2")

    with pytest.raises(WorkspaceIntegrityError):
        _save(store)
    with pytest.raises(WorkspaceIntegrityError):
        store.get_by_reference(USER_A, "proceso", REFERENCE)

    # Ninguna fila fue eliminada por el intento fallido.
    assert {r["item_id"] for r in _rows(db_path, "SELECT item_id FROM workspace_items")} == {
        "dup-1", "dup-2"
    }


# ── Contrato transaccional ──────────────────────────────────────────────────


def test_lookup_happens_inside_the_immediate_transaction(store, db_path):
    """El SELECT de identidad no puede ocurrir antes del ``BEGIN IMMEDIATE``.

    Se instrumenta la conexión y se lee el ORDEN real de las sentencias, porque
    el modo WAL no permite detectarlo por bloqueo: un lector no se bloquea
    contra un escritor, así que un lookup suelto pasaría inadvertido hasta que
    dos guardados simultáneos crearan el duplicado (lo que fija el test
    siguiente). Aquí se fija la causa; allí, la consecuencia.
    """
    ejecutadas: list[str] = []
    real_connect = store._connect

    class ConexionInstrumentada:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *args, **kwargs):
            ejecutadas.append(" ".join(sql.split())[:40])
            return self._conn.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    store._connect = lambda: ConexionInstrumentada(real_connect())
    try:
        _save(store)
    finally:
        store._connect = real_connect

    assert ejecutadas[0].startswith("BEGIN IMMEDIATE")
    selects = [i for i, sql in enumerate(ejecutadas) if sql.startswith("SELECT")]
    escrituras = [i for i, sql in enumerate(ejecutadas)
                  if sql.startswith(("INSERT", "UPDATE"))]
    commit = ejecutadas.index("COMMIT")
    assert selects and escrituras
    # Todo —lookup y escritura— entre el BEGIN y el COMMIT: sin ventana.
    assert max(selects + escrituras) < commit


def test_concurrent_saves_of_the_same_identity_produce_one_row(db_path):
    """Dos guardados simultáneos por la store no crean un duplicado."""
    tiendas = [WorkspaceStore(db_path) for _ in range(4)]
    barrera = threading.Barrier(len(tiendas))
    errores: list[Exception] = []

    def guardar(store):
        try:
            barrera.wait(timeout=10)
            store.save_item(
                USER_A, "proceso", REFERENCE,
                title="Mantenimiento vial", metadata={"source": "radar"},
            )
        except Exception as exc:  # noqa: BLE001 — se reportan al final
            errores.append(exc)

    hilos = [threading.Thread(target=guardar, args=(s,)) for s in tiendas]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join(timeout=30)

    assert errores == []
    assert len(_rows(db_path, "SELECT item_id FROM workspace_items")) == 1
