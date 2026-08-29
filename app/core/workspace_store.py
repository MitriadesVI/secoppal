"""workspace_store.py — lo que el usuario decidió conservar (Workspace V1).

Radar produce HECHOS FECHADOS. El escritorio guarda DECISIONES: "esto me
importa, quiero volver a encontrarlo". Son dos autoridades distintas y esta es
la segunda. La tabla ya existía (``workspace_items``, commit ``40b4283``); este
módulo es el único punto de escritura sobre ella y la autoridad de su DDL.

Tres decisiones que explican la forma del módulo:

1. **El DDL vive aquí, no en ``scripts/``.** Una instalación nueva debe poder
   inicializar la tabla sin que ``app/`` importe código de un script de
   migración. ``ensure_workspace_schema`` reproduce el DDL YA DESPLEGADO, letra
   por letra: solo ``CREATE ... IF NOT EXISTS``, nunca ``DROP`` ni ``ALTER``.
   El migrador delega aquí y conserva aparte su propio comportamiento legacy.

2. **La deduplicación es transaccional, no un constraint.** El schema desplegado
   NO tiene ``UNIQUE(user_id, item_type, reference_id)`` —el plan histórico lo
   proponía, la tabla real no lo llevó— y corregirlo exigiría recrear la tabla,
   que es un cambio de schema y no cabe en este bloque. Mientras tanto la
   unicidad la sostiene ``BEGIN IMMEDIATE``: el lookup y la escritura ocurren
   bajo el mismo lock de escritura de SQLite, así que dos guardados simultáneos
   se serializan y el segundo ve la fila del primero. Un ``SELECT`` fuera de la
   transacción dejaría exactamente la ventana que produce el duplicado.

3. **Guardar dos veces refresca, no acumula.** El re-guardado actualiza lo que
   describe el item (``title``, ``metadata_json``, ``updated_at``) y lo
   desarchiva; jamás toca ``note``, ``deadline``, ``is_pinned``, ``status``,
   ``save_reason``, ``created_at`` ni ``source_trace_id``. V1 todavía no escribe
   esos campos, pero pulsar Guardar no puede ser una forma de perder una nota.

Ante corrupción preexistente —más de una fila para la misma tripleta— la store
falla CERRADO con :class:`WorkspaceIntegrityError`. Elegir una arbitrariamente
escondería el problema y borrar la otra destruiría datos que nadie autorizó
borrar.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.db import connect_sqlite
from app.core.feedback import ensure_feedback_schema

__all__ = [
    "WORKSPACE_ITEM_TYPES",
    "WorkspaceIntegrityError",
    "WorkspaceItem",
    "WorkspaceSaveResult",
    "WorkspaceStore",
    "ensure_workspace_schema",
]


# Los diez tipos que el CHECK de la tabla desplegada acepta. Se declaran aquí
# para poder validar ANTES de tocar SQLite y devolver un error legible en vez de
# un IntegrityError opaco. La lista es un espejo del DDL de abajo: si alguna vez
# cambia, cambian las dos juntas.
WORKSPACE_ITEM_TYPES = (
    "proceso",
    "contrato",
    "contratista",
    "entidad",
    "busqueda",
    "analisis",
    "nota",
    "radar_ref",
    "documento",
    "paa_rubro",
)


WORKSPACE_ITEMS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS workspace_items (
    item_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    item_type TEXT NOT NULL CHECK (
        item_type IN (
            'proceso', 'contrato', 'contratista', 'entidad',
            'busqueda', 'analisis', 'nota', 'radar_ref',
            'documento', 'paa_rubro'
        )
    ),
    reference_id TEXT,
    title TEXT NOT NULL,
    note TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    deadline TEXT,
    source_trace_id TEXT,
    metadata_json TEXT,
    save_reason TEXT,
    is_pinned INTEGER NOT NULL DEFAULT 0 CHECK (is_pinned IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
) WITHOUT ROWID
"""

WORKSPACE_ITEMS_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_workspace_user ON workspace_items(user_id, archived)",
    "CREATE INDEX IF NOT EXISTS idx_workspace_user_deadline ON workspace_items(user_id, deadline)",
    """
    CREATE INDEX IF NOT EXISTS idx_workspace_user_type
      ON workspace_items(user_id, item_type, archived)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_workspace_user_pinned
      ON workspace_items(user_id, is_pinned) WHERE is_pinned = 1
    """,
    "CREATE INDEX IF NOT EXISTS idx_workspace_reference ON workspace_items(reference_id)",
)


class WorkspaceIntegrityError(Exception):
    """La tabla contiene más de un item para la misma tripleta de identidad.

    Es un estado que la store no puede resolver sola: no sabe cuál de las filas
    es la que el usuario cree tener guardada. Se levanta hacia arriba en vez de
    adivinar o de borrar.
    """


def ensure_workspace_schema(conn: sqlite3.Connection) -> None:
    """Crea ``workspace_items`` y sus índices si faltan. Nunca destruye.

    Idempotente y seguro sobre una base ya poblada: todo es ``IF NOT EXISTS``.
    No registra migraciones —eso es responsabilidad del migrador, que sí lleva
    la bitácora de ``schema_migrations``.
    """
    conn.execute(WORKSPACE_ITEMS_TABLE_SQL)
    for statement in WORKSPACE_ITEMS_INDEX_SQL:
        conn.execute(statement)


@dataclass(frozen=True, slots=True)
class WorkspaceSaveResult:
    """Qué pasó al guardar, para que la UI pueda decir la verdad.

    Los tres casos producen mensajes distintos y no son intercambiables:
    guardar por primera vez, refrescar algo que ya estaba, y devolver al
    escritorio algo que el usuario había quitado.
    """

    item_id: str
    created: bool = False
    refreshed: bool = False
    restored: bool = False


@dataclass(frozen=True, slots=True)
class WorkspaceItem:
    """Un item guardado, tal como está en la tabla.

    ``metadata`` es el ``metadata_json`` ya decodificado; un JSON ilegible
    colapsa a ``{}`` en vez de reventar la lectura del escritorio entero.
    """

    item_id: str
    user_id: str
    item_type: str
    reference_id: str | None
    title: str
    note: str | None
    status: str
    deadline: str | None
    source_trace_id: str | None
    metadata: dict[str, Any]
    save_reason: str | None
    is_pinned: bool
    created_at: str
    updated_at: str
    archived: bool


class WorkspaceStore:
    """CRUD mínimo sobre ``workspace_items``. Un punto único de escritura."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._setup()

    # ── Escrituras ────────────────────────────────────────────────────────
    def save_item(
        self,
        user_id: str,
        item_type: str,
        reference_id: str,
        *,
        title: str,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> WorkspaceSaveResult:
        """Guarda, refresca o restaura el item de esa identidad. Idempotente.

        Todo ocurre dentro de ``BEGIN IMMEDIATE``: ver la nota 2 del módulo.
        """
        user_id = _required_text(user_id, "user_id")
        reference_id = _required_text(reference_id, "reference_id")
        title = _required_text(title, "title")
        item_type = _validated_item_type(item_type)
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict):
            raise ValueError("metadata must be a dict")
        stamp = _iso(now)
        metadata_json = json.dumps(metadata, ensure_ascii=False, default=str)

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = conn.execute(
                    """
                    SELECT item_id, archived FROM workspace_items
                    WHERE user_id = ? AND item_type = ? AND reference_id = ?
                    ORDER BY created_at ASC, item_id ASC
                    """,
                    (user_id, item_type, reference_id),
                ).fetchall()
                if len(existing) > 1:
                    conn.execute("ROLLBACK")
                    raise WorkspaceIntegrityError(
                        f"{len(existing)} workspace_items comparten identidad "
                        f"({user_id!r}, {item_type!r}, {reference_id!r})"
                    )

                if not existing:
                    item_id = uuid.uuid4().hex
                    conn.execute(
                        """
                        INSERT INTO workspace_items (
                            item_id, user_id, item_type, reference_id, title,
                            metadata_json, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            item_id, user_id, item_type, reference_id, title,
                            metadata_json, stamp, stamp,
                        ),
                    )
                    result = WorkspaceSaveResult(item_id=item_id, created=True)
                else:
                    row = existing[0]
                    item_id = str(row["item_id"])
                    was_archived = bool(row["archived"])
                    # El SET enumera exactamente lo que describe al item. Todo
                    # lo que el usuario pudo haber añadido —nota, deadline, pin,
                    # status, save_reason— queda fuera a propósito.
                    conn.execute(
                        """
                        UPDATE workspace_items
                        SET title = ?, metadata_json = ?, updated_at = ?, archived = 0
                        WHERE item_id = ? AND user_id = ?
                        """,
                        (title, metadata_json, stamp, item_id, user_id),
                    )
                    result = WorkspaceSaveResult(
                        item_id=item_id, refreshed=True, restored=was_archived,
                    )
                conn.execute("COMMIT")
                return result
            except WorkspaceIntegrityError:
                raise
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    def archive_item(self, user_id: str, item_id: str, *, now: datetime | None = None) -> bool:
        """Quita el item del escritorio SIN borrarlo. ``False`` si no era suyo."""
        return self._set_archived(user_id, item_id, archived=True, now=now)

    def restore_item(self, user_id: str, item_id: str, *, now: datetime | None = None) -> bool:
        """Devuelve al escritorio un item archivado. ``False`` si no era suyo."""
        return self._set_archived(user_id, item_id, archived=False, now=now)

    # ── Lecturas ──────────────────────────────────────────────────────────
    def get_item(self, user_id: str, item_id: str) -> WorkspaceItem | None:
        """Un item por id. El ``user_id`` es parte del predicado, no un filtro
        que el caller pueda olvidar: un id adivinado no abre el item de otro."""
        if not user_id or not item_id:
            return None
        row = self._fetchone(
            "SELECT * FROM workspace_items WHERE user_id = ? AND item_id = ?",
            (user_id, item_id),
        )
        return _row_to_item(row) if row is not None else None

    def get_by_reference(
        self, user_id: str, item_type: str, reference_id: str,
    ) -> WorkspaceItem | None:
        """El item guardado para esa identidad natural, archivado o no.

        Devuelve también los archivados: la UI necesita saber que existe para
        poder restaurarlo en vez de crear un duplicado.
        """
        if not user_id or not item_type or not reference_id:
            return None
        rows = self._fetchall(
            """
            SELECT * FROM workspace_items
            WHERE user_id = ? AND item_type = ? AND reference_id = ?
            ORDER BY created_at ASC, item_id ASC
            """,
            (user_id, item_type, reference_id),
        )
        if len(rows) > 1:
            raise WorkspaceIntegrityError(
                f"{len(rows)} workspace_items comparten identidad "
                f"({user_id!r}, {item_type!r}, {reference_id!r})"
            )
        return _row_to_item(rows[0]) if rows else None

    def list_items(
        self,
        user_id: str,
        *,
        item_type: str | None = None,
        include_archived: bool = False,
        limit: int = 100,
    ) -> list[WorkspaceItem]:
        """Los items del usuario, los más recientes primero.

        Por defecto NO incluye archivados: quitar algo del escritorio tiene que
        notarse en el escritorio.
        """
        if not user_id:
            return []
        sql = "SELECT * FROM workspace_items WHERE user_id = ?"
        params: list[Any] = [user_id]
        if not include_archived:
            sql += " AND archived = 0"
        if item_type:
            sql += " AND item_type = ?"
            params.append(_validated_item_type(item_type))
        sql += " ORDER BY updated_at DESC, item_id DESC LIMIT ?"
        params.append(int(limit))
        return [_row_to_item(row) for row in self._fetchall(sql, tuple(params))]

    # ── Interno ───────────────────────────────────────────────────────────
    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def _setup(self) -> None:
        conn = self._connect()
        try:
            # ``users`` es el destino de la FK de la tabla, así que su ausencia
            # no rompe el CREATE (SQLite resuelve las FK al escribir) pero sí
            # rompería el primer INSERT. Ambas se aseguran juntas.
            ensure_feedback_schema(conn)
            ensure_workspace_schema(conn)
        finally:
            conn.close()

    def _set_archived(
        self, user_id: str, item_id: str, *, archived: bool, now: datetime | None,
    ) -> bool:
        if not user_id or not item_id:
            return False
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                UPDATE workspace_items
                SET archived = ?, updated_at = ?
                WHERE user_id = ? AND item_id = ?
                """,
                (1 if archived else 0, _iso(now), user_id, item_id),
            )
            return cursor.rowcount > 0
        finally:
            conn.close()

    def _fetchone(self, sql: str, params: tuple) -> sqlite3.Row | None:
        conn = self._connect()
        try:
            return conn.execute(sql, params).fetchone()
        finally:
            conn.close()

    def _fetchall(self, sql: str, params: tuple) -> list[sqlite3.Row]:
        conn = self._connect()
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()


def _validated_item_type(value: object) -> str:
    item_type = str(value or "").strip()
    if item_type not in WORKSPACE_ITEM_TYPES:
        raise ValueError(
            f"item_type inválido: {value!r} (válidos: {', '.join(WORKSPACE_ITEM_TYPES)})"
        )
    return item_type


def _required_text(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} no puede estar vacío")
    return text


def _iso(now: datetime | None) -> str:
    return (now or datetime.now(timezone.utc)).isoformat()


def _row_to_item(row: sqlite3.Row) -> WorkspaceItem:
    return WorkspaceItem(
        item_id=str(row["item_id"]),
        user_id=str(row["user_id"]),
        item_type=str(row["item_type"]),
        reference_id=row["reference_id"],
        title=str(row["title"]),
        note=row["note"],
        status=str(row["status"]),
        deadline=row["deadline"],
        source_trace_id=row["source_trace_id"],
        metadata=_loads_dict(row["metadata_json"]),
        save_reason=row["save_reason"],
        is_pinned=bool(row["is_pinned"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        archived=bool(row["archived"]),
    )


def _loads_dict(raw: object) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}
