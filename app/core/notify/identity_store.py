"""identity_store.py — quién es la persona y qué claves históricas le pertenecen.

``radars.owner`` no identifica a una persona: es un ``channel_user_key``
(``email:ana@x.com``, ``anon:abc``, un chat_id de Telegram) que depende del canal
por el que llegó. La misma persona acumula varios a lo largo del tiempo —entró
anónima, luego se registró, luego escribió por Telegram— y todos apuntan a
radares suyos.

De ahí las dos tablas: ``principals`` es la persona, ``principal_owner_keys`` son
sus claves. El vínculo es explícito y de una sola dirección: una clave pertenece a
lo sumo a un principal, un principal tiene muchas claves.

``radars.owner`` **no se migra ni se reescribe**. Reescribir owners sería una
migración destructiva de datos vivos, y la indirección de esta tabla resuelve lo
mismo sin tocarlos.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from app.core.auth import _normalize_email
from app.core.db import connect_sqlite
from app.core.notify.schema import (
    PRINCIPAL_ACTIVE,
    PRINCIPAL_STATUSES,
    PRINCIPAL_SUSPENDED,
    ensure_schema,
    new_id,
    now_iso,
)


class IdentityError(Exception):
    """Error de negocio de identidad."""


class PrincipalExistsError(IdentityError):
    """Ya existe un principal con ese email."""


class OwnerKeyConflictError(IdentityError):
    """El owner_key ya pertenece a otro principal."""


@dataclass(frozen=True)
class Principal:
    principal_id: str
    email: str
    status: str
    created_at: str

    @property
    def is_active(self) -> bool:
        return self.status == PRINCIPAL_ACTIVE

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Principal":
        return cls(**{k: row[k] for k in row.keys()})


class IdentityStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        conn = self._connect()
        try:
            ensure_schema(conn)
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def create_principal(self, email: str) -> str:
        """Crea la persona. El email se normaliza con la política de ``auth``."""
        normalized = _normalize_email(email)
        if not normalized:
            raise IdentityError("El email del principal no puede estar vacío.")
        principal_id = new_id()
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO principals (principal_id, email, status, created_at) "
                "VALUES (?, ?, ?, ?)",
                (principal_id, normalized, PRINCIPAL_ACTIVE, now_iso()),
            )
        except sqlite3.IntegrityError as exc:
            raise PrincipalExistsError(
                f"Ya existe un principal con el email '{normalized}'."
            ) from exc
        finally:
            conn.close()
        return principal_id

    def get_principal(self, principal_id: str) -> Principal | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM principals WHERE principal_id = ?", (principal_id,)
            ).fetchone()
        finally:
            conn.close()
        return Principal.from_row(row) if row else None

    def get_principal_by_email(self, email: str) -> Principal | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM principals WHERE email = ?", (_normalize_email(email),)
            ).fetchone()
        finally:
            conn.close()
        return Principal.from_row(row) if row else None

    def set_status(self, principal_id: str, status: str) -> None:
        """Suspender **no** borra: los endpoints y preferencias siguen ahí.

        Suspender es reversible y deja rastro; borrar destruiría la prueba de qué
        consintió la persona. La suspensión se hace valer al filtrar elegibles
        (ver ``endpoint_store.list_eligible_endpoints``).
        """
        if status not in PRINCIPAL_STATUSES:
            raise IdentityError(
                f"Estado inválido '{status}'. Válidos: {', '.join(PRINCIPAL_STATUSES)}."
            )
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE principals SET status = ? WHERE principal_id = ?",
                (status, principal_id),
            )
        finally:
            conn.close()

    def suspend(self, principal_id: str) -> None:
        self.set_status(principal_id, PRINCIPAL_SUSPENDED)

    def reactivate(self, principal_id: str) -> None:
        self.set_status(principal_id, PRINCIPAL_ACTIVE)

    def link_owner_key(self, owner_key: str, principal_id: str) -> None:
        """Vincula un ``radars.owner`` a una persona. Idempotente por par.

        Re-vincular la misma clave al mismo principal es un no-op deliberado
        (el caller no debería tener que consultar antes de vincular). Vincularla a
        *otro* principal falla ruidosamente: un owner_key que cambia de dueño en
        silencio transfiere los radares de una persona a otra, y con ellos el
        derecho a recibir sus notificaciones. Si esa transferencia hace falta
        algún día, será una operación explícita con su propia auditoría.
        """
        conn = self._connect()
        try:
            if self.get_principal(principal_id) is None:
                raise IdentityError(
                    f"No existe el principal '{principal_id}'; no se puede vincular."
                )
            row = conn.execute(
                "SELECT principal_id FROM principal_owner_keys WHERE owner_key = ?",
                (owner_key,),
            ).fetchone()
            if row is not None:
                if row["principal_id"] == principal_id:
                    return
                raise OwnerKeyConflictError(
                    f"El owner_key ya pertenece a otro principal; reasignarlo "
                    f"transferiría sus radares."
                )
            conn.execute(
                "INSERT INTO principal_owner_keys (owner_key, principal_id, linked_at) "
                "VALUES (?, ?, ?)",
                (owner_key, principal_id, now_iso()),
            )
        finally:
            conn.close()

    def resolve_principal_id(self, owner_key: str) -> str | None:
        """``owner_key`` → principal, o ``None`` si no está vinculado.

        No autocrea. Un owner_key desconocido es una pregunta sin respuesta, no
        una invitación a inventar una persona: autocrear aquí fabricaría un
        principal sin email verificado por cada chat anónimo que pase.
        """
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT principal_id FROM principal_owner_keys WHERE owner_key = ?",
                (owner_key,),
            ).fetchone()
        finally:
            conn.close()
        return row["principal_id"] if row else None

    def list_owner_keys(self, principal_id: str) -> list[str]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT owner_key FROM principal_owner_keys WHERE principal_id = ? "
                "ORDER BY linked_at",
                (principal_id,),
            ).fetchall()
        finally:
            conn.close()
        return [r["owner_key"] for r in rows]
