"""
SECOPPAL — Autenticación y gestión de cuentas
==============================================

Capa de credenciales para el login de la UI Streamlit. Vive en la misma
SQLite que el feedback (``data/secoppal.db``) pero en su propia tabla
``app_accounts``, separada de la tabla analítica ``users`` para no tocar
sus claves foráneas. El enlace entre ambas es el email: la cuenta
autenticada alimenta ``_current_user_identity()`` en streamlit_app.py, que
hace upsert en ``users`` por email → cada trace queda atada al usuario real.

Hashing: PBKDF2-HMAC-SHA256 de la stdlib (sin dependencias nuevas), con salt
aleatorio por cuenta y comparación en tiempo constante.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.db import connect_sqlite


logger = logging.getLogger(__name__)

_PBKDF2_ROUNDS = 200_000
_SALT_BYTES = 16

VALID_ROLES = ("admin", "user")


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS app_accounts (
    email         TEXT PRIMARY KEY,
    name          TEXT,
    password_hash TEXT NOT NULL,
    salt          TEXT NOT NULL,
    role          TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('admin','user')),
    is_active     INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS app_sessions (
    token      TEXT PRIMARY KEY,
    email      TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    FOREIGN KEY (email) REFERENCES app_accounts(email) ON DELETE CASCADE
);
"""

DEFAULT_SESSION_TTL_DAYS = 30


class AccountError(Exception):
    """Error de negocio de cuentas (validación, último admin, etc.)."""


class AccountExistsError(AccountError):
    """Ya existe una cuenta con ese email."""


@dataclass(frozen=True)
class Account:
    email: str
    name: str | None
    role: str
    is_active: bool

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def hash_password(password: str, salt: bytes | None = None) -> tuple[str, str]:
    """Devuelve (hash_hex, salt_hex). Genera salt aleatorio si no se pasa."""
    if not password:
        raise ValueError("password must not be empty")
    if salt is None:
        salt = secrets.token_bytes(_SALT_BYTES)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ROUNDS)
    return digest.hex(), salt.hex()


def verify_password(password: str, password_hash: str, salt_hex: str) -> bool:
    """Comparación en tiempo constante. Falso ante salt/hash corruptos."""
    if not password:
        return False
    try:
        salt = bytes.fromhex(salt_hex)
    except ValueError:
        return False
    candidate, _ = hash_password(password, salt)
    return hmac.compare_digest(candidate, password_hash)


class AccountStore:
    """CRUD de credenciales sobre ``app_accounts``."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._setup()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def _setup(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)

    @staticmethod
    def _row_to_account(row: sqlite3.Row) -> Account:
        return Account(
            email=row["email"],
            name=row["name"],
            role=row["role"],
            is_active=bool(row["is_active"]),
        )

    # ── Lecturas ──────────────────────────────────────────────────────────
    def get(self, email: str) -> Account | None:
        email = _normalize_email(email)
        if not email:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT email, name, role, is_active FROM app_accounts WHERE email = ?",
                (email,),
            ).fetchone()
        return self._row_to_account(row) if row else None

    def list_accounts(self) -> list[Account]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT email, name, role, is_active FROM app_accounts ORDER BY email ASC"
            ).fetchall()
        return [self._row_to_account(r) for r in rows]

    def count_active_admins(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM app_accounts WHERE role = 'admin' AND is_active = 1"
            ).fetchone()
        return int(row["n"])

    def _count_admins(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM app_accounts WHERE role = 'admin'"
            ).fetchone()
        return int(row["n"])

    # ── Escrituras ────────────────────────────────────────────────────────
    def create_account(
        self,
        email: str,
        password: str,
        name: str | None = None,
        role: str = "user",
    ) -> Account:
        email = _normalize_email(email)
        if not email or "@" not in email:
            raise AccountError("email inválido")
        if not password:
            raise AccountError("la contraseña no puede estar vacía")
        if role not in VALID_ROLES:
            raise AccountError(f"rol inválido: {role!r}")

        name = (name or "").strip() or None
        password_hash, salt = hash_password(password)
        now = _now()
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO app_accounts (
                        email, name, password_hash, salt, role,
                        is_active, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (email, name, password_hash, salt, role, now, now),
                )
        except sqlite3.IntegrityError as exc:
            raise AccountExistsError(f"ya existe una cuenta para {email}") from exc
        return Account(email=email, name=name, role=role, is_active=True)

    def authenticate(self, email: str, password: str) -> Account | None:
        """Devuelve la cuenta si las credenciales son válidas y está activa."""
        email = _normalize_email(email)
        if not email or not password:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT email, name, role, is_active, password_hash, salt
                FROM app_accounts WHERE email = ?
                """,
                (email,),
            ).fetchone()
        if row is None or not bool(row["is_active"]):
            return None
        if not verify_password(password, row["password_hash"], row["salt"]):
            return None
        return self._row_to_account(row)

    def set_active(self, email: str, active: bool) -> bool:
        email = _normalize_email(email)
        target = self.get(email)
        if target is None:
            return False
        if not active and target.is_admin and self.count_active_admins() <= 1:
            raise AccountError("no se puede desactivar al último admin activo")
        with self._connect() as conn:
            conn.execute(
                "UPDATE app_accounts SET is_active = ?, updated_at = ? WHERE email = ?",
                (1 if active else 0, _now(), email),
            )
        return True

    def set_role(self, email: str, role: str) -> bool:
        if role not in VALID_ROLES:
            raise AccountError(f"rol inválido: {role!r}")
        email = _normalize_email(email)
        target = self.get(email)
        if target is None:
            return False
        if role != "admin" and target.is_admin and self.count_active_admins() <= 1:
            raise AccountError("no se puede degradar al último admin activo")
        with self._connect() as conn:
            conn.execute(
                "UPDATE app_accounts SET role = ?, updated_at = ? WHERE email = ?",
                (role, _now(), email),
            )
        return True

    def reset_password(self, email: str, new_password: str) -> bool:
        if not new_password:
            raise AccountError("la contraseña no puede estar vacía")
        email = _normalize_email(email)
        if self.get(email) is None:
            return False
        password_hash, salt = hash_password(new_password)
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE app_accounts
                SET password_hash = ?, salt = ?, updated_at = ?
                WHERE email = ?
                """,
                (password_hash, salt, _now(), email),
            )
        return True

    def ensure_admin_seed(
        self,
        email: str | None,
        password: str | None,
        name: str | None = None,
    ) -> bool:
        """Crea el admin semilla en el primer arranque. Idempotente.

        No hace nada si: faltan credenciales, ya existe algún admin, o el
        email ya está registrado (no sobreescribe). Devuelve True si creó.
        """
        email = _normalize_email(email)
        if not email or not password:
            return False
        if self._count_admins() > 0:
            return False
        if self.get(email) is not None:
            return False
        self.create_account(email, password, name=name, role="admin")
        logger.info("admin semilla creado para %s", email)
        return True

    # ── Sesiones persistentes ("recordarme" vía cookie) ────────────────────
    def create_session(self, email: str, ttl_days: int = DEFAULT_SESSION_TTL_DAYS) -> str:
        """Crea un token de sesión opaco y lo persiste. Devuelve el token."""
        email = _normalize_email(email)
        if self.get(email) is None:
            raise AccountError("no existe la cuenta")
        token = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        expires = now + timedelta(days=ttl_days)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO app_sessions (token, email, created_at, expires_at)
                VALUES (?, ?, ?, ?)
                """,
                (token, email, now.isoformat(), expires.isoformat()),
            )
        return token

    def resolve_session(self, token: str | None) -> Account | None:
        """Devuelve la cuenta si el token es válido, no expiró y está activa.

        Elimina el token si expiró. No devuelve cuentas desactivadas.
        """
        if not token:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT email, expires_at FROM app_sessions WHERE token = ?",
                (token,),
            ).fetchone()
            if row is None:
                return None
            try:
                expired = datetime.fromisoformat(row["expires_at"]) < datetime.now(timezone.utc)
            except ValueError:
                expired = True
            if expired:
                conn.execute("DELETE FROM app_sessions WHERE token = ?", (token,))
                return None
        account = self.get(row["email"])
        if account is None or not account.is_active:
            return None
        return account

    def revoke_session(self, token: str | None) -> None:
        if not token:
            return
        with self._connect() as conn:
            conn.execute("DELETE FROM app_sessions WHERE token = ?", (token,))
