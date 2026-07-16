"""schema.py — DDL idempotente de las cuatro tablas de notificación.

Centralizado a propósito: los tres stores (identidad, endpoints, preferencias)
comparten base y se referencian entre sí por FK, así que si cada uno creara solo
"su" tabla, el que se inicializara primero dejaría la base en un estado donde sus
propias FK apuntan a tablas inexistentes. Aquí las cuatro se crean juntas y
cualquier store puede ser el primero en abrir la base.

**Dependencia externa que este módulo NO puede resolver:**
``radar_notification_preferences`` tiene una FK a ``radars(id)``, tabla que
pertenece a ``RadarStore``. SQLite acepta crear una FK hacia una tabla que aún no
existe, pero con ``PRAGMA foreign_keys=ON`` el primer INSERT fallaría. Por eso el
caller debe haber inicializado ``RadarStore`` sobre la misma base antes de
escribir preferencias. No se replica el DDL de ``radars`` aquí: dos definiciones
de la misma tabla divergirían.

Las cuatro tablas son separadas y no una sola con columnas opcionales porque
responden preguntas distintas y con ciclos de vida distintos: quién es la persona,
qué claves históricas le pertenecen, a dónde se le puede escribir, y qué aceptó
recibir en cada radar. Aplanarlas obligaría a borrar identidad para revocar un
destino.
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone


# Helpers de persistencia compartidos por los tres stores. Viven aquí —y no en
# cada store— por la misma razón que el DDL: una sola definición del formato de
# id y de marca temporal, o las tablas se llenan con formatos que no comparan.

def now_iso() -> str:
    """ISO-8601 UTC, igual que el resto del store del radar."""
    return datetime.now(timezone.utc).isoformat()


def new_id() -> str:
    return uuid.uuid4().hex


PRINCIPAL_ACTIVE = "active"
PRINCIPAL_SUSPENDED = "suspended"
PRINCIPAL_STATUSES = (PRINCIPAL_ACTIVE, PRINCIPAL_SUSPENDED)

CHANNEL_TELEGRAM = "telegram"
CHANNEL_WHATSAPP = "whatsapp"
CHANNEL_EMAIL = "email"
CHANNELS = (CHANNEL_TELEGRAM, CHANNEL_WHATSAPP, CHANNEL_EMAIL)

FREQUENCY_PER_RUN = "per_run"
FREQUENCY_DAILY = "daily"
FREQUENCY_WEEKLY = "weekly"
FREQUENCIES = (FREQUENCY_PER_RUN, FREQUENCY_DAILY, FREQUENCY_WEEKLY)

DEFAULT_TIMEZONE = "America/Bogota"
DEFAULT_MAX_ITEMS = 10


# ``ON DELETE RESTRICT`` en todas las FK, no CASCADE: borrar una persona no puede
# llevarse en silencio la prueba de a dónde se le escribió ni qué consintió. Si
# algo debe desaparecer, es una decisión explícita, no un efecto colateral.
_SCHEMA = """
CREATE TABLE IF NOT EXISTS principals (
    principal_id TEXT PRIMARY KEY,
    email        TEXT NOT NULL UNIQUE,
    status       TEXT NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active','suspended')),
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS principal_owner_keys (
    owner_key    TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(principal_id) ON DELETE RESTRICT,
    linked_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_owner_keys_principal
    ON principal_owner_keys(principal_id);

CREATE TABLE IF NOT EXISTS notification_endpoints (
    endpoint_id           TEXT PRIMARY KEY,
    principal_id          TEXT NOT NULL REFERENCES principals(principal_id) ON DELETE RESTRICT,
    channel               TEXT NOT NULL CHECK (channel IN ('telegram','whatsapp','email')),
    address_hash          TEXT NOT NULL,
    address_last4         TEXT,
    external_recipient_id TEXT,
    verified_at           TEXT,
    enabled               INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
    created_at            TEXT NOT NULL,
    revoked_at            TEXT,
    UNIQUE (channel, address_hash)
);

CREATE INDEX IF NOT EXISTS idx_endpoints_principal
    ON notification_endpoints(principal_id);

CREATE TABLE IF NOT EXISTS radar_notification_preferences (
    preference_id         TEXT PRIMARY KEY,
    radar_id              TEXT NOT NULL REFERENCES radars(id) ON DELETE RESTRICT,
    endpoint_id           TEXT NOT NULL REFERENCES notification_endpoints(endpoint_id) ON DELETE RESTRICT,
    enabled               INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0,1)),
    frequency             TEXT NOT NULL DEFAULT 'per_run'
                          CHECK (frequency IN ('per_run','daily','weekly')),
    quiet_hours_start     TEXT,
    quiet_hours_end       TEXT,
    timezone              TEXT NOT NULL DEFAULT 'America/Bogota',
    max_items             INTEGER NOT NULL DEFAULT 10 CHECK (max_items > 0),
    include_new           INTEGER NOT NULL DEFAULT 1 CHECK (include_new IN (0,1)),
    include_state_changes INTEGER NOT NULL DEFAULT 1 CHECK (include_state_changes IN (0,1)),
    include_value_changes INTEGER NOT NULL DEFAULT 1 CHECK (include_value_changes IN (0,1)),
    include_disappeared   INTEGER NOT NULL DEFAULT 0 CHECK (include_disappeared IN (0,1)),
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    UNIQUE (radar_id, endpoint_id)
);

CREATE INDEX IF NOT EXISTS idx_prefs_endpoint
    ON radar_notification_preferences(endpoint_id);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Crea las cuatro tablas si faltan. Idempotente."""
    conn.executescript(_SCHEMA)
