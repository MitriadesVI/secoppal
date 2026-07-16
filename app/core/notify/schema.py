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

import re
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

# Única frecuencia soportada. ``daily`` y ``weekly`` estuvieron aquí sin que nada
# implementara su cadencia: solo alargaban el TTL del batch, así que quien elegía
# "diario" recibía un mensaje por corrida. Una preferencia que el sistema no sabe
# cumplir es peor que una preferencia ausente — la ausente no miente.
#
# El vocabulario diferido se nombra para que el error diga "todavía no", que es la
# verdad, en vez de "frecuencia inválida", que sugiere una errata.
FREQUENCIES = (FREQUENCY_PER_RUN,)
DEFERRED_FREQUENCIES = ("daily", "weekly")

DEFAULT_TIMEZONE = "America/Bogota"
DEFAULT_MAX_ITEMS = 10


class SchemaIncompatibleError(Exception):
    """La base persistida no admite el schema actual y nadie puede decidir por ella.

    Se lanza sin escribir nada. No es un caso a reintentar ni a "arreglar": es una
    base con datos que este código ya no sabe cumplir, y convertirlos en silencio
    inventaría un consentimiento que la persona no dio.
    """


def _preferences_ddl(table_name: str) -> str:
    """DDL de preferencias, parametrizado solo por el nombre de la tabla.

    Existe como función —y no como dos literales— porque el reconstructor de una
    base heredada tiene que crear **exactamente** esta tabla bajo otro nombre. Dos
    definiciones del mismo DDL divergen: es el mismo argumento por el que este
    módulo no replica el DDL de ``radars``.

    ``frequency = 'per_run'`` en vez de ``IN ('per_run')``: el ``CHECK`` es la
    afirmación "solo existe una frecuencia", y un ``IN`` de un elemento invita a
    añadir el segundo sin pensar qué lo implementa.

    ``include_disappeared`` conserva su columna y **no** lleva ``CHECK`` que lo
    fuerce a 0. Es deliberado: el bloqueo vive en el store y el fail-closed en el
    selector, porque necesitamos poder construir —por SQL directo— la fila heredada
    con ``include_disappeared=1`` que prueba que el selector no la entrega. Un CHECK
    aquí haría inconstruible el caso que hay que defender.
    """
    return f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    preference_id         TEXT PRIMARY KEY,
    radar_id              TEXT NOT NULL REFERENCES radars(id) ON DELETE RESTRICT,
    endpoint_id           TEXT NOT NULL REFERENCES notification_endpoints(endpoint_id) ON DELETE RESTRICT,
    enabled               INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0,1)),
    frequency             TEXT NOT NULL DEFAULT 'per_run'
                          CHECK (frequency = 'per_run'),
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
);"""


# Columnas de preferencias, en orden. El reconstructor las nombra explícitamente en
# lugar de usar ``SELECT *``: con ``*``, un reordenamiento futuro del DDL copiaría
# ``timezone`` dentro de ``frequency`` sin que nada se queje.
_PREFERENCE_COLUMNS = (
    "preference_id", "radar_id", "endpoint_id", "enabled", "frequency",
    "quiet_hours_start", "quiet_hours_end", "timezone", "max_items",
    "include_new", "include_state_changes", "include_value_changes",
    "include_disappeared", "created_at", "updated_at",
)


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

{preferences_table}

CREATE INDEX IF NOT EXISTS idx_prefs_endpoint
    ON radar_notification_preferences(endpoint_id);

-- Outbox (P1B-2): la intención de entrega. ``radar_id`` sin FK a propósito —el
-- ledger debe sobrevivir al archivado del radar (y a una eventual retención de
-- ``radar_events``): es un registro de auditoría, no una vista derivada.
CREATE TABLE IF NOT EXISTS radar_notification_batches (
    notification_id          TEXT PRIMARY KEY,
    principal_id              TEXT NOT NULL,
    endpoint_id               TEXT NOT NULL,
    radar_id                  TEXT NOT NULL,
    batch_key                 TEXT NOT NULL,
    idempotency_key           TEXT NOT NULL,
    external_idempotency_key  TEXT,
    payload_json              TEXT NOT NULL,
    status                    TEXT NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending','claimed','sent','ambiguous','dead')),
    dead_reason               TEXT
                              CHECK (dead_reason IS NULL OR dead_reason IN
                              ('opted_out','endpoint_revoked','radar_archived',
                               'expired','invalid_address')),
    not_before                TEXT,
    claim_token               TEXT,
    claim_until               TEXT,
    attempt_count             INTEGER NOT NULL DEFAULT 0,
    created_at                TEXT NOT NULL,
    expires_at                TEXT NOT NULL,
    UNIQUE (endpoint_id, idempotency_key),
    UNIQUE (notification_id, endpoint_id)
);
CREATE INDEX IF NOT EXISTS idx_batches_claimable
    ON radar_notification_batches(status, not_before);
CREATE INDEX IF NOT EXISTS idx_batches_key
    ON radar_notification_batches(batch_key);

-- P1B-4. Ambos aditivos: ningún ALTER TABLE, ninguna tabla recreada, ningún
-- backfill. Las filas existentes conservan ``external_idempotency_key`` NULL y el
-- índice único parcial las ignora todas.
--
-- La unicidad de la clave externa la hace cumplir la base, no el worker: es la
-- afirmación "esta clave identifica un solo batch" y, si algún día la fórmula
-- dejara de ser inyectiva, queremos un IntegrityError, no dos mensajes que el
-- proveedor cree el mismo. El ``WHERE ... IS NOT NULL`` es obligatorio: sin él,
-- SQLite trataría cada NULL como distinto en un índice único —cierto— pero el
-- índice cargaría con todas las filas aún no autorizadas sin decir nada de ellas.
CREATE UNIQUE INDEX IF NOT EXISTS idx_batches_external_idem
    ON radar_notification_batches(external_idempotency_key)
    WHERE external_idempotency_key IS NOT NULL;

-- El sweep y la búsqueda de candidatos filtran por estado y comparan expiración.
-- ``idx_batches_claimable(status, not_before)`` no sirve para eso: su segunda
-- columna es la equivocada. No se añade ``claim_until`` como tercera columna: el
-- sweep ya llega a la fila por este índice y el resto del predicado se resuelve
-- sobre ella.
CREATE INDEX IF NOT EXISTS idx_batches_expiry
    ON radar_notification_batches(status, expires_at);

-- Asociación evento↔batch: la cadena de trazabilidad. ``event_id`` sin FK
-- (``radar_events`` se borra en CASCADE con su radar; el ledger no). La
-- UNIQUE(endpoint_id, event_id) es la que cierra el doble notificador: la PK
-- sola solo protege duplicados dentro de un mismo batch.
CREATE TABLE IF NOT EXISTS radar_notification_batch_events (
    notification_id TEXT NOT NULL,
    endpoint_id     TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    event_seq       INTEGER NOT NULL,
    PRIMARY KEY (notification_id, event_id),
    UNIQUE (endpoint_id, event_id),
    FOREIGN KEY (notification_id, endpoint_id)
        REFERENCES radar_notification_batches(notification_id, endpoint_id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_batch_events_seq
    ON radar_notification_batch_events(event_seq);

-- Intentos: uno por llamada al proveedor. Write-ahead — ``dispatching`` antes
-- de llamar al proveedor, ``finished`` después; nunca se registra solo al
-- terminar. ``RESTRICT`` hacia el batch: un intento no se borra con su batch.
CREATE TABLE IF NOT EXISTS radar_delivery_attempts (
    attempt_id          TEXT PRIMARY KEY,
    notification_id     TEXT NOT NULL REFERENCES radar_notification_batches(notification_id)
                        ON DELETE RESTRICT,
    attempt_number      INTEGER NOT NULL,
    phase               TEXT NOT NULL DEFAULT 'dispatching'
                        CHECK (phase IN ('dispatching','finished')),
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    provider_message_id TEXT,
    provider_status     TEXT,
    error_class         TEXT CHECK (error_class IS NULL OR error_class IN
                        ('rate_limited','invalid_address','provider_error',
                         'timeout_after_send','network_error','auth_error')),
    error_message_safe  TEXT,
    retry_after         INTEGER,
    outcome             TEXT CHECK (outcome IS NULL OR outcome IN
                        ('sent','retry','ambiguous','dead')),
    UNIQUE (notification_id, attempt_number),
    CHECK (
        (phase = 'dispatching' AND outcome IS NULL)
        OR (phase = 'finished' AND outcome IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_attempts_notification
    ON radar_delivery_attempts(notification_id);

-- Cursor por endpoint/radar: watermark de lo examinado, no de lo entregado.
-- P1B-2 solo define el schema; el selector y el avance del cursor son P1B-3.
CREATE TABLE IF NOT EXISTS radar_notification_cursors (
    endpoint_id       TEXT NOT NULL,
    radar_id          TEXT NOT NULL,
    last_examined_seq INTEGER NOT NULL DEFAULT 0,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (endpoint_id, radar_id)
);
"""


_PREFERENCES_TABLE = "radar_notification_preferences"

# Huella del CHECK vigente, con los espacios ya fuera. Se compara contra el DDL que
# SQLite guarda en ``sqlite_master``, que es el texto literal del CREATE original:
# si la base se creó con el vocabulario viejo, esta huella no aparece.
#
# Es una comparación de texto normalizado, con lo frágil que eso tiene: reescribir
# el CHECK como ``IN ('per_run')`` sin tocar esta constante haría que toda base
# vigente pareciera heredada. El test de preflight compara contra ``_preferences_ddl``
# real, así que ese desliz sale en la suite y no en producción.
_EXPECTED_FREQUENCY_CHECK = "check(frequency='per_run')"


def _normalized_ddl(sql: str | None) -> str:
    return re.sub(r"\s+", "", sql or "").lower()


def _preferences_table_is_current(conn: sqlite3.Connection) -> bool | None:
    """``None`` si la tabla no existe; si existe, si su CHECK ya es el vigente."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?",
        (_PREFERENCES_TABLE,),
    ).fetchone()
    if row is None:
        return None
    return _EXPECTED_FREQUENCY_CHECK in _normalized_ddl(row["sql"])


def _rebuild_preferences_table(conn: sqlite3.Connection) -> None:
    """Reconstruye la tabla bajo el CHECK vigente, conservando todas las filas.

    Solo se llega aquí con **cero** filas incompatibles ya verificado: esto no
    convierte datos, solo estrecha la regla que los admite. Negarse a abrir una base
    cuyas preferencias son todas ``per_run`` obligaría a alguien a destruir registros
    de consentimiento para poder arrancar, que es un remedio peor que la enfermedad.

    ``PRAGMA foreign_keys`` es un no-op dentro de una transacción, así que se apaga
    **antes** del BEGIN: sin eso, el DROP de una tabla con FK salientes falla. El
    ``foreign_key_check`` de dentro es lo que devuelve la garantía que el PRAGMA
    apagado quitó — y va antes del COMMIT, no después, para que una violación
    revierta en vez de quedar registrada.
    """
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            columns = ", ".join(_PREFERENCE_COLUMNS)
            temp = f"{_PREFERENCES_TABLE}_rebuild"
            conn.execute(f"DROP TABLE IF EXISTS {temp}")
            # ``execute``, no ``executescript``: este último emite un COMMIT antes de
            # correr, lo que cerraría la transacción que hace atómica la
            # reconstrucción y dejaría la tabla a medias si algo fallara después.
            # ``_preferences_ddl`` devuelve exactamente una sentencia.
            conn.execute(_preferences_ddl(temp))
            conn.execute(
                f"INSERT INTO {temp} ({columns}) "
                f"SELECT {columns} FROM {_PREFERENCES_TABLE}"
            )
            conn.execute(f"DROP TABLE {_PREFERENCES_TABLE}")
            conn.execute(f"ALTER TABLE {temp} RENAME TO {_PREFERENCES_TABLE}")
            violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise SchemaIncompatibleError(
                    f"Reconstruir '{_PREFERENCES_TABLE}' dejaría "
                    f"{len(violations)} violación(es) de clave foránea. No se "
                    f"reconstruye: la base ya era inconsistente antes de este cambio."
                )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _preflight_preferences(conn: sqlite3.Connection) -> None:
    """Decide qué hacer con una base heredada **antes** de tocar nada.

    ``CREATE TABLE IF NOT EXISTS`` no toca una tabla que ya existe: sin este
    preflight, una base creada con el vocabulario viejo conservaría en silencio un
    CHECK que sigue aceptando ``daily`` por SQL directo, y el invariante de este
    bloque sería falso justo donde nadie lo mira.
    """
    state = _preferences_table_is_current(conn)
    if state is None or state:
        return

    incompatible = conn.execute(
        f"SELECT preference_id, frequency FROM {_PREFERENCES_TABLE} "
        f"WHERE frequency <> ? ORDER BY preference_id",
        (FREQUENCY_PER_RUN,),
    ).fetchall()
    if incompatible:
        detail = ", ".join(
            f"{row['preference_id']}={row['frequency']!r}" for row in incompatible
        )
        raise SchemaIncompatibleError(
            f"La base tiene {len(incompatible)} preferencia(s) con una frecuencia "
            f"que este código ya no sabe cumplir ({', '.join(DEFERRED_FREQUENCIES)}): "
            f"{detail}. No se convierten a '{FREQUENCY_PER_RUN}' automáticamente: "
            f"quien eligió un resumen diario no consintió recibir uno por corrida. "
            f"Decida explícitamente y vuelva a abrir."
        )
    _rebuild_preferences_table(conn)


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Crea las tablas de notify si faltan. Idempotente.

    Identidad/endpoints/preferencias y el outbox (batches/asociación/intentos/
    cursores) viven en el mismo ``executescript``: cualquier Store de
    ``notify`` puede ser el primero en abrir la base sin dejar una tabla a
    medias. Ninguna de las tablas de outbox tiene FK hacia ``radars`` ni hacia
    ``radar_events``, así que —a diferencia de las preferencias— no dependen
    de que ``RadarStore`` se haya inicializado antes.

    El preflight de preferencias corre **antes** del script: si la base es heredada e
    irreconciliable, no se crea nada y el error llega intacto al caller.
    """
    _preflight_preferences(conn)
    conn.executescript(
        _SCHEMA.replace(
            "{preferences_table}", _preferences_ddl(_PREFERENCES_TABLE)
        )
    )
