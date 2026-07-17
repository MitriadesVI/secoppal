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

# ── Vocabularios cerrados del ledger (ADR-053) ────────────────────────────────────
#
# Son la fuente única: el DDL se genera a partir de estas tuplas y ``ledger_store``
# valida contra ellas. Escribirlos dos veces —una en el CHECK y otra en Python— es
# como acaban divergiendo la regla que la base impone y la que el código cree.
#
# El orden es normativo, no estético: la huella que detecta una base heredada se
# deriva del texto del CHECK, así que reordenar estas tuplas hace que toda base
# vigente parezca heredada. El test de preflight lo comprueba contra el DDL real.

DEAD_REASON_OPTED_OUT = "opted_out"
DEAD_REASON_ENDPOINT_REVOKED = "endpoint_revoked"
DEAD_REASON_RADAR_ARCHIVED = "radar_archived"
DEAD_REASON_EXPIRED = "expired"
# Reservado **exclusivamente** para una dirección inválida. No es el cajón de
# sastre de los fallos permanentes: usarlo para un error de nuestro payload o de
# nuestra política de reintentos afirmaría que la dirección de una persona real
# está mal, y ``dead_reason`` es el único registro que explica por qué no se
# entregó. Para esos dos casos existen ``render_error`` y ``retries_exhausted``.
DEAD_REASON_INVALID_ADDRESS = "invalid_address"
# Nuestro payload no se pudo decodificar, versionar o renderizar. El fallo es
# nuestro y ocurre **antes** de cualquier llamada: cero efecto externo.
DEAD_REASON_RENDER_ERROR = "render_error"
# La política de reintentos se agotó. El último intento no tuvo efecto externo;
# lo que murió es la paciencia, no el consentimiento ni el TTL.
DEAD_REASON_RETRIES_EXHAUSTED = "retries_exhausted"
# El proveedor rechazó de forma permanente antes de cualquier efecto externo, por
# una causa que no es la dirección. ``error_class`` conserva la causa concreta.
DEAD_REASON_PERMANENT_PROVIDER_ERROR = "permanent_provider_error"

DEAD_REASONS = (
    DEAD_REASON_OPTED_OUT,
    DEAD_REASON_ENDPOINT_REVOKED,
    DEAD_REASON_RADAR_ARCHIVED,
    DEAD_REASON_EXPIRED,
    DEAD_REASON_INVALID_ADDRESS,
    DEAD_REASON_RENDER_ERROR,
    DEAD_REASON_RETRIES_EXHAUSTED,
    DEAD_REASON_PERMANENT_PROVIDER_ERROR,
)

ERROR_CLASSES = (
    "rate_limited",
    "invalid_address",
    "provider_error",
    "timeout_after_send",
    "network_error",
    "auth_error",
    # Nuestro render falló. Es una clase de error del sistema, no del proveedor:
    # el proveedor nunca llegó a verse.
    "render_error",
    # No hay proveedor registrado para el canal. Es un fallo de configuración
    # nuestro y es reversible —registrar el proveedor lo resuelve—, así que su
    # outcome normal es ``retry``, no ``dead``.
    "provider_unavailable",
)

# Vocabularios con los que se creó la base anterior a ADR-053. Se nombran para poder
# **reconocer** una base heredada, no para admitirla: sin esta constante, el preflight
# no podría distinguir "base vieja que sé migrar" de "DDL que no reconozco", y esa
# distinción es justo la que decide entre migrar y parar.
_LEGACY_DEAD_REASONS = (
    DEAD_REASON_OPTED_OUT,
    DEAD_REASON_ENDPOINT_REVOKED,
    DEAD_REASON_RADAR_ARCHIVED,
    DEAD_REASON_EXPIRED,
    DEAD_REASON_INVALID_ADDRESS,
)

_LEGACY_ERROR_CLASSES = (
    "rate_limited",
    "invalid_address",
    "provider_error",
    "timeout_after_send",
    "network_error",
    "auth_error",
)


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


def _quoted(values: tuple[str, ...]) -> str:
    return ",".join(f"'{value}'" for value in values)


def _batches_ddl(table_name: str, dead_reasons: tuple[str, ...] = DEAD_REASONS) -> str:
    """DDL del outbox, parametrizado por nombre de tabla y vocabulario.

    Igual que ``_preferences_ddl``: existe como función porque el reconstructor tiene
    que crear **exactamente** esta tabla bajo otro nombre, y dos literales del mismo
    DDL divergen. ``dead_reasons`` es parámetro —y no la constante directamente— para
    que los tests puedan fabricar la base heredada real con el vocabulario viejo; sin
    eso, el caso que hay que defender sería inconstruible.

    ``radar_id`` sin FK a propósito: el ledger debe sobrevivir al archivado del radar.
    Es un registro de auditoría, no una vista derivada.
    """
    return f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    notification_id           TEXT PRIMARY KEY,
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
                              ({_quoted(dead_reasons)})),
    not_before                TEXT,
    claim_token               TEXT,
    claim_until               TEXT,
    attempt_count             INTEGER NOT NULL DEFAULT 0,
    created_at                TEXT NOT NULL,
    expires_at                TEXT NOT NULL,
    UNIQUE (endpoint_id, idempotency_key),
    UNIQUE (notification_id, endpoint_id)
);"""


def _attempts_ddl(
    table_name: str,
    error_classes: tuple[str, ...] = ERROR_CLASSES,
    batches_table: str = "radar_notification_batches",
) -> str:
    """DDL de los intentos. ``RESTRICT`` hacia el batch: un intento no se borra con él.

    El ``CHECK`` compuesto de ``phase``/``outcome`` es lo que hace imposible un intento
    ``finished`` sin desenlace o uno ``dispatching`` que ya afirme uno.
    """
    return f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    attempt_id          TEXT PRIMARY KEY,
    notification_id     TEXT NOT NULL REFERENCES {batches_table}(notification_id)
                        ON DELETE RESTRICT,
    attempt_number      INTEGER NOT NULL,
    phase               TEXT NOT NULL DEFAULT 'dispatching'
                        CHECK (phase IN ('dispatching','finished')),
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    provider_message_id TEXT,
    provider_status     TEXT,
    error_class         TEXT CHECK (error_class IS NULL OR error_class IN
                        ({_quoted(error_classes)})),
    error_message_safe  TEXT,
    retry_after         INTEGER,
    outcome             TEXT CHECK (outcome IS NULL OR outcome IN
                        ('sent','retry','ambiguous','dead')),
    UNIQUE (notification_id, attempt_number),
    CHECK (
        (phase = 'dispatching' AND outcome IS NULL)
        OR (phase = 'finished' AND outcome IS NOT NULL)
    )
);"""


def _batches_indexes_ddl(table_name: str) -> tuple[str, ...]:
    """Los índices del outbox, como sentencias sueltas y en una sola definición.

    Existe por la misma razón que ``_batches_ddl``: la migración los recrea dentro de
    su transacción y ``_SCHEMA`` los crea al abrir. Dos literales del mismo índice
    divergen, y el que divergiera sería justo el que no protege nada.

    ``idx_batches_external_idem`` es UNIQUE y **parcial**: el ``WHERE ... IS NOT
    NULL`` es obligatorio. Sin él, SQLite trataría cada NULL como distinto —cierto—
    pero el índice cargaría con todas las filas aún no autorizadas sin decir nada de
    ellas. Su unicidad es la afirmación "esta clave identifica un solo batch": si
    algún día la fórmula dejara de ser inyectiva, queremos un IntegrityError, no dos
    mensajes que el proveedor cree el mismo.

    ``idx_batches_expiry`` existe porque ``idx_batches_claimable(status, not_before)``
    no sirve para el sweep: su segunda columna es la equivocada.
    """
    return (
        f"CREATE INDEX IF NOT EXISTS idx_batches_claimable "
        f"ON {table_name}(status, not_before)",
        f"CREATE INDEX IF NOT EXISTS idx_batches_key ON {table_name}(batch_key)",
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_batches_external_idem "
        f"ON {table_name}(external_idempotency_key) "
        f"WHERE external_idempotency_key IS NOT NULL",
        f"CREATE INDEX IF NOT EXISTS idx_batches_expiry "
        f"ON {table_name}(status, expires_at)",
    )


def _joined_indexes(statements: tuple[str, ...]) -> str:
    """Las sentencias sueltas, unidas para ``executescript``. Es la única diferencia
    entre cómo las consume el DDL de apertura y cómo las consume la migración, que las
    ejecuta una a una dentro de su transacción."""
    return "".join(f"{statement};\n" for statement in statements)


def _attempts_indexes_ddl(table_name: str) -> tuple[str, ...]:
    """Los índices de los intentos, en una sola definición.

    ``idx_attempts_one_open`` **no es rendimiento**: es la restricción que hace
    físicamente imposible un segundo intento abierto sobre el mismo batch, y por
    tanto una segunda llamada al proveedor por el mismo mensaje. ``worker_store`` ya
    lo comprueba dentro de su transacción, pero esa comprobación solo protege a quien
    pasa por esa puerta: un script, un caller futuro, ``LedgerStore.begin_attempt`` o
    SQL directo abrirían el segundo sin que nadie se entere. Parcial a propósito: los
    ``finished`` se acumulan sin límite, que es el historial que queremos conservar.

    Que esté aquí —y no solo en ``_SCHEMA``— es lo que permite recrearlo **dentro**
    de la transacción de la migración. Dejarlo para el ``executescript`` posterior
    abriría una ventana con las tablas ya confirmadas y el índice todavía ausente.
    """
    return (
        f"CREATE INDEX IF NOT EXISTS idx_attempts_notification "
        f"ON {table_name}(notification_id)",
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_attempts_one_open "
        f"ON {table_name}(notification_id) WHERE phase = 'dispatching'",
    )


_BATCH_COLUMNS = (
    "notification_id", "principal_id", "endpoint_id", "radar_id", "batch_key",
    "idempotency_key", "external_idempotency_key", "payload_json", "status",
    "dead_reason", "not_before", "claim_token", "claim_until", "attempt_count",
    "created_at", "expires_at",
)

_ATTEMPT_COLUMNS = (
    "attempt_id", "notification_id", "attempt_number", "phase", "started_at",
    "finished_at", "provider_message_id", "provider_status", "error_class",
    "error_message_safe", "retry_after", "outcome",
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

-- Outbox (P1B-2): la intención de entrega. Sus índices —y los de los intentos— se
-- generan desde ``_batches_indexes_ddl``/``_attempts_indexes_ddl``, que es también
-- lo que la migración ejecuta dentro de su propia transacción: una sola definición
-- por índice, o el que divergiera sería el que no protege nada.
{batches_table}
{batches_indexes}

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
-- terminar.
{attempts_table}
{attempts_indexes}

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


_ATTEMPTS_TABLE = "radar_delivery_attempts"
_BATCHES_TABLE = "radar_notification_batches"

def _table_ddl(conn: sqlite3.Connection, table_name: str) -> str | None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return None if row is None else row["sql"]


def _canonical_form(ddl: str) -> str:
    """Normaliza para comparar contra lo que ``sqlite_master.sql`` persiste realmente.

    Tres diferencias, ninguna semántica, y cualquiera de ellas haría que ninguna
    forma canónica generada coincidiera jamás con una base real —incluida la que este
    mismo código acaba de migrar—:

    - ``IF NOT EXISTS``: SQLite no lo conserva en el texto que guarda. Lo que se
      ejecuta como ``CREATE TABLE IF NOT EXISTS foo (...)`` se lee después como
      ``CREATE TABLE foo (...)``.
    - El ``;`` final: los generadores (``_batches_ddl``/``_attempts_ddl``) terminan su
      texto con él porque también producen el DDL que se ejecuta, pero
      ``sqlite_master.sql`` guarda la sentencia sin su terminador.
    - Comillas dobles alrededor del nombre: ``ALTER TABLE ... RENAME TO`` —el paso
      final de ``_swap_table``— hace que SQLite reescriba el ``CREATE TABLE`` con el
      nombre nuevo **entrecomillado** (``CREATE TABLE "radar_notification_batches"
      (...)``), aunque el DDL original nunca las llevó. Ningún identificador de este
      schema usa comillas dobles por otra razón, así que quitarlas todas es seguro.
    """
    return (
        _normalized_ddl(ddl)
        .replace("ifnotexists", "")
        .replace('"', "")
        .rstrip(";")
    )


# Las cuatro formas canónicas completas, derivadas de los mismos generadores que
# escriben el DDL real. La comparación es del **texto entero** de la tabla —no de un
# fragmento del CHECK—: si el vocabulario coincide pero se perdió un UNIQUE, un
# ON DELETE RESTRICT o el CHECK compuesto de phase/outcome, ninguna de las cuatro
# formas coincide, y eso es exactamente lo que tiene que pasar. Reconocer solo el
# vocabulario dejaría clasificar como "vigente" una tabla que perdió cualquier otra
# invariante, porque ``CREATE TABLE IF NOT EXISTS`` no repara lo que no reconoce.
_BATCHES_CURRENT_DDL = _canonical_form(_batches_ddl(_BATCHES_TABLE, DEAD_REASONS))
_BATCHES_LEGACY_DDL = _canonical_form(_batches_ddl(_BATCHES_TABLE, _LEGACY_DEAD_REASONS))
_ATTEMPTS_CURRENT_DDL = _canonical_form(_attempts_ddl(_ATTEMPTS_TABLE, ERROR_CLASSES))
_ATTEMPTS_LEGACY_DDL = _canonical_form(
    _attempts_ddl(_ATTEMPTS_TABLE, _LEGACY_ERROR_CLASSES)
)


def _table_state(ddl: str, *, current: str, legacy: str, table_name: str) -> str:
    """``'current'`` o ``'legacy'``. Cualquier otra cosa para en seco.

    Fail-closed ante un DDL desconocido: si la tabla no coincide, carácter por
    carácter una vez normalizada, ni con la forma vigente ni con la heredada que este
    código sabe migrar, alguien la editó a mano, perdió una invariante que no es el
    vocabulario, o viene de una versión que este código no conoce. Reconstruirla "por
    si acaso" copiaría filas hacia un CHECK que quizá las rechace a medias, y seguir
    adelante dejaría una base cuya estructura real nadie verificó. Se enumera y se
    para: un humano decide.
    """
    normalized = _canonical_form(ddl)
    if normalized == current:
        return "current"
    if normalized == legacy:
        return "legacy"
    raise SchemaIncompatibleError(
        f"El DDL de '{table_name}' no coincide, completo, ni con la versión vigente "
        f"ni con la heredada que este código sabe migrar. La tabla se creó con una "
        f"versión desconocida, o perdió una restricción —una UNIQUE, una FK, un "
        f"CHECK compuesto— que el vocabulario por sí solo no delata. No se reconstruye "
        f"a ciegas: hacerlo copiaría filas hacia una estructura que nadie verificó. "
        f"Revíselo y vuelva a abrir."
    )


def _reject_unknown_values(
    conn: sqlite3.Connection, table_name: str, column: str, vocabulary: tuple[str, ...]
) -> None:
    """Se niega a migrar si la base ya contiene valores fuera del vocabulario nuevo.

    El vocabulario nuevo es un superconjunto del heredado, así que llegar aquí con
    filas fuera significa que algo escribió por un camino que no pasó por ningún
    CHECK. Copiarlas al CHECK nuevo fallaría a mitad de la reconstrucción con un
    IntegrityError que no dice de qué fila habla; y descartarlas silenciosamente
    borraría el único registro de por qué no se entregó un mensaje.
    """
    placeholders = ",".join("?" * len(vocabulary))
    rows = conn.execute(
        f"SELECT DISTINCT {column} FROM {table_name} "
        f"WHERE {column} IS NOT NULL AND {column} NOT IN ({placeholders}) "
        f"ORDER BY {column}",
        vocabulary,
    ).fetchall()
    if rows:
        detail = ", ".join(repr(row[column]) for row in rows)
        raise SchemaIncompatibleError(
            f"La base tiene valores de '{table_name}.{column}' fuera del vocabulario "
            f"que este código admite: {detail}. No se migran ni se descartan: son el "
            f"registro de por qué no se entregó un mensaje. Decida explícitamente y "
            f"vuelva a abrir."
        )


def _rebuild_ledger_tables(
    conn: sqlite3.Connection, *, rebuild_batches: bool, rebuild_attempts: bool
) -> None:
    """Reconstruye outbox e intentos bajo el vocabulario vigente. Una sola migración.

    Las dos tablas van en la **misma** transacción aunque solo una lo necesite: están
    unidas por una FK, y dejar la base con el outbox nuevo y los intentos viejos —o al
    revés— sería un estado que ningún preflight posterior sabría nombrar.

    Solo estrecha o ensancha la regla que admite los datos; no convierte ninguno. Se
    llega aquí con los valores fuera de vocabulario ya descartados por
    ``_reject_unknown_values``, así que cada fila que entra sale idéntica.

    ``PRAGMA foreign_keys`` es un no-op dentro de una transacción, así que se apaga
    antes del BEGIN: sin eso, el DROP de una tabla referenciada falla.
    ``legacy_alter_table=ON`` durante el RENAME es lo que impide que SQLite "ayude"
    reescribiendo las FK de las tablas que apuntan a la que estamos reemplazando —es
    el procedimiento que el propio manual de SQLite prescribe—. El
    ``foreign_key_check`` de dentro devuelve la garantía que el PRAGMA apagado quitó,
    y va antes del COMMIT para que una violación revierta en vez de quedar registrada.

    **Los índices se recrean aquí dentro, no después.** ``DROP TABLE`` se lleva por
    delante los de la tabla que reemplaza, y el ``executescript`` de ``ensure_schema``
    solo corre cuando esta transacción ya commiteó. Dejarlos para entonces abriría una
    ventana con las tablas nuevas ya confirmadas y ``idx_attempts_one_open`` todavía
    ausente: un crash o un segundo proceso en ese instante encontrarían una base sin la
    única restricción que impide físicamente dos intentos ``dispatching`` sobre el
    mismo batch — y por tanto dos llamadas al proveedor por el mismo mensaje. La
    migración es atómica o no lo es; "atómica salvo los índices" no significa nada.
    """
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("PRAGMA legacy_alter_table=ON")
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            if rebuild_batches:
                _swap_table(
                    conn, _BATCHES_TABLE, _BATCH_COLUMNS,
                    _batches_ddl(f"{_BATCHES_TABLE}_rebuild"),
                    _batches_indexes_ddl(_BATCHES_TABLE),
                )
            if rebuild_attempts:
                _swap_table(
                    conn, _ATTEMPTS_TABLE, _ATTEMPT_COLUMNS,
                    _attempts_ddl(f"{_ATTEMPTS_TABLE}_rebuild"),
                    _attempts_indexes_ddl(_ATTEMPTS_TABLE),
                )
            violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise SchemaIncompatibleError(
                    f"Migrar el ledger dejaría {len(violations)} violación(es) de "
                    f"clave foránea. No se migra: la base ya era inconsistente antes "
                    f"de este cambio."
                )
            integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise SchemaIncompatibleError(
                    f"La verificación de integridad tras migrar el ledger no pasó: "
                    f"{integrity}. Se revierte por completo."
                )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.execute("PRAGMA legacy_alter_table=OFF")
        conn.execute("PRAGMA foreign_keys=ON")


def _swap_table(
    conn: sqlite3.Connection,
    table_name: str,
    columns: tuple[str, ...],
    ddl: str,
    indexes: tuple[str, ...],
) -> None:
    """Crea la tabla nueva, copia fila a fila, la pone en el sitio de la vieja y
    **recrea sus índices**, todo dentro de la transacción del caller.

    Las columnas se nombran explícitamente en vez de ``SELECT *``: con ``*``, un
    reordenamiento futuro del DDL copiaría ``claim_until`` dentro de ``claim_token``
    sin que nada se queje. El recuento de filas se compara antes y después porque una
    copia que pierde filas en silencio es exactamente el fallo que esta migración no
    puede permitirse.

    Los índices son parte del swap y no un paso posterior: quien reemplaza la tabla es
    quien tiene que devolverle lo que el ``DROP`` le quitó, y tiene que hacerlo antes
    de que nadie más pueda ver la tabla nueva. Se crean sobre ``table_name`` —ya
    renombrada—, no sobre la temporal, para que sus nombres sean los definitivos.

    ``execute``, no ``executescript``: este último emite un COMMIT antes de correr, lo
    que cerraría la transacción que hace atómica la reconstrucción.
    """
    joined = ", ".join(columns)
    temp = f"{table_name}_rebuild"
    before = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    conn.execute(f"DROP TABLE IF EXISTS {temp}")
    conn.execute(ddl)
    conn.execute(f"INSERT INTO {temp} ({joined}) SELECT {joined} FROM {table_name}")
    copied = conn.execute(f"SELECT COUNT(*) FROM {temp}").fetchone()[0]
    if copied != before:
        raise SchemaIncompatibleError(
            f"Migrar '{table_name}' copió {copied} de {before} filas. No se acepta "
            f"una migración que pierde registros: se revierte entera."
        )
    conn.execute(f"DROP TABLE {table_name}")
    conn.execute(f"ALTER TABLE {temp} RENAME TO {table_name}")
    for statement in indexes:
        conn.execute(statement)


# Los dos índices que **no** son rendimiento: cada uno es una invariante de
# corrección que la base hace cumplir y el código no puede.
#
# - ``idx_attempts_one_open``: como máximo un intento abierto por batch. Sin su
#   UNIQUE, o sin su ``WHERE phase='dispatching'``, dos intentos abiertos coexisten y
#   con ellos dos llamadas posibles al proveedor por el mismo mensaje.
# - ``idx_batches_external_idem``: una clave externa identifica un solo batch. Sin su
#   UNIQUE, dos batches podrían llevar la misma clave al proveedor.
#
# Se derivan de los mismos generadores que los crean: una sola definición por índice.
_CRITICAL_INDEXES = {
    "idx_attempts_one_open": next(
        statement for statement in _attempts_indexes_ddl(_ATTEMPTS_TABLE)
        if "idx_attempts_one_open" in statement
    ),
    "idx_batches_external_idem": next(
        statement for statement in _batches_indexes_ddl(_BATCHES_TABLE)
        if "idx_batches_external_idem" in statement
    ),
}


def _index_ddl(conn: sqlite3.Connection, index_name: str) -> str | None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name = ?",
        (index_name,),
    ).fetchone()
    return None if row is None else row["sql"]


def _preflight_critical_indexes(conn: sqlite3.Connection) -> None:
    """Se niega a abrir si un índice de corrección existe con la definición equivocada.

    ``CREATE UNIQUE INDEX IF NOT EXISTS`` **no repara**: si ya hay un índice con ese
    nombre, no hace nada, y da igual que el existente haya perdido su ``UNIQUE`` o su
    cláusula parcial. El resto del preflight no lo ve, porque compara el DDL de las
    *tablas* y el de la tabla sigue siendo el vigente: un índice no vive en el
    ``CREATE TABLE``. La base quedaría aparentemente sana y aceptando dos intentos
    abiertos sobre el mismo batch.

    Ausente **sí** se permite: el ``executescript`` lo crea bien. Lo que no se permite
    es el nombre ocupado por otra cosa. Y no se repara automáticamente —ni un DROP, ni
    un CREATE encima—: recrear un índice único sobre datos que ya lo violan revienta
    con un ``IntegrityError`` que no dice de qué habla, y hacerlo sobre datos que lo
    cumplen dejaría sin explicar por qué el índice era otro. Se enumera y se para: un
    humano decide, con las filas delante.

    Solo estos dos. Un índice de rendimiento con la definición cambiada hace la base
    lenta, no incorrecta, y parar el arranque por eso sería desproporcionado.
    """
    for name, expected in _CRITICAL_INDEXES.items():
        actual = _index_ddl(conn, name)
        if actual is None:
            continue
        if _canonical_form(actual) != _canonical_form(expected):
            raise SchemaIncompatibleError(
                f"El índice '{name}' existe con una definición que no es la vigente. "
                f"No es un índice de rendimiento: es la restricción que la base hace "
                f"cumplir para que el código no pueda saltársela, y con esta "
                f"definición no la hace cumplir. 'CREATE ... IF NOT EXISTS' no lo "
                f"repara —ya existe ese nombre—, y no se recrea automáticamente: si "
                f"los datos ya violan la restricción, el DROP+CREATE reventaría sin "
                f"decir sobre qué filas. Revíselo y vuelva a abrir.\n"
                f"  esperado: {' '.join(expected.split())}\n"
                f"  encontrado: {' '.join(actual.split())}"
            )


def _preflight_ledger_vocabulary(conn: sqlite3.Connection) -> None:
    """Migra el ledger al vocabulario de ADR-053, o para en seco. **Antes** del DDL.

    ``CREATE TABLE IF NOT EXISTS`` no toca una tabla que ya existe: sin este
    preflight, una base creada con el vocabulario viejo seguiría rechazando
    ``render_error`` y ``retries_exhausted`` en silencio, y el worker no podría
    registrar honestamente por qué no entregó.

    Es idempotente por construcción: sobre una base ya vigente, ambos estados dan
    ``current`` y no se toca nada.
    """
    batches_ddl = _table_ddl(conn, _BATCHES_TABLE)
    attempts_ddl = _table_ddl(conn, _ATTEMPTS_TABLE)
    # Base nueva: no hay nada que migrar y el DDL vigente se crea solo.
    if batches_ddl is None and attempts_ddl is None:
        return

    rebuild_batches = batches_ddl is not None and _table_state(
        batches_ddl,
        current=_BATCHES_CURRENT_DDL,
        legacy=_BATCHES_LEGACY_DDL,
        table_name=_BATCHES_TABLE,
    ) == "legacy"
    rebuild_attempts = attempts_ddl is not None and _table_state(
        attempts_ddl,
        current=_ATTEMPTS_CURRENT_DDL,
        legacy=_ATTEMPTS_LEGACY_DDL,
        table_name=_ATTEMPTS_TABLE,
    ) == "legacy"
    if not (rebuild_batches or rebuild_attempts):
        return

    if rebuild_batches:
        _reject_unknown_values(conn, _BATCHES_TABLE, "dead_reason", DEAD_REASONS)
    if rebuild_attempts:
        _reject_unknown_values(conn, _ATTEMPTS_TABLE, "error_class", ERROR_CLASSES)
    _rebuild_ledger_tables(
        conn, rebuild_batches=rebuild_batches, rebuild_attempts=rebuild_attempts
    )


def _preflight_open_attempts(conn: sqlite3.Connection) -> None:
    """Se niega a crear el índice único si la base ya viola lo que promete.

    ``CREATE UNIQUE INDEX`` sobre datos duplicados falla con un ``IntegrityError``
    de SQLite que no dice de qué batches habla. Peor: la tentación evidente sería
    "arreglarlo" cerrando uno de los intentos abiertos, y eso es exactamente lo
    que no se puede hacer. Cada intento abierto pudo haber llegado al proveedor;
    elegir cuál sobrevive inventaría una verdad que la base no contiene, y el que
    se cierre quedaría afirmando en silencio un desenlace que nadie observó.

    Se enumera y se para. Un humano decide, con el historial delante.
    """
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (_ATTEMPTS_TABLE,),
    ).fetchone()
    if exists is None:
        # Base nueva: no hay filas que puedan violar nada.
        return

    duplicated = conn.execute(
        f"SELECT notification_id, COUNT(*) AS open_count FROM {_ATTEMPTS_TABLE} "
        f"WHERE phase = 'dispatching' GROUP BY notification_id "
        f"HAVING COUNT(*) > 1 ORDER BY notification_id"
    ).fetchall()
    if not duplicated:
        return

    detail = ", ".join(
        f"{row['notification_id']}={row['open_count']} intentos" for row in duplicated
    )
    raise SchemaIncompatibleError(
        f"La base tiene {len(duplicated)} batch(es) con más de un intento "
        f"'dispatching' abierto: {detail}. No se cierra ninguno automáticamente: "
        f"cada intento abierto pudo haber llegado al proveedor, y elegir cuál "
        f"sobrevivió inventaría una verdad que la base no contiene. Resuélvalo "
        f"explícitamente y vuelva a abrir."
    )


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Crea las tablas de notify si faltan. Idempotente.

    Identidad/endpoints/preferencias y el outbox (batches/asociación/intentos/
    cursores) viven en el mismo ``executescript``: cualquier Store de
    ``notify`` puede ser el primero en abrir la base sin dejar una tabla a
    medias. Ninguna de las tablas de outbox tiene FK hacia ``radars`` ni hacia
    ``radar_events``, así que —a diferencia de las preferencias— no dependen
    de que ``RadarStore`` se haya inicializado antes.

    Los preflights corren **antes** del script: si la base es heredada e
    irreconciliable, no se crea nada y el error llega intacto al caller. El
    ``executescript`` de abajo **reafirma** el schema de forma idempotente; no repara
    nada. Todo lo que ya existe con el nombre correcto y el contenido equivocado
    —una tabla, un índice— tiene que haberlo cazado un preflight, porque el
    ``IF NOT EXISTS`` lo dejará exactamente como está.

    El orden es normativo:

    1. ``_preflight_preferences`` — puede reconstruir la tabla de preferencias.
    2. ``_preflight_ledger_vocabulary`` — puede migrar el ledger. Recrea los índices
       de las tablas que reemplaza **dentro de su propia transacción**, no aquí
       abajo: entre su COMMIT y este script no puede existir un instante con las
       tablas nuevas confirmadas y ``idx_attempts_one_open`` ausente.
    3. ``_preflight_critical_indexes`` — después de la migración a propósito: si la
       base era heredada, la reconstrucción ya dejó los índices correctos y no hay
       nada que objetar. Solo mira lo que sobrevive sin migrar.
    4. ``_preflight_open_attempts`` — el último que lee ``radar_delivery_attempts``,
       cuando ya nadie va a reemplazarla. Al revés leería una tabla a punto de
       desaparecer.
    """
    _preflight_preferences(conn)
    _preflight_ledger_vocabulary(conn)
    _preflight_critical_indexes(conn)
    _preflight_open_attempts(conn)
    conn.executescript(
        _SCHEMA
        .replace("{preferences_table}", _preferences_ddl(_PREFERENCES_TABLE))
        .replace("{batches_table}", _batches_ddl(_BATCHES_TABLE))
        .replace("{batches_indexes}", _joined_indexes(_batches_indexes_ddl(_BATCHES_TABLE)))
        .replace("{attempts_table}", _attempts_ddl(_ATTEMPTS_TABLE))
        .replace("{attempts_indexes}", _joined_indexes(_attempts_indexes_ddl(_ATTEMPTS_TABLE)))
    )
