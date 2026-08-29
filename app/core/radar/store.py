"""store.py — persistencia SQLite del Radar.

Cinco autoridades sobre la misma base que feedback (``data/secoppal.db``), creadas
de forma idempotente:

- ``radars``              — la definición del radar (QueryFrame serializado + metadatos).
- ``radar_runs``          — una fila por corrida, con auditoría (soql/params/execution_ms).
- ``radar_events``        — el feed de movimientos (nuevo/cambio de estado/valor).
- ``radar_seen_results``  — memoria de lo visto, para diffear corridas sucesivas.
- ``radar_scheduler_state`` — heartbeat singleton de la última pasada automática.

El ``RadarStore`` abre conexiones cortas por operación vía ``connect_sqlite``
(WAL + FK + busy_timeout ya aplicados).

La *materialización* de una corrida es la excepción: ``run``, ``seen``,
``events`` y el estado del radar describen un mismo hecho y se escriben en una
sola transacción (ver ``materialize_successful_run``). Escribirlos por separado
—como hacía cada método autocommit— permite que un crash entre `seen` y
`events` marque resultados como vistos sin haber anunciado su novedad, que se
pierde para siempre. Los métodos sueltos siguen existiendo para escrituras que
sí son hechos independientes; todos comparten el mismo SQL vía los helpers
connection-aware de este módulo.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.db import connect_sqlite
from app.core.radar import lease
from app.core.radar.lease import LeaseClaim


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex


def new_run_id() -> str:
    """Id de corrida generable antes de persistir.

    Permite construir eventos y vistos —que referencian el run— en memoria y
    fuera de la transacción, sin que la construcción dependa del store.
    """
    return _new_id()


class RadarExistsError(ValueError):
    """Ya existe un radar con ese (owner, name)."""


class RadarReservedNameError(ValueError):
    """El nombre pertenece a un radar sintético y no puede usarlo otro radar."""


# El radar sintético de "procesos seguidos" (ADR-035) es una fila más de
# ``radars``, distinguida por su ``frequency``. Las constantes viven aquí —y no
# en ``follows.py``— porque describen una regla de integridad de *esta* tabla:
# ``UNIQUE (owner, name)`` no alcanza para protegerla (ver ``_is_reserved_name``)
# y este módulo no puede importar ``follows`` sin invertir la dependencia.
# ``follows.py`` las re-exporta como su API pública.
FOLLOWS_FREQUENCY = "follows"
FOLLOWS_RADAR_NAME = "📌 Procesos seguidos"


def _is_reserved_name(name: str) -> bool:
    """¿``name`` colisiona con el nombre del radar sintético?

    Compara igual que ``get_radar_by_name`` (que busca con ``COLLATE NOCASE``),
    porque el ``UNIQUE (owner, name)`` es **binario**: sin esto, un radar normal
    llamado "📌 procesos seguidos" pasa el UNIQUE y aun así hace NOCASE-match
    con el canónico, dejando dos filas indistinguibles para la búsqueda.
    ``casefold`` cubre todo lo que NOCASE considera igual, y algo más: en una
    reserva, de más peca a favor.
    """
    return name.casefold() == FOLLOWS_RADAR_NAME.casefold()


def is_canonical_follows_name(name: str) -> bool:
    """¿Es *exactamente* el nombre canónico del radar sintético?

    Exacto, no ``casefold``: la unicidad de un follows por owner la sostiene el
    ``UNIQUE (owner, name)``, que es **binario**. Si se admitiera una variante
    de caja, dos filas ``frequency='follows'`` del mismo owner pasarían el UNIQUE
    y la watchlist se partiría en dos.
    """
    return name == FOLLOWS_RADAR_NAME


# ---------------------------------------------------------------------------
# Dataclasses de lectura
# ---------------------------------------------------------------------------

@dataclass
class Radar:
    id: str
    name: str
    description: str
    query_frame_json: str
    dataset: str | None
    frequency: str
    channel: str
    enabled: int
    owner: str
    created_at: str
    last_run_at: str | None = None
    created_from_trace_id: str | None = None
    last_error_at: str | None = None
    last_error_message: str | None = None
    failure_count: int = 0
    last_failure_reason: str | None = None
    next_run_after: str | None = None
    needs_attention: int = 0
    permanent_failure_streak: int = 0
    run_lease_token: str | None = None
    run_lease_acquired_at: str | None = None
    run_lease_expires_at: str | None = None
    archived_at: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Radar":
        return cls(**{k: row[k] for k in row.keys()})

    def frame_dict(self) -> dict:
        try:
            return json.loads(self.query_frame_json) or {}
        except (json.JSONDecodeError, TypeError):
            return {}

    @property
    def is_enabled(self) -> bool:
        return bool(self.enabled)


@dataclass
class RadarRun:
    id: str
    radar_id: str
    started_at: str
    finished_at: str | None
    status: str
    total_count: int
    new_count: int
    changed_count: int
    digest_json: str | None
    error: str | None
    soql: str | None
    params_json: str | None
    dataset_id: str | None
    approx_total: int | None
    execution_ms: int | None
    outcome: str | None = None
    reason: str | None = None
    run_kind: str | None = None
    baseline_count: int | None = None
    run_trigger: str | None = None
    coverage_status: str | None = None
    coverage_reason: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "RadarRun":
        return cls(**{k: row[k] for k in row.keys()})

    def digest(self) -> dict:
        try:
            return json.loads(self.digest_json) if self.digest_json else {}
        except (json.JSONDecodeError, TypeError):
            return {}


@dataclass(frozen=True)
class RadarSchedulerState:
    """Última evidencia persistida de una pasada real del scheduler."""

    last_heartbeat_at: str | None = None
    last_pass_started_at: str | None = None
    last_pass_finished_at: str | None = None
    last_pass_status: str | None = None


# ---------------------------------------------------------------------------
# Dataclasses de materialización
#
# Son datos planos: describen *qué* escribir, no *cómo*. El caller los arma en
# memoria mientras todavía puede hacer red; la transacción solo los vuelca.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RunRecord:
    """Una fila de ``radar_runs`` sin ``radar_id`` (lo aporta la operación)."""

    id: str
    started_at: str
    status: str
    total_count: int = 0
    new_count: int = 0
    changed_count: int = 0
    digest_json: str | None = None
    error: str | None = None
    soql: str | None = None
    params_json: str | None = None
    dataset_id: str | None = None
    approx_total: int | None = None
    execution_ms: int | None = None
    outcome: str | None = None
    reason: str | None = None
    run_kind: str | None = None
    baseline_count: int | None = None
    run_trigger: str | None = None
    coverage_status: str | None = None
    coverage_reason: str | None = None
    finished_at: str | None = None


@dataclass(frozen=True)
class SeenRecord:
    """Un resultado a recordar en ``radar_seen_results``."""

    result_key: str
    dataset: str | None
    fields: dict = field(default_factory=dict)
    snapshot_hash: str = ""
    last_snapshot_json: str | None = None
    relevance_label: str | None = None


@dataclass(frozen=True)
class EventRecord:
    """Un movimiento del feed, sin identidad de radar/corrida/dueño.

    A propósito no lleva ``radar_id``, ``run_id``, ``owner`` ni ``radar_name``:
    esos cuatro campos los deriva la operación transaccional del lease ya
    validado y de la fila de ``radars`` leída tras el fence, nunca del caller.
    Si el caller pudiera declararlos, una llamada autorizada sobre el radar A
    podría insertar un evento que se hace pasar por B o por otro dueño.
    """

    kind: str
    ts: str | None = None
    dataset: str | None = None
    result_key: str | None = None
    title: str | None = None
    entity: str | None = None
    value: str | None = None
    status: str | None = None
    prev_status: str | None = None
    prev_value: str | None = None
    urlproceso: str | None = None
    referencia: str | None = None
    # Datos del hecho, congelados al emitirlo (R2B-1B1). ``prev_*`` describe el
    # estado ANTERIOR; estos cuatro describen la fila TAL COMO SE OBSERVÓ ahora.
    modality: str | None = None
    closing_date: str | None = None
    contractor: str | None = None
    signature_date: str | None = None
    # Extracto del objeto/descripción público (TG-P2C). YA normalizado y
    # acotado a ~180 caracteres por radar/keys.py (build_object_excerpt) antes
    # de llegar aquí: radar_events.object_excerpt nunca guarda la descripción
    # completa. Nullable — un evento sin descripción queda en NULL.
    object_excerpt: str | None = None
    # Dónde ocurre el hecho (R1). Mismo criterio que los anteriores: describen,
    # no deciden. El detalle de la oportunidad los muestra desde el snapshot
    # congelado, nunca releyendo radar_seen_results (que es el estado de hoy).
    department: str | None = None
    city: str | None = None


@dataclass(frozen=True)
class FollowsSeenUpdate:
    """El refresco de **una** fila ya seguida, con los eventos que se derivan.

    Los eventos viajan **atados a su clave**, no en una lista aparte: si la fila
    ya no existe (el owner hizo ``unfollow`` mientras corría la red), la clave no
    se actualiza y sus eventos no se insertan, sin que ningún caller tenga que
    acordarse de filtrarlos. La regla vive en la estructura, no en un ``if``.

    ``expected_first_seen_at`` es la **versión de la membresía**: el
    ``first_seen_at`` que tenía la fila en el snapshot que abrió este refresco.
    Sin él, un ``unfollow`` + ``follow`` de la misma clave durante la red daría
    una fila *nueva* con la *misma* clave, el UPDATE acertaría con rowcount 1 y
    el refresco pisaría el ``snapshot_hash=''`` de esa membresía recién nacida
    —matándole la línea base— y emitiría un evento que pertenece a la encarnación
    muerta. La clave identifica *el proceso*; ``first_seen_at`` identifica *esta*
    membresía. Es obligatorio a propósito: un default lo volvería olvidable.

    No lleva ``first_run_id``: un refresco no crea membresía —eso es de
    ``follow()``— y por tanto tampoco fija su origen.
    """

    result_key: str
    expected_first_seen_at: str
    fields: dict = field(default_factory=dict)
    snapshot_hash: str = ""
    last_snapshot_json: str | None = None
    events: tuple[EventRecord, ...] = ()
    is_baseline: bool = False


@dataclass(frozen=True)
class MaterializedFollowsRefresh:
    """Resultado de materializar un refresco de seguidos.

    Los conteos son de lo que **se escribió**, no de lo que se pensaba escribir:
    una clave que un ``unfollow`` descartó a mitad de camino no puede contar como
    cambio. Reportar el conteo construido haría que el scheduler —y mañana el
    notifier— anunciaran un cambio que nunca entró al feed.

    ``lease_lost`` no es error: es un estado de control.
    """

    updated: int = 0
    events: int = 0
    baseline: int = 0
    changed_state: int = 0
    changed_value: int = 0
    lease_lost: bool = False
    not_follows: bool = False


@dataclass(frozen=True)
class MaterializedFollow:
    """Resultado de dar de alta un seguido. ``created=False`` = ya lo seguía."""

    created: bool = False
    not_follows: bool = False


@dataclass(frozen=True)
class FailureRecord:
    """El fallo operativo a imputar al radar junto con su corrida."""

    reason: str
    message: str
    next_run_after: str | None = None
    permanent: bool = False
    pause_after: int = 3


@dataclass(frozen=True)
class MaterializedRun:
    """Resultado de materializar. ``lease_lost`` no es error: es un estado."""

    run_id: str | None = None
    lease_lost: bool = False


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS radars (
    id                     TEXT PRIMARY KEY,
    name                   TEXT NOT NULL,
    description            TEXT NOT NULL DEFAULT '',
    query_frame_json       TEXT NOT NULL,
    dataset                TEXT,
    frequency              TEXT NOT NULL DEFAULT 'manual',
    channel                TEXT NOT NULL DEFAULT 'web',
    enabled                INTEGER NOT NULL DEFAULT 1,
    owner                  TEXT NOT NULL,
    created_at             TEXT NOT NULL,
    last_run_at            TEXT,
    created_from_trace_id  TEXT,
    last_error_at          TEXT,
    last_error_message     TEXT,
    failure_count          INTEGER NOT NULL DEFAULT 0,
    last_failure_reason    TEXT,
    next_run_after         TEXT,
    needs_attention        INTEGER NOT NULL DEFAULT 0,
    permanent_failure_streak INTEGER NOT NULL DEFAULT 0,
    run_lease_token        TEXT,
    run_lease_acquired_at  TEXT,
    run_lease_expires_at   TEXT,
    archived_at            TEXT,
    -- Sigue cubriendo a los archivados a propósito: un radar archivado conserva
    -- su nombre reservado. Si el UNIQUE los excluyera, crear un radar con el
    -- nombre de uno archivado dejaría dos filas homónimas del mismo owner, y
    -- restaurar el viejo sería imposible sin renombrar a mano.
    UNIQUE (owner, name)
);

CREATE TABLE IF NOT EXISTS radar_runs (
    id            TEXT PRIMARY KEY,
    radar_id      TEXT NOT NULL REFERENCES radars(id) ON DELETE CASCADE,
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    status        TEXT NOT NULL,
    total_count   INTEGER NOT NULL DEFAULT 0,
    new_count     INTEGER NOT NULL DEFAULT 0,
    changed_count INTEGER NOT NULL DEFAULT 0,
    digest_json   TEXT,
    error         TEXT,
    soql          TEXT,
    params_json   TEXT,
    dataset_id    TEXT,
    approx_total  INTEGER,
    execution_ms  INTEGER,
    outcome       TEXT,
    reason        TEXT,
    run_kind      TEXT,
    baseline_count INTEGER,
    run_trigger   TEXT,
    coverage_status TEXT,
    coverage_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_radar_runs_radar ON radar_runs(radar_id, started_at DESC);

CREATE TABLE IF NOT EXISTS radar_events (
    id           TEXT PRIMARY KEY,
    radar_id     TEXT NOT NULL REFERENCES radars(id) ON DELETE CASCADE,
    owner        TEXT NOT NULL,
    radar_name   TEXT,
    run_id       TEXT,
    ts           TEXT NOT NULL,
    kind         TEXT NOT NULL,
    result_key   TEXT,
    dataset      TEXT,
    title        TEXT,
    entity       TEXT,
    value        TEXT,
    status       TEXT,
    prev_status  TEXT,
    prev_value   TEXT,
    urlproceso   TEXT,
    referencia   TEXT,
    -- R2B-1B1: datos del evento congelados en su instante (``ts``). Nullable y
    -- sin default: un dataset que no expone el hecho, o una fila heredada, se
    -- quedan en NULL. No se completan leyendo ``radar_seen_results`` —esa tabla
    -- es el estado de HOY y mostraría el valor actual junto al cambio de ayer.
    modality       TEXT,
    closing_date   TEXT,
    contractor     TEXT,
    signature_date TEXT,
    -- TG-P2C: extracto del objeto/descripción público. Aditiva y nullable,
    -- mismo criterio que las cuatro de arriba: sin backfill sobre lo heredado.
    object_excerpt TEXT,
    -- R1: ubicación observada del hecho. Aditivas, nullable y sin backfill,
    -- por la misma razón: el dato de un evento viejo solo existe en el SECOP
    -- de hoy, y copiarlo inventaría un pasado que nadie observó.
    department    TEXT,
    city          TEXT
);
CREATE INDEX IF NOT EXISTS idx_radar_events_owner ON radar_events(owner, ts DESC);

CREATE TABLE IF NOT EXISTS radar_seen_results (
    radar_id           TEXT NOT NULL REFERENCES radars(id) ON DELETE CASCADE,
    result_key         TEXT NOT NULL,
    dataset            TEXT,
    urlproceso         TEXT,
    referencia         TEXT,
    title              TEXT,
    entity             TEXT,
    value              TEXT,
    status             TEXT,
    contractor         TEXT,
    first_seen_at      TEXT NOT NULL,
    last_seen_at       TEXT NOT NULL,
    snapshot_hash      TEXT,
    last_snapshot_json TEXT,
    first_run_id       TEXT,
    last_run_id        TEXT,
    relevance_label    TEXT,
    PRIMARY KEY (radar_id, result_key)
);

-- Generador de event_seq (P1B-2). Vive fuera de radar_events a propósito: una
-- futura poda de radar_events no debe poder afectar la garantía de "nunca
-- reutilizado" de la secuencia. AUTOINCREMENT asegura que el máximo histórico
-- sobrevive aunque se borren filas de esta misma tabla.
CREATE TABLE IF NOT EXISTS radar_event_seq (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS radar_scheduler_state (
    singleton_id          INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    last_heartbeat_at     TEXT,
    last_pass_started_at  TEXT,
    last_pass_finished_at TEXT,
    last_pass_status      TEXT
);
"""


# ---------------------------------------------------------------------------
# SQL de escritura + helpers connection-aware
#
# Cada sentencia vive una sola vez. Los métodos autocommit y la materialización
# transaccional la comparten, así que no pueden divergir en silencio.
# ---------------------------------------------------------------------------

_INSERT_RUN_SQL = """
INSERT INTO radar_runs
   (id, radar_id, started_at, finished_at, status, total_count,
    new_count, changed_count, digest_json, error, soql, params_json,
    dataset_id, approx_total, execution_ms, outcome, reason, run_kind,
    baseline_count, run_trigger, coverage_status, coverage_reason)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_EVENT_SQL = """
INSERT INTO radar_events
   (id, radar_id, owner, radar_name, run_id, ts, kind, result_key,
    dataset, title, entity, value, status, prev_status, prev_value,
    urlproceso, referencia, event_seq,
    modality, closing_date, contractor, signature_date, object_excerpt,
    department, city)
   VALUES (:id, :radar_id, :owner, :radar_name, :run_id, :ts, :kind,
    :result_key, :dataset, :title, :entity, :value, :status,
    :prev_status, :prev_value, :urlproceso, :referencia, :event_seq,
    :modality, :closing_date, :contractor, :signature_date, :object_excerpt,
    :department, :city)
"""

_RESERVE_EVENT_SEQ_SQL = "INSERT INTO radar_event_seq (ts) VALUES (?)"


def _reserve_event_seq(conn: sqlite3.Connection) -> int:
    """Reserva el siguiente event_seq sobre esta conexión, en esta transacción.

    ``cursor.lastrowid`` tras un INSERT en una tabla AUTOINCREMENT es un valor
    de la conexión, no una consulta aparte — no hay ventana de carrera entre
    reservar y leerlo. No se usa MAX(event_seq)+1 (se calcularía en memoria y
    dos conexiones concurrentes podrían leer el mismo máximo) ni rowid de
    ``radar_events`` (no sobrevive a un futuro borrado/retención de esa tabla).
    """
    cur = conn.execute(_RESERVE_EVENT_SEQ_SQL, (_now(),))
    return cur.lastrowid

_UPSERT_SEEN_SQL = """
INSERT INTO radar_seen_results
   (radar_id, result_key, dataset, urlproceso, referencia, title,
    entity, value, status, contractor, first_seen_at, last_seen_at,
    snapshot_hash, last_snapshot_json, first_run_id, last_run_id,
    relevance_label)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
   ON CONFLICT(radar_id, result_key) DO UPDATE SET
       last_seen_at       = excluded.last_seen_at,
       last_run_id        = excluded.last_run_id,
       snapshot_hash      = excluded.snapshot_hash,
       last_snapshot_json = excluded.last_snapshot_json,
       status             = excluded.status,
       value              = excluded.value,
       title              = excluded.title,
       entity             = excluded.entity,
       urlproceso         = excluded.urlproceso,
       referencia         = excluded.referencia,
       contractor         = excluded.contractor
"""

# Alta de membresía: INSERT que **no pisa** una fila existente. Re-seguir algo ya
# seguido no debe reescribir snapshot_hash a '' —eso borraría la línea base y
# haría que el refresco siguiente tratara un cambio real como baseline, es decir,
# lo silenciaría—, ni mover first_seen_at, ni emitir otro evento 'followed'.
# DO NOTHING deja la fila tal cual y delata por rowcount si hubo alta.
_INSERT_SEEN_IF_ABSENT_SQL = """
INSERT INTO radar_seen_results
   (radar_id, result_key, dataset, urlproceso, referencia, title,
    entity, value, status, contractor, first_seen_at, last_seen_at,
    snapshot_hash, last_snapshot_json, first_run_id, last_run_id,
    relevance_label)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
   ON CONFLICT(radar_id, result_key) DO NOTHING
"""

# Refresco de un seguido: UPDATE, nunca UPSERT. La membresía la crea ``follow``;
# un refresco que pudiera insertar resucitaría lo que ``unfollow`` borró mientras
# había red — el usuario dejaría de seguir un proceso y el sistema se lo
# devolvería en la pasada siguiente. Si la fila ya no está, rowcount 0 y listo.
#
# El `first_seen_at` del WHERE es un **fence de versión**, no un filtro de más:
# (radar_id, result_key) identifica el proceso, pero no *esta* membresía. Un
# unfollow+follow de la misma clave durante la red produce una fila nueva con la
# misma clave, y sin este predicado el refresco la actualizaría —borrándole el
# snapshot_hash='' que la deja pendiente de baseline— y emitiría un evento que
# describe una membresía que ya no existe. `first_seen_at` es inmutable mientras
# la membresía vive (ni este UPDATE ni el INSERT ... DO NOTHING lo tocan), así
# que sirve de versión sin añadir una columna.
#
# No toca first_seen_at/first_run_id/dataset: refrescar no redefine identidad.
_UPDATE_SEEN_SQL = """
UPDATE radar_seen_results
   SET last_seen_at       = ?,
       last_run_id        = ?,
       snapshot_hash      = ?,
       last_snapshot_json = ?,
       status             = ?,
       value              = ?,
       title              = ?,
       entity             = ?,
       urlproceso         = ?,
       referencia         = ?,
       contractor         = ?
 WHERE radar_id = ?
   AND result_key = ?
   AND first_seen_at = ?
"""

# Éxito: limpia error y ambos contadores. Deliberadamente NO toca dos columnas
# que no son estado técnico (P1B-Preflight 3):
#
# - ``enabled``: reactivar es decisión del operador, no consecuencia de que
#   SECOP volviera a responder. Un radar pausado sigue pausado.
# - ``needs_attention``: es una alerta *humana*. Una corrida OK prueba que la
#   consulta se pudo ejecutar, no que alguien entendiera ni corrigiera la causa
#   que exigía revisión. Solo una acción explícita de reconocimiento puede
#   bajarla; esa acción todavía no existe (deuda de P1B).
#
# Los dos ejes son independientes: needs_attention se preserva igual con
# enabled=0 que con enabled=1, sin CASE ni condicional de por medio.
_MARK_SUCCESS_SQL = """
UPDATE radars
   SET last_run_at = ?, last_error_at = NULL, last_error_message = NULL,
       failure_count = 0, last_failure_reason = NULL, next_run_after = NULL,
       permanent_failure_streak = 0
 WHERE id = ?
"""

# Toda la decisión de pausa ocurre aquí dentro: sin SELECT previo, la racha se
# lee y se decide en la misma sentencia, atómica frente a corridas paralelas.
_RECORD_FAILURE_SQL = """
UPDATE radars
   SET last_error_at = ?, last_error_message = ?,
       failure_count = failure_count + 1,
       permanent_failure_streak = CASE
           WHEN ? THEN permanent_failure_streak + 1
           ELSE 0
       END,
       last_failure_reason = ?,
       next_run_after = ?,
       needs_attention = CASE
           WHEN ? AND permanent_failure_streak + 1 >= ?
           THEN 1 ELSE needs_attention
       END,
       enabled = CASE
           WHEN ? AND permanent_failure_streak + 1 >= ?
           THEN 0 ELSE enabled
       END
 WHERE id = ?
"""


def _insert_run(conn: sqlite3.Connection, radar_id: str, run: RunRecord) -> str:
    conn.execute(
        _INSERT_RUN_SQL,
        (run.id, radar_id, run.started_at, run.finished_at or _now(), run.status,
         run.total_count, run.new_count, run.changed_count, run.digest_json,
         run.error, run.soql, run.params_json, run.dataset_id, run.approx_total,
         run.execution_ms, run.outcome, run.reason, run.run_kind,
         run.baseline_count, run.run_trigger, run.coverage_status,
         run.coverage_reason),
    )
    return run.id


def _insert_event_dicts(conn: sqlite3.Connection, events: list[dict]) -> None:
    """Inserta eventos ya armados como dicts completos (todas las columnas).

    Usada por el registro genérico (``record_events``), donde el caller es dueño
    de todos los campos —incluida la identidad— porque no hay transacción que
    pueda derivarla. ``follows.py`` fue su último consumidor y dejó de serlo en
    P1B-Preflight 2: sus dos rutas van por ``materialize_follow_if_absent`` y
    ``materialize_follows_refresh``, que derivan la identidad dentro de su propia
    transacción. Hoy ``record_events`` no tiene caller productivo.

    La materialización de una corrida tampoco pasa por aquí: usa
    ``_insert_events`` (abajo), que no acepta identidad del caller.

    ``event_seq`` (P1B-2) tampoco lo aporta el caller: cualquier clave
    ``"event_seq"`` presente en ``e`` se ignora. Es el único punto de
    convergencia real de las tres rutas productivas (``materialize_*``), así
    que reservar aquí, uno a uno, es lo que garantiza secuencias distintas y
    crecientes sin depender de que cada caller recuerde hacerlo.
    """
    if not events:
        return
    for e in events:
        event_seq = _reserve_event_seq(conn)
        conn.execute(
            _INSERT_EVENT_SQL,
            {
                "id": _new_id(),
                "radar_id": e.get("radar_id"),
                "owner": e.get("owner"),
                "radar_name": e.get("radar_name"),
                "run_id": e.get("run_id"),
                "ts": e.get("ts") or _now(),
                "kind": e.get("kind"),
                "result_key": e.get("result_key"),
                "dataset": e.get("dataset"),
                "title": e.get("title"),
                "entity": e.get("entity"),
                "value": e.get("value"),
                "status": e.get("status"),
                "prev_status": e.get("prev_status"),
                "prev_value": e.get("prev_value"),
                "urlproceso": e.get("urlproceso"),
                "referencia": e.get("referencia"),
                "event_seq": event_seq,
                "modality": e.get("modality"),
                "closing_date": e.get("closing_date"),
                "contractor": e.get("contractor"),
                "signature_date": e.get("signature_date"),
                "object_excerpt": e.get("object_excerpt"),
                "department": e.get("department"),
                "city": e.get("city"),
            },
        )


def _insert_events(
    conn: sqlite3.Connection,
    *,
    radar_id: str,
    owner: str,
    radar_name: str | None,
    run_id: str | None,
    events: list[EventRecord],
) -> None:
    """Inserta los eventos de una corrida materializada.

    La identidad —``radar_id``, ``owner``, ``radar_name``, ``run_id``— viene
    siempre de estos argumentos, nunca de ``events``: son los únicos datos que
    la transacción ya verificó (``radar_id`` contra el lease renovado,
    ``owner``/``radar_name`` contra la fila de ``radars`` leída después del
    fence, ``run_id`` contra el run recién insertado en esta misma
    transacción). ``EventRecord`` ni siquiera expone esos campos.
    """
    if not events:
        return
    _insert_event_dicts(conn, [
        {
            "radar_id": radar_id, "owner": owner, "radar_name": radar_name,
            "run_id": run_id, "ts": e.ts, "kind": e.kind,
            "result_key": e.result_key, "dataset": e.dataset, "title": e.title,
            "entity": e.entity, "value": e.value, "status": e.status,
            "prev_status": e.prev_status, "prev_value": e.prev_value,
            "urlproceso": e.urlproceso, "referencia": e.referencia,
            "modality": e.modality, "closing_date": e.closing_date,
            "contractor": e.contractor, "signature_date": e.signature_date,
            "object_excerpt": e.object_excerpt,
            "department": e.department, "city": e.city,
        }
        for e in events
    ])


def _upsert_seen(
    conn: sqlite3.Connection, radar_id: str, rec: SeenRecord, run_id: str, ts: str,
) -> None:
    f = rec.fields
    conn.execute(
        _UPSERT_SEEN_SQL,
        (radar_id, rec.result_key, rec.dataset, f.get("urlproceso", ""),
         f.get("referencia", ""), f.get("title", ""), f.get("entity", ""),
         f.get("value", ""), f.get("status", ""), f.get("contractor", ""),
         ts, ts, rec.snapshot_hash, rec.last_snapshot_json, run_id, run_id,
         rec.relevance_label),
    )


def _insert_seen_if_absent(
    conn: sqlite3.Connection,
    radar_id: str,
    rec: SeenRecord,
    run_id: str | None,
    ts: str,
) -> int:
    """Da de alta la membresía si no existía. Devuelve 1 si insertó, 0 si ya estaba."""
    f = rec.fields
    cur = conn.execute(
        _INSERT_SEEN_IF_ABSENT_SQL,
        (radar_id, rec.result_key, rec.dataset, f.get("urlproceso", ""),
         f.get("referencia", ""), f.get("title", ""), f.get("entity", ""),
         f.get("value", ""), f.get("status", ""), f.get("contractor", ""),
         ts, ts, rec.snapshot_hash, rec.last_snapshot_json, run_id, run_id,
         rec.relevance_label),
    )
    return cur.rowcount


def _update_seen(
    conn: sqlite3.Connection,
    radar_id: str,
    upd: FollowsSeenUpdate,
    run_id: str | None,
    ts: str,
) -> int:
    """Refresca una fila ya seguida.

    Devuelve filas afectadas: 0 si la membresía desapareció (``unfollow``) **o
    si fue reemplazada por otra encarnación** (``unfollow`` + ``follow`` de la
    misma clave mientras había red).
    """
    f = upd.fields
    cur = conn.execute(
        _UPDATE_SEEN_SQL,
        (ts, run_id, upd.snapshot_hash, upd.last_snapshot_json,
         f.get("status", ""), f.get("value", ""), f.get("title", ""),
         f.get("entity", ""), f.get("urlproceso", ""), f.get("referencia", ""),
         f.get("contractor", ""), radar_id, upd.result_key,
         upd.expected_first_seen_at),
    )
    return cur.rowcount


def _read_canonical_follows(
    conn: sqlite3.Connection, radar_id: str
) -> sqlite3.Row | None:
    """La fila del radar sintético si —y solo si— sigue siendo la watchlist.

    Se lee **dentro** de la transacción: entre que el caller resolvió el radar y
    esta escritura, la fila pudo archivarse, cambiar de ``frequency`` o perder el
    nombre canónico. Exige las cuatro condiciones porque las cuatro sostienen el
    invariante: estar vigente, existir, ser de tipo follows y llevar el nombre que
    el ``UNIQUE (owner, name)`` protege —sin lo último, la unicidad de la
    watchlist no está garantizada y escribir en ella sería adivinar.

    El filtro de archivado va aquí y no solo en el lease porque este guardián
    protege también a ``materialize_follow_if_absent``, que **no toma lease**: un
    "seguir proceso" manual sobre una watchlist archivada escribiría en ella sin
    que nada lo detuviera, y le diría al usuario que sigue algo que nadie vigila.

    ``None`` = no es una watchlist válida; el caller aborta sin escribir.
    """
    row = conn.execute(
        "SELECT owner, name, frequency FROM radars "
        "WHERE id = ? AND archived_at IS NULL",
        (radar_id,),
    ).fetchone()
    if row is None:
        return None
    if row["frequency"] != FOLLOWS_FREQUENCY:
        return None
    if not is_canonical_follows_name(row["name"]):
        return None
    return row


def _mark_success(conn: sqlite3.Connection, radar_id: str, ts: str) -> None:
    conn.execute(_MARK_SUCCESS_SQL, (ts, radar_id))


def _apply_failure(
    conn: sqlite3.Connection, radar_id: str, failure: FailureRecord, ts: str,
) -> None:
    permanent_flag = 1 if failure.permanent else 0
    conn.execute(
        _RECORD_FAILURE_SQL,
        (ts, failure.message,
         permanent_flag,
         failure.reason, failure.next_run_after,
         permanent_flag, failure.pause_after,
         permanent_flag, failure.pause_after,
         radar_id),
    )


# Puntos de inyección de fallo. Existen para probar que cada paso de la
# transacción revierte por completo; en runtime el hook siempre es None.
PHASE_AFTER_BEGIN = "after_begin"
PHASE_AFTER_LEASE_FENCE = "after_lease_fence"
PHASE_AFTER_RUN_INSERT = "after_run_insert"
PHASE_AFTER_SEEN_UPSERTS = "after_seen_upserts"
PHASE_AFTER_SEEN_UPDATES = "after_seen_updates"
PHASE_AFTER_MEMBERSHIP_INSERT = "after_membership_insert"
PHASE_AFTER_EVENT_INSERTS = "after_event_inserts"
PHASE_AFTER_RADAR_UPDATE = "after_radar_update"
PHASE_AFTER_FAILURE_UPDATE = "after_failure_update"
PHASE_BEFORE_COMMIT = "before_commit"

_FaultHook = Callable[[str], None]


def _fire(hook: _FaultHook | None, phase: str) -> None:
    if hook is not None:
        hook(phase)


class RadarStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def _ensure_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
            self._ensure_columns(conn)
        finally:
            conn.close()

    def _ensure_columns(self, conn: sqlite3.Connection) -> None:
        """Migraciones aditivas para bases ya creadas."""
        radar_cols = {row["name"] for row in conn.execute("PRAGMA table_info(radars)")}
        for name, ddl in {
            "failure_count": "INTEGER NOT NULL DEFAULT 0",
            "last_failure_reason": "TEXT",
            "next_run_after": "TEXT",
            "needs_attention": "INTEGER NOT NULL DEFAULT 0",
            "permanent_failure_streak": "INTEGER NOT NULL DEFAULT 0",
            "run_lease_token": "TEXT",
            "run_lease_acquired_at": "TEXT",
            "run_lease_expires_at": "TEXT",
            # Aditiva y sin default: las filas existentes quedan con NULL, que es
            # exactamente "no archivado". Ninguna base heredada cambia de
            # comportamiento al migrar.
            "archived_at": "TEXT",
        }.items():
            if name not in radar_cols:
                conn.execute(f"ALTER TABLE radars ADD COLUMN {name} {ddl}")

        run_cols = {row["name"] for row in conn.execute("PRAGMA table_info(radar_runs)")}
        for name, ddl in {
            "outcome": "TEXT",
            "reason": "TEXT",
            # Sin defaults: no se inventa semántica para corridas heredadas.
            "run_kind": "TEXT",
            "baseline_count": "INTEGER",
            "run_trigger": "TEXT",
            "coverage_status": "TEXT",
            "coverage_reason": "TEXT",
        }.items():
            if name not in run_cols:
                conn.execute(f"ALTER TABLE radar_runs ADD COLUMN {name} {ddl}")

        # Aditiva y sin default: las filas heredadas quedan con event_seq=NULL
        # (no se hace backfill — notificarlas sería un consentimiento retroactivo
        # que nadie dio). SQLite trata cada NULL como distinto en un índice
        # UNIQUE, así que múltiples filas heredadas en NULL no chocan entre sí.
        event_cols = {row["name"] for row in conn.execute("PRAGMA table_info(radar_events)")}
        if "event_seq" not in event_cols:
            conn.execute("ALTER TABLE radar_events ADD COLUMN event_seq INTEGER")
        # R2B-1B1: aditivas y sin default. Las filas heredadas quedan en NULL y
        # NO se rellenan: el dato de un evento viejo solo existe en el SECOP de
        # hoy, y copiarlo ahí inventaría un pasado que nadie observó entonces.
        for name in (
            "modality", "closing_date", "contractor", "signature_date",
            # TG-P2C: mismo patrón aditivo/nullable que las cuatro anteriores.
            "object_excerpt",
            # R1: ubicación observada, mismo patrón aditivo/nullable.
            "department", "city",
        ):
            if name not in event_cols:
                conn.execute(f"ALTER TABLE radar_events ADD COLUMN {name} TEXT")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_radar_events_seq "
            "ON radar_events(event_seq)"
        )

    # -- radars ------------------------------------------------------------

    def create_radar(
        self,
        *,
        name: str,
        owner: str,
        query_frame_json: str,
        dataset: str | None = None,
        description: str = "",
        frequency: str = "manual",
        channel: str = "web",
        created_from_trace_id: str | None = None,
    ) -> str:
        # El nombre del radar sintético está reservado: un radar de búsqueda que
        # lo use sería resuelto como la watchlist del owner y recibiría sus
        # ``radar_seen_results`` —marcando como "ya visto" lo que esa búsqueda
        # nunca encontró, y silenciando su novedad para siempre.
        if frequency != FOLLOWS_FREQUENCY and _is_reserved_name(name):
            raise RadarReservedNameError(
                f"El nombre '{name}' está reservado para la lista de procesos "
                f"seguidos. Elige otro nombre para el radar."
            )
        # Y al revés: un follows con nombre alternativo pasaría el UNIQUE y
        # crearía un segundo follows para el mismo owner. La unicidad de la
        # watchlist se apoya en ese índice, así que el nombre canónico es parte
        # del invariante, no decoración.
        if frequency == FOLLOWS_FREQUENCY and not is_canonical_follows_name(name):
            raise RadarReservedNameError(
                f"El radar de procesos seguidos debe llamarse "
                f"'{FOLLOWS_RADAR_NAME}', no '{name}'."
            )
        radar_id = _new_id()
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO radars
                   (id, name, description, query_frame_json, dataset, frequency,
                    channel, enabled, owner, created_at, created_from_trace_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
                (radar_id, name, description, query_frame_json, dataset, frequency,
                 channel, owner, _now(), created_from_trace_id),
            )
        except sqlite3.IntegrityError as exc:
            raise RadarExistsError(
                f"Ya existe un radar llamado '{name}'."
            ) from exc
        finally:
            conn.close()
        return radar_id

    # -- visibilidad ------------------------------------------------------
    #
    # Un radar archivado sigue en la tabla, con su historial intacto, pero no
    # existe para nadie que pregunte por radares: los lectores lo filtran y el
    # lease se lo niega (ver lease.py). El filtro se repite en cada lector, y no
    # se centraliza en una vista, porque cada uno tiene su propio WHERE y una
    # vista intermedia escondería el criterio justo donde hay que verlo.

    def get_radar(self, radar_id: str) -> Radar | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM radars WHERE id = ? AND archived_at IS NULL",
                (radar_id,),
            ).fetchone()
        finally:
            conn.close()
        return Radar.from_row(row) if row else None

    def get_radar_by_name(self, owner: str, name: str) -> Radar | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM radars WHERE owner = ? AND name = ? COLLATE NOCASE "
                "AND archived_at IS NULL",
                (owner, name),
            ).fetchone()
        finally:
            conn.close()
        return Radar.from_row(row) if row else None

    def list_radars_by_frequency(
        self, owner: str, frequency: str, *, limit: int = 2
    ) -> list[Radar]:
        """Radares de ese tipo del owner, hasta ``limit``.

        Devuelve una **lista**, no "el primero": para el radar sintético, que
        haya más de una fila es corrupción, y un ``LIMIT 1`` la escondería
        eligiendo una al azar —justo el fallo silencioso que la watchlist no
        puede permitirse—. El caller decide qué hacer con la cardinalidad; con
        ``limit=2`` basta para distinguir "una" de "más de una" sin traerlas
        todas.
        """
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radars WHERE owner = ? AND frequency = ? "
                "AND archived_at IS NULL "
                "ORDER BY created_at, rowid LIMIT ?",
                (owner, frequency, limit),
            ).fetchall()
        finally:
            conn.close()
        return [Radar.from_row(r) for r in rows]

    def list_radars(self, owner: str) -> list[Radar]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radars WHERE owner = ? AND archived_at IS NULL "
                "ORDER BY created_at DESC",
                (owner,),
            ).fetchall()
        finally:
            conn.close()
        return [Radar.from_row(r) for r in rows]

    def list_all_enabled_radars(self) -> list[Radar]:
        """Todos los radares activos de todos los dueños (para el scheduler)."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radars WHERE enabled = 1 AND archived_at IS NULL "
                "AND (next_run_after IS NULL OR next_run_after <= ?) "
                "ORDER BY owner, created_at",
                (_now(),),
            ).fetchall()
        finally:
            conn.close()
        return [Radar.from_row(r) for r in rows]

    # -- scheduler heartbeat ---------------------------------------------

    def start_scheduler_pass(self, ts: str | None = None) -> None:
        """Registra el inicio de una pasada, incluso si no hay radares due.

        ``last_pass_finished_at=NULL`` + ``last_pass_status='running'`` deja
        evidencia observable si el proceso muere antes de finalizar.
        """
        heartbeat_at = ts or _now()
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO radar_scheduler_state
                   (singleton_id, last_heartbeat_at, last_pass_started_at,
                    last_pass_finished_at, last_pass_status)
                   VALUES (1, ?, ?, NULL, 'running')
                   ON CONFLICT(singleton_id) DO UPDATE SET
                       last_heartbeat_at = excluded.last_heartbeat_at,
                       last_pass_started_at = excluded.last_pass_started_at,
                       last_pass_finished_at = NULL,
                       last_pass_status = 'running'""",
                (heartbeat_at, heartbeat_at),
            )
        finally:
            conn.close()

    def finish_scheduler_pass(self, status: str, ts: str | None = None) -> None:
        """Registra la finalización observable de la pasada actual."""
        if status not in {"completed", "completed_with_errors"}:
            raise ValueError(f"Estado final de scheduler inválido: {status!r}")
        finished_at = ts or _now()
        conn = self._connect()
        try:
            conn.execute(
                """UPDATE radar_scheduler_state
                      SET last_heartbeat_at = ?, last_pass_finished_at = ?,
                          last_pass_status = ?
                    WHERE singleton_id = 1""",
                (finished_at, finished_at, status),
            )
        finally:
            conn.close()

    def get_scheduler_state(self) -> RadarSchedulerState | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT last_heartbeat_at, last_pass_started_at, "
                "last_pass_finished_at, last_pass_status "
                "FROM radar_scheduler_state WHERE singleton_id = 1"
            ).fetchone()
        finally:
            conn.close()
        return RadarSchedulerState(**dict(row)) if row else None

    def set_enabled(self, radar_id: str, enabled: bool) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE radars SET enabled = ? WHERE id = ?",
                (1 if enabled else 0, radar_id),
            )
        finally:
            conn.close()

    def update_last_run(self, radar_id: str, ts: str | None = None) -> None:
        """Marca éxito fuera de una transacción de corrida.

        Misma sentencia que usa ``materialize_successful_run`` (``_mark_success``),
        para que no existan dos definiciones de "qué significa una corrida OK":
        reinicia el estado técnico y deja intactos ``enabled`` y
        ``needs_attention``.
        """
        conn = self._connect()
        try:
            _mark_success(conn, radar_id, ts or _now())
        finally:
            conn.close()

    def record_error(self, radar_id: str, message: str, ts: str | None = None) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE radars SET last_error_at = ?, last_error_message = ? WHERE id = ?",
                (ts or _now(), message, radar_id),
            )
        finally:
            conn.close()

    def record_failure(
        self,
        radar_id: str,
        *,
        reason: str,
        message: str,
        next_run_after: str | None = None,
        permanent: bool = False,
        pause_after: int = 3,
        ts: str | None = None,
    ) -> int:
        """Registra fallo operativo y devuelve el nuevo ``failure_count``.

        ``failure_count`` cuenta fallos consecutivos de cualquier clase desde
        el último éxito; ``permanent_failure_streak`` cuenta solo la racha de
        fallos *permanentes* consecutivos (un transitorio la reinicia a 0). La
        pausa automática (``needs_attention``/``enabled=0``) depende
        exclusivamente de esa racha, nunca de ``failure_count`` general — de
        lo contrario, transitorio+transitorio+permanente pausaría el radar
        con un solo fallo permanente real.

        Toda la decisión ocurre en un único UPDATE (sin SELECT previo) para
        que sea atómica respecto de corridas concurrentes.
        """
        failure = FailureRecord(
            reason=reason, message=message, next_run_after=next_run_after,
            permanent=permanent, pause_after=pause_after,
        )
        conn = self._connect()
        try:
            _apply_failure(conn, radar_id, failure, ts or _now())
            row = conn.execute(
                "SELECT failure_count FROM radars WHERE id = ?", (radar_id,)
            ).fetchone()
        finally:
            conn.close()
        return int(row["failure_count"]) if row else 0

    # -- run lease (exclusión mutua interproceso) --------------------------
    #
    # Política aparte de enabled/needs_attention/next_run_after: esos deciden
    # *si toca* correr; el lease decide *quién* corre. Ver lease.py.

    def try_acquire_run_lease(
        self,
        radar_id: str,
        *,
        now: datetime | None = None,
        lease_duration: timedelta | None = None,
    ) -> LeaseClaim:
        conn = self._connect()
        try:
            return lease.try_acquire(
                conn, radar_id, now=now, lease_duration=lease_duration
            )
        finally:
            conn.close()

    def renew_run_lease(
        self,
        radar_id: str,
        token: str,
        *,
        now: datetime | None = None,
        lease_duration: timedelta | None = None,
    ) -> bool:
        conn = self._connect()
        try:
            return lease.renew(
                conn, radar_id, token, now=now, lease_duration=lease_duration
            )
        finally:
            conn.close()

    def release_run_lease(self, radar_id: str, token: str) -> bool:
        conn = self._connect()
        try:
            return lease.release(conn, radar_id, token)
        finally:
            conn.close()

    def is_run_lease_owned(
        self, radar_id: str, token: str, *, now: datetime | None = None
    ) -> bool:
        conn = self._connect()
        try:
            return lease.is_owned(conn, radar_id, token, now=now)
        finally:
            conn.close()

    def archive_radar(self, radar_id: str, ts: str | None = None) -> None:
        """Retira el radar de circulación sin destruir nada.

        ``radar_runs``, ``radar_events`` y ``radar_seen_results`` cuelgan de
        ``radars`` con ``ON DELETE CASCADE``: borrar la fila se llevaba por
        delante el historial completo —cada corrida, cada movimiento anunciado y
        toda la memoria de lo visto—. Ese historial es el activo del producto y
        la única forma de responder "¿por qué me avisaron de esto?".

        Idempotente en la fecha vía ``COALESCE``: archivar dos veces conserva la
        original, que es la que dice desde cuándo dejó de operar. La **revocación
        del lease, en cambio, es incondicional** —sin ``WHERE archived_at IS
        NULL``—, para que una segunda llamada repare una fila ya archivada que
        arrastre un token residual (heredada de una versión anterior, de una
        migración o de SQL a mano). Con el filtro puesto, ese estado roto no
        habría forma de arreglarlo por esta puerta.

        **Revoca en la misma sentencia**, no en un UPDATE aparte: entre "marqué
        archivado" y "solté el lease" cabe la renovación del ejecutor en vuelo,
        que volvería a extenderlo. Archivar y quedarse sin dueño son un solo
        hecho, así que son una sola escritura. El dueño viejo se entera cuando su
        ``renew`` afecta 0 filas —por token nulo y por archivado, dos candados
        independientes.

        Archivar es ortogonal a ``needs_attention`` y a ``enabled``: no reconoce
        la alerta ni cambia lo que el usuario pidió. Si se restaura, el radar
        vuelve tal como estaba, alerta incluida.
        """
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE radars "
                "   SET archived_at = COALESCE(archived_at, ?), "
                "       run_lease_token = NULL, run_lease_acquired_at = NULL, "
                "       run_lease_expires_at = NULL "
                " WHERE id = ?",
                (ts or _now(), radar_id),
            )
        finally:
            conn.close()

    def restore_radar(self, radar_id: str) -> None:
        """Devuelve a circulación la misma fila, con su historial intacto.

        Limpia el lease aunque ``archive_radar`` ya lo haga: restaurar es la
        puerta por la que un radar vuelve a ser ejecutable, y no puede confiar en
        cómo llegó a estar archivado. Una fila archivada por una versión anterior,
        por una migración o por SQL a mano puede traer un ``run_lease_token``
        residual; restaurarla así devolvería un radar con un dueño que ya no
        existe, y ningún ejecutor podría tomarlo hasta que el lease venciera solo.

        Por eso mismo **solo actúa sobre filas archivadas**. Sin
        ``archived_at IS NOT NULL``, restaurar un radar vivo —o repetir la llamada
        sobre uno ya restaurado— le arrancaría el lease a un ejecutor que está
        corriendo ahora mismo, y un segundo ejecutor podría tomarlo en paralelo:
        justo la exclusión mutua que el lease existe para sostener. Limpiar un
        lease solo es seguro cuando su dueño ya no puede estar vivo, y eso solo lo
        garantiza el archivado.
        """
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE radars "
                "   SET archived_at = NULL, run_lease_token = NULL, "
                "       run_lease_acquired_at = NULL, run_lease_expires_at = NULL "
                " WHERE id = ? AND archived_at IS NOT NULL",
                (radar_id,),
            )
        finally:
            conn.close()

    def delete_radar(self, radar_id: str) -> None:
        """Archiva. **No borra** — el nombre se conserva por compatibilidad.

        Los callers piden "que el radar desaparezca", y archivar cumple eso: deja
        de existir para todo lector y no puede correr. Lo que ya no hace es
        destruir el historial. Se conserva el nombre en vez de migrar los callers
        porque la intención que expresan sigue siendo válida; lo que estaba mal
        era la implementación.
        """
        self.archive_radar(radar_id)

    # -- runs --------------------------------------------------------------

    def record_run(
        self,
        *,
        radar_id: str,
        started_at: str,
        status: str,
        total_count: int = 0,
        new_count: int = 0,
        changed_count: int = 0,
        digest_json: str | None = None,
        error: str | None = None,
        soql: str | None = None,
        params_json: str | None = None,
        dataset_id: str | None = None,
        approx_total: int | None = None,
        execution_ms: int | None = None,
        outcome: str | None = None,
        reason: str | None = None,
        run_kind: str | None = None,
        baseline_count: int | None = None,
        run_trigger: str | None = None,
        coverage_status: str | None = None,
        coverage_reason: str | None = None,
        finished_at: str | None = None,
    ) -> str:
        run = RunRecord(
            id=_new_id(), started_at=started_at, status=status,
            total_count=total_count, new_count=new_count, changed_count=changed_count,
            digest_json=digest_json, error=error, soql=soql, params_json=params_json,
            dataset_id=dataset_id, approx_total=approx_total,
            execution_ms=execution_ms, outcome=outcome, reason=reason,
            run_kind=run_kind, baseline_count=baseline_count,
            run_trigger=run_trigger, coverage_status=coverage_status,
            coverage_reason=coverage_reason,
            finished_at=finished_at,
        )
        conn = self._connect()
        try:
            return _insert_run(conn, radar_id, run)
        finally:
            conn.close()

    def get_runs(self, radar_id: str, limit: int = 50) -> list[RadarRun]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radar_runs WHERE radar_id = ? "
                "ORDER BY started_at DESC LIMIT ?",
                (radar_id, limit),
            ).fetchall()
        finally:
            conn.close()
        return [RadarRun.from_row(r) for r in rows]

    def has_successful_run(self, radar_id: str) -> bool:
        """Si ya se materializó una corrida OK (incluida baseline vacía)."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM radar_runs WHERE radar_id = ? AND status = 'ok' LIMIT 1",
                (radar_id,),
            ).fetchone()
        finally:
            conn.close()
        return row is not None

    # -- events (feed) -----------------------------------------------------

    def record_events(self, events: list[dict]) -> None:
        """Inserta movimientos en el feed. Cada dict trae las columnas de
        ``radar_events`` (sin ``id``, que se genera). No-op si la lista vacía.

        Genérico y de un solo hecho, sin transacción: el caller es dueño de la
        identidad de cada evento porque no hay una transacción que la derive.
        Ninguna ruta de follows pasa por aquí — ``follow()`` y
        ``refresh_follows()`` van por ``materialize_follow_if_absent`` y
        ``materialize_follows_refresh``, que derivan la identidad dentro de su
        propia transacción. Tampoco la materialización de una corrida — ver
        ``materialize_successful_run``.
        """
        if not events:
            return
        conn = self._connect()
        try:
            _insert_event_dicts(conn, events)
        finally:
            conn.close()

    def get_event(self, owner: str, event_id: str) -> dict | None:
        """Un movimiento concreto del feed del owner, o ``None``.

        Lectura pura sobre la tabla append-only: abrir el detalle de un evento
        no lo modifica ni lo marca. El ``owner`` es parte del predicado, no un
        filtro que el caller pueda olvidar — un id de evento adivinado no puede
        devolver el movimiento de otra persona.
        """
        if not event_id:
            return None
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM radar_events WHERE owner = ? AND id = ?",
                (owner, event_id),
            ).fetchone()
        finally:
            conn.close()
        return dict(row) if row is not None else None

    def get_feed(self, owner: str, limit: int = 60) -> list[dict]:
        """Feed cronológico de movimientos de todos los radares del owner."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radar_events WHERE owner = ? "
                "ORDER BY ts DESC, rowid DESC LIMIT ?",
                (owner, limit),
            ).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    # -- seen results ------------------------------------------------------

    def delete_seen(self, radar_id: str, result_key: str) -> None:
        """Borra un resultado seguido (para 'dejar de seguir')."""
        conn = self._connect()
        try:
            conn.execute(
                "DELETE FROM radar_seen_results WHERE radar_id = ? AND result_key = ?",
                (radar_id, result_key),
            )
        finally:
            conn.close()

    def get_seen(self, radar_id: str) -> dict[str, dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radar_seen_results WHERE radar_id = ?",
                (radar_id,),
            ).fetchall()
        finally:
            conn.close()
        return {r["result_key"]: dict(r) for r in rows}

    def upsert_seen(
        self,
        *,
        radar_id: str,
        result_key: str,
        dataset: str | None,
        fields: dict,
        snapshot_hash: str,
        run_id: str,
        last_snapshot_json: str | None = None,
        seen_at: str | None = None,
        relevance_label: str | None = None,
    ) -> None:
        """Inserta el resultado o actualiza su last_seen/snapshot.

        En INSERT fija first_seen_at/first_run_id; en UPDATE solo toca
        last_seen_at/last_run_id/snapshot y campos mutables (status/value).
        """
        rec = SeenRecord(
            result_key=result_key, dataset=dataset, fields=fields,
            snapshot_hash=snapshot_hash, last_snapshot_json=last_snapshot_json,
            relevance_label=relevance_label,
        )
        conn = self._connect()
        try:
            _upsert_seen(conn, radar_id, rec, run_id, seen_at or _now())
        finally:
            conn.close()

    # -- materialización transaccional de una corrida ----------------------
    #
    # El lease se revalida como primera sentencia DML de la transacción, no
    # antes de abrirla: entre una comprobación externa y las escrituras habría
    # una ventana en la que el lease vence y otro ejecutor toma el radar.
    # Dentro de BEGIN IMMEDIATE ya tenemos el lock de escritura, así que si el
    # fence pasa, nadie más puede escribir hasta el COMMIT.

    def materialize_successful_run(
        self,
        *,
        radar_id: str,
        token: str,
        run: RunRecord,
        seen: list[SeenRecord],
        events: list[EventRecord],
        now: datetime | None = None,
        lease_duration: timedelta | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> MaterializedRun:
        """Persiste corrida, vistos, eventos y estado del radar, o nada.

        ``seen`` y ``events`` describen el mismo hecho: si los vistos avanzan
        sin que los eventos lleguen al feed, la novedad se pierde para siempre
        (la próxima corrida ya no la ve como nueva). Por eso van juntos.

        ``events`` no trae ``radar_id``/``owner``/``radar_name``/``run_id``:
        ``EventRecord`` no los expone. Esta operación los deriva del lease ya
        renovado y de la fila de ``radars`` leída justo después, así que un
        caller autorizado sobre ``radar_id`` no puede declarar un evento como
        perteneciente a otro radar u otro dueño.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                if not lease.renew(
                    conn, radar_id, token, now=now, lease_duration=lease_duration
                ):
                    conn.execute("ROLLBACK")
                    return MaterializedRun(lease_lost=True)
                _fire(_fault_hook, PHASE_AFTER_LEASE_FENCE)

                radar_row = conn.execute(
                    "SELECT owner, name FROM radars WHERE id = ?", (radar_id,)
                ).fetchone()
                owner, radar_name = radar_row["owner"], radar_row["name"]

                _insert_run(conn, radar_id, run)
                _fire(_fault_hook, PHASE_AFTER_RUN_INSERT)

                ts = _now()
                for rec in seen:
                    _upsert_seen(conn, radar_id, rec, run.id, ts)
                _fire(_fault_hook, PHASE_AFTER_SEEN_UPSERTS)

                _insert_events(
                    conn, radar_id=radar_id, owner=owner, radar_name=radar_name,
                    run_id=run.id, events=events,
                )
                _fire(_fault_hook, PHASE_AFTER_EVENT_INSERTS)

                _mark_success(conn, radar_id, ts)
                _fire(_fault_hook, PHASE_AFTER_RADAR_UPDATE)

                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return MaterializedRun(run_id=run.id)

    def materialize_follows_refresh(
        self,
        *,
        radar_id: str,
        token: str,
        updates: list[FollowsSeenUpdate],
        seen_run_id: str = "follows_refresh",
        now: datetime | None = None,
        lease_duration: timedelta | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> MaterializedFollowsRefresh:
        """Persiste el refresco de los seguidos: snapshot + eventos, o nada.

        Operación propia y no ``materialize_successful_run`` porque follows no es
        una búsqueda: no tiene ``radar_run`` que insertar, y aquella limpiaría
        ``failure_count``/``permanent_failure_streak`` y marcaría ``last_run_at``,
        que es semántica de radar de búsqueda que este radar nunca tuvo.

        Comparte con ella lo que sí es común: el fence del lease como primera
        DML, la identidad de los eventos derivada dentro de la transacción, y el
        todo-o-nada entre snapshot y eventos —si el snapshot avanza sin que los
        eventos lleguen al feed, el refresco siguiente ya ve el hash nuevo y el
        cambio se pierde para siempre.

        Dos identidades que **no son la misma**, y por eso van en parámetros
        distintos:

        - ``radar_seen_results.last_run_id`` sí necesita un valor: sin él, dos
          refrescos consecutivos serían indistinguibles en el historial de la
          fila. ``seen_run_id`` es el centinela ``'follows_refresh'``, coherente
          con ``'follow_manual'`` en ``follow()`` — ambos declaran "esto no vino
          de una corrida", no "no hay dato".
        - ``radar_events.run_id`` es ``None`` **siempre**, sin excepción: no hay
          ``radar_run`` que auditar, y un id que no resuelve a ninguna fila de
          ``radar_runs`` miente. No se parametriza porque no hay caso en que
          debiera ser otra cosa. Ver ``docs/radar/FOLLOWS_LEASE_INVESTIGATION.md``
          §F.2.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                if not lease.renew(
                    conn, radar_id, token, now=now, lease_duration=lease_duration
                ):
                    conn.execute("ROLLBACK")
                    return MaterializedFollowsRefresh(lease_lost=True)
                _fire(_fault_hook, PHASE_AFTER_LEASE_FENCE)

                # El lease ya probó que la fila existe; esto prueba que sigue
                # siendo *la watchlist canónica* y no un radar de búsqueda: sus
                # eventos y su seen tienen semántica distinta.
                radar_row = _read_canonical_follows(conn, radar_id)
                if radar_row is None:
                    conn.execute("ROLLBACK")
                    return MaterializedFollowsRefresh(not_follows=True)
                owner, radar_name = radar_row["owner"], radar_row["name"]

                ts = _now()
                live: list[FollowsSeenUpdate] = []
                for upd in updates:
                    # rowcount 0 ⇒ un unfollow ganó la carrera mientras había
                    # red. No se inserta la fila (sería resucitarla) ni se emite
                    # su evento (sería anunciar algo que ya no se sigue).
                    if _update_seen(conn, radar_id, upd, seen_run_id, ts) == 1:
                        live.append(upd)
                _fire(_fault_hook, PHASE_AFTER_SEEN_UPDATES)

                events = [e for upd in live for e in upd.events]
                _insert_events(
                    conn, radar_id=radar_id, owner=owner, radar_name=radar_name,
                    run_id=None, events=events,
                )
                _fire(_fault_hook, PHASE_AFTER_EVENT_INSERTS)

                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        # Conteos derivados de ``live``: lo escrito, no lo planeado.
        return MaterializedFollowsRefresh(
            updated=len(live),
            events=len(events),
            baseline=sum(1 for upd in live if upd.is_baseline),
            changed_state=sum(1 for e in events if e.kind == "changed_state"),
            changed_value=sum(1 for e in events if e.kind == "changed_value"),
        )

    def materialize_follow_if_absent(
        self,
        *,
        radar_id: str,
        seen: SeenRecord,
        event: EventRecord,
        run_id: str | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> MaterializedFollow:
        """Da de alta un seguido y su evento ``followed``, o nada.

        Sin lease: ``follow`` es una acción del usuario sobre **su** fila, no una
        corrida, y el ganador de dos altas simultáneas lo decide la PRIMARY KEY
        (radar_id, result_key) dentro de la transacción. Un lease aquí serializaría
        clics que no compiten por nada.

        Idempotente por construcción: el INSERT es ``DO NOTHING``, así que
        re-seguir no reescribe el snapshot (no borra la línea base), no mueve
        ``first_seen_at`` y —al depender el evento del rowcount— no emite un
        segundo ``followed``. Membresía y evento comparten transacción: un alta
        sin su evento dejaría el feed sin explicar de dónde salió el seguido.

        ``event`` debe describir **esta** alta: se rechaza uno de otra clave, de
        otro dataset o de otro tipo. El evento y la membresía son dos caras del
        mismo hecho, y nada más abajo volvería a comprobar que coinciden.
        """
        if event.kind != "followed":
            raise ValueError(
                f"El evento de un alta debe ser 'followed', no '{event.kind}'."
            )
        if event.result_key != seen.result_key:
            raise ValueError(
                f"El evento describe la clave '{event.result_key}' pero la "
                f"membresía es de '{seen.result_key}'."
            )
        if event.dataset != seen.dataset:
            raise ValueError(
                f"El evento es del dataset '{event.dataset}' pero la membresía "
                f"es de '{seen.dataset}'."
            )

        conn = self._connect()
        created = False
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                radar_row = _read_canonical_follows(conn, radar_id)
                if radar_row is None:
                    conn.execute("ROLLBACK")
                    return MaterializedFollow(not_follows=True)
                owner, radar_name = radar_row["owner"], radar_row["name"]

                created = _insert_seen_if_absent(
                    conn, radar_id, seen, run_id, _now()
                ) == 1
                _fire(_fault_hook, PHASE_AFTER_MEMBERSHIP_INSERT)

                if created:
                    _insert_events(
                        conn, radar_id=radar_id, owner=owner, radar_name=radar_name,
                        run_id=run_id, events=[event],
                    )
                _fire(_fault_hook, PHASE_AFTER_EVENT_INSERTS)

                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return MaterializedFollow(created=created)

    def materialize_failed_run(
        self,
        *,
        radar_id: str,
        token: str,
        run: RunRecord,
        failure: FailureRecord,
        now: datetime | None = None,
        lease_duration: timedelta | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> MaterializedRun:
        """Persiste la corrida fallida junto con su backoff y contadores.

        Media transacción aquí significa un radar que registró el fallo sin la
        corrida que lo explica, o al revés: un backoff sin auditoría.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                if not lease.renew(
                    conn, radar_id, token, now=now, lease_duration=lease_duration
                ):
                    conn.execute("ROLLBACK")
                    return MaterializedRun(lease_lost=True)
                _fire(_fault_hook, PHASE_AFTER_LEASE_FENCE)

                _insert_run(conn, radar_id, run)
                _fire(_fault_hook, PHASE_AFTER_RUN_INSERT)

                _apply_failure(conn, radar_id, failure, _now())
                _fire(_fault_hook, PHASE_AFTER_FAILURE_UPDATE)

                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return MaterializedRun(run_id=run.id)
