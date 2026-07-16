"""preference_store.py — qué aceptó recibir la persona, en qué radar y a dónde.

Una preferencia une un radar con un endpoint. Nace **apagada** siempre: recibir
notificaciones es una decisión afirmativa, y un default en 1 convertiría "guardé
un radar" en "acepté que me escriban", que no es lo mismo ni legalmente ni
humanamente. Ni siquiera un endpoint ya verificado la enciende: verificar prueba
que el destino es tuyo, no que quieras usarlo para este radar.

**El control de aislamiento entre personas vive aquí, no en las FK.** Las FK
garantizan que el radar existe y que el endpoint existe — nada más. Con solo FK,
asociar el endpoint de Ana al radar de Beto es una escritura perfectamente válida
que hace que Ana reciba las búsquedas de Beto. El único vínculo que lo impide es:

    radars.owner → principal_owner_keys.owner_key → principal_id
                                                  ==  notification_endpoints.principal_id

Si el owner del radar no está vinculado a ningún principal, la creación falla en
vez de adivinar. Un radar de un chat anónimo no tiene dueño conocido, y elegirle
uno por proximidad sería inventar consentimiento.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from app.core.db import connect_sqlite
from app.core.notify.schema import (
    DEFAULT_MAX_ITEMS,
    DEFAULT_TIMEZONE,
    DEFERRED_FREQUENCIES,
    FREQUENCIES,
    FREQUENCY_PER_RUN,
    ensure_schema,
    new_id,
    now_iso,
)

# Puntos de inyección de fallo, igual que el resto de notify: existen para probar que
# el opt-in revierte por completo en cada paso; en runtime el hook siempre es None.
# Están puestos donde el estado empieza a ser observable, no donde sería cómodo.
PHASE_AFTER_CURSOR_UPSERT = "after_cursor_upsert"
PHASE_AFTER_ENABLE = "after_enable"
PHASE_BEFORE_COMMIT = "before_commit"

_FaultHook = Callable[[str], None]


def _fire(hook: _FaultHook | None, phase: str) -> None:
    if hook is not None:
        hook(phase)

_HHMM_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")

# Campos que el usuario puede cambiar después del alta. La lista es explícita
# —y no "todo lo que no sea la PK"— para que radar_id/endpoint_id no sean
# editables: mover una preferencia de un radar a otro saltaría el control de
# aislamiento que se hizo al crearla.
_UPDATABLE = frozenset({
    "frequency", "quiet_hours_start", "quiet_hours_end", "timezone", "max_items",
    "include_new", "include_state_changes", "include_value_changes",
    "include_disappeared",
})

_BOOL_FIELDS = frozenset({
    "include_new", "include_state_changes", "include_value_changes",
    "include_disappeared",
})


class PreferenceError(Exception):
    """Error de negocio de preferencias."""


class PreferenceExistsError(PreferenceError):
    """Ya existe una preferencia para ese par (radar, endpoint)."""


class RadarNotAvailableError(PreferenceError):
    """El radar no existe o está archivado."""


class OwnerNotLinkedError(PreferenceError):
    """El owner del radar no está vinculado a ningún principal."""


class PrincipalMismatchError(PreferenceError):
    """El endpoint pertenece a una persona distinta de la dueña del radar."""


class EndpointRevokedError(PreferenceError):
    """El endpoint fue revocado: no se puede consentir sobre un destino retirado."""


class UnsupportedPreferenceError(PreferenceError):
    """La preferencia es expresable pero el sistema todavía no sabe cumplirla.

    Distinta de ``PreferenceError`` a secas a propósito: no es un dato inválido que
    el usuario deba corregir, es una función que aún no existe. Quien la capture
    debe decir "todavía no", no "escribiste mal".
    """


@dataclass(frozen=True)
class Preference:
    preference_id: str
    radar_id: str
    endpoint_id: str
    enabled: int
    frequency: str
    quiet_hours_start: str | None
    quiet_hours_end: str | None
    timezone: str
    max_items: int
    include_new: int
    include_state_changes: int
    include_value_changes: int
    include_disappeared: int
    created_at: str
    updated_at: str

    @property
    def is_enabled(self) -> bool:
        return bool(self.enabled)

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Preference":
        return cls(**{k: row[k] for k in row.keys()})


# El dueño del radar, resuelto por indirección. LEFT JOIN a propósito: con un
# JOIN interno, "el radar no existe", "está archivado" y "su owner no está
# vinculado" colapsan en 0 filas y el error tendría que adivinar cuál fue. Así
# una sola consulta distingue los tres y cada uno dice la verdad —"vincula tu
# cuenta" y "ese radar está archivado" piden acciones distintas al usuario.
_RADAR_PRINCIPAL_SQL = """
SELECT r.archived_at, pok.principal_id
  FROM radars r
  LEFT JOIN principal_owner_keys pok ON pok.owner_key = r.owner
 WHERE r.id = ?
"""


def _validate_quiet_hours(start: str | None, end: str | None) -> None:
    for label, value in (("inicio", start), ("fin", end)):
        if value is not None and not _HHMM_RE.match(value):
            raise PreferenceError(
                f"Hora de silencio ({label}) inválida: '{value}'. Formato HH:MM."
            )
    if (start is None) != (end is None):
        raise PreferenceError(
            "Las horas de silencio van en pareja: se necesitan inicio y fin."
        )


def _validate_max_items(value: int) -> None:
    if int(value) <= 0:
        raise PreferenceError(
            f"max_items debe ser mayor que 0; recibido {value}. Un digest de cero "
            f"ítems no es una preferencia, es un envío vacío."
        )


def _validate_frequency(value: str) -> None:
    """``per_run`` o nada. ``daily``/``weekly`` tienen su propio mensaje.

    Aceptarlas y no implementarlas es peor que rechazarlas: la persona cree que pidió
    un resumen diario y recibe uno por corrida. El sistema no puede prometer una
    cadencia que nadie ejecuta.
    """
    if value in FREQUENCIES:
        return
    # ``FREQUENCIES`` es la **única** fuente de verdad sobre qué se acepta;
    # ``DEFERRED_FREQUENCIES`` solo elige el mensaje. Si la pertenencia se preguntara
    # primero contra la lista de diferidas, esa lista se convertiría en una segunda
    # definición de lo permitido y las dos podrían discrepar: añadir 'daily' a
    # ``FREQUENCIES`` dejaría de tener efecto aquí y el rechazo dependería de que
    # alguien recordara mantener 'daily' también en la otra lista.
    if value in DEFERRED_FREQUENCIES:
        raise UnsupportedPreferenceError(
            f"La frecuencia '{value}' todavía no está implementada: no existe "
            f"cadencia de digest, así que aceptarla entregaría un mensaje por "
            f"corrida bajo una etiqueta que promete otra cosa. Única frecuencia "
            f"soportada: '{FREQUENCY_PER_RUN}'."
        )
    raise PreferenceError(
        f"Frecuencia inválida '{value}'. Válidas: {', '.join(FREQUENCIES)}."
    )


def _validate_include_disappeared(value) -> None:
    """El interruptor existe en la tabla, pero encenderlo no entrega nada.

    Tres capas lo impiden hoy: ``runner.py`` nunca emite ``disappeared``, ningún
    caller pasa ``confirmed_disappeared_keys`` a ``compute_diff``, y ``radar_events``
    no tiene columna que confirme la desaparición. Ofrecerlo como opción sería vender
    una función inexistente; el rechazo es la única respuesta honesta hasta que las
    tres capas existan.
    """
    if bool(value):
        raise UnsupportedPreferenceError(
            "include_disappeared todavía no está disponible: el radar no genera ni "
            "persiste desapariciones confirmadas de punta a punta, así que activarlo "
            "no entregaría ninguna notificación. Se rechaza en vez de fingir que "
            "funciona."
        )


def _validate_timezone(value: str) -> None:
    if not (value or "").strip():
        raise PreferenceError(
            "La zona horaria es obligatoria: sin ella las horas de silencio no "
            "significan nada."
        )


class PreferenceStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        conn = self._connect()
        try:
            ensure_schema(conn)
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def create_preference(
        self,
        *,
        radar_id: str,
        endpoint_id: str,
        frequency: str = FREQUENCY_PER_RUN,
        quiet_hours_start: str | None = None,
        quiet_hours_end: str | None = None,
        timezone: str = DEFAULT_TIMEZONE,
        max_items: int = DEFAULT_MAX_ITEMS,
        include_new: bool = True,
        include_state_changes: bool = True,
        include_value_changes: bool = True,
        include_disappeared: bool = False,
    ) -> str:
        """Crea la preferencia **apagada**, tras probar que radar y endpoint son
        de la misma persona.

        La comprobación y el INSERT van en una sola transacción: entre "verifiqué
        que el endpoint es de Ana" y "escribí la preferencia" podría desvincularse
        el owner_key, y el control quedaría decidido sobre un estado que ya no
        existe.
        """
        _validate_frequency(frequency)
        _validate_timezone(timezone)
        _validate_max_items(max_items)
        _validate_quiet_hours(quiet_hours_start, quiet_hours_end)
        _validate_include_disappeared(include_disappeared)

        preference_id = new_id()
        ts = now_iso()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._assert_same_principal(conn, radar_id, endpoint_id)
                conn.execute(
                    """INSERT INTO radar_notification_preferences
                       (preference_id, radar_id, endpoint_id, enabled, frequency,
                        quiet_hours_start, quiet_hours_end, timezone, max_items,
                        include_new, include_state_changes, include_value_changes,
                        include_disappeared, created_at, updated_at)
                       VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (preference_id, radar_id, endpoint_id, frequency,
                     quiet_hours_start, quiet_hours_end, timezone, int(max_items),
                     int(include_new), int(include_state_changes),
                     int(include_value_changes), int(include_disappeared), ts, ts),
                )
                conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                conn.execute("ROLLBACK")
                raise PreferenceExistsError(
                    "Ya existe una preferencia para ese radar y ese endpoint."
                ) from exc
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return preference_id

    def _assert_same_principal(
        self, conn: sqlite3.Connection, radar_id: str, endpoint_id: str
    ) -> None:
        """Radar disponible, owner vinculado y misma persona. Dentro de la
        transacción: cualquiera de las tres puede dejar de ser cierta mientras
        se comprueban las otras."""
        row = conn.execute(_RADAR_PRINCIPAL_SQL, (radar_id,)).fetchone()
        if row is None:
            raise RadarNotAvailableError(f"El radar '{radar_id}' no existe.")
        if row["archived_at"] is not None:
            # Se lee aquí, y no antes de la transacción, porque archivar es
            # concurrente: un radar archivado ya no corre, así que consentir
            # notificaciones sobre él crearía un opt-in que nada puede cumplir.
            raise RadarNotAvailableError(
                f"El radar '{radar_id}' está archivado: no corre, y una "
                f"preferencia sobre él nunca notificaría nada. Restáuralo primero."
            )
        if row["principal_id"] is None:
            raise OwnerNotLinkedError(
                f"El owner del radar '{radar_id}' no está vinculado a ningún "
                f"principal. No se adivina el dueño de una notificación."
            )
        endpoint_row = conn.execute(
            "SELECT principal_id FROM notification_endpoints WHERE endpoint_id = ?",
            (endpoint_id,),
        ).fetchone()
        if endpoint_row is None:
            raise PreferenceError(f"No existe el endpoint '{endpoint_id}'.")
        if endpoint_row["principal_id"] != row["principal_id"]:
            raise PrincipalMismatchError(
                "El endpoint pertenece a otra persona: asociarlo a este radar le "
                "enviaría las búsquedas de alguien más."
            )

    def _assert_endpoint_not_revoked(
        self, conn: sqlite3.Connection, endpoint_id: str
    ) -> None:
        """Revocado = destino retirado. No se consiente sobre él.

        Solo se comprueba la revocación, no ``verified_at`` ni ``enabled``: esos dos
        son condiciones reversibles que la matriz de autorización trata como *hold*
        —el batch espera a que se resuelvan—, así que encender la preferencia antes de
        verificar el destino es un flujo legítimo. La revocación no se resuelve sola.
        """
        row = conn.execute(
            "SELECT revoked_at FROM notification_endpoints WHERE endpoint_id = ?",
            (endpoint_id,),
        ).fetchone()
        if row is None:
            raise PreferenceError(f"No existe el endpoint '{endpoint_id}'.")
        if row["revoked_at"] is not None:
            raise EndpointRevokedError(
                f"El endpoint '{endpoint_id}' está revocado: no se puede encender "
                f"una preferencia sobre un destino que la persona ya retiró."
            )

    def get_preference(self, preference_id: str) -> Preference | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM radar_notification_preferences WHERE preference_id = ?",
                (preference_id,),
            ).fetchone()
        finally:
            conn.close()
        return Preference.from_row(row) if row else None

    def list_preferences_for_radar(self, radar_id: str) -> list[Preference]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radar_notification_preferences WHERE radar_id = ? "
                "ORDER BY created_at",
                (radar_id,),
            ).fetchall()
        finally:
            conn.close()
        return [Preference.from_row(r) for r in rows]

    def list_preferences_for_endpoint(self, endpoint_id: str) -> list[Preference]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radar_notification_preferences WHERE endpoint_id = ? "
                "ORDER BY created_at",
                (endpoint_id,),
            ).fetchall()
        finally:
            conn.close()
        return [Preference.from_row(r) for r in rows]

    def enable_without_backfill(
        self,
        preference_id: str,
        *,
        now: datetime | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> Preference:
        """El opt-in explícito: enciende la preferencia **y** planta su cursor en el
        presente, atómicamente.

        Encender sin plantar el cursor deja ``last_examined_seq`` en 0, y la primera
        selección barre la historia entera del radar: el primer mensaje de alguien que
        acaba de aceptar notificaciones sería un volcado de eventos que nunca
        consintió recibir. Consentir "avísame" no es consentir "cuéntame todo lo que
        pasó antes de que dijera que sí".

        **Las dos escrituras son una sola transacción, y esa es toda la operación.**
        Si el cursor se plantara en una transacción y el ``enabled=1`` en otra, entre
        ambas existiría una ventana: un fallo tras la primera dejaría un cursor
        avanzado sobre una preferencia apagada —historia perdida en silencio si
        alguien la enciende luego—, y un fallo tras la segunda dejaría la preferencia
        encendida con cursor en 0, que es exactamente el volcado que esto evita.

        **Linearización.** ``BEGIN IMMEDIATE`` toma el lock de escritor, y en WAL solo
        hay uno. Reservar un ``event_seq`` es un INSERT en ``radar_event_seq``, o sea
        también una escritura: mientras esta transacción tiene el lock, nadie puede
        reservar. Por tanto todo evento cae inequívocamente de un lado del corte —si
        reservó antes, su commit precede a nuestro ``MAX(event_seq)`` y es historia;
        si reserva después, su seq es mayor que el cursor y se notifica—. No hay
        tercera posibilidad, y por eso ``MAX(event_seq)`` y ``enabled=1`` no pueden
        vivir en transacciones distintas.

        Revalida dentro del lock —radar, owner, endpoint, principal— porque entre
        crear la preferencia y encenderla pueden haber pasado meses: el radar pudo
        archivarse, el owner desvincularse o el endpoint revocarse. El permiso se
        comprueba contra el estado del instante en que se concede, no contra el del
        día en que se pidió.
        """
        moment = now or datetime.now(timezone.utc)
        if moment.tzinfo is None or moment.utcoffset() is None:
            raise PreferenceError(
                "now debe incluir zona horaria: una marca naive comparada contra "
                "marcas UTC ordena mal."
            )
        ts = moment.astimezone(timezone.utc).isoformat()

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT radar_id, endpoint_id FROM radar_notification_preferences "
                    "WHERE preference_id = ?",
                    (preference_id,),
                ).fetchone()
                if row is None:
                    raise PreferenceError(
                        f"No existe la preferencia '{preference_id}'."
                    )
                radar_id, endpoint_id = row["radar_id"], row["endpoint_id"]
                self._assert_same_principal(conn, radar_id, endpoint_id)
                self._assert_endpoint_not_revoked(conn, endpoint_id)

                max_seq = conn.execute(
                    "SELECT COALESCE(MAX(event_seq), 0) AS max_seq FROM radar_events "
                    "WHERE radar_id = ?",
                    (radar_id,),
                ).fetchone()["max_seq"]
                conn.execute(
                    "INSERT INTO radar_notification_cursors "
                    "(endpoint_id, radar_id, last_examined_seq, updated_at) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(endpoint_id, radar_id) DO UPDATE SET "
                    "last_examined_seq = MAX(last_examined_seq, excluded.last_examined_seq), "
                    "updated_at = excluded.updated_at",
                    (endpoint_id, radar_id, int(max_seq), ts),
                )
                _fire(_fault_hook, PHASE_AFTER_CURSOR_UPSERT)

                conn.execute(
                    "UPDATE radar_notification_preferences "
                    "SET enabled = 1, updated_at = ? WHERE preference_id = ?",
                    (ts, preference_id),
                )
                _fire(_fault_hook, PHASE_AFTER_ENABLE)
                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

        preference = self.get_preference(preference_id)
        assert preference is not None
        return preference

    def disable(self, preference_id: str) -> None:
        """Apaga la preferencia y **conserva el cursor**.

        No borra batches, ni intentos, ni historial: apagar es dejar de recibir, no
        reescribir lo que pasó. El cursor se conserva porque re-encender más tarde
        debe seguir sin arrastrar historia; borrarlo convertiría un opt-out temporal
        en el volcado que ``enable_without_backfill`` existe para impedir.
        """
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE radar_notification_preferences "
                "SET enabled = 0, updated_at = ? WHERE preference_id = ?",
                (now_iso(), preference_id),
            )
        finally:
            conn.close()

    def set_enabled(self, preference_id: str, enabled: bool) -> None:
        """Compatibilidad: delega, no duplica.

        Encender tiene **una** ruta. Si esta hiciera su propio UPDATE, existirían dos
        formas de encender una preferencia y solo una plantaría el cursor: la que
        alguien olvidara usar sería la que volcara la historia.
        """
        if enabled:
            self.enable_without_backfill(preference_id)
            return
        self.disable(preference_id)

    def update_preference(self, preference_id: str, **fields) -> None:
        """Actualiza solo campos de la whitelist, validando igual que el alta."""
        unknown = set(fields) - _UPDATABLE
        if unknown:
            raise PreferenceError(
                f"Campos no actualizables: {', '.join(sorted(unknown))}. "
                f"Permitidos: {', '.join(sorted(_UPDATABLE))}."
            )
        if not fields:
            return
        if "frequency" in fields:
            _validate_frequency(fields["frequency"])
        if "include_disappeared" in fields:
            _validate_include_disappeared(fields["include_disappeared"])
        if "timezone" in fields:
            _validate_timezone(fields["timezone"])
        if "max_items" in fields:
            _validate_max_items(fields["max_items"])
            fields["max_items"] = int(fields["max_items"])
        if "quiet_hours_start" in fields or "quiet_hours_end" in fields:
            current = self.get_preference(preference_id)
            if current is None:
                raise PreferenceError(f"No existe la preferencia '{preference_id}'.")
            _validate_quiet_hours(
                fields.get("quiet_hours_start", current.quiet_hours_start),
                fields.get("quiet_hours_end", current.quiet_hours_end),
            )
        for name in _BOOL_FIELDS & set(fields):
            fields[name] = int(bool(fields[name]))

        assignments = ", ".join(f"{name} = ?" for name in fields)
        conn = self._connect()
        try:
            conn.execute(
                f"UPDATE radar_notification_preferences SET {assignments}, "
                f"updated_at = ? WHERE preference_id = ?",
                (*fields.values(), now_iso(), preference_id),
            )
        finally:
            conn.close()
