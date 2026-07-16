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

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from app.core.db import connect_sqlite
from app.core.notify.schema import (
    DEFAULT_MAX_ITEMS,
    DEFAULT_TIMEZONE,
    FREQUENCIES,
    FREQUENCY_PER_RUN,
    ensure_schema,
    new_id,
    now_iso,
)

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
    if value not in FREQUENCIES:
        raise PreferenceError(
            f"Frecuencia inválida '{value}'. Válidas: {', '.join(FREQUENCIES)}."
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

    def set_enabled(self, preference_id: str, enabled: bool) -> None:
        """El opt-in explícito. Es su propia operación, no un campo de update:
        encender notificaciones no es lo mismo que ajustar cuántos ítems ver."""
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE radar_notification_preferences "
                "SET enabled = ?, updated_at = ? WHERE preference_id = ?",
                (1 if enabled else 0, now_iso(), preference_id),
            )
        finally:
            conn.close()

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
