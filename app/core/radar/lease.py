"""lease.py — exclusión mutua interproceso por radar.

El scheduler (systemd), el chat y la UI son procesos distintos: un lock de
Python en memoria no los coordina. La exclusión vive en SQLite y se expresa
como un *lease* con dueño (``token``) y vencimiento (``expires_at``):

- la adquisición es **un solo UPDATE condicional** — el ganador se decide por
  filas afectadas, nunca por un SELECT previo (eso tendría race condition);
- el ``token`` es la prueba de propiedad: renovar y liberar exigen token, de
  modo que un dueño vencido no puede pisar el lease de su sucesor;
- si el proceso dueño muere, el lease vence solo y otro ejecutor lo recupera.

Este módulo es SQL puro sobre una conexión ajena: no abre ni cierra
conexiones, no conoce ``RadarStore`` ni el runner. ``RadarStore`` expone los
métodos públicos; ``run_radar`` los usa.
"""
from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# Duración del lease. Peor caso de una corrida (ver runner): consulta de
# ventana + count (2 × 30s de timeout Socrata) más el barrido por clave
# (MAX_SEEN_REFRESH=500 / batch_size=100 = 5 lotes × 30s) ≈ 210s. 600s deja
# un margen ~2.9× sobre ese peor caso sin dejar un radar muerto bloqueado por
# mucho tiempo.
LEASE_DURATION = timedelta(seconds=600)

LEASE_ACQUIRED = "acquired"
LEASE_ALREADY_RUNNING = "already_running"
LEASE_RADAR_NOT_FOUND = "radar_not_found"

# Libre = sin dueño, o con un lease ya vencido (el dueño murió sin liberar).
_ACQUIRE_SQL = """
UPDATE radars
   SET run_lease_token       = :token,
       run_lease_acquired_at = :now,
       run_lease_expires_at  = :expires
 WHERE id = :radar_id
   AND (
        run_lease_token IS NULL
     OR run_lease_expires_at IS NULL
     OR run_lease_expires_at <= :now
   )
"""

# Renovar exige token vigente: un dueño que ya perdió el lease no lo recupera.
_RENEW_SQL = """
UPDATE radars
   SET run_lease_expires_at = :expires
 WHERE id = :radar_id
   AND run_lease_token = :token
   AND run_lease_expires_at > :now
"""

_RELEASE_SQL = """
UPDATE radars
   SET run_lease_token       = NULL,
       run_lease_acquired_at = NULL,
       run_lease_expires_at  = NULL
 WHERE id = :radar_id
   AND run_lease_token = :token
"""

_OWNED_SQL = """
SELECT 1 FROM radars
 WHERE id = :radar_id
   AND run_lease_token = :token
   AND run_lease_expires_at > :now
"""

_EXISTS_SQL = "SELECT 1 FROM radars WHERE id = ?"


@dataclass(frozen=True)
class LeaseClaim:
    """Resultado de intentar adquirir el lease.

    ``already_running`` es un estado normal de control, no un error: por eso
    se devuelve, no se lanza.
    """
    status: str
    token: str | None = None
    expires_at: str | None = None

    @property
    def acquired(self) -> bool:
        return self.status == LEASE_ACQUIRED


def _stamp(moment: datetime) -> str:
    """ISO-8601 UTC, el mismo formato que el resto del store.

    Todas las marcas comparten offset (+00:00), así que la comparación
    lexicográfica de SQLite ordena igual que la cronológica.
    """
    return moment.astimezone(timezone.utc).isoformat()


def _window(now: datetime | None, duration: timedelta | None) -> tuple[str, str]:
    moment = now or datetime.now(timezone.utc)
    return _stamp(moment), _stamp(moment + (duration or LEASE_DURATION))


def try_acquire(
    conn: sqlite3.Connection,
    radar_id: str,
    *,
    now: datetime | None = None,
    lease_duration: timedelta | None = None,
) -> LeaseClaim:
    """Reclama el lease del radar en una sola operación atómica."""
    now_s, expires_s = _window(now, lease_duration)
    token = uuid.uuid4().hex
    cur = conn.execute(
        _ACQUIRE_SQL,
        {"token": token, "now": now_s, "expires": expires_s, "radar_id": radar_id},
    )
    if cur.rowcount == 1:
        return LeaseClaim(LEASE_ACQUIRED, token=token, expires_at=expires_s)
    # 0 filas: o hay un dueño vigente, o el radar no existe. El claim ya está
    # decidido por rowcount; esto solo afina el diagnóstico.
    if conn.execute(_EXISTS_SQL, (radar_id,)).fetchone() is None:
        return LeaseClaim(LEASE_RADAR_NOT_FOUND)
    return LeaseClaim(LEASE_ALREADY_RUNNING)


def renew(
    conn: sqlite3.Connection,
    radar_id: str,
    token: str,
    *,
    now: datetime | None = None,
    lease_duration: timedelta | None = None,
) -> bool:
    """Extiende el lease. Falsa si el token no es dueño o el lease venció."""
    now_s, expires_s = _window(now, lease_duration)
    cur = conn.execute(
        _RENEW_SQL,
        {"expires": expires_s, "radar_id": radar_id, "token": token, "now": now_s},
    )
    return cur.rowcount == 1


def release(conn: sqlite3.Connection, radar_id: str, token: str) -> bool:
    """Libera el lease solo si ``token`` sigue siendo el dueño."""
    cur = conn.execute(_RELEASE_SQL, {"radar_id": radar_id, "token": token})
    return cur.rowcount == 1


def is_owned(
    conn: sqlite3.Connection,
    radar_id: str,
    token: str,
    *,
    now: datetime | None = None,
) -> bool:
    """¿``token`` posee un lease vigente? Lectura, no extiende nada."""
    now_s, _ = _window(now, None)
    row = conn.execute(
        _OWNED_SQL, {"radar_id": radar_id, "token": token, "now": now_s}
    ).fetchone()
    return row is not None
