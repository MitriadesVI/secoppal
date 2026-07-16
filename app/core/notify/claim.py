"""claim.py — exclusión mutua efímera sobre un batch de notificación (P1B-2).

SQL puro sobre una conexión ajena, igual que ``radar/lease.py``, pero **no**
lo importa ni reutiliza: el lease de radar protege un recurso de larga vida
(un radar que corre indefinidamente); un batch es efímero y de un solo uso.
Copiar el módulo arrastraría ``renew`` — un batch no vive lo bastante para
necesitar renovación, y P1B-4 fijará la duración del claim respecto del
timeout real del proveedor, no aquí.

- la reclamación es **un solo UPDATE condicional** — el ganador se decide por
  filas afectadas, nunca por un SELECT previo;
- el ``claim_token`` es la prueba de propiedad: toda escritura posterior al
  claim debe exigirlo en el WHERE, igual que ``lease.py`` exige su token;
- si el claim vence sin que nadie libere, otro worker lo recupera.
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone


def _stamp(moment: datetime) -> str:
    """ISO-8601 UTC — mismo formato que el resto del store, para que la
    comparación lexicográfica de SQLite ordene igual que la cronológica."""
    return moment.astimezone(timezone.utc).isoformat()


# Reclamable: no vencido, no en cuarentena (not_before), y en 'pending' o en
# 'claimed' con un claim_until ya superado (el dueño anterior murió sin
# liberar). Se rechazan explícitamente sent/ambiguous/dead —estados
# terminales— y un claimed todavía vigente.
_TRY_CLAIM_SQL = """
UPDATE radar_notification_batches
   SET status = 'claimed',
       claim_token = :token,
       claim_until = :claim_until
 WHERE notification_id = :notification_id
   AND expires_at > :now
   AND (not_before IS NULL OR not_before <= :now)
   AND (
        status = 'pending'
     OR (status = 'claimed' AND claim_until IS NOT NULL AND claim_until <= :now)
   )
"""

# Fenced por token: un dueño que ya perdió el claim (vencido, o liberado y
# reclamado por otro) no puede liberar el claim de su sucesor.
_RELEASE_SQL = """
UPDATE radar_notification_batches
   SET status = 'pending',
       claim_token = NULL,
       claim_until = NULL
 WHERE notification_id = :notification_id
   AND status = 'claimed'
   AND claim_token = :token
"""

_OWNED_SQL = """
SELECT 1 FROM radar_notification_batches
 WHERE notification_id = :notification_id
   AND status = 'claimed'
   AND claim_token = :token
   AND claim_until > :now
"""


def try_claim(
    conn: sqlite3.Connection,
    notification_id: str,
    now: datetime,
    claim_duration: timedelta,
) -> str | None:
    """Reclama el batch en una sola operación atómica.

    Devuelve el ``claim_token`` ganador, o ``None`` si perdió (vigente en
    manos de otro, en cuarentena, expirado, o en un estado terminal).
    """
    token = uuid.uuid4().hex
    now_s = _stamp(now)
    until_s = _stamp(now + claim_duration)
    cur = conn.execute(
        _TRY_CLAIM_SQL,
        {"token": token, "claim_until": until_s, "notification_id": notification_id,
         "now": now_s},
    )
    return token if cur.rowcount == 1 else None


def release(conn: sqlite3.Connection, notification_id: str, token: str) -> bool:
    """Libera el claim solo si ``token`` sigue siendo el dueño.

    Vuelve a ``pending`` y limpia ``claim_token``/``claim_until``. Un token
    viejo (ya vencido y recuperado por otro worker) nunca puede volver a
    coincidir con el ``claim_token`` vigente, así que este UPDATE jamás toca
    el claim del sucesor.
    """
    cur = conn.execute(
        _RELEASE_SQL, {"notification_id": notification_id, "token": token}
    )
    return cur.rowcount == 1


def is_owned(
    conn: sqlite3.Connection, notification_id: str, token: str, now: datetime,
) -> bool:
    """¿``token`` posee un claim vigente? Lectura, no extiende nada."""
    row = conn.execute(
        _OWNED_SQL,
        {"notification_id": notification_id, "token": token, "now": _stamp(now)},
    ).fetchone()
    return row is not None
