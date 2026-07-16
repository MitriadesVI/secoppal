"""authorization.py — la única definición de "¿se puede entregar esto ahora?" (P1B-4).

Promovido desde ``selector.py``. El selector decide si materializa una intención;
el worker la revalida justo antes de autorizar un intento. Si cada uno tuviera su
SQL o su matriz, divergirían en silencio y el worker acabaría enviando lo que el
selector nunca habría creado —o al revés—. Aquí hay **un** SQL y **una** matriz;
ambos los importan.

No escribe. No conoce proveedores. No abre transacciones: lee sobre la conexión (y
la transacción) del caller, porque en el worker esa lectura tiene que compartir
transacción con el INSERT del intento — es lo que ordena revocación y autorización.

**Lo que deliberadamente NO filtra:** ``radars.enabled``, ``needs_attention``,
``next_run_after``, ``failure_count``, ``permanent_failure_streak``. Describen la
salud del scrapeo, no el consentimiento. Un evento ya materializado es un hecho
ocurrido: que el radar esté hoy en mal estado no retira el permiso de contar lo que
ya pasó.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import sqlite3

from app.core.notify.schema import PRINCIPAL_ACTIVE

# Categorías cerradas. No hay una quinta: cada condición de la matriz cae en una de
# estas cuatro, y la exhaustividad es lo que permite al caller hacer un dispatch sin
# rama por defecto.
AUTHORIZED = "authorized"
TERMINAL = "terminal"
HOLD = "hold"
SECURITY_BLOCKED = "security_blocked"

CATEGORIES = (AUTHORIZED, TERMINAL, HOLD, SECURITY_BLOCKED)


@dataclass(frozen=True)
class AuthorizationSnapshot:
    """Los hechos de autorización en un instante, ya normalizados.

    ``external_recipient_id`` lleva ``repr=False``: el destino real de una persona no
    se filtra por un log de depuración ni por el mensaje de una excepción que alguien
    formatee sin pensarlo. Quien lo necesite tiene que leerlo explícitamente.
    """

    principal_id: str
    endpoint_id: str
    radar_id: str
    channel: str
    preference_enabled: bool
    endpoint_verified: bool
    endpoint_enabled: bool
    endpoint_revoked: bool
    principal_status: str
    radar_archived: bool
    owner_principal_id: str | None
    external_recipient_id: str | None = field(default=None, repr=False)
    notification_id: str | None = None


@dataclass(frozen=True)
class AuthorizationDecision:
    """``category`` gobierna qué hace el caller; ``reason`` explica por qué.

    ``dead_reason`` solo lo llevan las decisiones ``terminal``, y siempre es uno de
    los valores que acepta el CHECK de ``radar_notification_batches``: una decisión
    terminal que no se pueda persistir sería una muerte inexpresable.
    """

    category: str
    reason: str | None = None
    dead_reason: str | None = None


_AUTHORIZED = AuthorizationDecision(AUTHORIZED)
_PREFERENCE_NOT_FOUND = AuthorizationDecision(TERMINAL, "preference_not_found", "opted_out")
_PREFERENCE_DISABLED = AuthorizationDecision(TERMINAL, "preference_disabled", "opted_out")
_ENDPOINT_REVOKED = AuthorizationDecision(TERMINAL, "endpoint_revoked", "endpoint_revoked")
_RADAR_ARCHIVED = AuthorizationDecision(TERMINAL, "radar_archived", "radar_archived")
_ENDPOINT_UNVERIFIED = AuthorizationDecision(HOLD, "endpoint_unverified")
_ENDPOINT_MISSING_RECIPIENT = AuthorizationDecision(HOLD, "endpoint_missing_recipient")
_ENDPOINT_DISABLED = AuthorizationDecision(HOLD, "endpoint_disabled")
_PRINCIPAL_SUSPENDED = AuthorizationDecision(HOLD, "principal_suspended")
_OWNER_NOT_LINKED = AuthorizationDecision(HOLD, "owner_not_linked")
_PRINCIPAL_MISMATCH = AuthorizationDecision(SECURITY_BLOCKED, "principal_mismatch")


# El mismo SQL que usaba el selector, sin una coma de diferencia. Trae de más lo que
# el selector necesita para construir el payload (``frequency``, ``max_items``,
# ``include_*``, quiet hours, ``radar_name``): partirlo en dos consultas —una de
# autorización, otra de configuración— volvería a abrir la puerta a que el worker
# revalide contra un join distinto del que decidió crear el batch.
TARGET_SQL = """
SELECT pref.*, e.principal_id, e.channel, e.verified_at,
       e.external_recipient_id, e.enabled AS endpoint_enabled, e.revoked_at,
       p.status AS principal_status,
       r.name AS radar_name, r.archived_at, r.owner,
       pok.principal_id AS owner_principal_id
  FROM radar_notification_preferences pref
  JOIN notification_endpoints e ON e.endpoint_id = pref.endpoint_id
  JOIN principals p ON p.principal_id = e.principal_id
  JOIN radars r ON r.id = pref.radar_id
  LEFT JOIN principal_owner_keys pok ON pok.owner_key = r.owner
 WHERE pref.endpoint_id = ? AND pref.radar_id = ?
"""


def fetch_target_row(
    conn: sqlite3.Connection, endpoint_id: str, radar_id: str
) -> sqlite3.Row | None:
    """Lee el par preferencia/endpoint/principal/radar/owner sobre la tx del caller."""
    return conn.execute(TARGET_SQL, (endpoint_id, radar_id)).fetchone()


def snapshot_from_row(
    row: sqlite3.Row, *, notification_id: str | None = None
) -> AuthorizationSnapshot:
    """Normaliza la fila a hechos booleanos.

    ``row["enabled"]`` es el de ``pref.*`` —el consentimiento—; el del endpoint viaja
    como ``endpoint_enabled`` precisamente porque compartirían nombre.
    """
    return AuthorizationSnapshot(
        principal_id=row["principal_id"],
        endpoint_id=row["endpoint_id"],
        radar_id=row["radar_id"],
        channel=row["channel"],
        preference_enabled=bool(row["enabled"]),
        endpoint_verified=row["verified_at"] is not None,
        endpoint_enabled=bool(row["endpoint_enabled"]),
        endpoint_revoked=row["revoked_at"] is not None,
        principal_status=row["principal_status"],
        radar_archived=row["archived_at"] is not None,
        owner_principal_id=row["owner_principal_id"],
        external_recipient_id=row["external_recipient_id"],
        notification_id=notification_id,
    )


def evaluate(snapshot: AuthorizationSnapshot | None) -> AuthorizationDecision:
    """La matriz. ``None`` = no hay preferencia: nunca hubo consentimiento.

    El orden es el que ya tenía el selector y se conserva a propósito: es el contrato
    observable de ``SelectionResult.skipped_reason``, y cambiarlo reordenaría razones
    que P1B-3 ya reporta.
    """
    if snapshot is None:
        return _PREFERENCE_NOT_FOUND
    if not snapshot.preference_enabled:
        return _PREFERENCE_DISABLED
    if not snapshot.endpoint_verified:
        return _ENDPOINT_UNVERIFIED
    if not (snapshot.external_recipient_id or "").strip():
        return _ENDPOINT_MISSING_RECIPIENT
    if not snapshot.endpoint_enabled:
        return _ENDPOINT_DISABLED
    if snapshot.endpoint_revoked:
        return _ENDPOINT_REVOKED
    if snapshot.principal_status != PRINCIPAL_ACTIVE:
        return _PRINCIPAL_SUSPENDED
    if snapshot.radar_archived:
        return _RADAR_ARCHIVED
    if snapshot.owner_principal_id is None:
        return _OWNER_NOT_LINKED
    if snapshot.owner_principal_id != snapshot.principal_id:
        return _PRINCIPAL_MISMATCH
    return _AUTHORIZED


def authorize_row(
    row: sqlite3.Row | None, *, notification_id: str | None = None
) -> AuthorizationDecision:
    """Fila → decisión. ``None`` no puede dar snapshot: no hay endpoint que describir."""
    if row is None:
        return evaluate(None)
    return evaluate(snapshot_from_row(row, notification_id=notification_id))


def skip_reason(row: sqlite3.Row | None) -> str | None:
    """El contrato exacto que ``selector._target_skip_reason`` tenía: la razón, o
    ``None`` si se puede proceder. El selector no necesita la categoría —solo decide
    si sigue o no—, pero la razón que reporta sale ya de la misma matriz que el
    worker obedece."""
    decision = authorize_row(row)
    return None if decision.category == AUTHORIZED else decision.reason


__all__ = [
    "AUTHORIZED",
    "CATEGORIES",
    "HOLD",
    "SECURITY_BLOCKED",
    "TERMINAL",
    "TARGET_SQL",
    "AuthorizationDecision",
    "AuthorizationSnapshot",
    "authorize_row",
    "evaluate",
    "fetch_target_row",
    "skip_reason",
    "snapshot_from_row",
]
