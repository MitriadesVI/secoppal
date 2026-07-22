"""endpoint_store.py — a dónde se le puede escribir a una persona, y si se puede.

Un endpoint es un destino (un chat de Telegram, un número, un correo) que un
principal **probó** que controla. Cinco condiciones lo hacen elegible, y son
independientes:

- ``verified_at``  — probó que el destino es suyo. Sin esto, cualquiera registra
  el número de otro y le manda notificaciones.
- ``external_recipient_id`` — el identificador con el que el proveedor entrega
  (chat_id de Telegram, wa_id). Sin él no hay a dónde enviar: un endpoint
  "elegible" sin destinatario es una promesa que reventaría en el momento del
  envío, ya con el mensaje formado y el usuario esperándolo.
- ``enabled``      — la persona lo quiere activo ahora (pausa temporal).
- ``revoked_at``   — la persona lo dio de baja (definitivo, pero sin borrar).
- ``principal.status`` — la cuenta no está suspendida.

La revocación no borra la fila: la prueba de que ese destino existió y con qué
consentimiento es justamente lo que hay que conservar para responder "¿por qué le
escribieron?". Borrar destruiría esa prueba.

Ninguna de las cinco condiciones se comprueba en Python: viven todas en el SQL de
``_ELIGIBLE_WHERE``, en un solo sitio, porque un selector que reconstruya el
filtro por su cuenta acabará olvidando una y escribiéndole a alguien que revocó.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from app.core.db import connect_sqlite
from app.core.notify.masking import Masker, last4
from app.core.notify.schema import CHANNELS, ensure_schema, new_id, now_iso


class EndpointError(Exception):
    """Error de negocio de endpoints."""


class EndpointExistsError(EndpointError):
    """Ese destino ya está registrado en ese canal."""


@dataclass(frozen=True)
class Endpoint:
    endpoint_id: str
    principal_id: str
    channel: str
    address_hash: str
    address_last4: str | None
    external_recipient_id: str | None
    verified_at: str | None
    enabled: int
    created_at: str
    revoked_at: str | None

    @property
    def is_verified(self) -> bool:
        return self.verified_at is not None

    @property
    def is_revoked(self) -> bool:
        return self.revoked_at is not None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Endpoint":
        return cls(**{k: row[k] for k in row.keys()})


# La definición única de "se le puede escribir". Todo lo que decida enviar algo
# tiene que pasar por aquí.
#
# El ``<> ''`` no es paranoia: ``NOT NULL`` no distingue el string vacío, y un
# recipient id vacío pasa cualquier chequeo de presencia mientras es igual de
# inservible para entregar.
_ELIGIBLE_WHERE = """
    e.verified_at IS NOT NULL
AND e.external_recipient_id IS NOT NULL
AND e.external_recipient_id <> ''
AND e.enabled = 1
AND e.revoked_at IS NULL
AND p.status = 'active'
"""

_ELIGIBLE_SQL = f"""
SELECT e.* FROM notification_endpoints e
  JOIN principals p ON p.principal_id = e.principal_id
 WHERE {_ELIGIBLE_WHERE}
   AND ({{scope}})
 ORDER BY e.created_at
"""


class EndpointStore:
    def __init__(self, db_path: str | Path, *, masker: Masker) -> None:
        """``masker`` es obligatorio: sin él este store guardaría direcciones."""
        self.db_path = Path(db_path)
        self._masker = masker
        conn = self._connect()
        try:
            ensure_schema(conn)
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def create_endpoint(
        self,
        *,
        principal_id: str,
        channel: str,
        address: str,
    ) -> str:
        """Registra un destino. ``address`` se usa y se descarta.

        Nace sin verificar y **sin** ``external_recipient_id``: el alta declara la
        intención, no la prueba. Ambos datos aparecen juntos y solo entonces
        (``mark_verified``), porque el recipient id lo emite el proveedor
        precisamente al completar el desafío —antes de eso no existe, y
        aceptarlo aquí sería dejar que el caller lo invente.
        """
        if channel not in CHANNELS:
            raise EndpointError(
                f"Canal inválido '{channel}'. Válidos: {', '.join(CHANNELS)}."
            )
        if not (address or "").strip():
            raise EndpointError("La dirección del endpoint no puede estar vacía.")
        endpoint_id = new_id()
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO notification_endpoints
                   (endpoint_id, principal_id, channel, address_hash, address_last4,
                    external_recipient_id, verified_at, enabled, created_at, revoked_at)
                   VALUES (?, ?, ?, ?, ?, NULL, NULL, 1, ?, NULL)""",
                (endpoint_id, principal_id, channel,
                 self._masker.address_hash(channel, address), last4(address),
                 now_iso()),
            )
        except sqlite3.IntegrityError as exc:
            # Distingue el UNIQUE(channel, address_hash) de la FK a principals:
            # "ya existe" y "no existe la persona" son errores distintos.
            if "FOREIGN KEY" in str(exc).upper():
                raise EndpointError(
                    f"No existe el principal '{principal_id}'."
                ) from exc
            raise EndpointExistsError(
                f"Ese destino ya está registrado en el canal '{channel}'."
            ) from exc
        finally:
            conn.close()
        return endpoint_id

    def get_endpoint(self, endpoint_id: str) -> Endpoint | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM notification_endpoints WHERE endpoint_id = ?",
                (endpoint_id,),
            ).fetchone()
        finally:
            conn.close()
        return Endpoint.from_row(row) if row else None

    def find_endpoint(
        self, principal_id: str, channel: str, address: str
    ) -> Endpoint | None:
        """El endpoint **exacto** de esa persona para ese destino, o ``None``.

        Compara por el ``address_hash`` canónico —el mismo digest que decide el
        ``UNIQUE(channel, address_hash)`` del alta—, nunca por ``address_last4`` ni
        por la dirección desenmascarada. ``last4`` colisiona (dos chats con los
        mismos cuatro dígitos comparten sufijo) y la dirección cruda no se guarda:
        un lookup por cualquiera de las dos confundiría destinos distintos o
        obligaría a desenmascarar. El hash es la identidad, y es la que se consulta.
        """
        if channel not in CHANNELS:
            raise EndpointError(
                f"Canal inválido '{channel}'. Válidos: {', '.join(CHANNELS)}."
            )
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM notification_endpoints "
                "WHERE principal_id = ? AND channel = ? AND address_hash = ?",
                (principal_id, channel, self._masker.address_hash(channel, address)),
            ).fetchone()
        finally:
            conn.close()
        return Endpoint.from_row(row) if row else None

    def list_endpoints(self, principal_id: str) -> list[Endpoint]:
        """Todos, incluidos revocados: es la vista de gestión, no la de envío."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM notification_endpoints WHERE principal_id = ? "
                "ORDER BY created_at",
                (principal_id,),
            ).fetchall()
        finally:
            conn.close()
        return [Endpoint.from_row(r) for r in rows]

    def mark_verified(
        self,
        endpoint_id: str,
        *,
        external_recipient_id: str,
        ts: str | None = None,
    ) -> None:
        """Cierra la verificación: prueba de posesión **y** destinatario, juntos.

        ``external_recipient_id`` es obligatorio y se escribe en la misma
        sentencia que ``verified_at``. Si fuera un paso aparte —o un parámetro
        opcional— existiría un endpoint verificado sin destinatario: elegible para
        el selector, imposible de entregar. Aquí ese estado no se puede
        representar.

        Cinco casos, y ninguno es silencioso salvo el que debe serlo:

        - **inexistente** → error. Un UPDATE que afecta 0 filas "tiene éxito", y
          el caller creería haber verificado algo que no existe.
        - **revocado** → error, sin escribir. La revocación es la última palabra
          de la persona; verificar después reescribiría su historia para dejarla
          "verificada tras darse de baja". No basta con que siga inelegible: lo
          que consta es lo que se podrá auditar mañana.
        - **ya verificado con el mismo recipient** → no-op idempotente. Es el
          reintento legítimo (el proveedor reenvía el callback, la red duplica).
        - **ya verificado con otro recipient** → error. Es un intento de redirigir
          un destino probado hacia otro chat: o un bug, o alguien apropiándose de
          las notificaciones de otra persona. Antes esto era un no-op silencioso;
          fallar hace visible el intento en vez de esconderlo.
        - **sin verificar** → escribe fecha y recipient juntos.

        Todo dentro de ``BEGIN IMMEDIATE``: leer el estado y decidir sobre él son
        un solo hecho. Con un SELECT suelto, una revocación concurrente cabría
        entre la lectura y la escritura, y verificaríamos igual sobre un endpoint
        ya dado de baja.
        """
        recipient = (external_recipient_id or "").strip()
        if not recipient:
            raise EndpointError(
                "external_recipient_id es obligatorio al verificar: sin él el "
                "endpoint quedaría elegible y no habría a dónde entregar."
            )
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT verified_at, external_recipient_id, revoked_at "
                    "  FROM notification_endpoints WHERE endpoint_id = ?",
                    (endpoint_id,),
                ).fetchone()
                if row is None:
                    raise EndpointError(f"No existe el endpoint '{endpoint_id}'.")
                if row["revoked_at"] is not None:
                    raise EndpointError(
                        f"El endpoint '{endpoint_id}' está revocado: no puede "
                        f"verificarse. Registra uno nuevo."
                    )
                if row["verified_at"] is not None:
                    if (row["external_recipient_id"] or "") != recipient:
                        raise EndpointError(
                            f"El endpoint '{endpoint_id}' ya está verificado con "
                            f"otro destinatario. Cambiarlo redirigiría las "
                            f"notificaciones a otro destino."
                        )
                    conn.execute("COMMIT")      # reintento del mismo hecho
                    return
                conn.execute(
                    "UPDATE notification_endpoints "
                    "   SET verified_at = ?, external_recipient_id = ? "
                    " WHERE endpoint_id = ?",
                    (ts or now_iso(), recipient, endpoint_id),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    def set_enabled(self, endpoint_id: str, enabled: bool) -> None:
        """Pausa reversible. No toca ``revoked_at``: son ejes distintos."""
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE notification_endpoints SET enabled = ? WHERE endpoint_id = ?",
                (1 if enabled else 0, endpoint_id),
            )
        finally:
            conn.close()

    def revoke(self, endpoint_id: str, ts: str | None = None) -> None:
        """Da de baja sin borrar. Idempotente: no repisa la fecha original.

        La primera revocación es la que importa —es la que dice desde cuándo no
        se le puede escribir—, así que una segunda llamada no la mueve.
        """
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE notification_endpoints SET revoked_at = ? "
                "WHERE endpoint_id = ? AND revoked_at IS NULL",
                (ts or now_iso(), endpoint_id),
            )
        finally:
            conn.close()

    def list_eligible_endpoints(self, principal_id: str) -> list[Endpoint]:
        conn = self._connect()
        try:
            rows = conn.execute(
                _ELIGIBLE_SQL.format(scope="e.principal_id = ?"), (principal_id,)
            ).fetchall()
        finally:
            conn.close()
        return [Endpoint.from_row(r) for r in rows]

    def is_eligible(self, endpoint_id: str) -> bool:
        """Misma definición que el listado, no una copia en Python."""
        conn = self._connect()
        try:
            row = conn.execute(
                _ELIGIBLE_SQL.format(scope="e.endpoint_id = ?"), (endpoint_id,)
            ).fetchone()
        finally:
            conn.close()
        return row is not None
