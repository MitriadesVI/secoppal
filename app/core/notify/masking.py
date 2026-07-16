"""masking.py — la dirección de contacto nunca se persiste ni se loguea.

Un radar de compras públicas sabe qué busca cada persona; cruzar eso con su
teléfono o su correo convierte la base en un objetivo. Aquí la dirección entra,
se usa para derivar lo mínimo indispensable y no se guarda:

- ``address_hash``  — HMAC-SHA256, para *reconocer* que dos altas son el mismo
  destino (es lo que sostiene ``UNIQUE(channel, address_hash)``).
- ``address_last4`` — los últimos 4 caracteres, para que la persona identifique
  cuál de sus destinos es cuál en una lista.

Por qué HMAC y no SHA-256 pelado: el espacio de direcciones es enumerable. Un
teléfono colombiano son ~10 dígitos; un atacante con la base hashea todos los
números y los cruza en minutos. La clave rompe eso, y por eso es obligatoria e
inyectada: hardcodearla o dejarla opcional equivale a no tenerla.

El envío real necesitará la dirección; la resolverá el proveedor desde
``external_recipient_id`` (chat_id de Telegram, etc.), que no es reversible a un
teléfono sin las credenciales de ese proveedor. Eso es P1B-5, no este bloque.
"""
from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass

from app.core.auth import _normalize_email
from app.core.notify.schema import CHANNEL_EMAIL

# Longitud del hash truncado que aparece en logs. 8 hex = 32 bits: suficiente
# para correlacionar dos líneas del mismo log, inútil para invertir.
_LOG_DIGEST_CHARS = 8


def _normalize_address(channel: str, address: str) -> str:
    """Normaliza antes de hashear, o el UNIQUE no distingue lo mismo de lo mismo.

    ``Foo@Bar.com`` y ``foo@bar.com`` son el mismo buzón: sin normalizar darían
    dos hashes y la persona quedaría con dos endpoints para un solo destino.
    Para email se reutiliza la política de ``auth`` —la misma que decide qué
    cuenta es cuál al iniciar sesión— en vez de escribir una segunda que podría
    divergir de ella.

    Para telegram/whatsapp solo se recorta: normalizar teléfonos de verdad (E.164,
    prefijo país, ``+57`` vs ``57`` vs ``0057``) es una política propia, y no se
    inventa aquí sin un canal real que la ejercite. Queda como deuda declarada de
    P1B-5, donde el proveedor define el formato que acepta.
    """
    if channel == CHANNEL_EMAIL:
        return _normalize_email(address)
    return (address or "").strip()


def last4(address: str) -> str:
    """Últimos 4 caracteres, solo para reconocimiento humano en una lista."""
    return (address or "").strip()[-4:]


@dataclass(frozen=True)
class Masker:
    """Deriva identificadores no reversibles a partir de una clave inyectada.

    La clave viaja en el objeto y no en cada llamada a propósito: si
    ``safe_owner`` pudiera invocarse sin ella, alguien la llamaría sin clave
    desde un log y el resultado sería un digest enumerable. Aquí no existe esa
    ruta — sin clave no hay ``Masker``.
    """

    key: bytes

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError(
                "La clave HMAC de notificaciones es obligatoria: sin ella los "
                "hashes de direcciones son enumerables."
            )

    @classmethod
    def from_key(cls, key: str | bytes) -> "Masker":
        return cls(key.encode("utf-8") if isinstance(key, str) else key)

    def address_hash(self, channel: str, address: str) -> str:
        """HMAC-SHA256 de la dirección normalizada. Estable y no reversible."""
        normalized = _normalize_address(channel, address)
        return hmac.new(
            self.key, f"{channel}:{normalized}".encode("utf-8"), hashlib.sha256
        ).hexdigest()

    def safe_owner(self, owner_key: str) -> str:
        """Identificador de ``radars.owner`` apto para logs.

        ``radars.owner`` es un ``channel_user_key``: ``email:ana@x.com``,
        ``anon:abc`` o un chat_id suelto. Loguearlo crudo publica el correo en
        cada línea de diagnóstico. Se conserva el prefijo de canal —que es lo
        único útil para depurar— y el resto se sustituye por un digest con clave.

        No se hace enmascarado parcial (``a***@x.com``): el dominio más la
        inicial más la longitud identifican a una persona en un conjunto pequeño
        de usuarios, que es exactamente nuestro caso.
        """
        raw = (owner_key or "").strip()
        if not raw:
            return "anon:#empty"
        prefix, sep, rest = raw.partition(":")
        digest = hmac.new(
            self.key, raw.encode("utf-8"), hashlib.sha256
        ).hexdigest()[:_LOG_DIGEST_CHARS]
        return f"{prefix}:#{digest}" if sep else f"#{digest}"
