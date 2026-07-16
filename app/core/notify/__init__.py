"""notify — identidad, destinos y consentimiento de las notificaciones del Radar.

Cuatro tablas y cuatro responsabilidades separadas (P1B-1):

- ``schema``           — el DDL de las cuatro, en un solo sitio.
- ``identity_store``   — quién es la persona y qué ``radars.owner`` le pertenecen.
- ``endpoint_store``   — a dónde se le puede escribir, y si se puede.
- ``preference_store`` — qué aceptó recibir en cada radar (opt-in, apagado por
  defecto).
- ``masking``          — la dirección no se persiste ni se loguea.

Este paquete **no envía nada**: no hay outbox, selector, worker ni proveedor.
Solo responde de quién es cada radar y quién consintió qué.
"""
from app.core.notify.endpoint_store import (
    Endpoint,
    EndpointError,
    EndpointExistsError,
    EndpointStore,
)
from app.core.notify.identity_store import (
    IdentityError,
    IdentityStore,
    OwnerKeyConflictError,
    Principal,
    PrincipalExistsError,
)
from app.core.notify.masking import Masker, last4
from app.core.notify.preference_store import (
    OwnerNotLinkedError,
    Preference,
    PreferenceError,
    PreferenceExistsError,
    PreferenceStore,
    PrincipalMismatchError,
    RadarNotAvailableError,
)
from app.core.notify.schema import CHANNELS, FREQUENCIES, ensure_schema

__all__ = [
    "CHANNELS",
    "FREQUENCIES",
    "Endpoint",
    "EndpointError",
    "EndpointExistsError",
    "EndpointStore",
    "IdentityError",
    "IdentityStore",
    "Masker",
    "OwnerKeyConflictError",
    "OwnerNotLinkedError",
    "Preference",
    "PreferenceError",
    "PreferenceExistsError",
    "PreferenceStore",
    "Principal",
    "PrincipalExistsError",
    "PrincipalMismatchError",
    "RadarNotAvailableError",
    "ensure_schema",
    "last4",
]
