"""notify — identidad, destinos, consentimiento y outbox de las notificaciones
del Radar.

Ocho tablas y responsabilidades separadas:

P1B-1 — identidad y consentimiento:

- ``schema``           — el DDL de las ocho, en un solo sitio.
- ``identity_store``   — quién es la persona y qué ``radars.owner`` le pertenecen.
- ``endpoint_store``   — a dónde se le puede escribir, y si se puede.
- ``preference_store`` — qué aceptó recibir en cada radar (opt-in, apagado por
  defecto).
- ``masking``          — la dirección no se persiste ni se loguea.

P1B-2 — outbox, idempotencia, claim y ledger de intentos:

- ``idempotency``   — identidad determinista de un batch (funciones puras).
- ``outbox_store``  — la intención de entrega, atómica e idempotente.
- ``claim``         — exclusión mutua efímera sobre un batch (SQL puro).
- ``ledger_store``  — intentos de entrega, write-ahead antes/después del envío.

P1B-3 — selección shadow-only:

- ``grouping`` — colapso puro por resultado y orden determinista.
- ``render``   — payload congelado y texto puro por canal.
- ``selector`` — cursor, consentimiento, quiet hours y materialización atómica.

P1B-4 R2A — núcleo transaccional del worker:

- ``authorization`` — la única definición de "¿se puede entregar esto ahora?",
  compartida por el selector y el worker.
- ``retry``         — aritmética pura del tiempo del claim (funciones puras).
- ``worker_store``  — reclamar, autorizar+abrir intento, reconciliar huérfanos y
  barrer expirados. Todo transaccional, nada de red.

Este paquete **todavía no envía nada**: no hay worker ni proveedor. El selector
materializa intenciones reales en el outbox y ``worker_store`` prepara el terreno
para que, cuando exista un worker, no pueda mentir sobre lo que pasó.
"""
from app.core.notify.authorization import (
    AuthorizationDecision,
    AuthorizationSnapshot,
)
from app.core.notify.claim import is_owned as claim_is_owned
from app.core.notify.claim import release as claim_release
from app.core.notify.claim import try_claim
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
from app.core.notify.idempotency import (
    NOTIFICATION_POLICY_VERSION,
    batch_key,
    canonical_event_set,
    idempotency_key,
)
from app.core.notify.grouping import (
    GroupedRadarEvent,
    SelectableRadarEvent,
    group_events,
)
from app.core.notify.ledger_store import (
    AttemptNotDispatchingError,
    ClaimLostError,
    DeliveryAttempt,
    LedgerError,
    LedgerStore,
)
from app.core.notify.masking import Masker, last4
from app.core.notify.outbox_store import (
    BatchEventRef,
    BatchMaterialization,
    DuplicateEventAssociationError,
    NotificationBatch,
    OutboxError,
    OutboxStore,
)
from app.core.notify.render import (
    PAYLOAD_VERSION,
    RenderedMessage,
    build_payload,
    render_payload,
    serialize_payload,
)
from app.core.notify.selector import (
    EligibilityFlags,
    NotificationSelector,
    SelectionResult,
    SelectionTarget,
    SelectorError,
    expires_at_for_frequency,
    is_event_eligible,
    quiet_hours_not_before,
)
from app.core.notify.preference_store import (
    OwnerNotLinkedError,
    Preference,
    PreferenceError,
    PreferenceExistsError,
    PreferenceStore,
    PrincipalMismatchError,
    RadarNotAvailableError,
)
from app.core.notify.retry import (
    DEFAULT_CLAIM_DURATION,
    DEFAULT_RECONCILIATION_CLAIM_DURATION,
    claim_duration,
    reconciliation_claim_duration,
)
from app.core.notify.schema import CHANNELS, FREQUENCIES, ensure_schema
from app.core.notify.worker_store import (
    ClaimedCandidate,
    DispatchAuthorization,
    ReconciliationClaim,
    WorkerStore,
    WorkerStoreConsistencyError,
    WorkerStoreError,
    external_idempotency_key,
)

__all__ = [
    "NOTIFICATION_POLICY_VERSION",
    "CHANNELS",
    "DEFAULT_CLAIM_DURATION",
    "DEFAULT_RECONCILIATION_CLAIM_DURATION",
    "FREQUENCIES",
    "AttemptNotDispatchingError",
    "AuthorizationDecision",
    "AuthorizationSnapshot",
    "ClaimedCandidate",
    "DispatchAuthorization",
    "ReconciliationClaim",
    "WorkerStore",
    "WorkerStoreConsistencyError",
    "WorkerStoreError",
    "BatchEventRef",
    "BatchMaterialization",
    "ClaimLostError",
    "DeliveryAttempt",
    "DuplicateEventAssociationError",
    "Endpoint",
    "EndpointError",
    "EndpointExistsError",
    "EndpointStore",
    "EligibilityFlags",
    "GroupedRadarEvent",
    "IdentityError",
    "IdentityStore",
    "LedgerError",
    "LedgerStore",
    "Masker",
    "NotificationSelector",
    "NotificationBatch",
    "OutboxError",
    "OutboxStore",
    "PAYLOAD_VERSION",
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
    "RenderedMessage",
    "SelectableRadarEvent",
    "SelectionResult",
    "SelectionTarget",
    "SelectorError",
    "batch_key",
    "canonical_event_set",
    "claim_duration",
    "claim_is_owned",
    "claim_release",
    "ensure_schema",
    "expires_at_for_frequency",
    "external_idempotency_key",
    "reconciliation_claim_duration",
    "group_events",
    "idempotency_key",
    "is_event_eligible",
    "last4",
    "build_payload",
    "quiet_hours_not_before",
    "render_payload",
    "serialize_payload",
    "try_claim",
]
