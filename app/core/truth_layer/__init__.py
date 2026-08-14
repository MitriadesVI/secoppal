"""Pure, dormant SECOP Truth Layer foundation.

The raw contract resolvers exported here are low-level semantic authorities.
Operational consumers must compose them through observe_contract_truth_axes so
planned liquidation dates are grounded in an injected reference instant. No
runtime, workflow, query, modality or experiment code is imported here.
"""

from app.core.truth_layer.contract_changes import (
    ChangeAxisAssessment,
    ChangeKind,
    ChangeSignal,
    ContractChangeAssessment,
    HistoricalChangeConclusion,
    SourceCoverage,
    resolve_contract_changes,
)
from app.core.truth_layer.contract_lifecycle import (
    ContractLifecycleAssessment,
    compose_contract_lifecycle,
)
from app.core.truth_layer.contract_liquidation import (
    ContractLiquidationAssessment,
    LiquidationCompletion,
    LiquidationStatus,
    resolve_contract_liquidation,
)
from app.core.truth_layer.contract_performance import (
    CONTRACT_DATASET_ID,
    ContractPerformanceAssessment,
    ContractPerformanceState,
    resolve_contract_performance,
)
from app.core.truth_layer.contract_signature import (
    ContractSignatureAssessment,
    SignatureDateStatus,
    SignatureStatus,
    resolve_contract_signature,
)
from app.core.truth_layer.evidence import (
    AssessmentConflict,
    ConflictSeverity,
    Evidence,
    EvidenceRole,
    ResolutionCompleteness,
    ResolutionConfidence,
    ResolutionConsistency,
    ResolutionQuality,
    SourceError,
    SourceErrorKind,
    build_resolution_quality,
    validate_evidence_set,
)
from app.core.truth_layer.process_lifecycle import (
    PROCESS_DATASET_ID,
    AwardId,
    AwardIdentityExtraction,
    AwardIdentityStatus,
    AwardResolutionSignal,
    ContractLinkSignal,
    ContractLookupResult,
    ProcessLifecycleAssessment,
    ProcessLifecycleStage,
    ProcessResolutionAssessment,
    ProcessResolutionStatus,
    extract_award_identity,
    resolve_process_lifecycle,
    resolve_process_resolution,
)
from app.core.truth_layer.models import (
    ContractLifecycleSignals,
    FieldObservation,
    ObservationPresence,
    ProcessContractReference,
    ProcessLifecycleSignals,
    PublicationVisibilitySignals,
    RawScalar,
    TemporalQueryContext,
)
from app.core.truth_layer.local_wall_time import (
    LocalWallTimeResolution,
    LocalWallTimeResolver,
    LocalWallTimeStatus,
)
from app.core.truth_layer.temporal_range import (
    RangeMembership,
    TemporalEvent,
    TemporalRelation,
)
from app.core.truth_layer.temporal_contract import (
    LiquidationTemporalInput,
    SignatureTemporalInput,
    resolve_liquidation_event,
    resolve_signature_event,
)
from app.core.truth_layer.temporal_resolver import (
    AXIS_REASON_CODES,
    TemporalAssessment,
    build_temporal_assessment,
    resolve_temporal_assessment,
)
from app.core.truth_layer.temporal_value import (
    SECOP_SOURCE_TIMEZONE,
    TemporalValueResolution,
    TemporalValueStatus,
    resolve_temporal_value,
)
from app.core.truth_layer.visibility_resolver import (
    VISIBILITY_DATASET_ID,
    PublicationVisibility,
    PublicationVisibilityAssessment,
    UrlVerdict,
    resolve_publication_visibility,
)
from app.core.truth_layer.sentinels import (
    ComparisonMode,
    FieldSentinelPolicy,
    SentinelAssessment,
    SentinelRule,
    SentinelStatus,
    UnknownValueDisposition,
    assess_sentinel,
    get_field_sentinel_policy,
)
from app.core.truth_layer.observers import (
    observe_contract_truth_axes,
    observe_process_truth_axes,
)

__all__ = [
    # T1A — transport (8 exports, preserved exactly).
    "ContractLifecycleSignals",
    "FieldObservation",
    "ObservationPresence",
    "ProcessContractReference",
    "ProcessLifecycleSignals",
    "PublicationVisibilitySignals",
    "RawScalar",
    "TemporalQueryContext",
    # T1B-0A — field sentinel semantics (8 exports, preserved exactly).
    "SentinelStatus",
    "UnknownValueDisposition",
    "ComparisonMode",
    "SentinelRule",
    "FieldSentinelPolicy",
    "SentinelAssessment",
    "assess_sentinel",
    "get_field_sentinel_policy",
    # T1B-1 — evidence, conflicts and resolution quality (12 exports, new).
    "EvidenceRole",
    "SourceErrorKind",
    "ConflictSeverity",
    "ResolutionCompleteness",
    "ResolutionConsistency",
    "ResolutionConfidence",
    "SourceError",
    "Evidence",
    "AssessmentConflict",
    "ResolutionQuality",
    "validate_evidence_set",
    "build_resolution_quality",
    # T1B-2 — process stage and resolution (14 exports, new).
    "PROCESS_DATASET_ID",
    "ProcessLifecycleStage",
    "AwardResolutionSignal",
    "ContractLinkSignal",
    "ProcessResolutionStatus",
    "AwardIdentityStatus",
    "AwardId",
    "AwardIdentityExtraction",
    "ContractLookupResult",
    "ProcessLifecycleAssessment",
    "ProcessResolutionAssessment",
    "extract_award_identity",
    "resolve_process_lifecycle",
    "resolve_process_resolution",
    # T1B-3 — independent contract axes and authority-free aggregation.
    "CONTRACT_DATASET_ID",
    "ContractPerformanceState",
    "ContractPerformanceAssessment",
    "resolve_contract_performance",
    "SignatureStatus",
    "SignatureDateStatus",
    "ContractSignatureAssessment",
    "resolve_contract_signature",
    "ChangeKind",
    "ChangeSignal",
    "SourceCoverage",
    "HistoricalChangeConclusion",
    "ChangeAxisAssessment",
    "ContractChangeAssessment",
    "resolve_contract_changes",
    "LiquidationStatus",
    "LiquidationCompletion",
    "ContractLiquidationAssessment",
    "resolve_contract_liquidation",
    "ContractLifecycleAssessment",
    "compose_contract_lifecycle",
    # T1B-0C/T1B-4 — temporal validity, relations and the wall-time boundary.
    "SECOP_SOURCE_TIMEZONE",
    "TemporalValueStatus",
    "TemporalRelation",
    "RangeMembership",
    "TemporalEvent",
    "TemporalValueResolution",
    "TemporalAssessment",
    "LocalWallTimeStatus",
    "LocalWallTimeResolution",
    "LocalWallTimeResolver",
    "AXIS_REASON_CODES",
    "SignatureTemporalInput",
    "LiquidationTemporalInput",
    "resolve_temporal_value",
    "resolve_temporal_assessment",
    "build_temporal_assessment",
    "resolve_signature_event",
    "resolve_liquidation_event",
    # T1B-5 — public visibility.
    "VISIBILITY_DATASET_ID",
    "PublicationVisibility",
    "UrlVerdict",
    "PublicationVisibilityAssessment",
    "resolve_publication_visibility",
    # M1 — safe, pure, dormant product seams.
    "observe_process_truth_axes",
    "observe_contract_truth_axes",
]
