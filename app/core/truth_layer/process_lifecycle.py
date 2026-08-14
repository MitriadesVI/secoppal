"""SECOP Truth Layer T1B-2 — Process stage and process resolution.

Two INDEPENDENT axes over the same process row, resolved by two separate
functions with separate types, helpers and tests:

    resolve_process_lifecycle    -> ProcessLifecycleAssessment  (textual stage)
    resolve_process_resolution   -> ProcessResolutionAssessment (award + link)

Pure and deterministic: no clock, no I/O, no network, no filesystem, no
environment, no parser, no formatter, no Radar, no workflow, no SECOP client.
This module performs NO lookup — it consumes the pure result of a contract
lookup that some caller already completed.

Hard boundaries (binding contract §4, architecture §5):
  * ``estado_del_procedimiento`` is the primary interpretable signal;
  * ``id_estado_del_procedimiento`` is preserved as evidence only — it never
    arbitrates, orders or contradicts the text, and emits no per-row reason
    code. Its limitation lives in capability metadata;
  * ``estado_de_apertura_del_proceso="Abierto"`` NEVER determines the stage
    on its own and never proves that a process is still live; it is kept as
    corroborating evidence. This is a different field from
    ``estado_del_procedimiento="Abierto"``, which DOES take part in the
    precedence that yields ``published``;
  * award flag, award identity and a linked contract NEVER rewrite the stage;
  * every conclusion is DERIVED, never asserted: the assessments recompute
    stage, award signal and resolution status in ``__post_init__`` and reject
    an instance its own inputs contradict, so no public constructor —
    ``dataclasses.replace`` included — can mint an incoherent state;
  * conflicts are preserved in ``ResolutionQuality``, never silently dropped,
    and a conflict is never manufactured out of what happens to be at hand:
    a lookup that reports a contradiction must carry the contradictory
    evidence it found;
  * a cataloged field that the sentinel layer could not classify says so with
    an explicit reason code and may not be reported as complete/high;
  * a timeout or transport failure is ``source_error``, never
    ``absent_after_completed_lookup`` and never an absence;
  * no date is used to claim that a process is still live, and modality is
    not used to decide offerability — both belong to later blocks.

Binding documents, read in full before edit:
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_SEMANTIC_CONTRACT.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_EXECUTION_ROADMAP.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R1.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R2.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R3.md
"""
from __future__ import annotations

import unicodedata
from dataclasses import InitVar, dataclass, field
from typing import Literal, NewType

from app.core.truth_layer.evidence import (
    AssessmentConflict,
    Evidence,
    EvidenceRole,
    ResolutionQuality,
    SourceError,
    build_resolution_quality,
    validate_evidence_set,
)

# Sibling-module private helper, imported deliberately: `AssessmentConflict`
# rejects a non-canonical evidence order, so the canonical key must be the
# very one T1B-1 validates against. Re-deriving it here would be a second
# source of truth that could silently drift. Candidate for promotion to
# public API in a later block; `evidence.py` is frozen in this one.
from app.core.truth_layer.evidence import _evidence_sort_key
from app.core.truth_layer.models import FieldObservation, ProcessLifecycleSignals
from app.core.truth_layer.sentinels import (
    SentinelAssessment,
    assess_sentinel,
    get_field_sentinel_policy,
)

__all__ = [
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
]


PROCESS_DATASET_ID = "p6dx-8zbt"

# Field names of the processes dataset. Only two of them carry an authorized
# sentinel policy (``id_adjudicacion``, ``estado_resumen``); the rest produce
# evidence with no sentinel assessment attached, which is valid and does NOT
# mean "meaningful".
_FIELD_PROCEDURE_STATE = "estado_del_procedimiento"
_FIELD_PROCEDURE_STATE_ID = "id_estado_del_procedimiento"
_FIELD_OPENING_STATE = "estado_de_apertura_del_proceso"
_FIELD_PHASE = "fase"
_FIELD_SUMMARY_STATE = "estado_resumen"
_FIELD_ADJUDICATED = "adjudicado"
_FIELD_ADJUDICATION_DATE = "fecha_adjudicacion"
_FIELD_ADJUDICATION_ID = "id_adjudicacion"
_LIFECYCLE_EVIDENCE_ORDER = (
    _FIELD_OPENING_STATE,
    _FIELD_PROCEDURE_STATE,
    _FIELD_SUMMARY_STATE,
    _FIELD_PHASE,
    _FIELD_PROCEDURE_STATE_ID,
)
_RESOLUTION_EVIDENCE_ORDER = (
    _FIELD_ADJUDICATED,
    _FIELD_ADJUDICATION_DATE,
    _FIELD_ADJUDICATION_ID,
)


# ---------------------------------------------------------------------------
# Literals (public contract).
# ---------------------------------------------------------------------------

ProcessLifecycleStage = Literal[
    "draft",
    "approval",
    "published",
    "offer_phase_observed",
    "evaluation",
    "selected",
    "cancelled",
    "suspended",
    "unknown",
]

AwardResolutionSignal = Literal[
    "observed",
    "not_indicated",
    "unknown",
    "conflicting",
]

ContractLinkSignal = Literal[
    "observed",
    "absent_after_completed_lookup",
    "not_queried",
    "source_error",
    "unknown",
    "conflicting",
]

ProcessResolutionStatus = Literal[
    "resolved",
    "unresolved",
    "unknown",
    "conflicting",
]

AwardIdentityStatus = Literal[
    "extracted",
    "absent",
    "unknown",
    "unrecognized",
]

AwardId = NewType("AwardId", str)

_VALID_STAGES: frozenset[str] = frozenset(
    {
        "draft",
        "approval",
        "published",
        "offer_phase_observed",
        "evaluation",
        "selected",
        "cancelled",
        "suspended",
        "unknown",
    }
)
_VALID_AWARD_SIGNALS: frozenset[str] = frozenset(
    {"observed", "not_indicated", "unknown", "conflicting"}
)
_VALID_CONTRACT_LINK_SIGNALS: frozenset[str] = frozenset(
    {
        "observed",
        "absent_after_completed_lookup",
        "not_queried",
        "source_error",
        "unknown",
        "conflicting",
    }
)
_VALID_RESOLUTION_STATUSES: frozenset[str] = frozenset(
    {"resolved", "unresolved", "unknown", "conflicting"}
)
_VALID_IDENTITY_STATUSES: frozenset[str] = frozenset(
    {"extracted", "absent", "unknown", "unrecognized"}
)

# Which sentinel classifications may back each identity status. A whitelist,
# so a future ``SentinelStatus`` cannot silently become an admissible backing.
# ``meaningful`` appears under ``unknown`` too because it does not decide on
# its own: a meaningful-but-empty or non-string value still yields ``unknown``.
_SENTINEL_STATUS_FOR_IDENTITY: dict[str, frozenset[str]] = {
    "extracted": frozenset({"meaningful"}),
    "absent": frozenset({"sentinel_absence"}),
    "unrecognized": frozenset({"unrecognized"}),
    "unknown": frozenset({"meaningful", "sentinel_unknown", "not_applicable"}),
}

# Statuses that are a *sentinel verdict* and therefore cannot be claimed
# without the assessment that produced them. ``unknown`` is excluded: it also
# covers a non-observation, an unavailable policy and a type mismatch, none of
# which can produce an assessment at all.
_IDENTITY_STATUS_REQUIRES_ASSESSMENT: frozenset[str] = frozenset(
    {"extracted", "absent", "unrecognized"}
)

# How a lookup outcome maps onto T1A presence. ``unknown`` and ``conflicting``
# are deliberately absent: neither can be described by a single presence.
_LOOKUP_OUTCOME_PRESENCE: dict[str, str] = {
    "observed": "observed",
    "absent_after_completed_lookup": "missing",
    "not_queried": "not_observed",
    "source_error": "not_observed",
}


# ---------------------------------------------------------------------------
# Domain vocabulary (explicit catalogs, never substring matching).
# ---------------------------------------------------------------------------

# ``estado_del_procedimiento`` literals. "Abierto" is a genuine procedure
# state here and must not be confused with the opening-state field.
_STATE_CANCELLED = frozenset({"cancelado"})
_STATE_SUSPENDED = frozenset({"suspendido"})
_STATE_SELECTED = frozenset({"seleccionado"})
_STATE_EVALUATION = frozenset({"evaluacion"})
_STATE_DRAFT = frozenset({"borrador"})
_STATE_APPROVAL = frozenset({"en aprobacion", "aprobado"})
_STATE_PUBLISHED = frozenset({"publicado", "abierto"})

# ``fase`` literals that denote an offer window. NEVER matched by the
# substring "oferta": "presentación de ofertas" and "selección de ofertas"
# share the word and mean the opposite.
_PHASE_OFFER = frozenset(
    {
        "presentacion de oferta",
        "presentacion de ofertas",
        "fase de ofertas",
        "fase de seleccion presentacion de ofertas",
    }
)

_AFFIRMATIVE = frozenset({"si"})
_NEGATIVE = frozenset({"no"})

_POST_OFFER_STAGES: frozenset[str] = frozenset(
    {"evaluation", "selected", "cancelled", "suspended"}
)


def _fold(value: object) -> str:
    """Accent-insensitive, case-insensitive comparison form.

    Comparison only. The raw observation is never modified nor replaced;
    every assessment keeps the untouched ``FieldObservation``.
    """
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value).casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


def _observed_text(observation: FieldObservation) -> str:
    if observation.presence != "observed":
        return ""
    return _fold(observation.raw_value)


def _role_for(observation: FieldObservation) -> EvidenceRole:
    if observation.presence == "missing":
        return "missing"
    if observation.presence == "not_observed":
        return "not_observed"
    return "supports"


_STRING_COMPARISON_MODES = frozenset({"string_casefold", "string_casefold_strip"})


def _comparable_by(comparison_mode: str, raw_value: object) -> bool:
    """Can this policy's comparator actually classify this raw scalar?

    A string comparator raises on a non-string raw by design (T1B-0A refuses
    to collapse a type mismatch into a sentinel). A resolver must not die on
    an unexpected column type, so the mismatch is surfaced as evidence
    WITHOUT an assessment plus an explicit reason code — never as a silent
    sentinel and never as an absence.
    """
    if comparison_mode in _STRING_COMPARISON_MODES:
        return isinstance(raw_value, str)
    return True


def _build_evidence(
    field_name: str,
    observation: FieldObservation,
    *,
    code: str,
    dataset_id: str,
    role: EvidenceRole | None = None,
) -> Evidence:
    """Build evidence, attaching the sentinel assessment when a policy exists.

    This is the ONLY place that reaches the sentinel catalog. When the field
    has an authorized policy and a scalar was observed, the assessment is
    produced by T1B-0A and attached — the resolver never bypasses that layer.
    When there is no policy, evidence is emitted without an assessment, which
    means "no sentinel classification attached", never "meaningful".
    """
    assessment: SentinelAssessment | None = None
    normalized = None
    policy = get_field_sentinel_policy(dataset_id, field_name)
    if (
        policy is not None
        and observation.presence == "observed"
        and _comparable_by(policy.comparison_mode, observation.raw_value)
    ):
        assessment = assess_sentinel(observation, policy)
        normalized = assessment.normalized_comparison_value
    return Evidence(
        code=code,
        dataset=dataset_id,
        field=field_name,
        raw_observation=observation,
        sentinel_assessment=assessment,
        normalized_value=normalized,
        role=_role_for(observation) if role is None else role,
    )


def _canonical_evidence(evidences: list[Evidence]) -> tuple[Evidence, ...]:
    return tuple(sorted(evidences, key=_evidence_sort_key))


def _summary_sentinel_gap(
    observation: FieldObservation, *, dataset_id: str
) -> str | None:
    """Why an observed ``estado_resumen`` carries no sentinel classification.

    ``estado_resumen`` has a productive sentinel policy, so it must never slip
    past the sentinel layer in silence. Two situations legitimately leave the
    evidence without an assessment — the policy is unavailable for this
    dataset, or its comparator cannot read the observed type — and both must
    become an explicit reason code that forbids a ``complete``/``high``
    quality. Returning ``None`` means the field WAS classified.
    """
    if observation.presence != "observed":
        return None
    policy = get_field_sentinel_policy(dataset_id, _FIELD_SUMMARY_STATE)
    if policy is None:
        return "PROCESS_SUMMARY_SENTINEL_POLICY_UNAVAILABLE"
    if not _comparable_by(policy.comparison_mode, observation.raw_value):
        return "PROCESS_SUMMARY_SENTINEL_TYPE_MISMATCH"
    return None


# ---------------------------------------------------------------------------
# Award identity extraction — the single boundary that may produce an AwardId.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AwardIdentityExtraction:
    """The only boundary allowed to turn a raw scalar into an ``AwardId``.

    Only a sentinel ``meaningful`` classification over a non-blank string may
    produce an identity. ``"No Adjudicado"`` is encoded absence and never a
    grouping key; grouping, dedupe and linkage over the raw value are
    forbidden before this boundary, and this module performs none of them.

    ``dataset_id`` closes the provenance chain. Every attached assessment must
    have classified THIS observation, of THIS field, in THIS dataset — for
    EVERY status, not only ``extracted``. A classification borrowed from
    another observation, another field or another dataset is not evidence of
    anything about this row, so it may not back any claim here.
    """

    dataset_id: str
    raw_observation: FieldObservation
    sentinel_assessment: SentinelAssessment | None
    status: AwardIdentityStatus
    award_id: AwardId | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.dataset_id, str) or not self.dataset_id:
            raise ValueError(
                "AwardIdentityExtraction.dataset_id must be a non-empty string"
            )
        if not isinstance(self.raw_observation, FieldObservation):
            raise TypeError("raw_observation must be a FieldObservation")
        if self.status not in _VALID_IDENTITY_STATUSES:
            raise ValueError(f"invalid AwardIdentityStatus: {self.status!r}")
        if list(self.reason_codes) != sorted(set(self.reason_codes)):
            raise ValueError("reason_codes must be ordered and duplicate-free")

        assessment = self.sentinel_assessment
        if assessment is not None:
            if not isinstance(assessment, SentinelAssessment):
                raise TypeError("sentinel_assessment must be a SentinelAssessment")
            if self.raw_observation.presence != "observed":
                raise ValueError(
                    "a sentinel classification requires an observed scalar; "
                    f"presence={self.raw_observation.presence!r} cannot back "
                    f"status={self.status!r}"
                )
            # Provenance, enforced for every status: no classification may be
            # imported from a different observation, field or dataset.
            if assessment.dataset_id != self.dataset_id:
                raise ValueError(
                    "the assessment belongs to dataset "
                    f"{assessment.dataset_id!r}, not to {self.dataset_id!r}; "
                    "a classification of another dataset proves nothing here"
                )
            if assessment.field_name != _FIELD_ADJUDICATION_ID:
                raise ValueError(
                    "an award identity is only classified from "
                    f"{_FIELD_ADJUDICATION_ID!r}, not from "
                    f"{assessment.field_name!r}"
                )
            if assessment.raw_observation is not self.raw_observation:
                raise ValueError(
                    "the assessment must have classified this very observation; "
                    "an equal copy is a different piece of provenance"
                )
            if assessment.status not in _SENTINEL_STATUS_FOR_IDENTITY[self.status]:
                raise ValueError(
                    f"status={self.status!r} contradicts sentinel status "
                    f"{assessment.status!r}"
                )
        elif self.status in _IDENTITY_STATUS_REQUIRES_ASSESSMENT:
            raise ValueError(
                f"status={self.status!r} is a sentinel verdict and requires the "
                "SentinelAssessment that produced it; identity may not skip the "
                "sentinel layer"
            )

        if assessment is not None and assessment.status == "meaningful":
            raw_value = self.raw_observation.raw_value
            if not isinstance(raw_value, str):
                raise ValueError(
                    "a meaningful award identity classification requires an "
                    f"observed string; {type(raw_value).__name__} is invalid"
                )
            expected_status = "extracted" if raw_value.strip() else "unknown"
            if self.status != expected_status:
                raise ValueError(
                    "a meaningful award identity classification derives "
                    f"status={expected_status!r} from its raw string; "
                    f"status={self.status!r} cannot hide or invent an AwardId"
                )

        if self.status != "extracted":
            if self.award_id is not None:
                raise ValueError(f"status={self.status!r} cannot carry an award_id")
            return

        # ``extracted`` is the ONLY status that yields an AwardId, so every
        # premise of the boundary is enforced here rather than trusted. A
        # hand-built extraction must not be able to bypass what the extractor
        # guarantees.
        if self.raw_observation.presence != "observed":
            raise ValueError(
                "an award identity requires an observed scalar; "
                f"presence={self.raw_observation.presence!r} cannot produce one"
            )
        raw_value = self.raw_observation.raw_value
        if not isinstance(raw_value, str):
            raise ValueError(
                "an award identity requires an observed string; "
                f"{type(raw_value).__name__} cannot become an AwardId"
            )
        if not raw_value.strip():
            raise ValueError(
                "a blank raw value is not an identity; whitespace is neither a "
                "key nor a value the sentinel layer cleared as meaningful"
            )
        if not isinstance(self.award_id, str) or not self.award_id:
            raise ValueError("status='extracted' requires a non-empty award_id")
        if self.award_id != raw_value:
            raise ValueError(
                "the identity IS the raw value; an AwardId is never "
                "transformed, trimmed or rebuilt"
            )


def extract_award_identity(
    observation: FieldObservation,
    *,
    dataset_id: str = PROCESS_DATASET_ID,
) -> AwardIdentityExtraction:
    """Extract an ``AwardId`` from ``id_adjudicacion``, or explain why not.

    An identity is produced ONLY from: an observed scalar, classified
    ``meaningful`` by the sentinel layer, whose raw value is a non-empty
    string. A blank string is explicitly rejected: the production catalog
    classifies ``""`` as ``meaningful`` (there is no sentinel rule for it),
    so emptiness must be excluded here rather than assumed upstream.
    """
    if not isinstance(observation, FieldObservation):
        raise TypeError("extract_award_identity requires a FieldObservation")

    if observation.presence != "observed":
        return AwardIdentityExtraction(
            dataset_id=dataset_id,
            raw_observation=observation,
            sentinel_assessment=None,
            status="unknown",
            award_id=None,
            reason_codes=("AWARD_ID_NOT_OBSERVED",),
        )

    policy = get_field_sentinel_policy(dataset_id, _FIELD_ADJUDICATION_ID)
    if policy is not None and not _comparable_by(
        policy.comparison_mode, observation.raw_value
    ):
        # The comparator cannot classify this type, so an encoded absence
        # cannot be ruled out and no identity may be claimed.
        return AwardIdentityExtraction(
            dataset_id=dataset_id,
            raw_observation=observation,
            sentinel_assessment=None,
            status="unknown",
            award_id=None,
            reason_codes=("AWARD_ID_NOT_A_STRING",),
        )
    if policy is None:
        # Without an authorized policy the sentinel layer cannot rule out an
        # encoded absence, so no identity may be claimed.
        return AwardIdentityExtraction(
            dataset_id=dataset_id,
            raw_observation=observation,
            sentinel_assessment=None,
            status="unknown",
            award_id=None,
            reason_codes=("AWARD_ID_SENTINEL_POLICY_UNAVAILABLE",),
        )

    assessment = assess_sentinel(observation, policy)
    status = assessment.status
    if status == "sentinel_absence":
        return AwardIdentityExtraction(
            dataset_id=dataset_id,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="absent",
            award_id=None,
            reason_codes=("AWARD_ID_SENTINEL_ABSENCE",),
        )
    if status == "sentinel_unknown":
        return AwardIdentityExtraction(
            dataset_id=dataset_id,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="unknown",
            award_id=None,
            reason_codes=("AWARD_ID_SENTINEL_UNKNOWN",),
        )
    if status == "unrecognized":
        return AwardIdentityExtraction(
            dataset_id=dataset_id,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="unrecognized",
            award_id=None,
            reason_codes=("AWARD_ID_UNRECOGNIZED",),
        )
    if status == "not_applicable":
        return AwardIdentityExtraction(
            dataset_id=dataset_id,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="unknown",
            award_id=None,
            reason_codes=("AWARD_ID_NOT_APPLICABLE",),
        )

    raw = observation.raw_value
    if not isinstance(raw, str):
        return AwardIdentityExtraction(
            dataset_id=dataset_id,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="unknown",
            award_id=None,
            reason_codes=("AWARD_ID_NOT_A_STRING",),
        )
    if not raw.strip():
        return AwardIdentityExtraction(
            dataset_id=dataset_id,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="unknown",
            award_id=None,
            reason_codes=("AWARD_ID_EMPTY_STRING",),
        )
    # The identity is the raw string, untouched: an ID is never transformed.
    return AwardIdentityExtraction(
        dataset_id=dataset_id,
        raw_observation=observation,
        sentinel_assessment=assessment,
        status="extracted",
        award_id=AwardId(raw),
        reason_codes=("AWARD_ID_EXTRACTED",),
    )


# ---------------------------------------------------------------------------
# Contract lookup input — a completed lookup's pure result, never a lookup.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ContractLookupResult:
    """The pure outcome of a contract lookup performed elsewhere.

    This module never executes the lookup, never joins datasets and never
    touches the network. ``absent_after_completed_lookup`` means one concrete
    query finished without a link — never universal non-existence. A timeout
    or transport failure is ``source_error``.

    The result carries its own provenance — which dataset, which field, and
    what was actually observed — so a conclusion that pits the award axis
    against the contract axis can present BOTH sides. An outcome asserted
    without the observation that supports it would be a claim, not evidence.

    Invariants:
      * ``observation`` is REQUIRED for ``observed``: claiming a link means
        showing what was seen;
      * ``observed`` requires ``presence="observed"``;
      * ``absent_after_completed_lookup`` requires ``presence="missing"`` —
        the query ran and returned no scalar;
      * ``not_queried`` and ``source_error`` require ``not_observed``;
      * ``source_error`` requires a ``SourceError`` belonging to THIS dataset;
      * every other outcome forbids a source error;
      * ``conflicting`` requires the ``AssessmentConflict`` the lookup itself
        found, built ONLY from evidence of THIS dataset; every other outcome
        forbids a conflict.

    The last invariant is what makes ``conflicting`` an honest outcome. A
    lookup that reports a contradiction must carry the contradictory contract
    evidence it saw. Otherwise the consumer would have to manufacture a
    conflict out of what it has at hand — pairing the process award id with a
    contract non-observation — which names two datasets while proving nothing
    about the contract side.
    """

    outcome: ContractLinkSignal
    dataset_id: str
    field_name: str
    observation: FieldObservation | None = None
    source_error: SourceError | None = None
    conflict: AssessmentConflict | None = None

    def __post_init__(self) -> None:
        if self.outcome not in _VALID_CONTRACT_LINK_SIGNALS:
            raise ValueError(f"invalid ContractLinkSignal: {self.outcome!r}")
        if not isinstance(self.dataset_id, str) or not self.dataset_id:
            raise ValueError("ContractLookupResult.dataset_id must be non-empty")
        if not isinstance(self.field_name, str) or not self.field_name:
            raise ValueError("ContractLookupResult.field_name must be non-empty")

        if self.observation is not None and not isinstance(
            self.observation, FieldObservation
        ):
            raise TypeError("ContractLookupResult.observation must be FieldObservation")
        if self.outcome == "observed" and self.observation is None:
            raise ValueError(
                "outcome='observed' requires the observation that was seen; a "
                "link cannot be asserted without evidence"
            )
        expected_presence = _LOOKUP_OUTCOME_PRESENCE.get(self.outcome)
        if (
            expected_presence is not None
            and self.observation is not None
            and self.observation.presence != expected_presence
        ):
            raise ValueError(
                f"outcome={self.outcome!r} requires "
                f"presence={expected_presence!r}, got "
                f"{self.observation.presence!r}"
            )

        if self.outcome == "source_error":
            if not isinstance(self.source_error, SourceError):
                raise ValueError(
                    "outcome='source_error' requires a SourceError; a failed "
                    "source must state how it failed"
                )
            if self.source_error.dataset != self.dataset_id:
                raise ValueError(
                    "the source error must belong to the dataset that was "
                    "queried; provenance cannot contradict itself"
                )
        elif self.source_error is not None:
            raise ValueError(
                f"outcome={self.outcome!r} cannot carry a source error; only a "
                "source failure may"
            )

        if self.outcome == "conflicting":
            if not isinstance(self.conflict, AssessmentConflict):
                raise ValueError(
                    "outcome='conflicting' requires the AssessmentConflict the "
                    "lookup found; a contradiction asserted without the "
                    "evidence that contradicts is a claim, not a finding"
                )
            foreign = sorted(
                {
                    evidence.dataset
                    for evidence in self.conflict.evidences
                    if evidence.dataset != self.dataset_id
                }
            )
            if foreign:
                raise ValueError(
                    "a contract conflict may only cite evidence of the dataset "
                    f"that was queried ({self.dataset_id!r}); foreign evidence "
                    f"from {foreign} cannot be used as filler"
                )
        elif self.conflict is not None:
            raise ValueError(
                f"outcome={self.outcome!r} cannot carry a conflict; only a "
                "lookup that found a contradiction may"
            )

    def as_observation(self) -> FieldObservation:
        """The observation to build evidence from, derived when not supplied.

        Deriving a non-scalar presence invents no data: it states exactly
        what the outcome already says happened.
        """
        if self.observation is not None:
            return self.observation
        presence = _LOOKUP_OUTCOME_PRESENCE.get(self.outcome, "not_observed")
        return FieldObservation(presence, None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# ProcessLifecycleAssessment — the textual stage axis.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _ResolvedLifecycleStage:
    raw_procedure_state: FieldObservation
    raw_phase: FieldObservation
    stage: ProcessLifecycleStage
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality


@dataclass(frozen=True, slots=True)
class ProcessLifecycleAssessment:
    """Stage read from the process snapshot's textual signals.

    Deliberately does NOT carry ``adjudicado``, the award identity or any
    contract link: those belong to the orthogonal resolution axis and must
    not be able to rewrite the stage.

    The stage is DERIVED, never asserted: ``__post_init__`` recomputes it from
    ``raw_procedure_state`` and ``raw_phase`` and rejects any instance whose
    stage disagrees. A public constructor — including ``dataclasses.replace``
    — therefore cannot mint an assessment whose conclusion its own raw signals
    do not support. The opening state, the award, the contract, the modality
    and every date stay out of that recomputation, exactly as they stay out of
    the resolver.
    """

    stage: ProcessLifecycleStage
    raw_procedure_state: FieldObservation
    raw_procedure_state_id: FieldObservation
    raw_opening_state: FieldObservation
    raw_phase: FieldObservation
    raw_summary_state: FieldObservation
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality
    _resolved_stage: InitVar[_ResolvedLifecycleStage | None] = field(
        default=None, kw_only=True
    )

    def __post_init__(self, _resolved_stage: _ResolvedLifecycleStage | None) -> None:
        if self.stage not in _VALID_STAGES:
            raise ValueError(f"invalid ProcessLifecycleStage: {self.stage!r}")
        for name in (
            "raw_procedure_state",
            "raw_procedure_state_id",
            "raw_opening_state",
            "raw_phase",
            "raw_summary_state",
        ):
            if not isinstance(getattr(self, name), FieldObservation):
                raise TypeError(f"{name} must be a FieldObservation")
        if not isinstance(self.quality, ResolutionQuality):
            raise TypeError("quality must be a ResolutionQuality")

        if _resolved_stage is None:
            validate_evidence_set(self.evidence)
            expected_stage, _ = _stage_from_signals(
                _observed_text(self.raw_procedure_state),
                _observed_text(self.raw_phase),
            )
        else:
            if type(_resolved_stage) is not _ResolvedLifecycleStage:
                raise TypeError("_resolved_stage must be an internal lifecycle result")
            if (
                self.raw_procedure_state is not _resolved_stage.raw_procedure_state
                or self.raw_phase is not _resolved_stage.raw_phase
            ):
                raise ValueError(
                    "resolved lifecycle stage belongs to different raw signals"
                )
            if (
                self.evidence is not _resolved_stage.evidence
                or self.quality is not _resolved_stage.quality
            ):
                raise ValueError(
                    "resolved lifecycle stage belongs to different evidence or quality"
                )
            expected_stage = _resolved_stage.stage
        if (
            type(self.stage) is not type(expected_stage)
            or self.stage != expected_stage
        ):
            raise ValueError(
                f"stage={self.stage!r} contradicts the raw textual signals, "
                f"which yield {expected_stage!r}; the stage is derived from "
                "them, never asserted alongside them"
            )


def _stage_from_signals(state: str, phase: str) -> tuple[ProcessLifecycleStage, str]:
    """Apply the binding precedence. Returns (stage, decisive reason code).

    Precedence (binding contract §4.1), highest first::

        Cancelado                         -> cancelled
        Suspendido                        -> suspended
        Seleccionado                      -> selected
        Evaluación                        -> evaluation
        Borrador                          -> draft
        En aprobación / Aprobado          -> approval
        Publicado/Abierto + offer phase   -> offer_phase_observed
        Publicado/Abierto                 -> published
        no state + offer phase            -> offer_phase_observed
        no decisive signal                -> unknown
    """
    offer_phase = phase in _PHASE_OFFER
    if state in _STATE_CANCELLED:
        return "cancelled", "PROCESS_STATE_CANCELLED"
    if state in _STATE_SUSPENDED:
        return "suspended", "PROCESS_STATE_SUSPENDED"
    if state in _STATE_SELECTED:
        return "selected", "PROCESS_STATE_SELECTED"
    if state in _STATE_EVALUATION:
        return "evaluation", "PROCESS_STATE_EVALUATION"
    if state in _STATE_DRAFT:
        return "draft", "PROCESS_STATE_DRAFT"
    if state in _STATE_APPROVAL:
        return "approval", "PROCESS_STATE_APPROVAL"
    if state in _STATE_PUBLISHED:
        if offer_phase:
            return "offer_phase_observed", "PROCESS_PUBLISHED_WITH_OFFER_PHASE"
        return "published", "PROCESS_STATE_PUBLISHED"
    if not state and offer_phase:
        return "offer_phase_observed", "PROCESS_OFFER_PHASE_WITHOUT_STATE"
    return "unknown", "PROCESS_NO_DECISIVE_SIGNAL"


def resolve_process_lifecycle(
    signals: ProcessLifecycleSignals,
    *,
    dataset_id: str = PROCESS_DATASET_ID,
) -> ProcessLifecycleAssessment:
    """Resolve the textual stage from T1A transport signals. Pure.

    Consumes the T1A ``ProcessLifecycleSignals`` directly, so the
    ``FieldObservation -> SentinelAssessment -> Evidence -> resolver`` chain
    stays closed and no third shape of the same data is invented.
    """
    if not isinstance(signals, ProcessLifecycleSignals):
        raise TypeError("resolve_process_lifecycle requires ProcessLifecycleSignals")

    state = _observed_text(signals.raw_procedure_state)
    phase = _observed_text(signals.raw_phase)
    opening = _observed_text(signals.raw_opening_state)

    stage, decisive_code = _stage_from_signals(state, phase)

    evidences = [
        _build_evidence(
            _FIELD_PROCEDURE_STATE,
            signals.raw_procedure_state,
            code="PROCESS_STATE_EVIDENCE",
            dataset_id=dataset_id,
        ),
        # Preserved as evidence only: no local ID->text mapping exists, so the
        # ID never arbitrates. No per-row reason code is emitted for it.
        _build_evidence(
            _FIELD_PROCEDURE_STATE_ID,
            signals.raw_procedure_state_id,
            code="PROCESS_STATE_ID_EVIDENCE",
            dataset_id=dataset_id,
        ),
        # Corroboration only: "Abierto" here never determines the stage and
        # never proves that the process is still live.
        _build_evidence(
            _FIELD_OPENING_STATE,
            signals.raw_opening_state,
            code="PROCESS_OPENING_STATE_EVIDENCE",
            dataset_id=dataset_id,
        ),
        _build_evidence(
            _FIELD_PHASE,
            signals.raw_phase,
            code="PROCESS_PHASE_EVIDENCE",
            dataset_id=dataset_id,
        ),
        # Cataloged field: the sentinel assessment is attached, so
        # "No Definido" stays sentinel_unknown instead of leaking as a value.
        _build_evidence(
            _FIELD_SUMMARY_STATE,
            signals.raw_summary_state,
            code="PROCESS_SUMMARY_STATE_EVIDENCE",
            dataset_id=dataset_id,
        ),
    ]

    reason_codes = [decisive_code]
    # A cataloged field that could not be classified says so out loud; the
    # raw value stays on its evidence either way.
    summary_gap = _summary_sentinel_gap(
        signals.raw_summary_state, dataset_id=dataset_id
    )
    if summary_gap is not None:
        reason_codes.append(summary_gap)
    missing_fields = [
        field
        for field, observation in (
            (_FIELD_PROCEDURE_STATE, signals.raw_procedure_state),
            (_FIELD_PHASE, signals.raw_phase),
            (_FIELD_SUMMARY_STATE, signals.raw_summary_state),
        )
        if observation.presence == "missing"
    ]

    conflicts: list[AssessmentConflict] = []
    by_field = {evidence.field: evidence for evidence in evidences}
    offer_phase = phase in _PHASE_OFFER
    if offer_phase and stage in _POST_OFFER_STAGES:
        conflicts.append(
            AssessmentConflict(
                code="PROCESS_OFFER_PHASE_VS_POST_OFFER_STATE",
                evidences=_canonical_evidence(
                    [by_field[_FIELD_PHASE], by_field[_FIELD_PROCEDURE_STATE]]
                ),
                consequence="PROCESS_STAGE_AND_PHASE_DISAGREE",
                severity="material",
            )
        )
    if offer_phase and opening == "cerrado":
        conflicts.append(
            AssessmentConflict(
                code="PROCESS_OFFER_PHASE_VS_CLOSED_OPENING",
                evidences=_canonical_evidence(
                    [by_field[_FIELD_PHASE], by_field[_FIELD_OPENING_STATE]]
                ),
                consequence="PROCESS_OFFER_WINDOW_UNCERTAIN",
                severity="informational",
            )
        )

    if stage == "unknown":
        completeness = "insufficient"
        confidence = "low"
    elif missing_fields:
        completeness = "partial"
        confidence = "medium"
    else:
        completeness = "complete"
        confidence = "high"
    if summary_gap is not None:
        # An unclassified productive field is a hole in the reading, so the
        # assessment may never present itself as complete or high-confidence.
        if completeness == "complete":
            completeness = "partial"
        if confidence == "high":
            confidence = "medium"
    consistency = "conflicting" if conflicts else "consistent"
    if conflicts:
        confidence = "low"

    quality = build_resolution_quality(
        completeness=completeness,
        consistency=consistency,
        confidence=confidence,
        reason_codes=tuple(reason_codes),
        missing_fields=tuple(missing_fields),
        conflicts=tuple(conflicts),
        capabilities=(("process_state_id_mapping", "unavailable"),),
    )

    canonical_evidence = tuple(
        by_field[field] for field in _LIFECYCLE_EVIDENCE_ORDER
    )
    return ProcessLifecycleAssessment(
        stage=stage,
        raw_procedure_state=signals.raw_procedure_state,
        raw_procedure_state_id=signals.raw_procedure_state_id,
        raw_opening_state=signals.raw_opening_state,
        raw_phase=signals.raw_phase,
        raw_summary_state=signals.raw_summary_state,
        evidence=canonical_evidence,
        quality=quality,
        _resolved_stage=_ResolvedLifecycleStage(
            raw_procedure_state=signals.raw_procedure_state,
            raw_phase=signals.raw_phase,
            stage=stage,
            evidence=canonical_evidence,
            quality=quality,
        ),
    )


# ---------------------------------------------------------------------------
# ProcessResolutionAssessment — award and contract link, orthogonal axes.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _ResolvedProcessResolution:
    raw_adjudicated: FieldObservation
    award_identity: AwardIdentityExtraction
    contract_lookup: ContractLookupResult | None
    award_signal: AwardResolutionSignal
    contract_link_signal: ContractLinkSignal
    resolution_status: ProcessResolutionStatus
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality


@dataclass(frozen=True, slots=True)
class ProcessResolutionAssessment:
    """Award signal, contract-link signal and the status derived from both.

    The three pieces are independent. ``resolution_status`` is derived and
    never substitutes the two signals: a contract link does not erase an
    award, and an award does not prove a contract.

    All three are RECOMPUTED in ``__post_init__`` from the inputs that
    produced them — the award flag plus the identity extraction, and the
    lookup's own outcome — so no public constructor (``dataclasses.replace``
    included) can present a conclusion its own inputs contradict. Quality is
    checked against the same inputs: a conflicting signal forbids a
    ``consistent`` reading, and the lookup's source error and conflict must
    both survive into ``quality``.
    """

    award_signal: AwardResolutionSignal
    contract_link_signal: ContractLinkSignal
    resolution_status: ProcessResolutionStatus
    award_id: AwardId | None
    raw_adjudicated: FieldObservation
    raw_adjudication_date: FieldObservation
    award_identity: AwardIdentityExtraction
    contract_lookup: ContractLookupResult | None
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality
    _resolved_resolution: InitVar[_ResolvedProcessResolution | None] = field(
        default=None, kw_only=True
    )

    def __post_init__(
        self, _resolved_resolution: _ResolvedProcessResolution | None
    ) -> None:
        if self.contract_lookup is not None:
            if not isinstance(self.contract_lookup, ContractLookupResult):
                raise TypeError("contract_lookup must be a ContractLookupResult")
            if self.contract_link_signal != self.contract_lookup.outcome:
                raise ValueError(
                    "contract_link_signal must be the lookup's own outcome"
                )
        elif self.contract_link_signal != "not_queried":
            raise ValueError(
                "without a lookup the only honest signal is 'not_queried'"
            )
        if self.award_signal not in _VALID_AWARD_SIGNALS:
            raise ValueError(f"invalid AwardResolutionSignal: {self.award_signal!r}")
        if self.contract_link_signal not in _VALID_CONTRACT_LINK_SIGNALS:
            raise ValueError(
                f"invalid ContractLinkSignal: {self.contract_link_signal!r}"
            )
        if self.resolution_status not in _VALID_RESOLUTION_STATUSES:
            raise ValueError(
                f"invalid ProcessResolutionStatus: {self.resolution_status!r}"
            )
        if not isinstance(self.award_identity, AwardIdentityExtraction):
            raise TypeError("award_identity must be an AwardIdentityExtraction")
        if self.award_id != self.award_identity.award_id:
            raise ValueError(
                "award_id must come from award_identity; it is never derived "
                "independently"
            )
        for name in ("raw_adjudicated", "raw_adjudication_date"):
            if not isinstance(getattr(self, name), FieldObservation):
                raise TypeError(f"{name} must be a FieldObservation")
        if not isinstance(self.quality, ResolutionQuality):
            raise TypeError("quality must be a ResolutionQuality")

        if _resolved_resolution is None:
            validate_evidence_set(self.evidence)
            expected_award, _ = _award_signal_from(
                _observed_text(self.raw_adjudicated), self.award_identity
            )
            expected_status, _ = _resolution_status_from(
                self.award_signal, self.contract_link_signal
            )
        else:
            if type(_resolved_resolution) is not _ResolvedProcessResolution:
                raise TypeError(
                    "_resolved_resolution must be an internal process result"
                )
            for label, mine, theirs in (
                (
                    "raw_adjudicated",
                    self.raw_adjudicated,
                    _resolved_resolution.raw_adjudicated,
                ),
                (
                    "award_identity",
                    self.award_identity,
                    _resolved_resolution.award_identity,
                ),
                (
                    "contract_lookup",
                    self.contract_lookup,
                    _resolved_resolution.contract_lookup,
                ),
            ):
                if mine is not theirs:
                    raise ValueError(
                        f"resolved process result belongs to different {label}"
                    )
            if (
                self.evidence is not _resolved_resolution.evidence
                or self.quality is not _resolved_resolution.quality
            ):
                raise ValueError(
                    "resolved process result belongs to different evidence or quality"
                )
            expected_award = _resolved_resolution.award_signal
            if (
                type(self.contract_link_signal)
                is not type(_resolved_resolution.contract_link_signal)
                or self.contract_link_signal
                != _resolved_resolution.contract_link_signal
            ):
                raise ValueError(
                    "resolved process result has different contract_link_signal"
                )
            expected_status = _resolved_resolution.resolution_status
        if (
            type(self.award_signal) is not type(expected_award)
            or self.award_signal != expected_award
        ):
            raise ValueError(
                f"award_signal={self.award_signal!r} contradicts the award flag "
                f"and the identity extraction, which yield {expected_award!r}"
            )
        if (
            type(self.resolution_status) is not type(expected_status)
            or self.resolution_status != expected_status
        ):
            raise ValueError(
                f"resolution_status={self.resolution_status!r} contradicts its "
                f"own signals, which yield {expected_status!r}; the status is "
                "derived from them and never replaces them"
            )

        if (
            "conflicting"
            in {self.award_signal, self.contract_link_signal, self.resolution_status}
            and self.quality.consistency == "consistent"
        ):
            raise ValueError(
                "a conflicting signal can never be reported as a consistent "
                "resolution; conflicts are preserved, not resolved away"
            )
        if self.contract_lookup is not None:
            error = self.contract_lookup.source_error
            if error is not None and error not in self.quality.source_errors:
                raise ValueError(
                    "the lookup's source error must survive into quality; a "
                    "failed source is never an absence and never disappears"
                )
            conflict = self.contract_lookup.conflict
            if conflict is not None and conflict not in self.quality.conflicts:
                raise ValueError(
                    "the lookup's own conflict must survive into quality; a "
                    "contradiction found by the source is never dropped"
                )


def _award_signal_from(
    adjudicated: str, identity: AwardIdentityExtraction
) -> tuple[AwardResolutionSignal, str]:
    """Derive the award signal from the flag and the extracted identity."""
    flag_yes = adjudicated in _AFFIRMATIVE
    flag_no = adjudicated in _NEGATIVE
    extracted = identity.status == "extracted"
    absent = identity.status == "absent"

    if flag_no and extracted:
        return "conflicting", "AWARD_FLAG_NEGATIVE_WITH_AWARD_IDENTITY"
    if flag_yes and absent:
        return "conflicting", "AWARD_FLAG_POSITIVE_WITH_SENTINEL_ABSENCE"
    if extracted:
        return "observed", "AWARD_IDENTITY_EXTRACTED"
    if flag_yes:
        return "observed", "AWARD_FLAG_POSITIVE"
    if flag_no:
        return "not_indicated", "AWARD_FLAG_NEGATIVE"
    if absent:
        return "not_indicated", "AWARD_SENTINEL_ABSENCE"
    return "unknown", "AWARD_SIGNAL_INDETERMINATE"


def _resolution_status_from(
    award: AwardResolutionSignal, link: ContractLinkSignal
) -> tuple[ProcessResolutionStatus, str]:
    """Derive the status. The two signals are preserved regardless."""
    if award == "conflicting" or link == "conflicting":
        return "conflicting", "RESOLUTION_SIGNALS_CONFLICTING"
    if award == "not_indicated" and link == "observed":
        # The contract link contradicts the absence of an award; the status
        # keeps the contradiction instead of choosing a side.
        return "conflicting", "RESOLUTION_AWARD_ABSENT_WITH_CONTRACT_LINK"
    if award == "observed":
        # An award resolves the process even when the contract lookup timed
        # out or was never run: award does not prove a contract, and a failed
        # lookup proves nothing at all.
        return "resolved", "RESOLUTION_BY_AWARD"
    if link == "observed":
        return "resolved", "RESOLUTION_BY_CONTRACT_LINK"
    if award == "not_indicated" and link == "absent_after_completed_lookup":
        return "unresolved", "RESOLUTION_ABSENT_AFTER_COMPLETED_LOOKUP"
    if link == "source_error":
        return "unknown", "RESOLUTION_BLOCKED_BY_SOURCE_ERROR"
    return "unknown", "RESOLUTION_INDETERMINATE"


def resolve_process_resolution(
    signals: ProcessLifecycleSignals,
    *,
    contract_lookup: ContractLookupResult | None = None,
    dataset_id: str = PROCESS_DATASET_ID,
) -> ProcessResolutionAssessment:
    """Resolve the award and contract-link axes. Pure; performs no lookup.

    ``contract_lookup=None`` means the lookup was never attempted, which is
    ``not_queried`` — explicitly different from a completed lookup that found
    nothing, and from a source failure.
    """
    if not isinstance(signals, ProcessLifecycleSignals):
        raise TypeError("resolve_process_resolution requires ProcessLifecycleSignals")
    if contract_lookup is not None and not isinstance(
        contract_lookup, ContractLookupResult
    ):
        raise TypeError("contract_lookup must be a ContractLookupResult or None")

    identity = extract_award_identity(
        signals.raw_adjudication_id, dataset_id=dataset_id
    )
    adjudicated = _observed_text(signals.raw_adjudicated)
    award_signal, award_reason = _award_signal_from(adjudicated, identity)

    link_signal: ContractLinkSignal = (
        "not_queried" if contract_lookup is None else contract_lookup.outcome
    )
    source_errors = (
        (contract_lookup.source_error,)
        if contract_lookup is not None and contract_lookup.source_error is not None
        else ()
    )

    status, status_reason = _resolution_status_from(award_signal, link_signal)

    adjudicated_role: EvidenceRole = (
        "contradicts"
        if award_signal == "conflicting"
        and signals.raw_adjudicated.presence == "observed"
        else _role_for(signals.raw_adjudicated)
    )
    evidences = [
        _build_evidence(
            _FIELD_ADJUDICATED,
            signals.raw_adjudicated,
            code="AWARD_FLAG_EVIDENCE",
            dataset_id=dataset_id,
            role=adjudicated_role,
        ),
        _build_evidence(
            _FIELD_ADJUDICATION_ID,
            signals.raw_adjudication_id,
            code="AWARD_ID_EVIDENCE",
            dataset_id=dataset_id,
        ),
        _build_evidence(
            _FIELD_ADJUDICATION_DATE,
            signals.raw_adjudication_date,
            code="AWARD_DATE_EVIDENCE",
            dataset_id=dataset_id,
        ),
    ]
    by_field = {evidence.field: evidence for evidence in evidences}

    # The contract side must be able to speak for itself. Without its own
    # evidence, a conflict "award versus contract" would preserve only the
    # award side and the contract dataset would vanish from the assessment.
    contract_evidence: Evidence | None = None
    if contract_lookup is not None:
        link_observation = contract_lookup.as_observation()
        # "contradicts" is only available to an observed scalar: a lookup that
        # observed nothing keeps its T1A presence role, so a non-observation
        # can never pose as a counter-argument.
        contract_role: EvidenceRole | None = (
            "contradicts"
            if award_signal == "not_indicated"
            and link_observation.presence == "observed"
            else None
        )
        contract_evidence = _build_evidence(
            contract_lookup.field_name,
            link_observation,
            code="CONTRACT_LINK_EVIDENCE",
            dataset_id=contract_lookup.dataset_id,
            role=contract_role,
        )
        if contract_lookup.source_error is not None:
            contract_evidence = Evidence(
                code=contract_evidence.code,
                dataset=contract_evidence.dataset,
                field=contract_evidence.field,
                raw_observation=contract_evidence.raw_observation,
                sentinel_assessment=contract_evidence.sentinel_assessment,
                normalized_value=contract_evidence.normalized_value,
                role=contract_evidence.role,
                source_error=contract_lookup.source_error,
            )
        evidences.append(contract_evidence)
        if contract_lookup.conflict is not None:
            # The contradictory contract evidence enters the general evidence
            # set: a conflict whose evidence exists nowhere else in the
            # assessment could not be audited from the assessment alone.
            # Exact repeats are skipped, never merged or rewritten.
            seen = set(evidences)
            for found in contract_lookup.conflict.evidences:
                if found not in seen:
                    evidences.append(found)
                    seen.add(found)

    conflicts: list[AssessmentConflict] = []
    if award_signal == "conflicting":
        conflicts.append(
            AssessmentConflict(
                code="PROCESS_AWARD_FLAG_VS_AWARD_IDENTITY",
                evidences=_canonical_evidence(
                    [by_field[_FIELD_ADJUDICATED], by_field[_FIELD_ADJUDICATION_ID]]
                ),
                consequence="AWARD_SIGNAL_UNDECIDED",
                severity="critical",
            )
        )
    if (
        award_signal == "not_indicated"
        and link_signal == "observed"
        and contract_evidence is not None
    ):
        conflicts.append(
            AssessmentConflict(
                code="PROCESS_AWARD_ABSENT_VS_CONTRACT_LINK",
                evidences=_canonical_evidence(
                    [by_field[_FIELD_ADJUDICATION_ID], contract_evidence]
                ),
                consequence="RESOLUTION_SIGNALS_DISAGREE",
                severity="material",
            )
        )
    if contract_lookup is not None and contract_lookup.conflict is not None:
        # The lookup itself found contradictory contract evidence. It is
        # preserved VERBATIM: this block does not re-describe it and, above
        # all, does not manufacture a substitute by pairing the process award
        # id with a contract non-observation — that would name two datasets
        # while proving nothing about the contract side.
        conflicts.append(contract_lookup.conflict)

    reason_codes = [award_reason, status_reason, *identity.reason_codes]
    missing_fields = [
        field
        for field, observation in (
            (_FIELD_ADJUDICATED, signals.raw_adjudicated),
            (_FIELD_ADJUDICATION_ID, signals.raw_adjudication_id),
            (_FIELD_ADJUDICATION_DATE, signals.raw_adjudication_date),
        )
        if observation.presence == "missing"
    ]

    if status == "unknown":
        completeness = "insufficient"
        confidence = "low"
    elif missing_fields or link_signal in {"not_queried", "source_error"}:
        completeness = "partial"
        confidence = "medium"
    else:
        completeness = "complete"
        confidence = "high"
    # Consistency is derived from the signals too, not only from the modelled
    # conflicts: a contradiction this block cannot itemise must still never
    # be reported as a consistent resolution.
    contradicted = (
        bool(conflicts)
        or status == "conflicting"
        or award_signal == "conflicting"
        or link_signal == "conflicting"
    )
    consistency = "conflicting" if contradicted else "consistent"
    if contradicted:
        confidence = "low"

    quality = build_resolution_quality(
        completeness=completeness,
        consistency=consistency,
        confidence=confidence,
        reason_codes=tuple(reason_codes),
        missing_fields=tuple(missing_fields),
        source_errors=source_errors,
        conflicts=tuple(conflicts),
    )

    canonical_evidence = (
        tuple(by_field[field] for field in _RESOLUTION_EVIDENCE_ORDER)
        if contract_lookup is None
        else _canonical_evidence(evidences)
    )
    return ProcessResolutionAssessment(
        award_signal=award_signal,
        contract_link_signal=link_signal,
        resolution_status=status,
        award_id=identity.award_id,
        raw_adjudicated=signals.raw_adjudicated,
        raw_adjudication_date=signals.raw_adjudication_date,
        award_identity=identity,
        contract_lookup=contract_lookup,
        evidence=canonical_evidence,
        quality=quality,
        _resolved_resolution=_ResolvedProcessResolution(
            raw_adjudicated=signals.raw_adjudicated,
            award_identity=identity,
            contract_lookup=contract_lookup,
            award_signal=award_signal,
            contract_link_signal=link_signal,
            resolution_status=status,
            evidence=canonical_evidence,
            quality=quality,
        ),
    )
