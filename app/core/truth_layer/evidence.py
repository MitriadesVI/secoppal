"""SECOP Truth Layer T1B-1 — Evidence, conflicts and resolution quality.

Pure, deterministic contracts for *traceable evidence*, *preserved
conflicts* and *structured quality*. No I/O, no clock, no environment, no
filesystem, no network, no parser, no formatter, no Radar, no SECOP client.

Pipeline position (per
``docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md``, section 2)::

    FieldObservation -> SentinelAssessment -> Evidence -> resolver (future)

This module implements ONLY the shared Evidence/conflict/quality layer. It
defines no lifecycle stage, no process resolution, no contract axis, no
temporal assessment, no visibility assessment and no opportunity role. It
carries no domain rule and no domain vocabulary: an ``Evidence`` states
*what was observed and how it was classified*, never *what it means*.

Hard boundaries enforced here:
  * Evidence never classifies sentinels on its own — when a field carries a
    sentinel classification it MUST arrive as the ``SentinelAssessment``
    produced by T1B-0A, and Evidence never builds or looks one up;
  * ``normalized_value`` never substitutes ``raw_value`` and may not
    diverge from the sentinel layer's comparison form;
  * T1A ``missing`` and ``not_observed`` keep their T1A meaning and are
    never renamed, merged, or reused as domain signals;
  * a source error (timeout / transport / invalid payload) is a distinct
    third thing: it is neither ``missing`` nor a negative conclusion;
  * conflicts are preserved, never silently resolved and never scored.

Binding documents, read in full before edit:
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_SEMANTIC_CONTRACT.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_EXECUTION_ROADMAP.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R1.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R2.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R3.md
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.core.truth_layer.models import FieldObservation, RawScalar
from app.core.truth_layer.sentinels import SentinelAssessment

__all__ = [
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
]


# ---------------------------------------------------------------------------
# Literals (public contract, per SEMANTIC_CONTRACT section 2).
# ---------------------------------------------------------------------------

EvidenceRole = Literal[
    "supports",
    "contradicts",
    "missing",
    "not_observed",
]

# A source failure is NOT an absence. ``not_queried`` is deliberately absent
# from this union: the binding contract already models "field not projected
# or source not queried" as the T1A presence ``not_observed`` (see
# SEMANTIC_CONTRACT section 2), and models ``not_queried`` separately from
# ``source_error`` in ``ContractLinkSignal`` (section 4.2). Representing it
# again here would collapse a distinction the contract keeps open.
SourceErrorKind = Literal[
    "timeout",
    "transport_error",
    "invalid_payload",
]

ConflictSeverity = Literal["critical", "material", "informational"]

ResolutionCompleteness = Literal["complete", "partial", "insufficient"]
ResolutionConsistency = Literal["consistent", "conflicting", "unknown"]
ResolutionConfidence = Literal["high", "medium", "low"]

_VALID_EVIDENCE_ROLES: frozenset[str] = frozenset(
    {"supports", "contradicts", "missing", "not_observed"}
)
_OBSERVED_ROLES: frozenset[str] = frozenset({"supports", "contradicts"})
_VALID_SOURCE_ERROR_KINDS: frozenset[str] = frozenset(
    {"timeout", "transport_error", "invalid_payload"}
)
_VALID_SEVERITIES: frozenset[str] = frozenset(
    {"critical", "material", "informational"}
)
_VALID_COMPLETENESS: frozenset[str] = frozenset(
    {"complete", "partial", "insufficient"}
)
_VALID_CONSISTENCY: frozenset[str] = frozenset(
    {"consistent", "conflicting", "unknown"}
)
_VALID_CONFIDENCE: frozenset[str] = frozenset({"high", "medium", "low"})

# Roles map 1:1 onto T1A presences, so T1A structure is never re-encoded.
_ROLE_TO_PRESENCE: dict[str, str] = {
    "missing": "missing",
    "not_observed": "not_observed",
}

_STABLE_CODE_HEAD: frozenset[str] = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
_STABLE_CODE_CHARS: frozenset[str] = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
)


# ---------------------------------------------------------------------------
# Private validation helpers (pure).
# ---------------------------------------------------------------------------


def _require_non_empty_str(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _require_stable_code(value: object, label: str) -> str:
    """Reject anything that is not a stable UPPER_SNAKE_CASE code.

    Stable codes keep the layer machine-readable and structurally forbid
    product-visible prose from leaking into evidence, conflicts or quality.
    """
    code = _require_non_empty_str(value, label)
    if code[0] not in _STABLE_CODE_HEAD or not set(code) <= _STABLE_CODE_CHARS:
        raise ValueError(
            f"{label} must be a stable UPPER_SNAKE_CASE code, not free text: "
            f"{code!r}"
        )
    return code


def _raw_marker(value: RawScalar) -> tuple[str, str]:
    """Total, type-distinguishing ordering marker for a raw scalar.

    ``True``/``1`` and ``0``/``0.0`` must not collapse, exactly as in the
    sentinel comparator, so the type name participates in the key.
    """
    return (type(value).__name__, repr(value))


def _scalar_equality_sort_key(value: RawScalar) -> tuple[object, ...]:
    """Order scalars without separating values that compare equal.

    Dataclass equality follows Python scalar equality, where ``True == 1``
    and ``0 == 0.0``. A key used to make equal Evidence adjacent must
    therefore put bool/int/float in one equality domain even though
    ``_raw_marker`` remains type-distinguishing for sentinel comparisons.
    """
    if value is None:
        return ("2-none",)
    if isinstance(value, str):
        return ("0-str", value)
    return ("1-number", value)


def _same_scalar(left: RawScalar, right: RawScalar) -> bool:
    """Type-strict scalar equality, matching the sentinel ``exact`` comparator.

    Plain ``==`` is not enough: ``True == 1`` and ``0 == 0.0`` are both true
    in Python, which would let a normalized value of the wrong type pass as
    the sentinel comparison form.
    """
    return type(left) is type(right) and left == right


def _observation_sort_key(observation: FieldObservation) -> tuple[object, ...]:
    return (observation.presence, _scalar_equality_sort_key(observation.raw_value))


def _sentinel_sort_key(
    assessment: SentinelAssessment | None,
) -> tuple[object, ...]:
    if assessment is None:
        return ("None",)
    return (
        "SentinelAssessment",
        assessment.dataset_id,
        assessment.field_name,
        _observation_sort_key(assessment.raw_observation),
        assessment.status,
        ("none",)
        if assessment.sentinel_code is None
        else ("str", assessment.sentinel_code),
        _scalar_equality_sort_key(assessment.normalized_comparison_value),
    )


def _optional_source_error_sort_key(
    error: "SourceError | None",
) -> tuple[object, ...]:
    if error is None:
        return ("None",)
    return (
        "SourceError",
        error.code,
        error.dataset,
        error.kind,
    )


def _evidence_sort_key(evidence: "Evidence") -> tuple[object, ...]:
    # The explicit tail is total over every Evidence field while avoiding the
    # recursive dataclass repr formerly built for every comparison.
    return (
        evidence.dataset,
        evidence.field,
        evidence.code,
        evidence.role,
        _observation_sort_key(evidence.raw_observation),
        _sentinel_sort_key(evidence.sentinel_assessment),
        _scalar_equality_sort_key(evidence.normalized_value),
        _optional_source_error_sort_key(evidence.source_error),
    )


def _source_error_sort_key(error: "SourceError") -> tuple[str, ...]:
    return (error.dataset, error.kind, error.code)


def _conflict_sort_key(conflict: "AssessmentConflict") -> tuple[object, ...]:
    return (
        conflict.code,
        conflict.severity,
        conflict.consequence,
        *conflict.fields,
        tuple(_evidence_sort_key(evidence) for evidence in conflict.evidences),
    )


def _require_canonically_ordered(
    items: tuple[object, ...], key, label: str
) -> None:
    """Reject an out-of-order tuple instead of silently re-sorting it.

    Determinism is a contract, not a convenience: callers must present a
    canonical order so that equality and hashing are stable, and so that no
    caller-visible reordering happens behind their back.
    """
    previous = None
    for item in items:
        current = key(item)
        if previous is not None and current < previous:
            raise ValueError(f"{label} must be in canonical order")
        previous = current


# ---------------------------------------------------------------------------
# SourceError.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SourceError:
    """A failure of a source, which is never an absence of the fact.

    A timeout, a transport failure or an unusable payload prove that the
    source did not answer — they never prove that the process, contract or
    field does not exist (binding invariant 8).
    """

    code: str
    dataset: str
    kind: SourceErrorKind

    def __post_init__(self) -> None:
        _require_stable_code(self.code, "SourceError.code")
        _require_non_empty_str(self.dataset, "SourceError.dataset")
        if self.kind not in _VALID_SOURCE_ERROR_KINDS:
            raise ValueError(f"invalid SourceError.kind: {self.kind!r}")


# ---------------------------------------------------------------------------
# Evidence.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Evidence:
    """One traceable piece of evidence — never a conclusion.

    Evidence carries the T1A ``FieldObservation`` verbatim plus, when a
    scalar was actually observed, the T1B-0A ``SentinelAssessment`` that
    classified it. It asserts nothing about lifecycle, resolution or role.

    Invariants:
      * ``code`` is a stable, non-empty UPPER_SNAKE_CASE code;
      * ``dataset``/``field`` are non-empty and agree with the sentinel
        assessment and with the source error when either is present;
      * ``role`` and the observation's ``presence`` agree exactly:
        ``supports``/``contradicts`` require ``observed``; ``missing``
        requires ``missing``; ``not_observed`` requires ``not_observed``;
      * ``sentinel_assessment`` is OPTIONAL for an observed scalar and
        FORBIDDEN otherwise. Most fields have no authorized sentinel policy
        (``get_field_sentinel_policy`` returns ``None``), and such a field
        still produces perfectly valid evidence. A missing assessment means
        exactly one thing — "no sentinel classification is attached" — and
        never "meaningful". Evidence neither consults the catalog nor
        invents a policy;
      * when present, ``sentinel_assessment.raw_observation`` must be the
        very same object (``is``), not an equal copy: provenance is identity;
      * ``normalized_value`` requires an attached assessment, and must then
        equal that assessment's comparison form under type-strict equality
        (``True`` is not ``1``, ``0`` is not ``0.0``, ``0`` is not ``"0"``) —
        Evidence may not invent a normalization of its own;
      * ``source_error`` is only allowed on ``not_observed`` evidence, so a
        source failure can never masquerade as ``missing``.

    A ``sentinel_absence`` assessment stays semantic absence *inside* the
    assessment; it never becomes the structural T1A ``missing``.
    """

    code: str
    dataset: str
    field: str
    raw_observation: FieldObservation
    sentinel_assessment: SentinelAssessment | None
    normalized_value: RawScalar | None
    role: EvidenceRole
    source_error: SourceError | None = None

    def __post_init__(self) -> None:
        _require_stable_code(self.code, "Evidence.code")
        _require_non_empty_str(self.dataset, "Evidence.dataset")
        _require_non_empty_str(self.field, "Evidence.field")
        if not isinstance(self.raw_observation, FieldObservation):
            raise TypeError("Evidence.raw_observation must be a FieldObservation")
        if self.role not in _VALID_EVIDENCE_ROLES:
            raise ValueError(f"invalid EvidenceRole: {self.role!r}")

        presence = self.raw_observation.presence
        expected = _ROLE_TO_PRESENCE.get(self.role, "observed")
        if presence != expected:
            raise ValueError(
                f"role={self.role!r} requires presence={expected!r}; "
                f"observation has presence={presence!r}"
            )

        observed = self.role in _OBSERVED_ROLES
        if observed:
            if self.sentinel_assessment is None:
                # A field with no authorized sentinel policy still produces
                # evidence. The absence of an assessment means only "no
                # sentinel classification attached" — never "meaningful" —
                # so no normalized form may be claimed either.
                if self.normalized_value is not None:
                    raise ValueError(
                        "Evidence without a SentinelAssessment cannot carry a "
                        "normalized value; normalization belongs to the "
                        "sentinel layer"
                    )
            else:
                if not isinstance(self.sentinel_assessment, SentinelAssessment):
                    raise TypeError(
                        "Evidence.sentinel_assessment must be a SentinelAssessment"
                    )
                if self.sentinel_assessment.dataset_id != self.dataset:
                    raise ValueError(
                        "Evidence.dataset disagrees with the sentinel assessment"
                    )
                if self.sentinel_assessment.field_name != self.field:
                    raise ValueError(
                        "Evidence.field disagrees with the sentinel assessment"
                    )
                if self.sentinel_assessment.raw_observation is not self.raw_observation:
                    raise ValueError(
                        "Evidence.raw_observation must be the very observation "
                        "the sentinel assessed; an equal copy is a different "
                        "piece of provenance"
                    )
                if self.normalized_value is not None and not _same_scalar(
                    self.normalized_value,
                    self.sentinel_assessment.normalized_comparison_value,
                ):
                    raise ValueError(
                        "Evidence.normalized_value must equal the sentinel "
                        "comparison form, type included; Evidence may not "
                        "normalize on its own"
                    )
        else:
            if self.sentinel_assessment is not None:
                raise ValueError(
                    f"role={self.role!r} has no observed scalar and therefore "
                    "cannot carry a SentinelAssessment"
                )
            if self.normalized_value is not None:
                raise ValueError(
                    f"role={self.role!r} has no observed scalar and therefore "
                    "cannot carry a normalized value"
                )

        if self.source_error is not None:
            if not isinstance(self.source_error, SourceError):
                raise TypeError("Evidence.source_error must be a SourceError")
            if self.role != "not_observed":
                raise ValueError(
                    "a source error may only qualify not_observed evidence; it "
                    "is neither a missing field nor an observed value"
                )
            if self.source_error.dataset != self.dataset:
                raise ValueError(
                    "Evidence.dataset disagrees with the source error dataset"
                )


def validate_evidence_set(evidences: tuple[Evidence, ...]) -> tuple[Evidence, ...]:
    """Return ``evidences`` unchanged after checking set-level invariants.

    Rejects non-``Evidence`` entries, exact duplicates and non-canonical
    ordering. The tuple is returned as-is: this validates, it never rewrites.
    """
    if not isinstance(evidences, tuple):
        raise TypeError("evidence set must be a tuple")
    previous_item: Evidence | None = None
    previous_key: tuple[object, ...] | None = None
    for item in evidences:
        if not isinstance(item, Evidence):
            raise TypeError("evidence set entries must be Evidence")
        current_key = _evidence_sort_key(item)
        if previous_key is not None and current_key < previous_key:
            raise ValueError("evidence set must be in canonical order")
        if previous_item is not None and item == previous_item:
            raise ValueError("evidence set contains a duplicated Evidence")
        previous_item = item
        previous_key = current_key
    return evidences


# ---------------------------------------------------------------------------
# AssessmentConflict.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AssessmentConflict:
    """Two or more incompatible pieces of evidence, preserved as such.

    The conflict names no winner and drops no evidence. ``consequence`` is a
    stable code, never product prose, and no confidence percentage is
    attached: a conflict is a fact, not a score.

    ``fields`` and ``raw_values`` (binding contract section 2) are derived
    positionally from ``evidences`` so that they can never drift from the
    evidence they describe.
    """

    code: str
    evidences: tuple[Evidence, ...]
    consequence: str
    severity: ConflictSeverity

    def __post_init__(self) -> None:
        _require_stable_code(self.code, "AssessmentConflict.code")
        _require_stable_code(self.consequence, "AssessmentConflict.consequence")
        if self.severity not in _VALID_SEVERITIES:
            raise ValueError(f"invalid ConflictSeverity: {self.severity!r}")
        validate_evidence_set(self.evidences)
        if len(self.evidences) < 2:
            raise ValueError(
                "AssessmentConflict requires at least two pieces of evidence"
            )

    @property
    def fields(self) -> tuple[str, ...]:
        """Field names, positionally aligned with ``evidences``."""
        return tuple(evidence.field for evidence in self.evidences)

    @property
    def raw_values(self) -> tuple[RawScalar, ...]:
        """Raw scalars, positionally aligned with ``evidences``."""
        return tuple(
            evidence.raw_observation.raw_value for evidence in self.evidences
        )


# ---------------------------------------------------------------------------
# ResolutionQuality.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ResolutionQuality:
    """Structured, categorical quality of a resolution — never a percentage.

    Every axis is a closed vocabulary. There is no 0–1 score, because the
    binding contract forbids invented percentages: quality is derived from
    decisive fields declared by each resolver, not estimated.

    Invariants:
      * the three axes are valid literals;
      * codes are stable and both ordered and duplicate-free;
      * ``missing_fields`` are ordered and duplicate-free — a missing field
        is structural absence, distinct from ``not_observed`` and distinct
        from a source error;
      * ``source_errors`` are preserved with their kind, so a timeout never
        degrades into an absence;
      * ``conflicts`` are preserved, and a non-empty conflict tuple forbids
        ``consistency="consistent"`` — conflicts are never resolved away;
      * ``capabilities`` are ordered ``(key, value)`` pairs with unique keys;
        a global capability limit lives here precisely so it does not have
        to be emitted as a per-row reason code.

    Quality states nothing about lifecycle: a critical conflict may coexist
    with a partial, still-useful conclusion drawn by a future resolver.
    """

    completeness: ResolutionCompleteness
    consistency: ResolutionConsistency
    confidence: ResolutionConfidence
    reason_codes: tuple[str, ...] = ()
    missing_fields: tuple[str, ...] = ()
    source_errors: tuple[SourceError, ...] = ()
    conflicts: tuple[AssessmentConflict, ...] = ()
    capabilities: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.completeness not in _VALID_COMPLETENESS:
            raise ValueError(f"invalid completeness: {self.completeness!r}")
        if self.consistency not in _VALID_CONSISTENCY:
            raise ValueError(f"invalid consistency: {self.consistency!r}")
        if self.confidence not in _VALID_CONFIDENCE:
            raise ValueError(f"invalid confidence: {self.confidence!r}")

        if not isinstance(self.reason_codes, tuple):
            raise TypeError("reason_codes must be a tuple")
        for code in self.reason_codes:
            _require_stable_code(code, "reason code")
        previous_code = None
        for code in self.reason_codes:
            if previous_code is not None:
                if code == previous_code:
                    raise ValueError("reason_codes must not contain duplicates")
                if code < previous_code:
                    raise ValueError("reason_codes must be in canonical order")
            previous_code = code

        if not isinstance(self.missing_fields, tuple):
            raise TypeError("missing_fields must be a tuple")
        for name in self.missing_fields:
            _require_non_empty_str(name, "missing field name")
        previous_name = None
        for name in self.missing_fields:
            if previous_name is not None:
                if name == previous_name:
                    raise ValueError("missing_fields must not contain duplicates")
                if name < previous_name:
                    raise ValueError("missing_fields must be in canonical order")
            previous_name = name

        if not isinstance(self.source_errors, tuple):
            raise TypeError("source_errors must be a tuple")
        for error in self.source_errors:
            if not isinstance(error, SourceError):
                raise TypeError("source_errors entries must be SourceError")
        previous_error = None
        previous_error_key = None
        for error in self.source_errors:
            current_key = _source_error_sort_key(error)
            if previous_error_key is not None and current_key < previous_error_key:
                raise ValueError("source_errors must be in canonical order")
            if previous_error is not None and error == previous_error:
                raise ValueError("source_errors must not contain duplicates")
            previous_error = error
            previous_error_key = current_key

        if not isinstance(self.conflicts, tuple):
            raise TypeError("conflicts must be a tuple")
        for conflict in self.conflicts:
            if not isinstance(conflict, AssessmentConflict):
                raise TypeError("conflicts entries must be AssessmentConflict")
        previous_conflict = None
        previous_conflict_key = None
        for conflict in self.conflicts:
            current_key = _conflict_sort_key(conflict)
            if (
                previous_conflict_key is not None
                and current_key < previous_conflict_key
            ):
                raise ValueError("conflicts must be in canonical order")
            if previous_conflict is not None and conflict == previous_conflict:
                raise ValueError("conflicts must not contain duplicates")
            previous_conflict = conflict
            previous_conflict_key = current_key
        if self.conflicts and self.consistency == "consistent":
            raise ValueError(
                "conflicts are preserved, not resolved: consistency cannot be "
                "'consistent' while conflicts are present"
            )

        if not isinstance(self.capabilities, tuple):
            raise TypeError("capabilities must be a tuple")
        keys: list[str] = []
        for capability in self.capabilities:
            if not isinstance(capability, tuple) or len(capability) != 2:
                raise TypeError("capabilities entries must be (key, value) pairs")
            key, value = capability
            _require_non_empty_str(key, "capability key")
            _require_non_empty_str(value, "capability value")
            keys.append(key)
        if len(set(keys)) != len(keys):
            raise ValueError("capabilities must not repeat a key")
        if keys != sorted(keys):
            raise ValueError("capabilities must be in canonical key order")


def _canonical_unique_tuple(items: tuple, key=None) -> tuple:
    values = items if isinstance(items, tuple) else tuple(items)
    if len(values) < 2:
        return values
    previous = key(values[0]) if key is not None else values[0]
    for item in values[1:]:
        current = key(item) if key is not None else item
        if current <= previous:
            return tuple(sorted(set(values), key=key))
        previous = current
    return values


def build_resolution_quality(
    *,
    completeness: ResolutionCompleteness,
    consistency: ResolutionConsistency,
    confidence: ResolutionConfidence,
    reason_codes: tuple[str, ...] = (),
    missing_fields: tuple[str, ...] = (),
    source_errors: tuple[SourceError, ...] = (),
    conflicts: tuple[AssessmentConflict, ...] = (),
    capabilities: tuple[tuple[str, str], ...] = (),
) -> ResolutionQuality:
    """Build a ``ResolutionQuality`` from unordered, possibly repeated input.

    This is the only place that deduplicates and canonically orders: the
    dataclass itself stays strict so that a hand-built quality can never be
    silently reordered. Deduplication here removes exact repeats only; it
    never merges, ranks or discards distinct facts, and it never derives the
    three axes — those belong to each resolver's declared decisive fields.
    """
    return ResolutionQuality(
        completeness=completeness,
        consistency=consistency,
        confidence=confidence,
        reason_codes=_canonical_unique_tuple(reason_codes),
        missing_fields=_canonical_unique_tuple(missing_fields),
        source_errors=_canonical_unique_tuple(
            source_errors, _source_error_sort_key
        ),
        conflicts=_canonical_unique_tuple(conflicts, _conflict_sort_key),
        capabilities=_canonical_unique_tuple(capabilities),
    )
