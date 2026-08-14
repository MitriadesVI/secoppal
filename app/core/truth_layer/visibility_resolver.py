"""SECOP Truth Layer T1B-5 — public visibility of a publication.

Pure and deterministic: no network, no clock, no filesystem, no environment,
no parser, no formatter, no Radar, no workflow, no ranking, no SECOP call.
This module never RECONSTRUCTS a URL — it only reads the ones T1A carried.

Three facts that are routinely conflated stay separate here:

    a usable public view  !=  a row visible in the open data
    a row in the open data  !=  a prepublication

An empty URL proves none of them. A login URL is not a public view. A
``CO1.REQ.*`` identifier on its own is not a prepublication, and neither is a
draft on its own: ``prepublication_supported`` requires the joint evidence
frozen by the binding contract (section 9) and nothing less.

The lifecycle stage arrives as INJECTED data (``stage_context``), never by
importing the process resolver: T1B-5 depends on T1B-1 only, and visibility
must not become a second, quieter way of deciding a stage. Visibility also
decides nothing about offerability — that belongs to T1B-6.

Binding documents, read in full before edit:
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_SEMANTIC_CONTRACT.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_EXECUTION_ROADMAP.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R1.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R2.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R3.md
"""
from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from typing import Literal

from app.core.truth_layer.evidence import (
    Evidence,
    EvidenceRole,
    ResolutionQuality,
    SourceError,
    build_resolution_quality,
    validate_evidence_set,
)
from app.core.truth_layer.models import FieldObservation, PublicationVisibilitySignals
from app.core.truth_layer.sentinels import (
    SentinelAssessment,
    assess_sentinel,
    get_field_sentinel_policy,
)

__all__ = [
    "VISIBILITY_DATASET_ID",
    "PublicationVisibility",
    "UrlVerdict",
    "PublicationVisibilityAssessment",
    "resolve_publication_visibility",
]


VISIBILITY_DATASET_ID = "p6dx-8zbt"

_FIELD_PROCESS_ID = "id_del_proceso"
_FIELD_RAW_URL = "urlproceso"
_FIELD_NORMALIZATION_REASON = "urlproceso_normalization_reason"
# A transport-derived field, not a remote column: T1A carries the normalized
# form separately so that neither one can overwrite the other.
_FIELD_NORMALIZED_URL = "urlproceso_normalized"

# Ascending field order is the canonical Evidence order for this resolver:
# every field occurs exactly once and they all share one dataset.
_EVIDENCE_FIELD_ORDER = (
    _FIELD_PROCESS_ID,
    _FIELD_RAW_URL,
    _FIELD_NORMALIZATION_REASON,
    _FIELD_NORMALIZED_URL,
)

_PREPUBLICATION_ID_PREFIX = "co1.req."
_PREPUBLICATION_STAGES: frozenset[str] = frozenset({"draft", "approval"})
_SUPPORTED_SCHEMES: frozenset[str] = frozenset({"http", "https"})

PublicationVisibility = Literal[
    "public_view_available",
    "dataset_only_visible",
    "prepublication_supported",
    "link_missing_or_broken",
    "unknown_visibility",
]

UrlVerdict = Literal["public", "login", "unsupported_scheme", "malformed", "absent"]

_VALID_VISIBILITY: frozenset[str] = frozenset(
    {
        "public_view_available",
        "dataset_only_visible",
        "prepublication_supported",
        "link_missing_or_broken",
        "unknown_visibility",
    }
)
_VALID_URL_VERDICTS: frozenset[str] = frozenset(
    {"public", "login", "unsupported_scheme", "malformed", "absent"}
)


# ---------------------------------------------------------------------------
# URL classification (pure string reading, no reconstruction).
# ---------------------------------------------------------------------------


def _classify_url(raw_value: object) -> UrlVerdict:
    """Classify an observed URL scalar without rewriting or rebuilding it.

    Deliberately hand-rolled rather than delegated to a URL library: the only
    question asked here is "does this scalar denote a reachable public view",
    and a parser that repairs, quotes or normalizes its input would answer a
    different question.
    """
    if not isinstance(raw_value, str):
        return "malformed"
    text = raw_value.strip()
    if not text:
        return "malformed"

    scheme, separator, rest = text.partition("://")
    if not separator:
        # ``javascript:``/``data:`` and every other opaque scheme land here,
        # as does a bare token that is not a URL at all.
        head = text.split(":", 1)[0].casefold() if ":" in text else ""
        return "unsupported_scheme" if head else "malformed"
    if scheme.casefold() not in _SUPPORTED_SCHEMES:
        return "unsupported_scheme"

    authority = rest
    for delimiter in ("/", "?", "#"):
        authority = authority.split(delimiter, 1)[0]
    if not authority:
        return "malformed"

    path = rest[len(authority):].casefold()
    host = authority.casefold()
    if host.startswith("login.") or "/login" in path:
        return "login"
    # No host allow-list is applied: the binding contract freezes none, and
    # inventing one here would reject valid entities. The limitation is
    # reported as capability metadata, not as a per-row conclusion.
    return "public"


def _is_prepublication_id(observation: FieldObservation) -> bool:
    if observation.presence != "observed":
        return False
    raw = observation.raw_value
    return isinstance(raw, str) and raw.strip().casefold().startswith(
        _PREPUBLICATION_ID_PREFIX
    )


def _is_login_rejection_reason(observation: FieldObservation) -> bool:
    if observation.presence != "observed":
        return False
    raw = observation.raw_value
    return isinstance(raw, str) and "login" in raw.casefold()


# ---------------------------------------------------------------------------
# Derivation.
# ---------------------------------------------------------------------------


def _url_verdicts(
    signals: PublicationVisibilitySignals,
) -> tuple[UrlVerdict, UrlVerdict, UrlVerdict, str | None]:
    """Return (raw verdict, normalized verdict, effective verdict, field)."""
    raw_verdict: UrlVerdict = (
        _classify_url(signals.raw_url.raw_value)
        if signals.raw_url.presence == "observed"
        else "absent"
    )
    normalized_verdict: UrlVerdict = (
        _classify_url(signals.normalized_url.raw_value)
        if signals.normalized_url.presence == "observed"
        else "absent"
    )
    if signals.normalized_url.presence == "observed":
        return raw_verdict, normalized_verdict, normalized_verdict, _FIELD_NORMALIZED_URL
    if signals.raw_url.presence == "observed":
        return raw_verdict, normalized_verdict, raw_verdict, _FIELD_RAW_URL
    return raw_verdict, normalized_verdict, "absent", None


def _derive_visibility(
    signals: PublicationVisibilitySignals,
    stage_context: str | None,
    source_errors: tuple[SourceError, ...],
) -> tuple[PublicationVisibility, UrlVerdict, str | None, tuple[str, ...]]:
    raw_verdict, _normalized_verdict, verdict, verdict_field = _url_verdicts(signals)

    both_unobserved = (
        signals.raw_url.presence == "not_observed"
        and signals.normalized_url.presence == "not_observed"
    )
    if source_errors or both_unobserved:
        # A source that did not answer proves nothing about the publication.
        return (
            "unknown_visibility",
            verdict,
            verdict_field,
            (
                "VISIBILITY_SOURCE_ERROR"
                if source_errors
                else "VISIBILITY_URL_NOT_OBSERVED",
            ),
        )

    if verdict == "public":
        # A draft with a working public link is still publicly viewable: the
        # stage never vetoes an observed fact.
        return (
            "public_view_available",
            verdict,
            verdict_field,
            ("VISIBILITY_PUBLIC_URL_OBSERVED",),
        )

    prepublication = (
        stage_context in _PREPUBLICATION_STAGES
        and signals.normalized_url.presence == "missing"
        and (
            _is_prepublication_id(signals.raw_process_id)
            or _is_login_rejection_reason(signals.normalization_reason)
        )
    )
    if prepublication:
        return (
            "prepublication_supported",
            verdict,
            verdict_field,
            ("VISIBILITY_PREPUBLICATION_JOINT_EVIDENCE",),
        )

    if verdict in ("login", "unsupported_scheme", "malformed"):
        return (
            "link_missing_or_broken",
            verdict,
            verdict_field,
            (f"VISIBILITY_URL_{verdict.upper()}",),
        )

    if signals.raw_process_id.presence == "observed":
        # The row exists in the open data even though no public view is
        # available. That is a weaker fact than a link, never a stronger one.
        return (
            "dataset_only_visible",
            verdict,
            verdict_field,
            ("VISIBILITY_DATASET_ROW_WITHOUT_PUBLIC_VIEW",),
        )

    return (
        "unknown_visibility",
        verdict,
        verdict_field,
        ("VISIBILITY_INDETERMINATE",),
    )


def _role_for(
    observation: FieldObservation, *, contradicts: bool = False
) -> EvidenceRole:
    if observation.presence == "missing":
        return "missing"
    if observation.presence == "not_observed":
        return "not_observed"
    return "contradicts" if contradicts else "supports"


def _evidence_for(
    field: str,
    observation: FieldObservation,
    *,
    code: str,
    contradicts: bool = False,
) -> Evidence:
    assessment: SentinelAssessment | None = None
    normalized = None
    policy = get_field_sentinel_policy(VISIBILITY_DATASET_ID, field)
    if policy is not None and observation.presence == "observed":
        assessment = assess_sentinel(observation, policy)
        normalized = assessment.normalized_comparison_value
    return Evidence(
        code=code,
        dataset=VISIBILITY_DATASET_ID,
        field=field,
        raw_observation=observation,
        sentinel_assessment=assessment,
        normalized_value=normalized,
        role=_role_for(observation, contradicts=contradicts),
    )


def _build_evidence(
    signals: PublicationVisibilitySignals,
    verdict: UrlVerdict,
    verdict_field: str | None,
) -> tuple[Evidence, ...]:
    rejected = verdict in ("login", "unsupported_scheme", "malformed")
    by_field = {
        _FIELD_PROCESS_ID: _evidence_for(
            _FIELD_PROCESS_ID,
            signals.raw_process_id,
            code="VISIBILITY_PROCESS_ID_EVIDENCE",
        ),
        _FIELD_RAW_URL: _evidence_for(
            _FIELD_RAW_URL,
            signals.raw_url,
            code="VISIBILITY_RAW_URL_EVIDENCE",
            contradicts=rejected and verdict_field == _FIELD_RAW_URL,
        ),
        _FIELD_NORMALIZATION_REASON: _evidence_for(
            _FIELD_NORMALIZATION_REASON,
            signals.normalization_reason,
            code="VISIBILITY_NORMALIZATION_REASON_EVIDENCE",
        ),
        _FIELD_NORMALIZED_URL: _evidence_for(
            _FIELD_NORMALIZED_URL,
            signals.normalized_url,
            code="VISIBILITY_NORMALIZED_URL_EVIDENCE",
            contradicts=rejected and verdict_field == _FIELD_NORMALIZED_URL,
        ),
    }
    return tuple(by_field[field] for field in _EVIDENCE_FIELD_ORDER)


def _build_quality(
    signals: PublicationVisibilitySignals,
    visibility: PublicationVisibility,
    reasons: tuple[str, ...],
    source_errors: tuple[SourceError, ...],
) -> ResolutionQuality:
    reasons = tuple(reasons)
    if visibility == "public_view_available":
        # The syntactic fact is real, so the state stands. The QUALITY must
        # not overstate it: while no host allow-list is authorized, "this
        # string is a well-formed https URL" is not "this link reaches the
        # public view of this process". Declaring the capability unavailable
        # and still reporting complete/high would contradict our own metadata.
        completeness = "partial"
        confidence = "medium"
        reasons = reasons + ("VISIBILITY_PUBLIC_URL_HOST_UNVERIFIED",)
    elif visibility in ("dataset_only_visible", "prepublication_supported"):
        completeness = "partial"
        confidence = "medium"
    elif visibility == "link_missing_or_broken":
        completeness = "partial"
        confidence = "low"
    else:
        completeness = "insufficient"
        confidence = "low"

    consistency = "unknown" if source_errors else "consistent"
    if source_errors:
        confidence = "low"

    missing = tuple(
        field
        for field, observation in (
            (_FIELD_PROCESS_ID, signals.raw_process_id),
            (_FIELD_RAW_URL, signals.raw_url),
            (_FIELD_NORMALIZATION_REASON, signals.normalization_reason),
            (_FIELD_NORMALIZED_URL, signals.normalized_url),
        )
        if observation.presence == "missing"
    )
    return build_resolution_quality(
        completeness=completeness,  # type: ignore[arg-type]
        consistency=consistency,  # type: ignore[arg-type]
        confidence=confidence,  # type: ignore[arg-type]
        reason_codes=reasons,
        missing_fields=missing,
        source_errors=source_errors,
        capabilities=(("visibility_host_allowlist", "unavailable"),),
    )


@dataclass(frozen=True, slots=True)
class _VisibilityDerivation:
    signals: PublicationVisibilitySignals
    stage_context: str | None
    source_errors: tuple[SourceError, ...]
    visibility: PublicationVisibility
    verdict: UrlVerdict
    verdict_field: str | None
    reasons: tuple[str, ...]
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality


def _derive(
    signals: PublicationVisibilitySignals,
    stage_context: str | None,
    source_errors: tuple[SourceError, ...],
) -> _VisibilityDerivation:
    visibility, verdict, verdict_field, reasons = _derive_visibility(
        signals, stage_context, source_errors
    )
    evidence = _build_evidence(signals, verdict, verdict_field)
    quality = _build_quality(signals, visibility, reasons, source_errors)
    return _VisibilityDerivation(
        signals=signals,
        stage_context=stage_context,
        source_errors=quality.source_errors,
        visibility=visibility,
        verdict=verdict,
        verdict_field=verdict_field,
        reasons=reasons,
        evidence=evidence,
        quality=quality,
    )


# ---------------------------------------------------------------------------
# PublicationVisibilityAssessment.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PublicationVisibilityAssessment:
    """What can be said about seeing this publication — and nothing else.

    ``raw_url`` and ``normalized_url`` are kept apart by identity: the
    normalized form is a comparison aid, never a replacement, and neither is
    ever rebuilt here.

    The conclusion is recomputed in ``__post_init__``, so no public
    constructor — ``dataclasses.replace`` included — can mint a visibility its
    own signals do not support.
    """

    signals: PublicationVisibilitySignals
    stage_context: str | None
    visibility: PublicationVisibility
    url_verdict: UrlVerdict
    raw_url: FieldObservation
    normalized_url: FieldObservation
    raw_process_id: FieldObservation
    normalization_reason: FieldObservation
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality
    _resolved_derivation: InitVar[_VisibilityDerivation | None] = field(
        default=None, kw_only=True
    )

    def __post_init__(
        self, _resolved_derivation: _VisibilityDerivation | None
    ) -> None:
        if not isinstance(self.signals, PublicationVisibilitySignals):
            raise TypeError("signals must be PublicationVisibilitySignals")
        if self.stage_context is not None and (
            not isinstance(self.stage_context, str) or not self.stage_context
        ):
            raise ValueError("stage_context must be a non-empty string or None")
        if self.visibility not in _VALID_VISIBILITY:
            raise ValueError(f"invalid PublicationVisibility: {self.visibility!r}")
        if self.url_verdict not in _VALID_URL_VERDICTS:
            raise ValueError(f"invalid UrlVerdict: {self.url_verdict!r}")
        for name in (
            "raw_url",
            "normalized_url",
            "raw_process_id",
            "normalization_reason",
        ):
            if getattr(self, name) is not getattr(self.signals, name):
                raise ValueError(f"{name} must come from signals")
        if not isinstance(self.quality, ResolutionQuality):
            raise TypeError("quality must be a ResolutionQuality")

        if _resolved_derivation is None:
            validate_evidence_set(self.evidence)
            derived = _derive(
                self.signals, self.stage_context, self.quality.source_errors
            )
        else:
            if type(_resolved_derivation) is not _VisibilityDerivation:
                raise TypeError(
                    "_resolved_derivation must be an internal visibility derivation"
                )
            derived = _resolved_derivation
            if self.signals is not derived.signals:
                raise ValueError(
                    "resolved visibility derivation belongs to different signals"
                )
            if self.quality.source_errors is not derived.source_errors:
                raise ValueError(
                    "resolved visibility derivation belongs to different source_errors"
                )
            if (
                type(self.stage_context) is not type(derived.stage_context)
                or self.stage_context != derived.stage_context
            ):
                raise ValueError(
                    "resolved visibility derivation has different stage_context"
                )
            if (
                self.evidence is not derived.evidence
                or self.quality is not derived.quality
            ):
                raise ValueError(
                    "resolved visibility derivation belongs to different evidence or quality"
                )
        if (
            type(self.visibility) is not type(derived.visibility)
            or self.visibility != derived.visibility
        ):
            raise ValueError(
                f"visibility={self.visibility!r} contradicts its signals, which "
                f"yield {derived.visibility!r}"
            )
        if (
            type(self.url_verdict) is not type(derived.verdict)
            or self.url_verdict != derived.verdict
        ):
            raise ValueError("url_verdict must be derived from the observed URLs")
        if (
            type(self.evidence) is not type(derived.evidence)
            or self.evidence != derived.evidence
        ):
            raise ValueError("visibility evidence must be canonical and complete")
        if type(self.quality) is not type(derived.quality) or self.quality != derived.quality:
            raise ValueError("visibility quality must be derived from its inputs")


def resolve_publication_visibility(
    signals: PublicationVisibilitySignals,
    *,
    stage_context: str | None = None,
    source_errors: tuple[SourceError, ...] = (),
) -> PublicationVisibilityAssessment:
    """Resolve public visibility from carried signals and an injected stage."""
    if not isinstance(signals, PublicationVisibilitySignals):
        raise TypeError(
            "resolve_publication_visibility requires PublicationVisibilitySignals"
        )
    if not isinstance(source_errors, tuple):
        raise TypeError("source_errors must be a tuple")
    for error in source_errors:
        if not isinstance(error, SourceError):
            raise TypeError("source_errors entries must be SourceError")
        if error.dataset != VISIBILITY_DATASET_ID:
            raise ValueError(
                f"visibility source errors must belong to {VISIBILITY_DATASET_ID}"
            )
    derived = _derive(
        signals, stage_context, source_errors
    )
    return PublicationVisibilityAssessment(
        signals=signals,
        stage_context=stage_context,
        visibility=derived.visibility,
        url_verdict=derived.verdict,
        raw_url=signals.raw_url,
        normalized_url=signals.normalized_url,
        raw_process_id=signals.raw_process_id,
        normalization_reason=signals.normalization_reason,
        evidence=derived.evidence,
        quality=derived.quality,
        _resolved_derivation=derived,
    )
