"""Accredited assembly: the authorities answer, then one honest status (plan V2 9-17).

The assembly never executes: it decides READY / CLARIFY / OTHER_OWNER for a turn
a credential already claimed, and it never consults the legacy parser, the
QueryRouter or a source. Parameters are only what an authority accredited, and
the residual is computed over the raw turn from the accredited spans — the
credential's own evidence only consumes residual when it does not overreach into
a host-owned span.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date as Date
from enum import Enum

from app.core.accreditation.authorities import (
    JOB_EVIDENCE,
    SUPPLIER_MENTION,
    AccreditedSpan,
    AuthorityReport,
    actor_authority,
    buyer_authority,
    ranking_authority,
    source_authority,
    supplier_guard,
    time_authority,
)
from app.core.accreditation.credential import CapabilityCredential
from app.core.accreditation.guards import (
    actor_exact_guard,
    geography_guard,
    modality_guard,
    money_guard,
)
from app.core.accreditation.registry import CONTRACTS_UNIVERSE
from app.core.accreditation.spec import CapabilitySpec, Dimension
from app.core.query_text_consumption import compact_whitespace, mask_consumed_spans
from app.utils.money import normalize_text

_PUNCTUATION = re.compile(r"[¿?¡!.,;:()\[\]{}\"'«»…]+")
# The capability universe declared by the spec, in the vocabulary Frame reads.
_UNIVERSE_DATASET = {CONTRACTS_UNIVERSE: "contratos"}
# Plan V2 6: the spans whose text can never be swallowed by the model's evidence.
# The supplier mention and the view's own cardinality are the job's own surface —
# "proveedores" and the exact requested amount are what the job is made of, not a
# host filter the evidence could hide.
_HOST_OWNED = frozenset({
    Dimension.BUYER.value, Dimension.TIME.value, Dimension.ACTOR.value,
    Dimension.SOURCE.value, Dimension.RANKING.value, Dimension.MONEY.value,
    Dimension.MODALITY.value, Dimension.GEOGRAPHY.value, Dimension.TOPIC.value,
})


class AssemblyStatus(str, Enum):
    READY = "READY"
    CLARIFY = "CLARIFY"
    OTHER_OWNER = "OTHER_OWNER"


@dataclass(frozen=True, slots=True)
class AssemblyIssue:
    code: str
    dimension: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class AuthorityReceipt:
    dimension: str
    authority: str
    status: str
    detail: str = ""
    spans: int = 0


@dataclass(frozen=True, slots=True)
class AccreditationAssembly:
    status: AssemblyStatus
    reason: str
    params: dict
    spans: tuple[AccreditedSpan, ...]
    issues: tuple[AssemblyIssue, ...]
    authority_receipts: tuple[AuthorityReceipt, ...]
    residual: str
    residual_material: tuple[str, ...]
    credential_overreach: bool
    evidence_consumes_residual: bool
    owner: str = ""

    def receipt_fields(self) -> dict:
        """Flat provenance for the turn result (plan V2 21 counters on top)."""
        buyer = next((span.canonical_value for span in self.spans
                      if span.dimension == Dimension.BUYER.value and span.canonical_value), "")
        temporal = ""
        if self.params.get("fecha_desde") or self.params.get("fecha_hasta"):
            temporal = f"{self.params.get('fecha_desde') or '..'}..{self.params.get('fecha_hasta') or '..'}"
        return {
            "authority_status": self.status.value,
            "authority_reason": self.reason,
            "authority_owner": self.owner,
            "buyer_authority": buyer or "",
            "temporal_authority": temporal,
            "actor_owner": self.owner,
            "ranking_signal": self.params.get("ordering_signal", ""),
            "credential_overreach": self.credential_overreach,
            "evidence_consumes_residual": self.evidence_consumes_residual,
            "residual_text": self.residual,
            "residual_material": list(self.residual_material),
            "accredited_spans": [
                {"dimension": span.dimension, "start": span.start, "end": span.end,
                 "authority": span.authority, "canonical_value": span.canonical_value}
                for span in self.spans
            ],
            "authority_receipts": [
                {"dimension": receipt.dimension, "authority": receipt.authority,
                 "status": receipt.status, "detail": receipt.detail, "spans": receipt.spans}
                for receipt in self.authority_receipts
            ],
        }


def _valid(spans: tuple[AccreditedSpan, ...], raw: str) -> tuple[AccreditedSpan, ...]:
    return tuple(
        span for span in spans
        if 0 <= span.start < span.end <= len(raw)
    )


def _overlaps(first: tuple[int, int], second: tuple[int, int]) -> bool:
    return first[0] < second[1] and second[0] < first[1]


def residual_of(raw: str, spans: tuple[AccreditedSpan, ...]) -> str:
    """Raw turn minus the accredited spans, with only functional cleanup left."""
    intervals = [(span.start, span.end) for span in spans]
    masked = mask_consumed_spans(raw, intervals) if intervals else raw
    cleaned = _PUNCTUATION.sub(" ", compact_whitespace(masked))
    return " ".join(cleaned.split())


def material_tokens(residual: str) -> tuple[str, ...]:
    """Tokens that still name something: the general closed class is not material."""
    from app.data.stopwords import STOPWORDS

    return tuple(
        token for token in residual.split()
        if normalize_text(token) not in STOPWORDS
    )


def _params(reports: tuple[AuthorityReport, ...], spec: CapabilitySpec) -> dict:
    params: dict = {}
    for report in reports:
        for key, value in getattr(report, "params", ()):
            params[key] = value
    dataset = _UNIVERSE_DATASET.get(spec.universe)
    if dataset is not None:
        params["dataset"] = dataset
    return params


def assemble(
    raw: str,
    credential: CapabilityCredential,
    spec: CapabilitySpec,
    *,
    entity_resolver,
    today: Date | None = None,
    actor_resolver=None,
) -> AccreditationAssembly:
    """Run every authority over the raw turn and decide its single status.

    Order matters (plan V2 9-17): authorities first, then the guards of the
    dimensions the spec cannot apply, then span protection, then the evidence
    overreach check and only then the residual. A dimension stops being visible
    because its authority looked at the turn — never because a word happened to
    be in the functional closed class.
    """
    buyer = buyer_authority(raw, entity_resolver=entity_resolver)
    buyer_spans = _valid(buyer.spans, raw)
    protected = tuple((span.start, span.end) for span in buyer_spans)
    buyer_span = protected[0] if protected else None
    time = time_authority(raw, today=today, protected_spans=protected)
    time_spans = _valid(time.spans, raw)
    actor = actor_authority(raw)
    actor_spans = _valid(actor.spans, raw)
    ranking = ranking_authority(raw, protected_spans=protected + tuple(
        (span.start, span.end) for span in time_spans))
    guard = supplier_guard(raw)
    buyer_surface = next(
        (span.canonical_value for span in buyer_spans if span.canonical_value), "")
    source = source_authority(raw, protected_surfaces=(buyer_surface,) if buyer_surface else ())
    modality = modality_guard(raw)
    money = money_guard(raw)
    geography = geography_guard(raw, buyer_span=buyer_span)
    interpreted = tuple(
        (span.start, span.end)
        for report in (buyer, time, actor, ranking, guard, source, modality, money, geography)
        for span in _valid(report.spans, raw)
    )
    actor_exact = actor_exact_guard(raw, actor_resolver=actor_resolver,
                                    protected_spans=interpreted)
    reports = (buyer, time, actor, ranking, guard, source,
               modality, money, geography, actor_exact)

    spans = tuple(
        span for report in reports for span in _valid(report.spans, raw)
    )
    occupied = tuple(
        (span.start, span.end) for span in spans if span.dimension in _HOST_OWNED
    )
    overreach = any(_overlaps(credential.evidence_interval, interval) for interval in occupied)

    conflicts = [
        AssemblyIssue("ACTOR_BUYER_SPAN_CONFLICT", Dimension.ACTOR.value,
                      "el comprador y el actor reclaman el mismo texto")
        for buyer_span in buyer_spans for actor_span in actor_spans
        if _overlaps((buyer_span.start, buyer_span.end), (actor_span.start, actor_span.end))
    ]
    issues: list[AssemblyIssue] = list(conflicts)
    for report in reports:
        if report.status == "CLARIFY":
            issues.append(AssemblyIssue(report.detail or "authority_clarify",
                                        _dimension_of(report), report.clarification))

    evidence_consumes = not overreach
    consumed = spans + (
        (AccreditedSpan(JOB_EVIDENCE, credential.evidence_interval[0],
                        credential.evidence_interval[1], "natural_alpha.job"),)
        if evidence_consumes else ()
    )
    residual = residual_of(raw, tuple(consumed))
    material = material_tokens(residual)
    if material:
        issues.append(AssemblyIssue("UNACCREDITED_RESIDUAL", Dimension.TOPIC.value,
                                    " ".join(material)))

    receipts = tuple(
        AuthorityReceipt(_dimension_of(report), report.authority, report.status,
                         report.detail, len(report.spans))
        for report in reports
    )
    params = _params(reports, spec)
    status, reason, owner = _decide(conflicts, actor, issues, params)
    return AccreditationAssembly(
        status=status, reason=reason, params=params, spans=spans,
        issues=tuple(issues), authority_receipts=receipts,
        residual=residual, residual_material=material,
        credential_overreach=overreach, evidence_consumes_residual=evidence_consumes,
        owner=owner,
    )


def _dimension_of(report) -> str:
    if report.spans:
        return report.spans[0].dimension
    for name, dimension in (
        ("modality_resolver", Dimension.MODALITY.value),
        ("money_polarity", Dimension.MONEY.value),
        ("entity_resolver.TERRITORY_PHRASES", Dimension.GEOGRAPHY.value),
        ("ActorNameResolver", Dimension.ACTOR.value),
        ("buyer", Dimension.BUYER.value),
        ("temporal_semantics", Dimension.TIME.value),
        ("actor_intent", Dimension.ACTOR.value),
        ("control_language", Dimension.RANKING.value),
        ("supplier_request_authority", Dimension.CARDINALITY.value),
        ("contract_universe_intent", Dimension.SOURCE.value),
    ):
        if report.authority.startswith(name):
            return dimension
    return Dimension.TOPIC.value


def _scope_accepted(params: dict) -> bool:
    from app.core.entity_profile.scope import resolve_profile_scope
    from app.core.entity_profile.turn import profile_query_params

    return resolve_profile_scope(profile_query_params(dict(params))).scope is not None


def _decide(
    conflicts: list[AssemblyIssue], actor: AuthorityReport,
    issues: list[AssemblyIssue], params: dict,
) -> tuple[AssemblyStatus, str, str]:
    if conflicts:
        return AssemblyStatus.CLARIFY, conflicts[0].code, ""
    if actor.status == "OTHER_OWNER":
        return AssemblyStatus.OTHER_OWNER, "actor_subject", "ACTOR"
    blocking = [issue for issue in issues if issue.code != "UNACCREDITED_RESIDUAL"]
    if blocking:
        return AssemblyStatus.CLARIFY, blocking[0].code, ""
    if not _scope_accepted(params):
        return AssemblyStatus.CLARIFY, "scope_not_accredited", ""
    residual_issue = next((i for i in issues if i.code == "UNACCREDITED_RESIDUAL"), None)
    if residual_issue is not None:
        return AssemblyStatus.CLARIFY, "UNACCREDITED_RESIDUAL", ""
    return AssemblyStatus.READY, "accredited", ""
