"""Accredited assembly: the authorities answer, then one honest status (plan V2 9-17).

The assembly never executes: it decides READY / CLARIFY / OTHER_OWNER for a turn
a credential already claimed, and it never consults the legacy parser, the
QueryRouter or a source. Parameters are only what an authority accredited, and
the residual is computed over the raw turn from the accredited spans — the
credential's own evidence never consumes residual.

C4B order: the strong actor authority (``detect_actor_name``) keeps its place at
the head of the chain, then the deterministic authorities, then the residual —
the only point where nothing else can decide —, then the residual-role verifier
(B) and only then the weak exact actor guard. The verifier can only veto: MATERIAL,
UNKNOWN and every error stop the turn before the weak guard is even called, and
only all-``JOB_LANGUAGE`` lets the guard be asked.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date as Date
from enum import Enum

from app.core.accreditation.authorities import (
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
    GuardReport,
    actor_exact_guard,
    geography_guard,
    modality_guard,
    money_guard,
)
from app.core.accreditation.registry import CONTRACTS_UNIVERSE
from app.core.accreditation.residual_verifier import (
    ResidualVerification,
    not_reached,
    residual_role_contract,
    verify_residual,
)
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
# The receipt the weak guard leaves when the residual verifier stopped the turn:
# the guard has no opinion because it was never asked, and saying so is the point.
_GUARD_NOT_RUN = "NOT_RUN"
_GUARD_NOT_RUN_DETAIL = "not_run_residual_material"
_WEAK_GUARD_AUTHORITY = "ActorNameResolver.exact_aliases_in"


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
    # C4B: the verifier's receipt and whether the weak actor guard was asked.
    residual_verification: ResidualVerification | None = None
    weak_actor_guard_called: bool = False

    def receipt_fields(self) -> dict:
        """Flat provenance for the turn result (plan V2 21 counters on top)."""
        buyer = next((span.canonical_value for span in self.spans
                      if span.dimension == Dimension.BUYER.value and span.canonical_value), "")
        temporal = ""
        if self.params.get("fecha_desde") or self.params.get("fecha_hasta"):
            temporal = f"{self.params.get('fecha_desde') or '..'}..{self.params.get('fecha_hasta') or '..'}"
        fields = {
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
            "weak_actor_guard_called": self.weak_actor_guard_called,
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
        # The verifier's own receipt travels flat: called/not-reached/not-called,
        # the contract, the units it saw, its labels and its latency. No key, no
        # secret, ever.
        if self.residual_verification is not None:
            fields.update(self.residual_verification.receipt_fields())
        return fields


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


def _params(reports: tuple[AuthorityReport | GuardReport, ...], spec: CapabilitySpec) -> dict:
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
    residual_verifier=None,
) -> AccreditationAssembly:
    """Run every authority over the raw turn and decide its single status.

    Order matters (plan V2 9-17, C4B): authorities first, then the guards of the
    dimensions the spec cannot apply, then span protection, then the residual —
    and there the verifier answers before the weak exact actor guard is asked. A
    dimension stops being visible because its authority looked at the turn —
    never because a word happened to be in the functional closed class.
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
    reports = (buyer, time, actor, ranking, guard, source, modality, money, geography)
    # What the weak guard must never re-read as an actor: every span the host has
    # already interpreted (same call site the C4A.5 audit measured).
    interpreted = tuple(
        (span.start, span.end)
        for report in reports
        for span in _valid(report.spans, raw)
    )
    # The residual is what THESE authorities did not accredit. The weak guard is
    # deliberately absent here: its alias would otherwise hide text from the
    # verifier before the verifier was asked (that is what D01/H05 measured).
    spans = tuple(span for report in reports for span in _valid(report.spans, raw))

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

    # The credential's own evidence never consumes residual: the model's quote is
    # not an authority over the raw turn, so masking it could hide material the
    # host never accredited (C4A.3).
    evidence_consumes = False
    residual = residual_of(raw, spans)
    material = material_tokens(residual)
    params = _params(reports, spec)
    status, reason, owner = _decide(conflicts, actor, issues, params)

    verification: ResidualVerification = not_reached(spec.capability)
    weak_actor_guard_called = False
    guard_receipt = AuthorityReceipt(Dimension.ACTOR.value, _WEAK_GUARD_AUTHORITY,
                                     _GUARD_NOT_RUN, _GUARD_NOT_RUN_DETAIL)
    if status is AssemblyStatus.READY:
        # C4B: the residual is the last dimension without an owner. With nothing
        # to adjudicate there is no call at all; with units, the verifier decides
        # first and the weak actor guard is asked only if it did not veto.
        verification = verify_residual(
            raw, material, contract=residual_role_contract(spec.capability),
            provider=residual_verifier,
        )
        if material and not verification.adjudicated:
            status, reason = AssemblyStatus.CLARIFY, "UNACCREDITED_RESIDUAL"
            issues.append(AssemblyIssue("UNACCREDITED_RESIDUAL", Dimension.TOPIC.value,
                                        " ".join(material)))
        else:
            actor_exact = actor_exact_guard(raw, actor_resolver=actor_resolver,
                                            protected_spans=interpreted)
            weak_actor_guard_called = True
            spans = spans + _valid(actor_exact.spans, raw)
            guard_receipt = AuthorityReceipt(
                Dimension.ACTOR.value, actor_exact.authority, actor_exact.status,
                actor_exact.detail, len(actor_exact.spans))
            if actor_exact.status == "CLARIFY":
                status, reason = AssemblyStatus.CLARIFY, actor_exact.detail
                issues.append(AssemblyIssue(actor_exact.detail, Dimension.ACTOR.value,
                                            actor_exact.clarification))

    occupied = tuple(
        (span.start, span.end) for span in spans if span.dimension in _HOST_OWNED
    )
    overreach = any(_overlaps(credential.evidence_interval, interval) for interval in occupied)

    receipts = tuple(
        AuthorityReceipt(_dimension_of(report), report.authority, report.status,
                         report.detail, len(report.spans))
        for report in reports
    ) + (guard_receipt,)
    return AccreditationAssembly(
        status=status, reason=reason, params=params, spans=spans,
        issues=tuple(issues), authority_receipts=receipts,
        residual=residual, residual_material=material,
        credential_overreach=overreach, evidence_consumes_residual=evidence_consumes,
        owner=owner, residual_verification=verification,
        weak_actor_guard_called=weak_actor_guard_called,
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
    """Everything that decides before the residual: conflicts, actor, guards, scope.

    READY here means "the turn reached the residual point", not "the turn may
    execute": the residual verifier and the weak actor guard still have to answer.
    """
    if conflicts:
        return AssemblyStatus.CLARIFY, conflicts[0].code, ""
    if actor.status == "OTHER_OWNER":
        return AssemblyStatus.OTHER_OWNER, "actor_subject", "ACTOR"
    blocking = [issue for issue in issues if issue.code != "UNACCREDITED_RESIDUAL"]
    if blocking:
        return AssemblyStatus.CLARIFY, blocking[0].code, ""
    if not _scope_accepted(params):
        return AssemblyStatus.CLARIFY, "scope_not_accredited", ""
    return AssemblyStatus.READY, "accredited", ""
