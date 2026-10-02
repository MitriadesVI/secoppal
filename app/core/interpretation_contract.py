"""Foundation interpretation records (IR v2). Only the host may accept or project a proposal.

The proposal contains linguistic claims and raw evidence, never executable values.
Each record is immutable and serializes through ``to_dict`` with standard JSON.

IR v2 (spine v5, C0 §5.1): the operation the user asks for is a ``job`` (``jobs[]``, one per operation) and
no longer a field of the request; a request's subject may be ``unspecified``; a GEOGRAPHY condition names its
``level``; every operator belongs to a closed per-dimension table. The v4.2 wire is lifted into this shape by
``foundation_lift_v1`` without losing anything (INV-C0-2).
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import asdict, dataclass
from enum import Enum

from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_operations import Aspect, Operation, Subject, job_shape_error
from app.core.foundation_operator_table import operator_allowed
from app.core.semantic_dimensions import SemanticDimension


SCHEMA_VERSION = 2
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class ContractError(ValueError):
    """A record cannot be admitted as a Foundation V3 contract."""


class Polarity(str, Enum):
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"


class ConditionRole(str, Enum):
    FILTER = "FILTER"
    REQUESTED_UNIVERSE = "REQUESTED_UNIVERSE"


class Omission(str, Enum):
    NO_CHANGE = "NO_CHANGE"
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    ERROR = "ERROR"


class RelationKind(str, Enum):
    SHARES_SCOPE = "SHARES_SCOPE"
    USES_RESULT_OF = "USES_RESULT_OF"


class LogicOperator(str, Enum):
    ATOM = "ATOM"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


class AcceptanceStatus(str, Enum):
    READY = "READY"
    NEEDS_CLARIFICATION = "NEEDS_CLARIFICATION"
    UNSUPPORTED = "UNSUPPORTED"
    INVALID_PROPOSAL = "INVALID_PROPOSAL"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    NO_ACTION = "NO_ACTION"


class ResolutionDecision(str, Enum):
    RESOLVED = "RESOLVED"
    CLARIFY = "CLARIFY"
    BLOCK = "BLOCK"


class Disposition(str, Enum):
    CONSUMED = "CONSUMED"
    TRANSFORMED = "TRANSFORMED"
    DEFAULTED = "DEFAULTED"
    BLOCKED = "BLOCKED"


class ProjectionStatus(str, Enum):
    READY = "READY"
    BLOCKED = "BLOCKED"


class Effect(str, Enum):
    EQUIVALENT = "EQUIVALENT"
    WIDENING = "WIDENING"
    NARROWING = "NARROWING"
    APPROXIMATION = "APPROXIMATION"


def _identifier(value: str, label: str) -> None:
    if type(value) is not str or not value.strip():
        raise ContractError(f"INVALID_{label}")


def _ids(values: tuple[str, ...], label: str) -> None:
    if type(values) is not tuple or not values:
        raise ContractError(f"INVALID_{label}")
    for value in values:
        _identifier(value, label)
    if len(set(values)) != len(values):
        raise ContractError(f"DUPLICATE_{label}")


def _records(values: tuple, record_type: type, label: str) -> None:
    if type(values) is not tuple or any(type(item) is not record_type for item in values):
        raise ContractError(f"INVALID_{label}")


@dataclass(frozen=True, slots=True)
class RawSpan:
    start: int
    end: int
    quote: str

    def __post_init__(self) -> None:
        if (type(self.start) is not int or type(self.end) is not int
                or self.start < 0 or self.end <= self.start):
            raise ContractError("INVALID_RAW_SPAN")
        _identifier(self.quote, "RAW_QUOTE")

    def binds(self, raw: str) -> bool:
        return (
            self.end <= len(raw)
            and unicodedata.normalize("NFC", self.quote)
            == unicodedata.normalize("NFC", raw[self.start:self.end])
        )


@dataclass(frozen=True, slots=True)
class ConditionExpression:
    operator: LogicOperator
    condition_id: str | None = None
    children: tuple[ConditionExpression, ...] = ()

    def __post_init__(self) -> None:
        if type(self.operator) is not LogicOperator:
            raise ContractError("INVALID_LOGIC_OPERATOR")
        _records(self.children, ConditionExpression, "EXPRESSION_CHILDREN")
        if self.operator is LogicOperator.ATOM:
            _identifier(self.condition_id, "EXPRESSION_ATOM")
            if self.children:
                raise ContractError("ATOM_HAS_CHILDREN")
        elif self.condition_id is not None:
            raise ContractError("GROUP_HAS_ATOM")
        elif self.operator is LogicOperator.NOT and len(self.children) != 1:
            raise ContractError("INVALID_NOT_ARITY")
        elif self.operator in (LogicOperator.AND, LogicOperator.OR) and len(self.children) < 2:
            raise ContractError("INVALID_GROUP_ARITY")

    def atoms(self) -> tuple[str, ...]:
        if self.operator is LogicOperator.ATOM:
            return (self.condition_id,)
        return tuple(atom for child in self.children for atom in child.atoms())

    def has_nonconjunctive_atom(self, condition_id: str) -> bool:
        if self.operator is LogicOperator.ATOM:
            return False
        if self.operator in (LogicOperator.OR, LogicOperator.NOT):
            return condition_id in self.atoms()
        return any(child.has_nonconjunctive_atom(condition_id) for child in self.children)


@dataclass(frozen=True, slots=True)
class ProposedJob:
    """One operation the user asks to receive, with the requests it transports (C0 §5.1.1)."""

    job_id: str
    operation: Operation
    request_ids: tuple[str, ...] = ()
    evidence: tuple[RawSpan, ...] = ()
    aspect: Aspect | None = None
    aspect_label: str | None = None
    referent_evidence: RawSpan | None = None

    def __post_init__(self) -> None:
        _identifier(self.job_id, "JOB_ID")
        if type(self.operation) is not Operation:
            raise ContractError("INVALID_OPERATION")
        if type(self.request_ids) is not tuple:
            raise ContractError("INVALID_JOB_REQUEST_IDS")
        for request_id in self.request_ids:
            _identifier(request_id, "JOB_REQUEST_IDS")
        if len(set(self.request_ids)) != len(self.request_ids):
            raise ContractError("DUPLICATE_JOB_REQUEST_IDS")
        _records(self.evidence, RawSpan, "JOB_EVIDENCE")
        if self.aspect is not None and type(self.aspect) is not Aspect:
            raise ContractError("INVALID_ASPECT")
        if self.aspect_label is not None:
            _identifier(self.aspect_label, "ASPECT_LABEL")
        if self.referent_evidence is not None and type(self.referent_evidence) is not RawSpan:
            raise ContractError("INVALID_REFERENT_EVIDENCE")
        shape = job_shape_error(self.operation, self.aspect, self.aspect_label,
                                request_count=len(self.request_ids), evidence_count=len(self.evidence))
        if shape is not None:
            raise ContractError(shape)


@dataclass(frozen=True, slots=True)
class ProposedRequest:
    request_id: str
    scope_id: str
    subject: Subject
    evidence: tuple[RawSpan, ...]
    source_claim: str | None = None
    source_evidence: RawSpan | None = None
    universe_claim: str | None = None
    universe_evidence: RawSpan | None = None
    condition_expression: ConditionExpression | None = None

    def __post_init__(self) -> None:
        for name in ("request_id", "scope_id"):
            _identifier(getattr(self, name), name.upper())
        if type(self.subject) is not Subject:
            raise ContractError("INVALID_SUBJECT")
        _records(self.evidence, RawSpan, "REQUEST_EVIDENCE")
        if not self.evidence:
            raise ContractError("MISSING_REQUEST_EVIDENCE")
        for claim, evidence, label in (
            (self.source_claim, self.source_evidence, "SOURCE"),
            (self.universe_claim, self.universe_evidence, "UNIVERSE"),
        ):
            if (claim is None) != (evidence is None):
                raise ContractError(f"UNGROUNDED_{label}_CLAIM")
            if claim is not None:
                _identifier(claim, f"{label}_CLAIM")
                if type(evidence) is not RawSpan:
                    raise ContractError(f"INVALID_{label}_EVIDENCE")
        if self.condition_expression is not None and type(self.condition_expression) is not ConditionExpression:
            raise ContractError("INVALID_CONDITION_EXPRESSION")
        # The universe claim exists sí y sólo sí the user named a subject (C0 §5.1.2).
        if self.subject is Subject.UNSPECIFIED:
            if self.universe_claim is not None:
                raise ContractError("UNSPECIFIED_WITH_UNIVERSE_EVIDENCE")
        elif self.universe_claim is None:
            raise ContractError("UNGROUNDED_REQUEST_UNIVERSE")


@dataclass(frozen=True, slots=True)
class ProposedCondition:
    condition_id: str
    request_ids: tuple[str, ...]
    dimension: SemanticDimension
    operator: str
    polarity: Polarity
    claim: str
    evidence: tuple[RawSpan, ...]
    unsupported_label: str | None = None
    role: ConditionRole = ConditionRole.FILTER
    level: GeoLevel | None = None

    def __post_init__(self) -> None:
        _identifier(self.condition_id, "CONDITION_ID")
        _ids(self.request_ids, "CONDITION_REQUEST_IDS")
        if type(self.dimension) is not SemanticDimension:
            raise ContractError("INVALID_DIMENSION")
        _identifier(self.operator, "OPERATOR")
        if type(self.polarity) is not Polarity:
            raise ContractError("INVALID_POLARITY")
        _identifier(self.claim, "CLAIM")
        _records(self.evidence, RawSpan, "CONDITION_EVIDENCE")
        if not self.evidence:
            raise ContractError("MISSING_CONDITION_EVIDENCE")
        if type(self.role) is not ConditionRole:
            raise ContractError("INVALID_CONDITION_ROLE")
        if (self.role is ConditionRole.REQUESTED_UNIVERSE) != (
            self.dimension is SemanticDimension.RETRIEVAL_INTENT
        ):
            raise ContractError("INVALID_REQUEST_UNIVERSE_DIMENSION")
        if self.role is ConditionRole.REQUESTED_UNIVERSE and self.polarity is not Polarity.POSITIVE:
            raise ContractError("NEGATED_REQUEST_UNIVERSE")
        if self.dimension is SemanticDimension.UNSUPPORTED:
            _identifier(self.unsupported_label, "UNSUPPORTED_LABEL")
        elif self.unsupported_label is not None:
            raise ContractError("UNEXPECTED_UNSUPPORTED_LABEL")
        if not operator_allowed(self.dimension, self.operator):
            raise ContractError("OPERATOR_OUTSIDE_DIMENSION")
        if self.dimension is SemanticDimension.GEOGRAPHY:
            if self.level is None:
                raise ContractError("GEOGRAPHY_LEVEL_REQUIRED")
            if type(self.level) is not GeoLevel:
                raise ContractError("INVALID_GEO_LEVEL")
        elif self.level is not None:
            raise ContractError("LEVEL_OUTSIDE_GEOGRAPHY")


@dataclass(frozen=True, slots=True)
class ProposedOmission:
    request_id: str
    dimension: SemanticDimension
    state: Omission

    def __post_init__(self) -> None:
        _identifier(self.request_id, "OMISSION_REQUEST_ID")
        if type(self.dimension) is not SemanticDimension or type(self.state) is not Omission:
            raise ContractError("INVALID_OMISSION")


@dataclass(frozen=True, slots=True)
class ProposedRelation:
    kind: RelationKind
    members: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.kind) is not RelationKind:
            raise ContractError("INVALID_RELATION_KIND")
        _ids(self.members, "RELATION_MEMBERS")
        if len(self.members) < 2 or (self.kind is RelationKind.USES_RESULT_OF
                                     and len(self.members) != 2):
            raise ContractError("INVALID_RELATION_ARITY")


@dataclass(frozen=True, slots=True)
class InterpretationProposal:
    raw_sha256: str
    producer: str
    producer_version: str
    requests: tuple[ProposedRequest, ...]
    conditions: tuple[ProposedCondition, ...] = ()
    relations: tuple[ProposedRelation, ...] = ()
    omissions: tuple[ProposedOmission, ...] = ()
    jobs: tuple[ProposedJob, ...] = ()
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if type(self.raw_sha256) is not str or not _SHA256.fullmatch(self.raw_sha256):
            raise ContractError("INVALID_RAW_HASH")
        for name in ("producer", "producer_version"):
            _identifier(getattr(self, name), name.upper())
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ContractError("UNSUPPORTED_SCHEMA_VERSION")
        for name, record_type in (("requests", ProposedRequest),
                                  ("conditions", ProposedCondition),
                                  ("relations", ProposedRelation),
                                  ("omissions", ProposedOmission),
                                  ("jobs", ProposedJob)):
            _records(getattr(self, name), record_type, name.upper())
        request_ids = {item.request_id for item in self.requests}
        if len(request_ids) != len(self.requests):
            raise ContractError("DUPLICATE_REQUEST_ID")
        condition_ids = {item.condition_id for item in self.conditions}
        if len(condition_ids) != len(self.conditions):
            raise ContractError("DUPLICATE_CONDITION_ID")
        for condition in self.conditions:
            if not set(condition.request_ids) <= request_ids:
                raise ContractError("FOREIGN_CONDITION_REQUEST")
        for request in self.requests:
            expected = {
                condition.condition_id for condition in self.conditions
                if request.request_id in condition.request_ids
            }
            expression = request.condition_expression
            atoms = expression.atoms() if expression is not None else ()
            if len(atoms) != len(set(atoms)):
                raise ContractError("DUPLICATE_CONDITION_ATOM")
            if set(atoms) - expected:
                raise ContractError("FOREIGN_CONDITION_ATOM")
            if set(atoms) != expected:
                raise ContractError("INCOMPLETE_CONDITION_EXPRESSION")
            universe_conditions = [
                condition for condition in self.conditions
                if condition.role is ConditionRole.REQUESTED_UNIVERSE
                and request.request_id in condition.request_ids
            ]
            if len(universe_conditions) > 1:
                raise ContractError("MULTIPLE_REQUEST_UNIVERSES")
            for condition in universe_conditions:
                if (condition.request_ids != (request.request_id,)
                        or request.universe_claim != condition.claim
                        or request.universe_evidence is None
                        or condition.evidence != (request.universe_evidence,)):
                    raise ContractError("REQUEST_UNIVERSE_EVIDENCE_MISMATCH")
                if expression is not None and expression.has_nonconjunctive_atom(condition.condition_id):
                    raise ContractError("NONCONJUNCTIVE_REQUEST_UNIVERSE")
        omission_keys = {(item.request_id, item.dimension) for item in self.omissions}
        if len(omission_keys) != len(self.omissions):
            raise ContractError("DUPLICATE_OMISSION")
        if any(item.request_id not in request_ids for item in self.omissions):
            raise ContractError("FOREIGN_OMISSION_REQUEST")
        self._validate_relations(request_ids)
        self._validate_jobs(request_ids)
        self._validate_order_and_cardinality()

    def job_of(self, request_id: str) -> ProposedJob | None:
        """The one job that transports this request; None when the proposal has no such request."""
        return next((job for job in self.jobs if request_id in job.request_ids), None)

    def operation_of(self, request_id: str) -> Operation | None:
        job = self.job_of(request_id)
        return job.operation if job is not None else None

    @property
    def is_multi(self) -> bool:
        """MULTI is derived, never a wire value: more than one operation was asked."""
        return len(self.jobs) > 1

    def _validate_jobs(self, request_ids: set[str]) -> None:
        if not self.jobs:
            # Only the empty interpretation («no request») has no job; anything with a request needs its operation.
            if self.requests:
                raise ContractError("MISSING_JOBS")
            return
        if len({job.job_id for job in self.jobs}) != len(self.jobs):
            raise ContractError("DUPLICATE_JOB_ID")
        if len({job.operation for job in self.jobs}) != len(self.jobs):
            raise ContractError("DUPLICATE_JOB_OPERATION")
        carried: dict[str, int] = {}
        for job in self.jobs:
            if not set(job.request_ids) <= request_ids:
                raise ContractError("FOREIGN_JOB_REQUEST")
            for request_id in job.request_ids:
                carried[request_id] = carried.get(request_id, 0) + 1
        for request in self.requests:
            if carried.get(request.request_id, 0) == 0:
                raise ContractError("ORPHAN_REQUEST")
            if carried[request.request_id] > 1:
                raise ContractError("REQUEST_IN_TWO_JOBS")

    def _validate_order_and_cardinality(self) -> None:
        """ORDER and CARDINALITY: at most one per request; ORDER is a top-level AND atom; top_n needs an order."""
        for request in self.requests:
            mine = [c for c in self.conditions if request.request_id in c.request_ids]
            orders = [c for c in mine if c.dimension is SemanticDimension.ORDER]
            cardinalities = [c for c in mine if c.dimension is SemanticDimension.CARDINALITY]
            if len(orders) > 1:
                raise ContractError("MULTIPLE_ORDER")
            if len(cardinalities) > 1:
                raise ContractError("MULTIPLE_CARDINALITY")
            expression = request.condition_expression
            if orders and expression is not None and expression.has_nonconjunctive_atom(orders[0].condition_id):
                raise ContractError("NONCONJUNCTIVE_ORDER")
            if cardinalities and not orders and self.operation_of(request.request_id) is not Operation.RANK_SUPPLIERS:
                raise ContractError("CARDINALITY_WITHOUT_ORDER")

    def _validate_relations(self, request_ids: set[str]) -> None:
        groups: dict[str, set[str]] = {}
        for request in self.requests:
            groups.setdefault(request.scope_id, set()).add(request.request_id)
        expected = {frozenset(group) for group in groups.values() if len(group) > 1}
        shared: list[frozenset[str]] = []
        dependencies: dict[str, set[str]] = {request_id: set() for request_id in request_ids}
        for relation in self.relations:
            if not set(relation.members) <= request_ids:
                raise ContractError("FOREIGN_RELATION_MEMBER")
            if relation.kind is RelationKind.SHARES_SCOPE:
                shared.append(frozenset(relation.members))
            else:
                consumer, producer = relation.members
                dependencies[consumer].add(producer)
        if len(shared) != len(set(shared)) or set(shared) != expected:
            raise ContractError("INCOMPLETE_SHARED_SCOPE")
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(request_id: str) -> None:
            if request_id in visiting:
                raise ContractError("CYCLIC_RESULT_DEPENDENCY")
            if request_id in visited:
                return
            visiting.add(request_id)
            for parent in dependencies[request_id]:
                visit(parent)
            visiting.remove(request_id)
            visited.add(request_id)

        for request_id in request_ids:
            visit(request_id)

    def validate_raw(self, raw: str) -> None:
        if (type(raw) is not str or hashlib.sha256(
            raw.encode("utf-8", errors="surrogatepass")
        ).hexdigest() != self.raw_sha256):
            raise ContractError("RAW_HASH_MISMATCH")
        for record in (*self.requests, *self.conditions):
            if any(not span.binds(raw) for span in record.evidence):
                raise ContractError("UNBOUND_RAW_SPAN")
        for request in self.requests:
            if any(
                evidence is not None and not evidence.binds(raw)
                for evidence in (request.source_evidence, request.universe_evidence)
            ):
                raise ContractError("UNBOUND_RAW_SPAN")
        for job in self.jobs:
            if any(not span.binds(raw) for span in (*job.evidence, *(
                    (job.referent_evidence,) if job.referent_evidence is not None else ()))):
                raise ContractError("UNBOUND_RAW_SPAN")

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class HostResolution:
    target_id: str
    decision: ResolutionDecision
    authority: str
    resolved_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.target_id, "RESOLUTION_TARGET")
        _identifier(self.authority, "RESOLUTION_AUTHORITY")
        if type(self.decision) is not ResolutionDecision:
            raise ContractError("INVALID_RESOLUTION_DECISION")
        if type(self.resolved_ids) is not tuple:
            raise ContractError("INVALID_RESOLVED_IDS")
        if self.resolved_ids:
            _ids(self.resolved_ids, "RESOLVED_IDS")
        if self.decision is not ResolutionDecision.RESOLVED and self.resolved_ids:
            raise ContractError("UNADMITTED_RESOLVED_IDS")


@dataclass(frozen=True, slots=True)
class HostPolicy:
    policy_id: str
    version: str
    condition_ids: tuple[str, ...]
    effect: Effect

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "POLICY_ID")
        _identifier(self.version, "POLICY_VERSION")
        _ids(self.condition_ids, "POLICY_CONDITION_IDS")
        if type(self.effect) is not Effect:
            raise ContractError("INVALID_POLICY_EFFECT")


@dataclass(frozen=True, slots=True)
class InterpretationAcceptance:
    """Host admission of meaning; READY alone never authorizes a source call."""

    proposal: InterpretationProposal | None
    status: AcceptanceStatus
    resolutions: tuple[HostResolution, ...] = ()
    policies: tuple[HostPolicy, ...] = ()
    reason: str = ""

    def __post_init__(self) -> None:
        if type(self.status) is not AcceptanceStatus:
            raise ContractError("INVALID_ACCEPTANCE")
        _records(self.resolutions, HostResolution, "RESOLUTIONS")
        _records(self.policies, HostPolicy, "POLICIES")
        if self.proposal is None:
            if (self.status not in (AcceptanceStatus.PROVIDER_ERROR,
                                    AcceptanceStatus.INVALID_PROPOSAL,
                                    AcceptanceStatus.NO_ACTION)
                    or type(self.reason) is not str or not self.reason.strip()
                    or self.resolutions or self.policies):
                raise ContractError("PROPOSAL_REQUIRED")
            return
        if type(self.proposal) is not InterpretationProposal:
            raise ContractError("INVALID_ACCEPTANCE")
        if self.status is AcceptanceStatus.PROVIDER_ERROR:
            raise ContractError("PROVIDER_ERROR_HAS_PROPOSAL")
        targets = {item.request_id for item in self.proposal.requests} | {
            item.condition_id for item in self.proposal.conditions
        }
        if len(targets) != len(self.proposal.requests) + len(self.proposal.conditions):
            raise ContractError("AMBIGUOUS_TARGET_ID")
        if any(item.target_id not in targets for item in self.resolutions):
            raise ContractError("FOREIGN_RESOLUTION_TARGET")
        if len({item.target_id for item in self.resolutions}) != len(self.resolutions):
            raise ContractError("DUPLICATE_RESOLUTION_TARGET")
        condition_ids = {item.condition_id for item in self.proposal.conditions}
        if any(not set(item.condition_ids) <= condition_ids for item in self.policies):
            raise ContractError("FOREIGN_POLICY_CONDITION")
        if self.status is AcceptanceStatus.READY and any(
            item.decision is not ResolutionDecision.RESOLVED for item in self.resolutions
        ):
            raise ContractError("UNRESOLVED_READY_ACCEPTANCE")
        if self.status is AcceptanceStatus.READY and any(
            item.dimension is SemanticDimension.UNSUPPORTED for item in self.proposal.conditions
        ):
            raise ContractError("UNSUPPORTED_DIMENSION_READY")
        if self.status is AcceptanceStatus.READY:
            policy_targets = {condition_id for policy in self.policies
                              for condition_id in policy.condition_ids}
            resolved_targets = {resolution.target_id for resolution in self.resolutions
                                if resolution.decision is ResolutionDecision.RESOLVED
                                and resolution.resolved_ids}
            for condition in self.proposal.conditions:
                if condition.role is ConditionRole.REQUESTED_UNIVERSE:
                    if condition.condition_id not in policy_targets:
                        raise ContractError("REQUEST_UNIVERSE_POLICY_REQUIRED")
                    if condition.request_ids[0] not in resolved_targets:
                        raise ContractError("REQUEST_UNIVERSE_RESOLUTION_REQUIRED")

    def validate_raw(self, raw: str) -> None:
        if self.proposal is None:
            raise ContractError("NO_PROPOSAL")
        self.proposal.validate_raw(raw)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ConditionDisposition:
    condition_id: str
    request_ids: tuple[str, ...]
    disposition: Disposition
    effect: Effect
    operator: str
    polarity: Polarity
    unknown_treatment: str
    clause_or_policy: str
    role: ConditionRole = ConditionRole.FILTER

    def __post_init__(self) -> None:
        _identifier(self.condition_id, "DISPOSITION_CONDITION")
        _ids(self.request_ids, "DISPOSITION_REQUEST_IDS")
        if type(self.disposition) is not Disposition or type(self.effect) is not Effect:
            raise ContractError("INVALID_DISPOSITION")
        if type(self.polarity) is not Polarity:
            raise ContractError("INVALID_DISPOSITION_POLARITY")
        if type(self.role) is not ConditionRole:
            raise ContractError("INVALID_DISPOSITION_ROLE")
        for name in ("operator", "unknown_treatment", "clause_or_policy"):
            _identifier(getattr(self, name), name.upper())


@dataclass(frozen=True, slots=True)
class InterpretationProjection:
    acceptance: InterpretationAcceptance
    request_ids: tuple[str, ...]
    dataset: str | None
    query: str | None
    dispositions: tuple[ConditionDisposition, ...]
    status: ProjectionStatus = ProjectionStatus.READY
    reason: str = ""
    operation: str = "query"

    def __post_init__(self) -> None:
        if type(self.acceptance) is not InterpretationAcceptance:
            raise ContractError("INVALID_PROJECTION_ACCEPTANCE")
        if self.acceptance.status is not AcceptanceStatus.READY:
            raise ContractError("PROJECTION_REQUIRES_READY")
        if self.acceptance.proposal is None:
            raise ContractError("PROJECTION_REQUIRES_PROPOSAL")
        if type(self.operation) is not str or self.operation not in {"query", "count", "aggregate"}:
            raise ContractError("INVALID_PROJECTION_OPERATION")
        _ids(self.request_ids, "PROJECTION_REQUEST_IDS")
        proposal_request_ids = {item.request_id for item in self.acceptance.proposal.requests}
        if not set(self.request_ids) <= proposal_request_ids:
            raise ContractError("FOREIGN_PROJECTION_REQUEST")
        if type(self.status) is not ProjectionStatus:
            raise ContractError("INVALID_PROJECTION_STATUS")
        if self.status is ProjectionStatus.READY:
            _identifier(self.dataset, "DATASET")
            _identifier(self.query, "QUERY")
        elif self.query is not None or type(self.reason) is not str or not self.reason.strip():
            raise ContractError("BLOCKED_PROJECTION_REQUIRES_REASON_AND_NO_QUERY")
        if self.dataset is not None:
            _identifier(self.dataset, "DATASET")
        _records(self.dispositions, ConditionDisposition, "DISPOSITIONS")
        if self.status is ProjectionStatus.READY and any(
            item.disposition is Disposition.BLOCKED for item in self.dispositions
        ):
            raise ContractError("READY_PROJECTION_HAS_BLOCKED_CONDITION")
        conditions = {item.condition_id: item for item in self.acceptance.proposal.conditions}
        expected = {
            (condition.condition_id, request_id)
            for condition in conditions.values()
            for request_id in condition.request_ids if request_id in self.request_ids
        }
        actual = [
            (item.condition_id, request_id)
            for item in self.dispositions for request_id in item.request_ids
        ]
        if set(actual) != expected or len(actual) != len(expected):
            raise ContractError("INCOMPLETE_PROJECTION_DISPOSITIONS")
        for item in self.dispositions:
            condition = conditions[item.condition_id]
            if (not set(item.request_ids) <= set(condition.request_ids)
                    or item.operator != condition.operator
                    or item.polarity is not condition.polarity
                    or item.role is not condition.role):
                raise ContractError("DISPOSITION_CHANGED_CLAIM")
            if item.role is ConditionRole.REQUESTED_UNIVERSE:
                policies = [policy for policy in self.acceptance.policies
                            if item.condition_id in policy.condition_ids]
                if not any(item.clause_or_policy == f"{policy.policy_id}@{policy.version}"
                           and item.effect is policy.effect for policy in policies):
                    raise ContractError("REQUEST_UNIVERSE_POLICY_MISMATCH")
                resolutions = [resolution for resolution in self.acceptance.resolutions
                               if resolution.target_id in item.request_ids]
                if not any(self.dataset in resolution.resolved_ids for resolution in resolutions):
                    raise ContractError("REQUEST_UNIVERSE_DATASET_MISMATCH")

    def to_dict(self) -> dict:
        return asdict(self)
