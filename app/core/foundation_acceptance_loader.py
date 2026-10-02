"""Read a stored acceptance back into the current records (C0 §5.2, «Registros guardados»).

Traces and replays keep ``InterpretationAcceptance.to_dict()``. This is the only door through which such a
record becomes an object again: it accepts ``schema_version`` 1 (lifted by ``foundation_lift_v1``) or 2, and
anything else is ``UNSUPPORTED_SCHEMA_VERSION``. The reader is strict: an unknown or missing key, a foreign
enum value or a malformed record is a ``ContractError``, and every record is rebuilt through its own
constructor, so a stored record is held to the same rules as a fresh one. The version is read from the stored
proposal; it is never guessed from the shape of the payload.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum

from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_lift_v1 import lift_v1
from app.core.foundation_operations import Aspect, Operation, Subject
from app.core.interpretation_contract import (
    SCHEMA_VERSION, AcceptanceStatus, ConditionExpression, ConditionRole, ContractError, Effect,
    HostPolicy, HostResolution, InterpretationAcceptance, InterpretationProposal, LogicOperator, Omission,
    Polarity, ProposedCondition, ProposedJob, ProposedOmission, ProposedRelation, ProposedRequest,
    RawSpan, RelationKind, ResolutionDecision,
)
from app.core.semantic_dimensions import SemanticDimension

_BAD = "INVALID_STORED_RECORD"


def _record(value, keys: set[str], required: set[str] = frozenset()) -> Mapping:
    if not isinstance(value, Mapping) or set(value) - keys or not required <= set(value):
        raise ContractError(_BAD)
    return value


def _enum(kind: type[Enum], value):
    try:
        return kind(value)
    except ValueError as exc:
        raise ContractError(_BAD) from exc


def _seq(value) -> tuple:
    if not isinstance(value, (list, tuple)):
        raise ContractError(_BAD)
    return tuple(value)


def _span(value) -> RawSpan:
    value = _record(value, {"start", "end", "quote"}, {"start", "end", "quote"})
    return RawSpan(value["start"], value["end"], value["quote"])


def _optional_span(value) -> RawSpan | None:
    return None if value is None else _span(value)


def _expression(value) -> ConditionExpression | None:
    if value is None:
        return None
    value = _record(value, {"operator", "condition_id", "children"}, {"operator"})
    return ConditionExpression(_enum(LogicOperator, value["operator"]), value.get("condition_id"),
                               tuple(_expression(child) for child in _seq(value.get("children", ()))))


def _request(value) -> ProposedRequest:
    keys = {"request_id", "scope_id", "subject", "evidence", "source_claim", "source_evidence",
            "universe_claim", "universe_evidence", "condition_expression"}
    value = _record(value, keys, {"request_id", "scope_id", "subject", "evidence"})
    return ProposedRequest(
        value["request_id"], value["scope_id"], _enum(Subject, value["subject"]),
        tuple(_span(span) for span in _seq(value["evidence"])),
        value.get("source_claim"), _optional_span(value.get("source_evidence")),
        value.get("universe_claim"), _optional_span(value.get("universe_evidence")),
        _expression(value.get("condition_expression")))


def _condition(value) -> ProposedCondition:
    keys = {"condition_id", "request_ids", "dimension", "operator", "polarity", "claim", "evidence",
            "unsupported_label", "role", "level"}
    value = _record(value, keys, keys - {"unsupported_label", "role", "level"})
    level = value.get("level")
    return ProposedCondition(
        value["condition_id"], tuple(_seq(value["request_ids"])), _enum(SemanticDimension, value["dimension"]),
        value["operator"], _enum(Polarity, value["polarity"]), value["claim"],
        tuple(_span(span) for span in _seq(value["evidence"])), value.get("unsupported_label"),
        _enum(ConditionRole, value.get("role", ConditionRole.FILTER)),
        None if level is None else _enum(GeoLevel, level))


def _job(value) -> ProposedJob:
    keys = {"job_id", "operation", "request_ids", "evidence", "aspect", "aspect_label", "referent_evidence"}
    value = _record(value, keys, {"job_id", "operation"})
    aspect = value.get("aspect")
    return ProposedJob(
        value["job_id"], _enum(Operation, value["operation"]), tuple(_seq(value.get("request_ids", ()))),
        tuple(_span(span) for span in _seq(value.get("evidence", ()))),
        None if aspect is None else _enum(Aspect, aspect), value.get("aspect_label"),
        _optional_span(value.get("referent_evidence")))


def _relation(value) -> ProposedRelation:
    value = _record(value, {"kind", "members"}, {"kind", "members"})
    return ProposedRelation(_enum(RelationKind, value["kind"]), tuple(_seq(value["members"])))


def _omission(value) -> ProposedOmission:
    value = _record(value, {"request_id", "dimension", "state"}, {"request_id", "dimension", "state"})
    return ProposedOmission(value["request_id"], _enum(SemanticDimension, value["dimension"]),
                            _enum(Omission, value["state"]))


def load_proposal(record: Mapping) -> InterpretationProposal:
    """A stored proposal (schema 1 or 2) as the current ``InterpretationProposal``."""
    if not isinstance(record, Mapping):
        raise ContractError(_BAD)
    version = record.get("schema_version")
    if type(version) is not int or version not in (1, SCHEMA_VERSION):
        raise ContractError("UNSUPPORTED_SCHEMA_VERSION")
    if version == 1:
        record = lift_v1(record)
    keys = {"raw_sha256", "producer", "producer_version", "requests", "conditions", "relations", "omissions",
            "jobs", "schema_version"}
    record = _record(record, keys, {"raw_sha256", "producer", "producer_version", "requests", "schema_version"})
    return InterpretationProposal(
        record["raw_sha256"], record["producer"], record["producer_version"],
        tuple(_request(item) for item in _seq(record["requests"])),
        tuple(_condition(item) for item in _seq(record.get("conditions", ()))),
        tuple(_relation(item) for item in _seq(record.get("relations", ()))),
        tuple(_omission(item) for item in _seq(record.get("omissions", ()))),
        tuple(_job(item) for item in _seq(record.get("jobs", ()))), record["schema_version"])


def load_acceptance(record: Mapping) -> InterpretationAcceptance:
    """A stored ``InterpretationAcceptance.to_dict()`` (proposal schema 1 or 2) as a current acceptance."""
    keys = {"proposal", "status", "resolutions", "policies", "reason"}
    record = _record(record, keys, {"proposal", "status"})
    proposal = record["proposal"]
    resolutions = tuple(
        HostResolution(item["target_id"], _enum(ResolutionDecision, item["decision"]), item["authority"],
                       tuple(_seq(item.get("resolved_ids", ()))))
        for item in (_record(raw, {"target_id", "decision", "authority", "resolved_ids"},
                             {"target_id", "decision", "authority"}) for raw in _seq(record.get("resolutions", ()))))
    policies = tuple(
        HostPolicy(item["policy_id"], item["version"], tuple(_seq(item["condition_ids"])),
                   _enum(Effect, item["effect"]))
        for item in (_record(raw, {"policy_id", "version", "condition_ids", "effect"},
                             {"policy_id", "version", "condition_ids", "effect"})
                     for raw in _seq(record.get("policies", ()))))
    return InterpretationAcceptance(
        None if proposal is None else load_proposal(proposal), _enum(AcceptanceStatus, record["status"]),
        resolutions, policies, record.get("reason", ""))
