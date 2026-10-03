"""Wire 5 decoder: explicit jobs and evidence, never v1 lifting or version sniffing."""
from __future__ import annotations

import hashlib

from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_operations import Aspect, Operation, Subject
from app.core.foundation_universe_kind import expand_universe_kind
from app.core.interpretation_contract import (
    ConditionExpression, ConditionRole, ContractError, InterpretationProposal, LogicOperator,
    Omission, Polarity, ProposedCondition, ProposedJob, ProposedOmission, ProposedRelation,
    ProposedRequest, RawSpan, RelationKind,
)
from app.core.semantic_dimensions import SemanticDimension


def _keys(record, allowed, required=()):
    if type(record) is not dict or set(record) - set(allowed) or not set(required) <= set(record):
        raise ContractError("INVALID_LINGUISTIC_RECORD_KEYS")


def _enum(kind, value, label):
    if type(value) is not str:
        raise ContractError(f"INVALID_{label}")
    try:
        return kind(value)
    except ValueError as exc:
        raise ContractError(f"INVALID_{label}") from exc


def decode_proposal_v2(payload: dict, raw: str, *, arm: str, candidates=()) -> InterpretationProposal:
    if arm not in {"B", "C"}:
        raise ContractError("INVALID_WIRE_ARM")
    _keys(payload, {"jobs", "requests", "conditions", "relations", "omissions"},
          {"jobs", "requests", "conditions"})
    for name in ("jobs", "requests", "conditions", "relations", "omissions"):
        if type(payload.get(name, [])) is not list:
            raise ContractError("INVALID_LINGUISTIC_RECORD_LIST")
    payload = expand_universe_kind(payload, arm, version="5")
    inventory = {candidate["candidate_id"]: candidate for candidate in candidates}

    def spans(record, field="evidence", *, optional=False):
        key = ("candidate_ids" if field == "evidence" else field + "_candidate_ids") if arm == "B" else field
        if optional and record.get(key) is None:
            return ()
        values = record.get(key)
        if arm == "B":
            if type(values) is not list or not values or any(identifier not in inventory for identifier in values):
                raise ContractError("FOREIGN_OR_MISSING_HOST_CANDIDATE")
            values = [inventory[identifier] for identifier in values]
        elif field != "evidence":
            values = [values]
        if type(values) is not list or not values:
            raise ContractError("MISSING_RAW_EVIDENCE")
        result = []
        for value in values:
            _keys(value, {"start", "end", "quote", "candidate_id"}, {"start", "end", "quote"})
            result.append(RawSpan(value["start"], value["end"], value["quote"]))
        if field != "evidence" and len(result) != 1:
            raise ContractError("SINGLE_EVIDENCE_REQUIRED")
        return tuple(result)

    def expression(value):
        if value is None:
            return None
        _keys(value, {"operator", "condition_id", "children"}, {"operator"})
        children = value.get("children", [])
        if type(children) is not list:
            raise ContractError("INVALID_EXPRESSION_CHILDREN")
        return ConditionExpression(_enum(LogicOperator, value["operator"], "LOGIC_OPERATOR"), value.get("condition_id"),
                                   tuple(expression(child) for child in children))

    evidence_keys = ({"candidate_ids", "universe_evidence_candidate_ids", "source_evidence_candidate_ids"}
                     if arm == "B" else {"evidence", "universe_evidence", "source_evidence"})
    requests = []
    for request in payload["requests"]:
        _keys(request, {"request_id", "scope_id", "subject", "condition_expression"} | evidence_keys,
              {"request_id", "scope_id", "subject"})
        subject = _enum(Subject, request["subject"], "SUBJECT")
        universe = spans(request, "universe_evidence", optional=subject is Subject.UNSPECIFIED)
        source = spans(request, "source_evidence", optional=True)
        requests.append(ProposedRequest(
            request["request_id"], request["scope_id"], subject, spans(request),
            source[0].quote if source else None, source[0] if source else None,
            universe[0].quote if universe else None, universe[0] if universe else None,
            expression(request.get("condition_expression")),
        ))
    jobs = []
    job_evidence_keys = ({"candidate_ids", "referent_evidence_candidate_ids"} if arm == "B"
                         else {"evidence", "referent_evidence"})
    for job in payload["jobs"]:
        _keys(job, {"job_id", "operation", "request_ids", "aspect", "aspect_label"} | job_evidence_keys,
              {"job_id", "operation", "request_ids"})
        operation = _enum(Operation, job["operation"], "OPERATION")
        referent = spans(job, "referent_evidence", optional=True)
        evidence = spans(job, optional=True) if operation is Operation.SEARCH else spans(job)
        if type(job["request_ids"]) is not list:
            raise ContractError("INVALID_JOB_REQUEST_IDS")
        jobs.append(ProposedJob(job["job_id"], operation, tuple(job["request_ids"]), evidence,
                                _enum(Aspect, job["aspect"], "ASPECT") if job.get("aspect") is not None else None,
                                job.get("aspect_label"), referent[0] if referent else None))
    conditions = []
    for condition in payload["conditions"]:
        _keys(condition, {"condition_id", "request_ids", "dimension", "operator", "polarity", "role",
                          "unsupported_label", "level", "candidate_ids" if arm == "B" else "evidence"},
              {"condition_id", "request_ids", "dimension", "operator", "polarity"})
        if type(condition["request_ids"]) is not list:
            raise ContractError("INVALID_CONDITION_REQUEST_IDS")
        evidence = spans(condition)
        conditions.append(ProposedCondition(
            condition["condition_id"], tuple(condition["request_ids"]), _enum(SemanticDimension, condition["dimension"], "DIMENSION"),
            condition["operator"], _enum(Polarity, condition["polarity"], "POLARITY"), evidence[0].quote, evidence,
            condition.get("unsupported_label"), _enum(ConditionRole, condition.get("role", "FILTER"), "CONDITION_ROLE"),
            _enum(GeoLevel, condition["level"], "GEO_LEVEL") if condition.get("level") is not None else None,
        ))
    relations, omissions = [], []
    for relation in payload.get("relations", []):
        _keys(relation, {"kind", "members"}, {"kind", "members"})
        if type(relation["members"]) is not list:
            raise ContractError("INVALID_RELATION_MEMBERS")
        relations.append(ProposedRelation(_enum(RelationKind, relation["kind"], "RELATION_KIND"), tuple(relation["members"])))
    for omission in payload.get("omissions", []):
        _keys(omission, {"request_id", "dimension", "state"}, {"request_id", "dimension", "state"})
        omissions.append(ProposedOmission(omission["request_id"], _enum(SemanticDimension, omission["dimension"], "DIMENSION"),
                                         _enum(Omission, omission["state"], "OMISSION")))
    proposal = InterpretationProposal(hashlib.sha256(raw.encode("utf-8", errors="surrogatepass")).hexdigest(),
                                      f"foundation-{arm}", "5", tuple(requests), tuple(conditions),
                                      tuple(relations), tuple(omissions), tuple(jobs))
    proposal.validate_raw(raw)
    return proposal
