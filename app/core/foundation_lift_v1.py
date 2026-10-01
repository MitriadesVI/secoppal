"""Lift of the v4.2 wire (IR v1) into the IR v2, without losing anything (INV-C0-2, C0 §5.2).

IR v1 carried the operation on each request and a free-text subject; IR v2 carries the operation on a job and
a closed ``Subject``. The lift is pure and only restates what v1 already said:

* a request becomes a member of the job of its own operation (one job per operation, so two SEARCH requests
  that share a scope are one SEARCH job); ``COUNT`` and ``SUM_VALUE`` keep their operation with
  ``aspect=TOTAL`` and the request's own evidence as the signal of the operation;
* ``subject`` goes into the enum, and a GEOGRAPHY condition gets ``level=DEPARTMENT``, which is exactly what
  the host already assumed for every geography claim (``foundation_admission``);
* nothing else changes: spans, operators, polarity, relations and omissions pass through untouched.

The lift never decides anything the producer did not say. A v1 request whose operation or subject is outside
the closed v4.2 vocabulary (``SEARCH|COUNT|SUM_VALUE`` and ``processes|contracts``) cannot be lifted: it is a
``ContractError``, not a guess.

``lift_v1`` works on the stored record (``to_dict`` shape); the wire decoder reuses ``lift_operation``,
``lift_subject`` and ``v1_job_specs`` so both paths share one rule.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_operations import Aspect, Operation, Subject
from app.core.interpretation_contract import SCHEMA_VERSION, ContractError
from app.core.semantic_dimensions import SemanticDimension

V1_SCHEMA_VERSION = 1
V1_OPERATIONS = {"SEARCH": Operation.SEARCH, "COUNT": Operation.COUNT, "SUM_VALUE": Operation.SUM_VALUE}
V1_SUBJECTS = {"processes": Subject.PROCESSES, "contracts": Subject.CONTRACTS}


def _word(value, code: str) -> str:
    if type(value) is not str or not value.strip():
        raise ContractError(code)
    return value


def lift_operation(value) -> Operation:
    """The v2 operation of a v1 request; outside the v4.2 vocabulary is not liftable."""
    found = V1_OPERATIONS.get(_word(value, "INVALID_OPERATION"))
    if found is None:
        raise ContractError("UNSUPPORTED_V1_OPERATION")
    return found


def lift_subject(value) -> Subject:
    """The v2 subject of a v1 request; outside the v4.2 vocabulary is not liftable."""
    found = V1_SUBJECTS.get(_word(value, "INVALID_SUBJECT"))
    if found is None:
        raise ContractError("UNSUPPORTED_V1_SUBJECT")
    return found


def lift_level(dimension: SemanticDimension) -> GeoLevel | None:
    """v1 never said the level of a geography claim; the host has always read it as a department."""
    return GeoLevel.DEPARTMENT if dimension is SemanticDimension.GEOGRAPHY else None


def v1_job_specs(entries: Sequence[tuple[str, Operation, Sequence]]) -> list[dict]:
    """One job spec per operation, in order of first appearance; ``entries`` are (request_id, operation, evidence).

    Evidence is opaque here (spans or their stored dicts) so the decoder and the stored-record lift share the rule.
    SEARCH has no signal of its own in v1; COUNT/SUM_VALUE are grounded on their request's evidence.
    """
    specs: dict[Operation, dict] = {}
    for request_id, operation, evidence in entries:
        spec = specs.setdefault(operation, {
            "job_id": f"job-{operation.value.lower()}", "operation": operation, "request_ids": [],
            "evidence": [], "aspect": None if operation is Operation.SEARCH else Aspect.TOTAL,
            "aspect_label": None, "referent_evidence": None,
        })
        spec["request_ids"].append(request_id)
        if operation is not Operation.SEARCH:
            spec["evidence"].extend(span for span in evidence if span not in spec["evidence"])
    return [{**spec, "request_ids": tuple(spec["request_ids"]), "evidence": tuple(spec["evidence"])}
            for spec in specs.values()]


def _items(value, code: str) -> list:
    if not isinstance(value, (list, tuple)):
        raise ContractError(code)
    return list(value)


def lift_v1(record: Mapping) -> dict:
    """A stored v1 proposal record (``to_dict`` shape) as the v2 record that says the same thing."""
    if not isinstance(record, Mapping):
        raise ContractError("INVALID_STORED_RECORD")
    version = record.get("schema_version")
    if type(version) is not int or version != V1_SCHEMA_VERSION:
        raise ContractError("UNSUPPORTED_SCHEMA_VERSION")
    requests, conditions = [], []
    for request in _items(record.get("requests"), "INVALID_STORED_RECORD"):
        if not isinstance(request, Mapping):
            raise ContractError("INVALID_STORED_RECORD")
        lifted = {key: value for key, value in request.items() if key != "operation"}
        lifted["subject"] = lift_subject(request.get("subject")).value
        requests.append(lifted)
    for condition in _items(record.get("conditions"), "INVALID_STORED_RECORD"):
        if not isinstance(condition, Mapping):
            raise ContractError("INVALID_STORED_RECORD")
        try:
            dimension = SemanticDimension(condition.get("dimension"))
        except ValueError as exc:
            raise ContractError("INVALID_DIMENSION") from exc
        level = lift_level(dimension)
        conditions.append({**condition, "level": level.value if level is not None else None})
    jobs = v1_job_specs([
        (request.get("request_id"), lift_operation(source.get("operation")), source.get("evidence") or ())
        for request, source in zip(requests, record["requests"], strict=True)
    ])
    return {**record, "requests": requests, "conditions": conditions, "jobs": jobs, "schema_version": SCHEMA_VERSION}
