"""Vocabulary and shape rules of the IR v2 job (C0 §5.1.1, §5.1.4).

``Operation`` is what the user asks to *receive* (a list, a count, a profile ...). One job carries one
operation, and ``MULTI`` is never a value: a proposal with more than one job is multi by derivation.
Only SEARCH executes in M1; the rest of the vocabulary exists so the host can decline it explicitly
instead of reading it as a search.

This module is pure vocabulary. ``ProposedJob`` itself lives in ``interpretation_contract`` next to the
other proposal records (it needs ``RawSpan`` and ``ContractError``); ``job_shape_error`` is the one place
that says which operation/aspect combinations exist, and returns the closed ``ContractError`` code.
"""

from __future__ import annotations

from enum import Enum


class Operation(str, Enum):
    SEARCH = "SEARCH"
    COUNT = "COUNT"
    SUM_VALUE = "SUM_VALUE"
    GROUP_BY = "GROUP_BY"
    RANK_SUPPLIERS = "RANK_SUPPLIERS"
    PROFILE_BUYER = "PROFILE_BUYER"
    PROFILE_ACTOR = "PROFILE_ACTOR"
    FOLLOW = "FOLLOW"
    EXPORT = "EXPORT"
    FOLLOWUP_DELTA = "FOLLOWUP_DELTA"
    OUT_OF_DOMAIN = "OUT_OF_DOMAIN"
    PROTOCOL = "PROTOCOL"


class Aspect(str, Enum):
    TOTAL = "TOTAL"
    BY_MODALITY = "BY_MODALITY"
    BY_YEAR = "BY_YEAR"
    BY_BUYER = "BY_BUYER"
    BY_SUPPLIER = "BY_SUPPLIER"
    BY_STATE = "BY_STATE"
    BY_TYPE = "BY_TYPE"
    OTHER = "OTHER"


class Subject(str, Enum):
    """What a SEARCH request retrieves. ``UNSPECIFIED``: the user named neither processes nor contracts."""

    PROCESSES = "processes"
    CONTRACTS = "contracts"
    UNSPECIFIED = "unspecified"


# The operations that may carry an aspect at all.
ASPECT_OPERATIONS = frozenset({Operation.GROUP_BY, Operation.COUNT, Operation.SUM_VALUE})


def job_shape_error(operation: Operation, aspect: Aspect | None, aspect_label: str | None, *,
                    request_count: int, evidence_count: int) -> str | None:
    """The closed ``ContractError`` code a job's shape violates; None when the shape is valid."""
    if operation is Operation.SEARCH and request_count < 1:
        return "SEARCH_JOB_WITHOUT_REQUEST"
    if operation is not Operation.SEARCH and evidence_count < 1:
        return "UNGROUNDED_OPERATION"
    if aspect is not None and operation not in ASPECT_OPERATIONS:
        return "ASPECT_FORBIDDEN"
    if operation is Operation.GROUP_BY and aspect is None:
        return "ASPECT_REQUIRED"
    if operation is Operation.COUNT and aspect not in (None, Aspect.TOTAL):
        return "COUNT_ASPECT_NOT_TOTAL"
    if (aspect is Aspect.OTHER) != (aspect_label is not None):
        return "ASPECT_LABEL_MISMATCH"
    return None
