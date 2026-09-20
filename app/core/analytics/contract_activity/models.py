"""Immutable contracts for the narrow Contract Activity capability."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


CONTRACTS_DATASET = "jbjy-vk9h"
CONTRACT_ACTIVITY_INTENT = "contract_activity"


class AdmissionStatus(str, Enum):
    HANDOFF = "HANDOFF"
    CLARIFY = "CLARIFY"
    READY = "READY"


class DecisionKind(str, Enum):
    HANDOFF = "HANDOFF"
    CLARIFY = "CLARIFY"
    ANSWER = "ANSWER"


class Operation(str, Enum):
    TOTAL = "TOTAL"
    BY_MODALITY = "BY_MODALITY"
    BY_REPORTED_STATE = "BY_REPORTED_STATE"


@dataclass(frozen=True, slots=True)
class MoneyConstraint:
    operator: str
    amount_cop: int

    def __post_init__(self) -> None:
        if self.operator not in {">", ">=", "<", "<="}:
            raise ValueError("UNSUPPORTED_MONEY_OPERATOR")
        if type(self.amount_cop) is not int or self.amount_cop <= 0:  # noqa: E721
            raise ValueError("INVALID_MONEY_AMOUNT")


@dataclass(frozen=True, slots=True)
class ContractActivityRequest:
    raw: str
    operation: Operation
    buyer_units: tuple[str, ...]
    buyer_display: str
    source_buyer_names: tuple[str, ...]
    date_from: str | None = None
    date_to: str | None = None
    modality_values: tuple[str, ...] = ()
    reported_states: tuple[str, ...] = ()
    money: MoneyConstraint | None = None

    def __post_init__(self) -> None:
        if not self.raw.strip() or not self.buyer_units or not self.source_buyer_names:
            raise ValueError("INCOMPLETE_CONTRACT_ACTIVITY_REQUEST")
        if (self.date_from is None) != (self.date_to is None):
            raise ValueError("PARTIAL_SIGNATURE_PERIOD")


@dataclass(frozen=True, slots=True)
class ContractActivityPlan:
    request: ContractActivityRequest
    identity_where: str
    identity_fingerprint: str
    where: str
    fingerprint: str
    identity_sql: str
    scalar_sql: str
    groups_sql: str | None = None

    @property
    def call_count(self) -> int:
        return 3 if self.groups_sql is not None else 2


@dataclass(frozen=True, slots=True)
class ContractActivityGroup:
    label: str
    contract_count: int
    share: float


@dataclass(frozen=True, slots=True)
class ContractActivityResult:
    operation: Operation
    total: int
    groups: tuple[ContractActivityGroup, ...] = ()
    unit: str = "DISTINCT_CONTRACT_ID"
    source: str = CONTRACTS_DATASET
    temporal_basis: str = "CONTRACT_SIGNATURE_DATE"


@dataclass(frozen=True, slots=True)
class Admission:
    status: AdmissionStatus
    request: ContractActivityRequest | None = None
    reason: str = ""


@dataclass(frozen=True, slots=True)
class ContractActivityDecision:
    kind: DecisionKind
    reason: str = ""
    request: ContractActivityRequest | None = None
    plan: ContractActivityPlan | None = None
    result: ContractActivityResult | None = None
