"""T1B-3 contractual lifecycle aggregation.

This module chooses no global state. It only preserves the four independent
contract assessments, their shared evidence, quality limits, conflicts and
source errors.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.core.truth_layer.contract_changes import ContractChangeAssessment
from app.core.truth_layer.contract_liquidation import (
    ContractLiquidationAssessment,
)
from app.core.truth_layer.contract_performance import (
    ContractPerformanceAssessment,
)
from app.core.truth_layer.contract_signature import ContractSignatureAssessment
from app.core.truth_layer.evidence import (
    Evidence,
    ResolutionQuality,
    build_resolution_quality,
    validate_evidence_set,
)

__all__ = [
    "ContractLifecycleAssessment",
    "compose_contract_lifecycle",
]


# Every child assessment validates its exact evidence set. This order is the
# known canonical order for the finite T1B-3 evidence union: field first, then
# code. No private helper from evidence.py is imported or duplicated.
_EVIDENCE_ORDER = (
    ("estado_contrato", "CONTRACT_CHANGE_STATE_EVIDENCE"),
    ("estado_contrato", "CONTRACT_LIQUIDATION_STATE_EVIDENCE"),
    ("estado_contrato", "CONTRACT_PERFORMANCE_STATE_EVIDENCE"),
    ("estado_contrato", "CONTRACT_SIGNATURE_STATE_EVIDENCE"),
    ("fecha_de_firma", "CONTRACT_SIGNATURE_DATE_EVIDENCE"),
    ("fecha_fin_liquidacion", "LIQUIDATION_END_DATE_EVIDENCE"),
    ("fecha_inicio_liquidacion", "LIQUIDATION_START_DATE_EVIDENCE"),
    ("liquidaci_n", "LIQUIDATION_FLAG_EVIDENCE"),
)


def _aggregate_evidence(
    performance: ContractPerformanceAssessment,
    signature: ContractSignatureAssessment,
    changes: ContractChangeAssessment,
    liquidation: ContractLiquidationAssessment,
) -> tuple[Evidence, ...]:
    found = {
        (item.field, item.code): item
        for assessment in (performance, signature, changes, liquidation)
        for item in assessment.evidence
    }
    if set(found) != set(_EVIDENCE_ORDER):
        raise ValueError("child assessments expose an unexpected evidence schema")
    return tuple(found[key] for key in _EVIDENCE_ORDER)


def _aggregate_quality(
    performance: ContractPerformanceAssessment,
    signature: ContractSignatureAssessment,
    changes: ContractChangeAssessment,
    liquidation: ContractLiquidationAssessment,
) -> ResolutionQuality:
    children = (performance, signature, changes, liquidation)
    completeness = (
        "insufficient"
        if any(
            child.quality.completeness == "insufficient"
            for child in children
        )
        else (
            "partial"
            if any(child.quality.completeness == "partial" for child in children)
            else "complete"
        )
    )
    consistency = (
        "conflicting"
        if any(
            child.quality.consistency == "conflicting"
            or child.quality.conflicts
            for child in children
        )
        else (
            "unknown"
            if any(child.quality.consistency == "unknown" for child in children)
            else "consistent"
        )
    )
    confidence = (
        "low"
        if any(child.quality.confidence == "low" for child in children)
        else (
            "medium"
            if any(child.quality.confidence == "medium" for child in children)
            else "high"
        )
    )
    return build_resolution_quality(
        completeness=completeness,
        consistency=consistency,
        confidence=confidence,
        reason_codes=(
            "CONTRACT_LIFECYCLE_AXES_PRESERVED",
            *(
                code
                for child in children
                for code in child.quality.reason_codes
            ),
        ),
        missing_fields=tuple(
            field
            for child in children
            for field in child.quality.missing_fields
        ),
        source_errors=tuple(
            error
            for child in children
            for error in child.quality.source_errors
        ),
        conflicts=tuple(
            conflict
            for child in children
            for conflict in child.quality.conflicts
        ),
        capabilities=tuple(
            capability
            for child in children
            for capability in child.quality.capabilities
        ),
    )


@dataclass(frozen=True, slots=True)
class ContractLifecycleAssessment:
    performance: ContractPerformanceAssessment
    signature: ContractSignatureAssessment
    changes: ContractChangeAssessment
    liquidation: ContractLiquidationAssessment
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality

    def __post_init__(self) -> None:
        expected_types = (
            (self.performance, ContractPerformanceAssessment),
            (self.signature, ContractSignatureAssessment),
            (self.changes, ContractChangeAssessment),
            (self.liquidation, ContractLiquidationAssessment),
        )
        for assessment, expected_type in expected_types:
            if not isinstance(assessment, expected_type):
                raise TypeError(
                    f"lifecycle axis must be {expected_type.__name__}"
                )
        signals = self.performance.signals
        if not all(
            assessment.signals is signals
            for assessment in (
                self.signature,
                self.changes,
                self.liquidation,
            )
        ):
            raise ValueError(
                "all contract assessments must come from the same "
                "ContractLifecycleSignals object"
            )
        validate_evidence_set(self.evidence)
        if not isinstance(self.quality, ResolutionQuality):
            raise TypeError("quality must be ResolutionQuality")
        evidence = _aggregate_evidence(
            self.performance,
            self.signature,
            self.changes,
            self.liquidation,
        )
        if self.evidence != evidence:
            raise ValueError("lifecycle evidence must preserve every child axis")
        quality = _aggregate_quality(
            self.performance,
            self.signature,
            self.changes,
            self.liquidation,
        )
        if self.quality != quality:
            raise ValueError(
                "lifecycle quality must preserve child conflicts and source errors"
            )


def compose_contract_lifecycle(
    performance: ContractPerformanceAssessment,
    signature: ContractSignatureAssessment,
    changes: ContractChangeAssessment,
    liquidation: ContractLiquidationAssessment,
) -> ContractLifecycleAssessment:
    """Compose four already-resolved axes without reading raw fields."""
    evidence = _aggregate_evidence(
        performance, signature, changes, liquidation
    )
    quality = _aggregate_quality(
        performance, signature, changes, liquidation
    )
    return ContractLifecycleAssessment(
        performance=performance,
        signature=signature,
        changes=changes,
        liquidation=liquidation,
        evidence=evidence,
        quality=quality,
    )
