"""T1B-3 contract lifecycle aggregation and transverse tests."""
from __future__ import annotations

import ast
import dataclasses
import inspect
import os
from pathlib import Path
import subprocess
import sys

import pytest

from app.core.truth_layer import (
    ChangeAxisAssessment,
    ContractChangeAssessment,
    ContractLifecycleAssessment,
    ContractLifecycleSignals,
    ContractLiquidationAssessment,
    ContractPerformanceAssessment,
    ContractSignatureAssessment,
    FieldObservation,
    SourceError,
    compose_contract_lifecycle,
    resolve_contract_changes,
    resolve_contract_liquidation,
    resolve_contract_performance,
    resolve_contract_signature,
)
import app.core.truth_layer.contract_changes as changes_mod
import app.core.truth_layer.contract_lifecycle as lifecycle_mod
import app.core.truth_layer.contract_liquidation as liquidation_mod
import app.core.truth_layer.contract_performance as performance_mod
import app.core.truth_layer.contract_signature as signature_mod


ROOT = Path(__file__).resolve().parents[1]


def _obs(value: object) -> FieldObservation:
    return FieldObservation("observed", value)  # type: ignore[arg-type]


def _missing() -> FieldObservation:
    return FieldObservation("missing", None)


def _not_observed() -> FieldObservation:
    return FieldObservation("not_observed", None)


def _signals(
    *,
    state: object = "En ejecución",
    signature: object = "2025-01-01",
    start: object = "2025-01-02",
    end: object = "2025-12-31",
    flag: object = "Si",
    liquidation_start: object = "2026-01-01",
    liquidation_end: object = "2026-02-01",
    absent: FieldObservation | None = None,
) -> ContractLifecycleSignals:
    blank = absent if absent is not None else _missing()

    def field(value: object) -> FieldObservation:
        if isinstance(value, FieldObservation):
            return value
        return blank if value is None else _obs(value)

    return ContractLifecycleSignals(
        raw_contract_state=field(state),
        raw_signature_date=field(signature),
        raw_start_date=field(start),
        raw_end_date=field(end),
        raw_liquidation_flag=field(flag),
        raw_liquidation_start_date=field(liquidation_start),
        raw_liquidation_end_date=field(liquidation_end),
    )


def _axes(
    signals: ContractLifecycleSignals,
    *,
    source_errors: tuple[SourceError, ...] = (),
) -> tuple[
    ContractPerformanceAssessment,
    ContractSignatureAssessment,
    ContractChangeAssessment,
    ContractLiquidationAssessment,
]:
    return (
        resolve_contract_performance(signals, source_errors=source_errors),
        resolve_contract_signature(signals, source_errors=source_errors),
        resolve_contract_changes(signals, source_errors=source_errors),
        resolve_contract_liquidation(signals, source_errors=source_errors),
    )


def test_aggregator_preserves_four_axes_without_a_global_state() -> None:
    assessment = compose_contract_lifecycle(*_axes(_signals()))
    names = {field.name for field in dataclasses.fields(assessment)}
    assert names == {
        "performance",
        "signature",
        "changes",
        "liquidation",
        "evidence",
        "quality",
    }
    assert not names & {
        "overall_contract_state",
        "canonical_contract_state",
        "best_state",
        "single_lifecycle_status",
    }
    assert assessment.performance.performance == "executing"
    assert assessment.signature.signature_status == "directly_observed"
    assert assessment.liquidation.liquidation_status == "yes"


def test_all_axes_retain_the_exact_same_t1a_signal_object() -> None:
    signals = _signals()
    assessment = compose_contract_lifecycle(*_axes(signals))
    assert assessment.performance.signals is signals
    assert assessment.signature.signals is signals
    assert assessment.changes.signals is signals
    assert assessment.liquidation.signals is signals


def test_dataclasses_replace_cannot_mix_contracts() -> None:
    first = compose_contract_lifecycle(*_axes(_signals(state="Cerrado")))
    second = compose_contract_lifecycle(*_axes(_signals(state="Cerrado")))
    with pytest.raises(ValueError, match="same ContractLifecycleSignals"):
        dataclasses.replace(first, signature=second.signature)


def test_conflicts_from_different_axes_are_not_resolved_away() -> None:
    signals = _signals(
        state="Borrador",
        signature="2025-01-01",
        flag="No",
        liquidation_start="2025-02-01",
        liquidation_end=None,
    )
    assessment = compose_contract_lifecycle(*_axes(signals))
    codes = {conflict.code for conflict in assessment.quality.conflicts}
    assert codes == {
        "CONTRACT_SIGNATURE_PRE_STATE_WITH_DATE",
        "LIQUIDATION_FLAG_VS_DATES",
    }
    assert assessment.quality.consistency == "conflicting"
    assert assessment.signature.signature_status == "conflicting"
    assert assessment.liquidation.liquidation_status == "conflicting"


def test_source_errors_survive_aggregation_without_becoming_missing() -> None:
    signals = _signals(
        state=_not_observed(),
        signature=_not_observed(),
        start=_not_observed(),
        end=_not_observed(),
        flag=_not_observed(),
        liquidation_start=_not_observed(),
        liquidation_end=_not_observed(),
    )
    error = SourceError(
        code="CONTRACT_DATASET_TIMEOUT",
        dataset="jbjy-vk9h",
        kind="timeout",
    )
    assessment = compose_contract_lifecycle(
        *_axes(signals, source_errors=(error,))
    )
    assert assessment.quality.source_errors == (error,)
    assert assessment.quality.missing_fields == ()
    assert {item.role for item in assessment.evidence} == {"not_observed"}


def test_aggregate_evidence_is_canonical_complete_and_raw_preserving() -> None:
    signals = _signals()
    assessment = compose_contract_lifecycle(*_axes(signals))
    assert len(assessment.evidence) == 8
    assert [(item.field, item.code) for item in assessment.evidence] == list(
        lifecycle_mod._EVIDENCE_ORDER
    )
    by_field = {}
    for item in assessment.evidence:
        by_field.setdefault(item.field, item.raw_observation)
    assert by_field["estado_contrato"] is signals.raw_contract_state
    assert by_field["fecha_de_firma"] is signals.raw_signature_date
    assert by_field["liquidaci_n"] is signals.raw_liquidation_flag


def test_all_public_assessments_are_frozen_slotted_and_hashable() -> None:
    lifecycle = compose_contract_lifecycle(*_axes(_signals()))
    classes_and_instances = (
        (ContractPerformanceAssessment, lifecycle.performance),
        (ContractSignatureAssessment, lifecycle.signature),
        (ChangeAxisAssessment, lifecycle.changes.modified),
        (ContractChangeAssessment, lifecycle.changes),
        (ContractLiquidationAssessment, lifecycle.liquidation),
        (ContractLifecycleAssessment, lifecycle),
    )
    for model, instance in classes_and_instances:
        assert model.__dataclass_params__.frozen is True
        assert "__slots__" in model.__dict__
        assert "__dict__" not in model.__dict__
        assert isinstance(hash(instance), int)


def test_quality_is_categorical_and_contains_no_percentage_axis() -> None:
    lifecycle = compose_contract_lifecycle(*_axes(_signals()))
    names = {field.name for field in dataclasses.fields(lifecycle.quality)}
    assert not names & {"score", "percentage", "confidence_percentage"}
    assert lifecycle.quality.completeness in {
        "complete",
        "partial",
        "insufficient",
    }
    assert lifecycle.quality.confidence in {"high", "medium", "low"}


def test_four_resolvers_do_not_import_each_other() -> None:
    modules = (
        performance_mod,
        signature_mod,
        changes_mod,
        liquidation_mod,
    )
    forbidden = {
        "app.core.truth_layer.contract_performance",
        "app.core.truth_layer.contract_signature",
        "app.core.truth_layer.contract_changes",
        "app.core.truth_layer.contract_liquidation",
    }
    for module in modules:
        tree = ast.parse(inspect.getsource(module))
        imports = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }
        assert not imports & (forbidden - {module.__name__})


def test_aggregator_never_calls_resolvers_or_reads_raw_fields() -> None:
    source = inspect.getsource(lifecycle_mod.compose_contract_lifecycle)
    assert "resolve_contract_" not in source
    assert "raw_" not in source


def test_t1b3_has_no_runtime_consumers() -> None:
    truth_layer_files = {
        "contract_performance.py",
        "contract_signature.py",
        "contract_changes.py",
        "contract_liquidation.py",
        "contract_lifecycle.py",
        "__init__.py",
    }
    consumers: list[str] = []
    for path in (ROOT / "app").rglob("*.py"):
        if path.parent.name == "truth_layer" and path.name in truth_layer_files:
            continue
        source = path.read_text(encoding="utf-8")
        if "app.core.truth_layer.contract_" in source:
            consumers.append(str(path.relative_to(ROOT)))
    assert consumers == []


def test_negative_scope_has_no_lots_temporality_or_offerability() -> None:
    modules = (
        performance_mod,
        signature_mod,
        changes_mod,
        liquidation_mod,
        lifecycle_mod,
    )
    executable = "\n".join(
        ast.unparse(ast.parse(inspect.getsource(module))) for module in modules
    ).casefold()
    for forbidden in (
        "numero_de_lotes",
        "lotidentity",
        "temporalquerycontext",
        "relation_to_reference",
        "offerability",
        "bid_ready",
    ):
        assert forbidden.casefold() not in executable


def test_resolution_is_deterministic_across_pythonhashseed() -> None:
    script = """
from app.core.truth_layer import *
o=lambda v: FieldObservation("observed", v)
s=ContractLifecycleSignals(o("Prorrogado"),o("2025-01-01"),o("a"),o("b"),o("No"),o("c"),o("d"))
p=resolve_contract_performance(s)
g=resolve_contract_signature(s)
c=resolve_contract_changes(s)
l=resolve_contract_liquidation(s)
a=compose_contract_lifecycle(p,g,c,l)
print(repr((a.evidence,a.quality,c,l)))
"""
    outputs = []
    for seed in ("1", "73", "999"):
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = seed
        outputs.append(
            subprocess.check_output(
                [sys.executable, "-c", script],
                cwd=ROOT,
                env=environment,
                text=True,
            )
        )
    assert len(set(outputs)) == 1
