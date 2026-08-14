"""Pure, dormant row observers for the SECOP Truth Layer foundation.

The functions in this module compose already-transported row evidence. They
perform no query, I/O, ranking, filtering, workflow dispatch or clock read.
Callers must inject a timezone-aware ``reference_instant``.

Contract axis resolvers remain useful low-level libraries, but operational
consumers must use :func:`observe_contract_truth_axes`: it is the seam that
grounds planned liquidation dates in the injected temporal reference.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .contract_changes import resolve_contract_changes
from .contract_lifecycle import compose_contract_lifecycle
from .contract_liquidation import resolve_contract_liquidation
from .contract_performance import resolve_contract_performance
from .contract_signature import resolve_contract_signature
from .models import (
    ContractLifecycleSignals,
    FieldObservation,
    ProcessLifecycleSignals,
    PublicationVisibilitySignals,
    TemporalQueryContext,
)
from .process_lifecycle import (
    PROCESS_DATASET_ID,
    resolve_process_lifecycle,
    resolve_process_resolution,
)
from .temporal_contract import (
    resolve_liquidation_event,
    resolve_signature_event,
)
from .temporal_resolver import resolve_temporal_assessment
from .visibility_resolver import resolve_publication_visibility

__all__ = ["observe_contract_truth_axes", "observe_process_truth_axes"]

REFERENCE_TIMEZONE = "America/Bogota"
PROCESS_CLOSING_DATE_FIELD = "fecha_de_recepcion_de"
PROCESS_MODALITY_FIELD = "modalidad_de_contratacion"
PROCESS_URL_FIELD = "urlproceso"


def _validate_reference(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("reference_instant must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("reference_instant must be timezone-aware")
    return value


def _observation(row: Mapping[str, Any], field: str) -> FieldObservation:
    if field not in row:
        return FieldObservation("not_observed", None)
    value = row[field]
    if value is None:
        return FieldObservation("missing", None)
    return FieldObservation("observed", value)


_COMPLETENESS_RANK = {"complete": 0, "partial": 1, "insufficient": 2}
_CONSISTENCY_RANK = {"consistent": 0, "unknown": 1, "conflicting": 2}
_CONFIDENCE_RANK = {"high": 0, "medium": 1, "low": 2}


def _not_better_than(value: str, floor: str, rank: dict[str, int]) -> str:
    """Return the weaker conclusion so grounding never upgrades quality."""
    return value if rank.get(value, 3) >= rank.get(floor, 3) else floor


def _quality_summary(*qualities: object) -> dict[str, Any]:
    return {
        "completeness": max(
            (getattr(item, "completeness") for item in qualities),
            key=lambda value: _COMPLETENESS_RANK.get(value, 3),
        ),
        "consistency": max(
            (getattr(item, "consistency") for item in qualities),
            key=lambda value: _CONSISTENCY_RANK.get(value, 3),
        ),
        "confidence": max(
            (getattr(item, "confidence") for item in qualities),
            key=lambda value: _CONFIDENCE_RANK.get(value, 3),
        ),
        "conflict_codes": sorted({
            conflict.code
            for item in qualities
            for conflict in getattr(item, "conflicts", ())
        }),
        "source_error_codes": sorted({
            error.code
            for item in qualities
            for error in getattr(item, "source_errors", ())
        }),
    }


def observe_process_truth_axes(
    row: Mapping[str, Any], *, reference_instant: datetime,
) -> dict[str, Any]:
    """Resolve neutral process lifecycle, resolution, temporal and visibility axes."""
    _validate_reference(reference_instant)
    if not isinstance(row, Mapping):
        raise TypeError("row must be a mapping")

    observe = lambda field: _observation(row, field)  # noqa: E731
    signals = ProcessLifecycleSignals(
        raw_procedure_state=observe("estado_del_procedimiento"),
        raw_procedure_state_id=observe("id_estado_del_procedimiento"),
        raw_opening_state=observe("estado_de_apertura_del_proceso"),
        raw_phase=observe("fase"),
        raw_summary_state=observe("estado_resumen"),
        raw_adjudicated=observe("adjudicado"),
        raw_adjudication_date=observe("fecha_adjudicacion"),
        raw_adjudication_id=observe("id_adjudicacion"),
        raw_portfolio_id=observe("id_del_portafolio"),
        raw_modality=observe(PROCESS_MODALITY_FIELD),
    )
    lifecycle = resolve_process_lifecycle(signals)
    resolution = resolve_process_resolution(signals)
    temporal_context = TemporalQueryContext(
        reference_instant=reference_instant,
        reference_timezone=REFERENCE_TIMEZONE,
        requested_range_start=None,
        requested_range_end=None,
        requested_event_raw="offer_deadline",
    )
    deadline = resolve_temporal_assessment(
        observe(PROCESS_CLOSING_DATE_FIELD),
        context=temporal_context,
        requested_event="offer_deadline",
        applied_field=PROCESS_CLOSING_DATE_FIELD,
        dataset=PROCESS_DATASET_ID,
    )
    visibility = resolve_publication_visibility(
        PublicationVisibilitySignals(
            raw_url=observe(PROCESS_URL_FIELD),
            normalized_url=observe("urlproceso_normalized"),
            raw_process_id=observe("id_del_proceso"),
            normalization_reason=observe("urlproceso_normalization_reason"),
        ),
        stage_context=lifecycle.stage,
    )
    qualities = (
        lifecycle.quality,
        resolution.quality,
        deadline.quality,
        visibility.quality,
    )
    return {
        "process_lifecycle": lifecycle.stage,
        "process_resolution": {
            "award_signal": resolution.award_signal,
            "contract_link_signal": resolution.contract_link_signal,
            "resolution_status": resolution.resolution_status,
        },
        "temporal": {
            "event": deadline.requested_event,
            "status": deadline.value.status,
            "relation": deadline.relation_to_reference,
            "range_membership": deadline.range_membership,
            "source_timezone": deadline.value.source_timezone,
            "range_timezone": deadline.context.range_timezone,
        },
        "visibility": visibility.visibility,
        "quality_summary": _quality_summary(*qualities),
        "reason_codes": sorted({
            code for quality in qualities for code in quality.reason_codes
        }),
    }


def _temporal_payload(assessment: object) -> dict[str, Any]:
    value = getattr(assessment, "value")
    return {
        "event": getattr(assessment, "requested_event"),
        "status": value.status,
        "relation": getattr(assessment, "relation_to_reference"),
        "range_membership": getattr(assessment, "range_membership"),
    }


def _grounded_liquidation(
    liquidation: object, end_event: object, start_event: object,
) -> tuple[dict[str, str], tuple[str, ...], bool]:
    """Prevent a planned or same-day end date from proving completion."""
    status = getattr(liquidation, "liquidation_status")
    completion = getattr(liquidation, "completion")
    payload = {"status": status, "completion": completion}
    if completion != "finished":
        return payload, (), False
    if getattr(end_event, "relation_to_reference") == "past":
        return payload, (), False

    started = getattr(start_event, "relation_to_reference") == "past"
    return (
        {
            "status": "in_progress" if started else status,
            "completion": "unknown",
        },
        (
            "LIQUIDATION_STARTED_END_DATE_NOT_YET_REACHED"
            if started
            else "LIQUIDATION_END_DATE_NOT_YET_REACHED",
        ),
        True,
    )


def observe_contract_truth_axes(
    row: Mapping[str, Any], *, reference_instant: datetime,
) -> dict[str, Any]:
    """Resolve the six contract axes through the safe temporal composition seam."""
    _validate_reference(reference_instant)
    if not isinstance(row, Mapping):
        raise TypeError("row must be a mapping")

    observe = lambda field: _observation(row, field)  # noqa: E731
    signals = ContractLifecycleSignals(
        raw_contract_state=observe("estado_contrato"),
        raw_signature_date=observe("fecha_de_firma"),
        raw_start_date=observe("fecha_de_inicio_del_contrato"),
        raw_end_date=observe("fecha_de_fin_del_contrato"),
        raw_liquidation_flag=observe("liquidaci_n"),
        raw_liquidation_start_date=observe("fecha_inicio_liquidacion"),
        raw_liquidation_end_date=observe("fecha_fin_liquidacion"),
    )
    performance = resolve_contract_performance(signals)
    signature = resolve_contract_signature(signals)
    changes = resolve_contract_changes(signals)
    liquidation = resolve_contract_liquidation(signals)
    lifecycle = compose_contract_lifecycle(
        performance, signature, changes, liquidation
    )
    context = TemporalQueryContext(
        reference_instant=reference_instant,
        reference_timezone=REFERENCE_TIMEZONE,
        requested_range_start=None,
        requested_range_end=None,
        requested_event_raw=None,
    )
    signature_event = resolve_signature_event(signature, context=context)
    liquidation_event = resolve_liquidation_event(
        liquidation, phase="end", context=context
    )
    liquidation_start_event = resolve_liquidation_event(
        liquidation, phase="start", context=context
    )
    liquidation_payload, grounding_reasons, grounded = _grounded_liquidation(
        liquidation, liquidation_event, liquidation_start_event
    )
    qualities = (
        lifecycle.quality,
        signature_event.quality,
        liquidation_event.quality,
    )
    quality = _quality_summary(*qualities)
    if grounded:
        quality["completeness"] = _not_better_than(
            quality["completeness"], "partial", _COMPLETENESS_RANK
        )
        quality["confidence"] = _not_better_than(
            quality["confidence"], "medium", _CONFIDENCE_RANK
        )
    return {
        "performance": performance.performance,
        "signature": {
            "status": signature.signature_status,
            "date_status": signature.signature_date_status,
        },
        "changes": {
            axis.kind: {
                "signal": axis.signal,
                "source_coverage": axis.source_coverage,
                "historical_conclusion": axis.historical_conclusion,
            }
            for axis in (changes.modified, changes.extended, changes.assigned)
        },
        "liquidation": liquidation_payload,
        "signature_event": _temporal_payload(signature_event),
        "liquidation_event": _temporal_payload(liquidation_event),
        "quality_summary": quality,
        "reason_codes": sorted(
            {code for item in qualities for code in item.reason_codes}
            | set(grounding_reasons)
        ),
    }
