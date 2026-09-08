"""El snapshot inmutable de lo autorizado y lo observado, para publicarlo.

`plan_ref` y `evidence_ref` del contrato de continuación tienen que apuntar a
algo que exista después de que el proceso muera. Un caché en memoria no sirve, y
«buscar un plan parecido» tampoco: dos planes con el mismo foco pueden tener
predicados distintos.

Este registro es esa cosa: el plan tal como se congeló —SQL incluido— y el
resumen de la evidencia por llamada y por métrica. No reinterpreta el Frame, no
sustituye la evidencia cruda de las trazas y no guarda filas: guarda qué se
autorizó, qué se intentó y qué cifra quedó acreditada.

Se serializa en JSON ESTRICTO. Un valor no finito no entra —ya lo rechaza la
acreditación— y al leer tampoco se acepta: NaN e Infinity no son JSON, y un
lector que los admita acaba imprimiéndolos como si fueran montos.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
import uuid

from app.core.entity_profile.frame_compiler import ProfilePlan
from app.core.entity_profile.frame_evidence import MetricStatus
from app.core.entity_profile.frame_runner import ProfileEvidence


EXECUTION_RECORD_VERSION = 1

EXECUTION_RECORD_KEY = "execution_record"


class ExecutionRecordError(ValueError):
    """Un registro ilegible no se completa a ojo: invalida la continuidad."""


@dataclass(frozen=True, slots=True)
class RecordedMetric:
    name: str
    value: str | None
    status: str

    def to_dict(self) -> dict:
        return {"name": self.name, "value": self.value, "status": self.status}

    @classmethod
    def from_dict(cls, raw: object) -> "RecordedMetric":
        if not isinstance(raw, dict):
            raise ExecutionRecordError("metric must be an object")
        try:
            return cls(
                name=str(raw["name"]),
                value=None if raw["value"] is None else str(raw["value"]),
                status=MetricStatus(raw["status"]).value,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ExecutionRecordError(f"malformed metric: {exc}") from exc


@dataclass(frozen=True, slots=True)
class RecordedCall:
    job_id: str
    call_id: str
    dataset_id: str
    predicate_fingerprint: str
    sql: str
    status: str
    attempted: bool
    metrics: tuple[RecordedMetric, ...] = ()

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id, "call_id": self.call_id,
            "dataset_id": self.dataset_id,
            "predicate_fingerprint": self.predicate_fingerprint,
            "sql": self.sql, "status": self.status, "attempted": self.attempted,
            "metrics": [metric.to_dict() for metric in self.metrics],
        }

    @classmethod
    def from_dict(cls, raw: object) -> "RecordedCall":
        if not isinstance(raw, dict):
            raise ExecutionRecordError("call must be an object")
        try:
            return cls(
                job_id=str(raw["job_id"]), call_id=str(raw["call_id"]),
                dataset_id=str(raw["dataset_id"]),
                predicate_fingerprint=str(raw["predicate_fingerprint"]),
                sql=str(raw["sql"]), status=str(raw["status"]),
                attempted=bool(raw["attempted"]),
                metrics=tuple(
                    RecordedMetric.from_dict(item)
                    for item in raw.get("metrics") or ()
                ),
            )
        except (KeyError, TypeError) as exc:
            raise ExecutionRecordError(f"malformed call: {exc}") from exc


@dataclass(frozen=True, slots=True)
class ExecutionRecord:
    """Plan congelado + resumen de evidencia. Referenciable por `evidence_ref`."""

    record_id: str
    plan_id: str
    frame_ref: tuple[str, int]
    catalog_version: str
    focus: str
    expected_sections: tuple[str, ...]
    not_requested_sections: tuple[str, ...]
    limitations: tuple[str, ...]
    calls: tuple[RecordedCall, ...]
    schema_version: int = EXECUTION_RECORD_VERSION

    @property
    def planned_jobs(self) -> int:
        return len(self.calls)

    @property
    def attempted_calls(self) -> int:
        return sum(call.attempted for call in self.calls)

    @property
    def accredited_metrics(self) -> int:
        return sum(
            1 for call in self.calls for metric in call.metrics
            if metric.status == MetricStatus.VALID.value
        )

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "record_id": self.record_id, "plan_id": self.plan_id,
            "frame_ref": list(self.frame_ref),
            "catalog_version": self.catalog_version, "focus": self.focus,
            "expected_sections": list(self.expected_sections),
            "not_requested_sections": list(self.not_requested_sections),
            "limitations": list(self.limitations),
            "calls": [call.to_dict() for call in self.calls],
        }

    @classmethod
    def from_dict(cls, raw: object) -> "ExecutionRecord":
        if not isinstance(raw, dict):
            raise ExecutionRecordError("execution record must be an object")
        version = raw.get("schema_version")
        if version != EXECUTION_RECORD_VERSION:
            raise ExecutionRecordError(f"unknown execution record: {version!r}")
        try:
            ref = raw["frame_ref"]
            return cls(
                record_id=str(raw["record_id"]), plan_id=str(raw["plan_id"]),
                frame_ref=(str(ref[0]), int(ref[1])),
                catalog_version=str(raw["catalog_version"]),
                focus=str(raw["focus"]),
                expected_sections=tuple(str(v) for v in raw["expected_sections"]),
                not_requested_sections=tuple(
                    str(v) for v in raw["not_requested_sections"]
                ),
                limitations=tuple(str(v) for v in raw.get("limitations") or ()),
                calls=tuple(
                    RecordedCall.from_dict(item) for item in raw.get("calls") or ()
                ),
            )
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            raise ExecutionRecordError(f"malformed execution record: {exc}") from exc


def build_execution_record(
    plan: ProfilePlan, evidence: ProfileEvidence,
) -> ExecutionRecord:
    if evidence.plan_id != plan.plan_id:
        raise ExecutionRecordError("evidence belongs to another plan")
    calls: list[RecordedCall] = []
    for job in plan.jobs:
        observed = evidence.call(job.job_id)
        if observed is None:
            continue
        calls.append(RecordedCall(
            job_id=job.job_id, call_id=job.call_id, dataset_id=job.dataset_id,
            predicate_fingerprint=observed.predicate_fingerprint,
            sql=observed.sql, status=observed.status, attempted=observed.attempted,
            metrics=tuple(
                RecordedMetric(
                    name=metric.name, value=_serialize(metric.value),
                    status=metric.status.value,
                )
                for metric in observed.metrics
            ),
        ))
    return ExecutionRecord(
        record_id=uuid.uuid4().hex[:16],
        plan_id=plan.plan_id, frame_ref=plan.frame_ref,
        catalog_version=plan.catalog_version, focus=plan.focus,
        expected_sections=tuple(sorted(plan.expected_sections)),
        not_requested_sections=tuple(sorted(plan.not_requested_sections)),
        limitations=plan.limitations, calls=tuple(calls),
    )


def _serialize(value: Any) -> str | None:
    """Decimal e int se guardan como texto exacto; nada no finito llega aquí."""
    if value is None:
        return None
    if isinstance(value, Decimal) and not value.is_finite():
        raise ExecutionRecordError("a non-finite metric never reaches a record")
    if isinstance(value, float) and value != value:  # NaN
        raise ExecutionRecordError("a non-finite metric never reaches a record")
    return str(value)


__all__ = [
    "EXECUTION_RECORD_KEY",
    "EXECUTION_RECORD_VERSION",
    "ExecutionRecord",
    "ExecutionRecordError",
    "RecordedCall",
    "RecordedMetric",
    "build_execution_record",
]
