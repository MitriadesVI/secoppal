"""Cross-component validation; no SQL recompilation or semantic recovery on reload."""
from dataclasses import fields, is_dataclass
from collections.abc import Mapping
from enum import Enum
from decimal import Decimal
import hashlib
import json

from app.core.entity_profile.frame_compiler import CATALOG_VERSION, SECTION_CALLS


def snapshot(value):
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if is_dataclass(value):
        return {field.name: snapshot(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {snapshot(key): snapshot(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [snapshot(item) for item in value]
    return value


def snapshot_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def validate_frame_plan(frame, plan):
    if plan.frame_ref != (frame.frame_id, frame.revision):
        raise ValueError('Frame is not the authorized revision')
    for source, compiled in plan.source_plans.items():
        if frame.source_predicates.get(source) != compiled.predicates:
            raise ValueError('Frame predicates differ from the authorized plan')


def validate_record(frame, reference, record):
    if record.catalog_version != CATALOG_VERSION:
        raise ValueError('incompatible execution catalog')
    if (record.frame_ref != (frame.frame_id, frame.revision)
            or reference.frame_ref != record.frame_ref
            or reference.plan_ref != record.plan_id
            or reference.evidence_ref != record.record_id
            or record.focus != frame.focus):
        raise ValueError('broken Frame/plan/evidence reference')
    plan = record.authorized_plan
    if not isinstance(plan, dict) or not plan:
        raise ValueError('missing authorized plan snapshot')
    if record.plan_digest != snapshot_digest(plan):
        raise ValueError('altered plan snapshot')
    if (plan['plan_id'] != record.plan_id or plan['frame_ref'] != list(record.frame_ref)
            or plan['catalog_version'] != record.catalog_version or plan['focus'] != record.focus):
        raise ValueError('foreign plan snapshot')
    if record.confirmed_frame != frame.to_dict():
        raise ValueError('foreign confirmed Frame snapshot')
    expected = set(record.expected_sections)
    unrequested = set(record.not_requested_sections)
    if expected & unrequested or expected | unrequested != set(SECTION_CALLS):
        raise ValueError('incoherent section coverage')
    from app.core.entity_profile.frame_values import SourceKind
    for name, source in plan['source_plans'].items():
        predicates = frame.source_predicates[SourceKind(name)]
        if source['predicates'] != snapshot(predicates) or source['fingerprint'] != predicates.fingerprint():
            raise ValueError('foreign source predicates')
    jobs = {job['job_id']: job for job in plan['jobs']}
    if len(jobs) != len(plan['jobs']) or set(jobs) != {call.job_id for call in record.calls}:
        raise ValueError('incomplete execution job membership')
    for call in record.calls:
        for metric in call.metrics:
            if metric.status == 'VALID':
                try:
                    finite = metric.value is not None and Decimal(metric.value).is_finite()
                except ArithmeticError as exc:
                    raise ValueError('invalid accredited metric') from exc
                if not finite:
                    raise ValueError('invalid accredited metric')
        job = jobs[call.job_id]
        source = plan['source_plans'][job['source']]
        if (call.call_id != job['call_id'] or call.dataset_id != job['dataset_id']
                or call.sql != job['sql'] or call.predicate_fingerprint != source['fingerprint']
                or source['dataset_id'] != job['dataset_id']):
            raise ValueError('foreign execution component')
