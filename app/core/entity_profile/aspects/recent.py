"""Small, explicitly ordered recent process/contract samples."""
from __future__ import annotations

from typing import Any

from app.core.entity_profile.aspects.common import as_float, build_where, iso_date, scalar_url
from app.core.entity_profile.models import CONTRACT_DATASET, RecentRecord
from app.core.result_identity import result_identity


RECENT_LIMIT = 3


def _primary_identity_field(dataset_id: str) -> str:
    return "id_contrato" if dataset_id == CONTRACT_DATASET else "id_del_proceso"


def build_recent_query(builder: Any, dataset_id: str, params: dict) -> str:
    spec, where = build_where(builder, dataset_id, params)
    fields = list(spec.select_fields)
    for optional in (spec.contractor_document, spec.contractor_document_type):
        if optional and optional not in fields:
            fields.append(optional)
    identity = _primary_identity_field(dataset_id)
    return (
        f"SELECT {', '.join(fields)} WHERE {where} "
        f"ORDER BY {spec.date} DESC, {identity} ASC LIMIT {RECENT_LIMIT}"
    )


def parse_recent(rows: list[dict], dataset_id: str, spec: Any) -> tuple[RecentRecord, ...]:
    parsed: list[RecentRecord] = []
    for row in rows:
        identity = result_identity(row, dataset_id)
        parsed.append(RecentRecord(
            dataset_id=dataset_id,
            stable_identity=identity.stable_key,
            identity_status=identity.identity_status,
            title=str(row.get(spec.object_name) or "Sin descripción"),
            date=iso_date(row.get(spec.date)),
            value=as_float(row.get(spec.value)),
            url=scalar_url(row.get(spec.url)),
        ))
    return tuple(parsed)
