"""Two distinct contractor rankings, keyed by typed document identity."""
from __future__ import annotations

from typing import Any, Literal

from app.core.actor_document_types import is_personal_source_type
from app.core.actor_identity import normalize_identity
from app.core.entity_profile.aspects.common import as_float, as_int, build_where
from app.core.entity_profile.models import CONTRACT_DATASET, SupplierMetric


SUPPLIER_LIMIT = 5
SupplierRanking = Literal["count", "value"]


def build_supplier_query(builder: Any, params: dict, ranking: SupplierRanking) -> str:
    spec, where = build_where(builder, CONTRACT_DATASET, params)
    document = spec.contractor_document
    document_type = spec.contractor_document_type
    supplier = spec.contractor
    if not document or not document_type or not supplier:
        raise ValueError("Contracts dataset has no stable supplier identity fields")
    metric = "contract_count" if ranking == "count" else "total_contract_value"
    return (
        f"SELECT {document}, {document_type}, max({supplier}) AS supplier_name, "
        f"count(*) AS contract_count, sum({spec.value}) AS total_contract_value "
        f"WHERE {where} AND {document} IS NOT NULL AND {document} != '' "
        f"GROUP BY {document}, {document_type} "
        f"ORDER BY {metric} DESC, {document_type} ASC, {document} ASC "
        f"LIMIT {SUPPLIER_LIMIT}"
    )


def _declared_type(source_type: object) -> str | None:
    if not isinstance(source_type, str):
        return None
    if source_type.strip().upper() == "NIT":
        return "NIT"
    if is_personal_source_type(source_type):
        return "CC"
    return None


def parse_suppliers(rows: list[dict]) -> tuple[SupplierMetric, ...]:
    parsed: list[SupplierMetric] = []
    for row in rows:
        source_type = str(row.get("tipodocproveedor") or "No definido")
        document = row.get("documento_proveedor")
        match = normalize_identity(
            document,
            document_type=_declared_type(source_type),
            source=CONTRACT_DATASET,
        )
        if match.key is None:
            continue
        key = match.key
        parsed.append(SupplierMetric(
            actor_identity=f"{key.document_type}:{key.base_number}",
            document_type=key.document_type,
            document=key.base_number,
            observed_name=str(row.get("supplier_name") or "Proveedor sin nombre"),
            contract_count=as_int(row.get("contract_count")),
            total_contract_value=as_float(row.get("total_contract_value")) or 0.0,
        ))
    return tuple(parsed)
