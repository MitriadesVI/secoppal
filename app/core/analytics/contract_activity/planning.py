"""Single authority for Contract Activity WHERE, SQL, and fingerprints."""
from __future__ import annotations

import hashlib

from app.core.contract_universe import ContractUniverse, predicate

from .models import ContractActivityPlan, ContractActivityRequest, Operation


_GROUP_FIELD = {
    Operation.BY_MODALITY: "modalidad_de_contratacion",
    Operation.BY_REPORTED_STATE: "estado_contrato",
}


def build_plan(request: ContractActivityRequest) -> ContractActivityPlan:
    identity_clauses = [
        predicate(ContractUniverse.CELEBRATED),
        _any_exact("nombre_entidad", request.source_buyer_names),
    ]
    identity_where = " AND ".join(clause for clause in identity_clauses if clause)
    clauses = list(identity_clauses)
    if request.date_from is not None:
        clauses.extend((
            f"fecha_de_firma >= '{_escape(request.date_from)}'",
            f"fecha_de_firma <= '{_escape(request.date_to or '')}'",
        ))
    if request.modality_values:
        clauses.append(_any_exact("modalidad_de_contratacion", request.modality_values))
    if request.reported_states:
        clauses.append(_any_exact("estado_contrato", request.reported_states))
    if request.money is not None:
        clauses.append(
            f"valor_del_contrato {request.money.operator} {request.money.amount_cop}"
        )
    where = " AND ".join(clause for clause in clauses if clause)
    fingerprint = _fingerprint(f"{request.operation.value}\0{where}")
    identity_fingerprint = _fingerprint(identity_where)
    group_field = _GROUP_FIELD.get(request.operation)
    return ContractActivityPlan(
        request=request,
        identity_where=identity_where,
        identity_fingerprint=identity_fingerprint,
        where=where,
        fingerprint=fingerprint,
        identity_sql=_identity_sql(identity_where),
        scalar_sql=_scalar_sql(where, group_field),
        groups_sql=_groups_sql(where, group_field) if group_field else None,
    )


def _identity_sql(where: str) -> str:
    return (
        "SELECT id_contrato, count(*) AS row_count "
        f"WHERE {where} GROUP BY id_contrato "
        "HAVING count(*) > 1 OR count(id_contrato) = 0 "
        "OR trim(id_contrato) = ''"
    )


def _scalar_sql(where: str, group_field: str | None) -> str:
    metrics = [
        "count(*) AS row_count",
        "count(id_contrato) AS id_count",
        "count(distinct id_contrato) AS distinct_contract_count",
    ]
    if group_field:
        metrics.extend((
            f"count({group_field}) AS group_non_null_count",
            f"count(distinct {group_field}) AS distinct_group_count",
        ))
    return f"SELECT {', '.join(metrics)} WHERE {where}"


def _groups_sql(where: str, field: str) -> str:
    return (
        f"SELECT {field} AS group_label, count(*) AS row_count, "
        "count(id_contrato) AS id_count, "
        "count(distinct id_contrato) AS distinct_contract_count "
        f"WHERE {where} GROUP BY {field} "
        "ORDER BY distinct_contract_count DESC, group_label ASC"
    )


def _any_exact(field: str, values: tuple[str, ...]) -> str:
    clauses = tuple(f"{field} = '{_escape(value)}'" for value in values)
    if len(clauses) == 1:
        return clauses[0]
    return f"({' OR '.join(clauses)})"


def _escape(value: str) -> str:
    return value.replace("'", "''")


def _fingerprint(where: str) -> str:
    return hashlib.sha256(where.encode("utf-8")).hexdigest()
