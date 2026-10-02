"""Offline Foundation V3 projection of host-accepted SEARCH obligations.

Only host resolutions and policies enter executable params. The producer's
claims remain evidence; they never supply dataset IDs, columns, params or SoQL.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from app.core.buyer_source_names import project_buyer_names
from app.core.contract_universe import (
    CELEBRATED_STATES, POLICY_VERSION, ContractUniverse, fold_state,
)
from app.core.foundation_operations import Operation, Subject
from app.core.modality_dataset_projection import project_modality_filter
from app.core.modality_filter import ModalityFilter
from app.core.interpretation_contract import (
    AcceptanceStatus, ConditionDisposition, ConditionExpression, ConditionRole,
    Disposition, Effect, InterpretationAcceptance, InterpretationProjection,
    LogicOperator, Polarity, ProposedCondition, RelationKind, ResolutionDecision,
)
from app.core.opportunity_retrieval_policy import REFERENCE_INSTANT_PARAM
from app.core.semantic_dimensions import SemanticDimension
from app.core.foundation_token_plan import CANDIDATE_CAP, TokenFilter, approved_tokens, superset_term
from app.core.soql_builder import ClauseOrigin, SoQLBuilder, UnsafeGlobalQueryError, WhereClauseReceipt
from app.data.departamentos import DEPARTAMENTOS_OFICIALES


O1B_VERSION = "1"
EXPLICIT_OPPORTUNITY_POLICY = "opportunity-explicit-state@1"
CONTRACT_UNIVERSE_POLICY = f"contract-universe@{POLICY_VERSION}"
NEGATIVE_STATE_POLICY = f"celebrated-state-complement@{POLICY_VERSION}"
FUNNEL_ABSORPTION_POLICY = "ADR-100@1"
LITERAL_PUBLISHED_POLICY = "published-opening@1"
MODALITY_FAMILIES = {
    "modality-lp-family": frozenset({"licitacion_publica", "licitacion_publica_obra"}),
    "modality-direct-family": frozenset({"contratacion_directa", "contratacion_directa_con_ofertas"}),
}
# Closed source projection for this SEARCH contract. These are physical source
# columns, not a fixture schema or values completed from returned rows.
SOURCE_SELECT_FIELDS = {
    SoQLBuilder.PROCESOS_DATASET: (
        "id_del_proceso", "referencia_del_proceso", "entidad", "departamento_entidad",
        "ciudad_entidad", "nombre_del_procedimiento", "descripci_n_del_procedimiento",
        "justificaci_n_modalidad_de", "modalidad_de_contratacion", "precio_base",
        "estado_de_apertura_del_proceso", "estado_del_procedimiento", "fase",
        "fecha_de_publicacion_del", "fecha_de_recepcion_de",
    ),
    SoQLBuilder.CONTRATOS_DATASET: (
        "id_contrato", "referencia_del_contrato", "nombre_entidad", "departamento", "ciudad",
        "objeto_del_contrato", "descripcion_del_proceso", "tipo_de_contrato",
        "justificacion_modalidad_de", "modalidad_de_contratacion", "valor_del_contrato",
        "estado_contrato", "fecha_de_firma",
    ),
}


class ProjectionError(ValueError):
    """A READY acceptance cannot be compiled without changing its meaning."""


@dataclass(frozen=True, slots=True)
class PolicyEffect:
    policy_ref: str
    effect: Effect


@dataclass(frozen=True, slots=True)
class ApprovedCallManifest:
    request_id: str
    scope_id: str
    operation: str
    dataset: str
    soql: str
    clauses: tuple[WhereClauseReceipt, ...]
    policies: tuple[PolicyEffect, ...]
    interpretation_hash: str
    plan_hash: str
    local_filter: TokenFilter | None = None


@dataclass(frozen=True, slots=True)
class ProjectionOutcome:
    status: AcceptanceStatus
    calls: tuple[InterpretationProjection, ...] = ()
    manifest: tuple[ApprovedCallManifest, ...] = ()
    reason: str = ""


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8", errors="surrogatepass")).hexdigest()


def _resolution(accepted: InterpretationAcceptance, target_id: str) -> tuple[str, ...]:
    matches = [item for item in accepted.resolutions if item.target_id == target_id]
    if len(matches) != 1 or matches[0].decision is not ResolutionDecision.RESOLVED:
        raise ProjectionError(f"MISSING_HOST_RESOLUTION:{target_id}")
    values = matches[0].resolved_ids
    if not values:
        raise ProjectionError(f"EMPTY_HOST_RESOLUTION:{target_id}")
    return values


def _one_resolution(accepted: InterpretationAcceptance, target_id: str) -> str:
    values = _resolution(accepted, target_id)
    if len(values) != 1:
        raise ProjectionError(f"MULTIPLE_HOST_RESOLUTIONS:{target_id}")
    return values[0]


def _policy(
    accepted: InterpretationAcceptance, condition_id: str, policy_id: str,
    version: str,
) -> str:
    matches = [item for item in accepted.policies
               if item.policy_id == policy_id and item.version == version
               and condition_id in item.condition_ids]
    if len(matches) != 1 or matches[0].effect is not Effect.EQUIVALENT:
        raise ProjectionError(f"MISSING_HOST_POLICY:{condition_id}:{policy_id}@{version}")
    return f"{policy_id}@{version}"


def _add_origin(
    origins: dict[str, ClauseOrigin], key: str, condition_id: str,
    *policy_ids: str,
) -> None:
    if key in origins:
        raise ProjectionError(f"CONFLICTING_PROJECTION_KEY:{key}")
    origins[key] = ClauseOrigin((condition_id,), tuple(policy_ids))


def _disposition(
    condition: ProposedCondition, *, policy_ref: str | None = None,
    transformed: bool = False, unknown: str = "NOT_APPLICABLE",
) -> ConditionDisposition:
    return ConditionDisposition(
        condition.condition_id, condition.request_ids,
        Disposition.TRANSFORMED if transformed else Disposition.CONSUMED,
        Effect.EQUIVALENT, condition.operator, condition.polarity, unknown,
        policy_ref or f"condition:{condition.condition_id}", condition.role,
    )


def _date_year(value: str) -> tuple[str, str]:
    if not value.isascii() or not value.isdigit() or len(value) != 4:
        raise ProjectionError("INVALID_HOST_YEAR")
    year = int(value)
    if year < 2000 or year >= 9999:
        raise ProjectionError("INVALID_HOST_YEAR")
    return f"{year:04d}-01-01", f"{year + 1:04d}-01-01"


def _amount(value: str) -> int:
    if not value.isascii() or not value.isdigit():
        raise ProjectionError("INVALID_HOST_AMOUNT")
    amount = int(value)
    if amount < 0:
        raise ProjectionError("INVALID_HOST_AMOUNT")
    return amount


def _conjunctive(node: ConditionExpression | None) -> bool:
    """The builder joins every WHERE clause with AND; only that tree compiles."""
    if node is None or node.operator is LogicOperator.ATOM:
        return True
    return node.operator is LogicOperator.AND and all(_conjunctive(c) for c in node.children)


def _validate_request_universe(request, dataset: str) -> None:
    if request.source_claim is not None:
        raise ProjectionError("EXPLICIT_SOURCE_UNSUPPORTED")
    expected = {
        Subject.PROCESSES: SoQLBuilder.PROCESOS_DATASET,
        Subject.CONTRACTS: SoQLBuilder.CONTRATOS_DATASET,
    }
    # The accepted linguistic subject defines the universe. The quote grounds
    # that claim; it is not a second lexical classifier after host admission.
    if expected.get(request.subject) != dataset:
        raise ProjectionError("UNIVERSE_DATASET_CONFLICT")


def _modality_condition(accepted, dataset, condition) -> tuple[list[str], str | None]:
    values = _resolution(accepted, condition.condition_id)
    ref = None
    if condition.operator == "is":
        if len(values) != 1:
            raise ProjectionError("LITERAL_MODALITY_REQUIRES_ONE_PROCEDURE")
    elif condition.operator == "family":
        families = [key for key, ids in MODALITY_FAMILIES.items() if ids == set(values)]
        if len(families) != 1:
            raise ProjectionError("UNACCREDITED_MODALITY_FAMILY")
        ref = _policy(accepted, condition.condition_id, families[0], "1")
    else:
        raise ProjectionError("UNSUPPORTED_MODALITY_OPERATOR")
    if condition.polarity is not Polarity.POSITIVE:
        raise ProjectionError("UNSUPPORTED_MODALITY_POLARITY")
    try:
        projection = project_modality_filter(
            ModalityFilter(include_procedure_ids=values),
            "procesos" if dataset == SoQLBuilder.PROCESOS_DATASET else "contratos",
        )
    except ValueError as exc:
        raise ProjectionError(f"MODALITY_SOURCE_BINDING_REQUIRED:{exc}") from exc
    if projection.include_unsatisfiable:
        raise ProjectionError("MODALITY_SOURCE_BINDING_REQUIRED")
    return list(values), ref


def _condition_params(
    accepted: InterpretationAcceptance, dataset: str,
    conditions: tuple[ProposedCondition, ...],
    reference_instant: datetime,
) -> tuple[dict, dict[str, ClauseOrigin], tuple[ConditionDisposition, ...]]:
    params: dict = {}
    origins: dict[str, ClauseOrigin] = {}
    dispositions: list[ConditionDisposition] = []
    if dataset == SoQLBuilder.CONTRATOS_DATASET:
        params.update(dataset="contratos", contract_universe=ContractUniverse.CELEBRATED.value,
                      contract_universe_version=POLICY_VERSION)
        origins["contract_universe"] = ClauseOrigin((), (CONTRACT_UNIVERSE_POLICY,))
    else:
        params["dataset"] = "procesos"

    opportunity = next((c for c in conditions if c.role is ConditionRole.REQUESTED_UNIVERSE), None)
    explicit_funnel = next((c for c in conditions if c.dimension is SemanticDimension.STATE
                            and _resolution(accepted, c.condition_id) == ("oferta_abierta",)), None)
    if opportunity is not None:
        if dataset != SoQLBuilder.PROCESOS_DATASET:
            raise ProjectionError("OPPORTUNITY_REQUIRES_PROCESSES")
        if any(c.dimension in {SemanticDimension.STATE, SemanticDimension.TIME}
               for c in conditions if c is not opportunity and c is not explicit_funnel):
            raise ProjectionError("UNSUPPORTED_OPPORTUNITY_OVERRIDE")
        if explicit_funnel is None:
            ref = _policy(accepted, opportunity.condition_id, "O1B", O1B_VERSION)
            params["intent_type"] = "opportunity_search"
            params[REFERENCE_INSTANT_PARAM] = reference_instant.isoformat()
            origins["opportunity_policy"] = ClauseOrigin((opportunity.condition_id,), (ref,))
        else:
            ref = _policy(accepted, opportunity.condition_id, "opportunity-explicit-state", "1")
        dispositions.append(_disposition(
            opportunity, policy_ref=ref, transformed=True,
            unknown="INCLUDE_AND_WARN" if explicit_funnel is None else "NOT_APPLICABLE",
        ))

    for condition in conditions:
        if condition.role is ConditionRole.REQUESTED_UNIVERSE:
            continue
        dimension = condition.dimension
        operator = condition.operator
        polarity = condition.polarity
        cid = condition.condition_id
        if dimension is SemanticDimension.MODALITY:
            values, ref = _modality_condition(accepted, dataset, condition)
            params["modalidad_include_procedure_ids"] = values
            _add_origin(origins, "modality", cid, *((ref,) if ref else ()))
            dispositions.append(_disposition(
                condition, policy_ref=ref, transformed=ref is not None, unknown="EXCLUDE_AND_WARN",
            ))
            continue
        value = _one_resolution(accepted, cid)
        if (dimension is SemanticDimension.TOPIC and operator == "whole_token_folded"
                and polarity is Polarity.POSITIVE):
            # Candidate superset only; the manifest's local filter restores the meaning.
            terms = params.setdefault("foundation_topic_terms", [])
            _add_origin(origins, f"foundation_topic_terms:{len(terms)}", cid)
            terms.append(superset_term(approved_tokens(value)))
            params["limit"] = CANDIDATE_CAP
        elif dimension is SemanticDimension.TOPIC and operator == "contains" and polarity is Polarity.POSITIVE:
            terms = params.setdefault("foundation_topic_terms", [])
            topic_policy = {"aseo": "topic-aseo-morph",
                            "convocatoria cultural": "convocatoria-vocab"}.get(value)
            refs = ()
            if topic_policy and any(p.policy_id == topic_policy and cid in p.condition_ids
                                    for p in accepted.policies):
                refs = (_policy(accepted, cid, topic_policy, "1"),)
            _add_origin(origins, f"foundation_topic_terms:{len(terms)}", cid, *refs)
            terms.append(value)
            if refs:
                dispositions.append(_disposition(
                    condition, policy_ref=refs[0], transformed=True, unknown="EXCLUDE_AND_WARN",
                ))
                continue
        elif (dimension is SemanticDimension.CONTRACT_TYPE_OR_NATURE
              and operator in {"is", "family"} and polarity is Polarity.POSITIVE):
            params["contract_type_include"] = [value]
            _add_origin(origins, "contract_type", cid)
        elif dimension is SemanticDimension.BUYER and operator == "is" and polarity is Polarity.POSITIVE:
            if not project_buyer_names(value, dataset):
                raise ProjectionError("BUYER_SOURCE_PROJECTION_REQUIRED")
            params["entidad_resolved"] = value
            _add_origin(origins, "entidad_resolved", cid)
        elif dimension is SemanticDimension.GEOGRAPHY and operator == "is" and polarity is Polarity.POSITIVE:
            if value not in DEPARTAMENTOS_OFICIALES:
                raise ProjectionError("UNACCREDITED_TERRITORY")
            params["departamento_resolved"] = value
            _add_origin(origins, "departamento_resolved", cid)
        elif dimension is SemanticDimension.TIME and operator == "during_year" and polarity is Polarity.POSITIVE:
            start, end = _date_year(value)
            params["fecha_desde"] = start
            params["fecha_hasta_exclusive"] = end
            _add_origin(origins, "fecha_desde", cid)
            _add_origin(origins, "fecha_hasta_exclusive", cid)
        elif dimension is SemanticDimension.VALUE and polarity is Polarity.POSITIVE and operator in {
            "less_than", "greater_than",
        }:
            key = "valor_max_exclusive" if operator == "less_than" else "valor_min_exclusive"
            params[key] = _amount(value)
            _add_origin(origins, key, cid)
        elif dimension is SemanticDimension.STATE and dataset == SoQLBuilder.PROCESOS_DATASET:
            if polarity is not Polarity.POSITIVE:
                raise ProjectionError("UNSUPPORTED_PROCESS_STATE_POLARITY")
            if operator == "family" and value == "oferta_abierta":
                from app.core.estado_families import ESTADO_FAMILIES

                params["estado_del_procedimiento"] = list(
                    ESTADO_FAMILIES["oferta_abierta"]["procesos"]["estado_del_procedimiento"]
                )
                params["estado_de_apertura_del_proceso"] = ["Abierto"]
                policy_refs: tuple[str, ...] = ()
                if len(condition.evidence) > 1:
                    policy_refs = (_policy(accepted, cid, "ADR-100", "1"),)
                if opportunity is not None:
                    policy_refs += (EXPLICIT_OPPORTUNITY_POLICY,)
                _add_origin(origins, "estado_del_procedimiento", cid, *policy_refs)
                _add_origin(origins, "estado_de_apertura_del_proceso", cid, *policy_refs)
                if len(condition.evidence) > 1:
                    dispositions.append(_disposition(
                        condition, policy_ref=FUNNEL_ABSORPTION_POLICY, transformed=True,
                    ))
                    continue
            elif operator == "is" and value == "Publicado":
                params["estado_del_procedimiento"] = ["Publicado"]
                params["estado_de_apertura_del_proceso"] = ["Abierto"]
                _add_origin(origins, "estado_del_procedimiento", cid, LITERAL_PUBLISHED_POLICY)
                _add_origin(origins, "estado_de_apertura_del_proceso", cid, LITERAL_PUBLISHED_POLICY)
                dispositions.append(_disposition(
                    condition, policy_ref=LITERAL_PUBLISHED_POLICY, transformed=True,
                ))
                continue
            else:
                raise ProjectionError("UNSUPPORTED_PROCESS_STATE")
        elif dimension is SemanticDimension.STATE and dataset == SoQLBuilder.CONTRATOS_DATASET:
            if operator != "is" or fold_state(value) != "TERMINADO":
                raise ProjectionError("UNSUPPORTED_CONTRACT_STATE")
            if polarity is Polarity.POSITIVE:
                params["estado_contrato"] = [next(
                    state for state in CELEBRATED_STATES if fold_state(state) == "TERMINADO"
                )]
                _add_origin(origins, "estado_contrato", cid)
            else:
                ref = _policy(accepted, cid, "celebrated-state-complement", POLICY_VERSION)
                permitted = [state for state in CELEBRATED_STATES
                             if fold_state(state) != "TERMINADO"]
                if not permitted or params["contract_universe"] != ContractUniverse.CELEBRATED.value:
                    raise ProjectionError("UNSAFE_STATE_COMPLEMENT")
                params["estado_contrato"] = permitted
                _add_origin(origins, "estado_contrato", cid, ref)
                dispositions.append(_disposition(condition, policy_ref=ref, transformed=True,
                                                 unknown="EXCLUDE_AND_WARN"))
                continue
        else:
            raise ProjectionError(f"UNSUPPORTED_CONDITION:{cid}")
        dispositions.append(_disposition(
            condition, unknown=("NOT_APPLICABLE" if dimension is SemanticDimension.STATE
                                and dataset == SoQLBuilder.PROCESOS_DATASET
                                else "EXCLUDE_AND_WARN"),
        ))

    return params, origins, tuple(dispositions)


def _token_filter(accepted, builder, dataset, conditions) -> TokenFilter | None:
    """Bind the approved tokens to the same text columns the superset clause reads."""
    token = next((c for c in conditions if c.dimension is SemanticDimension.TOPIC
                  and c.operator == "whole_token_folded"), None)
    if token is None:
        return None
    spec = builder.SPECS[dataset]
    columns = tuple(field for field in (spec.object_name, spec.description) if field)
    if not set(columns) <= set(SOURCE_SELECT_FIELDS[dataset]):
        raise ProjectionError("TOKEN_COLUMNS_NOT_SELECTED")
    return TokenFilter(token.condition_id, columns,
                       approved_tokens(_one_resolution(accepted, token.condition_id)),
                       candidate_cap=CANDIDATE_CAP)


def project_accepted(
    accepted: InterpretationAcceptance, *, reference_instant: datetime,
    builder: SoQLBuilder | None = None,
) -> ProjectionOutcome:
    """Compile accepted meaning; never inspect the original turn or execute SoQL."""
    if type(accepted) is not InterpretationAcceptance:
        raise ProjectionError("INVALID_ACCEPTANCE")
    if accepted.status is not AcceptanceStatus.READY:
        return ProjectionOutcome(accepted.status, reason=accepted.reason)
    if accepted.proposal is None:
        raise ProjectionError("READY_REQUIRES_PROPOSAL")
    if type(reference_instant) is not datetime or reference_instant.utcoffset() is None:
        raise ProjectionError("REFERENCE_INSTANT_REQUIRED")
    requests = accepted.proposal.requests
    searches = all(accepted.proposal.operation_of(item.request_id) is Operation.SEARCH for item in requests)
    if len(requests) > 1:
        shared = [item for item in accepted.proposal.relations
                  if item.kind is RelationKind.SHARES_SCOPE]
        if (len(requests) != 2 or len(shared) != 1
                or set(shared[0].members) != {item.request_id for item in requests}
                or not searches):
            return ProjectionOutcome(AcceptanceStatus.UNSUPPORTED, reason="MULTI_REQUEST")
    if not searches:
        return ProjectionOutcome(AcceptanceStatus.UNSUPPORTED, reason="UNSUPPORTED_OPERATION")
    if not requests:
        raise ProjectionError("READY_WITHOUT_REQUEST")
    if not all(_conjunctive(item.condition_expression) for item in requests):
        return ProjectionOutcome(AcceptanceStatus.UNSUPPORTED, reason="NONCONJUNCTIVE_EXPRESSION")
    token_conditions = [item for item in accepted.proposal.conditions
                        if item.dimension is SemanticDimension.TOPIC
                        and item.operator == "whole_token_folded"]
    for item in token_conditions:
        if item.polarity is not Polarity.POSITIVE:
            return ProjectionOutcome(AcceptanceStatus.UNSUPPORTED, reason="TOKEN_NEGATION_NOT_ACCREDITED")
        value = _one_resolution(accepted, item.condition_id)
        try:
            approved_tokens(value)
        except ValueError as exc:
            return ProjectionOutcome(AcceptanceStatus.UNSUPPORTED, reason=str(exc))
    if any(sum(request.request_id in item.request_ids for item in token_conditions) > 1
           for request in requests):
        return ProjectionOutcome(AcceptanceStatus.UNSUPPORTED, reason="MULTIPLE_TOKEN_FILTERS")
    builder = builder or SoQLBuilder()
    interpretation_hash = _digest(accepted.to_dict())
    projections: list[InterpretationProjection] = []
    manifests: list[ApprovedCallManifest] = []
    datasets: list[str] = []
    for request in requests:
        if request.universe_claim is None or request.universe_evidence is None:
            raise ProjectionError("UNGROUNDED_REQUEST_UNIVERSE")
        dataset = _one_resolution(accepted, request.request_id)
        if dataset not in {builder.PROCESOS_DATASET, builder.CONTRATOS_DATASET}:
            raise ProjectionError("REQUEST_DATASET_BINDING")
        _validate_request_universe(request, dataset)
        datasets.append(dataset)
        conditions = tuple(c for c in accepted.proposal.conditions
                           if request.request_id in c.request_ids)
        params, origins, dispositions = _condition_params(
            accepted, dataset, conditions, reference_instant,
        )
        allow_time_only = (len(conditions) == 1
                           and conditions[0].dimension is SemanticDimension.TIME)
        local_filter = _token_filter(accepted, builder, dataset, conditions)
        for operation in ("query", "count"):
            receipts: list[WhereClauseReceipt] = []
            try:
                if operation == "query":
                    soql = builder.build(
                        dataset, params, allow_time_only=allow_time_only, allow_opportunity_only=True,
                        select_fields=SOURCE_SELECT_FIELDS[dataset],
                        clause_origins=origins, clause_receipts=receipts,
                    )
                else:
                    soql = builder.build_count(
                        dataset, params, allow_time_only=allow_time_only, allow_opportunity_only=True,
                        clause_origins=origins, clause_receipts=receipts,
                    )
            except (KeyError, TypeError, ValueError, UnsafeGlobalQueryError) as exc:
                raise ProjectionError(f"SOQL_PROJECTION_BLOCKED:{exc}") from exc
            projection = InterpretationProjection(
                accepted, (request.request_id,), dataset, soql, dispositions,
                operation=operation,
            )
            clauses = tuple(receipts)
            host_policies = {
                f"{policy.policy_id}@{policy.version}": policy.effect
                for policy in accepted.policies
            }
            internal_policies = {
                CONTRACT_UNIVERSE_POLICY: Effect.NARROWING,
                LITERAL_PUBLISHED_POLICY: Effect.EQUIVALENT,
            }
            policy_refs = {ref for clause in clauses for ref in clause.policy_ids}
            policy_refs.update(d.clause_or_policy for d in dispositions
                               if "@" in d.clause_or_policy)
            if not policy_refs <= set(host_policies) | set(internal_policies):
                raise ProjectionError("UNKNOWN_PROJECTION_POLICY")
            policies = tuple(PolicyEffect(
                ref, {**internal_policies, **host_policies}[ref],
            ) for ref in sorted(policy_refs))
            plan_hash = _digest({
                "request_id": request.request_id, "scope_id": request.scope_id,
                "operation": operation, "dataset": dataset, "soql": soql,
                "clauses": [
                    {"clause": c.clause, "conditions": c.condition_ids,
                     "policies": c.policy_ids} for c in clauses
                ],
                "policies": [(p.policy_ref, p.effect.value) for p in policies],
                **({"local_filter": local_filter.receipt()} if local_filter else {}),
            })
            projections.append(projection)
            manifests.append(ApprovedCallManifest(
                request.request_id, request.scope_id, operation, dataset, soql,
                clauses, policies, interpretation_hash, plan_hash, local_filter,
            ))
    if len(requests) > 1 and set(datasets) != {
        builder.PROCESOS_DATASET, builder.CONTRATOS_DATASET,
    }:
        raise ProjectionError("DUAL_REQUEST_DATASETS")
    for pair in range(0, len(manifests), 2):
        if manifests[pair].clauses != manifests[pair + 1].clauses:
            raise ProjectionError("QUERY_COUNT_WHERE_MISMATCH")
    supplied = {f"{policy.policy_id}@{policy.version}" for policy in accepted.policies}
    used = {policy.policy_ref for manifest in manifests for policy in manifest.policies}
    if not supplied <= used:
        raise ProjectionError("ORPHAN_HOST_POLICY")
    return ProjectionOutcome(AcceptanceStatus.READY, tuple(projections), tuple(manifests))
