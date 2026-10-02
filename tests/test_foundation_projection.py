"""Negative contracts for the offline Foundation V3 host projector."""

from datetime import datetime
from dataclasses import replace
import hashlib

import pytest
from hypothesis import given, strategies as st

from app.core.foundation_operations import Operation, Subject
from app.core.foundation_projection import ProjectionError, project_accepted
from app.core.interpretation_contract import (
    AcceptanceStatus,
    ConditionExpression,
    ConditionRole,
    Effect,
    HostResolution,
    HostPolicy,
    InterpretationAcceptance,
    InterpretationProposal,
    LogicOperator,
    Polarity,
    ProposedCondition,
    ProposedJob,
    ProposedRequest,
    RawSpan,
    ResolutionDecision,
)
from app.core.semantic_dimensions import SemanticDimension
from app.core.soql_builder import ClauseOrigin, SoQLBuilder, UnsafeGlobalQueryError
from tests.foundation_ir import with_requests


NOW = datetime.fromisoformat("2026-09-26T12:00:00-05:00")
RAW = "Busca contratos de aseo por menos de 200 millones"


def _acceptance(*, dataset="jbjy-vk9h", condition=None, resolved="aseo"):
    condition = condition or ProposedCondition(
        "q1.c1", ("q1",), SemanticDimension.TOPIC, "contains", Polarity.POSITIVE,
        "aseo", (RawSpan(19, 23, "aseo"),),
    )
    proposal = InterpretationProposal(
        hashlib.sha256(RAW.encode()).hexdigest(), "test", "1",
        (ProposedRequest(
            "q1", "q1", Subject.CONTRACTS, (RawSpan(0, len(RAW), RAW),),
            universe_claim="contratos", universe_evidence=RawSpan(6, 15, "contratos"),
            condition_expression=ConditionExpression(LogicOperator.ATOM, "q1.c1"),
        ),),
        (condition,), jobs=(ProposedJob("job-search", Operation.SEARCH, ("q1",)),),
    )
    return InterpretationAcceptance(proposal, AcceptanceStatus.READY, (
        HostResolution("q1", ResolutionDecision.RESOLVED, "host-universe", (dataset,)),
        HostResolution("q1.c1", ResolutionDecision.RESOLVED, "host-topic",
                       resolved if type(resolved) is tuple else (resolved,)),
    ))


def test_ready_request_requires_exact_host_dataset_binding():
    with pytest.raises(ProjectionError, match="REQUEST_DATASET_BINDING"):
        project_accepted(_acceptance(dataset="contracts"), reference_instant=NOW)
    accepted = _acceptance()
    without_request = InterpretationAcceptance(
        accepted.proposal, AcceptanceStatus.READY, accepted.resolutions[1:],
    )
    with pytest.raises(ProjectionError, match="MISSING_HOST_RESOLUTION:q1"):
        project_accepted(without_request, reference_instant=NOW)
    with pytest.raises(ProjectionError, match="UNIVERSE_DATASET_CONFLICT"):
        project_accepted(_acceptance(dataset="p6dx-8zbt"), reference_instant=NOW)


def test_query_and_count_have_exact_same_where_and_condition_provenance():
    projected = project_accepted(_acceptance(), reference_instant=NOW)
    assert [call.operation for call in projected.calls] == ["query", "count"]
    assert len(projected.manifest) == 2
    assert projected.manifest[0].clauses == projected.manifest[1].clauses
    assert any("q1.c1" in clause.condition_ids for clause in projected.manifest[0].clauses)
    assert all(clause.condition_ids or clause.policy_ids
               for call in projected.manifest for clause in call.clauses)
    assert all(call.plan_hash for call in projected.manifest)
    assert all(policy.policy_ref == "contract-universe@contract-universe-1.1"
               for call in projected.manifest for policy in call.policies)


def test_terminal_acceptance_emits_no_projection_or_manifest():
    terminal = InterpretationAcceptance(None, AcceptanceStatus.PROVIDER_ERROR, reason="timeout")
    result = project_accepted(terminal, reference_instant=NOW)
    assert result.calls == result.manifest == ()
    assert result.status is AcceptanceStatus.PROVIDER_ERROR


@given(st.integers(min_value=1, max_value=10**12))
def test_strict_money_comparators_keep_boundary_exclusive(boundary: int):
    builder = SoQLBuilder()
    with pytest.raises(UnsafeGlobalQueryError):
        builder.build(builder.CONTRATOS_DATASET, {"valor_max_exclusive": boundary})
    params = {"objeto": ["aseo"], "valor_max_exclusive": boundary}
    query = builder.build(builder.CONTRATOS_DATASET, params)
    assert f"valor_del_contrato < {boundary}" in query
    assert f"valor_del_contrato <= {boundary}" not in query
    with pytest.raises(ValueError, match="CONFLICTING_VALUE_BOUND"):
        builder.build(builder.CONTRATOS_DATASET, {
            **params, "valor_max": boundary,
        })


def test_time_only_scope_requires_host_flag_and_bounded_exclusive_interval():
    builder = SoQLBuilder()
    params = {"fecha_desde": "2025-01-01", "fecha_hasta_exclusive": "2026-01-01"}
    with pytest.raises(UnsafeGlobalQueryError):
        builder.build(builder.CONTRATOS_DATASET, params)
    query = builder.build(builder.CONTRATOS_DATASET, params, allow_time_only=True)
    assert "fecha_de_firma >= '2025-01-01'" in query
    assert "fecha_de_firma < '2026-01-01'" in query
    with pytest.raises(ValueError, match="INVALID_TIME_ONLY_SCOPE"):
        builder.build(builder.CONTRATOS_DATASET, {
            "fecha_desde": "2025-01-01", "fecha_hasta_exclusive": "2027-01-01",
        }, allow_time_only=True)


def test_opt_in_builder_trace_refuses_anonymous_predicates():
    builder = SoQLBuilder()
    with pytest.raises(ValueError, match="UNATTRIBUTED_WHERE_CLAUSE:objeto"):
        builder.build(builder.PROCESOS_DATASET, {"objeto": ["aseo"]},
                      clause_origins={"unused": ClauseOrigin(("q1.c1",))},
                      clause_receipts=[])


def test_host_policy_without_any_clause_or_disposition_is_rejected():
    accepted = _acceptance()
    orphan = InterpretationAcceptance(
        accepted.proposal, accepted.status, accepted.resolutions,
        (HostPolicy("unused", "1", ("q1.c1",), Effect.EQUIVALENT),),
    )
    with pytest.raises(ProjectionError, match="ORPHAN_HOST_POLICY"):
        project_accepted(orphan, reference_instant=NOW)


def _token_condition(polarity=Polarity.POSITIVE):
    return ProposedCondition(
        "q1.c1", ("q1",), SemanticDimension.TOPIC, "whole_token_folded", polarity,
        "aseo", (RawSpan(19, 23, "aseo"),),
    )


@pytest.mark.parametrize("dataset,columns", [
    ("jbjy-vk9h", ("objeto_del_contrato", "descripcion_del_proceso")),
    ("p6dx-8zbt", ("nombre_del_procedimiento", "descripci_n_del_procedimiento")),
])
def test_whole_unicode_token_is_a_declared_candidate_superset_plus_local_filter(dataset, columns):
    accepted = _acceptance(dataset=dataset, condition=_token_condition(), resolved="obras")
    if dataset == "p6dx-8zbt":
        proposal = replace(accepted.proposal, requests=(replace(
            accepted.proposal.requests[0], subject=Subject.PROCESSES),))
        accepted = replace(accepted, proposal=proposal)
    outcome = project_accepted(accepted, reference_instant=NOW)
    assert outcome.status is AcceptanceStatus.READY
    query, count = outcome.manifest
    for manifest in (query, count):
        assert manifest.local_filter is not None
        assert manifest.local_filter.condition_id == "q1.c1"
        assert manifest.local_filter.columns == columns
        assert manifest.local_filter.tokens == ("OBRA", "OBRAS")
    # The source only receives the folded superset; it never stands in for the token meaning.
    superset = [c for c in query.clauses if c.condition_ids == ("q1.c1",)]
    assert [c.clause for c in superset] == [
        "(" + " OR ".join(f"unaccent(UPPER({c})) LIKE unaccent(UPPER('%OBRA%'))" for c in columns) + ")"
    ]
    assert query.soql.endswith("LIMIT 1000") and "ORDER BY" in query.soql
    assert query.clauses == count.clauses


def test_local_filter_is_bound_into_the_plan_hash():
    from app.core.foundation_projection import _digest

    outcome = project_accepted(_acceptance(condition=_token_condition(), resolved="obra"),
                               reference_instant=NOW)
    for manifest in outcome.manifest:
        payload = {
            "request_id": manifest.request_id, "scope_id": manifest.scope_id,
            "operation": manifest.operation, "dataset": manifest.dataset, "soql": manifest.soql,
            "clauses": [{"clause": c.clause, "conditions": c.condition_ids, "policies": c.policy_ids}
                        for c in manifest.clauses],
            "policies": [(p.policy_ref, p.effect.value) for p in manifest.policies],
        }
        assert manifest.plan_hash != _digest(payload)
        assert manifest.plan_hash == _digest({**payload, "local_filter": manifest.local_filter.receipt()})
    plain = project_accepted(_acceptance(), reference_instant=NOW)
    assert all(m.local_filter is None for m in plain.manifest)


@pytest.mark.parametrize("resolved,polarity,reason", [
    ("aseo", Polarity.POSITIVE, "TOKEN_SET_NOT_ACCREDITED"),
    ("cobra", Polarity.POSITIVE, "TOKEN_SET_NOT_ACCREDITED"),
    ("obra", Polarity.NEGATIVE, "TOKEN_NEGATION_NOT_ACCREDITED"),
])
def test_unapproved_or_negated_token_meaning_fails_closed_without_calls(resolved, polarity, reason):
    outcome = project_accepted(_acceptance(condition=_token_condition(polarity), resolved=resolved),
                               reference_instant=NOW)
    assert outcome.status is AcceptanceStatus.UNSUPPORTED
    assert outcome.reason == reason
    assert outcome.calls == outcome.manifest == ()


def test_token_condition_without_host_resolution_is_a_projection_error_not_unsupported():
    accepted = _acceptance(condition=_token_condition(), resolved="obra")
    missing = replace(accepted, resolutions=accepted.resolutions[:1])
    with pytest.raises(ProjectionError, match="MISSING_HOST_RESOLUTION"):
        project_accepted(missing, reference_instant=NOW)


def test_family_modalities_require_complete_accredited_resolution_and_version():
    from app.core.foundation_projection import _modality_condition

    condition = ProposedCondition(
        "q1.c1", ("q1",), SemanticDimension.MODALITY, "family", Polarity.POSITIVE,
        "aseo", (RawSpan(19, 23, "aseo"),),
    )
    ids = ("licitacion_publica", "licitacion_publica_obra")
    accepted = _acceptance(condition=condition, resolved=ids)
    complete = InterpretationAcceptance(
        accepted.proposal, accepted.status, accepted.resolutions,
        (HostPolicy("modality-lp-family", "1", ("q1.c1",), Effect.EQUIVALENT),),
    )
    assert _modality_condition(complete, "jbjy-vk9h", condition) == (list(ids), "modality-lp-family@1")
    with pytest.raises(ProjectionError, match="MISSING_HOST_POLICY"):
        _modality_condition(accepted, "jbjy-vk9h", condition)
    incomplete = _acceptance(condition=condition, resolved="licitacion_publica")
    with pytest.raises(ProjectionError, match="UNACCREDITED_MODALITY_FAMILY"):
        _modality_condition(incomplete, "jbjy-vk9h", condition)


@pytest.mark.parametrize("ids", [
    ("licitacion_publica", "licitacion_publica_obra", "acuerdo_marco_precio_secop"),
    ("contratacion_directa", "regimen_especial"),
])
def test_named_family_never_inherits_agreement_framework_or_special_regime(ids):
    from app.core.foundation_projection import _modality_condition

    condition = ProposedCondition(
        "q1.c1", ("q1",), SemanticDimension.MODALITY, "family", Polarity.POSITIVE,
        "aseo", (RawSpan(19, 23, "aseo"),),
    )
    with pytest.raises(ProjectionError, match="UNACCREDITED_MODALITY_FAMILY"):
        _modality_condition(_acceptance(condition=condition, resolved=ids), "p6dx-8zbt", condition)


def test_conjunctive_topics_preserve_each_clause_without_recall_or_demoting_terms():
    accepted = _acceptance()
    conditions = tuple(replace(accepted.proposal.conditions[0], condition_id=f"q1.c{i}")
                       for i in range(4))
    expression = ConditionExpression(LogicOperator.AND, children=tuple(
        ConditionExpression(LogicOperator.ATOM, c.condition_id) for c in conditions
    ))
    proposal = replace(accepted.proposal, conditions=conditions, requests=(replace(
        accepted.proposal.requests[0], condition_expression=expression,
    ),))
    terms = ("aseo", "mantenimiento vial", "obra hidráulica", "convocatoria cultural")
    resolutions = (accepted.resolutions[0],) + tuple(
        replace(accepted.resolutions[1], target_id=c.condition_id, resolved_ids=(term,))
        for c, term in zip(conditions, terms)
    )
    projected = project_accepted(InterpretationAcceptance(
        proposal, accepted.status, resolutions,
    ), reference_instant=NOW)
    for manifest in projected.manifest:
        topic_clauses = [c for c in manifest.clauses if c.condition_ids]
        assert len(topic_clauses) == 4
        assert [c.condition_ids for c in topic_clauses] == [(c.condition_id,) for c in conditions]
        assert all(clause.clause.count(" LIKE ") == 2 for clause in topic_clauses)
        assert all("coalesce" not in clause.clause for clause in topic_clauses)


def test_condition_and_request_identifiers_do_not_select_a_special_projection():
    accepted = _acceptance()
    original = project_accepted(accepted, reference_instant=NOW)
    cid, rid = "arbitrary-obligation", "arbitrary-request"
    request = replace(
        accepted.proposal.requests[0], request_id=rid, scope_id=rid,
        condition_expression=ConditionExpression(LogicOperator.ATOM, cid),
    )
    condition = replace(accepted.proposal.conditions[0], condition_id=cid, request_ids=(rid,))
    proposal = with_requests(accepted.proposal, (request,), conditions=(condition,))
    resolutions = tuple(replace(item, target_id=rid if item.target_id == "q1" else cid)
                        for item in accepted.resolutions)
    renamed = project_accepted(InterpretationAcceptance(proposal, accepted.status, resolutions),
                               reference_instant=NOW)
    assert [call.query for call in renamed.calls] == [call.query for call in original.calls]
    assert all(call.request_ids == (rid,) for call in renamed.calls)
    assert all(cid in clause.condition_ids for manifest in renamed.manifest
               for clause in manifest.clauses if clause.condition_ids)


def test_contract_type_uses_the_host_canonical_class_and_traces_its_clause():
    condition = ProposedCondition(
        "q1.c1", ("q1",), SemanticDimension.CONTRACT_TYPE_OR_NATURE, "is", Polarity.POSITIVE,
        "aseo", (RawSpan(19, 23, "aseo"),),
    )
    projected = project_accepted(_acceptance(condition=condition, resolved="obra"), reference_instant=NOW)
    for manifest in projected.manifest:
        assert any("tipo_de_contrato = 'Obra'" in c.clause and c.condition_ids == ("q1.c1",)
                   for c in manifest.clauses)
        assert "modalidad_de_contratacion IN" not in manifest.soql


def test_admitted_linguistic_universe_is_not_reclassified_by_its_raw_quote():
    raw = "Busco alguna licitación de aseo"
    quote = "alguna licitación"
    universe = RawSpan(raw.index(quote), raw.index(quote) + len(quote), quote)
    topic = RawSpan(raw.index("aseo"), len(raw), "aseo")
    conditions = (
        ProposedCondition("u", ("r",), SemanticDimension.RETRIEVAL_INTENT, "is", Polarity.POSITIVE,
                          quote, (universe,), role=ConditionRole.REQUESTED_UNIVERSE),
        ProposedCondition("t", ("r",), SemanticDimension.TOPIC, "contains", Polarity.POSITIVE,
                          "aseo", (topic,)),
    )
    proposal = InterpretationProposal(
        hashlib.sha256(raw.encode()).hexdigest(), "test", "1", (ProposedRequest(
            "r", "r", Subject.PROCESSES, (RawSpan(0, len(raw), raw),),
            universe_claim=quote, universe_evidence=universe,
            condition_expression=ConditionExpression(LogicOperator.AND, children=(
                ConditionExpression(LogicOperator.ATOM, "u"),
                ConditionExpression(LogicOperator.ATOM, "t"),
            )),
        ),), conditions, jobs=(ProposedJob("job-search", Operation.SEARCH, ("r",)),),
    )
    proposal.validate_raw(raw)
    accepted = InterpretationAcceptance(proposal, AcceptanceStatus.READY, (
        HostResolution("r", ResolutionDecision.RESOLVED, "host-universe", ("p6dx-8zbt",)),
        HostResolution("t", ResolutionDecision.RESOLVED, "host-topic", ("aseo",)),
    ), (HostPolicy("O1B", "1", ("u",), Effect.EQUIVALENT),))
    outcome = project_accepted(accepted, reference_instant=NOW)
    assert outcome.status is AcceptanceStatus.READY
    assert all(call.dataset == "p6dx-8zbt" for call in outcome.calls)
    assert all("modalidad_de_contratacion IN" not in call.query for call in outcome.calls)


def test_builder_scope_refusal_is_a_structured_projection_error():
    condition = ProposedCondition(
        "q1.c1", ("q1",), SemanticDimension.VALUE, "greater_than", Polarity.POSITIVE,
        "aseo", (RawSpan(19, 23, "aseo"),),
    )
    with pytest.raises(ProjectionError, match="SOQL_PROJECTION_BLOCKED"):
        project_accepted(_acceptance(condition=condition, resolved="1000000"), reference_instant=NOW)


def test_common_source_select_does_not_invent_legacy_display_columns_or_null_values():
    outcome = project_accepted(_acceptance(), reference_instant=NOW)
    query, count = outcome.manifest
    selected = query.soql.split(" WHERE ", 1)[0]
    assert "id_contrato" in selected and "fecha_de_firma" in selected
    assert "valor_del_contrato" in selected and "justificacion_modalidad_de" in selected
    assert not any(name in selected for name in (
        "urlproceso", "valor_facturado", "proveedor_adjudicado", "documento_proveedor", "NULL",
    ))
    assert count.soql.startswith("SELECT count(*) WHERE ")
    assert query.clauses == count.clauses
