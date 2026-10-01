"""First-turn Foundation SEARCH composition through the public workflow (ADR-102).

One profile only: producer B v4.2, mandatory universe_kind and mandatory H7.
"""

from __future__ import annotations

import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from app.core.foundation_admission import admit_proposal
from app.core.foundation_messages import user_message
from app.core.foundation_presentation import export_fields, group_cards, response_text
from app.core.foundation_route_owner import FOUNDATION_TURN_MARK
from app.core.foundation_guard import ExecutionGuard, FoundationGuardError, execution_guard
from app.core.foundation_projection import ProjectionError, project_accepted
from app.core.foundation_token_plan import select_members
from app.core.foundation_universe_policy import apply_universe_policy
from app.core.interpretation_contract import AcceptanceStatus, InterpretationAcceptance
from app.core.socrata_transport import Soda3OrderingSpecifier
from app.core.source_call_receipts import last_call_id, source_call_scope, source_calls_snapshot
from app.core.turn_ledger import AdmissionCode, declined_by, record_admission, record_production
from app.core.workflow.results import empty_result
from app.utils.money import normalize_text


def first_search_ineligibility(workflow, ctx, seed) -> AdmissionCode | None:
    """Why a turn is not an eligible first search; None when it is. Same gate, with its reason."""
    if (seed is not None or ctx.actor_candidate_selection is not None
            or ctx.profile_action_selection is not None):
        return AdmissionCode.INELIGIBLE_SEED_OR_SELECTION
    if ctx.chat_id and workflow.conv_store.get_history(ctx.chat_id, last_n=1):
        return AdmissionCode.INELIGIBLE_HISTORY
    raw = normalize_text(ctx.user_query)
    nouns = r"(?:procesos?|contratos?|oportunidades?|licitacion(?:es)?)"
    if re.match(r"cuenta\b", raw) and re.search(r"\bsuma\b", raw) and re.search(rf"\b{nouns}\b", raw):
        return None  # Admit the compound request so its unsupported capacity blocks before calls.
    if (re.match(rf"(?:busca(?:r)?|muestra(?:me)?|quiero|necesito|que|algun[oa]?|{nouns})\b", raw)
            and re.search(rf"\b{nouns}\b", raw)):
        return None
    return AdmissionCode.INELIGIBLE_LEXICAL


def eligible_first_search(workflow, ctx, seed) -> bool:
    """Fix route eligibility before producing or observing a candidate answer."""
    return first_search_ineligibility(workflow, ctx, seed) is None


def host_universe(proposal):
    """ADR-101/102: the host's universe decisions (H7). Nothing is ever added to the request."""
    return apply_universe_policy(proposal)


def _scoped_call(guard, client, manifest, *, reason, parent=None, fallback=False):
    with guard.permit(manifest, reason=reason, parent_call_id=parent, fallback=fallback) as soql:
        with source_call_scope(request_id=manifest.request_id, scope_id=manifest.scope_id,
                               reason=reason, parent_call_id=parent,
                               interpretation_hash=manifest.interpretation_hash,
                               plan_hash=manifest.plan_hash):
            if manifest.operation == "count":
                return client.count(manifest.dataset, soql)
            ordering = Soda3OrderingSpecifier.NO_PRESERVE if fallback else Soda3OrderingSpecifier.TOTAL
            return client.query(manifest.dataset, soql, ordering_specifier=ordering)


def execute_accepted(workflow, ctx, accepted: InterpretationAcceptance, *, reference_instant: datetime,
                     universe_policy=None) -> dict:
    """Project once, enforce the host protocol, and publish actual source results."""
    try:
        outcome = project_accepted(accepted, reference_instant=reference_instant, builder=workflow.soql_builder)
    except (ProjectionError, ValueError) as exc:
        message = user_message("UNSUPPORTED", str(exc), accepted.proposal)
        result = empty_result(response=message.text, route_reason="foundation_unsupported",
                              needs_clarification=True, total_count=None)
        result.update(state="unsupported", groups=[], foundation_mode="foundation_v3",
                      foundation_plan_decision="UNSUPPORTED", foundation_terminal="UNSUPPORTED",
                      foundation_internal_reason=str(exc), foundation_choices=list(message.choices),
                      total_quality="not_applicable")
        if universe_policy is not None:
            result["foundation_universe"] = universe_policy.to_dict()
        return result
    result = empty_result(response="", route_reason=f"foundation_{outcome.status.value.lower()}"
                          if outcome.status is not AcceptanceStatus.READY else "foundation_search",
                          needs_clarification=outcome.status is AcceptanceStatus.NEEDS_CLARIFICATION,
                          total_count=None)
    result.update(foundation_mode="foundation_v3", foundation_plan_decision=outcome.status.value,
                  foundation_terminal=outcome.status.value, groups=[],
                  foundation_acceptance=accepted.to_dict(), foundation_plan=[{
                      "request_id": m.request_id, "scope_id": m.scope_id, "dataset": m.dataset,
                      "operation": m.operation, "soql": m.soql,
                      "interpretation_hash": m.interpretation_hash, "plan_hash": m.plan_hash,
                  } for m in outcome.manifest])
    if universe_policy is not None:
        result["foundation_universe"] = universe_policy.to_dict()
    if outcome.status is not AcceptanceStatus.READY:
        message = user_message(outcome.status.value, outcome.reason, accepted.proposal)
        result.update(state=outcome.status.value.lower(), response=message.text,
                      foundation_internal_reason=outcome.reason or None,
                      foundation_choices=list(message.choices), total_quality="not_applicable")
        return result
    guard = ExecutionGuard(outcome, ctx.user_query, reference_instant=reference_instant,
                           builder=workflow.soql_builder)
    groups, warnings = [], ["LIMITES_DE_COBERTURA"]
    with execution_guard(guard):
        for query in (m for m in outcome.manifest if m.operation == "query"):
            fallback, source_error, count_error = False, None, None
            try:
                rows = _scoped_call(guard, workflow.secop_client, query, reason="search:query")
            except FoundationGuardError:
                raise
            except Exception as exc:
                guard.query_failure(query.request_id, exc, call_id=last_call_id())
                # An unordered H4 sample cannot bound a token filter's candidates.
                if guard.states[query.request_id].primary_timeout and query.local_filter is None:
                    fallback = True
                    try:
                        rows = _scoped_call(guard, workflow.secop_client, query, reason="h4_fallback:query",
                                            parent=last_call_id(), fallback=True)
                    except FoundationGuardError:
                        raise
                    except Exception as fb_exc:
                        source_error, rows = fb_exc, []
                else:
                    source_error, rows = exc, []
            total, quality = None, "not_applicable"
            if source_error is None and query.local_filter is not None:
                guard.query_result(query.request_id, rows, call_id=last_call_id())
                try:
                    members, complete = select_members(rows, query.local_filter)
                except ValueError as exc:
                    source_error, rows = exc, []
                else:
                    # Truncated candidates still give an exact ordered prefix; only the total is a floor.
                    total, quality = len(members), "exact" if complete else "lower_bound"
                    rows = members[:query.local_filter.page_limit]
                    if not complete:
                        warnings.append("TOKEN_CANDIDATES_TRUNCATED")
            elif source_error is None:
                guard.query_result(query.request_id, rows, call_id=last_call_id())
                limit = int(re.search(r"\bLIMIT\s+(\d+)\s*$", query.soql, re.I)[1])
                total, quality = len(rows), "exact" if len(rows) < limit else "lower_bound"
                if len(rows) >= limit:
                    count = next(m for m in outcome.manifest if m.request_id == query.request_id
                                 and m.operation == "count")
                    try:
                        total = _scoped_call(guard, workflow.secop_client, count, reason="search:count",
                                             parent=last_call_id())
                        quality = "exact"
                    except FoundationGuardError:
                        raise
                    except Exception as exc:
                        count_error = type(exc.__cause__ or exc).__name__
                        warnings.append("COUNT_UNAVAILABLE")
            universe = "processes" if query.dataset == workflow.soql_builder.PROCESOS_DATASET else "contracts"
            id_field = "id_del_proceso" if universe == "processes" else "id_contrato"
            error_type = type(source_error.__cause__ or source_error).__name__ if source_error else None
            technical_reason = "fetch_timeout" if error_type == "TimeoutError" else "source_error" if source_error else None
            groups.append({"request_id": query.request_id, "scope_id": query.scope_id,
                           "dataset": query.dataset, "universe": universe, "rows": rows,
                           "row_ids": [row[id_field] for row in rows], "total": total,
                           "total_quality": quality, "warnings": list(warnings),
                           "state": "source_error" if source_error else ("ok_order_sample" if fallback else "ok"),
                           "technical_reason": technical_reason, "error_type": error_type,
                           "count_error_type": count_error, "fallback_attempted": fallback})
    all_rows = [row for group in groups for row in group["rows"]]
    failed = any(group["state"] == "source_error" for group in groups)
    state = "partial" if failed and all_rows else "source_error" if failed else "ok"
    if not failed and any(group["fallback_attempted"] for group in groups):
        state = "ok_order_sample"
    cards_by_group = group_cards(workflow.formatter, groups, reference_instant=reference_instant)
    result.update(state=state, groups=groups, rows=[card for cards in cards_by_group for card in cards],
                  results=all_rows,
                  foundation_terminal="SOURCE_ERROR" if failed else "READY",
                  dataset_id=groups[0]["dataset"] if len(groups) == 1 else None,
                  dataset_scope=[group["dataset"] for group in groups],
                  total_count=sum(group["total"] for group in groups) if not failed else None,
                  total_count_approximate=any(group["total_quality"] != "exact" for group in groups),
                  result_row_count=len(all_rows), coverage_warning="LIMITES_DE_COBERTURA",
                  warnings=warnings, query_error="SOURCE_ERROR" if failed else "",
                  technical_reason=next((g["technical_reason"] for g in groups if g["technical_reason"]), None),
                  outcome_reason=next((g["technical_reason"] for g in groups if g["technical_reason"]), ""),
                  outcome="SOURCE_ERROR" if failed else "RESULTS" if all_rows else "NO_RESULTS",
                  result_order_guarantee="ORDER_SAMPLE" if state == "ok_order_sample" else "exact",
                  fallback_attempted=any(group["fallback_attempted"] for group in groups))
    result.update(export_fields(groups))
    result["response"] = response_text(groups, cards_by_group, channel=ctx.channel,
                                       limit=workflow.formatter.max_results)
    return result


def _telemetry(producer, accepted, universe, result) -> dict:
    """Dogfood fields; no raw text and no provider payload."""
    from app.core.foundation_producers import ProducerReceipt

    receipt = getattr(producer, "receipt", None)
    receipt = receipt if isinstance(receipt, ProducerReceipt) else None
    decisions = universe.decisions if universe is not None else ()
    telemetry = {
        "foundation_called": True,
        **(receipt.to_dict() if receipt is not None else {"producer_version": None, "prompt_sha256": None,
                                                           "provider_model_reported": None,
                                                           "provider_latency_ms": None,
                                                           "provider_error": None, "provider_calls": None}),
        "proposal_valid": accepted.status not in (AcceptanceStatus.INVALID_PROPOSAL,
                                                  AcceptanceStatus.PROVIDER_ERROR),
        "acceptance_status": accepted.status.value,
        "internal_reason_code": result.get("foundation_internal_reason"),
        "h7_authority": [item.authority for item in decisions],
        "h7_rule_ids": [list(item.rules) for item in decisions],
        "h7_model_disagreement": any(item.disagrees_with_producer for item in decisions),
        "h7_conflict": universe.conflict if universe is not None else None,
        "route": "accepted",
        "source_calls": len(source_calls_snapshot() or ()),
    }
    return telemetry


def _record(workflow, ctx, result: dict) -> dict:
    """One trace per Foundation turn; the history turn carries no legacy params."""
    feedback = getattr(workflow, "feedback", None)
    if feedback is not None:
        result["trace_id"] = feedback.log_trace(ctx.user_query, ctx.channel, result,
                                                user_identity=ctx.user_identity)
    if ctx.chat_id:
        workflow.conv_store.append_turn(
            ctx.chat_id, ctx.user_query, result["response"], {},
            result_ids=[identifier for group in result.get("groups", []) for identifier in group["row_ids"]],
            trace_id=result.get("trace_id", ""), outcome=result.get("outcome", ""),
            outcome_reason=FOUNDATION_TURN_MARK, result_count=len(result.get("rows", [])),
        )
    return result


def _continuation_clarification(workflow, ctx) -> dict:
    record_admission(AdmissionCode.CONTINUATION_BLOCKED, ctx.user_query)
    result = empty_result(
        response=user_message("NEEDS_CLARIFICATION", "FOUNDATION_CONTINUATION_UNSUPPORTED").text,
        route_reason="foundation_continuation_unsupported", needs_clarification=True, total_count=None)
    result.update(foundation_mode="foundation_v3", foundation_terminal="NEEDS_CLARIFICATION",
                  foundation_internal_reason="FOUNDATION_CONTINUATION_UNSUPPORTED",
                  foundation_telemetry={"foundation_called": False, "route": "continuation_blocked",
                                        "internal_reason_code": "FOUNDATION_CONTINUATION_UNSUPPORTED"},
                  state="needs_clarification", groups=[], rows=[], total_quality="not_applicable")
    return _record(workflow, ctx, result)


def run_foundation_search(workflow, ctx, seed, *, observation=None, binding=None) -> dict | None:
    """Claim a first-turn SEARCH that every existing owner declined; otherwise None."""
    if getattr(workflow.settings, "secoppal_foundation_mode", "legacy") != "foundation_v3":
        return None
    from app.core.foundation_route_owner import continues_foundation_turn, decline_reason

    if seed is None and ctx.actor_candidate_selection is None and continues_foundation_turn(workflow, ctx):
        return _continuation_clarification(workflow, ctx)
    ineligible = first_search_ineligibility(workflow, ctx, seed)
    if ineligible is not None:
        record_admission(ineligible, ctx.user_query)
        return None
    owner = decline_reason(workflow, ctx, observation=observation, binding=binding)
    if owner is not None:
        record_admission(declined_by(owner), ctx.user_query)
        return None
    record_admission(AdmissionCode.ADMITTED, ctx.user_query)
    started = time.perf_counter()
    producer = getattr(workflow, "foundation_producer", None)
    if producer is None:
        from app.core.foundation_producers import configured_producer

        producer = configured_producer(workflow)
    accepted = producer(ctx.user_query) if callable(producer) else producer.produce(ctx.user_query)
    if isinstance(accepted, InterpretationAcceptance) and accepted.status is AcceptanceStatus.PROVIDER_ERROR:
        # A real provider outage (never a malformed answer) may run the labeled basic search.
        from app.core.foundation_basic_fallback import run_basic_fallback

        result = run_basic_fallback(workflow, ctx)
        result.update(foundation_mode="foundation_v3", foundation_terminal="PROVIDER_ERROR",
                      foundation_plan_decision="PROVIDER_ERROR", foundation_internal_reason=accepted.reason)
        result["foundation_telemetry"] = {**_telemetry(producer, accepted, None, result), **result["foundation_fallback"],
                                          "foundation_latency_ms": round((time.perf_counter() - started) * 1000, 1)}
        record_production(result["foundation_telemetry"], result, ctx.user_query)
        return _record(workflow, ctx, result)
    universe = None
    if not isinstance(accepted, InterpretationAcceptance):
        # H7 is part of the profile: every admitted proposal crosses it first.
        universe = host_universe(accepted)
        accepted = (InterpretationAcceptance(universe.proposal, AcceptanceStatus.NEEDS_CLARIFICATION,
                                             reason=universe.conflict) if universe.conflict
                    else admit_proposal(universe.proposal, ctx.user_query,
                                        entity_resolver=workflow.entity_resolver))
    instant = getattr(workflow, "foundation_reference_instant", None)
    instant = instant or datetime.now(ZoneInfo("America/Bogota"))
    result = execute_accepted(workflow, ctx, accepted, reference_instant=instant, universe_policy=universe)
    result.setdefault("foundation_internal_reason", accepted.reason or None)
    result["foundation_telemetry"] = _telemetry(producer, accepted, universe, result)
    result["foundation_telemetry"]["foundation_latency_ms"] = round((time.perf_counter() - started) * 1000, 1)
    record_production(result["foundation_telemetry"], result, ctx.user_query)
    return _record(workflow, ctx, result)
