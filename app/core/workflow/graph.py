"""Burr ApplicationBuilder for the normal-flow pipeline.

What this module owns
---------------------
The DAG of @action nodes and their transitions: parse_query →
apply_context → llm_parse → resolve_actor_name → plan_query → resolve_entities →
build_query → execute_query → degrade_query → observe_universe →
suggester_action → format_response (with clarify_query as the escape
hatch on any branch that sets needs_clarification).

Imports the action functions directly from ``workflow/actions/*`` —
``orchestrator.py`` is no longer in the import chain (Paso 5F broke the
last coupling).

Why a function, not a class
---------------------------
Burr's Application is request-scoped — it carries the initial State of
*this particular* run_query call. A fresh Application is built per
request, so a plain function with all the per-request inputs as
keyword arguments is the right shape.
"""
from __future__ import annotations

from datetime import date as Date
from typing import TYPE_CHECKING

from burr.core import ApplicationBuilder, default, expr

from app.core.temporal_semantics import civil_today
from app.core.workflow.actions.execution import (
    degrade_query,
    execute_query,
    observe_universe,
)
from app.core.workflow.actions.actor_resolution import resolve_actor_name
from app.core.workflow.actions.parsing import (
    apply_context,
    llm_parse,
    parse_query,
)
from app.core.workflow.actions.planning import (
    build_query,
    plan_query,
    resolve_entities,
)
from app.core.workflow.actions.ranking import rank_results
from app.core.workflow.actions.rendering import (
    clarify_query,
    format_response,
    suggester_action,
)

if TYPE_CHECKING:
    from burr.core import Application

    from app.core.workflow.deps import WorkflowDeps


def build_application(
    deps: "WorkflowDeps",
    *,
    user_query: str,
    channel: str,
    followup: bool,
    context_params: dict,
    today: Date | None = None,
) -> "Application":
    """Construct the per-request Burr Application.

    Parameters mirror what ``run_query`` currently inlines:

    - ``deps``: source of the collaborators each action binds against.
    - ``user_query``, ``channel``: request inputs surfaced in initial State.
    - ``followup``: drives the parse_query → apply_context transition.
    - ``context_params``: previous-turn parsed_params for follow-up merging.
    """
    return (
        ApplicationBuilder()
        .with_actions(
            parse_query=parse_query.bind(query_router=deps.query_router),
            apply_context=apply_context,
            llm_parse=llm_parse.bind(llm_handler=deps.llm_handler),
            resolve_actor_name=resolve_actor_name.bind(
                actor_name_resolver=deps.actor_name_resolver,
            ),
            plan_query=plan_query,
            resolve_entities=resolve_entities.bind(entity_resolver=deps.entity_resolver),
            build_query=build_query.bind(soql_builder=deps.soql_builder),
            execute_query=execute_query.bind(
                secop_client=deps.secop_client, soql_builder=deps.soql_builder,
                capability_binding=getattr(deps, "capability_binding", None),
            ),
            degrade_query=degrade_query.bind(
                secop_client=deps.secop_client, soql_builder=deps.soql_builder,
            ),
            observe_universe=observe_universe.bind(
                secop_client=deps.secop_client, soql_builder=deps.soql_builder,
            ),
            suggester_action=suggester_action,
            rank_results=rank_results,
            format_response=format_response.bind(formatter=deps.formatter),
            clarify_query=clarify_query,
        )
        .with_transitions(
            ("parse_query", "apply_context", expr("followup")),
            ("parse_query", "llm_parse", expr("not followup and needs_llm")),
            ("parse_query", "resolve_actor_name", expr("not followup and not needs_llm")),
            ("apply_context", "clarify_query", expr("needs_clarification")),
            ("apply_context", "llm_parse", expr("not needs_clarification and needs_llm")),
            ("apply_context", "resolve_actor_name", expr("not needs_clarification and not needs_llm")),
            ("llm_parse", "resolve_actor_name", default),
            ("resolve_actor_name", "plan_query", default),
            ("plan_query", "resolve_entities", default),
            ("resolve_entities", "clarify_query", expr("needs_clarification")),
            ("resolve_entities", "build_query", expr("not needs_clarification")),
            ("build_query", "execute_query", default),
            ("execute_query", "clarify_query", expr("needs_clarification")),
            ("execute_query", "degrade_query", default),
            ("degrade_query", "clarify_query", expr("needs_clarification")),
            ("degrade_query", "observe_universe", expr("not needs_clarification")),
            ("observe_universe", "suggester_action", default),
            ("suggester_action", "rank_results", default),
            ("rank_results", "format_response", default),
        )
        .with_state(
            user_query=user_query,
            raw_query=user_query,
            today=civil_today(today),
            channel=channel,
            parsed_params={},
            resolved_params={},
            results=[],
            ranked_results=[],
            formatted_rows=[],
            formatted_response="",
            soql_query="",
            dataset_id="",
            route_reason="",
            query_error="",
            query_plan={},
            should_execute_decision=True,
            needs_llm=False,
            needs_clarification=False,
            clarification_reason="",
            total_count=0,
            degraded=False,
            degraded_hint="",
            # H4 — fallback degradado sin ORDER BY (defaults presentacionales).
            result_order_guarantee="exact_order",
            fallback_attempted=False,
            fallback_used=False,
            fallback_strategy="",
            followup=followup,
            context_params=context_params,
            universe_insights=None,
            suggestions=[],
            timeout_suggestions=[],
            risk_flag_timeout=False,
            intent_type="",
            followup_intent_type="",
            followup_mutation={},
            analytical_response="",
            analytical_intent="",
            analytic_controls=[],
            material_residual=[],
            analytic_limitation="",
            entity_profile_view=None,
            timings_ms={},
        )
        .with_entrypoint("parse_query")
        .build()
    )
