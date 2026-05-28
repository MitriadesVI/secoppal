"""Burr ApplicationBuilder for the normal-flow pipeline.

What this module owns
---------------------
The DAG of @action nodes and their transitions: parse_query →
apply_context → llm_parse → plan_query → resolve_entities →
build_query → execute_query → degrade_query → observe_universe →
suggester_action → format_response (with clarify_query as the escape
hatch on any branch that sets needs_clarification).

What it does NOT own
--------------------
The actions themselves still live in ``app/core/orchestrator.py``.
They will be moved into ``app/core/workflow/actions/`` in Paso 5; until
then ``build_application`` imports them lazily (inside the function
body) so that this module loads before the actions are defined without
triggering a circular import at module-load time.

Why a function, not a class
---------------------------
Burr's Application is request-scoped — it carries the initial State of
*this particular* run_query call. A fresh Application is built per
request, so a plain function with all the per-request inputs as
keyword arguments is the right shape.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from burr.core import ApplicationBuilder, default, expr

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
) -> "Application":
    """Construct the per-request Burr Application.

    Parameters mirror what ``run_query`` currently inlines:

    - ``deps``: source of the collaborators each action binds against.
    - ``user_query``, ``channel``: request inputs surfaced in initial State.
    - ``followup``: drives the parse_query → apply_context transition.
    - ``context_params``: previous-turn parsed_params for follow-up merging.
    """
    # Lazy import to avoid a circular dependency with orchestrator.py
    # where these actions still live (Paso 5 will move them under
    # workflow/actions/ and this import will become direct).
    from app.core.orchestrator import (
        apply_context,
        build_query,
        clarify_query,
        degrade_query,
        execute_query,
        format_response,
        llm_parse,
        observe_universe,
        parse_query,
        plan_query,
        resolve_entities,
        suggester_action,
    )

    return (
        ApplicationBuilder()
        .with_actions(
            parse_query=parse_query.bind(query_router=deps.query_router),
            apply_context=apply_context,
            llm_parse=llm_parse.bind(llm_handler=deps.llm_handler),
            plan_query=plan_query,
            resolve_entities=resolve_entities.bind(entity_resolver=deps.entity_resolver),
            build_query=build_query.bind(soql_builder=deps.soql_builder),
            execute_query=execute_query.bind(
                secop_client=deps.secop_client, soql_builder=deps.soql_builder,
            ),
            degrade_query=degrade_query.bind(
                secop_client=deps.secop_client, soql_builder=deps.soql_builder,
            ),
            observe_universe=observe_universe.bind(
                secop_client=deps.secop_client, soql_builder=deps.soql_builder,
            ),
            suggester_action=suggester_action,
            format_response=format_response.bind(formatter=deps.formatter),
            clarify_query=clarify_query,
        )
        .with_transitions(
            ("parse_query", "apply_context", expr("followup")),
            ("parse_query", "llm_parse", expr("not followup and needs_llm")),
            ("parse_query", "plan_query", expr("not followup and not needs_llm")),
            ("apply_context", "clarify_query", expr("needs_clarification")),
            ("apply_context", "llm_parse", expr("not needs_clarification and needs_llm")),
            ("apply_context", "plan_query", expr("not needs_clarification and not needs_llm")),
            ("llm_parse", "plan_query", default),
            ("plan_query", "resolve_entities", default),
            ("resolve_entities", "clarify_query", expr("needs_clarification")),
            ("resolve_entities", "build_query", expr("not needs_clarification")),
            ("build_query", "execute_query", default),
            ("execute_query", "clarify_query", expr("needs_clarification")),
            ("execute_query", "degrade_query", default),
            ("degrade_query", "clarify_query", expr("needs_clarification")),
            ("degrade_query", "observe_universe", expr("not needs_clarification")),
            ("observe_universe", "suggester_action", default),
            ("suggester_action", "format_response", default),
        )
        .with_state(
            user_query=user_query,
            raw_query=user_query,
            channel=channel,
            parsed_params={},
            resolved_params={},
            results=[],
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
            followup=followup,
            context_params=context_params,
            universe_insights=None,
            suggestions=[],
            timeout_suggestions=[],
            risk_flag_timeout=False,
            intent_type="",
            followup_intent_type="",
            analytical_response="",
            analytical_intent="",
            timings_ms={},
        )
        .with_entrypoint("parse_query")
        .build()
    )
