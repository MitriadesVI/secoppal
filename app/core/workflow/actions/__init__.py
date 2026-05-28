"""Burr @action nodes for the SecopalWorkflow pipeline.

Each submodule groups actions by domain so individual contributors can
work on one concern without scrolling through 1500 lines:

- ``parsing``   — turn user_query → parsed_params (parse_query,
                 llm_parse, apply_context).
- ``planning``  — turn parsed_params → resolved_params → soql_query
                 (plan_query, resolve_entities, build_query).
- ``execution`` — hit SECOP and degrade on failure (execute_query,
                 degrade_query, observe_universe).
- ``rendering`` — turn state → user-facing response (suggester_action,
                 format_response, clarify_query).

Composition lives in ``app/core/workflow/graph.py`` which wires the
actions into a Burr Application. ``__init__`` deliberately has no
re-exports: callers should import from the specific submodule so the
domain split stays visible at the call site.
"""
