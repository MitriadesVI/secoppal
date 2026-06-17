"""Planning actions — parsed_params → resolved_params → soql_query.

Three Burr nodes that turn the structured parameter dict into a
ready-to-execute SoQL query:

- ``plan_query``        — compute QueryPlan (block / fail-loud / proceed).
- ``resolve_entities``  — fuzzy/LIKE for entities the parser scan missed,
                         plus Entity Ecosystem fan-out.
- ``build_query``       — assemble the SoQL string via SoQLBuilder.

Moved here verbatim from ``app/core/orchestrator.py`` in Paso 5C. No
behavior changes; the @action decorators, ``reads``/``writes`` and
bodies are byte-for-byte identical.

Owns ``ENABLED_RULES_SPRINT_2A``
--------------------------------
This module-level constant gates which Sprint-2A rules ``plan_query``
activates. It used to live in orchestrator.py; moving it here breaks the
orchestrator → planning coupling the Paso 5 mandate is trying to remove.
The default ``None`` means "all rules active" (B4.5).
"""
from __future__ import annotations

import time
from dataclasses import asdict
from datetime import date

from burr.core import State, action

from app.core.entity_resolver import EntityResolver
from app.core.query_plan import compute_query_plan
from app.core.response_policy import build_clarification_response
from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError
from app.core.workflow.helpers import _with_timing


# ── Sprint 2A rule activation — expanded per sub-block B4.x ──────────────
# B4.1: {\"P3\", \"P9\"}  → B4.2: +P4  → B4.3: +P5+P6+P7  → B4.4: all  → B4.5: None
ENABLED_RULES_SPRINT_2A: set[str] | None = None  # B4.5: None = all rules (default)


@action(
    reads=["parsed_params", "raw_query", "prev_params", "turn_index"],
    writes=["query_plan", "parsed_params", "should_execute_decision"],
)
def plan_query(state: State) -> State:
    """Compute QueryPlan. If blocked, set formatted_response directly for clarify path."""
    params = state.get("parsed_params", {})
    raw = state.get("raw_query", "")
    context = {
        "prev_params": state.get("prev_params"),
        "turn_index": state.get("turn_index", 0),
        "today": date.today(),
    }
    plan = compute_query_plan(params, raw, context, enabled_rules=ENABLED_RULES_SPRINT_2A)
    return state.update(
        query_plan=asdict(plan),
        parsed_params=plan.params,
        should_execute_decision=plan.should_execute,
    )


@action(
    reads=["parsed_params", "timings_ms"],
    writes=["resolved_params", "dataset_id", "needs_clarification", "clarification_reason", "formatted_response", "formatted_rows", "timings_ms"],
)
def resolve_entities(state: State, entity_resolver: EntityResolver) -> State:
    """
    Resolve entities to SECOP-compatible values.
    If gazetteer scan already resolved in parse_query, just pass through.
    Only does fuzzy/LIKE for entities the scan missed (e.g. from LLM).

    B4.4: if plan_query blocked execution, pass through to clarify_query.
    """
    # ── Plan guard: if QueryPlan blocked, route to clarification ───────
    qp = state.get("query_plan", {}) or {}
    if not qp.get("should_execute", True):
        cr = build_clarification_response(qp)
        # Persist the (post-rules) parsed params as resolved_params so the
        # clarified turn's scope survives into the next turn. Without this
        # the blocked frame is stored empty and a follow-up like
        # "mantenimiento" loses the territory we already understood.
        return state.update(
            formatted_response=cr["response"],
            formatted_rows=[],
            needs_clarification=True,
            clarification_reason=qp.get("reason", ""),
            resolved_params=dict(state.get("parsed_params", {})),
        )

    start = time.perf_counter()
    params = dict(state["parsed_params"])
    resolved = dict(params)
    resolved.pop("dataset_explicit", None)  # meta-field, not for SECOP
    needs_clarification = False
    clarification_reason = ""

    # ── G7C: Entity Ecosystem detection (after parse, before entity resolution) ──
    from app.core.entity_ecosystem_router import (
        resolve_entity_ecosystem, build_entity_ecosystem_filter
    )
    raw_query = state.get("user_query", "")
    ecosystem_match = resolve_entity_ecosystem(raw_query)
    if ecosystem_match:
        ecosystem_filter = build_entity_ecosystem_filter(ecosystem_match)
        resolved.update(ecosystem_filter)
        # If ecosystem provides department, don't override if already resolved
        dept_from_ecosystem = ecosystem_match.department
        if dept_from_ecosystem and not resolved.get("departamento_resolved"):
            resolved["departamento_resolved"] = dept_from_ecosystem
        # Mark entity as "covered by ecosystem" so normal entity resolver skips
        resolved["entidad_ecosystem_covered"] = True
        # G7C.1: scrub institutional tokens from objeto so they don't become topic filters
        from app.core.entity_ecosystem_router import scrub_institutional_tokens_from_objeto
        if resolved.get("objeto"):
            resolved["objeto"] = scrub_institutional_tokens_from_objeto(
                resolved["objeto"], ecosystem_match.family
            )

    # Department: only resolve if not already resolved by scan or ecosystem
    if params.get("departamento") and not params.get("departamento_resolved"):
        resolution = entity_resolver.resolve_departamento(str(params["departamento"]))
        resolved["departamento_resolution"] = resolution.to_dict()
        if resolution.value:
            resolved["departamento_resolved"] = resolution.value
        else:
            needs_clarification = True
            clarification_reason = f"No pude ubicar el departamento '{params['departamento']}'."

    # Entity: only resolve if not already resolved by scan
    if params.get("entidad") and not params.get("entidad_resolved"):
        depto_value = resolved.get("departamento_resolved")
        resolution = entity_resolver.resolve_entidad(str(params["entidad"]), departamento=depto_value)
        resolved["entidad_resolution"] = resolution.to_dict()
        if resolution.value:
            resolved["entidad_resolved"] = resolution.value
        elif resolution.like_value:
            resolved["entidad_like"] = resolution.like_value
        elif resolution.clarification_needed:
            needs_clarification = True
            clarification_reason = (
                resolution.clarification_hint
                or f"No pude ubicar la entidad '{params['entidad']}'."
            )

    dataset_id = SoQLBuilder.dataset_id_for(str(params.get("dataset")))
    return _with_timing(
        state,
        "resolve_ms",
        start,
        resolved_params=resolved,
        dataset_id=dataset_id,
        needs_clarification=needs_clarification,
        clarification_reason=clarification_reason,
    )


@action(reads=["resolved_params", "dataset_id", "timings_ms"], writes=["soql_query", "timings_ms"])
def build_query(state: State, soql_builder: SoQLBuilder) -> State:
    start = time.perf_counter()
    try:
        soql = soql_builder.build(state["dataset_id"], state["resolved_params"])
    except UnsafeGlobalQueryError:
        # SAFE-SOQL-001: el builder rechazó una consulta global accidental.
        # execute_query tiene un guard equivalente (SCOPE_TOPIC_KEYS) que
        # convierte este caso en needs_clarification. Dejamos soql_query
        # vacío para que ese guard sea quien responda al usuario.
        soql = ""
    return _with_timing(state, "build_soql_ms", start, soql_query=soql)
