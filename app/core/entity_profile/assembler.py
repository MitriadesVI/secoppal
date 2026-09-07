"""Ensamblar UN perfil a partir de UN plan de ejecución ya validado."""
from __future__ import annotations

from dataclasses import replace
import time
from typing import Any

from app.core.entity_profile.execution_plan import (
    EntityProfileExecutionPlan,
    build_execution_plan,
)
from app.core.entity_profile.models import (
    EntityProfile,
    EntityProfileIdentity,
    EntityProfileScope,
    SourceCalls,
)
from app.core.entity_profile.query_runner import execute_jobs
from app.core.entity_profile.scope import validate_query_scope
from app.core.entity_profile.sections import build_sections
from app.core.entity_profile.semantic_request import (
    EntityProfileSemanticRequest,
    build_semantic_request,
)


def build_entity_profile(
    *,
    scope: EntityProfileScope,
    identity: EntityProfileIdentity,
    params: dict,
    focus: str,
    secop_client: Any,
    soql_builder: Any,
    user_query: str = "",
    plan: EntityProfileExecutionPlan | None = None,
) -> EntityProfile:
    """Build one profile; an individual failed call never aborts other sections.

    ``plan`` es la autoridad. Se acepta ya construido —lo normal, porque el
    turno necesita conocer su aclaración ANTES de consultar la fuente— y en su
    ausencia se deriva aquí de la misma petición semántica, para que no existan
    dos formas de decidir qué se ejecuta.
    """
    started = time.perf_counter()
    validate_query_scope(scope, params)
    if plan is None:
        plan = build_execution_plan(
            _request_from(params, user_query, scope, focus), params,
        )
    rows, calls = execute_jobs(plan.jobs, secop_client, soql_builder)
    sections = build_sections(plan, scope, rows, calls, soql_builder)
    warnings = tuple(dict.fromkeys(
        warning
        for section in sections
        for warning in section.warnings
    ))
    ordered_calls = tuple(
        calls[job.call_id]
        for job in plan.jobs
        if job.call_id in calls
    )
    return EntityProfile(
        scope=scope,
        identity=identity,
        activity=sections[0],
        values=sections[1],
        modalities=sections[2],
        suppliers=sections[3],
        temporal=sections[4],
        recent=sections[5],
        source_calls=SourceCalls(
            calls=ordered_calls,
            total_latency_ms=(time.perf_counter() - started) * 1000,
        ),
        warnings=warnings,
        focus=plan.focus,
        plan=plan,
    )


def _request_from(
    params: dict, user_query: str, scope: EntityProfileScope, focus: str,
) -> EntityProfileSemanticRequest:
    """La petición persistida manda sobre una re-derivación desde el texto.

    Un turno reanudado o recargado trae ``_entity_profile_request`` y ya no
    tiene el texto original; volver a leerlo de un ``user_query`` vacío sería
    una SEGUNDA forma de decidir lo mismo, y las dos podrían discrepar.
    """
    persisted = EntityProfileSemanticRequest.from_dict(
        params.get("_entity_profile_request"),
    )
    if persisted is not None and persisted.focus == focus:
        return persisted
    if persisted is not None:
        return replace(persisted, focus=focus)
    return build_semantic_request(
        user_query=user_query, params=params, scope=scope, focus=focus,
    )
