"""Assemble an Entity Profile from a fixed source-call plan."""
from __future__ import annotations

import time
from typing import Any

from app.core.entity_profile.models import (
    EntityProfile,
    EntityProfileIdentity,
    EntityProfileScope,
    SourceCalls,
)
from app.core.entity_profile.query_runner import execute_jobs, jobs_for_focus
from app.core.entity_profile.sections import build_sections


def build_entity_profile(
    *,
    scope: EntityProfileScope,
    identity: EntityProfileIdentity,
    params: dict,
    focus: str,
    secop_client: Any,
    soql_builder: Any,
) -> EntityProfile:
    """Build one profile; an individual failed call never aborts other sections."""
    started = time.perf_counter()
    jobs = jobs_for_focus(focus)
    rows, calls = execute_jobs(jobs, params, secop_client, soql_builder)
    sections = build_sections(focus, scope, rows, calls, soql_builder)
    warnings = tuple(dict.fromkeys(
        warning
        for section in sections
        for warning in section.warnings
    ))
    ordered_calls = tuple(
        calls[job.call_id]
        for job in jobs
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
        focus=focus,
    )
