"""HandlerContext — per-request input for workflow handlers.

Frozen dataclass that bundles everything ``run_query`` receives from its
caller plus a monotonic ``run_started_at`` timestamp used for end-to-end
timing.

Why frozen?
-----------
Handlers in Paso 5 will branch on context fields (``chat_id``,
``seed_resolved_params``) but must never mutate them — accidental
mutation would mean later handlers see a different request than earlier
ones did. Freezing turns those mistakes into immediate ``FrozenInstance``
errors.

Why ``run_started_at`` is mandatory (with default_factory)?
-----------------------------------------------------------
``run_query`` measures total wall time from its first line. If each
handler started its own stopwatch we would have multiple clocks and the
final ``timings_ms.total`` would not match end-to-end latency. A single
authoritative timestamp baked into the context fixes this.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date as Date
from typing import Any

from app.core.temporal_semantics import civil_today


@dataclass(frozen=True)
class HandlerContext:
    """Per-request input passed to every workflow handler.

    Constructed once at the top of ``SecopalWorkflow.run_query``; handed
    to each handler that participates in the chain of responsibility.
    """

    user_query: str
    channel: str = "whatsapp"
    chat_id: str | None = None
    user_identity: dict[str, Any] | None = None
    seed_resolved_params: dict | None = None
    actor_candidate_selection: dict[str, Any] | None = None
    tabular_referent: Any = None
    export_search_params: dict | None = None
    publication_required: bool = False
    today: Date = field(default_factory=civil_today)
    run_started_at: float = field(default_factory=time.perf_counter)
