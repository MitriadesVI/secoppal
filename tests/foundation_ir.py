"""Builders for IR v2 proposals in tests: the jobs that say «these requests are SEARCH» (C1.2)."""

from __future__ import annotations

from dataclasses import replace

from app.core.foundation_operations import Operation
from app.core.interpretation_contract import ProposedJob


def search_jobs(requests) -> tuple[ProposedJob, ...]:
    """The one SEARCH job that carries every request (the shape ``lift_v1`` gives a v4.2 proposal)."""
    return (ProposedJob("job-search", Operation.SEARCH, tuple(r.request_id for r in requests)),) if requests else ()


def with_requests(proposal, requests, **changes):
    """``replace`` the requests of a proposal and keep its SEARCH job in step."""
    requests = tuple(requests)
    return replace(proposal, requests=requests, jobs=search_jobs(requests), **changes)
