"""Exact execution grants for a host-admitted Foundation SEARCH plan (Foundation side only).

The source-boundary hooks that ``secop_client`` reaches live in the leaf ``app.core.foundation_guard``; this
module holds the ``ExecutionGuard`` they drive. It is NOT re-exported from the leaf: legacy must not reach it.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

from app.core.foundation_projection import ApprovedCallManifest, ProjectionOutcome, project_accepted
from app.core.interpretation_contract import AcceptanceStatus
from app.core.source_call_receipts import current_source_scope


class FoundationGuardError(ValueError):
    """A call cannot cross the source boundary under the accepted plan."""


def _without_order(soql: str) -> str:
    masked = re.sub(r"'(?:[^']|'')*'", lambda m: " " * len(m.group()), soql)
    order = re.search(r"\bORDER\s+BY\b", masked, re.I)
    limit = re.search(r"\bLIMIT\s+(\d+)\s*$", masked, re.I)
    if order is None or limit is None or int(limit[1]) > 50:
        raise FoundationGuardError("H4_REQUIRES_PRESENTATIONAL_ORDER_AND_BOUND")
    return soql[:order.start()].rstrip() + " " + soql[limit.start():]


@dataclass
class RequestExecution:
    primary_started: bool = False
    primary_timeout: bool = False
    fallback_started: bool = False
    succeeded: bool = False
    page_size: int = 0
    count_started: bool = False
    last_call_id: str | None = None


class ExecutionGuard:
    def __init__(self, outcome: ProjectionOutcome, raw: str, *, reference_instant: datetime,
                 builder=None) -> None:
        if outcome.status is not AcceptanceStatus.READY or not outcome.calls:
            raise FoundationGuardError("PLAN_NOT_READY")
        acceptance = outcome.calls[0].acceptance
        acceptance.validate_raw(raw)
        canonical = project_accepted(acceptance, reference_instant=reference_instant, builder=builder)
        if canonical != outcome:
            raise FoundationGuardError("PLAN_BINDING_MISMATCH")
        self.outcome = canonical
        self.states = {manifest.request_id: RequestExecution() for manifest in canonical.manifest}
        self.pending: tuple[ApprovedCallManifest, str, str | None, str] | None = None

    @contextmanager
    def permit(self, manifest: ApprovedCallManifest, *, reason: str,
               parent_call_id: str | None = None, fallback: bool = False):
        if manifest not in self.outcome.manifest or self.pending is not None:
            raise FoundationGuardError("FOREIGN_OR_NESTED_GRANT")
        state = self.states[manifest.request_id]
        soql = _without_order(manifest.soql) if fallback else manifest.soql
        if manifest.operation == "query":
            if fallback:
                if (not state.primary_timeout or state.fallback_started or parent_call_id is None
                        or parent_call_id != state.last_call_id):
                    raise FoundationGuardError("H4_PRECONDITION")
                state.fallback_started = True
            else:
                if state.primary_started:
                    raise FoundationGuardError("DUPLICATE_PRIMARY")
                state.primary_started = True
        elif manifest.operation == "count":
            limit = next(re.search(r"\bLIMIT\s+(\d+)\s*$", item.soql, re.I) for item in
                         self.outcome.manifest if item.request_id == manifest.request_id
                         and item.operation == "query")
            # A filtered plan would count the candidate superset, not the members.
            if (manifest.local_filter is not None
                    or not state.succeeded or limit is None or state.page_size < int(limit[1])
                    or state.count_started or parent_call_id is None
                    or parent_call_id != state.last_call_id or fallback):
                raise FoundationGuardError("COUNT_PRECONDITION")
            state.count_started = True
        else:
            raise FoundationGuardError("UNSUPPORTED_OPERATION")
        self.pending = (manifest, reason, parent_call_id, soql)
        try:
            yield soql
            if self.pending is not None:
                raise FoundationGuardError("GRANT_NOT_CONSUMED")
        finally:
            self.pending = None

    def check(self, operation: str, dataset: str, soql: str) -> None:
        if self.pending is None:
            raise FoundationGuardError("UNAUTHORIZED_SOURCE_CALL")
        manifest, reason, parent, approved_soql = self.pending
        scope = current_source_scope()
        if ((operation, dataset, soql) != (manifest.operation, manifest.dataset, approved_soql)
                or (scope.request_id, scope.scope_id, scope.reason, scope.parent_call_id,
                    scope.interpretation_hash, scope.plan_hash) != (
                        manifest.request_id, manifest.scope_id, reason, parent,
                        manifest.interpretation_hash, manifest.plan_hash)):
            raise FoundationGuardError("EFFECTIVE_CALL_BINDING_MISMATCH")
        self.pending = None

    def query_result(self, request_id: str, rows: list[dict], *, call_id: str | None = None) -> None:
        state = self.states[request_id]
        state.succeeded = True
        state.page_size = len(rows)
        state.last_call_id = call_id

    def query_failure(self, request_id: str, exc: Exception, *, call_id: str | None = None) -> None:
        # SecopClient wraps its last source exception; preserve its causal class.
        while exc.__cause__ is not None:
            exc = exc.__cause__
        self.states[request_id].primary_timeout = isinstance(exc, TimeoutError)
        self.states[request_id].last_call_id = call_id
