"""Record the legacy shadow of an accepted Foundation turn in the turn ledger (M0).

Observational only: it reads the legacy parse beside Foundation's acceptance and
appends one SHADOW row with a verdict per dimension. It never changes the result,
and a failure here records SHADOW_ERROR instead of reaching the turn. Values stay
out of the row; the trace already carries the query, and replay rebuilds them.
"""

from __future__ import annotations

from app.core.legacy_shadow import compare
from app.core import turn_ledger
from app.core.turn_ledger import Stage, StageName, digest


def record_legacy_shadow(workflow, ctx, accepted) -> None:
    if turn_ledger.current_ledger() is None:
        return
    try:
        params = workflow.deps.query_router.parse(ctx.user_query, today=ctx.today).params
        diff = compare(ctx.user_query, ctx.today, params, accepted)
    except Exception as exc:  # noqa: BLE001 - an instrument must never fail the turn
        turn_ledger.record(Stage(StageName.SHADOW, "LEGACY_SHADOW", "SHADOW_ERROR", reason_code=type(exc).__name__))
        return
    turn_ledger.record(Stage(
        StageName.SHADOW, "LEGACY_SHADOW", diff.status.value,
        reason_code="PARITY" if diff.is_parity else "DIVERGENT",
        input_digest=diff.raw_digest, output_digest=digest(diff.to_dict()),
        detail={**{item.dimension: item.verdict.value for item in diff.dimensions},
                "unmapped_legacy_keys": len(diff.unmapped_legacy_keys)},
    ))
