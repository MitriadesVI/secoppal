"""Observe a completed explicit handler; never execute Alpha or append a turn."""
import logging

from .admission import eligible, mode
from .coverage_context import coverage_scope, current_receipt
from .coverage_metadata import coverage_metadata

logger = logging.getLogger(__name__)


def ensure_alpha_coverage_trace(deps, ctx, result: dict, *, excluded_route_class="FAST_PATH_BYPASS") -> dict:
    if (mode(deps.settings) != "alpha" or not eligible(deps.settings, ctx.channel, ctx.user_identity)
            or result.get("trace_id")):
        return result
    receipt = current_receipt(deps.feedback, ctx.user_query, ctx.channel, ctx.user_identity)
    if receipt is None:
        with coverage_scope(deps, ctx, excluded_route_class=excluded_route_class) as receipt:
            return _trace_only(deps, ctx, result, receipt)
    return _trace_only(deps, ctx, result, receipt)


def _trace_only(deps, ctx, result, receipt):
    if receipt is None or receipt.consumed:
        return result
    if not coverage_metadata({"natural_alpha_coverage_class": receipt.classification,
                              "natural_alpha_excluded_by_route": result.get("route_reason"),
                              "route_reason": result.get("route_reason")}):
        return result
    try:
        trace_id = deps.feedback.log_coverage_trace(
            ctx.user_query, ctx.channel, result, user_identity=ctx.user_identity,
        )
    except Exception as exc:
        # Observability must not change a completed reset/Radar/selection result.
        logger.warning("natural_alpha_coverage_trace_failed error_type=%s", type(exc).__name__)
        return result
    if type(trace_id) is not str or not trace_id or receipt.persisted_trace_id != trace_id:
        logger.warning("natural_alpha_coverage_trace_unconfirmed")
        return result
    return {**result, **receipt.metadata, "trace_id": trace_id}
