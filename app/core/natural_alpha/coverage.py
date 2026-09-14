"""Observe a completed explicit handler; never execute Alpha or append a turn."""
import logging

from .admission import eligible, mode
from .coverage_metadata import coverage_metadata

logger = logging.getLogger(__name__)


def ensure_alpha_coverage_trace(deps, ctx, result: dict, *, excluded_route_class="FAST_PATH_BYPASS") -> dict:
    if (mode(deps.settings) != "alpha" or not eligible(deps.settings, ctx.channel, ctx.user_identity)
            or result.get("trace_id")):
        return result
    metadata = coverage_metadata({
        "natural_alpha_coverage_class": excluded_route_class,
        "natural_alpha_excluded_by_route": result.get("route_reason"),
        "route_reason": result.get("route_reason"),
    })
    if not metadata:
        return result
    observed = {**result, **metadata}
    try:
        trace_id = deps.feedback.log_trace(ctx.user_query, ctx.channel, observed, user_identity=ctx.user_identity)
    except Exception as exc:
        # Observability must not change a completed reset/Radar/selection result.
        logger.warning("natural_alpha_coverage_trace_failed error_type=%s", type(exc).__name__)
        return result
    observed["trace_id"] = trace_id
    return observed
