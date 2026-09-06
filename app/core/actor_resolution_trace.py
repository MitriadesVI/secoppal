"""Compact name-resolution observability without promoting candidate identity."""
from app.core.actor_resolution import actor_resolution_method, actor_resolution_status


def actor_resolution_trace(params: dict) -> dict:
    status = actor_resolution_status(params)
    method = actor_resolution_method(params)
    if not status and not method:
        return {}
    pending = params.get("actor_pending")
    candidates = pending.get("candidates", []) if isinstance(pending, dict) else []
    return {
        "actor_resolution_status": status,
        "actor_resolution_method": method,
        "candidates_count": len(candidates) if isinstance(candidates, list) else 0,
    }
