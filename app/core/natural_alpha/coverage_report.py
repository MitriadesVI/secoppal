"""Classify recorded host routes, never user language or future routing."""
from collections import Counter
import math

from .coverage_metadata import coverage_metadata

LANGUAGE_CLASSES = ("REACHED_ALPHA", "FAST_PATH_BYPASS", "PROFILE_BYPASS", "OTHER_BYPASS")
FAST_ROUTES = frozenset({
    "refinement", "pagination_more", "pagination_no_context", "pagination_dual_clarify",
    "pagination_exhausted", "pagination_capacity_reached", "contract_universe_review_required",
    "suggestion", "suggestion_error", "suggestion_invalid", "direct_lookup_id", "direct_lookup_url",
    "direct_lookup_entity_ref", "entity_ref_disambiguation", "radar_save_no_context", "radar_saved",
    "radar_save_reserved_name", "radar_save_duplicate", "radar_list", "radar_list_empty",
    "radar_run_not_found", "radar_not_found", "radar_run_skipped", "radar_run_ok", "radar_run_error",
    "actor_selection_seed", "actor_selection_expired", "actor_selection_invalid",
    "actor_selection_incompatible", "actor_selection_stale",
})
PROFILE_ROUTES = frozenset({
    "entity_profile_overview", "entity_profile_suppliers", "entity_profile_values",
    "entity_profile_modalities", "entity_profile_recent", "entity_profile_temporal",
    "entity_profile_invalid_context", "entity_profile_scope_clarification",
    "entity_profile_pending_cancelled", "entity_profile_pending_repeated", "entity_profile_pending_unresolved",
})


def is_reached_alpha(row):
    return (row["natural_alpha_mode"] == "alpha" and row["natural_alpha_eligible"] == 1
            and type(row["natural_alpha_status"]) is str and bool(row["natural_alpha_status"]))


def classify_coverage(row):
    # Real Alpha observation wins even when legacy executes an Entity Profile.
    if is_reached_alpha(row):
        return "REACHED_ALPHA"
    route = row["route_reason"]
    explicit = coverage_metadata({
        "natural_alpha_coverage_class": row.get("coverage_class"),
        "natural_alpha_excluded_by_route": row.get("coverage_excluded_route"), "route_reason": route,
    })
    if explicit and explicit["natural_alpha_coverage_class"] != "PROFILE_BYPASS":
        return explicit["natural_alpha_coverage_class"]
    if route == "reset":
        return "CONTROL_BYPASS"
    # These two timing witnesses are emitted by workflow/profile_route.py,
    # the pre-seam Frame lane. A Profile intent/route alone proves nothing.
    witnesses = (row.get("profile_planned_jobs"), row.get("profile_attempted_calls"))
    if (route in PROFILE_ROUTES and all(type(value) in (int, float) and math.isfinite(value) and value >= 0
                                       for value in witnesses)):
        return "PROFILE_BYPASS"
    return "FAST_PATH_BYPASS" if route in FAST_ROUTES else "OTHER_BYPASS"


def coverage_summary(window):
    classified = [(row, classify_coverage(row)) for row in window]
    counts = Counter(classification for _, classification in classified)
    linguistic = sum(counts[name] for name in LANGUAGE_CLASSES)
    excluded = Counter(row["route_reason"] for row, classification in classified if classification != "REACHED_ALPHA")
    controls = Counter(row["route_reason"] for row, classification in classified if classification == "CONTROL_BYPASS")
    return {
        **{name.lower(): counts[name] for name in (*LANGUAGE_CLASSES, "CONTROL_BYPASS")},
        "traffic_turns_window": len(window), "natural_work_turns_window": linguistic,
        "language_coverage_classes": list(LANGUAGE_CLASSES),
        "coverage_gap": linguistic - counts["REACHED_ALPHA"],
        "coverage_rate": counts["REACHED_ALPHA"] / linguistic if linguistic else None,
        # Includes controls for backward-compatible traffic inventory; the
        # language gap excludes them and control_by_route identifies that part.
        "excluded_by_route": dict(sorted(excluded.items())), "control_by_route": dict(sorted(controls.items())),
    }
