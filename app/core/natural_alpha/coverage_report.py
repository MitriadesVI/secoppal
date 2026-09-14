"""Classify recorded host routes, never user language or future routing."""
from collections import Counter
import math

from .coverage_metadata import coverage_metadata
from .telemetry import STATUSES

LANGUAGE_CLASSES = ("REACHED_ALPHA", "FAST_PATH_BYPASS", "PROFILE_BYPASS", "OTHER_BYPASS")
REACHED_STATUSES = frozenset(STATUSES)
PROFILE_ROUTES = frozenset({
    "entity_profile_overview", "entity_profile_suppliers", "entity_profile_values",
    "entity_profile_modalities", "entity_profile_recent", "entity_profile_temporal",
    "entity_profile_invalid_context", "entity_profile_scope_clarification",
    "entity_profile_pending_cancelled", "entity_profile_pending_repeated", "entity_profile_pending_unresolved",
})


def is_reached_alpha(row):
    selected_mode, eligible, status = (row.get(key) for key in (
        "natural_alpha_mode", "natural_alpha_eligible", "natural_alpha_status",
    ))
    return (type(selected_mode) is str and selected_mode == "alpha"
            and type(eligible) is int and eligible == 1
            and type(status) is str and status in REACHED_STATUSES)


def classify_coverage(row):
    # Real Alpha observation wins even when legacy executes an Entity Profile.
    if is_reached_alpha(row):
        return "REACHED_ALPHA"
    # Contradictory/partial Alpha metadata cannot become bypass authority.
    if any(row.get(key) is not None for key in (
        "natural_alpha_mode", "natural_alpha_eligible", "natural_alpha_status",
    )):
        return "UNACCREDITED"
    route = row["route_reason"]
    explicit = coverage_metadata({
        "natural_alpha_coverage_class": row.get("coverage_class"),
        "natural_alpha_excluded_by_route": row.get("coverage_excluded_route"), "route_reason": route,
    })
    if not explicit:
        return "UNACCREDITED"
    if explicit["natural_alpha_coverage_class"] != "PROFILE_BYPASS":
        return explicit["natural_alpha_coverage_class"]
    # These two timing witnesses are emitted by workflow/profile_route.py,
    # the pre-seam Frame lane. A Profile intent/route alone proves nothing.
    witnesses = (row.get("profile_planned_jobs"), row.get("profile_attempted_calls"))
    if (route in PROFILE_ROUTES and all(type(value) in (int, float) and math.isfinite(value) and value >= 0
                                       for value in witnesses)):
        return "PROFILE_BYPASS"
    return "UNACCREDITED"


def coverage_summary(window):
    classified = [(row, classify_coverage(row)) for row in window]
    counts = Counter(classification for _, classification in classified)
    linguistic = sum(counts[name] for name in LANGUAGE_CLASSES)
    excluded = Counter(row["route_reason"] for row, classification in classified
                       if classification not in ("REACHED_ALPHA", "UNACCREDITED"))
    controls = Counter(row["route_reason"] for row, classification in classified if classification == "CONTROL_BYPASS")
    unaccredited = Counter(row["route_reason"] for row, classification in classified if classification == "UNACCREDITED")
    return {
        **{name.lower(): counts[name] for name in (*LANGUAGE_CLASSES, "CONTROL_BYPASS")},
        "traffic_turns_window": len(window), "natural_work_turns_window": linguistic,
        "eligible_turns_window": linguistic + counts["CONTROL_BYPASS"],
        "unaccredited_turns_window": counts["UNACCREDITED"],
        "unaccredited_by_route": dict(sorted(unaccredited.items())),
        "language_coverage_classes": list(LANGUAGE_CLASSES),
        "coverage_gap": linguistic - counts["REACHED_ALPHA"],
        "coverage_rate": counts["REACHED_ALPHA"] / linguistic if linguistic else None,
        # Only positively accredited bypasses; unknown historical/off traffic
        # is inventoried separately and never inflates language opportunity.
        "excluded_by_route": dict(sorted(excluded.items())), "control_by_route": dict(sorted(controls.items())),
    }
