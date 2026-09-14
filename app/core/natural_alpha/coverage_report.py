"""Summarize host-recorded turn coverage; never user language or future routing."""
from collections import Counter

from .coverage_classification import BYPASS_CLASSES, COVERAGE_CLASSES, REACHED_ALPHA
from .telemetry import STATUSES

LANGUAGE_CLASSES = ("REACHED_ALPHA", "FAST_PATH_BYPASS", "PROFILE_BYPASS", "OTHER_BYPASS")
REACHED_STATUSES = frozenset(STATUSES)
_ALPHA_FIELDS = ("natural_alpha_mode", "natural_alpha_eligible", "natural_alpha_status")


def _valid_alpha(row):
    selected_mode, eligible, status = (row.get(key) for key in _ALPHA_FIELDS)
    return (type(selected_mode) is str and selected_mode == "alpha"
            and type(eligible) is int and eligible == 1
            and type(status) is str and status in REACHED_STATUSES)


def is_reached_alpha(row):
    # A turn receipt must agree; rows before receipts carry neither field.
    recorded, turn = row.get("coverage_class"), row.get("coverage_turn_id")
    return _valid_alpha(row) and (
        (type(recorded) is str and recorded == REACHED_ALPHA and type(turn) is str)
        or (recorded is None and turn is None)
    )


def classify_coverage(row):
    recorded = row.get("coverage_class")
    if recorded is not None and (type(recorded) is not str or recorded not in COVERAGE_CLASSES):
        return "UNACCREDITED"
    if is_reached_alpha(row):
        return REACHED_ALPHA
    # Contradictory/partial Alpha metadata cannot become bypass authority.
    if any(row.get(key) is not None for key in _ALPHA_FIELDS):
        return "UNACCREDITED"
    if (recorded in BYPASS_CLASSES and type(row.get("coverage_turn_id")) is str
            and row.get("coverage_excluded_route") == row["route_reason"]):
        return recorded
    return "UNACCREDITED"


def _secondary(row, classified_turns):
    # A later legacy trace of a turn whose first trace carried the class.
    turn = row.get("coverage_turn_id")
    return (row.get("coverage_class") is None and type(turn) is str and turn in classified_turns
            and all(row.get(key) is None for key in _ALPHA_FIELDS))


def coverage_summary(window):
    classified_turns = {row["coverage_turn_id"] for row in window
                        if type(row.get("coverage_turn_id")) is str and row.get("coverage_class") is not None}
    turns = [row for row in window if not _secondary(row, classified_turns)]
    classified = [(row, classify_coverage(row)) for row in turns]
    counts = Counter(classification for _, classification in classified)
    linguistic = sum(counts[name] for name in LANGUAGE_CLASSES)
    excluded = Counter(row["route_reason"] for row, classification in classified
                       if classification not in ("REACHED_ALPHA", "UNACCREDITED"))
    controls = Counter(row["route_reason"] for row, classification in classified if classification == "CONTROL_BYPASS")
    unaccredited = Counter(row["route_reason"] for row, classification in classified if classification == "UNACCREDITED")
    return {
        **{name.lower(): counts[name] for name in (*LANGUAGE_CLASSES, "CONTROL_BYPASS")},
        "traffic_turns_window": len(turns), "secondary_traces_window": len(window) - len(turns),
        "natural_work_turns_window": linguistic,
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
