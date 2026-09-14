"""Host coverage class of one turn, from host witnesses of its real result.

No user text is read. Precedence: REACHED_ALPHA, PROFILE_BYPASS,
CONTROL_BYPASS, FAST_PATH_BYPASS, OTHER_BYPASS; UNACCREDITED only for
inconsistent evidence.
"""
from .telemetry import trace_metadata

REACHED_ALPHA = "REACHED_ALPHA"
BYPASS_CLASSES = frozenset({"PROFILE_BYPASS", "CONTROL_BYPASS", "FAST_PATH_BYPASS", "OTHER_BYPASS"})
COVERAGE_CLASSES = frozenset({REACHED_ALPHA, "UNACCREDITED", *BYPASS_CLASSES})
PROFILE_ACTION, ACTOR_CANDIDATE = "profile_action_selection", "actor_candidate_selection"
_ALPHA_FIELDS = ("natural_alpha_mode", "natural_alpha_eligible", "natural_alpha_status")
# Routes minted by the explicit dispatcher handlers before the seam.
_FAST_PATH_PREFIXES = ("radar_", "pagination_", "suggestion", "direct_lookup_", "actor_selection_")
_FAST_PATH_ROUTES = frozenset({"refinement", "entity_ref_disambiguation", "contract_universe_review_required"})


def classify_turn(*, seam_reached: bool, structured: frozenset, result: dict) -> str:
    route = result.get("route_reason")
    if type(route) is not str or not route:
        return "UNACCREDITED"
    alpha = trace_metadata(result)
    if seam_reached:
        return REACHED_ALPHA if alpha.get("natural_alpha_mode") == "alpha" else "UNACCREDITED"
    if alpha or any(result.get(key) is not None for key in _ALPHA_FIELDS):
        return "UNACCREDITED"  # Alpha evidence without the seam witness.
    if PROFILE_ACTION in structured or _profile_lane(result):
        return "PROFILE_BYPASS"
    if ACTOR_CANDIDATE in structured or route == "reset" or route.startswith("admission_"):
        return "CONTROL_BYPASS"
    if route.startswith(_FAST_PATH_PREFIXES) or route in _FAST_PATH_ROUTES:
        return "FAST_PATH_BYPASS"
    return "OTHER_BYPASS"


def _profile_lane(result):
    # Only workflow/profile_route.py, the pre-seam Frame lane, emits these.
    return (result.get("analytical_intent") == "entity_profile"
            and all(type(result.get(key)) is int and result[key] >= 0
                    for key in ("profile_planned_jobs", "profile_attempted_calls")))
