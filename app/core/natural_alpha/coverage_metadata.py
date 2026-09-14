"""Closed coverage fields, independent of Natural Alpha model classification."""

BYPASS_CLASSES = frozenset({"FAST_PATH_BYPASS", "CONTROL_BYPASS", "PROFILE_BYPASS", "OTHER_BYPASS"})


def coverage_metadata(result: dict) -> dict:
    classification = result.get("natural_alpha_coverage_class")
    route = result.get("natural_alpha_excluded_by_route")
    if (type(classification) is not str or classification not in BYPASS_CLASSES
            or type(route) is not str or not route or route != result.get("route_reason")):
        return {}
    return {"natural_alpha_coverage_class": classification, "natural_alpha_excluded_by_route": route}
