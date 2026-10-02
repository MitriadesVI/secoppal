"""M2A-1 — closed M2a types (``foundation_accepted``). Owner in wave 1: the M2A-3 lane, and only because it is the
declared adjacent writer of ``app/core/foundation_accepted.py`` (shared canonical form with the future M2A-1b)."""

STEP = "M2A-1"
MODULES = ("app.core.foundation_accepted",)
DEPENDS_ON = ()
CLOSURE = frozenset({
    "app", "app.core",
    "app.core.foundation_accepted",
    "app.core.foundation_authority_values",
    "app.core.foundation_geo_level",
    "app.core.foundation_operations",
    "app.core.foundation_operator_table",
    "app.core.interpretation_contract",
    "app.core.semantic_dimensions",
})
