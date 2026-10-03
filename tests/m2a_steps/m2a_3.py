"""M2A-3 — pure reducer (``compose``), the delta's outcome (host rules, §8.4) and the turn scope. Owner: the M2A-3 lane
(``mission/m2a-3-reducer``). Three small modules of one responsibility each (no monolith): ``foundation_delta``
(§6 steps 1–3b and 5), ``foundation_delta_outcome`` (§6 step 4, §8.4, §19 dependencies) and ``foundation_turn_scope``
(§7). They read the closed types only through ``foundation_accepted`` (R1)."""

STEP = "M2A-3"
MODULES = ("app.core.foundation_delta", "app.core.foundation_delta_outcome", "app.core.foundation_turn_scope")
DEPENDS_ON = ("M2A-1",)
CLOSURE = frozenset({
    "app.core.foundation_delta",
    "app.core.foundation_delta_outcome",
    "app.core.foundation_turn_scope",
})
