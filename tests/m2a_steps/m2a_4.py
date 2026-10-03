"""M2A-4 — pending clarification (§8 creation and answer, §10.1 expiry, §17 ``CANCEL_PENDING``). Owner: the M2A-4 lane
(``mission/m2a-4-pending``). Five small modules of one responsibility each (no monolith): ``foundation_pending_clock``
(the plazo and its expiry over an injected instant), ``foundation_pending_cancel`` (§17), ``foundation_pending_answer``
(§8.3 rows 1 and 4–8), ``foundation_pending_create`` (§8.1, §8.2, §8.4: the one place a pending is built) and
``foundation_pending`` (the decision of a turn that meets a pending). They read the closed types only through
``foundation_accepted`` and the reducer through ``foundation_delta*`` / ``foundation_turn_scope`` (M2A-3)."""

STEP = "M2A-4"
MODULES = (
    "app.core.foundation_pending",
    "app.core.foundation_pending_answer",
    "app.core.foundation_pending_cancel",
    "app.core.foundation_pending_clock",
    "app.core.foundation_pending_create",
)
DEPENDS_ON = ("M2A-3",)
CLOSURE = frozenset({
    "app.core.foundation_pending",
    "app.core.foundation_pending_answer",
    "app.core.foundation_pending_cancel",
    "app.core.foundation_pending_clock",
    "app.core.foundation_pending_create",
})
