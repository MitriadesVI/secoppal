"""M2A-7 — presentation/echo: the pure ``echo_items`` and the §11 coverage rule. Two small modules of one responsibility
each (no monolith): ``foundation_echo_diff`` (the typed difference between the composition base and the executed
effective) and ``foundation_echo`` (the echo classes from that difference and from the provenance). They read the closed
types only through ``foundation_accepted``. Owner: the M2A-7 lane (``mission/m2a-7-echo``)."""

STEP = "M2A-7"
MODULES = ("app.core.foundation_echo", "app.core.foundation_echo_diff")
DEPENDS_ON = ("M2A-3",)
CLOSURE = frozenset({
    "app.core.foundation_echo",
    "app.core.foundation_echo_diff",
})
