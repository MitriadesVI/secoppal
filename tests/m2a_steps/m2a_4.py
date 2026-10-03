"""M2A-4 — pending clarification (§8.3 protocol, CANCEL_PENDING §17, expiry, asks cap). Planned module:
``app.core.foundation_pending``. Owner: the M2A-4 lane (``mission/m2a-4-pending``). MODULES stays empty until the step
lands; the lane fills MODULES and CLOSURE in this file only."""

STEP = "M2A-4"
MODULES = ()
DEPENDS_ON = ("M2A-3",)
CLOSURE = frozenset()
