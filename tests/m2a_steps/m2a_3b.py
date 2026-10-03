"""M2A-3b — inheritance: a separate entry for the host's inputs (unit, year, previous TimeValue). Planned module:
``app.core.foundation_authority_inheritance``. Owner: the M2A-3b lane (``mission/m2a-3b-inheritance``). MODULES stays
empty until the step lands; the lane fills MODULES and CLOSURE in this file only."""

STEP = "M2A-3b"
MODULES = ()
DEPENDS_ON = ("M2A-3",)
CLOSURE = frozenset()
