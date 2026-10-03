"""M2A-3b — inheritance: a separate entry for the host's inputs (unit, year, previous TimeValue). One module of one
responsibility: ``foundation_authority_inheritance`` derives those inputs from the accepted base alone and reads the closed
types only through ``foundation_accepted`` (R1). Owner: the M2A-3b lane (``mission/m2a-3b-inheritance``)."""

STEP = "M2A-3b"
MODULES = ("app.core.foundation_authority_inheritance",)
DEPENDS_ON = ("M2A-3",)
CLOSURE = frozenset({"app.core.foundation_authority_inheritance"})
