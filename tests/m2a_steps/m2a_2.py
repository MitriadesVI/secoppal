"""M2A-2 — append-only accepted store. Owner: the M2A-2 lane (``mission/m2a-2-store``), which fills this file when
``app/core/foundation_accepted_store.py`` lands. Empty until then."""

STEP = "M2A-2"
MODULES = ()
DEPENDS_ON = ("M2A-1",)
CLOSURE = frozenset()
