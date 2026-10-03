"""M2A-2 — append-only accepted store (``foundation_accepted_store``). Owner: the M2A-2 lane
(``mission/m2a-2-store``). Its increment over M2A-1 is the store module itself and ``store_lock`` (the per-chat
``conversation_lock``); everything else it reaches is already M2A-1's closure."""

STEP = "M2A-2"
MODULES = ("app.core.foundation_accepted_store",)
DEPENDS_ON = ("M2A-1",)
CLOSURE = frozenset({
    "app.core.foundation_accepted_store",
    "app.core.store_lock",
})
