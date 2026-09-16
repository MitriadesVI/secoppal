"""What one A2 SUPPLIERS turn is allowed to record, and nothing else.

The dogfood pilot is judged on turns, not on impressions, so each bound turn
leaves a receipt: what Alpha proposed, what the host admitted, whether the
sidecar was asked and what it answered, and where the turn ended.

Two rules shape it. The values are enumerated or numeric -- never the user's
text, never the buyer's name, never anything the provider returned -- so a
receipt cannot become a second, unaudited copy of the query. And a field the
runtime did not set is absent rather than defaulted, because a fabricated
``False`` in a safety count is worse than a missing one.
"""
from __future__ import annotations

import contextvars
import math

# Written by the contract through its optional recorder, read back here.
ROUTES = frozenset({
    "INTENT_WITNESS_MISSING", "INTENT_WITNESS_NOT_POSITIVE",
    "DOGFOOD_CARDINAL_AMBIGUITY", "UNSUPPORTED_CARDINALITY", "ACTOR_SUBJECT",
    "UNSUPPORTED_DIMENSION", "UNACCREDITED_RESIDUAL", "TERRITORY_NOT_BUYER",
    "UNIVERSE_WITHOUT_BUYER", "BUYER_REQUIRED", "BUYER_NOT_ACCREDITED",
    "GEOGRAPHY_WITH_BUYER", "POLARITY_SIDECAR_UNAVAILABLE", "POLARITY_VETO",
    "ACCREDITED",
})
SIDECAR_STATUSES = frozenset({"NOT_WIRED", "CONFIRMED", "VETOED", "UNAVAILABLE"})
OUTCOMES = frozenset({"EXECUTE", "CLARIFY", "NOT_BOUND"})
POLARITIES = frozenset({"POSITIVE", "NEGATED", "MIXED", "UNKNOWN"})
RELATIONS = frozenset({
    "NONE", "EXACT", "LOWER", "UPPER", "RANGE", "NEGATED", "UNKNOWN",
})
AMBIGUITY = frozenset({"APPROXIMATE", "NOT_EQUAL", "OPEN_ENDED"})

_ENUMS = {
    "a2_route": ROUTES,
    "a2_outcome": OUTCOMES,
    "a2_sidecar_status": SIDECAR_STATUSES,
    "a2_turn_polarity": POLARITIES,
    "a2_deterministic_polarity": POLARITIES,
    "a2_cardinal_relation": RELATIONS,
    "a2_ambiguity_class": AMBIGUITY,
}
_BOOLS = (
    "a2_sidecar_called", "a2_rescued_by_sidecar", "a2_buyer_accredited",
    "a2_period_accredited", "a2_deterministic_executable",
)
_INTS = ("a2_mention_witnesses", "a2_quantity_witnesses", "a2_source_calls_expected")
_FLOATS = ("a2_sidecar_latency_ms",)
_TEXT = ("a2_capability", "a2_veto_reason")

TRACE_KEYS = (*_ENUMS, *_BOOLS, *_INTS, *_FLOATS, *_TEXT)


def note(record: dict | None, **fields) -> None:
    """Record fields for this turn; a ``None`` recorder costs nothing."""
    if record is not None:
        record.update(fields)


def a2_trace_metadata(result: dict) -> dict:
    """Validate a turn's receipt. Anything unrecognized is dropped, not coerced."""
    raw = result.get("a2_capability_telemetry")
    if type(raw) is not dict or raw.get("a2_route") not in ROUTES:
        return {}
    clean: dict = {}
    for key, allowed in _ENUMS.items():
        if raw.get(key) in allowed:
            clean[key] = raw[key]
    for key in _BOOLS:
        if type(raw.get(key)) is bool:
            clean[key] = raw[key]
    for key in _INTS:
        value = raw.get(key)
        if type(value) is int and not isinstance(value, bool) and value >= 0:
            clean[key] = value
    for key in _FLOATS:
        value = raw.get(key)
        if type(value) in (int, float) and math.isfinite(value) and value >= 0:
            clean[key] = float(value)
    for key in _TEXT:
        value = raw.get(key)
        # Bounded and enumerated at the source; truncated here so a detail
        # string can never grow into free text.
        if type(value) is str and value:
            clean[key] = value[:64]
    return clean


# ── per-turn collection ───────────────────────────────────────────────────────
# One receipt per ``run_query``, isolated by context so concurrent Streamlit
# workers never read each other's turn. Inactive by default: a turn that does
# not call ``begin_turn`` makes ``record_turn`` a no-op, exactly like the HTTP
# collector in app/core/instrumentation.py.
_receipt: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "secoppal_a2_capability_receipt", default=None,
)


def begin_turn() -> contextvars.Token:
    """Open this turn's receipt; the token restores an outer one on nesting."""
    return _receipt.set({})


def record_turn(fields: dict) -> None:
    """Merge a bound turn's fields. No-op when no turn is collecting."""
    current = _receipt.get()
    if current is not None and type(fields) is dict:
        current.update(fields)


def end_turn(token: contextvars.Token | None = None) -> dict:
    current = dict(_receipt.get() or {})
    if token is not None:
        _receipt.reset(token)
    else:
        _receipt.set(None)
    return current


def attach_a2_telemetry(result: dict, receipt: dict) -> None:
    """Publish the receipt on the turn's result, validated, or not at all."""
    if type(result) is not dict or type(receipt) is not dict or not receipt:
        return
    result["a2_capability_telemetry"] = receipt
    if not a2_trace_metadata(result):
        # A receipt that does not validate is removed rather than shipped: a
        # half-written measurement would be read as a real one.
        result.pop("a2_capability_telemetry", None)
