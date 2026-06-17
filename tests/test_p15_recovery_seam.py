"""P15 ↔ follow-up recovery seam (OPP-SCOPE-001 + B5b).

Verifies, WITHOUT hitting SECOP, that a turn blocked by P15 persists enough
context for the next turn to recover. Exercises the real path:

    frame_from_params(persisted_blocked_params)   # B5b persists these
        → detect_and_merge(next_text, next_params, previous_frame)
        → compute_query_plan(merged)              # P15 should now pass

The persisted params mirror what resolve_entities writes on the blocked
branch (resolved_params = parsed_params), per planning.py.
"""

from app.core.followup_engine import detect_and_merge
from app.core.query_frame import frame_from_params
from app.core.query_plan import compute_query_plan


def _plan(params, text):
    return compute_query_plan(params, text, enabled_rules={"P15"})


# Params as B5b persists them for a blocked bidder turn (no objeto).
BLOCKED_BIDDER_BOLIVAR = {
    "dataset": "procesos",
    "intent_type": "opportunity_search",
    "estado_family": "oferta_abierta",
    "departamento_resolved": "Bolívar",
}

# Blocked browse turn on a city.
BLOCKED_BROWSE_BARRANQUILLA = {
    "dataset": "procesos",
    "intent_type": "opportunity_search",
    "estado_family": "oferta_abierta",
    "ciudad": "Barranquilla",
}


def test_blocked_bidder_then_topic_recovers_and_executes():
    prev = frame_from_params(BLOCKED_BIDDER_BOLIVAR)
    intent, merged = detect_and_merge(
        text="mantenimiento",
        current_params={"objeto": ["mantenimiento"]},
        previous_frame=prev,
    )
    # Territory survived
    assert merged.get("departamento_resolved") == "Bolívar"
    # Topic added
    assert "mantenimiento" in (merged.get("objeto") or [])
    # Opportunity classification preserved (rides in raw_params)
    assert merged.get("intent_type") == "opportunity_search"
    # P15 now lets it through
    assert _plan(merged, "mantenimiento").should_execute is True


def test_blocked_browse_then_recency_recovers_and_executes():
    prev = frame_from_params(BLOCKED_BROWSE_BARRANQUILLA)
    # Simulate parser output for "todos los rubros de los últimos 30 días":
    # a narrow date window, no real topic.
    intent, merged = detect_and_merge(
        text="todos los rubros de los últimos 30 días",
        current_params={"fecha_desde": "2026-05-15", "fecha_hasta": "2026-06-13"},
        previous_frame=prev,
    )
    assert merged.get("ciudad") == "Barranquilla"
    assert merged.get("fecha_desde") and merged.get("fecha_hasta")
    assert _plan(merged, "...").should_execute is True


def test_blocked_then_territory_only_followup_still_blocks():
    # STRESS-OPP-004 (fixed): a follow-up that only re-states the same
    # territory after a P15 block is treated as continuation (refine_filter),
    # not new_search, so the opportunity classification survives and P15
    # keeps governing — no broad territorial query slips through.
    prev = frame_from_params(BLOCKED_BIDDER_BOLIVAR)
    intent, merged = detect_and_merge(
        text="en bolívar",
        current_params={"departamento_resolved": "Bolívar"},
        previous_frame=prev,
    )
    assert merged.get("intent_type") == "opportunity_search"
    assert _plan(merged, "en bolívar").should_execute is False


def test_blocked_then_unrelated_query_does_not_inherit_territory():
    # W4: B5b makes every blocked turn a heritable frame. An unrelated
    # follow-up must NOT drag the old territory.
    prev = frame_from_params(BLOCKED_BIDDER_BOLIVAR)
    intent, merged = detect_and_merge(
        text="contratos firmados en antioquia",
        current_params={
            "departamento_resolved": "Antioquia",
            "force_dataset": "contratos",
        },
        previous_frame=prev,
    )
    # New scope wins; Bolívar must be gone.
    assert merged.get("departamento_resolved") == "Antioquia"
