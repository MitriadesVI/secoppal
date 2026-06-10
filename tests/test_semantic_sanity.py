"""Tests for the passive semantic sanity layer (P14 / semantic_sanity.py).

The contract under test is deliberately narrow and conservative:

1. It EMITS ``fragile_objeto_and`` for an AND of >=2 rare tokens the builder
   won't demote (root cause #3, e.g. "construir parques").
2. It is FAIL-OPEN: the flag never flips ``should_execute`` to False and never
   mutates ``params`` — the query runs exactly as it would have.
3. It does NOT fire for cases the builder already handles or that commonly
   return results (single token, one-rare pairs, >=4 tokens demoted to OR,
   OR-sublists).
"""

from app.core.query_plan import compute_query_plan
from app.core.semantic_sanity import compute_sanity_flags


# ── Direct unit tests on compute_sanity_flags ────────────────────────────────

def test_fragile_and_of_two_rare_tokens_is_flagged():
    flags = compute_sanity_flags({"objeto": ["construir", "parques"]})
    assert "fragile_objeto_and" in flags


def test_one_rare_one_common_is_not_flagged():
    # "vial" is rare, "mantenimiento" is in COMMON_TOKENS → only 1 rare term.
    flags = compute_sanity_flags({"objeto": ["mantenimiento", "vial"]})
    assert "fragile_objeto_and" not in flags


def test_single_token_is_not_flagged():
    flags = compute_sanity_flags({"objeto": ["pavimentacion"]})
    assert "fragile_objeto_and" not in flags


def test_four_tokens_not_flagged_builder_demotes_to_or():
    # len>=4 → SoQLBuilder demotes AND→OR, so it is not a fragile AND.
    flags = compute_sanity_flags(
        {"objeto": ["construir", "parques", "infantiles", "biosaludables"]}
    )
    assert "fragile_objeto_and" not in flags


def test_or_sublist_is_not_flagged():
    # An OR-group (sublist) is not a plain AND of strings.
    flags = compute_sanity_flags({"objeto": [["aires", "acondicionados"], "parques"]})
    assert "fragile_objeto_and" not in flags


def test_empty_and_missing_objeto_are_safe():
    assert compute_sanity_flags({}) == []
    assert compute_sanity_flags({"objeto": []}) == []
    assert compute_sanity_flags({"objeto": None}) == []


# ── Integration: the flag is passive (fail-open) inside compute_query_plan ────

def test_plan_emits_flag_but_still_executes():
    params = {"objeto": ["construir", "parques"], "ciudad": "bogota"}
    plan = compute_query_plan(params, raw_query="construir parques en bogota")
    assert "fragile_objeto_and" in plan.risk_flags
    # Fail-open: the flag never blocks execution.
    assert plan.should_execute is True


def test_plan_does_not_mutate_input_params():
    params = {"objeto": ["construir", "parques"]}
    before = {"objeto": list(params["objeto"])}
    compute_query_plan(params, raw_query="construir parques")
    assert params == before


def test_p14_can_be_disabled_via_enabled_rules():
    params = {"objeto": ["construir", "parques"]}
    plan = compute_query_plan(
        params, raw_query="construir parques", enabled_rules={"P1", "P13"}
    )
    assert "fragile_objeto_and" not in plan.risk_flags
