"""M2A-7: one mutant per rule of the echo (and of the diff it reads); a test that passes without killing its mutant does
not count.

A mutant is the REAL source of ``foundation_echo`` / ``foundation_echo_diff`` with ONE exact textual change (the anchor
must occur once), loaded as its own module. It is KILLED when any of three judges says no:

* the hand-written scenarios of ``test_foundation_echo`` (``CASES``: the exact items the contract owes; an exception
  where an item was owed is a kill);
* the refusals of ``test_foundation_echo`` (``REFUSALS``: an incoherent turn must raise its ``ContractError``);
* the generated turns of ``test_foundation_echo_properties`` against the reference (completeness, justification, the
  classes compose reports, silent turns, fresh acceptances, pages).

Every mutant must be killed by the scenarios or the refusals (deterministic, the gate) and, because the generator is
the claim ``echo ↔ diff(base, efectiva)`` of INV-M2A-12, by the properties too; the few rules the generator reaches
only through a handful of values are listed in ``SCENARIO_ONLY`` with the reason, never silently.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest
from hypothesis import HealthCheck, find, settings
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
from app.core.interpretation_contract import ContractError
from tests.test_foundation_delta_properties import st_base, st_case
from tests.test_foundation_echo import CASES, REFUSALS
from tests.test_foundation_echo_properties import (
    PATCHED_RESOLUTIONS, composed_of, fresh_problems, not_ready_problems, page_problems, problems, silent_problems,
    st_plan_turn, st_value_turn,
)

ROOT = Path(__file__).resolve().parents[1]
DIFF_SOURCE = (ROOT / "app/core/foundation_echo_diff.py").read_text("utf-8")
ECHO_SOURCE = (ROOT / "app/core/foundation_echo.py").read_text("utf-8")
DIFF_MODULE, ECHO_MODULE = "m2a7_diff_mutant", "m2a7_echo_mutant"


def _load(name: str, source: str) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__file__ = f"<{name}>"
    sys.modules[name] = module
    exec(compile(source, f"<{name}>", "exec"), module.__dict__)
    return module


def _apply(source: str, changes) -> str:
    for old, new in changes:
        assert source.count(old) == 1, f"mutation anchor is not unique ({source.count(old)}): {old!r}"
        source = source.replace(old, new)
    return source


def mutant(diff_changes=(), echo_changes=()) -> types.ModuleType:
    _load(DIFF_MODULE, _apply(DIFF_SOURCE, diff_changes))
    echo = _apply(ECHO_SOURCE.replace("from app.core.foundation_echo_diff import", f"from {DIFF_MODULE} import"),
                  echo_changes)
    return _load(ECHO_MODULE, echo)


def _drop():
    for name in (DIFF_MODULE, ECHO_MODULE):
        sys.modules.pop(name, None)


# ---- the mutants: (diff changes, echo changes) ---------------------------------------------------------------------
UNIT_TABLE = "UNIT_ECHO = {UnitSource.PREVIOUS_ACCEPTED_VALUE: EchoClass.UNIT_INHERITED,\n"
PLAN_CARD = "        if condition.dimension is SemanticDimension.CARDINALITY and RULE_TOPN_CAPPED in condition.receipt_rules:"
RESET = "        if resolution is TurnResolution.NEW_SEARCH and base is not None and not anchor_expired:"
DEFAULTED = ("        defaulted = (ORDER_ECHO.get(condition.provenance.order_direction_source) is not None\n"
             "                     or RULE_ORDER_DEFAULTED in condition.receipt_rules)")
CARRIED_ITEMS = "        items += [_item(EchoClass.CARRIED, condition.slot, (condition.condition_id,)) for condition in diff.carried]"
UNIVERSE_CARRIED = "        if not diff.universe_replaced:"
RANGE_CLEAR = "            diff.wide_value_clear and bool(placed) and bool(gone))"
TOPIC_GUARD = "    if not placed or not any(old.value not in {new.value for new in placed} for old in gone):"
ORDER_BY = "_slot_rank(item.slot))))"
SHARED_IDS = "[c.condition_id for c in diff.placed if c.dimension is _VALUE]))"
RELATIVE = "                and RULE_RELATIVE_TO_PREVIOUS in condition.receipt_rules):"

ECHO_MUTANTS = {
    # CARRIED
    "carried_omitted": [(CARRIED_ITEMS, "        pass")],
    "carried_universe_omitted": [(UNIVERSE_CARRIED, "        if False:")],
    "carried_universe_even_when_replaced": [(UNIVERSE_CARRIED, "        if True:")],
    # the provenance of what the turn placed
    "unit_inherited_omitted": [(UNIT_TABLE + "             UnitSource.SIBLING_IN_RANGE: EchoClass.UNIT_SHARED_IN_RANGE}",
                                "UNIT_ECHO = {UnitSource.SIBLING_IN_RANGE: EchoClass.UNIT_SHARED_IN_RANGE}")],
    "unit_inherited_class_changed": [("UnitSource.PREVIOUS_ACCEPTED_VALUE: EchoClass.UNIT_INHERITED,",
                                      "UnitSource.PREVIOUS_ACCEPTED_VALUE: EchoClass.YEAR_INHERITED,")],
    "unit_shared_omitted": [("             UnitSource.SIBLING_IN_RANGE: EchoClass.UNIT_SHARED_IN_RANGE}", "             }")],
    "unit_shared_only_the_flagged_bound": [(SHARED_IDS, "[c.condition_id for c in shared]))")],
    "year_inherited_omitted": [("YEAR_ECHO = {YearSource.PREVIOUS_ACCEPTED_TIME: EchoClass.YEAR_INHERITED,", "YEAR_ECHO = {")],
    "year_clock_inferred_omitted": [("             YearSource.CLOCK_INFERRED: EchoClass.YEAR_CLOCK_INFERRED,\n", "")],
    "year_clock_inferred_class_changed": [("YearSource.CLOCK_INFERRED: EchoClass.YEAR_CLOCK_INFERRED,",
                                           "YearSource.CLOCK_INFERRED: EchoClass.YEAR_INHERITED,")],
    "relative_window_clock_omitted": [("             YearSource.RELATIVE_TO_CLOCK: EchoClass.RELATIVE_WINDOW_RESOLVED}", "             }")],
    "relative_window_receipt_ignored": [(RELATIVE, "                and False):")],
    "relative_window_without_its_receipt": [(RELATIVE, "                and True):")],
    "provenance_of_the_carried_reannounced": [("    for condition in diff.placed:\n        provenance = condition.provenance",
                                               "    for condition in (*diff.carried, *diff.placed):\n"
                                               "        provenance = condition.provenance")],
    # the executed plan: a cap and a default hold at every execution (INV-M2A-21)
    "topn_cap_unannounced": [(PLAN_CARD, "        if False:")],
    "topn_cap_without_its_receipt": [(PLAN_CARD, "        if condition.dimension is SemanticDimension.CARDINALITY:")],
    "topn_cap_receipt_on_any_dimension": [(PLAN_CARD, "        if RULE_TOPN_CAPPED in condition.receipt_rules:")],
    "carried_on_a_button_event": [("carried=resolution is not TurnResolution.PROTOCOL_EVENT)", "carried=True)")],
    "order_default_unannounced": [("        if condition.dimension is SemanticDimension.ORDER and defaulted:", "        if False:")],
    "order_default_provenance_ignored": [(DEFAULTED, "        defaulted = RULE_ORDER_DEFAULTED in condition.receipt_rules")],
    "order_default_receipt_ignored": [(DEFAULTED, "        defaulted = ORDER_ECHO.get(condition.provenance.order_direction_source) is not None")],
    "order_default_announced_when_stated": [(DEFAULTED, "        defaulted = True")],
    "plan_only_on_what_was_placed": [("        items += _plan_items(effective)",
                                      "        items += [i for i in _plan_items(effective)"
                                      " if i.condition_ids[0] in {c.condition_id for c in diff.placed}]")],
    # CONTEXT_RESET, CONTEXT_EXPIRED, what a turn executes
    "context_reset_omitted": [(RESET, "        if False:")],
    "context_reset_over_an_expired_anchor": [(RESET, "        if resolution is TurnResolution.NEW_SEARCH and base is not None:")],
    "context_reset_without_an_anchor": [(RESET, "        if resolution is TurnResolution.NEW_SEARCH and not anchor_expired:")],
    "context_expired_omitted": [("    if anchor_expired and not executes:", "    if False:")],
    "context_expired_on_an_executing_turn": [("    if anchor_expired and not executes:", "    if anchor_expired:")],
    "context_expired_without_the_fact": [("    if anchor_expired and not executes:", "    if not executes:")],
    "a_page_echoes_its_execution": [("    if executes and resolution is not TurnResolution.PROTOCOL_PAGE:", "    if executes:")],
    "a_turn_without_effective_executes": [(
        "    executes = resolution in EXECUTING and (effective is not None or resolution is TurnResolution.PROTOCOL_PAGE)",
        "    executes = resolution in EXECUTING")],
    "difference_echo_on_a_fresh_search": [(
        "        if resolution in PATCHED:\n"
        "            items += _difference_items(effective, diff, carried=resolution is not TurnResolution.PROTOCOL_EVENT)",
        "        items += _difference_items(effective, diff, carried=True)")],
    # by difference
    "value_range_a81_unannounced": [("        replaced = any(r.departure is Departure.A81_OPPOSITE_BOUND for r in gone) or (",
                                     "        replaced = False or (")],
    "value_range_clear_set_unannounced": [(RANGE_CLEAR, "            False)")],
    "value_range_clear_alone_replaces": [(RANGE_CLEAR, "            diff.wide_value_clear and bool(gone))")],
    "value_range_any_set_after_a_one_side_clear": [(RANGE_CLEAR, "            bool(placed) and bool(gone))")],
    "value_range_of_a_restored_side": [("        if gone and side not in standing and replaced:", "        if gone and replaced:")],
    "topic_replaced_unannounced": [(TOPIC_GUARD + "\n        return []", "    return []")],
    "topic_replaced_when_restated": [(TOPIC_GUARD, "    if not placed or not gone:")],
    "topic_replaced_by_a_negative": [("    placed = [c for c in diff.placed if c.slot == _TOPIC_POSITIVE]",
                                      "    placed = [c for c in diff.placed if c.dimension is _TOPIC]")],
    "polarity_inverted_unannounced": [("        if removal.departure is not Departure.F16_POSITIVE_INVERTED:\n            continue",
                                       "        continue")],
    "universe_changed_unannounced": [("    if diff.universe_changed:\n        carried_time", "    if False:\n        carried_time")],
    "universe_changed_when_restated": [("    if diff.universe_changed:\n        carried_time",
                                        "    if diff.universe_replaced:\n        carried_time")],
    "universe_changed_without_the_carried_time": [(
        "        carried_time = [c.condition_id for c in diff.carried if c.dimension is SemanticDimension.TIME]",
        "        carried_time = []")],
    "topn_carried_unannounced": [("    if diff.set_order:", "    if False:")],
    "topn_carried_without_a_set_order": [("    if diff.set_order:", "    if True:")],
    # order, key, and the guards of the arguments
    "items_in_reverse_order": [(ORDER_BY, "_slot_rank(item.slot)), reverse=True))")],
    "display_key_is_the_class_only": [('    key = echo_class.value if slot is None else f"{echo_class.value}:{slot.key}"',
                                       "    key = echo_class.value")],
    "arguments_not_compared_with_the_record": [("    if patch != record.patch or effective != record.effective:", "    if False:")],
    "expiry_question_without_expiry_accepted": [("    if asks_expiry and not anchor_expired:", "    if False:")],
    "composition_without_a_base_accepted": [(
        '        if base is None:\n            raise ContractError("ECHO_COMPOSITION_WITHOUT_BASE")', "        pass")],
    "effective_of_another_base_accepted": [("        if effective.base_trace_id != record.base_trace_id:", "        if False:")],
    "anchor_expired_not_typed": [("    if type(anchor_expired) is not bool:", "    if False:")],
    "record_not_typed": [("    if type(record) is not AcceptedTurnRecord:", "    if False:")],
    "base_not_typed": [("    if base is not None and type(base) is not AcceptedInterpretation:", "    if False:")],
}

DIFF_MUTANTS = {
    "uncited_removal_accepted": [('    raise ContractError("ECHO_UNCITED_REMOVAL")', "    return Departure.CITED")],
    "changed_carried_condition_accepted": [(
        '        else:\n            raise ContractError("ECHO_CARRIED_CONDITION_CHANGED")', "        else:\n            carried.append(condition)")],
    "carried_must_match_its_requests_too": [(
        "        elif replace(old, request_ids=condition.request_ids) == condition:  # §6.3: byte-identical, but P-1's request_ids",
        "        elif old == condition:")],
    "placed_conditions_not_checked": [("    if Counter(map(_shape, placed)) != Counter(_shape(op.condition) for op in placers):",
                                       "    if False:")],
    "a_writer_does_not_cite": [("    if old.slot in written or old.slot in cleared:", "    if old.slot in cleared:")],
    "a_clear_does_not_cite": [("    if old.slot in written or old.slot in cleared:", "    if old.slot in written:")],
    "a_whole_dimension_clear_covers_only_the_positive": [(
        "    return frozenset(Slot(slot.dimension, polarity) for polarity in (Polarity.POSITIVE, Polarity.NEGATIVE))",
        "    return frozenset({Slot(slot.dimension, Polarity.POSITIVE)})")],
    "a_whole_value_clear_covers_only_the_lower": [("        return frozenset({_LOWER_SIDE, _UPPER_SIDE})", "        return frozenset({_LOWER_SIDE})")],
    "a81_without_an_op_on_the_opposite_side": [("        if any(op.slot == opposite for op in placers):", "        if True:")],
    "f16_any_value": [("        if any(op.slot == negative and canonical_value(op.condition) == canonical_value(old) for op in placers):",
                       "        if any(op.slot == negative for op in placers):")],
    "a81_departure_cited": [("            return Departure.A81_OPPOSITE_BOUND", "            return Departure.CITED")],
    "f16_departure_cited": [("            return Departure.F16_POSITIVE_INVERTED", "            return Departure.CITED")],
    "whole_value_clear_flag_off": [("    wide_value_clear = any(", "    wide_value_clear = False and any(")],
    "a_one_side_clear_is_a_whole_one": [("                           and op.slot.side is None for op in patch.ops)", "                           for op in patch.ops)")],
    "set_order_flag_off": [("any(op.op is PatchOpKind.SET_ORDER for op in patch.ops))", "False)")],
    "universe_changed_is_the_operation": [("universe_before != universe_after,", "bool(redirects),")],
    "requests_changed_without_operation_accepted": [("    if not redirects and effective.requests != base.requests:", "    if False:")],
    "universe_not_the_requested_accepted": [(
        "    if redirects and universe_after != tuple((op.universe.subject, op.universe.universe_kind) for op in redirects):",
        "    if False:")],
    "fresh_acceptance_carries": [("    return TurnDiff((), effective.conditions, (), False, False, False, False)",
                                  "    return TurnDiff(effective.conditions, (), (), False, False, False, False)")],
}

MUTANTS = {**{f"echo.{name}": ((), changes) for name, changes in ECHO_MUTANTS.items()},
           **{f"diff.{name}": (changes, ()) for name, changes in DIFF_MUTANTS.items()}}


# ---- the judges ----------------------------------------------------------------------------------------------------
def killed_by_the_cases(echo_items) -> str | None:
    for case in CASES:
        try:
            if case.run(echo_items) != case.expected:
                return f"scenario {case.name}"
        except Exception:  # noqa: BLE001 - an exception where items were owed is a kill
            return f"scenario {case.name} (raised)"
    for name, args, kwargs, code in REFUSALS:
        try:
            echo_items(*args, **kwargs)
        except ContractError as error:
            if code in str(error):
                continue
        except Exception:  # noqa: BLE001
            pass
        return f"refusal {name}"
    return None


FIND = settings(max_examples=800, database=None, derandomize=True, deadline=None,
                suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large, HealthCheck.filter_too_much])


def _finds(strategy, bad) -> bool:
    try:
        find(strategy, bad, settings=FIND)
    except Exception as error:  # noqa: BLE001 - hypothesis' NoSuchExample: nothing found
        if type(error).__name__ == "NoSuchExample":
            return False
        raise
    return True


def killed_by_the_properties(echo_items) -> str | None:
    def composed_bad(case):
        base, patch, _ = case[0]
        composed = composed_of(base, patch)
        return composed is not None and bool(problems(echo_items, base, patch, composed, case[1]))

    deltas = st.tuples(st.one_of(st_plan_turn(), st_value_turn(), st_case(), st_case(rich=False)),
                       st.sampled_from(PATCHED_RESOLUTIONS))
    if _finds(deltas, composed_bad):
        return "property: a composed turn"
    if _finds(st.one_of(st_case(), st_case(rich=False)),
              lambda case: bool(not_ready_problems(echo_items, case[0], case[1]))):
        return "property: a delta that is not ready"
    if _finds(st.tuples(st_base(), st.one_of(st.none(), st_base()), st.booleans()),
              lambda case: bool(fresh_problems(echo_items, *case))):
        return "property: a fresh acceptance"
    if _finds(st.tuples(st_base(), st.booleans()), lambda case: bool(page_problems(echo_items, *case))):
        return "property: a page"
    silent = st.tuples(st_base(), st.booleans(), st.sampled_from([fa.TurnResolution.CLARIFY, fa.TurnResolution.INFORM,
                                                                  fa.TurnResolution.REASK]))
    if _finds(silent, lambda case: bool(silent_problems(echo_items, case[0], case[1], case[2]))):
        return "property: a turn that executes nothing"
    return None


# Rules the generated turns do not discriminate, with the reason; the scenarios still kill them.
SCENARIO_ONLY = {
    "diff.universe_not_the_requested_accepted": "an effective that disagrees with its REPLACE_UNIVERSE is unreachable from "
                                                "compose; only a hand-built turn shows it",
    "diff.requests_changed_without_operation_accepted": "unreachable from compose",
    "diff.changed_carried_condition_accepted": "unreachable from compose (INV-M2A-02)",
    "diff.placed_conditions_not_checked": "unreachable from compose",
    "diff.uncited_removal_accepted": "unreachable from compose (INV-M2A-16)",
    "diff.a81_without_an_op_on_the_opposite_side": "unreachable from compose",
    "diff.f16_any_value": "unreachable from compose",
    "echo.composition_without_a_base_accepted": "the guard of an incoherent call",
    "echo.effective_of_another_base_accepted": "the guard of an incoherent call",
    "echo.arguments_not_compared_with_the_record": "the guard of an incoherent call",
    "echo.expiry_question_without_expiry_accepted": "the guard of an incoherent call",
    "echo.anchor_expired_not_typed": "the guard of an incoherent call",
    "echo.record_not_typed": "the guard of an incoherent call",
    "echo.base_not_typed": "the guard of an incoherent call",
    "echo.display_key_is_the_class_only": "the display key is a field the scenarios read, not a claim of the reference",
}


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_every_mutant_is_killed(name):
    diff_changes, echo_changes = MUTANTS[name]
    try:
        module = mutant(diff_changes, echo_changes)
        by_cases = killed_by_the_cases(module.echo_items)
        assert by_cases is not None, f"{name} survives the scenarios and the refusals"
        if name not in SCENARIO_ONLY:
            assert killed_by_the_properties(module.echo_items) is not None, f"{name} survives the properties"
    finally:
        _drop()


def test_the_unmutated_module_survives_every_judge():
    """The control: the harness kills a mutant because the mutation is wrong, not because a judge is."""
    try:
        module = mutant()
        assert killed_by_the_cases(module.echo_items) is None
        assert killed_by_the_properties(module.echo_items) is None
    finally:
        _drop()


def test_the_scenario_only_list_names_real_mutants():
    assert set(SCENARIO_ONLY) <= set(MUTANTS)


def test_there_is_a_mutant_for_every_rule_of_the_issue():
    """eco omitido, eco sin justificación, clase cambiada, tope sin anunciar, defecto de orden sin anunciar, CONTEXT_EXPIRED
    omitido (the six the lane's brief names), each by name."""
    assert {"echo.carried_omitted", "echo.provenance_of_the_carried_reannounced", "echo.unit_inherited_class_changed",
            "echo.topn_cap_unannounced", "echo.order_default_unannounced", "echo.context_expired_omitted"} <= set(MUTANTS)
