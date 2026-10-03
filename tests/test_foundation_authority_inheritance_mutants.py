"""M2A-3b: every rule of the inheritance input dies with its mutant (INV-M2A-07, INV-M2A-17). A test that passes without
killing its mutant does not count.

The technique of ``test_foundation_accepted_contract``: each mutant is the module's SOURCE with ONE replacement, executed
as a module; a BATTERY (a function that returns the list of its failures) runs against the real module (it must be empty)
and against every mutant (it must not be). The batteries are module-agnostic: they compare ``.value`` strings, never classes.

* UNIT (§15.4): the first of two units, an unrecorded bound ignored, a default unit, the donor's own origin as provenance,
  only one side read, donors that are not bounds.
* YEAR / REFERENT (§16.1, §16.3): the first year of a plurianual TIME, a relative window as referent (clock and receipt),
  the first of several TIME, a negative TIME counted, a TIME under OR/NOT accepted, an open interval as absent, the
  granularity ignored (days as months), months off by one, the value dropped, years swapped.
* SHAPE: the discriminated union relaxed in each of its five clauses.
* INV-M2A-07 (the input is derived from the base alone): variants that read the previous record, the anchor, the text's link,
  the chain, the clock or an authority are caught by a PIN of the module's read surface (imports, attributes, signatures).
  One of them (the origin's text link) changes no behavior at all: only the pin can see it, and the test says so.
"""
from __future__ import annotations

import ast
import inspect
import itertools
import sys
import types
from enum import Enum
from pathlib import Path

import pytest

import app.core.foundation_accepted as fa
import app.core.foundation_authority_inheritance as inh
from app.core.interpretation_contract import ContractError
from tests import test_foundation_authority_inheritance_properties as properties
from tests.test_foundation_authority_inheritance import (
    ABSOLUTE, BASE_TRACE, BOUND, EXPECTED_SHAPE, LOGICS, REFERENT_PAYLOAD, SHAPES, UNIT_PAYLOAD, YEAR_PAYLOAD,
    bounds_of, build_base, buyer, expected_unit, topic, when,
)

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "app/core/foundation_authority_inheritance.py"
SOURCE = PATH.read_text(encoding="utf-8")


def _load(source: str) -> types.ModuleType:
    name = "m2a3b_inheritance_mutant"
    module = types.ModuleType(name)
    module.__file__ = "<mutant>"
    sys.modules[name] = module
    try:
        exec(compile(source, "<mutant>", "exec"), module.__dict__)
    finally:
        sys.modules.pop(name, None)
    return module


def mutated_source(*replacements: tuple[str, str]) -> str:
    source = SOURCE
    for old, new in replacements:
        assert source.count(old) == 1, f"mutation anchor not unique: {old!r}"
        source = source.replace(old, new)
    return source


def plain(view: tuple) -> tuple:
    return tuple(item.value if isinstance(item, Enum) else item for item in view)


def attempt(out: list[str], label: str, function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except Exception as error:  # a mutant may fail in any way: that is a failure of the battery, never a pass
        out.append(f"{label}: raised {type(error).__name__}")
        return None


def year_of(result) -> tuple:
    status = result.status.value
    return (status, result.year) if status == "PRESENT" else (status, result.reason.value) if status == "AMBIGUOUS" else (
        status,)


def referent_of(result) -> tuple:
    status = result.status.value
    return ((status, result.first_year, result.last_year, result.months) if status == "PRESENT"
            else (status, result.reason.value) if status == "AMBIGUOUS" else (status,))


# ---- the batteries ---------------------------------------------------------------------------------------------------------
def unit_failures(mod) -> list[str]:
    out: list[str] = []
    for noise, (lower_state, upper_state) in itertools.product((0, 5), itertools.product(BOUND, repeat=2)):
        label = f"unit {lower_state}/{upper_state}/noise{noise}"
        specs = (topic(), buyer(), *bounds_of(lower_state, upper_state))
        result = attempt(out, label, mod.unit_inheritance, build_base(specs, noise=noise), base_trace_id=BASE_TRACE)
        if result is None:
            continue
        status, unit, reason = expected_unit(lower_state, upper_state)
        got = (result.status.value, result.unit and result.unit.value, result.reason and result.reason.value)
        if got != (status.value, unit and unit.value, reason and reason.value):
            out.append(f"{label}: {got}")
        if status.value == "PRESENT" and (result.inherited_from, result.source_condition_ids) != (
                BASE_TRACE, tuple(c.cid for c in bounds_of(lower_state, upper_state))):
            out.append(f"{label}: provenance {result.inherited_from} {result.source_condition_ids}")
    return out


def time_cases() -> list[tuple]:
    """(label, specs, kwargs, expected year view, expected referent view), all from the contract's tables."""
    relative = ("AMBIGUOUS", "RELATIVE_WINDOW")
    cases = []
    for shape, (start_end, (year, referent)) in ((s, (SHAPES[s], EXPECTED_SHAPE[s])) for s in sorted(SHAPES)):
        for source in ABSOLUTE:
            cases.append((f"{shape}/{source}", (topic(), when(*start_end, source=source)), {}, plain(year), plain(referent)))
        cases.append((f"{shape}/clock", (when(*start_end, source=fa.YearSource.RELATIVE_TO_CLOCK),), {}, relative, relative))
        cases.append((f"{shape}/receipt", (when(*start_end, source=fa.YearSource.PREVIOUS_ACCEPTED_TIME,
                                                rules=("time.relative_to_previous",)),), {}, relative, relative))
        cases.append((f"{shape}/inherited-receipt", (when(*start_end, source=fa.YearSource.PREVIOUS_ACCEPTED_TIME,
                                                          rules=("year.inherited",)),), {}, plain(year), plain(referent)))
    absent = (("ABSENT",), ("ABSENT",))
    cases += [("none", (topic(),), {}, *absent),
              ("negative", (topic(), when(*SHAPES["year"], excluded=True)), {}, *absent),
              ("positive+negative", (when(*SHAPES["year"]), when(*SHAPES["month"], cid="c4", excluded=True)), {},
               ("PRESENT", 2024), ("PRESENT", 2024, 2024, 12))]
    several = ("AMBIGUOUS", "MULTIPLE_TIMES")
    for first, second in itertools.combinations_with_replacement(["year", "month", "three_years"], 2):
        cases.append((f"several {first}+{second}", (when(*SHAPES[first]), when(*SHAPES[second], cid="c4")), {}, several,
                      several))
    for name, (logic, inside) in sorted(LOGICS.items()):
        for excluded in (False, True):
            expected = (("AMBIGUOUS", "NONCONJUNCTIVE_TIME"),) * 2 if inside else (
                absent if excluded else (("PRESENT", 2024), ("PRESENT", 2024, 2024, 12)))
            cases.append((f"logic {name}/{excluded}", (topic(), buyer(), when(*SHAPES["year"], excluded=excluded)),
                          {"logic": logic}, *expected))
    cases.append(("relative+several", (when(*SHAPES["year"], source=fa.YearSource.RELATIVE_TO_CLOCK),
                                       when(*SHAPES["month"], cid="c4")), {}, several, several))
    cases.append(("open+relative", (when(*SHAPES["open_from"], source=fa.YearSource.RELATIVE_TO_CLOCK),), {}, relative, relative))
    return cases


def time_failures(mod) -> list[str]:
    out: list[str] = []
    for label, specs, kwargs, expected_year, expected_referent in time_cases():
        for noise in (0, 7):
            base = build_base(specs, noise=noise, **kwargs)
            year = attempt(out, f"{label}/year", mod.year_inheritance, base, base_trace_id=BASE_TRACE)
            referent = attempt(out, f"{label}/referent", mod.time_referent, base, base_trace_id=BASE_TRACE)
            if year is not None and year_of(year) != expected_year:
                out.append(f"{label}/noise{noise}: year {year_of(year)}")
            if referent is not None and referent_of(referent) != expected_referent:
                out.append(f"{label}/noise{noise}: referent {referent_of(referent)}")
            if year is not None and year.status.value == "PRESENT" and (year.condition_id, year.inherited_from) != (
                    "c3", BASE_TRACE):
                out.append(f"{label}: year provenance")
            if referent is not None and referent.status.value == "PRESENT" and (
                    referent.condition_id, referent.inherited_from) != ("c3", BASE_TRACE):
                out.append(f"{label}: referent provenance")
    for shape in sorted(SHAPES):  # the value of a referent is the base's own time@1 encoding
        spec = when(*SHAPES[shape])
        referent = attempt(out, f"value {shape}", mod.time_referent, build_base((topic(), spec)), base_trace_id=BASE_TRACE)
        if referent is not None and referent.status.value == "PRESENT" and referent.value != spec.value.encode():
            out.append(f"value {shape}: not the base's encoding")
    return out


def check_failures(mod) -> list[str]:
    """The arguments: only an accepted interpretation is a base, and the trace id is an identifier even when nothing is
    inherited (the input is derived from the typed base; nothing else may stand in for it)."""
    out: list[str] = []
    good = build_base((topic(),))
    for name in ("unit_inheritance", "year_inheritance", "time_referent", "host_inheritance_inputs"):
        function = getattr(mod, name)
        for not_a_base in (None, {}, good.to_dict()):
            try:
                function(not_a_base, base_trace_id=BASE_TRACE)
                out.append(f"{name}: accepted {type(not_a_base).__name__} as a base")
            except ContractError as error:
                if "INHERITANCE_BASE_NOT_ACCEPTED" not in str(error):
                    out.append(f"{name}: wrong code {error}")
            except Exception as error:
                out.append(f"{name}: {type(error).__name__} instead of ContractError")
        for bad in (None, "", 7):
            try:
                function(good, base_trace_id=bad)
                out.append(f"{name}: accepted base_trace_id={bad!r}")
            except ContractError:
                pass
            except Exception as error:
                out.append(f"{name}: {type(error).__name__} instead of ContractError")
    return out


def cross_check_failures(mod) -> list[str]:
    """A TIME whose dates disagree with ``time_years`` (a drifted codec) is an error, never a different year."""
    out: list[str] = []
    base = build_base((when(*SHAPES["year"]),))
    original = mod.time_years
    mod.time_years = lambda condition: (1999, 1999)
    try:
        for name in ("year_inheritance", "time_referent"):
            try:
                getattr(mod, name)(base, base_trace_id=BASE_TRACE)
                out.append(f"{name}: a disagreement passed")
            except ContractError:
                pass
            except Exception as error:
                out.append(f"{name}: {type(error).__name__}")
    finally:
        mod.time_years = original
    return out


def shape_failures(mod) -> list[str]:
    """The discriminated union (status, reason, payload) of each input, walked in full on the module's own classes."""
    out: list[str] = []
    allowed = {"UnitInheritance": {a.value for a in inh.UNIT_AMBIGUITIES}, "YearInheritance": {a.value for a in inh.YEAR_AMBIGUITIES},
               "TimeReferent": {a.value for a in inh.REFERENT_AMBIGUITIES}}
    payloads = {"UnitInheritance": UNIT_PAYLOAD, "YearInheritance": YEAR_PAYLOAD, "TimeReferent": REFERENT_PAYLOAD}
    for name, payload in payloads.items():
        kind = getattr(mod, name)
        for status, reason, with_payload in itertools.product(mod.Presence, (None, *mod.Ambiguity), (False, True)):
            fields = {**(payload if with_payload else {}), "status": status, "reason": reason}
            ok = ((with_payload and reason is None) if status.value == "PRESENT" else
                  (not with_payload and reason is None) if status.value == "ABSENT" else
                  (not with_payload and reason is not None and reason.value in allowed[name]))
            try:
                kind(**fields)
                built = True
            except ContractError:
                built = False
            if built != ok:
                out.append(f"{name}({status.value}, {reason and reason.value}, payload={with_payload}): built={built}")
        for field_name in payload:  # one field missing from a PRESENT input
            try:
                kind(status=mod.Presence.PRESENT, **{k: v for k, v in payload.items() if k != field_name})
                out.append(f"{name}: PRESENT without {field_name}")
            except ContractError:
                pass
        mistyped = {"unit": "MILLONES", "inherited_from": 7, "source_condition_ids": ["v1"], "year": True, "condition_id": "",
                    "value": ["time@1"], "first_year": "2024", "last_year": 0, "months": False}
        for field_name in payload:
            try:
                kind(status=mod.Presence.PRESENT, **{**payload, field_name: mistyped[field_name]})
                out.append(f"{name}: PRESENT with a mistyped {field_name}")
            except ContractError:
                pass
    return out


BATTERIES = {"unit": unit_failures, "time": time_failures, "check": check_failures, "cross": cross_check_failures,
             "shape": shape_failures}


def test_the_real_module_passes_every_battery():
    for name, battery in BATTERIES.items():
        assert battery(inh) == [], name


# ---- the behavioral mutants ----------------------------------------------------------------------------------------------------
BOUNDS = "    bounds = [c for c in base.conditions if c.dimension is SemanticDimension.VALUE]"
LAST_YEARS = "    if extent[0] != extent[1]:\n        return YearInheritance(Presence.AMBIGUOUS, reason=Ambiguity.MULTI_YEAR)\n"
SEVERAL = "    if len(positive) > 1:\n        return Ambiguity.MULTIPLE_TIMES\n"
NONCONJ = "    if base.logic is not None and any(base.logic.has_nonconjunctive_atom(c.condition_id) for c in times):"
RELATIVE = "    return Ambiguity.RELATIVE_WINDOW if _is_relative(positive[0]) else positive[0]"
RECEIPT = "\n            or RELATIVE_TO_PREVIOUS_RULE in condition.receipt_rules)"
CLOCK_MARK = "condition.provenance.year_source is YearSource.RELATIVE_TO_CLOCK"
UNALIGNED = "        return years[0], years[1], None\n"
MONTHS = "(after.year - first.year) * 12 + after.month - first.month"
OPEN_YEAR = "        return YearInheritance(Presence.AMBIGUOUS, reason=Ambiguity.OPEN_INTERVAL)"
OPEN_REFERENT = "        return TimeReferent(Presence.AMBIGUOUS, reason=Ambiguity.OPEN_INTERVAL)"
REFERENT_FIELDS = "active.condition_id, active.value, extent[0], extent[1], extent[2],"
CHECK_BASE = "    if type(base) is not AcceptedInterpretation:\n        _fail(\"INHERITANCE_BASE_NOT_ACCEPTED\")\n"
CHECK_TRACE = "    if not _text_ok(base_trace_id):\n        _fail(\"INHERITANCE_BASE_TRACE_ID\")\n"
CROSS = "    if years != (first.year, (after - timedelta(days=1)).year):\n        _fail(\"TIME_YEARS_DISAGREE\")\n"

MUTANTS = {  # name: (battery, replacements)
    # UNIT §15.4
    "unit_first_of_two_units": ("unit", [("    if len(units) > 1:", "    if False:"),
                                         ("    (unit,) = units", "    unit = sorted(units, key=lambda u: u.value)[0]")]),
    "unit_unrecorded_bound_ignored": ("unit", [("    if any(c.provenance.unit is None for c in bounds):", "    if False:"),
                                               ("    (unit,) = units", "    unit = next(iter(units), None)")]),
    "unit_defaults_to_pesos": ("unit", [("        return UnitInheritance(Presence.ABSENT)",
                                         "        return UnitInheritance(Presence.PRESENT, Unit.PESOS, base_trace_id, ('x',))")]),
    "unit_provenance_from_the_donor_origin": ("unit", [("Presence.PRESENT, unit, base_trace_id, tuple(",
                                                        "Presence.PRESENT, unit, bounds[0].origin.trace_id, tuple(")]),
    "unit_reads_only_the_lower_bound": ("unit", [(BOUNDS, "    bounds = [c for c in base.conditions if c.dimension is "
                                                          "SemanticDimension.VALUE and c.slot.side.value == 'lower']")]),
    "unit_donors_are_every_condition": ("unit", [("tuple(c.condition_id for c in bounds))",
                                                  "tuple(c.condition_id for c in base.conditions))")]),
    # YEAR §16.1 / REFERENT §16.3
    "year_first_of_a_plurianual_time": ("time", [(LAST_YEARS, "")]),
    "time_relative_window_accepted": ("time", [(RELATIVE, "    return positive[0]")]),
    "time_relative_receipt_ignored": ("time", [(RECEIPT, ")")]),
    "time_relative_clock_marker_ignored": ("time", [(CLOCK_MARK, "False")]),
    "time_first_of_several": ("time", [(SEVERAL, "")]),
    "time_negative_counted": ("time", [("    positive = [c for c in times if c.polarity is Polarity.POSITIVE]",
                                        "    positive = list(times)")]),
    "time_nonconjunctive_accepted": ("time", [(NONCONJ, "    if False:")]),
    "year_open_interval_is_absent": ("time", [(OPEN_YEAR, "        return YearInheritance(Presence.ABSENT)")]),
    "referent_granularity_ignored": ("time", [(UNALIGNED, "        return years[0], years[1], max(1, (after - first).days // 30)\n")]),
    "referent_months_off_by_one": ("time", [(MONTHS, MONTHS + " + 1")]),
    "referent_open_reason_swapped": ("time", [(OPEN_REFERENT, "        return TimeReferent(Presence.AMBIGUOUS, "
                                                              "reason=Ambiguity.INCOMPATIBLE_GRANULARITY)")]),
    "referent_value_dropped": ("time", [(REFERENT_FIELDS, "active.condition_id, active.value[:1], extent[0], extent[1], extent[2],")]),
    "referent_years_swapped": ("time", [(REFERENT_FIELDS, "active.condition_id, active.value, extent[1], extent[0], extent[2],")]),
    # the arguments
    "base_of_any_type": ("check", [(CHECK_BASE, "")]),
    "base_trace_id_unchecked": ("check", [(CHECK_TRACE, "")]),
    "dates_not_cross_checked": ("cross", [(CROSS, "")]),
    # SHAPE: the discriminated union relaxed in each clause
    "shape_absent_may_carry_a_payload": ("shape", [("        if reason is not None or payload:", "        if reason is not None:")]),
    "shape_present_may_be_partial": ("shape", [("        if reason is not None or not complete:", "        if reason is not None:")]),
    "shape_present_may_carry_a_reason": ("shape", [("        if reason is not None or not complete:", "        if not complete:")]),
    "shape_ambiguous_with_any_reason": ("shape", [(" or reason not in allowed or payload:", " or payload:")]),
    "shape_ambiguous_may_carry_a_payload": ("shape", [(" or reason not in allowed or payload:", " or reason not in allowed:")]),
    "shape_bool_is_an_int": ("shape", [("    return type(value) is int and value >= low", "    return isinstance(value, int) and value >= low")]),
    "shape_unit_any_type": ("shape", [("type(self.unit) is Unit and", "self.unit is not None and")]),
}


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_every_behavioral_mutant_is_killed(name):
    battery, replacements = MUTANTS[name]
    assert BATTERIES[battery](_load(mutated_source(*replacements))), f"mutant {name} survived the {battery} battery"


# The generated properties are not decoration either: run against the mutants of each rule they must fail too.
PROPERTIES = (properties.test_p1_the_inputs_are_what_the_independent_oracle_says,
              properties.test_p2_nothing_but_the_conditions_reaches_the_inputs,
              properties.test_p2b_inherited_from_is_the_callers_and_never_the_bases,
              properties.test_p3_a_present_input_never_hides_two_units_several_times_or_several_years,
              properties.test_p5_a_random_pair_of_dates_against_the_oracle_and_the_closed_codec)
KILLED_BY_PROPERTIES = (
    "unit_first_of_two_units", "unit_unrecorded_bound_ignored", "unit_defaults_to_pesos", "unit_provenance_from_the_donor_origin",
    "unit_reads_only_the_lower_bound", "unit_donors_are_every_condition", "year_first_of_a_plurianual_time",
    "time_relative_window_accepted", "time_relative_receipt_ignored", "time_relative_clock_marker_ignored",
    "time_first_of_several", "time_negative_counted", "time_nonconjunctive_accepted", "year_open_interval_is_absent",
    "referent_granularity_ignored", "referent_months_off_by_one", "referent_value_dropped",
)


@pytest.mark.parametrize("name", KILLED_BY_PROPERTIES)
def test_the_generated_properties_also_kill_the_mutants_of_each_rule(name, monkeypatch):
    mutant = _load(mutated_source(*MUTANTS[name][1]))
    monkeypatch.setattr(properties, "inh", mutant)
    for generated in PROPERTIES:
        try:
            generated()
        except Exception:  # a failed property (or a mutant that cannot even build its answer): the mutant is seen
            return
    pytest.fail(f"mutant {name} survived every generated property")


# ---- INV-M2A-07: the input is derived from the base alone ---------------------------------------------------------------------
# The module's whole read surface, pinned. Adding a read (another attribute, import or argument) is a decision to review.
ALLOWED_IMPORTS = frozenset({
    ("__future__", ("annotations",)), ("dataclasses", ("dataclass",)), ("datetime", ("date", "timedelta")),
    ("enum", ("Enum",)),
    ("app.core.foundation_accepted", ("AcceptedCondition", "AcceptedInterpretation", "Unit", "YearSource", "time_years")),
    ("app.core.interpretation_contract", ("ContractError", "Polarity")),
    ("app.core.semantic_dimensions", ("SemanticDimension",)),
})
READ_SURFACE = frozenset({  # attribute names the module may touch: the base's conditions, their typed facts, nothing else
    "ABSENT", "AMBIGUOUS", "INCOMPATIBLE_GRANULARITY", "MULTIPLE_TIMES", "MULTIPLE_UNITS", "MULTI_YEAR",
    "NONCONJUNCTIVE_TIME", "OPEN_INTERVAL", "POSITIVE", "PRESENT", "RELATIVE_TO_CLOCK", "RELATIVE_WINDOW", "TIME",
    "UNIT_UNRECORDED", "VALUE", "condition_id", "conditions", "day", "dimension", "first_year", "fromisoformat",
    "has_nonconjunctive_atom", "inherited_from", "last_year", "logic", "month", "months", "polarity", "provenance",
    "reason", "receipt_rules", "referent", "source_condition_ids", "startswith", "status", "unit", "value", "year",
    "year_source",
})
FORBIDDEN_NAMES = frozenset({"open", "eval", "exec", "compile", "input", "__import__", "globals", "getattr", "setattr",
                             "resolve"})
PUBLIC = ("unit_inheritance", "year_inheritance", "time_referent", "host_inheritance_inputs")


def purity_problems(source: str) -> list[str]:
    """Why a source is not «derived from the typed base alone»: an import outside the pinned set (a store, a record, a
    clock, an authority), an attribute outside the pinned read surface (the origin's text link, the chain, the requests,
    the policies, ``today``), a forbidden call (``resolve``, dynamic access), or a public function that takes anything
    but ``(base, *, base_trace_id)``."""
    tree = ast.parse(source)
    out: list[str] = []
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imports.add((node.module, tuple(sorted(alias.name for alias in node.names))))
        elif isinstance(node, ast.Import):
            out += [f"import {alias.name}" for alias in node.names]
        elif isinstance(node, ast.Attribute) and node.attr not in READ_SURFACE:
            out.append(f"reads .{node.attr}")
        elif isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            out.append(f"uses {node.id}")
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in FORBIDDEN_NAMES:
                out.append(f"defines {node.name}")
            if node.name in PUBLIC:
                arguments = ([a.arg for a in node.args.args], [a.arg for a in node.args.kwonlyargs], node.args.vararg,
                             node.args.kwarg)
                if arguments != (["base"], ["base_trace_id"], None, None):
                    out.append(f"{node.name} takes {arguments[:2]}")
    out += [f"import from {module}: {names}" for module, names in sorted(imports - ALLOWED_IMPORTS, key=str)]
    return out


def signature_failures(mod) -> list[str]:
    out = []
    for name in PUBLIC:
        parameters = list(inspect.signature(getattr(mod, name)).parameters.values())
        if [(p.name, p.kind) for p in parameters] != [("base", inspect.Parameter.POSITIONAL_OR_KEYWORD),
                                                      ("base_trace_id", inspect.Parameter.KEYWORD_ONLY)]:
            out.append(f"{name}{inspect.signature(getattr(mod, name))}")
    return out


def test_the_real_module_reads_only_the_typed_base():
    assert purity_problems(SOURCE) == []
    assert signature_failures(inh) == []
    assert not hasattr(inh, "resolve")  # INV-M2A-07: nothing here is an authority and nothing of an authority is called


SOURCE_MUTANTS = {  # name: (replacements, why it is the INV-M2A-07 violation it is)
    "reads_the_origin_text_link": [("    units = {c.provenance.unit for c in bounds if c.provenance.unit is not None}",
                                    "    units = {c.provenance.unit for c in bounds if c.provenance.unit is not None "
                                    "and c.origin.raw_sha256}")],
    "reads_the_chain_of_the_base": [(BOUNDS, "    bounds = [c for c in base.conditions if c.dimension is SemanticDimension.VALUE "
                                             "and base.base_trace_id is None]")],
    "reads_the_requests": [(BOUNDS, "    bounds = [c for c in base.conditions if c.dimension is SemanticDimension.VALUE "
                                    "and base.requests]")],
    "reads_the_policies": [("    _check(base, base_trace_id)\n    bounds", "    _check(base, base_trace_id)\n    _ = base.policies\n    bounds")],
    "takes_an_anchor_argument": [("def unit_inheritance(base: AcceptedInterpretation, *, base_trace_id: str) -> UnitInheritance:",
                                  "def unit_inheritance(base: AcceptedInterpretation, *, base_trace_id: str, anchor=None)"
                                  " -> UnitInheritance:")],
    "takes_a_record_instead_of_the_trace": [("def year_inheritance(base: AcceptedInterpretation, *, base_trace_id: str)",
                                             "def year_inheritance(base: AcceptedInterpretation, record, *, base_trace_id: str)")],
    "takes_the_turn_text": [("def time_referent(base: AcceptedInterpretation, *, base_trace_id: str)",
                             "def time_referent(base: AcceptedInterpretation, *, base_trace_id: str, text: str = '')")],
    "imports_the_store": [("from enum import Enum\n", "from enum import Enum\nimport app.core.foundation_accepted_store as store\n")],
    "imports_a_record_type": [("    AcceptedCondition, AcceptedInterpretation, Unit, YearSource, time_years,",
                               "    AcceptedCondition, AcceptedInterpretation, AcceptedTurnRecord, Unit, YearSource, time_years,")],
    "imports_the_clock": [("from datetime import date, timedelta", "from datetime import date, datetime, timedelta")],
    "reads_the_clock": [("    years = time_years(condition)\n", "    years = time_years(condition)\n    _ = date.today()\n")],
    "calls_an_authority": [("    _check(base, base_trace_id)\n    bounds", "    _check(base, base_trace_id)\n    resolve(base)\n    bounds")],
    "defines_resolve": [("def _fail(code: str):", "def resolve(base):\n    return base\n\n\ndef _fail(code: str):")],
    "reads_by_dynamic_access": [(BOUNDS, "    bounds = [c for c in base.conditions if getattr(c, 'dimension') is SemanticDimension.VALUE]")],
}


@pytest.mark.parametrize("name", sorted(SOURCE_MUTANTS))
def test_every_variant_that_reads_beyond_the_base_is_caught_by_the_pin(name):
    assert purity_problems(mutated_source(*SOURCE_MUTANTS[name])), f"variant {name} reads beyond the base: the pin missed it"


def test_the_signature_pin_also_sees_a_variant_by_behavior():
    mutant = _load(mutated_source(("def unit_inheritance(base: AcceptedInterpretation, *, base_trace_id: str) -> UnitInheritance:",
                                   "def unit_inheritance(base: AcceptedInterpretation, *, base_trace_id: str, anchor=None)"
                                   " -> UnitInheritance:")))
    assert signature_failures(mutant)  # the same variant, read through ``inspect`` instead of the AST
    assert signature_failures(inh) == []


def test_the_origin_text_link_variant_changes_no_behavior_so_only_the_pin_can_see_it():
    """INV-M2A-07 made concrete: a variant that peeks at the origin's text link (``raw_sha256``) passes every behavioral
    battery on bases whose origins are SPANs, exactly as a variant that peeks at a previous record would. Only the pin of
    the read surface rejects it. That is why the pin is a test and not a comment."""
    mutant = _load(mutated_source(*SOURCE_MUTANTS["reads_the_origin_text_link"]))
    assert unit_failures(mutant) == [] and time_failures(mutant) == []
    assert purity_problems(mutated_source(*SOURCE_MUTANTS["reads_the_origin_text_link"]))


def test_the_pin_has_no_false_alarm_on_what_the_module_legitimately_reads():
    """Positive control: reading the base's conditions again changes nothing. And the import list is pinned EXACTLY: a
    shorter one is also a change to review, not a pass."""
    assert purity_problems(mutated_source(("    _check(base, base_trace_id)\n    bounds",
                                           "    _check(base, base_trace_id)\n    _ = base.conditions\n    bounds"))) == []
    assert purity_problems("from app.core.foundation_accepted import time_years\n") != []


def test_a_battery_that_is_blind_would_be_caught():
    """The batteries are not vacuous: a module that answers ABSENT for everything fails every one of them."""
    blind = types.SimpleNamespace(
        unit_inheritance=lambda base, base_trace_id: inh.UnitInheritance(inh.Presence.ABSENT),
        year_inheritance=lambda base, base_trace_id: inh.YearInheritance(inh.Presence.ABSENT),
        time_referent=lambda base, base_trace_id: inh.TimeReferent(inh.Presence.ABSENT),
        host_inheritance_inputs=lambda base, base_trace_id: None,
        UnitInheritance=inh.UnitInheritance, YearInheritance=inh.YearInheritance, TimeReferent=inh.TimeReferent,
        Presence=inh.Presence, Ambiguity=inh.Ambiguity, time_years=inh.time_years)
    assert unit_failures(blind) and time_failures(blind) and check_failures(blind)
