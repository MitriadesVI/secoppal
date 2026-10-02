"""M2a boundary pins (contract §3.7, INV-M2A-04, INV-M2A-24; mission M2A-1a invariants 4, 5 and 6; wave-1 prep).

M2a modules are registered PER STEP in ``tests/m2a_steps/m2a_<n>.py`` (``STEP``, ``MODULES``, ``DEPENDS_ON``,
``CLOSURE``): each lane edits only its own file, never a list shared with another lane.

* REGISTRY: every module is under ``app.core.``, exists, is registered once, in one step; step names are unique and
  ``DEPENDS_ON`` names known steps, never itself, never a cycle.
* IMPORTS: per step, the transitive import closure of its modules MINUS the closure of the steps it depends on (its own
  increment) is EXACTLY its ``CLOSURE``: a change in M2A-1 never breaks the registry of M2A-2. On the union of all steps
  no module reaches ``query_router``, ``followup_engine``, ``followup_mutations``, the conversation store, the
  orchestrator, owner detectors or ``accreditation`` (the contract allows only the future ``accredited_span`` leaf,
  which does not exist yet: adding it means adding it, by name, to the step that needs it).
* NO BRIDGE: no M2a module names ``parsed_params`` or a legacy input type (``QueryFrame``, ``ConversationContext``,
  ``FollowupEngine``, ``QueryRouter``) in code: no function, attribute, argument, keyword or string key.
* NO LEXICON: no M2a module imports a regex engine or a lexicon module (A8.1 and A9.1 decide on typed values).
* NO DYNAMIC IMPORT: no M2a module uses ``__import__``, ``importlib`` or ``import_module``, whatever the argument
  (literal or variable): the static closure above is complete only because nothing in M2a imports at run time.
  These are not resolved, they are forbidden; and an unresolvable dynamic import inside the closure fails the pin.
* CONSUMPTION (definitive, user decision 2026-10-02): internal M2a consumption is allowed ONLY between modules
  explicitly registered, and only along ``DEPENDS_ON`` (a module of step S imports a module of step T only if T is S or
  a transitive dependency of S). No unregistered module of ``app``/``scripts``/``streamlit_app`` imports a registered
  one; neither the legacy closure nor the Foundation runtime reaches M2a (and the legacy closure still stays clear of
  ``interpretation_contract``). Registering a module never opens a door: a legacy module that is registered is still
  forbidden in the closure and still consumed by legacy.

Each pin is shown not to be vacuous by a mutant it must catch (and by a positive control where a mutant could be a false
alarm). The import graph is the AST scanner of ``tests/test_m1_wave1_parity_pins.py`` (static, relative, lazy,
``importlib`` and dotted string literals), extended here with VIRTUAL modules: sources that exist only in memory, so a
mutant can register, import and reach a module without creating a file.
"""
from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st

from tests.m2a_steps import STEPS, Step, registered_modules, step_from_namespace
from tests.test_m1_wave1_parity_pins import ENTRYPOINTS, build_graph, closure, module_edges, module_index

ROOT = Path(__file__).resolve().parents[1]

M2A_MODULES = registered_modules()
MODULE = "app.core.foundation_accepted"  # explicit: the mutants below patch THIS module's source

FORBIDDEN_MODULES = frozenset({
    "app.core.query_router", "app.core.followup_engine", "app.core.followup_mutations",
    "app.core.conversation_store", "app.core.orchestrator", "app.core.foundation_runtime",
})
# M2A-1a consumes closed TYPES (``foundation_authority_values``, ``foundation_geo_level``), never authorities: no
# registry, authority contract, evidence window, host authorities, resolver or ``*_authority`` module.
FORBIDDEN_PREFIXES = ("app.core.accreditation", "app.core.workflow", "app.data", "app.core.host_authorities",
                      "app.core.foundation_authority_registry", "app.core.foundation_authority_contract",
                      "app.core.foundation_evidence_window", "app.core.foundation_admission")
FORBIDDEN_SUFFIXES = ("_authority", "_resolver", "_authorities")
FORBIDDEN_IDENTIFIERS = ("parsed_params", "queryframe", "conversationcontext", "followupengine", "queryrouter",
                         "followup_engine", "query_router", "query_frame", "conversation_context")
REGEX_MODULES = frozenset({"re", "regex", "fnmatch", "sre_compile", "sre_parse"})
# Any name, attribute, alias or string literal equal to one of these is a dynamic import (``getattr(builtins,
# "__import__")`` included); ``importlib`` may not be imported at all.
DYNAMIC_IMPORT_NAMES = frozenset({"__import__", "import_module"})
DYNAMIC_IMPORT_MODULES = frozenset({"importlib"})
LEXICON_MARKERS = ("lexic", "lexeme", "vocab", "stopword", "keyword", "trigger", "synonym", "_terms", "phrases",
                   "text_predicates", "normaliz", "grammar")
LEGACY_MUST_STAY_OUT = "app.core.interpretation_contract"  # the touched wire module: never part of the legacy closure


def _source(module: str) -> str:
    return (ROOT / (module.replace(".", "/") + ".py")).read_text(encoding="utf-8")


# ---- the graph, with virtual modules --------------------------------------------------------------------------------
def _graph(overrides: dict[str, str] | None = None, virtual: dict[str, str] | None = None):
    """``build_graph`` that also takes VIRTUAL modules: (index, edges, modules with an unresolvable dynamic import).

    ``overrides`` replace the source of a module that exists on disk; ``virtual`` adds modules that do not exist. Edges
    to and from a virtual module resolve against an index extended with a fictitious path, so a virtual module can be
    registered, imported and reached. A virtual module that exists on disk, or an override of one that does not, is a
    broken mutant, not a pass.
    """
    overrides, virtual = dict(overrides or {}), dict(virtual or {})
    index, graph, flagged = build_graph()
    if clash := sorted(set(virtual) & set(index)):
        raise ValueError(f"virtual modules that exist on disk: {clash}")
    if missing := sorted(set(overrides) - set(index)):
        raise ValueError(f"overrides of modules that do not exist on disk: {missing}")
    index = {**index, **{name: ROOT / (name.replace(".", "/") + ".py") for name in virtual}}
    graph, flagged = dict(graph), set(flagged)
    for name, source in {**overrides, **virtual}.items():
        edges, dyn = module_edges(source, name, index, is_package=index[name].name == "__init__.py")
        graph[name] = edges
        flagged.discard(name)
        flagged |= dyn
    return index, graph, flagged


# ---- the registry ---------------------------------------------------------------------------------------------------
def _reachable(direct: dict[str, set[str]], start: str) -> set[str]:
    """Steps reachable from ``start`` through at least one edge (``start`` itself only if it is on a cycle)."""
    seen: set[str] = set()
    stack = list(direct.get(start, ()))
    while stack:
        item = stack.pop()
        if item not in seen:
            seen.add(item)
            stack.extend(direct.get(item, ()))
    return seen


def _dependencies(steps: tuple[Step, ...]) -> dict[str, set[str]]:
    """Every step's TRANSITIVE dependencies, never itself (unknown names and cycles are registry problems)."""
    direct = {step.name: set(step.depends_on) for step in steps}
    return {name: _reachable(direct, name) - {name} for name in direct}


def registry_problems(steps: tuple[Step, ...], present: set[str] | frozenset[str]) -> list[str]:
    """Why a set of steps is not a well-formed registry (``present``: the modules that exist)."""
    out: list[str] = []
    names: set[str] = set()
    for step in steps:
        if step.name in names:
            out.append(f"duplicate step {step.name}")
        names.add(step.name)
    owner: dict[str, str] = {}
    for step in steps:
        for module in step.modules:
            if not module.startswith("app.core."):
                out.append(f"{step.name}: {module} is not an app.core module")
            elif module not in present:
                out.append(f"{step.name}: {module} does not exist")
            if module in owner:
                out.append(f"{module} registered twice ({owner[module]} and {step.name})")
            owner.setdefault(module, step.name)
        for position, dep in enumerate(step.depends_on):
            if dep not in names:
                out.append(f"{step.name} depends on unknown step {dep}")
            if dep == step.name:
                out.append(f"{step.name} depends on itself")
            if dep in step.depends_on[:position]:
                out.append(f"{step.name} lists {dep} twice")
    direct = {step.name: {dep for dep in step.depends_on if dep != step.name} for step in steps}
    out += [f"{name} is on a dependency cycle" for name in sorted(direct) if name in _reachable(direct, name)]
    return out


# ---- the pins' functions --------------------------------------------------------------------------------------------
def closure_problems(overrides: dict[str, str] | None = None, virtual: dict[str, str] | None = None,
                     steps: tuple[Step, ...] = STEPS) -> list[str]:
    """Per step: closure(MODULES) - closure(modules of the steps it depends on) == CLOSURE, exactly. On the union:
    nothing forbidden, no unresolvable dynamic import."""
    _, graph, flagged = _graph(overrides, virtual)
    by_name = {step.name: step for step in steps}
    dependencies = _dependencies(steps)

    def reach(modules) -> set[str]:
        return set(closure(graph, entrypoints=tuple(modules), cut=()))

    out: list[str] = []
    for step in steps:
        inherited = reach(module for dep in sorted(dependencies[step.name]) if dep in by_name
                          for module in by_name[dep].modules)
        own = reach(step.modules) - inherited
        out += [f"{step.name}: closure reaches {name}" for name in sorted(own - step.closure)]
        out += [f"{step.name}: closure lost {name} (update the registry on purpose)"
                for name in sorted(step.closure - own)]
    reached = reach(registered_modules(steps))
    out += [f"forbidden {name}" for name in sorted(reached)
            if name in FORBIDDEN_MODULES or name.startswith(FORBIDDEN_PREFIXES) or name.endswith(FORBIDDEN_SUFFIXES)]
    out += [f"unresolvable dynamic import in {name}" for name in sorted(flagged & reached)]
    return out


def direction_problems(overrides: dict[str, str] | None = None, virtual: dict[str, str] | None = None,
                       steps: tuple[Step, ...] = STEPS) -> list[str]:
    """A direct edge from a module of step S to one of step T needs T == S or T a transitive dependency of S."""
    _, graph, _ = _graph(overrides, virtual)
    owners: dict[str, set[str]] = {}
    for step in steps:
        for module in step.modules:
            owners.setdefault(module, set()).add(step.name)
    dependencies = _dependencies(steps)
    out = []
    for step in steps:
        allowed = {step.name} | dependencies[step.name]
        for module in step.modules:
            for target in sorted(graph.get(module, set()) & set(owners)):
                if not owners[target] & allowed:
                    out.append(f"{module} ({step.name}) imports {target} ({', '.join(sorted(owners[target]))}) "
                               f"without DEPENDS_ON")
    return out


def consumer_problems(overrides: dict[str, str] | None = None, virtual: dict[str, str] | None = None,
                      steps: tuple[Step, ...] = STEPS) -> list[str]:
    """The definitive invariant: no module that is not registered imports one that is (direct edges are enough: a
    transitive consumer is a direct importer of something registered, or is itself registered)."""
    _, graph, _ = _graph(overrides, virtual)
    registered = set(registered_modules(steps))
    return [f"{name} imports {target}" for name, edges in sorted(graph.items()) if name not in registered
            for target in sorted(edges & registered)]


def legacy_reach_problems(overrides: dict[str, str] | None = None, virtual: dict[str, str] | None = None,
                          steps: tuple[Step, ...] = STEPS) -> list[str]:
    """Neither the legacy closure (cutting only the declared mode frontier) nor the Foundation runtime (the same
    closure through the frontier) reaches a registered module; the legacy closure also stays clear of the IR wire."""
    _, graph, _ = _graph(overrides, virtual)
    registered = set(registered_modules(steps))
    legacy = set(closure(graph))
    runtime = set(closure(graph, cut=()))
    out = [f"legacy closure reaches {name}" for name in sorted(legacy & registered)]
    out += [f"Foundation runtime closure reaches {name}" for name in sorted((runtime - legacy) & registered)]
    if LEGACY_MUST_STAY_OUT in legacy:
        out.append(f"legacy closure reaches {LEGACY_MUST_STAY_OUT}")
    return out


def _docstring_nodes(tree: ast.AST) -> set[int]:
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                out.add(id(first.value))
    return out


def source_problems(source: str) -> list[str]:
    """Bridge identifiers, regex engines and lexicon imports in one M2a module's code (docstrings excluded)."""
    tree = ast.parse(source)
    docstrings = _docstring_nodes(tree)
    out = []
    for node in ast.walk(tree):
        names: list[str] = []
        if isinstance(node, ast.Name):
            names.append(node.id)
        elif isinstance(node, ast.Attribute):
            names.append(node.attr)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.arg):
            names.append(node.arg)
        elif isinstance(node, ast.keyword) and node.arg:
            names.append(node.arg)
        elif isinstance(node, ast.alias):
            names.extend(filter(None, (node.name, node.asname)))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
            names.append(node.value)
        if isinstance(node, ast.Call) and (
                (isinstance(node.func, ast.Attribute) and node.func.attr == "resolve")
                or (isinstance(node.func, ast.Name) and node.func.id == "resolve")):
            out.append(f"authority call resolve() (line {node.lineno})")
        for name in names:
            if name in DYNAMIC_IMPORT_NAMES:
                out.append(f"dynamic import {name!r} (line {getattr(node, 'lineno', '?')})")
            folded = name.lower()
            hit = next((ident for ident in FORBIDDEN_IDENTIFIERS if ident in folded), None)
            if hit:
                out.append(f"bridge identifier {name!r} (line {getattr(node, 'lineno', '?')})")
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            modules = [node.module or ""] + [f"{node.module}.{alias.name}" for alias in node.names]
        else:
            continue
        for module in modules:
            if module.split(".")[0] in DYNAMIC_IMPORT_MODULES:
                out.append(f"dynamic import machinery {module!r}")
            if module.split(".")[0] in REGEX_MODULES:
                out.append(f"regex engine import {module!r}")
            if any(marker in module.lower() for marker in LEXICON_MARKERS):
                out.append(f"lexicon import {module!r}")
    return out


# ---- the pins ------------------------------------------------------------------------------------------------------
def test_the_step_registry_is_well_formed():
    assert registry_problems(STEPS, set(module_index())) == []


def test_every_registry_file_is_loaded_and_the_registry_sees_the_real_modules():
    stems = {path.stem for path in (ROOT / "tests" / "m2a_steps").glob("m2a_*.py")}
    assert {step.stem for step in STEPS} == stems and stems
    _, graph, _ = _graph()
    assert M2A_MODULES and set(M2A_MODULES) <= set(graph)  # a registered module the graph cannot see pins nothing


def test_the_m2a_import_closure_is_exactly_the_registry():
    assert closure_problems() == []


@pytest.mark.parametrize("module", M2A_MODULES)
def test_no_bridge_no_regex_no_lexicon(module):
    assert source_problems(_source(module)) == []


def test_m2a_is_consumed_only_by_registered_m2a_modules():
    """Replaces the temporary «M2A has no consumer yet»: only registered modules import registered modules."""
    assert consumer_problems() == []


def test_internal_m2a_consumption_follows_the_declared_dependencies():
    assert direction_problems() == []


def test_the_legacy_closure_and_the_foundation_runtime_never_reach_m2a():
    assert legacy_reach_problems() == []


# ---- the graph with virtual modules is itself checked ----------------------------------------------------------------
VIRTUAL_A = "app.core.m2a_virtual_a"
VIRTUAL_B = "app.core.m2a_virtual_b"
VIRTUAL_C = "app.core.m2a_virtual_c"
ON_M2A_1 = f"from {MODULE.rsplit('.', 1)[0]} import {MODULE.rsplit('.', 1)[1]}\n"  # imports foundation_accepted
IMPORT_A = "from app.core import m2a_virtual_a\n"
IMPORT_B = "from app.core import m2a_virtual_b\n"
IMPORT_C = "from app.core import m2a_virtual_c\n"
LATE_IMPORT_OF_A = "\n\ndef _m2a():\n    import app.core.m2a_virtual_a\n"  # appended to an on-disk module
LATE_IMPORT_OF_IR = f"\n\ndef _late():\n    import {LEGACY_MUST_STAY_OUT}\n"
LATE_IMPORT_OF_M2A = f"\n\ndef _m2a():\n    import {MODULE}\n"


def _step(name: str, modules=(), depends=(), closure=()) -> Step:
    return Step(name, name.lower().replace("-", "_"), tuple(modules), tuple(depends), frozenset(closure))


def _swap(steps: tuple[Step, ...], name: str, **fields) -> tuple[Step, ...]:
    return tuple(replace(step, **fields) if step.name == name else step for step in steps)


def _real(name: str) -> Step:
    return next(step for step in STEPS if step.name == name)


_DISK_MODULES = frozenset(build_graph()[0])


def _present(*extra: str) -> set[str]:
    return set(_DISK_MODULES) | set(extra)


def _all_problems(overrides=None, virtual=None, steps=STEPS) -> dict[str, list[str]]:
    return {
        "registry": registry_problems(steps, _present(*(virtual or {}))),
        "closure": closure_problems(overrides, virtual, steps),
        "direction": direction_problems(overrides, virtual, steps),
        "consumer": consumer_problems(overrides, virtual, steps),
        "legacy": legacy_reach_problems(overrides, virtual, steps),
    }


def test_a_virtual_module_resolves_edges_in_both_directions():
    _, graph, _ = _graph(virtual={VIRTUAL_A: ON_M2A_1, VIRTUAL_B: IMPORT_A})
    assert MODULE in graph[VIRTUAL_A] and VIRTUAL_A in graph[VIRTUAL_B] and VIRTUAL_A not in graph[MODULE]
    _, graph, _ = _graph({MODULE: _source(MODULE) + "\n\ndef _late():\n    import app.core.m2a_virtual_a\n"},
                         {VIRTUAL_A: ""})
    assert VIRTUAL_A in graph[MODULE]  # an on-disk module can import a virtual one


def test_a_broken_mutant_is_refused_not_passed():
    with pytest.raises(ValueError):
        _graph(virtual={MODULE: ""})  # exists on disk: it is not virtual
    with pytest.raises(ValueError):
        _graph({VIRTUAL_A: ""})  # an override of a module that does not exist


def test_the_graph_with_no_mutation_is_the_disk_graph():
    index, graph, flagged = build_graph()
    index2, graph2, flagged2 = _graph()
    assert (index2, graph2, flagged2) == (index, graph, flagged)
    assert VIRTUAL_A not in index  # the cached disk index is never polluted by a virtual module


# ---- non-vacuity: the registry ---------------------------------------------------------------------------------------
def _namespace(**changes) -> dict:
    namespace = {"STEP": "M2A-7", "MODULES": (), "DEPENDS_ON": (), "CLOSURE": frozenset()}
    namespace.update(changes)
    return {key: value for key, value in namespace.items() if value is not ...}


def test_a_well_formed_registry_file_is_accepted_and_private_names_are_free():
    step, problems = step_from_namespace("m2a_7", _namespace(MODULES=("app.core.x",), DEPENDS_ON=("M2A-1",),
                                                              CLOSURE=frozenset({"app.core.x"})))
    assert problems == [] and step == _step("M2A-7", ("app.core.x",), ("M2A-1",), {"app.core.x"})
    assert step_from_namespace("m2a_7", _namespace(_BASE=1, helper=2, __doc__="x"))[1] == []


@pytest.mark.parametrize("changes, expected", [
    ({"EXTRA": 1}, "unknown field EXTRA"),
    ({"CLOSURE_EXTRA": frozenset()}, "unknown field CLOSURE_EXTRA"),
    ({"CLOSURE": ...}, "missing field CLOSURE"),
    ({"DEPENDS_ON": ...}, "missing field DEPENDS_ON"),
    ({"MODULES": ...}, "missing field MODULES"),
    ({"STEP": ...}, "missing field STEP"),
    ({"STEP": "M2A-8"}, "does not name this file"),
    ({"STEP": "m2a_7"}, "does not name this file"),
    ({"STEP": 7}, "does not name this file"),
    ({"MODULES": ["app.core.x"]}, "MODULES must be a tuple of str"),
    ({"MODULES": "app.core.x"}, "MODULES must be a tuple of str"),
    ({"MODULES": ("app.core.x", 1)}, "MODULES must be a tuple of str"),
    ({"DEPENDS_ON": ["M2A-1"]}, "DEPENDS_ON must be a tuple of str"),
    ({"DEPENDS_ON": "M2A-1"}, "DEPENDS_ON must be a tuple of str"),
    ({"DEPENDS_ON": (1,)}, "DEPENDS_ON must be a tuple of str"),
    ({"CLOSURE": {"app"}}, "CLOSURE must be a frozenset of str"),
    ({"CLOSURE": ("app",)}, "CLOSURE must be a frozenset of str"),
    ({"CLOSURE": frozenset({1})}, "CLOSURE must be a frozenset of str"),
])
def test_mutant_a_malformed_registry_file_is_refused(changes, expected):
    step, problems = step_from_namespace("m2a_7", _namespace(**changes))
    assert step is None and any(expected in problem for problem in problems), problems


def _two(first=(), second=(), depends=()) -> tuple[Step, ...]:
    return _step("M2A-P1", first), _step("M2A-P2", second, depends)


def test_registry_control_a_clean_synthetic_registry_has_no_problem():
    steps = (_step("M2A-P1", (VIRTUAL_A,)), _step("M2A-P2", (VIRTUAL_B,), ("M2A-P1",)),
             _step("M2A-P3", (VIRTUAL_C,), ("M2A-P1", "M2A-P2")))
    assert registry_problems(steps, _present(VIRTUAL_A, VIRTUAL_B, VIRTUAL_C)) == []


@pytest.mark.parametrize("steps, expected", [
    (lambda: _two((VIRTUAL_A,), (VIRTUAL_A,)), "registered twice"),
    (lambda: _two((VIRTUAL_A, VIRTUAL_A)), "registered twice"),
    (lambda: _two((MODULE,), (MODULE,)), "registered twice"),
    (lambda: _two(("app.main",)), "is not an app.core module"),
    (lambda: _two(("scripts.run_radar_scheduler",)), "is not an app.core module"),
    (lambda: _two(("streamlit_app",)), "is not an app.core module"),
    (lambda: _two(("app.core_other.x",)), "is not an app.core module"),
    (lambda: _two(("app.core",)), "is not an app.core module"),
    (lambda: _two(("app.core.m2a_no_such_module",)), "does not exist"),
    (lambda: _two(depends=("M2A-NOPE",)), "depends on unknown step M2A-NOPE"),
    (lambda: (_step("M2A-P1", depends=("M2A-P1",)),), "depends on itself"),
    (lambda: (_step("M2A-P1", depends=("M2A-P2",)), _step("M2A-P2", depends=("M2A-P1",))), "dependency cycle"),
    (lambda: (_step("M2A-P1", depends=("M2A-P3",)), _step("M2A-P2", depends=("M2A-P1",)),
              _step("M2A-P3", depends=("M2A-P2",))), "dependency cycle"),
    (lambda: (_step("M2A-P1"), _step("M2A-P2", depends=("M2A-P1", "M2A-P1"))), "lists M2A-P1 twice"),
    (lambda: (_step("M2A-P1"), _step("M2A-P1")), "duplicate step M2A-P1"),
])
def test_mutant_a_malformed_registry_is_caught(steps, expected):
    problems = registry_problems(steps(), _present())
    assert any(expected in problem for problem in problems), problems


def test_a_cycle_is_reported_on_its_members_only():
    steps = (_step("M2A-P1"), _step("M2A-P2", depends=("M2A-P3",)), _step("M2A-P3", depends=("M2A-P2",)),
             _step("M2A-P4", depends=("M2A-P3",)))
    cyclic = [problem for problem in registry_problems(steps, _present()) if "cycle" in problem]
    assert sorted(cyclic) == ["M2A-P2 is on a dependency cycle", "M2A-P3 is on a dependency cycle"]


def _closed_dependencies(direct: dict[str, set[str]]) -> dict[str, set[str]]:
    """Independent oracle: a fixed point instead of a graph walk."""
    reach = {name: set(deps) for name, deps in direct.items()}
    changed = True
    while changed:
        changed = False
        for name in reach:
            grown = reach[name].union(*(reach[dep] for dep in reach[name]))
            if grown != reach[name]:
                reach[name], changed = grown, True
    return reach


@settings(max_examples=100, deadline=None)
@given(st.integers(min_value=1, max_value=7), st.data())
def test_property_the_registry_accepts_any_dag_and_catches_the_edge_that_closes_a_cycle(count, data):
    names = [f"M2A-H{n}" for n in range(count)]
    steps = tuple(_step(name, (f"app.core.m2a_h{n}",),
                        data.draw(st.lists(st.sampled_from(names[:n]), unique=True, max_size=n)) if n else ())
                  for n, name in enumerate(names))
    present = _present(*(module for step in steps for module in step.modules))
    assert registry_problems(steps, present) == []
    dependencies = _dependencies(steps)
    assert dependencies == {name: closed - {name} for name, closed in
                            _closed_dependencies({step.name: set(step.depends_on) for step in steps}).items()}
    pairs = [(ancestor, name) for name in names for ancestor in sorted(dependencies[name])]
    if pairs:
        ancestor, descendant = data.draw(st.sampled_from(pairs))
        mutated = tuple(replace(step, depends_on=(*step.depends_on, descendant)) if step.name == ancestor else step
                        for step in steps)
        assert f"{ancestor} is on a dependency cycle" in registry_problems(mutated, present)


# ---- non-vacuity: closure per step -----------------------------------------------------------------------------------
STEP_1 = _real("M2A-1")


def _synthetic(closure_extra: frozenset[str] | set[str] = frozenset(), body: str = ON_M2A_1):
    steps = (*STEPS, _step("M2A-S1", (VIRTUAL_A,), ("M2A-1",), {VIRTUAL_A, *closure_extra}))
    return {VIRTUAL_A: body}, steps


def test_closure_control_a_virtual_step_that_only_adds_itself_is_clean_on_every_pin():
    virtual, steps = _synthetic()
    assert _all_problems(virtual=virtual, steps=steps) == {name: [] for name in
                                                          ("registry", "closure", "direction", "consumer", "legacy")}


@pytest.mark.parametrize("dropped", sorted(STEP_1.closure))
def test_mutant_a_closure_with_one_element_less_is_caught(dropped):
    steps = _swap(STEPS, "M2A-1", closure=STEP_1.closure - {dropped})
    assert f"M2A-1: closure reaches {dropped}" in closure_problems(steps=steps)


@pytest.mark.parametrize("added", ["app.core.money_polarity", "app.core.conversation_store", "app.main"])
def test_mutant_a_closure_with_one_element_more_is_caught(added):
    steps = _swap(STEPS, "M2A-1", closure=STEP_1.closure | {added})
    assert any(f"closure lost {added}" in problem for problem in closure_problems(steps=steps))


def test_mutant_a_virtual_registered_module_that_reaches_something_undeclared_is_caught():
    virtual, steps = _synthetic(body=ON_M2A_1 + "import app.core.money_polarity\n")
    assert "M2A-S1: closure reaches app.core.money_polarity" in closure_problems(None, virtual, steps)


def test_mutant_a_virtual_step_that_declares_what_it_does_not_reach_is_caught():
    virtual, steps = _synthetic(closure_extra={"app.core.money_polarity"})
    assert any("M2A-S1: closure lost app.core.money_polarity" in p for p in closure_problems(None, virtual, steps))


def test_a_dependent_step_inherits_the_closure_of_its_dependency_so_a_change_in_m2a_1_does_not_break_it():
    """The point of the increment: M2A-1 drifts (flagged on M2A-1 only); M2A-S1 declares nothing M2A-1 reaches."""
    virtual, steps = _synthetic()
    drift = {MODULE: _source(MODULE) + "\n\ndef _late():\n    import app.core.money_polarity\n"}
    problems = closure_problems(drift, virtual, steps)
    assert "M2A-1: closure reaches app.core.money_polarity" in problems
    assert not [problem for problem in problems if problem.startswith("M2A-S1")], problems


def test_a_step_inherits_the_closure_of_its_transitive_dependencies():
    """S3 -> S2 -> S1, and S2 imports nothing of S1: S3 still inherits S1's closure (it declares only its increment)."""
    store_lock = "app.core.store_lock"  # a leaf outside M2A-1's closure
    virtual = {VIRTUAL_A: ON_M2A_1 + f"import {store_lock}\n", VIRTUAL_B: "", VIRTUAL_C: f"import {store_lock}\n"}
    steps = (*STEPS, _step("M2A-S1", (VIRTUAL_A,), ("M2A-1",), {VIRTUAL_A, store_lock}),
             _step("M2A-S2", (VIRTUAL_B,), ("M2A-S1",), {VIRTUAL_B}),
             _step("M2A-S3", (VIRTUAL_C,), ("M2A-S2",), {VIRTUAL_C}))
    assert closure_problems(None, virtual, steps) == []
    undeclared = _swap(steps, "M2A-S3", closure=frozenset({VIRTUAL_C, "app.core.money_polarity"}))
    assert any("M2A-S3: closure lost app.core.money_polarity" in p for p in closure_problems(None, virtual, undeclared))


def test_a_step_with_no_dependency_declares_the_whole_closure():
    steps = (*STEPS, _step("M2A-S1", (VIRTUAL_A,), (), {VIRTUAL_A}))  # the synthetic step with DEPENDS_ON dropped
    problems = closure_problems(None, {VIRTUAL_A: ON_M2A_1}, steps)
    assert f"M2A-S1: closure reaches {MODULE}" in problems and "M2A-S1: closure reaches app.core" in problems


@pytest.mark.parametrize("module", ["app.core.query_router", "app.core.followup_engine", "app.core.conversation_store",
                                    "app.core.orchestrator", "app.core.foundation_runtime"])
def test_mutant_registering_a_legacy_or_forbidden_module_does_not_launder_it(module):
    """Registration opens no door: the module is still forbidden in the closure, still consumed by unregistered
    modules and still part of what legacy (or the Foundation runtime) reaches."""
    steps = (*STEPS, _step("M2A-S1", (module,), ("M2A-1",)))
    assert f"forbidden {module}" in closure_problems(steps=steps)
    assert consumer_problems(steps=steps), "its importers are unregistered modules"
    assert any(module in problem for problem in legacy_reach_problems(steps=steps)) or module == "app.core.orchestrator"


@pytest.mark.parametrize("name", ["app.core.m2a_virtual_resolver", "app.core.m2a_virtual_authority",
                                  "app.core.m2a_virtual_authorities"])
def test_mutant_a_registered_module_named_as_an_authority_or_resolver_is_forbidden(name):
    steps = (*STEPS, _step("M2A-S1", (name,), ("M2A-1",), {name}))
    assert f"forbidden {name}" in closure_problems(None, {name: ON_M2A_1}, steps)


# ---- non-vacuity: direction ------------------------------------------------------------------------------------------
def _chain(*, b_depends=(), c_depends=(), a_body=ON_M2A_1, b_body=IMPORT_A, c_body=IMPORT_B):
    virtual = {VIRTUAL_A: a_body, VIRTUAL_B: b_body, VIRTUAL_C: c_body}
    steps = (*STEPS, _step("M2A-S1", (VIRTUAL_A,), ("M2A-1",)), _step("M2A-S2", (VIRTUAL_B,), b_depends),
             _step("M2A-S3", (VIRTUAL_C,), c_depends))
    return virtual, steps


def test_mutant_step_a_importing_step_b_without_depends_on_is_caught():
    virtual, steps = _chain(b_depends=(), c_depends=("M2A-S2",))
    problems = direction_problems(None, virtual, steps)
    assert any(f"{VIRTUAL_B} (M2A-S2) imports {VIRTUAL_A} (M2A-S1) without DEPENDS_ON" in p for p in problems)


def test_direction_control_the_declared_dependency_is_allowed_and_a_transitive_one_too():
    virtual, steps = _chain(b_depends=("M2A-S1",), c_depends=("M2A-S2",), c_body=IMPORT_B + IMPORT_A)
    assert direction_problems(None, virtual, steps) == []  # C imports B (declared) and A (transitive through B)


def test_mutant_a_dependency_does_not_allow_the_reverse_edge():
    virtual, steps = _chain(b_depends=("M2A-S1",), c_depends=("M2A-S2",), a_body=ON_M2A_1 + IMPORT_B)
    problems = direction_problems(None, virtual, steps)
    assert any(f"{VIRTUAL_A} (M2A-S1) imports {VIRTUAL_B} (M2A-S2)" in p for p in problems), problems


def test_mutant_a_module_importing_a_sibling_step_that_it_does_not_depend_on_is_caught():
    virtual, steps = _chain(b_depends=("M2A-S1",), c_depends=("M2A-S1",))  # C -> B, but C depends on S1 only
    assert direction_problems(None, virtual, steps)


def test_a_module_may_import_a_module_of_its_own_step():
    virtual = {VIRTUAL_A: ON_M2A_1, VIRTUAL_B: IMPORT_A}
    steps = (*STEPS, _step("M2A-S1", (VIRTUAL_A, VIRTUAL_B), ("M2A-1",)))
    assert direction_problems(None, virtual, steps) == []


def test_mutant_a_real_module_importing_a_step_that_the_step_does_not_depend_on():
    """Same shape on a real module: foundation_accepted (M2A-1, no dependencies) imports a later step's module."""
    virtual, steps = _synthetic()
    overrides = {MODULE: _source(MODULE) + LATE_IMPORT_OF_A}
    problems = direction_problems(overrides, virtual, steps)
    assert any(f"{MODULE} (M2A-1) imports {VIRTUAL_A} (M2A-S1)" in problem for problem in problems), problems


# ---- non-vacuity: consumption (the definitive invariant) -------------------------------------------------------------
@pytest.mark.parametrize("name", ["app.core.m2a_virtual_ext", "app.m2a_virtual_ext", "scripts.m2a_virtual_ext"])
def test_mutant_an_unregistered_virtual_module_importing_m2a_is_caught(name):
    assert consumer_problems(None, {name: ON_M2A_1}) == [f"{name} imports {MODULE}"]


def test_consumer_control_the_same_source_registered_in_a_step_is_not_a_consumer():
    virtual, steps = _synthetic()
    assert consumer_problems(None, virtual, steps) == []
    assert consumer_problems(None, virtual) == [f"{VIRTUAL_A} imports {MODULE}"]  # unregistered: caught


def test_mutant_an_unregistered_module_importing_a_registered_virtual_module_is_caught():
    virtual, steps = _synthetic()
    overrides = {"app.core.orchestrator": _source("app.core.orchestrator") + LATE_IMPORT_OF_A}
    assert f"app.core.orchestrator imports {VIRTUAL_A}" in consumer_problems(overrides, virtual, steps)


def test_mutant_a_wildcard_dynamic_import_that_can_load_m2a_is_a_consumer():
    """``importlib.import_module(f"app.core.{name}")`` can load any module under the prefix: it reaches M2a."""
    extra = "\n\ndef _late(name):\n    import importlib\n    return importlib.import_module(f'app.core.{name}')\n"
    problems = consumer_problems({"app.core.store_lock": _source("app.core.store_lock") + extra})
    assert f"app.core.store_lock imports {MODULE}" in problems


# ---- non-vacuity: legacy and Foundation runtime never reach M2a ------------------------------------------------------
def test_mutant_the_legacy_closure_reaching_a_registered_module_is_caught():
    virtual, steps = _synthetic()
    overrides = {"app.core.conversation_store":
                 _source("app.core.conversation_store") + LATE_IMPORT_OF_A}
    assert f"legacy closure reaches {VIRTUAL_A}" in legacy_reach_problems(overrides, virtual, steps)


def test_mutant_the_foundation_runtime_reaching_a_registered_module_is_caught_apart_from_legacy():
    virtual, steps = _synthetic()
    overrides = {"app.core.foundation_runtime":
                 _source("app.core.foundation_runtime") + LATE_IMPORT_OF_A}
    problems = legacy_reach_problems(overrides, virtual, steps)
    assert f"Foundation runtime closure reaches {VIRTUAL_A}" in problems
    assert not [problem for problem in problems if problem.startswith("legacy")]  # the mode frontier keeps legacy clear


def test_mutant_the_legacy_closure_reaching_the_ir_wire_module_is_caught():
    overrides = {"app.core.conversation_store": _source("app.core.conversation_store") + LATE_IMPORT_OF_IR}
    assert legacy_reach_problems(overrides) == [f"legacy closure reaches {LEGACY_MUST_STAY_OUT}"]


def test_mutant_the_real_m2a_module_in_the_legacy_closure_is_caught():
    overrides = {"app.core.conversation_store":
                 _source("app.core.conversation_store") + LATE_IMPORT_OF_M2A}
    problems = legacy_reach_problems(overrides)
    assert f"legacy closure reaches {MODULE}" in problems
    assert f"legacy closure reaches {LEGACY_MUST_STAY_OUT}" in problems  # M2A-1 imports the IR: the legacy path too


# ---- non-vacuity: imports, bridge, lexicon, dynamic import (the M2A-1a mutants, unchanged) ---------------------------
@pytest.mark.parametrize("extra", [
    "\n\ndef _late():\n    from app.core.query_router import QueryRouter\n    return QueryRouter\n",
    "\n\ndef _late():\n    import app.core.followup_engine\n",
    "\n\ndef _late():\n    import importlib\n    return importlib.import_module('app.core.followup_mutations')\n",
    "\n\nfrom app.core.accreditation import authorities\n",
    "\n\ndef _late():\n    from app.core import conversation_store\n",
    "\n\nfrom app.core.foundation_authority_registry import REGISTRY\n",
    "\n\ndef _late():\n    from app.core import foundation_authority_contract\n",
    "\n\ndef _late():\n    from app.core.foundation_evidence_window import build_window\n",
    "\n\ndef _late():\n    from app.core import value_authority\n",
])
def test_mutant_legacy_import_is_caught(extra):
    assert closure_problems({MODULE: _source(MODULE) + extra})


@pytest.mark.parametrize("extra", [
    "\n\ndef to_parsed_params(effective):\n    return {}\n",
    "\n\ndef _bridge(effective):\n    return {'parsed_params': effective.to_dict()}\n",
    "\n\ndef _frame(effective, frame):\n    return frame.QueryFrame\n",
    "\n\nimport re\n",
    "\n\nfrom app.core.foundation_text_predicates import nothing\n",
    "\n\ndef _accredit(authority, window):\n    return authority.resolve(window)\n",
])
def test_mutant_bridge_or_lexicon_is_caught(extra):
    assert source_problems(_source(MODULE) + extra)


@pytest.mark.parametrize("extra", [
    "\n\ndef _late(name):\n    return __import__(name)\n",
    "\n\ndef _late():\n    return __import__('app.core.semantic_dimensions')\n",
    "\n\ndef _late(name):\n    import importlib\n    return importlib.import_module(name)\n",
    "\n\ndef _late():\n    import importlib\n    return importlib.import_module('app.core.semantic_dimensions')\n",
    "\n\nfrom importlib import import_module as _load\n",
    "\n\nimport importlib as _il\n",
    "\n\ndef _late(name, loader):\n    return loader.import_module(name)\n",
    "\n\ndef _late(name):\n    import builtins\n    return getattr(builtins, '__import__')(name)\n",
    "\n\ndef _late(name):\n    import builtins\n    return builtins.__import__(name)\n",
    "\n\nfrom importlib.util import find_spec\n",
])
def test_mutant_dynamic_import_is_caught(extra):
    """Forbidden whatever the argument: an ALLOWED literal target and a variable target are caught alike."""
    assert any("dynamic import" in problem for problem in source_problems(_source(MODULE) + extra))


def test_mutant_unresolved_dynamic_import_fails_the_closure_pin():
    extra = "\n\ndef _late(name):\n    import importlib\n    return importlib.import_module(name)\n"
    assert any("unresolvable dynamic import" in p for p in closure_problems({MODULE: _source(MODULE) + extra}))


def test_mutant_a_virtual_registered_module_with_an_unresolved_dynamic_import_fails_the_closure_pin():
    virtual, steps = _synthetic(body=ON_M2A_1 + "\n\ndef _late(name):\n    import importlib\n"
                                                "    return importlib.import_module(name)\n")
    assert f"unresolvable dynamic import in {VIRTUAL_A}" in closure_problems(None, virtual, steps)


@pytest.mark.parametrize("consumer, extra", [
    ("app.core.orchestrator", "\n\ndef _m2a():\n    from app.core.foundation_accepted import AcceptedTurnRecord\n"),
    ("app.core.foundation_runtime", "\n\nfrom app.core import foundation_accepted\n"),
    ("app.core.foundation_projection",
     "\n\ndef _m2a():\n    import importlib\n    return importlib.import_module('app.core.foundation_accepted')\n"),
    ("streamlit_app", "\n\nimport app.core.foundation_accepted as _m2a\n"),
])
def test_mutant_productive_consumer_is_caught(consumer, extra):
    """No productive module consumes M2a: any unregistered module importing it kills the pin."""
    assert consumer_problems({consumer: _source(consumer) + extra})


def test_the_docstring_mention_of_the_barrier_is_not_a_bridge():
    assert "parsed_params" in _source(MODULE)  # the module documents the barrier...
    assert source_problems(_source(MODULE)) == []  # ...and the pin reads code, not prose


def test_entrypoints_are_the_legacy_ones():
    assert "app.core.query_router" in ENTRYPOINTS and "app.core.orchestrator" in ENTRYPOINTS
