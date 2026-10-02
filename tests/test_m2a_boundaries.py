"""M2a boundary pins (contract §3.7, INV-M2A-04, INV-M2A-24; mission M2A-1a invariants 4, 5 and 6; wave-1 prep).

M2a modules are registered PER STEP in ``tests/m2a_steps/m2a_<n>.py`` (``STEP``, ``MODULES``, ``DEPENDS_ON``,
``CLOSURE``): each lane edits only its own file, never a list shared with another lane.

* REGISTRY: every module is under ``app.core.``, exists, is NEW (not in main's manifest nor among the modules that
  existed at the prep base: a step registers only what it creates), is registered once, in one step; step names are
  unique and ``DEPENDS_ON`` names known steps, never itself, never a cycle. A registry file holds a docstring and the
  four literal assignments, no logic.
* DAG: the dependency graph of this wave is a REVIEWED constant (``PLANNED_DEPENDENCIES``): a lane cannot widen what its
  step may import by editing its own ``DEPENDS_ON``; widening is an explicit edit of that constant.
* IMPORTS: per step, the transitive import closure of its modules MINUS the closure of the steps it depends on (its own
  increment) is EXACTLY its ``CLOSURE``. A change in M2A-1 leaves the registry of M2A-2 alone unless both grow toward
  the SAME module: then M2A-2 fails safe (``closure lost X``: never silent) and its owner drops X from its own
  ``CLOSURE``. On the union of all steps no module reaches ``query_router``, ``followup_engine``,
  ``followup_mutations``, the conversation store, the orchestrator, owner detectors or ``accreditation`` (the contract
  allows only the future ``accredited_span`` leaf, which does not exist yet: adding it means adding it, by name, to the
  step that needs it).
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

LIMITED GUARANTEES (user decision: the denylist is not widened). ``consumer_problems`` reads the edges of the AST
scanner: it does not see ``getattr(app.core, "foundation_accepted")`` (attribute access on a package object), nor an
unresolvable dynamic import (``importlib.import_module(name)`` with a variable ``name``) in an UNREGISTERED module;
inside the registered closure the latter does fail (``unresolvable dynamic import``). The registry's «already existed»
rule reads main's manifest and a frozen list of the post-main modules at the prep base: a module created later by
someone else and registered by a lane is not seen as pre-existing.

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

from tests.m2a_steps import (
    STEPS, Step, file_problems, registered_modules, step_from_namespace, steps_from_namespaces,
)
from tests.test_m1_wave1_parity_pins import (
    ENTRYPOINTS, build_graph, closure, legacy_manifest, module_edges, module_index,
)

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

# REVIEWED DAG of wave 1 (user decision 2026-10-02): the steps and what each may depend on. Each lane writes its own
# registry file, so its ``DEPENDS_ON`` cannot be the authority on what it may import: widening this is an explicit
# edit HERE (a reviewed decision), never a side effect. ``M2A-3`` (the reducer) never depends on ``M2A-2`` (the store).
PLANNED_DEPENDENCIES = {"M2A-1": (), "M2A-2": ("M2A-1",), "M2A-3": ("M2A-1",)}

# ``app.core`` modules that existed at the prep base (6be95aaf) without being in main's manifest (88653752), except
# ``foundation_accepted`` (M2A-1's own). With the manifest they are every module a step may NOT register.
POST_MAIN_EXISTING = frozenset({
    "app.core.foundation_acceptance_loader", "app.core.foundation_execution_guard", "app.core.foundation_lift_v1",
    "app.core.foundation_operations", "app.core.foundation_operator_table",
    "app.core.host_authorities.datasets", "app.core.host_authorities.modality", "app.core.host_authorities.state",
    "app.core.host_authorities.state_catalog",
})


def manifest_modules(manifest: dict[str, str]) -> frozenset[str]:
    """``app.core`` modules of a path -> sha manifest (the frozen reference of main; this never calls git)."""
    names = set()
    for path in manifest:
        if path.startswith("app/core/") and path.endswith(".py"):
            parts = path[: -len(".py")].split("/")
            names.add(".".join(parts[:-1] if parts[-1] == "__init__" else parts))
    return frozenset(names)


PREEXISTING = manifest_modules(legacy_manifest()) | POST_MAIN_EXISTING


def _source(module: str) -> str:
    return (ROOT / (module.replace(".", "/") + ".py")).read_text(encoding="utf-8")


# ---- the graph, with virtual modules -------------------------------------------------------------------------------
def _graph(overrides: dict[str, str] | None = None, virtual: dict[str, str] | None = None,
           steps: tuple[Step, ...] | None = None):
    """``build_graph`` that also takes VIRTUAL modules: (index, edges, modules with an unresolvable dynamic import).

    ``overrides`` replace the source of a module that exists on disk; ``virtual`` adds modules that do not exist. Edges
    to and from a virtual module resolve against an index extended with a fictitious path, so a virtual module can be
    registered, imported and reached. A virtual module that exists on disk, or an override of one that does not, is a
    broken mutant, not a pass.

    ``steps`` is the registry of a SYNTHETIC world (``WORLD``). A module that the real registry (``STEPS``) registers
    and ``steps`` does not is a lane's real file that world knows nothing about: it is BLANKED (an empty source), so
    what a lane has or has not written can never change the verdict of a mutant. ``steps=None`` or the real registry
    blanks nothing.
    """
    overrides, virtual = dict(overrides or {}), dict(virtual or {})
    index, graph, flagged = build_graph()
    if steps is not None:
        known = set(registered_modules(steps))
        overrides = {**{module: "" for module in registered_modules(STEPS) if module in index and module not in known},
                     **overrides}
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


# ---- the registry --------------------------------------------------------------------------------------------------
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


def registry_problems(steps: tuple[Step, ...], present: set[str] | frozenset[str],
                      preexisting: frozenset[str] = PREEXISTING) -> list[str]:
    """Why a set of steps is not a well-formed registry (``present``: the modules that exist; ``preexisting``: the
    ones that existed before M2a and so may not be registered)."""
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
            if module in preexisting:
                out.append(f"{step.name}: {module} already existed before M2a: a step registers only what it creates")
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


def dag_problems(steps: tuple[Step, ...], planned: dict[str, tuple[str, ...]] = PLANNED_DEPENDENCIES) -> list[str]:
    """Every step's ``DEPENDS_ON`` is exactly the reviewed one, and every reviewed step is registered."""
    out = []
    for step in steps:
        if step.name not in planned:
            out.append(f"{step.name} is not in the reviewed DAG")
        elif set(step.depends_on) != set(planned[step.name]):
            out.append(f"{step.name} DEPENDS_ON {sorted(step.depends_on)} but the reviewed DAG says "
                       f"{sorted(planned[step.name])}")
    out += [f"reviewed step {name} has no registry file" for name in sorted(set(planned) - {s.name for s in steps})]
    return out


# ---- the pins' functions -------------------------------------------------------------------------------------------
def closure_problems(overrides: dict[str, str] | None = None, virtual: dict[str, str] | None = None,
                     steps: tuple[Step, ...] = STEPS) -> list[str]:
    """Per step: closure(MODULES) - closure(modules of the steps it depends on) == CLOSURE, exactly. On the union:
    nothing forbidden, no unresolvable dynamic import."""
    _, graph, flagged = _graph(overrides, virtual, steps)
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
    _, graph, _ = _graph(overrides, virtual)  # only registered modules' edges are read: a world blanks none of them
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
    _, graph, _ = _graph(overrides, virtual, steps)
    registered = set(registered_modules(steps))
    return [f"{name} imports {target}" for name, edges in sorted(graph.items()) if name not in registered
            for target in sorted(edges & registered)]


def legacy_reach_problems(overrides: dict[str, str] | None = None, virtual: dict[str, str] | None = None,
                          steps: tuple[Step, ...] = STEPS) -> list[str]:
    """Neither the legacy closure (cutting only the declared mode frontier) nor the Foundation runtime (the same
    closure through the frontier) reaches a registered module; the legacy closure also stays clear of the IR wire."""
    _, graph, _ = _graph(overrides, virtual, steps)
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


# ---- the graph with virtual modules is itself checked --------------------------------------------------------------
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


# The synthetic world of every mutant: M2A-1 (the one registry a test may assume: it is the foundation) and two EMPTY
# lane steps with their reviewed DAG. The real lane steps are never reused: a lane fills its own registry file, and no
# pin of this file may depend on what it holds (``_graph`` blanks the real lane modules a world does not register).
WORLD = (_real("M2A-1"), _step("M2A-2", (), ("M2A-1",)), _step("M2A-3", (), ("M2A-1",)))

_DISK_MODULES = frozenset(build_graph()[0])


def _present(*extra: str) -> set[str]:
    return set(_DISK_MODULES) | set(extra)


def _all_problems(overrides=None, virtual=None, steps=WORLD) -> dict[str, list[str]]:
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


def test_an_override_replaces_the_dynamic_import_flag_of_its_module(monkeypatch):
    index, graph, _ = build_graph()
    monkeypatch.setitem(globals(), "build_graph", lambda: (index, graph, {"app.core.store_lock"}))
    assert "app.core.store_lock" in _graph()[2]
    assert "app.core.store_lock" not in _graph({"app.core.store_lock": "X = 1\n"})[2]  # the override has none
    dynamic = "import importlib\n\ndef _late(name):\n    return importlib.import_module(name)\n"
    assert "app.core.store_lock" in _graph({"app.core.store_lock": dynamic})[2]


def test_relative_imports_resolve_against_the_package_of_the_module():
    _, graph, _ = _graph(virtual={"app.core.m2a_virtual_rel": "from . import foundation_accepted\n"})
    assert MODULE in graph["app.core.m2a_virtual_rel"]  # a module: its package is the parent
    _, graph, _ = _graph({"app.core": "from . import foundation_accepted\n"})
    assert MODULE in graph["app.core"]  # a package (__init__.py): it is its own package


def test_a_world_blanks_the_real_lane_modules_it_does_not_register_and_the_real_registry_blanks_nothing(monkeypatch):
    lane = "app.core.foundation_messages"  # a real module with edges, standing in for a lane's real file
    fake = (_real("M2A-1"), _step("M2A-2", (lane, "app.core.m2a_not_on_disk"), ("M2A-1",)),
            _step("M2A-3", (), ("M2A-1",)))
    monkeypatch.setitem(globals(), "STEPS", fake)
    disk_edges = build_graph()[1][lane]
    assert disk_edges  # the stand-in does import things
    assert _graph(steps=WORLD)[1][lane] == set()  # unregistered by the world: blanked (and a missing one is skipped)
    assert _graph(steps=fake)[1][lane] == disk_edges  # registered: its real edges stay
    assert _graph(steps=None)[1][lane] == disk_edges  # no world: nothing blanked
    assert _graph({lane: "import app.core.store_lock\n"}, None, WORLD)[1][lane] == {"app", "app.core",
                                                                                      "app.core.store_lock"}
    assert _graph(steps=WORLD)[1][MODULE] == build_graph()[1][MODULE]  # M2A-1 is the foundation: never blanked


def test_the_pins_of_a_world_ignore_what_a_real_lane_file_imports(monkeypatch):
    """A lane's real file is not on disk in this prep, so a patched graph node stands in for it: it imports M2A-1 and a
    module that legacy reaches. For a world that does not register it that is nothing; for the registry that does, it
    is judged as a registered module should be."""
    lane = "app.core.m2a_lane_stand_in"
    index, graph, flagged = build_graph()
    index = {**index, lane: ROOT / "app/core/m2a_lane_stand_in.py"}
    graph = {**graph, lane: {MODULE, "app.core.money_polarity"},
             "app.core.conversation_store": graph["app.core.conversation_store"] | {lane}}
    monkeypatch.setitem(globals(), "build_graph", lambda: (index, graph, flagged))
    registered = (_real("M2A-1"), _step("M2A-2", (lane,), ("M2A-1",)), _step("M2A-3", (), ("M2A-1",)))
    monkeypatch.setitem(globals(), "STEPS", registered)
    virtual = {VIRTUAL_A: ON_M2A_1 + f"import {lane}\n"}
    world = (*WORLD, _step("M2A-S1", (VIRTUAL_A,), ("M2A-1",), {VIRTUAL_A, lane}))
    assert consumer_problems(None, None, WORLD) == []  # not registered here: the lane's file is not a consumer
    assert legacy_reach_problems(None, None, WORLD) == []  # ...nor does legacy reach M2A-1 through it
    assert closure_problems(None, virtual, world) == []  # ...nor does its money_polarity edge enter a closure
    assert consumer_problems(None, None, registered) == [  # registered: only legacy's import of it is a consumer
        f"app.core.conversation_store imports {lane}"]
    assert any(f"legacy closure reaches {lane}" in p for p in legacy_reach_problems(None, None, registered))
    assert any("M2A-2: closure reaches app.core.money_polarity" in p
               for p in closure_problems(None, None, registered))  # registered: its real edges count


def test_the_graph_with_no_mutation_is_the_disk_graph():
    index, graph, flagged = build_graph()
    index2, graph2, flagged2 = _graph()
    assert (index2, graph2, flagged2) == (index, graph, flagged)
    assert VIRTUAL_A not in index  # the cached disk index is never polluted by a virtual module


# ---- non-vacuity: the registry -------------------------------------------------------------------------------------
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


def test_a_malformed_registry_raises_and_is_never_dropped():
    assert steps_from_namespaces({"m2a_8": _namespace(STEP="M2A-8"), "m2a_7": _namespace()}) == (
        _step("M2A-7"), _step("M2A-8"))
    with pytest.raises(ValueError, match="malformed M2a step registry"):
        steps_from_namespaces({"m2a_7": _namespace(), "m2a_8": _namespace(STEP="M2A-9")})  # a good one hides nothing
    with pytest.raises(ValueError, match="m2a_8: unknown field EXTRA"):
        steps_from_namespaces({"m2a_8": _namespace(STEP="M2A-8", EXTRA=1)})
    assert steps_from_namespaces({}) == ()


def test_the_loaded_steps_are_the_ones_of_the_registry_files():
    assert [step.stem for step in STEPS] == sorted(step.stem for step in STEPS)
    assert {step.name for step in STEPS} >= {"M2A-1", "M2A-2", "M2A-3"}


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


def test_a_self_dependency_is_one_problem_not_also_a_cycle():
    assert registry_problems((_step("M2A-P1", depends=("M2A-P1",)),), _present()) == ["M2A-P1 depends on itself"]


def test_dependencies_never_list_the_step_itself_even_on_a_cycle():
    steps = (_step("M2A-P1", depends=("M2A-P2",)), _step("M2A-P2", depends=("M2A-P1",)),
             _step("M2A-P3", depends=("M2A-P3",)))
    assert _dependencies(steps) == {"M2A-P1": {"M2A-P2"}, "M2A-P2": {"M2A-P1"}, "M2A-P3": set()}


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


# ---- non-vacuity: a registered module must be NEW ------------------------------------------------------------------
def test_the_preexisting_list_is_real_not_stale():
    assert POST_MAIN_EXISTING <= _DISK_MODULES  # each one still exists...
    assert not POST_MAIN_EXISTING & manifest_modules(legacy_manifest())  # ...and is exactly what main's manifest lacks
    assert MODULE not in PREEXISTING  # M2A-1's own module is the one pre-existing file a step may register
    assert {"app.core.store_lock", "app.core.entity_ecosystem"} <= PREEXISTING


def test_manifest_modules_maps_paths_to_modules():
    manifest = dict.fromkeys(["app/core/a.py", "app/core/pkg/__init__.py", "app/core/pkg/b.py", "app/data/c.py",
                              "scripts/d.py", "app/core/e.json", "tests/app/core/f.py"], "sha")
    assert manifest_modules(manifest) == {"app.core.a", "app.core.pkg", "app.core.pkg.b"}


def _exact_step(virtual: dict[str, str], module: str, name: str = "M2A-S1") -> tuple[Step, ...]:
    """The world plus a step that depends on M2A-1 and declares EXACTLY its own increment: every closure pin is then
    satisfied by construction, so only the clause under test can speak."""
    _, graph, _ = _graph(None, virtual, WORLD)
    inherited = set(closure(graph, entrypoints=_real("M2A-1").modules, cut=()))
    own = set(closure(graph, entrypoints=(module,), cut=())) - inherited
    return (*WORLD, _step(name, (module,), ("M2A-1",), own))


# Spelled out (not derived from the constant): removing a name from POST_MAIN_EXISTING must make one of these fail.
POST_MAIN_EXPECTED = (
    "app.core.foundation_acceptance_loader", "app.core.foundation_execution_guard", "app.core.foundation_lift_v1",
    "app.core.foundation_operations", "app.core.foundation_operator_table", "app.core.host_authorities.datasets",
    "app.core.host_authorities.modality", "app.core.host_authorities.state", "app.core.host_authorities.state_catalog",
)


def test_the_post_main_list_is_exactly_the_reviewed_one():
    assert set(POST_MAIN_EXPECTED) == POST_MAIN_EXISTING and len(POST_MAIN_EXPECTED) == len(POST_MAIN_EXISTING)


@pytest.mark.parametrize("module", ["app.core.entity_ecosystem", "app.core.result_grouping", "app.core.polysemic",
                                    "app.core.host_authorities", "app.core.store_lock",
                                    "app.core.query_text_consumption", *POST_MAIN_EXPECTED])
def test_mutant_registering_a_module_that_already_existed_is_refused(module):
    assert module in _DISK_MODULES
    problems = registry_problems((*WORLD, _step("M2A-S1", (module,), ("M2A-1",))), _present())
    assert [p for p in problems if "already existed" in p] == [
        f"M2A-S1: {module} already existed before M2a: a step registers only what it creates"]


@pytest.mark.parametrize("module", ["app.core.entity_ecosystem", "app.core.result_grouping", "app.core.polysemic",
                                    "app.core.foundation_acceptance_loader"])
def test_the_orphans_that_every_other_pin_lets_through_are_refused_by_the_registry(module):
    """No unregistered module imports them and their closure is declared exactly: only the registry can object."""
    steps = _exact_step({}, module)
    problems = _all_problems(steps=steps)
    assert [name for name, found in problems.items() if found] == ["registry"], problems


def test_registry_control_a_new_module_and_m2a_1s_own_are_accepted():
    steps = (*WORLD, _step("M2A-S1", (VIRTUAL_A,), ("M2A-1",)))
    assert registry_problems(steps, _present(VIRTUAL_A)) == []
    assert MODULE in registered_modules(steps)


# ---- non-vacuity: the reviewed DAG ---------------------------------------------------------------------------------
def test_the_step_dag_is_the_reviewed_one():
    assert dag_problems(STEPS) == []
    assert {step.name for step in STEPS} == set(PLANNED_DEPENDENCIES)


@pytest.mark.parametrize("name, depends", [
    ("M2A-3", ("M2A-1", "M2A-2")), ("M2A-3", ("M2A-2",)), ("M2A-3", ()),
    ("M2A-2", ()), ("M2A-2", ("M2A-1", "M2A-3")), ("M2A-1", ("M2A-2",)), ("M2A-1", ("M2A-3",)),
])
def test_mutant_a_lane_widening_or_dropping_its_own_depends_on_is_caught(name, depends):
    problems = dag_problems(_swap(WORLD, name, depends_on=depends))
    assert len(problems) == 1 and problems[0].startswith(f"{name} DEPENDS_ON {sorted(depends)} but the reviewed DAG")


def test_mutant_an_unreviewed_step_and_a_missing_reviewed_step_are_caught():
    assert dag_problems((*WORLD, _step("M2A-4", (), ("M2A-1",)))) == ["M2A-4 is not in the reviewed DAG"]
    assert dag_problems(tuple(step for step in WORLD if step.name != "M2A-3")) == [
        "reviewed step M2A-3 has no registry file"]


def test_mutant_the_dag_pin_is_what_keeps_the_reducer_from_importing_the_store():
    """M2A-3 adds «M2A-2» to its own DEPENDS_ON and imports the store: the direction pin is then satisfied (declared),
    the registry is clean and the closure is exact, so the reviewed DAG is the only pin that objects."""
    virtual = {VIRTUAL_B: ON_M2A_1, VIRTUAL_C: IMPORT_B}
    stored = _swap(WORLD, "M2A-2", modules=(VIRTUAL_B,), closure=frozenset({VIRTUAL_B}))
    steps = _swap(stored, "M2A-3", modules=(VIRTUAL_C,), depends_on=("M2A-1", "M2A-2"),
                  closure=frozenset({VIRTUAL_C}))
    assert [name for name, found in _all_problems(virtual=virtual, steps=steps).items() if found] == []
    assert dag_problems(steps) == ["M2A-3 DEPENDS_ON ['M2A-1', 'M2A-2'] but the reviewed DAG says ['M2A-1']"]
    assert direction_problems(None, virtual, _swap(steps, "M2A-3", depends_on=("M2A-1",)))  # undeclared: caught too


# ---- non-vacuity: a registry file is data --------------------------------------------------------------------------
REGISTRY_FILES = sorted((ROOT / "tests" / "m2a_steps").glob("m2a_*.py"))
GOOD_REGISTRY = '"""doc."""\nSTEP = "M2A-7"\nMODULES = ()\nDEPENDS_ON = ("M2A-1",)\nCLOSURE = frozenset()\n'


@pytest.mark.parametrize("path", REGISTRY_FILES, ids=lambda path: path.name)
def test_a_registry_file_is_data_not_code(path):
    assert file_problems(path.read_text(encoding="utf-8")) == []


def test_registry_file_control_literal_forms_are_accepted():
    assert file_problems(GOOD_REGISTRY) == []
    assert file_problems("STEP = 'M2A-7'\nMODULES = ('a.b', 'c')\nDEPENDS_ON = ()\n"
                         "CLOSURE = frozenset({'a', 'b'})\n") == []
    assert file_problems("") == []  # absence of a field is step_from_namespace's business, not this check's


@pytest.mark.parametrize("extra, expected", [
    ("import os\n", "Import is not allowed"), ("from tests.x import Y\n", "ImportFrom is not allowed"),
    ("def helper():\n    return 1\n", "FunctionDef is not allowed"),
    ("class C:\n    pass\n", "ClassDef is not allowed"),
    ("print('x')\n", "Expr is not allowed"), ("'a second docstring'\n", "Expr is not allowed"),
    ("if True:\n    X = 1\n", "If is not allowed"), ("for _ in ():\n    pass\n", "For is not allowed"),
    ("with open('x'):\n    pass\n", "With is not allowed"), ("assert True\n", "Assert is not allowed"),
    ("_BASE = frozenset({'a'})\n", "_BASE is not a registry field"),
    ("HELPER = ('a',)\n", "HELPER is not a registry field"),
    ("EXTRA: int = 1\n", "AnnAssign is not allowed"), ("MODULES += ('a',)\n", "AugAssign is not allowed"),
    ("STEP, MODULES = 'a', ()\n", "Assign is not allowed"), ("STEP = MODULES = ()\n", "Assign is not allowed"),
    ("STEP = 'M2A-8'\n", "STEP is assigned more than once"),
])
def test_mutant_a_registry_file_with_logic_is_caught(extra, expected):
    problems = file_problems(GOOD_REGISTRY + extra)
    assert any(expected in problem for problem in problems), problems


@pytest.mark.parametrize("old, new", [
    ('STEP = "M2A-7"', 'STEP = "M2A-" + "7"'), ('STEP = "M2A-7"', 'STEP = f"M2A-{7}"'), ('STEP = "M2A-7"', "STEP = 7"),
    ('STEP = "M2A-7"', "STEP = _NAME"),
    ("MODULES = ()", 'MODULES = ["a"]'), ("MODULES = ()", 'MODULES = ("a", b)'),
    ("MODULES = ()", 'MODULES = ("a", *("b",))'), ("MODULES = ()", 'MODULES = ("a", 1)'),
    ("MODULES = ()", 'MODULES = tuple(x for x in "a")'), ("MODULES = ()", 'MODULES = ("a",) + ("b",)'),
    ("MODULES = ()", 'MODULES = "a".split()'),
    ('DEPENDS_ON = ("M2A-1",)', "DEPENDS_ON = _DEPS"), ('DEPENDS_ON = ("M2A-1",)', 'DEPENDS_ON = ("M2A-1", None)'),
    ("CLOSURE = frozenset()", 'CLOSURE = frozenset({"a"}) | frozenset({"b"})'),
    ("CLOSURE = frozenset()", "CLOSURE = frozenset(_BASE)"),
    ("CLOSURE = frozenset()", 'CLOSURE = frozenset({x for x in "ab"})'),
    ("CLOSURE = frozenset()", "CLOSURE = frozenset({1})"), ("CLOSURE = frozenset()", 'CLOSURE = frozenset(("a",))'),
    ("CLOSURE = frozenset()", 'CLOSURE = frozenset(iterable={"a"})'), ("CLOSURE = frozenset()", "CLOSURE = set()"),
    ("CLOSURE = frozenset()", "CLOSURE = frozenset({})"), ("CLOSURE = frozenset()", 'CLOSURE = frozenset({"a": 1})'),
    ("CLOSURE = frozenset()", 'CLOSURE = frozenset({"a"}, {"b"})'),
    ("CLOSURE = frozenset()", "CLOSURE = frozenset(f())"),
    ("CLOSURE = frozenset()", 'CLOSURE = builtins.frozenset({"a"})'),
])
def test_mutant_a_non_literal_registry_value_is_caught(old, new):
    assert old in GOOD_REGISTRY
    problems = file_problems(GOOD_REGISTRY.replace(old, new))
    assert any("must be a literal" in problem for problem in problems), problems


@pytest.mark.parametrize("head, expected", [
    ('HELPER = "x"\n', "HELPER is not a registry field"),  # an assignment of a str is not a docstring
    ("42\n", "Expr is not allowed"), ('b"doc"\n', "Expr is not allowed"), ("None\n", "Expr is not allowed"),
    ("...\n", "Expr is not allowed"),
])
def test_mutant_a_first_statement_that_is_not_a_docstring_is_not_skipped(head, expected):
    problems = file_problems(head + GOOD_REGISTRY.split("\n", 1)[1])
    assert any(expected in problem for problem in problems), problems


def test_mutant_a_registry_docstring_that_is_not_a_plain_string_is_caught():
    assert file_problems(GOOD_REGISTRY.replace('"""doc."""', 'f"""doc {1}"""'))  # an f-string is code, not a docstring
    assert file_problems(GOOD_REGISTRY.replace('"""doc."""\n', ""))== []  # no docstring at all is fine


# ---- non-vacuity: closure per step ---------------------------------------------------------------------------------
STEP_1 = _real("M2A-1")


def _synthetic(closure_extra: frozenset[str] | set[str] = frozenset(), body: str = ON_M2A_1):
    steps = (*WORLD, _step("M2A-S1", (VIRTUAL_A,), ("M2A-1",), {VIRTUAL_A, *closure_extra}))
    return {VIRTUAL_A: body}, steps


def test_closure_control_a_virtual_step_that_only_adds_itself_is_clean_on_every_pin():
    virtual, steps = _synthetic()
    assert _all_problems(virtual=virtual, steps=steps) == {name: [] for name in
                                                          ("registry", "closure", "direction", "consumer", "legacy")}


@pytest.mark.parametrize("dropped", sorted(STEP_1.closure))
def test_mutant_a_closure_with_one_element_less_is_caught(dropped):
    steps = _swap(WORLD, "M2A-1", closure=STEP_1.closure - {dropped})
    assert f"M2A-1: closure reaches {dropped}" in closure_problems(steps=steps)


@pytest.mark.parametrize("added", ["app.core.money_polarity", "app.core.conversation_store", "app.main"])
def test_mutant_a_closure_with_one_element_more_is_caught(added):
    steps = _swap(WORLD, "M2A-1", closure=STEP_1.closure | {added})
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
    steps = (*WORLD, _step("M2A-S1", (VIRTUAL_A,), ("M2A-1",), {VIRTUAL_A, store_lock}),
             _step("M2A-S2", (VIRTUAL_B,), ("M2A-S1",), {VIRTUAL_B}),
             _step("M2A-S3", (VIRTUAL_C,), ("M2A-S2",), {VIRTUAL_C}))
    assert closure_problems(None, virtual, steps) == []
    undeclared = _swap(steps, "M2A-S3", closure=frozenset({VIRTUAL_C, "app.core.money_polarity"}))
    assert any("M2A-S3: closure lost app.core.money_polarity" in p for p in closure_problems(None, virtual, undeclared))


def test_when_two_steps_grow_towards_the_same_module_the_dependent_one_fails_safe_and_the_fix_is_local():
    """The honest limit of the increment: M2A-1 and M2A-2 both reach ``store_lock``. M2A-2 fails (never silent) with
    ``lost`` and its owner drops the module from its own CLOSURE; M2A-1's registry is not touched."""
    store_lock = "app.core.store_lock"
    virtual = {VIRTUAL_A: ON_M2A_1 + f"import {store_lock}\n"}
    steps = _swap(WORLD, "M2A-2", modules=(VIRTUAL_A,), closure=frozenset({VIRTUAL_A, store_lock}))
    assert closure_problems(None, virtual, steps) == []
    drift = {MODULE: _source(MODULE) + f"\nimport {store_lock}\n"}
    grown = _swap(steps, "M2A-1", closure=_real("M2A-1").closure | {store_lock})
    assert closure_problems(drift, virtual, grown) == [
        f"M2A-2: closure lost {store_lock} (update the registry on purpose)"]
    assert closure_problems(drift, virtual, _swap(grown, "M2A-2", closure=frozenset({VIRTUAL_A}))) == []


def test_the_closure_pin_survives_an_unknown_dependency_that_the_registry_reports():
    steps = (*WORLD, _step("M2A-S1", (), ("M2A-NOPE",)))
    assert closure_problems(steps=steps) == []  # nothing to inherit from a step that does not exist, and no crash
    assert any("unknown step M2A-NOPE" in problem for problem in registry_problems(steps, _present()))


def test_an_unresolvable_dynamic_import_outside_the_registered_closure_is_not_the_closure_pins_business():
    virtual = {VIRTUAL_A: "import importlib\n\ndef _late(name):\n    return importlib.import_module(name)\n"}
    assert VIRTUAL_A in _graph(None, virtual, WORLD)[2]
    assert closure_problems(None, virtual, WORLD) == []  # unregistered, unreached: a limited guarantee (docstring)


def test_a_step_with_no_dependency_declares_the_whole_closure():
    steps = (*WORLD, _step("M2A-S1", (VIRTUAL_A,), (), {VIRTUAL_A}))  # the synthetic step with DEPENDS_ON dropped
    problems = closure_problems(None, {VIRTUAL_A: ON_M2A_1}, steps)
    assert f"M2A-S1: closure reaches {MODULE}" in problems and "M2A-S1: closure reaches app.core" in problems


@pytest.mark.parametrize("module", ["app.core.query_router", "app.core.followup_engine", "app.core.followup_mutations",
                                    "app.core.conversation_store", "app.core.orchestrator",
                                    "app.core.foundation_runtime"])
def test_mutant_registering_a_legacy_or_forbidden_module_does_not_launder_it(module):
    """Registration opens no door: the module is still forbidden in the closure, still consumed by unregistered
    modules and still part of what legacy (or the Foundation runtime) reaches."""
    steps = (*WORLD, _step("M2A-S1", (module,), ("M2A-1",)))
    assert f"forbidden {module}" in closure_problems(steps=steps)
    assert consumer_problems(steps=steps), "its importers are unregistered modules"
    assert any(module in problem for problem in legacy_reach_problems(steps=steps)) or module == "app.core.orchestrator"


@pytest.mark.parametrize("target", [
    "app.core.accreditation.coverage", "app.core.workflow.store_epoch", "app.data.smmlv",
    "app.core.host_authorities.state", "app.core.foundation_authority_registry",
    "app.core.foundation_authority_contract", "app.core.foundation_evidence_window", "app.core.foundation_admission",
])
def test_mutant_a_forbidden_prefix_is_caught_even_when_the_closure_is_declared_exactly(target):
    """One real module per FORBIDDEN_PREFIX that no other clause (module list, suffix) forbids: the step declares its
    own increment exactly, so the prefix clause is the only one that can object."""
    assert target in _graph()[0]
    assert target.startswith(FORBIDDEN_PREFIXES) and target not in FORBIDDEN_MODULES
    assert not target.endswith(FORBIDDEN_SUFFIXES)
    package, leaf = target.rsplit(".", 1)
    virtual = {VIRTUAL_A: ON_M2A_1 + f"from {package} import {leaf}\n"}
    problems = closure_problems(None, virtual, _exact_step(virtual, VIRTUAL_A))
    assert f"forbidden {target}" in problems
    assert all(problem.startswith("forbidden") for problem in problems), problems


def test_every_forbidden_prefix_has_a_representative_above():
    covered = {"app.core.accreditation", "app.core.workflow", "app.data", "app.core.host_authorities",
               "app.core.foundation_authority_registry", "app.core.foundation_authority_contract",
               "app.core.foundation_evidence_window", "app.core.foundation_admission"}
    assert set(FORBIDDEN_PREFIXES) == covered  # a new prefix needs its own mutant


@pytest.mark.parametrize("name", ["app.core.m2a_virtual_resolver", "app.core.m2a_virtual_authority",
                                  "app.core.m2a_virtual_authorities"])
def test_mutant_a_registered_module_named_as_an_authority_or_resolver_is_forbidden(name):
    steps = (*WORLD, _step("M2A-S1", (name,), ("M2A-1",), {name}))
    assert f"forbidden {name}" in closure_problems(None, {name: ON_M2A_1}, steps)


# ---- non-vacuity: direction ----------------------------------------------------------------------------------------
def _chain(*, b_depends=(), c_depends=(), a_body=ON_M2A_1, b_body=IMPORT_A, c_body=IMPORT_B):
    virtual = {VIRTUAL_A: a_body, VIRTUAL_B: b_body, VIRTUAL_C: c_body}
    steps = (*WORLD, _step("M2A-S1", (VIRTUAL_A,), ("M2A-1",)), _step("M2A-S2", (VIRTUAL_B,), b_depends),
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
    steps = (*WORLD, _step("M2A-S1", (VIRTUAL_A, VIRTUAL_B), ("M2A-1",)))
    assert direction_problems(None, virtual, steps) == []


def test_mutant_a_real_module_importing_a_step_that_the_step_does_not_depend_on():
    """Same shape on a real module: foundation_accepted (M2A-1, no dependencies) imports a later step's module."""
    virtual, steps = _synthetic()
    overrides = {MODULE: _source(MODULE) + LATE_IMPORT_OF_A}
    problems = direction_problems(overrides, virtual, steps)
    assert any(f"{MODULE} (M2A-1) imports {VIRTUAL_A} (M2A-S1)" in problem for problem in problems), problems


# ---- non-vacuity: consumption (the definitive invariant) -----------------------------------------------------------
@pytest.mark.parametrize("name", ["app.core.m2a_virtual_ext", "app.m2a_virtual_ext", "scripts.m2a_virtual_ext"])
def test_mutant_an_unregistered_virtual_module_importing_m2a_is_caught(name):
    assert consumer_problems(None, {name: ON_M2A_1}, WORLD) == [f"{name} imports {MODULE}"]


def test_consumer_control_the_same_source_registered_in_a_step_is_not_a_consumer():
    virtual, steps = _synthetic()
    assert consumer_problems(None, virtual, steps) == []
    assert consumer_problems(None, virtual, WORLD) == [f"{VIRTUAL_A} imports {MODULE}"]  # unregistered: caught


def test_mutant_an_unregistered_module_importing_a_registered_virtual_module_is_caught():
    virtual, steps = _synthetic()
    overrides = {"app.core.orchestrator": _source("app.core.orchestrator") + LATE_IMPORT_OF_A}
    assert f"app.core.orchestrator imports {VIRTUAL_A}" in consumer_problems(overrides, virtual, steps)


def test_mutant_a_wildcard_dynamic_import_that_can_load_m2a_is_a_consumer():
    """``importlib.import_module(f"app.core.{name}")`` can load any module under the prefix: it reaches M2a."""
    extra = "\n\ndef _late(name):\n    import importlib\n    return importlib.import_module(f'app.core.{name}')\n"
    problems = consumer_problems({"app.core.store_lock": _source("app.core.store_lock") + extra}, None, WORLD)
    assert f"app.core.store_lock imports {MODULE}" in problems


# ---- non-vacuity: legacy and Foundation runtime never reach M2a ----------------------------------------------------
def test_mutant_the_legacy_closure_reaching_a_registered_module_is_caught():
    virtual, steps = _synthetic()
    overrides = {"app.core.conversation_store":
                 _source("app.core.conversation_store") + LATE_IMPORT_OF_A}
    assert legacy_reach_problems(overrides, virtual, steps) == [
        f"legacy closure reaches {MODULE}", f"legacy closure reaches {VIRTUAL_A}",
        f"legacy closure reaches {LEGACY_MUST_STAY_OUT}"]  # A imports M2A-1, which imports the IR: all three, no echo


def test_mutant_the_foundation_runtime_reaching_a_registered_module_is_caught_apart_from_legacy():
    virtual, steps = _synthetic()
    overrides = {"app.core.foundation_runtime":
                 _source("app.core.foundation_runtime") + LATE_IMPORT_OF_A}
    problems = legacy_reach_problems(overrides, virtual, steps)
    assert problems == [f"Foundation runtime closure reaches {MODULE}",
                        f"Foundation runtime closure reaches {VIRTUAL_A}"]  # the mode frontier keeps legacy clear


def test_mutant_the_legacy_closure_reaching_the_ir_wire_module_is_caught():
    overrides = {"app.core.conversation_store": _source("app.core.conversation_store") + LATE_IMPORT_OF_IR}
    assert legacy_reach_problems(overrides, None, WORLD) == [f"legacy closure reaches {LEGACY_MUST_STAY_OUT}"]


def test_mutant_the_real_m2a_module_in_the_legacy_closure_is_caught():
    overrides = {"app.core.conversation_store":
                 _source("app.core.conversation_store") + LATE_IMPORT_OF_M2A}
    problems = legacy_reach_problems(overrides, None, WORLD)
    assert f"legacy closure reaches {MODULE}" in problems
    assert f"legacy closure reaches {LEGACY_MUST_STAY_OUT}" in problems  # M2A-1 imports the IR: the legacy path too


# ---- robustness: every pin against SIMULATED, filled lane registries ------------------------------------------------
# A lane fills its own registry file, so no pin of this prep may depend on what a lane holds. These registries simulate
# the two lanes AFTER they land: a store module that imports M2A-1 and ``store_lock`` (M2A-2) and a reducer split in two
# modules over the IR v2 vocabulary (M2A-3). Every pin must stay green on them, and every mutant keep catching its own.
SIM_STORE, SIM_DELTA, SIM_SCOPE = "app.core.m2a_sim_store", "app.core.m2a_sim_delta", "app.core.m2a_sim_scope"
IR_V2_VOCABULARY = ("from app.core import (\n    foundation_operations, foundation_operator_table, "
                    "interpretation_contract, semantic_dimensions,\n)\n")
SIM_VIRTUAL = {
    SIM_STORE: ON_M2A_1 + "import app.core.store_lock\n",
    SIM_DELTA: ON_M2A_1 + IR_V2_VOCABULARY,
    SIM_SCOPE: ON_M2A_1 + "from app.core import interpretation_contract\n",
}
SIM = (_real("M2A-1"), _step("M2A-2", (SIM_STORE,), ("M2A-1",), {SIM_STORE, "app.core.store_lock"}),
       _step("M2A-3", (SIM_DELTA, SIM_SCOPE), ("M2A-1",), {SIM_DELTA, SIM_SCOPE}))
PIN_NAMES = ("registry", "closure", "direction", "consumer", "legacy")


def _render(step: Step) -> str:
    """A registry file for a step, in the only shape allowed: a docstring and four literal assignments."""
    closure_text = "frozenset({" + ", ".join(repr(item) for item in sorted(step.closure)) + "})" if step.closure \
        else "frozenset()"
    return (f'"""{step.name} (simulated)."""\n\nSTEP = {step.name!r}\nMODULES = {step.modules!r}\n'
            f"DEPENDS_ON = {step.depends_on!r}\nCLOSURE = {closure_text}\n")


def _namespace_of(source: str) -> dict:
    """The namespace of a literal-only registry file, read by the AST (no ``exec``)."""
    out = {}
    for node in ast.parse(source).body[1:]:
        value = node.value
        if isinstance(value, ast.Call):  # frozenset(...)
            out[node.targets[0].id] = frozenset(ast.literal_eval(value.args[0])) if value.args else frozenset()
        else:
            out[node.targets[0].id] = ast.literal_eval(value)
    return out


def _with(module: str, extra: str) -> dict[str, str]:
    return {**SIM_VIRTUAL, module: SIM_VIRTUAL[module] + extra}


def _declared(virtual: dict[str, str], steps: tuple[Step, ...]) -> tuple[Step, ...]:
    """The same steps with every lane CLOSURE declared exactly from the graph (a lane that did its job), so only a
    forbidden clause can object."""
    _, graph, _ = _graph(None, virtual, steps)
    dependencies = _dependencies(steps)
    by_name = {step.name: step for step in steps}
    out = []
    for step in steps:
        if step.name == "M2A-1":
            out.append(step)
            continue
        inherited = set(closure(graph, entrypoints=tuple(m for d in dependencies[step.name]
                                                         for m in by_name[d].modules), cut=()))
        out.append(replace(step, closure=frozenset(set(closure(graph, entrypoints=step.modules, cut=())) - inherited)))
    return tuple(out)


def _conversation_store(extra: str) -> dict[str, str]:
    return {"app.core.conversation_store": _source("app.core.conversation_store") + extra}


def test_robustness_every_pin_is_green_on_simulated_filled_lane_registries():
    assert _all_problems(virtual=SIM_VIRTUAL, steps=SIM) == {name: [] for name in PIN_NAMES}
    assert dag_problems(SIM) == []
    assert all(source_problems(source) == [] for source in SIM_VIRTUAL.values())
    assert registered_modules(SIM) == (MODULE, SIM_STORE, SIM_DELTA, SIM_SCOPE)


def test_robustness_the_simulated_registries_are_well_formed_files():
    namespaces = {}
    for step in SIM:
        source = _render(step)
        assert file_problems(source) == [], source
        namespaces[step.stem] = _namespace_of(source)
        assert step_from_namespace(step.stem, namespaces[step.stem]) == (step, [])
    assert steps_from_namespaces(namespaces) == SIM  # file order: m2a_1, m2a_2, m2a_3


def test_robustness_the_same_mutant_gets_the_same_verdict_over_empty_and_over_filled_lanes():
    for steps, virtual in ((WORLD, {}), (SIM, SIM_VIRTUAL)):
        virtual = {**virtual, VIRTUAL_A: ON_M2A_1 + "import app.core.money_polarity\n"}
        extended = (*steps, _step("M2A-S1", (VIRTUAL_A,), ("M2A-1",), {VIRTUAL_A}))
        assert "M2A-S1: closure reaches app.core.money_polarity" in closure_problems(None, virtual, extended)


SIM_MUTANTS = {
    "closure: M2A-2 leaves store_lock out of its CLOSURE": (
        "M2A-2: closure reaches app.core.store_lock",
        lambda: closure_problems(None, SIM_VIRTUAL, _swap(SIM, "M2A-2", closure=frozenset({SIM_STORE})))),
    "closure: M2A-3 declares a module it does not reach": (
        "M2A-3: closure lost app.core.money_polarity",
        lambda: closure_problems(None, SIM_VIRTUAL, _swap(SIM, "M2A-3", closure=frozenset(
            {SIM_DELTA, SIM_SCOPE, "app.core.money_polarity"})))),
    "closure: the reducer reaches something undeclared": (
        "M2A-3: closure reaches app.core.money_polarity",
        lambda: closure_problems(None, _with(SIM_DELTA, "import app.core.money_polarity\n"), SIM)),
    "closure: a forbidden prefix, with the CLOSURE declared exactly": (
        "forbidden app.core.workflow.store_epoch",
        lambda: closure_problems(None, _with(SIM_SCOPE, "from app.core.workflow import store_epoch\n"),
                                 _declared(_with(SIM_SCOPE, "from app.core.workflow import store_epoch\n"), SIM))),
    "closure: the store reaches a legacy module": (
        "forbidden app.core.conversation_store",
        lambda: closure_problems(None, _with(SIM_STORE, "from app.core import conversation_store\n"),
                                 _declared(_with(SIM_STORE, "from app.core import conversation_store\n"), SIM))),
    "closure: an unresolvable dynamic import in the store": (
        f"unresolvable dynamic import in {SIM_STORE}",
        lambda: closure_problems(None, _with(SIM_STORE, "import importlib\n\ndef _l(n):\n    return "
                                                         "importlib.import_module(n)\n"), SIM)),
    "direction: the reducer imports the store": (
        f"{SIM_DELTA} (M2A-3) imports {SIM_STORE} (M2A-2) without DEPENDS_ON",
        lambda: direction_problems(None, _with(SIM_DELTA, "from app.core import m2a_sim_store\n"), SIM)),
    "direction: the store imports the reducer": (
        f"{SIM_STORE} (M2A-2) imports {SIM_DELTA} (M2A-3) without DEPENDS_ON",
        lambda: direction_problems(None, _with(SIM_STORE, "from app.core import m2a_sim_delta\n"), SIM)),
    "dag: the reducer widens its own DEPENDS_ON to the store": (
        "M2A-3 DEPENDS_ON ['M2A-1', 'M2A-2'] but the reviewed DAG says ['M2A-1']",
        lambda: dag_problems(_swap(SIM, "M2A-3", depends_on=("M2A-1", "M2A-2")))),
    "dag: the store starts depending on the reducer": (
        "M2A-2 DEPENDS_ON ['M2A-1', 'M2A-3'] but the reviewed DAG says ['M2A-1']",
        lambda: dag_problems(_swap(SIM, "M2A-2", depends_on=("M2A-1", "M2A-3")))),
    "consumer: an unregistered app module imports the store": (
        "app.core.m2a_sim_ext imports app.core.m2a_sim_store",
        lambda: consumer_problems(None, {**SIM_VIRTUAL, "app.core.m2a_sim_ext": "from app.core import m2a_sim_store\n"},
                                  SIM)),
    "consumer: a script imports the reducer": (
        "scripts.m2a_sim_ext imports app.core.m2a_sim_delta",
        lambda: consumer_problems(None, {**SIM_VIRTUAL, "scripts.m2a_sim_ext": "from app.core import m2a_sim_delta\n"},
                                  SIM)),
    "consumer: the orchestrator imports the scope module": (
        f"app.core.orchestrator imports {SIM_SCOPE}",
        lambda: consumer_problems({"app.core.orchestrator": _source("app.core.orchestrator")
                                   + f"\n\ndef _m2a():\n    import {SIM_SCOPE}\n"}, SIM_VIRTUAL, SIM)),
    "legacy: conversation_store imports the store": (
        f"legacy closure reaches {SIM_STORE}",
        lambda: legacy_reach_problems(_conversation_store(f"\n\ndef _m2a():\n    import {SIM_STORE}\n"),
                                      SIM_VIRTUAL, SIM)),
    "legacy: foundation_runtime imports the reducer": (
        f"Foundation runtime closure reaches {SIM_DELTA}",
        lambda: legacy_reach_problems({"app.core.foundation_runtime": _source("app.core.foundation_runtime")
                                       + f"\n\ndef _m2a():\n    import {SIM_DELTA}\n"}, SIM_VIRTUAL, SIM)),
    "legacy: conversation_store imports the IR wire module": (
        f"legacy closure reaches {LEGACY_MUST_STAY_OUT}",
        lambda: legacy_reach_problems(_conversation_store(LATE_IMPORT_OF_IR), SIM_VIRTUAL, SIM)),
    "registry: the reducer also registers the store's module": (
        "registered twice",
        lambda: registry_problems(_swap(SIM, "M2A-3", modules=(SIM_DELTA, SIM_SCOPE, SIM_STORE)),
                                  _present(*SIM_VIRTUAL))),
    "registry: the reducer registers a module that already existed": (
        "already existed before M2a",
        lambda: registry_problems(_swap(SIM, "M2A-3", modules=(SIM_DELTA, "app.core.polysemic")),
                                  _present(*SIM_VIRTUAL))),
    "registry: the store registers something outside app.core": (
        "is not an app.core module",
        lambda: registry_problems(_swap(SIM, "M2A-2", modules=(SIM_STORE, "app.main")), _present(*SIM_VIRTUAL))),
    "registry: the store registers a module that does not exist": (
        "does not exist",
        lambda: registry_problems(_swap(SIM, "M2A-2", modules=(SIM_STORE, "app.core.m2a_sim_nowhere")),
                                  _present(*SIM_VIRTUAL))),
    "file: the reducer's registry file holds an import": (
        "Import is not allowed", lambda: file_problems(_render(SIM[2]) + "import os\n")),
    "file: the store's registry file computes its closure": (
        "must be a literal",
        lambda: file_problems(_render(SIM[1]).replace("CLOSURE = frozenset({", "CLOSURE = _BASE | frozenset({"))),
    "file: the store's registry file names another step": (
        "does not name this file",
        lambda: step_from_namespace("m2a_2", {**_namespace_of(_render(SIM[1])), "STEP": "M2A-3"})[1]),
    "source: the reducer names a legacy input type": (
        "bridge identifier",
        lambda: source_problems(SIM_VIRTUAL[SIM_DELTA] + "\ndef to_parsed_params(x):\n    pass\n")),
    "source: the store imports a regex engine": (
        "regex engine import", lambda: source_problems(SIM_VIRTUAL[SIM_STORE] + "\nimport re\n")),
}


@pytest.mark.parametrize("label", list(SIM_MUTANTS))
def test_robustness_each_mutant_still_catches_its_own_on_the_simulated_registries(label):
    expected, run = SIM_MUTANTS[label]
    problems = run()
    assert any(expected in problem for problem in problems), problems


# ---- non-vacuity: imports, bridge, lexicon, dynamic import (the M2A-1a mutants, unchanged) -------------------------
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


@pytest.mark.parametrize("extra", [
    "\n\ndef _arg(queryframe):\n    pass\n",  # an argument
    "\n\nasync def to_parsed_params():\n    pass\n",  # an async function
    "\n\nclass QueryFrame:\n    pass\n",  # a class
    "\n\n_x = dict(parsed_params=1)\n",  # a keyword
    "\n\nimport json as parsed_params\n",  # an alias
    "\n\nfrom json import dumps as query_router\n",  # an alias of a from-import
    "\n\nimport conversation_context\n",  # an imported name
    "\n\ndef _accredit(window):\n    return resolve(window)\n",  # a bare resolve() call
])
def test_mutant_every_kind_of_bridge_identifier_is_caught(extra):
    assert source_problems(_source(MODULE) + extra)


@pytest.mark.parametrize("identifier", ["parsed_params", "queryframe", "conversationcontext", "followupengine",
                                        "queryrouter", "followup_engine", "query_router", "query_frame",
                                        "conversation_context"])
def test_mutant_each_forbidden_identifier_is_caught_on_its_own(identifier):
    assert set(FORBIDDEN_IDENTIFIERS) == {"parsed_params", "queryframe", "conversationcontext", "followupengine",
                                          "queryrouter", "followup_engine", "query_router", "query_frame",
                                          "conversation_context"}
    assert source_problems(f"def _x({identifier}):\n    pass\n")


@pytest.mark.parametrize("engine", ["re", "regex", "fnmatch", "sre_compile", "sre_parse"])
def test_mutant_each_regex_engine_is_caught_on_its_own(engine):
    assert set(REGEX_MODULES) == {"re", "regex", "fnmatch", "sre_compile", "sre_parse"}
    assert any("regex engine import" in problem for problem in source_problems(f"import {engine}\n"))


@pytest.mark.parametrize("marker", ["lexic", "lexeme", "vocab", "stopword", "keyword", "trigger", "synonym", "_terms",
                                    "phrases", "text_predicates", "normaliz", "grammar"])
def test_mutant_each_lexicon_marker_is_caught_on_its_own(marker):
    assert set(LEXICON_MARKERS) == {"lexic", "lexeme", "vocab", "stopword", "keyword", "trigger", "synonym", "_terms",
                                    "phrases", "text_predicates", "normaliz", "grammar"}
    assert any("lexicon import" in problem for problem in source_problems(f"from app.core.m2a{marker}x import y\n"))


@pytest.mark.parametrize("source", ["", "from . import foundation_operations\n", "from .. import x\n", "f(**kw)\n",
                                    "x = {'a': 1}\n\ndef f(a, *rest, **kw):\n    return a\n"])
def test_the_source_scan_does_not_crash_on_unusual_but_harmless_code(source):
    assert source_problems(source) == []


def test_the_docstring_mention_of_the_barrier_is_not_a_bridge():
    assert "parsed_params" in _source(MODULE)  # the module documents the barrier...
    assert source_problems(_source(MODULE)) == []  # ...and the pin reads code, not prose


def test_entrypoints_are_the_legacy_ones():
    assert "app.core.query_router" in ENTRYPOINTS and "app.core.orchestrator" in ENTRYPOINTS
