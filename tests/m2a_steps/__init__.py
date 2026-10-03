"""Per-step registry of the M2a modules (wave-1 prep, user decision 2026-10-02).

One file per implementation step (``m2a_1.py``, ``m2a_2.py``, ...); each lane writes ONLY its own file, so two
parallel lanes never edit a shared list. A file is discovered by its name and must define exactly:

* ``STEP``        the step name, ``M2A-<n>`` (``M2A-3b`` is allowed); ``M2A-1`` lives in ``m2a_1.py``
                  (``STEP.lower().replace("-", "_")`` = file stem);
* ``MODULES``     the ``app.core.*`` modules the step adds (an empty tuple until the step lands);
* ``DEPENDS_ON``  the steps whose modules this step may import (internal M2a consumption is only along these edges;
                  the wave's dependency graph is a REVIEWED constant, ``PLANNED_DEPENDENCIES``, not this file's call);
* ``CLOSURE``     the EXACT transitive import closure of ``MODULES`` minus the closure of the steps it depends on
                  (the step's own increment): growing it is a reviewed decision of that step, never a side effect.

A registry file is DATA, not code: a docstring and exactly those four assignments, each a literal (``str``, a tuple
of ``str``, ``frozenset({...})`` or ``frozenset()``). No imports, helpers, expressions or other statements
(``file_problems``, pinned by ``tests/test_m2a_boundaries.py``).

The pins live in ``tests/test_m2a_boundaries.py`` (closure, direction, legacy and Foundation runtime never reach
M2a, no external consumer) and ``tests/test_foundation_authority_contract.py`` (C1.x closed-type exception).
"""
from __future__ import annotations

import ast
import importlib
import re
from dataclasses import dataclass
from pathlib import Path

FIELDS = frozenset({"STEP", "MODULES", "DEPENDS_ON", "CLOSURE"})
STEP_NAME = re.compile(r"M2A-[0-9]+[a-z0-9]*")


@dataclass(frozen=True)
class Step:
    name: str
    stem: str
    modules: tuple[str, ...]
    depends_on: tuple[str, ...]
    closure: frozenset[str]


def step_from_namespace(stem: str, namespace: dict) -> tuple[Step | None, list[str]]:
    """A registry file's public names as a ``Step``, or why they are not one (strict: no extra, no missing field)."""
    public = {name for name in namespace if not name.startswith("_") and name.isupper()}
    problems = [f"{stem}: unknown field {name}" for name in sorted(public - FIELDS)]
    problems += [f"{stem}: missing field {name}" for name in sorted(FIELDS - public)]
    if problems:
        return None, problems
    name, modules, depends, closure = (namespace[field] for field in ("STEP", "MODULES", "DEPENDS_ON", "CLOSURE"))
    if type(name) is not str or not STEP_NAME.fullmatch(name) or name.lower().replace("-", "_") != stem:
        problems.append(f"{stem}: STEP {name!r} does not name this file (M2A-<n> in m2a_<n>.py)")
    for field, value in (("MODULES", modules), ("DEPENDS_ON", depends)):
        if type(value) is not tuple or not all(type(item) is str for item in value):
            problems.append(f"{stem}: {field} must be a tuple of str")
    if type(closure) is not frozenset or not all(type(item) is str for item in closure):
        problems.append(f"{stem}: CLOSURE must be a frozenset of str")
    if problems:
        return None, problems
    return Step(name, stem, modules, depends, closure), []


def _str_literals(node: ast.AST, kinds: tuple[type, ...]) -> bool:
    return isinstance(node, kinds) and all(
        isinstance(item, ast.Constant) and type(item.value) is str for item in node.elts)


def _literal_value(node: ast.AST) -> bool:
    """``"x"``, ``("a", "b")``, ``frozenset({"a"})`` or ``frozenset()``: nothing that computes."""
    if isinstance(node, ast.Constant):
        return type(node.value) is str
    if isinstance(node, ast.Tuple):
        return _str_literals(node, (ast.Tuple,))
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "frozenset"
            and not node.keywords
            and (not node.args or (len(node.args) == 1 and _str_literals(node.args[0], (ast.Set,)))))


def file_problems(source: str) -> list[str]:
    """Why a registry file is more than its docstring and the four literal assignments (it must hold no logic)."""
    body = ast.parse(source).body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
            and type(body[0].value.value) is str:
        body = body[1:]
    problems, assigned = [], []
    for node in body:
        if not (isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)):
            problems.append(f"line {node.lineno}: only a docstring and plain NAME = literal assignments "
                            f"({type(node).__name__} is not allowed)")
            continue
        name = node.targets[0].id
        assigned.append(name)
        if name not in FIELDS:
            problems.append(f"line {node.lineno}: {name} is not a registry field")
        elif not _literal_value(node.value):
            problems.append(f"line {node.lineno}: {name} must be a literal (str, tuple of str, frozenset({{...}}))")
    problems += [f"{name} is assigned more than once" for name in sorted(set(assigned)) if assigned.count(name) > 1]
    return problems


def steps_from_namespaces(namespaces: dict[str, dict]) -> tuple[Step, ...]:
    """Steps from ``file stem -> module namespace``, in stem order. A malformed registry raises, never dropped."""
    steps, problems = [], []
    for stem in sorted(namespaces):
        step, found = step_from_namespace(stem, namespaces[stem])
        problems += found
        if step is not None:
            steps.append(step)
    if problems:
        raise ValueError("malformed M2a step registry:\n" + "\n".join(problems))
    return tuple(steps)


def load_steps() -> tuple[Step, ...]:
    """Every ``m2a_*.py`` registry next to this file, in file order. A malformed registry raises."""
    stems = sorted(path.stem for path in Path(__file__).parent.glob("m2a_*.py"))
    return steps_from_namespaces({stem: vars(importlib.import_module(f"{__name__}.{stem}")) for stem in stems})


STEPS = load_steps()


def registered_modules(steps: tuple[Step, ...] = STEPS) -> tuple[str, ...]:
    return tuple(module for step in steps for module in step.modules)
