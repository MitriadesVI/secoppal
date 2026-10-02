"""M2a boundary pins (contract §3.7, INV-M2A-04, INV-M2A-24; mission M2A-1a invariants 4, 5 and 6).

* IMPORTS: the transitive import closure of every M2a module is EXACTLY the pinned allowlist. In particular no M2a
  module reaches ``query_router``, ``followup_engine``, ``followup_mutations``, the conversation store, the
  orchestrator, owner detectors or ``accreditation`` (the contract allows only the future ``accredited_span`` leaf,
  which does not exist yet: adding it means adding it here, by name).
* NO BRIDGE: no M2a module names ``parsed_params`` or a legacy input type (``QueryFrame``, ``ConversationContext``,
  ``FollowupEngine``, ``QueryRouter``) in code: no function, attribute, argument, keyword or string key.
* NO LEXICON: no M2a module imports a regex engine or a lexicon module (A8.1 and A9.1 decide on typed values).
* NO DYNAMIC IMPORT: no M2a module uses ``__import__``, ``importlib`` or ``import_module``, whatever the argument
  (literal or variable): the static closure above is complete only because nothing in M2a imports at run time.
  These are not resolved, they are forbidden; and an unresolvable dynamic import inside the closure fails the pin.
* NO CONSUMER: M2A-1a has no productive consumer. No ``app``/``scripts``/``streamlit_app`` module imports an M2a
  module, so neither the legacy closure nor the Foundation runtime can observe it (no runtime change).

Each pin is shown not to be vacuous by a mutant it must catch. The import graph is the AST scanner of
``tests/test_m1_wave1_parity_pins.py`` (static, relative, lazy, ``importlib`` and dotted string literals).
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.test_m1_wave1_parity_pins import ENTRYPOINTS, build_graph, closure

ROOT = Path(__file__).resolve().parents[1]

# FROZEN: every module M2a adds. Each M2A-n step appends its own modules here.
M2A_MODULES = ("app.core.foundation_accepted",)

# FROZEN: the exact transitive import closure of M2A_MODULES. Growing it is a reviewed decision, never a side effect.
M2A_ALLOWED_CLOSURE = frozenset({
    "app", "app.core",
    "app.core.foundation_accepted",
    "app.core.foundation_authority_values",
    "app.core.foundation_geo_level",
    "app.core.foundation_operations",
    "app.core.foundation_operator_table",
    "app.core.interpretation_contract",
    "app.core.semantic_dimensions",
})

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


def _source(module: str) -> str:
    return (ROOT / (module.replace(".", "/") + ".py")).read_text(encoding="utf-8")


def closure_problems(overrides: dict[str, str] | None = None) -> list[str]:
    _, graph, flagged = build_graph(overrides)
    reached = set(closure(graph, entrypoints=M2A_MODULES, cut=()))
    out = [f"M2a closure reaches {name}" for name in sorted(reached - M2A_ALLOWED_CLOSURE)]
    out += [f"M2a closure lost {name} (update the allowlist on purpose)"
            for name in sorted(M2A_ALLOWED_CLOSURE - reached)]
    out += [f"forbidden {name}" for name in sorted(reached)
            if name in FORBIDDEN_MODULES or name.startswith(FORBIDDEN_PREFIXES) or name.endswith(FORBIDDEN_SUFFIXES)]
    out += [f"unresolvable dynamic import in {name}" for name in sorted(flagged & reached)]
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


def consumer_problems(overrides: dict[str, str] | None = None) -> list[str]:
    _, graph, _ = build_graph(overrides)
    return [f"{name} imports {target}" for name, edges in sorted(graph.items()) if name not in M2A_MODULES
            for target in sorted(edges & set(M2A_MODULES))]


# ---- the pins ------------------------------------------------------------------------------------------------------
def test_the_m2a_import_closure_is_exactly_the_allowlist():
    assert closure_problems() == []


@pytest.mark.parametrize("module", M2A_MODULES)
def test_no_bridge_no_regex_no_lexicon(module):
    assert source_problems(_source(module)) == []


def test_m2a_has_no_consumer_yet():
    assert consumer_problems() == []


def test_the_legacy_closure_never_reaches_m2a_nor_the_51_directives():
    _, graph, _ = build_graph()
    reached = set(closure(graph))  # legacy entrypoints, cutting only the declared mode frontier
    assert not reached & set(M2A_MODULES)
    assert "app.core.interpretation_contract" not in reached  # the touched wire module stays out of legacy
    assert set(closure(graph, cut=())) & set(M2A_MODULES) == set()  # not even through the Foundation runtime


# ---- non-vacuity: each pin catches its mutant ----------------------------------------------------------------------
MODULE = M2A_MODULES[0]


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


@pytest.mark.parametrize("consumer, extra", [
    ("app.core.orchestrator", "\n\ndef _m2a():\n    from app.core.foundation_accepted import AcceptedTurnRecord\n"),
    ("app.core.foundation_runtime", "\n\nfrom app.core import foundation_accepted\n"),
    ("app.core.foundation_projection",
     "\n\ndef _m2a():\n    import importlib\n    return importlib.import_module('app.core.foundation_accepted')\n"),
    ("streamlit_app", "\n\nimport app.core.foundation_accepted as _m2a\n"),
])
def test_mutant_productive_consumer_is_caught(consumer, extra):
    """M2A-1a has no runtime consumer: any productive module importing it kills the pin."""
    assert consumer_problems({consumer: _source(consumer) + extra})


def test_the_docstring_mention_of_the_barrier_is_not_a_bridge():
    assert "parsed_params" in _source(MODULE)  # the module documents the barrier...
    assert source_problems(_source(MODULE)) == []  # ...and the pin reads code, not prose


def test_entrypoints_are_the_legacy_ones():
    assert "app.core.query_router" in ENTRYPOINTS and "app.core.orchestrator" in ENTRYPOINTS
