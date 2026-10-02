"""M2a boundary pins (contract §3.7, INV-M2A-04, INV-M2A-24; mission M2A-1 invariants 4, 5 and 6).

* IMPORTS: the transitive import closure of every M2a module is EXACTLY the pinned allowlist. In particular no M2a
  module reaches ``query_router``, ``followup_engine``, ``followup_mutations``, the conversation store, the
  orchestrator, owner detectors or ``accreditation`` (the contract allows only the future ``accredited_span`` leaf,
  which does not exist yet: adding it means adding it here, by name).
* NO BRIDGE: no M2a module names ``parsed_params`` or a legacy input type (``QueryFrame``, ``ConversationContext``,
  ``FollowupEngine``, ``QueryRouter``) in code: no function, attribute, argument, keyword or string key.
* NO LEXICON: no M2a module imports a regex engine or a lexicon module (A8.1 and A9.1 decide on typed values).
* NO CONSUMER: M2A-1 has no productive consumer. No ``app``/``scripts``/``streamlit_app`` module imports an M2a
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
FORBIDDEN_PREFIXES = ("app.core.accreditation", "app.core.workflow", "app.data")
FORBIDDEN_IDENTIFIERS = ("parsed_params", "queryframe", "conversationcontext", "followupengine", "queryrouter",
                         "followup_engine", "query_router", "query_frame", "conversation_context")
REGEX_MODULES = frozenset({"re", "regex", "fnmatch", "sre_compile", "sre_parse"})
LEXICON_MARKERS = ("lexic", "lexeme", "vocab", "stopword", "keyword", "trigger", "synonym", "_terms", "phrases",
                   "text_predicates", "normaliz", "grammar")


def _source(module: str) -> str:
    return (ROOT / (module.replace(".", "/") + ".py")).read_text(encoding="utf-8")


def _m2a_closure(overrides: dict[str, str] | None = None) -> dict:
    _, graph, _ = build_graph(overrides)
    return closure(graph, entrypoints=M2A_MODULES, cut=())


def closure_problems(overrides: dict[str, str] | None = None) -> list[str]:
    reached = set(_m2a_closure(overrides))
    out = [f"M2a closure reaches {name}" for name in sorted(reached - M2A_ALLOWED_CLOSURE)]
    out += [f"M2a closure lost {name} (update the allowlist on purpose)"
            for name in sorted(M2A_ALLOWED_CLOSURE - reached)]
    out += [f"forbidden {name}" for name in sorted(reached)
            if name in FORBIDDEN_MODULES or name.startswith(FORBIDDEN_PREFIXES)]
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
        for name in names:
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
])
def test_mutant_legacy_import_is_caught(extra):
    assert closure_problems({MODULE: _source(MODULE) + extra})


@pytest.mark.parametrize("extra", [
    "\n\ndef to_parsed_params(effective):\n    return {}\n",
    "\n\ndef _bridge(effective):\n    return {'parsed_params': effective.to_dict()}\n",
    "\n\ndef _frame(effective, frame):\n    return frame.QueryFrame\n",
    "\n\nimport re\n",
    "\n\nfrom app.core.foundation_text_predicates import nothing\n",
])
def test_mutant_bridge_or_lexicon_is_caught(extra):
    assert source_problems(_source(MODULE) + extra)


def test_mutant_productive_consumer_is_caught():
    orchestrator = _source("app.core.orchestrator")
    mutant = orchestrator + "\n\ndef _m2a():\n    from app.core.foundation_accepted import AcceptedTurnRecord\n"
    assert consumer_problems({"app.core.orchestrator": mutant})


def test_the_docstring_mention_of_the_barrier_is_not_a_bridge():
    assert "parsed_params" in _source(MODULE)  # the module documents the barrier...
    assert source_problems(_source(MODULE)) == []  # ...and the pin reads code, not prose


def test_entrypoints_are_the_legacy_ones():
    assert "app.core.query_router" in ENTRYPOINTS and "app.core.orchestrator" in ENTRYPOINTS
