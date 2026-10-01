"""M1 wave 1 PARIDAD (TIME + VALUE): pins that keep the legacy path equal to ``main`` and the
quote-route grammars out of the legacy import closure.

Decision (paridad completa): the legacy path reads time and money exactly as ``main`` (4ec99bc1).

* ``app/core/temporal_semantics.py`` and ``app/core/money_polarity.py`` are the full-text scanners legacy
  consumes; they stay BYTE-IDENTICAL to ``main``.
* The rewritten grammars live in modules only the quote route / Foundation import: TIME
  (``time_grammar`` and its exclusive dependencies), VALUE (``money_comparators``, ``money_suffix_*``,
  ``value_authority``), D-GEO (``geo_*``) and C1.1 (``foundation_authority_*``,
  ``foundation_evidence_window``, ``host_authorities``).
* The no-import pin is TRANSITIVE: it computes the import closure (AST: static, relative, lazy imports inside
  functions, ``from pkg import submodule``, exact dotted string literals and ``importlib``/``__import__`` calls,
  with wildcard edges for f-strings) from the explicit legacy ENTRYPOINTS and fails if the closure holds any
  quote-route module.

MODE FRONTIER. ``orchestrator`` imports ``foundation_runtime`` lazily at every turn, and ``foundation_runtime``
reaches ``foundation_admission`` -> ``foundation_value_direction`` -> ``money_comparators`` (D-MONEY, Foundation
only). In ``legacy`` mode ``run_foundation_search`` returns ``None`` before any of that runs, so the closure is
computed CUTTING exactly that one declared edge. The frontier is itself pinned: it must stay a function-level
import, no other edge may reach the Foundation modules, and
``tests/test_m1_wave1_legacy_mode_runs_no_foundation.py`` proves by execution that in ``legacy`` mode they are
IMPORTED but never RUN.
"""
from __future__ import annotations

import ast
import functools
import hashlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# sha256 at main 4ec99bc1.
TEMPORAL_SEMANTICS_MAIN_SHA256 = "cc3d5e8ae5a45ed1a31b164ff64052d0803cb96130bc2f169b9d08baa48b5227"
MONEY_POLARITY_MAIN_SHA256 = "1e4173cacdd949b746aa0a186b739df244819f3d9cdfdfa3712276987d15e948"

# Productive legacy entrypoints (explicit): Streamlit, FastAPI/webhooks, the workflow service, the
# orchestrator and the router, and the radar scheduler the systemd unit runs.
ENTRYPOINTS = (
    "streamlit_app",
    "app.main",
    "app.service",
    "app.core.orchestrator",
    "app.core.query_router",
    "scripts.run_radar_scheduler",
)

# frontera de modo: foundation_v3; en legacy run_foundation_search devuelve None
# (foundation_runtime.py: ``if ...secoppal_foundation_mode != "foundation_v3": return None``).
MODE_FRONTIER = ("app.core.orchestrator", "app.core.foundation_runtime")
FOUNDATION_MODE_MODULES = frozenset({
    "app.core.foundation_runtime", "app.core.foundation_admission", "app.core.foundation_value_direction",
})

TIME_QUOTE_ROUTE = (
    "time_grammar", "time_authority", "time_relative", "time_window", "temporal_window_anchor",
    "temporal_noun_anchor", "non_canonical_year", "normative_citation", "temporal_bounds",
)
VALUE_QUOTE_ROUTE = ("money_comparators", "money_suffix_event", "money_suffix_place", "value_authority")
GEO_QUOTE_ROUTE = ("geo_authority", "geo_window", "geo_regions")
FORBIDDEN_EXACT = frozenset(
    {f"app.core.{name}" for name in (*TIME_QUOTE_ROUTE, *VALUE_QUOTE_ROUTE, *GEO_QUOTE_ROUTE)}
    | {"app.data.geo_secop_variants", "app.core.host_authorities"}
)
FORBIDDEN_PREFIXES = (
    "app.core.foundation_authority_", "app.core.foundation_evidence_window", "app.core.host_authorities.",
)


def _sha(relative: str) -> str:
    return hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()


def test_temporal_semantics_is_byte_identical_to_main():
    assert _sha("app/core/temporal_semantics.py") == TEMPORAL_SEMANTICS_MAIN_SHA256, (
        "temporal_semantics.py is the legacy full-text scanner: parity with main is frozen. "
        "The rewritten grammar goes in app/core/time_grammar.py."
    )


def test_money_polarity_is_byte_identical_to_main():
    assert _sha("app/core/money_polarity.py") == MONEY_POLARITY_MAIN_SHA256, (
        "money_polarity.py is the legacy money scanner: parity with main is required. "
        "The D-MONEY grammar goes in app/core/money_comparators.py (quote route only)."
    )


# ── the import closure ─────────────────────────────────────────────────────

def module_index(root: Path = ROOT) -> dict[str, Path]:
    index: dict[str, Path] = {}
    for base in ("app", "scripts"):
        for path in sorted((root / base).rglob("*.py")):
            parts = list(path.relative_to(root).with_suffix("").parts)
            if parts[-1] == "__init__":
                parts.pop()
            index[".".join(parts)] = path
    if (root / "streamlit_app.py").exists():
        index["streamlit_app"] = root / "streamlit_app.py"
    return index


def _with_parents(name: str, index: dict[str, Path]) -> set[str]:
    parts = name.split(".")
    return {".".join(parts[:i]) for i in range(1, len(parts) + 1) if ".".join(parts[:i]) in index}


def _constant_prefix(node: ast.AST) -> str | None:
    """Leading constant text of an f-string / concatenation; the whole string for a literal."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        prefix = ""
        for part in node.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                prefix += part.value
            else:
                break
        return prefix
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Mod)):
        return _constant_prefix(node.left)
    return None


def _dynamic_import_call(node: ast.Call) -> bool:
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else func.id if isinstance(func, ast.Name) else ""
    return name in {"import_module", "__import__"}


def module_edges(source: str, module: str, index: dict[str, Path], *, is_package: bool = False,
                 scope: str = "any") -> tuple[set[str], set[str]]:
    """(modules this source imports, dynamic imports it cannot resolve).

    ``scope`` is ``"any"`` (lazy imports inside functions count: they run) or ``"module"`` (only imports that
    run when the module is imported: not inside a function or class-less lambda body).
    """
    package = module if is_package else (module.rsplit(".", 1)[0] if "." in module else "")
    found: set[str] = set()
    unresolved: set[str] = set()

    def add(name: str) -> None:
        found.update(_with_parents(name, index))

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.depth = 0

        def _visible(self) -> bool:
            return scope == "any" or self.depth == 0

        def visit_FunctionDef(self, node):  # noqa: N802
            self.depth += 1
            self.generic_visit(node)
            self.depth -= 1

        visit_AsyncFunctionDef = visit_FunctionDef  # noqa: N815

        def visit_Import(self, node):  # noqa: N802
            if self._visible():
                for alias in node.names:
                    add(alias.name)

        def visit_ImportFrom(self, node):  # noqa: N802
            if not self._visible():
                return
            base = node.module or ""
            if node.level:
                anchor = package.split(".") if package else []
                anchor = anchor[: len(anchor) - (node.level - 1)]
                base = ".".join([*anchor, *([base] if base else [])])
            add(base)
            for alias in node.names:
                add(f"{base}.{alias.name}")

        def visit_Constant(self, node):  # noqa: N802
            if self._visible() and isinstance(node.value, str) and "." in node.value and node.value in index:
                add(node.value)  # an exact dotted module name used as data (importlib, registries)

        def visit_Call(self, node):  # noqa: N802
            if self._visible() and _dynamic_import_call(node) and node.args:
                arg = node.args[0]
                prefix = _constant_prefix(arg)
                literal = isinstance(arg, ast.Constant) and isinstance(arg.value, str)
                if literal:
                    add(prefix)
                elif prefix and "." in prefix:
                    # f"app.core.{name}": any module under the constant prefix may be loaded.
                    found.update(name for name in index if name.startswith(prefix))
                else:
                    unresolved.add(module)
            self.generic_visit(node)

    Visitor().visit(ast.parse(source))
    found.discard(module)
    return found, unresolved


@functools.lru_cache(maxsize=1)
def _disk_graph():
    index = module_index()
    graph: dict[str, set[str]] = {}
    unresolved: dict[str, bool] = {}
    for name, path in index.items():
        edges, dyn = module_edges(path.read_text(encoding="utf-8"), name, index, is_package=path.name == "__init__.py")
        graph[name] = edges
        unresolved[name] = bool(dyn)
    return index, graph, unresolved


def build_graph(overrides: dict[str, str] | None = None):
    """(index, edges, modules with an unresolvable dynamic import); ``overrides`` replace module sources in memory."""
    index, base, dynamic = _disk_graph()
    graph, flagged = dict(base), {name for name, flag in dynamic.items() if flag}
    for name, source in (overrides or {}).items():
        edges, dyn = module_edges(source, name, index, is_package=index[name].name == "__init__.py")
        graph[name] = edges
        flagged.discard(name)
        flagged |= dyn
    return index, graph, flagged


def closure(graph, entrypoints=ENTRYPOINTS, cut=(MODE_FRONTIER,)):
    cuts = set(cut)
    start = [name for name in entrypoints if name in graph]
    parent: dict[str, str | None] = {name: None for name in start}
    stack = list(start)
    while stack:
        current = stack.pop()
        for target in sorted(graph.get(current, ())):
            if (current, target) in cuts or target in parent:
                continue
            parent[target] = current
            stack.append(target)
    return parent


def is_forbidden(name: str) -> bool:
    return name in FORBIDDEN_EXACT or name.startswith(FORBIDDEN_PREFIXES)


def chain(parent, name: str) -> str:
    out = []
    while name is not None:
        out.append(name)
        name = parent[name]
    return " <- ".join(out)


def frontier_problems(source: str, module: str, index: dict[str, Path]) -> list[str]:
    """The declared edge may only be a function-level import (it runs per turn, never at module import)."""
    problems = []
    top, _ = module_edges(source, module, index, scope="module")
    if MODE_FRONTIER[1] in top:
        problems.append(f"{module} imports {MODE_FRONTIER[1]} at module level")
    anywhere, _ = module_edges(source, module, index)
    if MODE_FRONTIER[1] not in anywhere:
        problems.append(f"{module} no longer imports {MODE_FRONTIER[1]}: the declared frontier is stale")
    return problems


def violations(overrides: dict[str, str] | None = None) -> list[str]:
    index, graph, unresolved = build_graph(overrides)
    parent = closure(graph)
    out = [f"{chain(parent, name)}" for name in sorted(parent) if is_forbidden(name)]
    out += [f"{chain(parent, name)}  [Foundation-mode module outside the frontier]"
            for name in sorted(parent) if name in FOUNDATION_MODE_MODULES]
    out += [f"unresolvable dynamic import in {name}" for name in sorted(unresolved & set(parent))]
    orchestrator = MODE_FRONTIER[0]
    source = (overrides or {}).get(orchestrator) or index[orchestrator].read_text(encoding="utf-8")
    out += frontier_problems(source, orchestrator, index)
    return out


def test_the_legacy_closure_holds_no_quote_route_module():
    found = violations()
    assert not found, "the legacy path must not reach the quote route (paridad con main):\n" + "\n".join(found)


def test_no_edge_reaches_the_foundation_modules_except_the_declared_frontier():
    index, graph, _ = build_graph()
    parent = closure(graph)
    assert not FOUNDATION_MODE_MODULES & set(parent), {n: chain(parent, n) for n in FOUNDATION_MODE_MODULES & set(parent)}
    edges = {(name, target) for name, targets in graph.items() if name in parent
             for target in FOUNDATION_MODE_MODULES & targets}
    assert edges == {MODE_FRONTIER}, f"legacy modules import the Foundation-mode modules: {sorted(edges)}"


def test_the_only_legacy_reachable_divergence_from_main_is_inert():
    """``entity_resolver`` (M1-H0) is the one module of the legacy closure that differs from main: it only ADDS
    ``EntityResolver.alias_owners`` (+9 lines), which only the Foundation-mode modules call. No legacy module may."""
    index, graph, _ = build_graph()
    callers = []
    for name in closure(graph):
        tree = ast.parse(index[name].read_text(encoding="utf-8"))
        callers += [name for node in ast.walk(tree) if isinstance(node, ast.Attribute) and node.attr == "alias_owners"]
    assert callers == [], f"legacy modules call EntityResolver.alias_owners (main does not have it): {callers}"


def test_the_mode_frontier_is_a_lazy_function_level_import():
    index = module_index()
    source = index[MODE_FRONTIER[0]].read_text(encoding="utf-8")
    assert frontier_problems(source, MODE_FRONTIER[0], index) == []


# ── non-vacuity: the pin must be able to fail ──────────────────────────────

def test_the_closure_is_not_empty_and_sees_the_legacy_chain():
    _, graph, _ = build_graph()
    parent = closure(graph)
    assert len(parent) > 100
    for name in ("app.core.orchestrator", "app.core.query_router", "app.core.money_polarity",
                 "app.core.temporal_semantics", "app.core.workflow.graph"):
        assert name in parent, name


def test_without_the_frontier_the_scanner_reaches_the_foundation_chain_and_the_quote_route():
    """Proof that the cut matters and that lazy function-level imports are followed."""
    _, graph, _ = build_graph()
    parent = closure(graph, cut=())
    reached = {name for name in parent if is_forbidden(name)}
    assert {"app.core.money_comparators", "app.core.money_suffix_place", "app.core.geo_authority"} <= reached
    assert FOUNDATION_MODE_MODULES <= set(parent)
    assert chain(parent, "app.core.money_comparators").endswith("app.core.orchestrator")


def _extend(name: str, extra: str) -> dict[str, str]:
    index = module_index()
    return {name: index[name].read_text(encoding="utf-8") + "\n" + extra + "\n"}


def test_mutation_a_transitive_import_through_an_allowed_module_is_caught():
    _, graph, _ = build_graph()
    allowed = "app.core.money_resolution"  # a legacy module (not in the quote route) money_polarity imports
    assert allowed in closure(graph) and not is_forbidden(allowed)
    for target in ("from app.core import time_grammar", "import app.core.value_authority",
                   "from app.core.money_comparators import read_comparators", "from . import geo_authority"):
        found = violations(_extend(allowed, target))
        assert found, target
        assert any(allowed in line for line in found), (target, found)


def test_mutation_b_an_importlib_fstring_in_the_orchestrator_is_caught():
    for extra in (
        "import importlib\nimportlib.import_module(f'app.core.{NAME}')",
        "def _late(name):\n    import importlib\n    return importlib.import_module(f'app.core.{name}')",
        "__import__('app.core.time_authority')",
        "def _dyn(name):\n    import importlib\n    return importlib.import_module(name)",
    ):
        assert violations(_extend("app.core.orchestrator", extra)), extra


def test_mutation_d_the_frontier_edge_as_a_module_level_import_is_caught():
    for extra in ("from app.core.foundation_runtime import run_foundation_search",
                  "import app.core.foundation_runtime",
                  "from app.core import foundation_runtime"):
        found = violations(_extend("app.core.orchestrator", extra))
        assert any("module level" in line for line in found), (extra, found)


def test_mutation_e_any_other_edge_to_the_foundation_modules_is_caught():
    _, graph, _ = build_graph()
    legacy = "app.core.workflow.deps"
    assert legacy in closure(graph)
    for extra in ("from app.core import foundation_admission", "from app.core.foundation_value_direction import operator_reason",
                  "def f():\n    from app.core.foundation_runtime import run_foundation_search"):
        assert violations(_extend(legacy, extra)), extra


def test_a_removed_frontier_import_is_reported_as_stale():
    index = module_index()
    source = index[MODE_FRONTIER[0]].read_text(encoding="utf-8")
    assert any("stale" in p for p in frontier_problems(source.replace("foundation_runtime", "foundation_runtimx"),
                                                       MODE_FRONTIER[0], index))


@pytest.mark.parametrize("source", [
    "from app.core import time_grammar",
    "from app.core.time_grammar import resolve_temporal_scope",
    "import app.core.time_authority",
    "from app.core.time_relative import resolve_relative",
    "def f():\n    from app.core import temporal_noun_anchor",
    "import importlib\nimportlib.import_module('app.core.temporal_bounds')",
    "from . import time_window",
    "X = 'app.core.money_comparators'",
    "import importlib\nimportlib.import_module(f'app.core.{x}')",
])
def test_the_edge_scanner_is_not_vacuous(source):
    index = module_index()
    edges, dynamic = module_edges(source, "app.core.query_router", index)
    assert any(is_forbidden(name) for name in edges), (source, edges, dynamic)


@pytest.mark.parametrize("source", [
    "from app.core.temporal_semantics import resolve_temporal_scope",
    "from app.core.accreditation.authorities import time_authority",
    "from app.core import time_authority_thing",
    "'a docstring that mentions app.core.time_grammar in passing'",
])
def test_the_edge_scanner_ignores_other_modules(source):
    index = module_index()
    edges, dynamic = module_edges(source, "app.core.query_router", index)
    assert not any(is_forbidden(name) for name in edges), (source, edges)
    assert not dynamic


def test_the_quote_route_modules_exist_and_the_legacy_only_ones_do_not():
    index = module_index()
    for name in TIME_QUOTE_ROUTE:
        assert f"app.core.{name}" in index, name
    for gone in ("period_consistency", "period_free_text", "pagination_blocked", "temporal_period_question"):
        assert f"app.core.{gone}" not in index, gone
    assert "app.core.analytics.contract_activity.currentness" not in index
