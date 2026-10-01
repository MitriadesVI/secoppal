"""M1 wave 1 PARIDAD (TIME + VALUE): pins that keep the legacy path equal to ``main`` and the
quote-route grammars out of the legacy import closure.

Decision (paridad completa): the legacy path reads time and money exactly as ``main`` (4ec99bc1).

* ``app/core/temporal_semantics.py`` and ``app/core/money_polarity.py`` are the full-text scanners legacy
  consumes; they stay BYTE-IDENTICAL to ``main``.
* The rewritten grammars live in modules only the quote route / Foundation import. ``WAVE1_MODULES`` is the
  FROZEN literal list of every ``app/`` module wave1 added or changed relative to main (see the comment there).
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

# FROZEN, literal. Generated from the real diff, not from a hand-kept taxonomy:
#   git diff --name-only 4ec99bc1 708ab0d5 -- app      (modified + added modules, 30 files)
# minus ``app.core.entity_resolver`` (M1-H0: the one divergence legacy legitimately reaches; its pin is
# ``test_the_only_legacy_reachable_divergence_from_main_is_inert``). Adding a new ``app/`` module to wave1 (or
# modifying one that legacy would not otherwise read) REQUIRES updating this list: the legacy closure, cutting
# only the MODE_FRONTIER edge, must contain none of them.
WAVE1_MODULES = frozenset({
    "app.core.foundation_admission",
    "app.core.foundation_authority_contract",
    "app.core.foundation_authority_registry",
    "app.core.foundation_authority_values",
    "app.core.foundation_buyer_choices",
    "app.core.foundation_evidence_window",
    "app.core.foundation_geo_level",
    "app.core.foundation_messages",
    "app.core.foundation_runtime",
    "app.core.foundation_time_coverage",
    "app.core.foundation_value_direction",
    "app.core.geo_authority",
    "app.core.geo_regions",
    "app.core.geo_window",
    "app.core.host_authorities",
    "app.core.money_comparators",
    "app.core.money_suffix_event",
    "app.core.money_suffix_place",
    "app.core.non_canonical_year",
    "app.core.normative_citation",
    "app.core.temporal_bounds",
    "app.core.temporal_noun_anchor",
    "app.core.temporal_window_anchor",
    "app.core.time_authority",
    "app.core.time_grammar",
    "app.core.time_relative",
    "app.core.time_window",
    "app.core.value_authority",
    "app.data.geo_secop_variants",
})
# Submodules a future ``host_authorities`` package may grow.
FORBIDDEN_PREFIXES = ("app.core.host_authorities.",)
# The Foundation chain the frontier edge leads to (used only by the non-vacuity pins).
FOUNDATION_CHAIN = frozenset({
    "app.core.foundation_runtime", "app.core.foundation_admission", "app.core.foundation_value_direction",
})


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


def _package_argument(node: ast.Call) -> str | None:
    """The literal ``package`` of ``importlib.import_module(name, package)`` (positional or keyword)."""
    arg = node.args[1] if len(node.args) > 1 else next((k.value for k in node.keywords if k.arg == "package"), None)
    return arg.value if isinstance(arg, ast.Constant) and isinstance(arg.value, str) else None


def _resolve_relative(name: str, package: str) -> str:
    level = len(name) - len(name.lstrip("."))
    anchor = package.split(".")
    anchor = anchor[: len(anchor) - (level - 1)]
    return ".".join(part for part in (*anchor, name.lstrip(".")) if part)


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
                if prefix is not None and prefix.startswith("."):
                    # relative name (``import_module('.time_grammar', 'app.core')``): resolve it against the package.
                    package_name = _package_argument(node)
                    if package_name is None:
                        unresolved.add(module)
                        self.generic_visit(node)
                        return
                    prefix = _resolve_relative(prefix, package_name)
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
    return name in WAVE1_MODULES or name.startswith(FORBIDDEN_PREFIXES)


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
    assert not WAVE1_MODULES & set(parent), {n: chain(parent, n) for n in WAVE1_MODULES & set(parent)}
    edges = {(name, target) for name, targets in graph.items() if name in parent
             for target in WAVE1_MODULES & targets}
    assert edges == {MODE_FRONTIER}, f"legacy modules import the Foundation-mode modules: {sorted(edges)}"


ALIAS_OWNERS_DEFINER = "app.core.entity_resolver"


def alias_owners_uses(source: str) -> list[str]:
    """Every way a module can reach ``alias_owners``: attribute, ``getattr``/dict key/any literal string, name, import."""
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Attribute) and node.attr == "alias_owners":
            found.append(f"attribute @{node.lineno}")
        elif isinstance(node, ast.Constant) and node.value == "alias_owners":
            found.append(f"string literal @{node.lineno}")
        elif isinstance(node, ast.Name) and node.id == "alias_owners":
            found.append(f"name @{node.lineno}")
        elif isinstance(node, ast.alias) and node.name.rsplit(".", 1)[-1] == "alias_owners":
            found.append("import")
    return found


def test_the_only_legacy_reachable_divergence_from_main_is_inert():
    """``entity_resolver`` (M1-H0) is the one module of the legacy closure that differs from main: it only ADDS
    ``EntityResolver.alias_owners`` (+9 lines), which only the Foundation-mode modules call. No legacy module may
    reach it by attribute, ``getattr`` or a literal ``'alias_owners'`` (``entity_resolver`` itself defines it)."""
    index, graph, _ = build_graph()
    callers = []
    for name in closure(graph):
        if name == ALIAS_OWNERS_DEFINER:
            continue
        callers += [f"{name}: {use}" for use in alias_owners_uses(index[name].read_text(encoding="utf-8"))]
    assert callers == [], f"legacy modules reach EntityResolver.alias_owners (main does not have it): {callers}"


@pytest.mark.parametrize("source", [
    "def f(r):\n    return r.alias_owners('x')",
    "def f(r):\n    return getattr(r, 'alias_owners')('x')",
    "NAMES = {'alias_owners': 1}",
    "from app.core.entity_resolver import alias_owners",
    "def f(r, name='alias_owners'):\n    return getattr(r, name)",
])
def test_the_alias_owners_pin_is_not_vacuous(source):
    assert alias_owners_uses(source), source


def test_the_alias_owners_pin_ignores_unrelated_code():
    assert alias_owners_uses("def f(r):\n    # alias_owners in a comment\n    return r.alias_owner") == []


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
    assert FOUNDATION_CHAIN <= set(parent)
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
    "import importlib\nimportlib.import_module('.time_grammar', 'app.core')",
    "import importlib\nimportlib.import_module('.time_grammar', package='app.core')",
    "import importlib\nimportlib.import_module('..core.time_grammar', 'app.core')",
    "import importlib\nimportlib.import_module(f'.{x}', 'app.core')",
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
    "import importlib\nimportlib.import_module('.accreditation', 'app.core')",
])
def test_the_edge_scanner_ignores_other_modules(source):
    index = module_index()
    edges, dynamic = module_edges(source, "app.core.query_router", index)
    assert not any(is_forbidden(name) for name in edges), (source, edges)
    assert not dynamic


def test_the_legacy_only_modules_are_gone():
    index = module_index()
    for gone in ("period_consistency", "period_free_text", "pagination_blocked", "temporal_period_question"):
        assert f"app.core.{gone}" not in index, gone
    assert "app.core.analytics.contract_activity.currentness" not in index


def test_a_relative_dynamic_import_without_a_literal_package_is_unresolved():
    index = module_index()
    for source in ("import importlib\nimportlib.import_module('.time_grammar')",
                   "import importlib\nimportlib.import_module('.time_grammar', __package__)"):
        _, dynamic = module_edges(source, "app.core.query_router", index)
        assert dynamic, source


def test_the_frozen_list_names_real_modules_and_excludes_the_alias_owners_definer():
    """A typo or a rename would silently weaken the pin."""
    index = module_index()
    assert sorted(WAVE1_MODULES - set(index)) == []
    assert len(WAVE1_MODULES) == 29
    assert ALIAS_OWNERS_DEFINER not in WAVE1_MODULES


@pytest.mark.parametrize("extra", [
    "from app.core.foundation_time_coverage import uncited_time_qualifier",
    "from app.core.foundation_messages import user_message",
    "from app.core.foundation_buyer_choices import buyer_choices",
    "def f():\n    import importlib\n    return importlib.import_module('.time_grammar', 'app.core')",
])
def test_mutation_f_any_wave1_module_reached_from_the_legacy_closure_is_caught(extra):
    assert violations(_extend("app.core.query_router", extra)), extra
