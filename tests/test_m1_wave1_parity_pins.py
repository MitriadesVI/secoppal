"""M1 wave 1 PARIDAD TEMPORAL: pins that keep the legacy path equal to ``main`` and the
D-TIME grammar on the quote-only route.

Decision (opción A): the legacy path has full temporal parity with ``main`` (4ec99bc1).
``app/core/temporal_semantics.py`` is the full-text scanner legacy consumes; it stays
BYTE-IDENTICAL to ``main``. The rewritten grammar (windows, anchors, mentions, implicit
year, bounds) lives in ``app/core/time_grammar.py`` and is imported ONLY by the quote
route (``time_authority`` and its exclusive dependencies), ``host_authorities`` and the
C1.1 ``foundation_authority_*`` modules.
"""
from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app"

# sha256 of app/core/temporal_semantics.py at main 4ec99bc1.
TEMPORAL_SEMANTICS_MAIN_SHA256 = "cc3d5e8ae5a45ed1a31b164ff64052d0803cb96130bc2f169b9d08baa48b5227"

# congelado M1 wave 1: ninguna gramática de dinero nueva entra por legacy.
VALUE_FROZEN_SHA256 = {
    "app/core/money_comparators.py": "5e5647edb52af1038e7f465257a20f3703218d8feda4cc59be1dbf71796fa015",
    "app/core/money_polarity.py": "ccf4cb25fbc1a94764a78343e30a097ee20b5d6a98a44baade07f9aab419efa8",
    "app/core/money_suffix_event.py": "90ec102c88e09bedfd8bdd1af1c64b71075537c8ffedbfb4f4e23ee05aa2ae79",
    "app/core/money_suffix_place.py": "bdbdfcd4552b0fdc6851151247edfce9638cc00657f14ade6c64e7bc3ca29006",
    "app/core/value_authority.py": "32691a5057ba5ae87545b5fc3dc9388f88db08cb0f6c17d022ef2b03c2e754dd",
    "app/core/foundation_value_direction.py": "ba38fb9d7f94b11122e8b7561bc6219c1ba359ed6dafbd90d18de6dcfefe1bac",
}

# Modules that belong to the quote route only.
QUOTE_ROUTE = frozenset({
    "time_grammar", "time_authority", "time_relative", "time_window",
    "temporal_window_anchor", "temporal_noun_anchor", "non_canonical_year",
    "normative_citation", "temporal_bounds",
})
QUOTE_DOTTED = {f"app.core.{name}" for name in QUOTE_ROUTE}


def _sha(relative: str) -> str:
    return hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()


def test_temporal_semantics_is_byte_identical_to_main():
    assert _sha("app/core/temporal_semantics.py") == TEMPORAL_SEMANTICS_MAIN_SHA256, (
        "temporal_semantics.py is the legacy full-text scanner: parity with main is frozen "
        "(opción A). The rewritten grammar goes in app/core/time_grammar.py."
    )


@pytest.mark.parametrize("relative", sorted(VALUE_FROZEN_SHA256))
def test_value_modules_are_frozen(relative):
    # congelado M1 wave 1: ninguna gramática de dinero nueva entra por legacy
    assert _sha(relative) == VALUE_FROZEN_SHA256[relative], relative


def _module_name(path: Path) -> str:
    parts = list(path.relative_to(ROOT).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _is_allowed(path: Path) -> bool:
    relative = path.relative_to(APP / "core") if (APP / "core") in path.parents else None
    if relative is None:
        return False
    first = relative.parts[0]
    stem = relative.stem
    if len(relative.parts) == 1 and stem in QUOTE_ROUTE:
        return True
    if first == "host_authorities":
        return True
    return len(relative.parts) == 1 and (
        stem.startswith("foundation_authority_") or stem == "foundation_evidence_window"
    )


def quote_route_imports(source: str, module: str) -> set[str]:
    """Quote-route modules this source imports (static imports and dotted string literals)."""
    package = module.rsplit(".", 1)[0] if "." in module else ""
    found: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                anchor = package.split(".")
                anchor = anchor[: len(anchor) - (node.level - 1)]
                base = ".".join([*anchor, *([base] if base else [])])
            names = [base, *(f"{base}.{alias.name}" for alias in node.names)]
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            names = [node.value]  # importlib.import_module("app.core.time_grammar")
        else:
            continue
        found |= {name for name in names if name in QUOTE_DOTTED}
    return found


def test_no_legacy_module_imports_the_quote_route():
    offenders: dict[str, set[str]] = {}
    for path in sorted(APP.rglob("*.py")):
        if _is_allowed(path):
            continue
        hit = quote_route_imports(path.read_text(encoding="utf-8"), _module_name(path))
        if hit:
            offenders[str(path.relative_to(ROOT))] = hit
    assert not offenders, (
        "legacy modules must not import the quote-route grammar (paridad temporal con main): "
        f"{offenders}"
    )


@pytest.mark.parametrize("source", [
    "from app.core import time_grammar",
    "from app.core.time_grammar import resolve_temporal_scope",
    "import app.core.time_authority",
    "from app.core.time_relative import resolve_relative",
    "def f():\n    from app.core import temporal_noun_anchor",
    "import importlib\nimportlib.import_module('app.core.temporal_bounds')",
    "from . import time_window",
])
def test_the_import_scanner_is_not_vacuous(source):
    assert quote_route_imports(source, "app.core.query_router"), source


@pytest.mark.parametrize("source", [
    "from app.core.temporal_semantics import resolve_temporal_scope",
    "from app.core.accreditation.authorities import time_authority",
    "from app.core import time_authority_thing",
])
def test_the_import_scanner_ignores_other_modules(source):
    assert not quote_route_imports(source, "app.core.query_router"), source


def test_the_quote_route_modules_exist_and_the_legacy_only_ones_do_not():
    for name in QUOTE_ROUTE:
        assert (APP / "core" / f"{name}.py").exists(), name
    for gone in ("period_consistency", "period_free_text", "pagination_blocked", "temporal_period_question"):
        assert not (APP / "core" / f"{gone}.py").exists(), gone
    assert not (APP / "core/analytics/contract_activity/currentness.py").exists()
