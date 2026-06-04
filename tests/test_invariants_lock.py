"""Lock-in tests for QP-INV invariant consolidation.

These tests protect the architectural decision that legal/semantic invariant
catalogs live in app/core/contract_invariants.py, not in ad-hoc local lists.
"""

from __future__ import annotations

import ast
from pathlib import Path


CORE_DIR = Path("app/core")
SOURCE_OF_TRUTH = CORE_DIR / "contract_invariants.py"

FORBIDDEN_SYMBOL_NAMES = {
    "_INVALID_CONTRACT_STATES",
    "_CLOSED_STATES",
    "_BIDDER_PHRASES",
    "_SUBSTANTIAL_FILTER_KEYS",
}

INVALID_STATE_LITERALS = {"firmado", "celebrado"}
NON_OPPORTUNITY_LITERALS = {"cerrado", "terminado", "liquidado"}
BIDDER_PHRASE_LITERALS = {
    "quiero presentarme",
    "para presentarme",
    "puedo ofertar",
    "quiero ofertar",
}
ALLOWED_DETECTOR_CATALOG_NAMES = {
    "STOPWORDS",
    "_DATASET_KEYWORDS",
}


def _core_python_files() -> list[Path]:
    return [
        path
        for path in CORE_DIR.rglob("*.py")
        if path != SOURCE_OF_TRUTH
    ]


def _assigned_names(node: ast.AST) -> set[str]:
    names: set[str] = set()
    targets = []
    if isinstance(node, ast.Assign):
        targets = list(node.targets)
    elif isinstance(node, ast.AnnAssign):
        targets = [node.target]

    for target in targets:
        if isinstance(target, ast.Name):
            names.add(target.id)
        elif isinstance(target, ast.Attribute):
            names.add(target.attr)
    return names


def _literal_strings(node: ast.AST) -> set[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value.lower()}
    if isinstance(node, (ast.Set, ast.List, ast.Tuple)):
        values: set[str] = set()
        for item in node.elts:
            values.update(_literal_strings(item))
        return values
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"frozenset", "set", "list", "tuple"}
        and node.args
    ):
        return _literal_strings(node.args[0])
    return set()


def test_no_legacy_duplicate_invariant_symbol_names():
    offenders: list[str] = []
    for path in _core_python_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            names = _assigned_names(node)
            forbidden = names & FORBIDDEN_SYMBOL_NAMES
            if forbidden:
                offenders.append(f"{path}: {', '.join(sorted(forbidden))}")

    assert not offenders, (
        "Invariant catalog symbols must live in contract_invariants.py only: "
        + "; ".join(offenders)
    )


def test_no_duplicate_invariant_catalog_literals_outside_source_of_truth():
    offenders: list[str] = []
    for path in _core_python_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue

            names = _assigned_names(node)
            if names & ALLOWED_DETECTOR_CATALOG_NAMES:
                continue

            value = node.value if isinstance(node, (ast.Assign, ast.AnnAssign)) else None
            literals = _literal_strings(value) if value is not None else set()

            if INVALID_STATE_LITERALS <= literals and len(literals) <= 16:
                offenders.append(f"{path}:{node.lineno} duplicates invalid contract states {sorted(names)}")
            if NON_OPPORTUNITY_LITERALS <= literals:
                offenders.append(f"{path}:{node.lineno} duplicates non-opportunity states {sorted(names)}")
            if BIDDER_PHRASE_LITERALS <= literals:
                offenders.append(f"{path}:{node.lineno} duplicates bidder phrases {sorted(names)}")

    assert not offenders, (
        "Invariant literal catalogs must live in contract_invariants.py only: "
        + "; ".join(offenders)
    )
