"""Validación del registro canónico de invariantes (TEST_ARCHITECTURE_V1 F2).

Todo invariante nombra su dueño y la defensa que lo sostiene. Las referencias a
tests son ``path::función`` o ``path::Clase::método`` y se verifican contra el
AST del archivo; una referencia rota es un error, no una advertencia.
"""
from __future__ import annotations

import ast
import re
from collections.abc import Mapping
from functools import cache
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = Path(__file__).resolve().parent / "invariants.yaml"

DOMAINS = ("ACTOR", "BUYER", "ENTITY", "GEO", "MONEY", "STATE", "TIME", "NEG", "CTRL", "SEM",
           "SOURCE", "CONV", "PAG", "LOOKUP", "DUAL", "PERSIST", "HARNESS")
CANONICAL_ID = re.compile(r"^INV-(%s)-[A-Z0-9]+(-[A-Z0-9]+)*$" % "|".join(DOMAINS))
CRITICALITY = {"P0", "P1", "P2"}
STATUS = {"DEFENDED", "PARTIAL", "UNDEFENDED", "SUPERSEDED"}
REQUIRED = {"title", "description", "criticality", "owner", "authority", "status",
            "primary_tests", "supporting_tests", "product_cases", "legacy_ids"}
OPTIONAL = {"product_case_justification", "gap_justification", "open_product_gaps",
            "re_home_before_f6", "notes"}
HISTORICAL_FILE = re.compile(r"(independent|remediation|audit|dogfood|adjudicat|final_)")


def load(path: Path = REGISTRY) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@cache
def _defined(path: str) -> frozenset[str]:
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.ClassDef):
            names |= {f"{node.name}::{n.name}" for n in node.body
                      if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    return frozenset(names)


def reference_error(ref: str) -> str | None:
    path, _, symbol = ref.partition("::")
    if not symbol:
        return f"{ref}: must be path::function"
    if not (ROOT / path).is_file():
        return f"{ref}: file does not exist"
    if symbol not in _defined(path):
        return f"{ref}: {symbol} is not defined in {path}"
    return None


def validate(document: Mapping, *, scenario_ids: set[str], gap_ids: set[str]) -> list[str]:
    errors: list[str] = []
    invariants = document.get("invariants") or {}
    legacy_owner: dict[tuple[str, str], str] = {}
    for inv_id, inv in invariants.items():
        where = inv_id
        if not CANONICAL_ID.match(inv_id):
            errors.append(f"{where}: id outside the canonical namespace INV-<DOMAIN>-<SLUG>")
        errors += [f"{where}: missing {k}" for k in sorted(REQUIRED - set(inv))]
        errors += [f"{where}: unknown key {k}" for k in sorted(set(inv) - REQUIRED - OPTIONAL)]
        if inv.get("criticality") not in CRITICALITY:
            errors.append(f"{where}: invalid criticality")
        if inv.get("status") not in STATUS:
            errors.append(f"{where}: invalid status")
        critical = inv.get("criticality") in {"P0", "P1"}
        primaries = inv.get("primary_tests") or []
        if critical and not primaries and not inv.get("gap_justification"):
            errors.append(f"{where}: P0/P1 needs a primary test or a gap_justification")
        if inv.get("status") in {"PARTIAL", "UNDEFENDED", "SUPERSEDED"} and not inv.get("gap_justification"):
            errors.append(f"{where}: status {inv.get('status')} needs a gap_justification")
        if critical and not inv.get("product_cases") and not inv.get("product_case_justification"):
            errors.append(f"{where}: P0/P1 needs a product case or a product_case_justification")
        for ref in [*primaries, *(inv.get("supporting_tests") or [])]:
            problem = reference_error(ref)
            if problem:
                errors.append(f"{where}: {problem}")
        historical = [r for r in primaries if HISTORICAL_FILE.search(Path(r.split("::")[0]).stem)]
        if historical and primaries and len(historical) == len(primaries) and not inv.get("re_home_before_f6"):
            errors.append(f"{where}: every primary lives in a historical-named suite; set re_home_before_f6")
        errors += [f"{where}: unknown product case {c}" for c in inv.get("product_cases") or []
                   if c not in scenario_ids]
        errors += [f"{where}: unknown open product gap {g}" for g in inv.get("open_product_gaps") or []
                   if g not in gap_ids]
        for legacy in inv.get("legacy_ids") or []:
            key = (legacy.get("id"), legacy.get("scope"))
            if not all(key):
                errors.append(f"{where}: legacy id needs id and scope")
            elif key in legacy_owner:
                errors.append(f"{where}: legacy {key} already mapped to {legacy_owner[key]}")
            else:
                legacy_owner[key] = inv_id
    return errors
