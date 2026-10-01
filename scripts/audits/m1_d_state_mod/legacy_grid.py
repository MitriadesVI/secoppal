"""Snapshot of ``resolve_estado`` over every token the legacy lexicons declare (M1-D-STATE/MOD).

Run on the candidate (``legacy_grid.py out.json``) and on a copy of the base module
(``git show <base>:app/core/estado_families.py > base.py; legacy_grid.py out.json base.py``) and diff
the two JSON files: that is the declared legacy impact of the estado_families fixes.  Pure, offline.
"""
from __future__ import annotations

import importlib.util
import json
import re
import sys

from app.core import estado_families as ef
from app.data import estados


def _accented(token: str) -> set[str]:
    """Spellings a user may type: the declared token and the accented renderings of it."""
    variants = {token}
    for plain, accented in (("ion", "ión"), ("cion", "ción"), ("ucion", "ución")):
        if re.search(plain + r"\b", token):
            variants.add(re.sub(plain + r"\b", accented, token))
    return variants


def tokens() -> list[str]:
    declared = {*ef.NATURAL_TO_FAMILY, *ef.LEXICAL_EXACT_STATE, *ef.DATASET_AWARE_FAMILY,
                *estados.ESTADO_APERTURA_SYNONYMS, *estados.ESTADO_PROCEDIMIENTO_SYNONYMS,
                *estados.ESTADO_CONTRATO_SYNONYMS}
    out: set[str] = set()
    for token in declared:
        out |= _accented(token)
    # the accented forms a real user types for the multi-word states
    out |= {"en ejecución", "en evaluación", "ejecución", "evaluación", "en aprobación", "adjudicadas",
            "adjudicada", "terminadas", "liquidados", "en curso", "finalizados", "todavía reciben ofertas"}
    return sorted(out)


def _resolver(module_file: str | None):
    """The module under measure: the current one, or a copy of the base loaded from ``module_file``."""
    if module_file is None:
        return ef
    spec = importlib.util.spec_from_file_location("estado_families_base", module_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main(path: str, module_file: str | None = None) -> None:
    resolver = _resolver(module_file)
    grid = {}
    for token in tokens():
        for dataset in ("procesos", "contratos"):
            resolved = resolver.resolve_estado(token, dataset)
            grid[f"{dataset}|{token}"] = None if resolved is None else {
                "family": resolved["family"], "kind": resolved["kind"],
                "filters": {k: list(v) for k, v in sorted(resolved["filters"].items())},
            }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(grid, handle, ensure_ascii=False, indent=1, sort_keys=True)
    print(len(grid), "cells", sum(v is not None for v in grid.values()), "resolved")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
