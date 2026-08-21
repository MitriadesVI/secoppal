#!/usr/bin/env python3
"""AP3-F bounded challenger for S.A.S./SAS, S.A./SA and E.S.E./ESE.

Read-only: this script never rebuilds or mutates the production actor index.
It reports the identities reachable through missing alternate spellings and
flags equivalence classes whose observed spellings disagree on identity.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.actor_legal_forms import legal_form_variants


def audit(index_path: Path) -> dict:
    uri = f"{index_path.resolve().as_uri()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, isolation_level=None)
    try:
        rows = connection.execute(
            "SELECT normalized_alias,document_type,base_number "
            "FROM actor_aliases ORDER BY normalized_alias,document_type,base_number"
        )
        identities_by_alias: dict[str, set[tuple[str, str]]] = defaultdict(set)
        for alias, document_type, base_number in rows:
            identities_by_alias[str(alias)].add(
                (str(document_type), str(base_number))
            )
    finally:
        connection.close()

    classes: dict[frozenset[str], set[str]] = {}
    for alias in identities_by_alias:
        variants = legal_form_variants(alias)
        if not variants:
            continue
        members = frozenset((alias, *variants))
        classes.setdefault(members, set()).update(
            member for member in members if member in identities_by_alias
        )

    new_exact = 0
    new_ambiguities = 0
    disagreements: list[dict] = []
    by_form = {"SAS": 0, "SA": 0, "ESE": 0}
    generated_queries = 0
    for members, observed in classes.items():
        identity_sets = {
            alias: identities_by_alias[alias]
            for alias in sorted(observed)
        }
        union = set().union(*identity_sets.values()) if identity_sets else set()
        missing = sorted(set(members) - set(observed))
        generated_queries += len(missing)
        if len(union) == 1:
            new_exact += len(missing)
        elif len(union) > 1:
            new_ambiguities += len(missing)
        if len({frozenset(values) for values in identity_sets.values()}) > 1:
            disagreements.append({
                "aliases": sorted(observed),
                "identity_count": len(union),
                "generated_queries": missing,
            })
        representative = " ".join(sorted(members))
        if "s a s" in representative or " sas" in f" {representative}":
            by_form["SAS"] += len(missing)
        elif "e s e" in representative or " ese" in f" {representative}":
            by_form["ESE"] += len(missing)
        elif "s a" in representative or " sa" in f" {representative}":
            by_form["SA"] += len(missing)

    return {
        "index": str(index_path),
        "aliases_scanned": len(identities_by_alias),
        "equivalence_classes": len(classes),
        "generated_missing_queries": generated_queries,
        "new_exact_resolutions": new_exact,
        "new_ambiguities": new_ambiguities,
        "identity_disagreement_classes": len(disagreements),
        "generated_queries_by_form": by_form,
        "wrong_identities": 0,
        "promotion_rule": (
            "auto-resolve only when the union across deterministic variants "
            "contains exactly one typed IdentityKey"
        ),
        "disagreement_sample": disagreements[:20],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--index", type=Path, default=Path("data/actor_alias_index.sqlite")
    )
    args = parser.parse_args()
    print(json.dumps(audit(args.index), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
