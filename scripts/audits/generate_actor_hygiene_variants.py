"""Generate HCA-R7 variants; execution belongs to the existing isolated runner.

Usage: .venv/bin/python3 scripts/audits/generate_actor_hygiene_variants.py --out /tmp/actor-variants.yaml
Requires the same real, immutable actor index as actor_hygiene_v1.yaml.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import yaml


def actor_hygiene_variants() -> list[dict]:
    cases: list[dict] = []
    absent = ["actor_scope", "actor_pending", "actor_resolution_status", "contratista"]
    casings = (str.lower, str.upper, str.title)

    def add(category: str, *turns: dict) -> None:
        cases.append({"id": f"AHV-{len(cases) + 1:03}", "category": category, "turns": list(turns)})

    topics = (
        "aseo", "mantenimiento", "servicios funerarios", "papelería",
        "comedores comunitarios", "obra pública", "vías", "alimentación",
    )
    tails = (
        "en Barranquilla", "en 2025 por más de 100 millones",
        "en Bolívar por licitación pública",
    )
    for topic in topics:
        for tail in tails:
            for casing in casings:
                for lead in ("contratos de ", "busca "):
                    add("SEARCH_TO_ACTOR_HIJACK", {
                        "q": casing(f"{lead}{topic} {tail}"), "absent": absent,
                    })

    pending = {"q": "jorge verbel", "expect": "block"}
    for query in (
        "contratos de aseo en barranquilla", "procesos de vías en bolívar",
        "convenios de alimentación en 2026", "muéstrame contratos por más de 500 millones",
        "muéstrame más contratos de aseo en Barranquilla", "busca mantenimiento en Bogotá",
    ):
        for casing in casings:
            add("PENDING_NEW_SEARCH_HIJACK", pending, {
                "q": casing(query), "absent": absent, "resp_not": ["verbel"],
            })

    for name in ("FUNSOCOM", "LMP INGENIERIA SAS", "UNION TEMPORAL ASEO SCJ"):
        for casing in casings:
            for lead in ("", "contratos de ", "contratos del proveedor "):
                add("ACTOR_TO_SEARCH_FALSE_NEGATIVE", {
                    "q": casing(lead + name), "expect": "execute",
                    "eq": {"actor_resolution_status": "RESOLVED"}, "absent": ["actor_pending"],
                })

    for typo in ("ingieneria", "ingeneria", "ingeniera"):
        for casing in casings:
            for lead in ("", "contratos de ", "busca proveedor "):
                add("TYPO_RECALL_FAILURE", {
                    "q": casing(lead + "lmp " + typo), "expect": "block",
                    "eq": {"actor_resolution_status": "CANDIDATES"}, "absent": ["contratista"],
                    "candidate_present": {"document_type": "NIT", "base_number": "900739049", "max_rank": 1},
                })

    for tail in (
        "en 2026", "por más de 500 millones", "en Barranquilla",
        "por licitación pública", "por mayor valor",
    ):
        for casing in casings:
            add("STRUCTURED_ACTOR", {
                "q": casing("contratos del proveedor FUNSOCOM " + tail), "expect": "execute",
                "actor_identity": {"document_type": "NIT", "base_number": "900334689"},
                "absent": ["actor_pending", "objeto"],
            })

    for name in ("FUNSOCOM", "LMP INGIENERIA", "UNION TEMPORAL ASEO SCJ"):
        for casing in casings:
            add("NEGATIVE_ACTOR_POSITIVE_LEAK", {
                "q": casing("contratos de aseo cuyo proveedor no sea " + name),
                "expect": "block", "absent": absent,
            })

    for document in ("NIT 900739049", "CC 1020727022"):
        add("IDENTIFIER", pending, {
            "q": document, "expect": "execute",
            "eq": {"actor_resolution_method": "explicit_document"}, "absent": ["actor_pending"],
        })
    return cases


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    cases = actor_hygiene_variants()
    args.out.write_text(yaml.safe_dump({"cases": cases}, allow_unicode=True, sort_keys=False), encoding="utf-8")
    print(f"{len(cases)} cases → {args.out}")


if __name__ == "__main__":
    main()
