#!/usr/bin/env python3
"""Matriz 32/32 de compradores raíz departamentales — read-only.

Separa dos cosas que R2A reportó juntas y no son la misma (R2A.1 §5):

``supporting_alias``      qué alias del gazetteer respalda la raíz — propiedad
                          del DATO.
``runtime_method``        cómo resolvió el runtime esta vez — exact, evidencia
                          raíz, fuzzy… — propiedad de la EJECUCIÓN.

Un alias exacto correcto no prueba que el runtime lo haya usado: Norte de
Santander tenía ``gobernacion de norte de santander`` en el gazetteer y aun así
resolvía por ``fuzzy`` sobre la captura truncada "Norte", arrastrando un
departamento residual. Declarar ``ROOT_FUZZY = 0`` mirando el dato habría sido
falso; hay que mirar el método.

``residual_department`` es la tercera columna que faltaba: el territorio que la
consulta acaba filtrando sin que el usuario lo haya nombrado.

Uso:
    python scripts/audits/governorate_core_matrix.py [--json PATH]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.core.query_router import QueryRouter  # noqa: E402

DEFAULT_DB = ROOT / "app" / "data" / "aliases_db.json"

# Consulta natural por jurisdicción, con el artículo que usa un hablante.
NATURAL_QUERY = {
    "Amazonas": "Gobernación del Amazonas",
    "Antioquia": "Gobernación de Antioquia",
    "Arauca": "Gobernación de Arauca",
    "Atlántico": "Gobernación del Atlántico",
    "Bolívar": "Gobernación de Bolívar",
    "Boyacá": "Gobernación de Boyacá",
    "Caldas": "Gobernación de Caldas",
    "Caquetá": "Gobernación del Caquetá",
    "Casanare": "Gobernación de Casanare",
    "Cauca": "Gobernación del Cauca",
    "Cesar": "Gobernación del Cesar",
    "Chocó": "Gobernación del Chocó",
    "Cundinamarca": "Gobernación de Cundinamarca",
    "Córdoba": "Gobernación de Córdoba",
    "Guainía": "Gobernación del Guainía",
    "Guaviare": "Gobernación del Guaviare",
    "Huila": "Gobernación del Huila",
    "La Guajira": "Gobernación de La Guajira",
    "Magdalena": "Gobernación del Magdalena",
    "Meta": "Gobernación del Meta",
    "Nariño": "Gobernación de Nariño",
    "Norte de Santander": "Gobernación de Norte de Santander",
    "Putumayo": "Gobernación del Putumayo",
    "Quindío": "Gobernación del Quindío",
    "Risaralda": "Gobernación de Risaralda",
    "San Andrés, Providencia y Santa Catalina": "Gobernación de San Andrés",
    "Santander": "Gobernación de Santander",
    "Sucre": "Gobernación de Sucre",
    "Tolima": "Gobernación del Tolima",
    "Valle del Cauca": "Gobernación del Valle del Cauca",
    "Vaupés": "Gobernación del Vaupés",
    "Vichada": "Gobernación del Vichada",
}


def _runtime_method(resolution: dict) -> str:
    """Método REAL, no el nombre de la familia de reescritura.

    ``entidad_resolution['method']`` dice "rewrite_gobernación": eso identifica
    la gramática que disparó, no cómo se acreditó la identidad. La evidencia
    está en la metadata que el resolver adjunta.
    """
    metadata = resolution.get("metadata") or {}
    inner = metadata.get("resolution_method")
    if inner and str(inner).startswith("fuzzy"):
        # Un fuzzy interno NO se deja tapar por el nombre de la gramática.
        return str(inner)
    evidence = metadata.get("root_evidence")
    if evidence == "unique_alias_owner":
        return "root_alias_evidence"
    if evidence == "ambiguous":
        return "root_alias_ambiguous"
    if evidence == "missing":
        return "root_evidence_missing"
    return str(inner or resolution.get("method") or "unresolved")


def build_matrix(aliases_db: Path = DEFAULT_DB) -> list[dict]:
    resolver = EntityResolver(aliases_db)
    router = QueryRouter(entity_resolver=resolver)

    rows: list[dict] = []
    for department, query in NATURAL_QUERY.items():
        direct = resolver.resolve_entidad(query)
        params = router.parse(f"contratos de {query} en 2025").params
        resolution = params.get("entidad_resolution") or {}
        evidence = resolver._root_evidence_by_department.get(department, {})

        resolved = params.get("entidad_resolved")
        supporting = None
        if resolved and resolved in evidence:
            supporting = evidence[resolved][0]

        if resolved:
            status = "PASS"
        elif resolution.get("clarification_needed"):
            status = "FAIL_CLOSED"
        else:
            status = "OTHER"

        rows.append({
            "department": department,
            "natural_query": query,
            "resolved_entity": resolved,
            "runtime_method": _runtime_method(resolution),
            "direct_method": direct.method,
            "supporting_alias": supporting,
            "residual_department": params.get("departamento_resolved"),
            "status": status,
        })
    return rows


def summarize(rows: list[dict]) -> dict:
    return {
        "PASS": sum(1 for r in rows if r["status"] == "PASS"),
        "FAIL_CLOSED": sum(1 for r in rows if r["status"] == "FAIL_CLOSED"),
        "OTHER": sum(1 for r in rows if r["status"] == "OTHER"),
        "RUNTIME_EXACT": sum(
            1 for r in rows
            if r["status"] == "PASS"
            and r["runtime_method"] in {"exact", "root_alias_evidence"}
        ),
        "RUNTIME_FUZZY": sum(
            1 for r in rows if r["runtime_method"].startswith("fuzzy")
        ),
        "SUPPORTING_ALIAS_COUNT": sum(1 for r in rows if r["supporting_alias"]),
        "RESIDUAL_TERRITORY_ERRORS": sum(
            1 for r in rows if r["residual_department"]
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aliases-db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--json", type=Path, default=None)
    args = parser.parse_args()

    rows = build_matrix(args.aliases_db)
    header = f"{'status':12} {'natural_query':36} {'runtime_method':22} {'residual':10} entity"
    print(header)
    print("-" * len(header))
    for row in rows:
        print(
            f"{row['status']:12} {row['natural_query']:36} "
            f"{row['runtime_method']:22} "
            f"{str(row['residual_department'] or '—'):10} "
            f"{str(row['resolved_entity'] or '—')[:46]}"
        )
    summary = summarize(rows)
    print()
    for key, value in summary.items():
        print(f"{key:26} {value}")

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps({"rows": rows, "summary": summary}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"\n→ {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
