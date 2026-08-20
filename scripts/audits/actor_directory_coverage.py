#!/usr/bin/env python3
"""AP3-E — cobertura del directorio de actores, medida contra la fuente.

Separa deliberadamente dos cosas que suelen confundirse:

* ``RESOLVER_QUALITY`` — dado un alias presente, ¿el resolver acierta?
* ``INDEX_COVERAGE``   — ¿qué fracción de los actores reales está presente?

Un resolver perfecto sobre un directorio del 4% sigue siendo inútil, y un
directorio completo con un resolver laxo es peligroso. Se reportan aparte.

Subcomandos:
    coverage   denominadores, cobertura global y por segmento
    holdout    holdout externo (6 actores) + holdout aleatorio reproducible
"""
from __future__ import annotations

import argparse
import collections
import json
import random
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATASET = "jbjy-vk9h"
INDEX = ROOT / "data" / "actor_alias_index.sqlite"
OUT = ROOT / "tests" / "fixtures" / "actor_directory"

# Muestreo del holdout aleatorio: semilla propia, distinta de la del corpus
# adversarial y del build, para que no herede ninguno de sus sesgos.
HOLDOUT_SEED = 20260820

EXTERNAL_HOLDOUT = [
    ("ASILO DE SAN ANTONIO", "890.102.897-7", "890102897"),
    ("ASILO HOGAR GRANJA SAN JOSE", "890.102.896-1", "890102896"),
    ("FUNDACIÓN ANA DELIA", "901.717.025-0", "901717025"),
    ("TUS ABUELOS BARRANQUILLA S.A.S.", "900.923.481-5", "900923481"),
    ("FUNDACION HOGAR DE SAN CAMILO", "802.019.565-2", "802019565"),
    ("FUNDACION HOGAR DE PASO MIS MEJORES AÑOS", "901.313.012-0", "901313012"),
]


def _env() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def soql(query: str, timeout: int = 300):
    env = _env()
    url = (
        f"https://{env.get('DATOS_GOV_DOMAIN', 'www.datos.gov.co')}"
        f"/resource/{DATASET}.json?$query=" + urllib.parse.quote(query)
    )
    request = urllib.request.Request(
        url, headers={"X-App-Token": env.get("SECOP_APP_TOKEN", "")}
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def connect():
    return sqlite3.connect(f"file:{INDEX}?mode=ro", uri=True)


def index_metadata() -> dict:
    connection = connect()
    connection.row_factory = sqlite3.Row
    row = connection.execute(
        "SELECT * FROM actor_index_metadata WHERE singleton = 1"
    ).fetchone()
    connection.close()
    return dict(row)


def indexed_identities() -> set[tuple[str, str]]:
    connection = connect()
    rows = connection.execute(
        "SELECT DISTINCT document_type, base_number FROM actor_aliases"
    ).fetchall()
    connection.close()
    return {(str(a), str(b)) for a, b in rows}


def cmd_coverage(args: argparse.Namespace) -> int:
    meta = index_metadata()
    identities = indexed_identities()
    print("── índice ──")
    for key in (
        "schema_version", "normalization_version", "built_at", "source_dataset",
        "source_max_date", "source_row_count", "identity_count", "alias_count",
        "ambiguous_alias_count", "excluded_dirty_count",
    ):
        if key in meta:
            print(f"  {key}: {meta[key]}")

    quality = json.loads(meta.get("source_quality_json") or "{}")
    print("\n── razones de exclusión ──")
    for reason, count in sorted(
        (quality.get("dirty_reason_counts") or {}).items(),
        key=lambda item: -item[1],
    ):
        print(f"  {reason}: {count:,}")

    eligible = int(quality.get("accepted_aggregated_rows") or 0)
    print("\n── denominadores ──")
    source_documents = soql(
        "SELECT count(distinct documento_proveedor) AS n "
        "WHERE documento_proveedor IS NOT NULL AND documento_proveedor != 'No Definido'"
    )[0]["n"]
    print(f"  documentos distintos en la fuente : {int(source_documents):,}")
    print(f"  grupos aceptados por el build     : {eligible:,}")
    print(f"  identidades indexadas             : {len(identities):,}")

    by_type = collections.Counter(document_type for document_type, _ in identities)
    print(f"  por tipo: {dict(by_type)}")

    payload = {
        "metadata": {k: meta[k] for k in meta if k != "build_parameters_json"},
        "source_distinct_documents": int(source_documents),
        "indexed_identities": len(identities),
        "by_type": dict(by_type),
        "dirty_reason_counts": quality.get("dirty_reason_counts"),
    }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "coverage.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


def _classify(result, expected_base: str | None) -> str:
    """Traduce el resultado del resolver a la taxonomía de §9.

    ``WRONG_IDENTITY`` es el único estado bloqueante: significa que el
    directorio afirmó una identidad y se equivocó. Los demás son grados de
    cautela — no saber es cobertura, no falsa atribución.
    """
    from app.core.actor_name import ActorResolutionStatus

    if result.status == ActorResolutionStatus.RESOLVED:
        assert result.identity is not None
        if expected_base and result.identity.base_number != expected_base:
            return "WRONG_IDENTITY"
        return "EXACT_RESOLVED"
    if result.status == ActorResolutionStatus.AMBIGUOUS:
        return "AMBIGUOUS_SAFE"
    if result.status == ActorResolutionStatus.CANDIDATES:
        return "CANDIDATES_SAFE"
    if result.status == ActorResolutionStatus.INDEX_UNAVAILABLE:
        return "INDEX_UNAVAILABLE"
    return "UNRESOLVED_SAFE"


def _exact_alias_rows(normalized: str) -> list[tuple[str, str, str]]:
    """Oracle: el índice consultado DIRECTAMENTE, sin pasar por el resolver."""
    connection = connect()
    rows = connection.execute(
        "SELECT document_type, base_number, raw_alias FROM actor_aliases "
        "WHERE normalized_alias = ?",
        (normalized,),
    ).fetchall()
    connection.close()
    return [(str(a), str(b), str(c)) for a, b, c in rows]


def cmd_holdout(args: argparse.Namespace) -> int:
    from app.core.actor_name import normalize_actor_name
    from app.data.actor_alias_resolver import ActorNameResolver

    resolver = ActorNameResolver(INDEX)
    OUT.mkdir(parents=True, exist_ok=True)

    print("── holdout externo (§9) ──")
    external: list[dict] = []
    for name, raw_nit, base in EXTERNAL_HOLDOUT:
        normalized = normalize_actor_name(name)
        rows = _exact_alias_rows(normalized)
        result = resolver.resolve(name)
        status = _classify(result, base)
        external.append({
            "name": name, "expected_nit_raw": raw_nit, "expected_base_number": base,
            "normalized_alias": normalized,
            "present_in_index": bool(rows),
            "exact_identity_set": [f"{t}:{b}" for t, b, _ in rows],
            "resolver_status": result.status, "resolver_method": result.method,
            "resolved_base": result.identity.base_number if result.identity else None,
            "status": status,
        })
        print(f"  {name[:44]:46s} {status:18s} idx={bool(rows)} "
              f"resuelto={result.identity.base_number if result.identity else None}")

    print("\n── holdout aleatorio (§10) ──")
    # Muestreo independiente del ranking del índice: se toma de la FUENTE, en
    # páginas dispersas del agrupamiento, y luego se elige al azar con semilla
    # propia. El índice no participa en la selección.
    rng = random.Random(HOLDOUT_SEED)
    pool: list[dict] = []
    base_query = (
        "SELECT tipodocproveedor, documento_proveedor, proveedor_adjudicado, "
        "count(*) AS occurrences "
        "WHERE documento_proveedor IS NOT NULL AND documento_proveedor != 'No Definido' "
        "AND proveedor_adjudicado IS NOT NULL "
        "GROUP BY tipodocproveedor, documento_proveedor, proveedor_adjudicado "
        "ORDER BY documento_proveedor, tipodocproveedor, proveedor_adjudicado"
    )
    for offset in args.offsets:
        try:
            page = soql(base_query + f" LIMIT 2000 OFFSET {offset}")
            pool.extend(page)
            print(f"  muestra offset={offset}: {len(page)} grupos")
        except Exception as exc:
            print(f"  muestra offset={offset}: FALLA {str(exc)[:70]}")

    typed = [
        row for row in pool
        if row.get("tipodocproveedor") in {"NIT", "Cédula de Ciudadanía"}
        and str(row.get("proveedor_adjudicado") or "").strip()
    ]
    sample = rng.sample(typed, min(args.size, len(typed)))

    counts: collections.Counter[str] = collections.Counter()
    random_rows: list[dict] = []
    for row in sample:
        name = str(row["proveedor_adjudicado"])
        document = str(row["documento_proveedor"])
        source_type = str(row["tipodocproveedor"])
        document_type = "NIT" if source_type == "NIT" else "CC"
        from app.core.actor_identity import normalize_identity

        match = normalize_identity(document, document_type=document_type)
        expected = match.base_number
        result = resolver.resolve(name)
        status = _classify(result, expected)
        counts[status] += 1
        random_rows.append({
            "name": name, "document": document, "source_type": source_type,
            "expected_base_number": expected, "occurrences": int(row["occurrences"]),
            "resolver_status": result.status, "status": status,
            "resolved_base": result.identity.base_number if result.identity else None,
        })

    print(f"\n  muestra: {len(random_rows)} actores reales")
    for status, count in counts.most_common():
        print(f"    {status:20s} {count:5d}  ({count / max(len(random_rows), 1) * 100:5.1f}%)")

    (OUT / "holdout.json").write_text(
        json.dumps({
            "external": external,
            "random": {"seed": HOLDOUT_SEED, "size": len(random_rows),
                       "counts": dict(counts), "rows": random_rows},
        }, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    wrong = counts["WRONG_IDENTITY"] + sum(
        1 for row in external if row["status"] == "WRONG_IDENTITY"
    )
    print(f"\n  WRONG_IDENTITY total: {wrong}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    coverage = sub.add_parser("coverage")
    coverage.set_defaults(func=cmd_coverage)
    holdout = sub.add_parser("holdout")
    holdout.add_argument("--size", type=int, default=120)
    holdout.add_argument("--offsets", type=int, nargs="+",
                         default=[7_000, 250_000, 480_000, 760_000, 1_050_000])
    holdout.set_defaults(func=cmd_holdout)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
