#!/usr/bin/env python3
"""Smoke test 1.5 — 25 queries diversas contra SECOP real.

Uso:
    python scripts/smoke_test.py              # todas las queries
    python scripts/smoke_test.py --fast       # timeout reducido (5s)
    python scripts/smoke_test.py --query 3    # solo query #3

Criterio de done: >= 22/25 retornan >0 resultados.
Retorna exit code 0 si pasa, 1 si falla el criterio.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Asegurar que el root del proyecto este en el path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow

# ---------------------------------------------------------------------------
# Corpus de 25 queries
# Cubre: contratos, procesos, departamentos, entidades, rangos, estados,
#        variantes morfologicas, spell correction, degradacion elegante
# ---------------------------------------------------------------------------
QUERIES = [
    # --- Basicas (deben tener muchos resultados) ---
    (1,  "contratos de obra en Bogota"),
    (2,  "contratos de mantenimiento vial en Antioquia"),
    (3,  "procesos abiertos en Cundinamarca"),
    (4,  "contratos de construccion en Medellin"),
    (5,  "contratos de suministro en Valle del Cauca"),

    # --- Filtros de valor ---
    (6,  "contratos entre 100 y 500 millones en Bogota"),
    (7,  "contratos mayores a 1000 millones en Colombia"),
    (8,  "contratos menores a 50 millones de pavimentacion"),
    (9,  "contratos entre 200 y 800 millones de dotacion"),
    (10, "contratos de obra mayores a 500 millones en Santander"),

    # --- Entidades especificas ---
    (11, "contratos de la alcaldia de Barranquilla"),
    (12, "contratos del INVIAS"),
    (13, "contratos de la gobernacion de Boyaca"),
    (14, "contratos del ministerio de educacion"),
    (15, "procesos de la alcaldia de Cali"),

    # --- Variantes morfologicas ---
    (16, "mantenimientos de via en Nariño"),
    (17, "pavimentaciones en Cordoba"),
    (18, "adecuaciones de colegios en Huila"),
    (19, "construcciones de acueducto en Cauca"),
    (20, "suministros de medicamentos en Atlantico"),

    # --- Estados ---
    (21, "contratos adjudicados en Risaralda"),
    (22, "procesos convocados en Tolima"),
    (23, "contratos activos de interventoria en Bolivar"),

    # --- Casos que pueden retornar 0 / degradacion ---
    (24, "contratos de trampa de grasa en Putumayo"),       # objeto muy especifico
    (25, "contratos entre 999 y 1001 millones en Guainia"), # rango muy estrecho + depto poco activo
]

# ---------------------------------------------------------------------------


def run_smoke(queries: list[tuple], timeout_override: int | None = None) -> int:
    settings = Settings()
    if timeout_override:
        settings.secop_timeout_seconds = timeout_override

    workflow = SecopalWorkflow(settings)

    results_table: list[dict] = []
    passed = 0
    degraded_count = 0

    print(f"\n{'#':>3}  {'QUERY':<52} {'N':>5} {'T(s)':>5}  STATUS")
    print("-" * 80)

    for idx, query in queries:
        t0 = time.time()
        try:
            result = workflow.run_query(query, channel="telegram")
            elapsed = time.time() - t0
            n = len(result.get("results") or [])
            deg = result.get("degraded", False)
            ok = n > 0
            if ok:
                passed += 1
            if deg:
                degraded_count += 1
            status = "OK" if ok else ("DEG" if deg else "FAIL")
            if deg and ok:
                status = "DEG-OK"
        except Exception as exc:
            elapsed = time.time() - t0
            n = 0
            ok = False
            status = f"ERR: {exc!s:.30}"

        flag = "✓" if ok else "✗"
        print(f"{idx:>3}  {query:<52} {n:>5} {elapsed:>5.1f}  {flag} {status}")
        results_table.append({"idx": idx, "query": query, "n": n, "ok": ok, "elapsed": elapsed, "status": status})

    # Resumen
    total = len(queries)
    print("-" * 80)
    print(f"\nResultados: {passed}/{total} con >0 resultados  |  {degraded_count} degradados")

    threshold = 22 * total // 25  # escala si se corre subconjunto
    if total == 25:
        threshold = 22

    if passed >= threshold:
        print(f"PASS — criterio cumplido ({passed} >= {threshold})")
    else:
        print(f"FAIL — criterio NO cumplido ({passed} < {threshold})")

    # Detalle de fallos
    failed = [r for r in results_table if not r["ok"]]
    if failed:
        print(f"\nQueries sin resultados ({len(failed)}):")
        for r in failed:
            print(f"  #{r['idx']:>2}  {r['query']}")

    return 0 if passed >= threshold else 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test SECOP real")
    parser.add_argument("--fast", action="store_true", help="Timeout 5s por query")
    parser.add_argument("--query", type=int, metavar="N", help="Correr solo query #N")
    args = parser.parse_args()

    queries = QUERIES
    if args.query:
        queries = [(idx, q) for idx, q in QUERIES if idx == args.query]
        if not queries:
            print(f"Query #{args.query} no existe.")
            sys.exit(1)

    timeout = 5 if args.fast else None
    sys.exit(run_smoke(queries, timeout_override=timeout))


if __name__ == "__main__":
    main()
