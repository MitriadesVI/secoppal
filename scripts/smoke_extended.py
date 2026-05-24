#!/usr/bin/env python3
"""Smoke test extendido — 30 queries en 5 buckets para auditar precisión empírica.

Buckets:
  A — ACCENT (10): normalización de tildes (ACCENT-NORMALIZATION-001)
  B — MULTI-AND (5): degradación AND→OR (MULTI-AND-OVER-RESTRICTIVE-001)
  C — VALUE POLARITY (5): polaridad monetaria (MIN-MAX-INVERSION-001)
  D — ENTITY-MULTI-REGIONAL (5): entidades regionales (ENTITY-MULTI-REGIONAL-001)
  E — STATE-CATALOG (5): catálogo de estados (STATE-CATALOG-INCOMPLETE-001)

Criterio por bucket:
  - PASS si >0 resultados (la query encontró algo)
  - FAIL si 0 resultados (puede ser timeout o bug real)
  - DEG-OK si degraded pero trajo resultados
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow

BUCKETS: dict[str, list[tuple[int, str]]] = {
    "A-ACCENT": [
        (1, "consultoria en sistemas en Bogota"),
        (2, "diseño de obras en Antioquia"),
        (3, "jardineria y mantenimiento en Cundinamarca"),
        (4, "servicios turisticos en Atlantico"),
        (5, "cafeteria y restaurante escolar"),
        (6, "logistica y transporte en Valle"),
        (7, "consultoria informatica en Santander"),
        (8, "servicios artisticos y culturales en Caldas"),
        (9, "residuos solidos en Bolivar"),
        (10, "construccion de vias en Tolima"),
    ],
    "B-MULTI-AND": [
        (11, "aseo cafeteria jardineria y mantenimiento"),
        (12, "control de calidad de agua para consumo humano"),
        (13, "alimentacion escolar PAE en Cordoba"),
        (14, "transporte escolar rural integral en Huila"),
        (15, "dotacion de uniformes educativos en Narino"),
    ],
    "C-VALUE-POLARITY": [
        (16, "contratos no mayores a 100 millones en Cundinamarca"),
        (17, "contratos no menores a 500 millones en Antioquia"),
        (18, "contratos menor o igual a 50 millones en Bogota"),
        (19, "contratos a lo sumo 200 millones en Santander"),
        (20, "contratos por debajo de 30 millones en Atlantico"),
    ],
    "D-ENTITY-MULTI-REGIONAL": [
        (21, "contratos del SENA en Bolivar"),
        (22, "contratos del SENA en Atlantico"),
        (23, "contratos del ICBF en Magdalena"),
        (24, "contratos del SENA en Antioquia"),
        (25, "contratos del ICBF en Tolima"),
    ],
    "E-STATE-CATALOG": [
        (26, "procesos publicados en Tolima"),
        (27, "procesos en borrador en Bogota"),
        (28, "procesos en evaluacion en Antioquia"),
        (29, "procesos aprobados en Cundinamarca"),
        (30, "procesos seleccionados en Valle del Cauca"),
    ],
}


def main() -> int:
    s = Settings()
    s.secop_timeout_seconds = 15
    wf = SecopalWorkflow(s)

    bucket_results: dict[str, dict] = {}
    total_pass = 0
    total_queries = 0

    for bucket_name, queries in BUCKETS.items():
        print(f"\n{'=' * 70}")
        print(f"  BUCKET: {bucket_name} ({len(queries)} queries)")
        print(f"{'=' * 70}")
        print(f"{'#':>3}  {'QUERY':<58} {'N':>5} {'T(s)':>5}  STATUS")
        print("-" * 80)

        bucket_pass = 0
        bucket_time = 0.0
        bucket_fails: list[dict] = []

        for idx, query in queries:
            t0 = time.time()
            try:
                r = wf.run_query(query, channel="streamlit")
                elapsed = time.time() - t0
                n = len(r.get("results") or [])
                deg = r.get("degraded", False)
                ok = n > 0
                if ok:
                    bucket_pass += 1
                    total_pass += 1
                status = "OK" if ok else ("DEG-OK" if deg else "FAIL")
                flag = "✓" if ok else "✗"
            except Exception as exc:
                elapsed = time.time() - t0
                n = 0
                ok = False
                status = f"ERR: {exc!s:.30}"
                flag = "✗"

            bucket_time += elapsed
            total_queries += 1
            print(f"{idx:>3}  {query:<58} {n:>5} {elapsed:>5.1f}  {flag} {status}")
            if not ok:
                bucket_fails.append({
                    "idx": idx, "query": query, "elapsed": elapsed, "status": status
                })

        avg = bucket_time / len(queries) if queries else 0
        pct = bucket_pass / len(queries) * 100 if queries else 0
        print("-" * 80)
        print(f"  Pass: {bucket_pass}/{len(queries)} ({pct:.0f}%) | Avg: {avg:.1f}s")
        bucket_results[bucket_name] = {
            "pass": bucket_pass,
            "total": len(queries),
            "pct": pct,
            "avg_time": round(avg, 1),
            "fails": bucket_fails,
        }

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  RESUMEN FINAL")
    print(f"{'=' * 70}")
    print(f"{'Bucket':<20} {'Pass':>6} {'%':>5} {'Avg(s)':>7}")
    print("-" * 45)
    for name, res in bucket_results.items():
        print(f"{name:<20} {res['pass']}/{res['total']:>3} {res['pct']:>4.0f}% {res['avg_time']:>6.1f}s")
    print("-" * 45)
    print(f"{'TOTAL':<20} {total_pass}/{total_queries:>3} {total_pass/total_queries*100:>4.0f}%")
    print(f"\nQueries con 0 resultados:")
    for name, res in bucket_results.items():
        for f in res["fails"]:
            print(f"  [{name}] #{f['idx']} {f['query']:<50} {f['elapsed']:>5.1f}s  {f['status']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
