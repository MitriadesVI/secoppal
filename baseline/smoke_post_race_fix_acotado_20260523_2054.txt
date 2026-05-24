#!/usr/bin/env python3
"""Smoke test acotado — 10 queries representativas para validar race-fix.

Uso:
    python scripts/smoke_acotado.py

Criterio: >=7/10 con >0 resultados = PASS.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow

QUERIES = [
    (2,  "contratos de mantenimiento vial en Antioquia"),
    (3,  "procesos abiertos en Cundinamarca"),
    (6,  "contratos entre 100 y 500 millones en Bogota"),
    (10, "contratos de obra mayores a 500 millones en Santander"),
    (12, "contratos del INVIAS"),
    (13, "contratos de la gobernacion de Boyaca"),
    (15, "procesos de la alcaldia de Cali"),
    (20, "suministros de medicamentos en Atlantico"),
    (22, "procesos convocados en Tolima"),
    (24, "contratos de trampa de grasa en Putumayo"),
]


def main() -> int:
    s = Settings()
    s.secop_timeout_seconds = 15
    wf = SecopalWorkflow(s)

    passed = 0
    total_time = 0.0
    results: list[dict] = []

    print(f"{'#':>3}  {'QUERY':<52} {'N':>5} {'T(s)':>5}  STATUS")
    print("-" * 80)

    for idx, query in QUERIES:
        t0 = time.time()
        try:
            r = wf.run_query(query, channel="telegram")
            elapsed = time.time() - t0
            n = len(r.get("results") or [])
            deg = r.get("degraded", False)
            ok = n > 0
            if ok:
                passed += 1
            status = "OK" if ok else ("DEG-OK" if deg else "FAIL")
            flag = "✓" if ok else "✗"
        except Exception as exc:
            elapsed = time.time() - t0
            n = 0
            ok = False
            status = f"ERR: {exc!s:.30}"
            flag = "✗"

        total_time += elapsed
        print(f"{idx:>3}  {query:<52} {n:>5} {elapsed:>5.1f}  {flag} {status}")
        results.append({"idx": idx, "query": query, "n": n, "ok": ok, "elapsed": elapsed, "status": status})

    print("-" * 80)
    print(f"Resultados: {passed}/{len(QUERIES)} con >0 resultados")
    avg = total_time / len(QUERIES)
    print(f"Tiempo medio: {avg:.1f}s/query")

    threshold = 7
    if passed >= threshold:
        print(f"\nPASS — race fix validado ({passed} >= {threshold})")
        print("Listo para Sprint 3 fase 2.")
    else:
        print(f"\nFAIL — criterio NO cumplido ({passed} < {threshold})")
        failed = [r for r in results if not r["ok"]]
        print(f"\nQueries fallidas ({len(failed)}):")
        for r in failed:
            print(f"  #{r['idx']:>2}  {r['query']:<52} {r['elapsed']:>5.1f}s  {r['status']}")

    return 0 if passed >= threshold else 1


if __name__ == "__main__":
    sys.exit(main())
