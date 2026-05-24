#!/usr/bin/env python3
"""Fase 5 — Smoke gate: 50 queries en 7 buckets, target ≥45/50.

Buckets:
  A — ACCENT (8)
  B — MULTI-AND + RELEVANCE-PHRASE (10)
  C — VALUE POLARITY (5)
  D — ENTITY-MULTI-REGIONAL (5)
  E — ENTITY-ECOSYSTEM (7)
  F — POLYSEMIC (5)
  G — STATE-CATALOG + AUTO-FALLBACK (10)

Protocolo anti-saturación:
  - Pre-check curl <2s antes de cada bucket
  - Bursts de 5 + sleep 30s entre buckets
  - Si >3 timeouts en un bucket, abortar y guardar checkpoint
  - Output: baseline/smoke_sprint6_gate_<timestamp>.txt
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow

# ── 7 buckets, 50 queries ──────────────────────────────────────────────────

BUCKETS: dict[str, list[tuple[int, str]]] = {
    "A-ACCENT": [
        (1, "consultoria en sistemas en Bogota"),
        (2, "diseño de obras en Antioquia"),
        (3, "jardineria y mantenimiento en Cundinamarca"),
        (4, "servicios turisticos en Atlantico"),
        (5, "cafeteria y restaurante escolar en Santander"),
        (6, "logistica y transporte en Valle"),
        (7, "consultoria informatica en Santander"),
        (8, "servicios artisticos y culturales en Caldas"),
    ],
    "B-MULTI-AND+RELEVANCE": [
        (9, "aseo cafeteria jardineria y mantenimiento"),
        (10, "control de calidad de agua para consumo humano"),
        (11, "alimentacion escolar PAE rural integral"),
        (12, "transporte escolar rural integral en Huila"),
        (13, "dotacion de uniformes educativos en Narino"),
        (14, "mantenimiento correctivo y preventivo equipos"),
        (15, "consultoria juridica y financiera en Bolivar"),
        (16, "construccion de vias pavimentacion y alcantarillado Cauca"),
        (17, "suministro de medicamentos y dispositivos medicos en Santander"),
        (18, "interventoria tecnica administrativa y financiera en Tolima"),
    ],
    "C-VALUE-POLARITY": [
        (19, "contratos no mayores a 100 millones en Cundinamarca"),
        (20, "contratos no menores a 500 millones en Antioquia"),
        (21, "contratos menor o igual a 50 millones en Bogota"),
        (22, "contratos a lo sumo 200 millones en Santander"),
        (23, "contratos por debajo de 30 millones en Atlantico"),
    ],
    "D-ENTITY-MULTI-REGIONAL": [
        (24, "contratos del SENA en Bolivar"),
        (25, "contratos del SENA en Atlantico"),
        (26, "contratos del ICBF en Magdalena"),
        (27, "contratos del SENA en Antioquia"),
        (28, "contratos del ICBF en Tolima"),
    ],
    "E-ENTITY-ECOSYSTEM": [
        (29, "contratos Bogota"),
        (30, "procesos Medellin"),
        (31, "contratos secretarias Cali"),
        (32, "contratos Gobernacion de Antioquia"),
        (33, "contratos Gobernacion de Cundinamarca"),
        (34, "contratos alcaldia de Cali"),
        (35, "contratos Gobernacion del Valle"),
    ],
    "F-POLYSEMIC": [
        (36, "alojamiento"),                     # sin contexto → clarification?
        (37, "alojamiento estudiantes en Cali"),  # con contexto → debería resolver
        (38, "transporte"),                       # sin contexto
        (39, "transporte escolar en Narino"),      # con contexto
        (40, "servicios en Bolivar"),             # ambiguo con contexto parcial
    ],
    "G-STATE-CATALOG+AUTO-FALLBACK": [
        (41, "procesos publicados en Tolima"),
        (42, "procesos en borrador en Bogota"),
        (43, "procesos en evaluacion en Antioquia"),
        (44, "procesos aprobados en Cundinamarca"),
        (45, "procesos seleccionados en Valle del Cauca"),
        (46, "procesos Adjudicado en Atlantico 2026"),     # AUTO-FALLBACK: año futuro
        (47, "contratos Celebrado en Bogota 2026"),         # AUTO-FALLBACK: año futuro
        (48, "contratos Liquidado en Santander"),           # state + normal query
        (49, "procesos en curso en Magdalena 2026"),        # state ambigua + año futuro
        (50, "procesos Convocado en Bolivar"),              # estado específico
    ],
}

BURST_SLEEP = 30
BURST_SIZE = 5
CHECKPOINT_PATH = Path(__file__).resolve().parents[1] / "baseline" / ".smoke_checkpoint.json"
BASELINE_DIR = Path(__file__).resolve().parents[1] / "baseline"

# ── Helpers ─────────────────────────────────────────────────────────────────


def pre_check() -> float:
    """curl rápido al API. Retorna tiempo en segundos."""
    try:
        r = subprocess.run(
            ["curl", "-s", "-w", "%{time_total}", "--max-time", "15",
             "https://www.datos.gov.co/resource/p6dx-8zbt.json?$limit=1&$select=count(*)"],
            capture_output=True, text=True, timeout=18,
        )
        output = r.stdout.strip()
        # output ends with time_total (e.g. "...\n0.345")
        lines = output.splitlines()
        try:
            return float(lines[-1])
        except ValueError:
            return 999.0
    except Exception:
        return 999.0


def save_checkpoint(
    results: dict,
    completed_buckets: list[str],
    total_pass: int,
    total_queries: int,
) -> None:
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.write_text(json.dumps({
        "results": results,
        "completed_buckets": completed_buckets,
        "total_pass": total_pass,
        "total_queries": total_queries,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }, indent=2, ensure_ascii=False))


def load_checkpoint() -> dict | None:
    if not CHECKPOINT_PATH.exists():
        return None
    try:
        return json.loads(CHECKPOINT_PATH.read_text())
    except Exception:
        return None


def write_report(
    results: dict,
    total_pass: int,
    total_queries: int,
    elapsed_total: float,
    total_timeouts: int,
    aborted: bool = False,
) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = BASELINE_DIR / f"smoke_sprint6_gate_{ts}.txt"
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)

    lines = [
        f"SMOKE SPRINT 6 GATE — {ts}",
        "=" * 70,
        f"Target: ≥45/50 (90%)",
        f"Status: {'ABORTED' if aborted else 'COMPLETE'}",
        f"Elapsed total: {elapsed_total:.0f}s",
        f"",
        f"{'Bucket':<28} {'Pass':>6} {'%':>5} {'Avg(s)':>7} {'Timeouts':>9}",
        "-" * 60,
    ]
    for name, res in results.items():
        lines.append(
            f"{name:<28} {res['pass']}/{res['total']:>3} {res['pct']:>4.0f}% "
            f"{res['avg_time']:>6.1f}s {res['timeouts']:>5}"
        )
    lines.append("-" * 60)
    pct = total_pass / total_queries * 100 if total_queries else 0
    lines.append(f"{'TOTAL':<28} {total_pass}/{total_queries:>3} {pct:>4.0f}%")
    lines.append(f"Timeouts: {total_timeouts}")
    lines.append(f"API pre-check: {pre_check():.1f}s")

    if total_pass >= 45:
        lines.append("\n✅ GATE VERDE — Sprint 6 cerrado. Listo para cohorte real.")
    elif not aborted:
        lines.append(f"\n❌ GATE FALLIDO — {total_pass}/50. Investigar buckets <70%.")

    report = "\n".join(lines) + "\n"
    path.write_text(report)
    return path


# ── Main ────────────────────────────────────────────────────────────────────


def main() -> int:
    s = Settings()
    s.secop_timeout_seconds = 15
    wf = SecopalWorkflow(s)

    # Checkpoint resume
    cp = load_checkpoint()
    bucket_results: dict[str, dict] = cp["results"] if cp else {}
    completed_buckets = set(cp["completed_buckets"] if cp else [])
    total_pass = cp["total_pass"] if cp else 0
    total_queries = cp["total_queries"] if cp else 0
    total_timeouts = 0
    aborted = False
    t_start = time.time()

    if completed_buckets:
        print(f"[checkpoint] Retomando — {len(completed_buckets)} buckets ya completados:")
        for b in completed_buckets:
            print(f"  ✓ {b}")

    bucket_order = [(n, q) for n, q in BUCKETS.items() if n not in completed_buckets]

    for bi, (bucket_name, queries) in enumerate(bucket_order):
        # Pre-check
        api_latency = pre_check()
        print(f"\n[pre] API latency: {api_latency:.1f}s")
        if api_latency > 5.0:
            print(f"  ⚠️  API lenta ({api_latency:.1f}s) — riesgo alto de timeouts.")

        if bi > 0 or completed_buckets:
            print(f"\n  [sleep {BURST_SLEEP}s...]")
            time.sleep(BURST_SLEEP)

        print(f"\n{'=' * 70}")
        print(f"  BUCKET: {bucket_name} ({len(queries)} queries)")
        print(f"{'=' * 70}")
        print(f"{'#':>3}  {'QUERY':<58} {'N':>5} {'T(s)':>5}  STATUS")
        print("-" * 80)

        bucket_pass = 0
        bucket_time = 0.0
        bucket_timeouts = 0
        bucket_fails: list[dict] = []

        for idx, query in queries:
            t0 = time.time()
            try:
                r = wf.run_query(query, channel="streamlit")
                elapsed = time.time() - t0
                n = len(r.get("results") or [])
                deg = r.get("degraded", False)
                ok = n > 0
                is_timeout = elapsed > 14.0 and n == 0  # real timeout: slow AND failed
                if is_timeout:
                    bucket_timeouts += 1
                    total_timeouts += 1
                if ok:
                    bucket_pass += 1
                    total_pass += 1
                status = "OK" if ok else ("DEG-OK" if deg else "FAIL")
                flag = "✓" if ok else "✗"
            except Exception as exc:
                elapsed = time.time() - t0
                n = 0
                ok = False
                is_timeout = elapsed > 14.0
                if is_timeout:
                    bucket_timeouts += 1
                    total_timeouts += 1
                status = f"ERR: {exc!s:.30}"
                flag = "✗"

            bucket_time += elapsed
            total_queries += 1
            time_mark = " TIMEOUT" if is_timeout else ""
            print(f"{idx:>3}  {query:<58} {n:>5} {elapsed:>5.1f}  {flag} {status}{time_mark}")
            if not ok:
                bucket_fails.append({
                    "idx": idx, "query": query, "elapsed": elapsed,
                    "status": status, "timeout": is_timeout,
                })

        avg = bucket_time / len(queries) if queries else 0
        pct = bucket_pass / len(queries) * 100 if queries else 0
        print("-" * 80)
        print(f"  Pass: {bucket_pass}/{len(queries)} ({pct:.0f}%) | Avg: {avg:.1f}s | Timeouts: {bucket_timeouts}")

        bucket_results[bucket_name] = {
            "pass": bucket_pass,
            "total": len(queries),
            "pct": pct,
            "avg_time": round(avg, 1),
            "timeouts": bucket_timeouts,
            "fails": bucket_fails,
        }
        completed_buckets.add(bucket_name)

        # Guardar checkpoint tras cada bucket
        save_checkpoint(bucket_results, list(completed_buckets), total_pass, total_queries)

        # Abortar si >3 timeouts en este bucket
        if bucket_timeouts > 3:
            remaining = sum(len(q) for n, q in bucket_order[bi + 1:])
            print(f"\n  ⚠️  >3 timeouts en {bucket_name} — API congestionada.")
            print(f"  Checkpoint guardado en {CHECKPOINT_PATH}")
            print(f"  Quedan {remaining} queries en {len(bucket_order) - bi - 1} buckets.")
            print(f"  Retomar más tarde con: python3 scripts/smoke_burst.py")
            aborted = True
            break

    elapsed_total = time.time() - t_start

    # Reporte
    print(f"\n{'=' * 70}")
    print(f"  RESUMEN FINAL" + (" (ABORTADO)" if aborted else ""))
    print(f"{'=' * 70}")
    print(f"{'Bucket':<28} {'Pass':>6} {'%':>5} {'Avg(s)':>7} {'Timeouts':>9}")
    print("-" * 60)
    for name, res in bucket_results.items():
        print(f"{name:<28} {res['pass']}/{res['total']:>3} {res['pct']:>4.0f}% {res['avg_time']:>6.1f}s {res['timeouts']:>5}")
    print("-" * 60)
    pct = total_pass / total_queries * 100 if total_queries else 0
    print(f"{'TOTAL':<28} {total_pass}/{total_queries:>3} {pct:>4.0f}%")
    print(f"Timeouts: {total_timeouts}")

    report_path = write_report(bucket_results, total_pass, total_queries, elapsed_total, total_timeouts, aborted)
    print(f"\nReporte: {report_path}")

    if not aborted:
        CHECKPOINT_PATH.unlink(missing_ok=True)

    if total_pass >= 45 and not aborted:
        print("\n✅ GATE VERDE — Sprint 6 cerrado. Listo para cohorte real.")
        return 0
    elif not aborted:
        print(f"\n❌ GATE FALLIDO — {total_pass}/50. Investigar buckets <70%.")
        return 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
