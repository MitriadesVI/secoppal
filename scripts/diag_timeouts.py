#!/usr/bin/env python3
"""Diagnóstico de timeouts contra datos.gov.co — separa las 4 hipótesis.

NO adivina: mide. Para cada query real, ejecuta la SoQL varias veces y con dos
límites distintos, registra latencia/timeout y la ESTRUCTURA de la SoQL, y reporta
el patrón que distingue:

  H1 API intermitente   → la MISMA query varía mucho entre intentos (no determinista)
  H2 unaccent LIKE scan → lenta determinísticamente, correlaciona con #LIKE objeto
  H3 payload ($limit)   → la latencia cae fuerte al bajar el limit
  H4 count query        → el timeout está en COUNT, no en los datos

Uso:
    .venv/bin/python scripts/diag_timeouts.py --max 15 --repeat 3
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import get_settings
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.core.secop_client import SecopClient

REPO = Path(__file__).resolve().parents[1]
SECOPPAL_DB = REPO / "data" / "secoppal.db"


def soql_features(soql: str) -> dict:
    """Caracteriza la SoQL: lo que correlaciona con H2/H3."""
    low = soql.lower()
    limit_m = re.search(r"limit\s+(\d+)", low)
    return {
        "n_like": low.count(" like "),
        "n_unaccent": low.count("unaccent("),
        "n_or": low.count(" or "),
        "has_where": " where " in low,
        "limit": int(limit_m.group(1)) if limit_m else None,
        "len": len(soql),
    }


def timed_query(client: SecopClient, ds_id: str, soql: str) -> tuple[float, bool, int]:
    """Ejecuta una SoQL una vez. Devuelve (latencia_s, ok, n_filas).
    No usa el retry interno — queremos ver el comportamiento crudo por intento."""
    t0 = time.time()
    try:
        rows = client.query(ds_id, soql)
        return time.time() - t0, True, len(rows)
    except Exception:
        return time.time() - t0, False, 0


def load_queries(limit: int) -> list[str]:
    conn = sqlite3.connect(SECOPPAL_DB)
    rows = conn.execute(
        "SELECT DISTINCT user_query FROM traces "
        "WHERE user_query IS NOT NULL AND length(user_query) > 8"
    ).fetchall()
    conn.close()
    return [r[0].strip() for r in rows if r[0]][:limit]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=15, help="nº de queries a sondear")
    ap.add_argument("--repeat", type=int, default=3, help="ejecuciones por query (determinismo)")
    ap.add_argument("--fetch", type=int, default=300, help="$limit alto (réplica del experimento)")
    ap.add_argument("--low", type=int, default=10, help="$limit bajo (aísla H3 payload)")
    ap.add_argument("--timeout", type=int, default=45, help="timeout por intento (s)")
    args = ap.parse_args(argv)

    s = get_settings()
    # max_retries=1 → SIN retry interno: medimos cada intento crudo
    client = SecopClient(domain=s.datos_gov_domain, app_token=s.secop_app_token,
                         timeout=args.timeout, max_retries=1)
    print(f"app_token: {'sí' if s.secop_app_token else 'NO (¡throttling!)'}  |  "
          f"timeout={args.timeout}s  repeat={args.repeat}  fetch={args.fetch}/{args.low}\n")

    router = QueryRouter()
    builder = SoQLBuilder()
    ctx = {"today": date.today()}

    queries = [q for q in load_queries(args.max * 2)
               if router.parse(q).params.get("objeto")][:args.max]

    print(f"{'query':<34} {'ds':<4} {'#LK':>3} {'lim':>4} "
          f"{'lat (alto, xN)':<22} {'lat(bajo)':>9} {'veredicto'}")
    print("-" * 104)

    summary = {"timeout_alto": 0, "timeout_bajo": 0, "deterministic_slow": 0,
               "intermittent": 0, "payload_sensitive": 0, "total": 0, "with_like_slow": 0}

    for q in queries:
        params = compute_query_plan(router.parse(q).params, q, ctx).params
        ds_id = builder.dataset_id_for(params.get("dataset"))
        ds_short = "ctr" if ds_id == builder.CONTRATOS_DATASET else "prc"
        try:
            soql_hi = builder.build(ds_id, {**params, "limit": args.fetch})
            soql_lo = builder.build(ds_id, {**params, "limit": args.low})
        except Exception as exc:
            print(f"{q[:33]:<34} build ERR: {exc!s:.40}")
            continue

        feat = soql_features(soql_hi)
        # N ejecuciones con limit alto → determinismo vs varianza
        lats_hi, oks_hi = [], []
        for _ in range(args.repeat):
            lat, ok, _n = timed_query(client, ds_id, soql_hi)
            lats_hi.append(lat); oks_hi.append(ok)
            time.sleep(0.2)
        # 1 ejecución con limit bajo → ¿el payload importa? (H3)
        lat_lo, ok_lo, _ = timed_query(client, ds_id, soql_lo)

        ok_hi = sum(oks_hi)
        mn, mx = min(lats_hi), max(lats_hi)
        spread = mx - mn
        any_to_hi = ok_hi < args.repeat
        # clasificación
        verdicts = []
        if any_to_hi and ok_hi > 0:
            verdicts.append("INTERMITENTE(H1)"); summary["intermittent"] += 1
        elif ok_hi == 0:
            verdicts.append("SIEMPRE-TO"); summary["deterministic_slow"] += 1
        if ok_hi and ok_lo and (mx > 0) and lat_lo < mx * 0.5:
            verdicts.append("PAYLOAD(H3)"); summary["payload_sensitive"] += 1
        if ok_hi == args.repeat and spread < max(1.0, mn * 0.4) and mn > 8:
            verdicts.append("LENTO-DET(H2)")
        if feat["n_like"] >= 1 and (any_to_hi or mn > 8):
            summary["with_like_slow"] += 1
        if not verdicts:
            verdicts = ["ok"]

        summary["total"] += 1
        summary["timeout_alto"] += int(any_to_hi)
        summary["timeout_bajo"] += int(not ok_lo)
        lat_str = " ".join(f"{l:4.1f}{'x' if not o else ''}" for l, o in zip(lats_hi, oks_hi))
        print(f"{q[:33]:<34} {ds_short:<4} {feat['n_like']:>3} {feat['limit'] or 0:>4} "
              f"{lat_str:<22} {lat_lo:>7.1f}{'x' if not ok_lo else ' '} {','.join(verdicts)}")

    print("-" * 104)
    t = summary["total"] or 1
    print(f"\nResumen ({summary['total']} queries):")
    print(f"  timeout con limit alto:  {summary['timeout_alto']}/{t}")
    print(f"  timeout con limit bajo:  {summary['timeout_bajo']}/{t}  "
          f"(si ≪ alto → H3 payload; si ≈ alto → H2/H1 no es el payload)")
    print(f"  intermitentes (H1):      {summary['intermittent']}/{t}  "
          f"(misma query falla a veces → API saturada, NO la query)")
    print(f"  siempre-timeout (H2):    {summary['deterministic_slow']}/{t}  "
          f"(falla las {args.repeat} veces → query costosa)")
    print(f"  sensibles al payload:    {summary['payload_sensitive']}/{t}")
    print(f"  con LIKE Y lentas:       {summary['with_like_slow']}/{t}  "
          f"(correlación unaccent-LIKE ↔ lentitud → confirma H2)")
    print("\nLectura: 'x' tras una latencia = ese intento fue timeout. "
          "Compara la columna lat(bajo) vs lat(alto): si baja mucho, es payload (H3); "
          "si igual de lenta, es el WHERE (H2). Varianza alta entre los N intentos = H1.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
