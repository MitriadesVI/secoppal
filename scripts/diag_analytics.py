#!/usr/bin/env python3
"""Fase 0 — Auditoría analítica contra el backend REAL de datos.gov.co.

Read-only. NO toca producción. Responde tres preguntas que el plan necesita antes
de construir nada:

  1. ¿Cuáles de los 6 builders analíticos del soql_builder generan SoQL que el
     backend ACEPTA? (hoy los tests solo verifican el string, no tocan red)
  2. ¿Soporta el backend sintaxis avanzada — window functions (sum() OVER()) y
     comparativa multi-entidad (IN (...) GROUP BY)? (define viabilidad de Fase 3)
  3. Semántica de VALOR: cobertura (% no-nulo) y magnitud de valor_del_contrato vs
     valor_pagado vs valor_pendiente_de_ejecucion → recomendar cuál usar.

Robusto a H1 (intermitencia de datos.gov.co ya diagnosticada): cada consulta se
reintenta con backoff para no confundir "SoQL inválido" con "red caída".

Uso:
    .venv/bin/python scripts/diag_analytics.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import get_settings
from app.core.analytics import build_aggregate_sum_query
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder

DS_CONTRATOS = "jbjy-vk9h"
VALUE_FIELDS = ["valor_del_contrato", "valor_pagado", "valor_pendiente_de_ejecucion"]


def run_soql(client: SecopClient, ds_id: str, soql: str, *, retries: int = 4) -> tuple[str, list[dict]]:
    """Ejecuta SoQL con retry/backoff. Distingue red (H1) de SoQL inválido.
    Devuelve (estado, filas). estado ∈ {OK, INVALIDO, NO-RED}."""
    last_exc = ""
    for attempt in range(retries):
        try:
            rows = client.aggregate(ds_id, soql)
            return "OK", rows
        except Exception as exc:
            msg = str(exc)
            last_exc = msg
            # 400 / bad request = SoQL que el backend rechaza (determinista, no reintentar)
            if "400" in msg or "Bad Request" in msg or "invalid" in msg.lower():
                return "INVALIDO", []
            # timeout / conexión = H1, reintentar con backoff
            time.sleep(1.5 * (attempt + 1))
    return f"NO-RED ({last_exc[:30]})", []


def main() -> int:
    s = get_settings()
    client = SecopClient(domain=s.datos_gov_domain, app_token=s.secop_app_token,
                         timeout=40, max_retries=1)
    b = SoQLBuilder()
    ds = DS_CONTRATOS
    params = {"objeto": ["mantenimiento"], "departamento_resolved": "Antioquia"}
    print(f"Dataset: {ds} (contratos)  |  scope de prueba: {params}\n")

    # ── 1. Los 6 builders existentes ───────────────────────────────────────────
    print("=" * 72)
    print("1. BUILDERS ANALÍTICOS (¿generan SoQL que el backend acepta?)")
    print("=" * 72)
    builders = [
        ("aggregate_sum",  lambda: build_aggregate_sum_query(b, ds, params)),
        ("top_entities",   lambda: b.build_top_entities(ds, params)),
        ("value_stats",    lambda: b.build_value_stats(ds, params)),
        ("top_modalities", lambda: b.build_top_modalities(ds, params)),
        ("date_range",     lambda: b.build_date_range(ds, params)),
        ("temporal_dist",  lambda: b.build_temporal_dist(ds, params)),
    ]
    print(f"{'builder':<16} {'estado':<10} muestra")
    print("-" * 72)
    for name, fn in builders:
        try:
            soql = fn()
        except Exception as exc:
            print(f"{name:<16} {'BUILD-ERR':<10} {exc!s:.45}")
            continue
        estado, rows = run_soql(client, ds, soql)
        sample = str(rows[0])[:48] if rows else "∅"
        print(f"{name:<16} {estado:<10} {sample}")

    # ── 2. Sintaxis avanzada (Fase 3) ──────────────────────────────────────────
    print("\n" + "=" * 72)
    print("2. SINTAXIS AVANZADA (¿viable para entes de control?)")
    print("=" * 72)
    advanced = [
        ("window OVER()",
         "SELECT departamento, count(*) AS n, "
         "count(*) / sum(count(*)) OVER () AS pct "
         "GROUP BY departamento ORDER BY n DESC LIMIT 3"),
        ("multi-entidad IN",
         "SELECT departamento, count(*) AS n "
         "WHERE departamento IN ('Antioquia','Valle del Cauca') "
         "GROUP BY departamento"),
        ("having",
         "SELECT departamento, count(*) AS n "
         "GROUP BY departamento HAVING count(*) > 100000 LIMIT 3"),
    ]
    print(f"{'feature':<18} {'estado':<10} muestra")
    print("-" * 72)
    for name, soql in advanced:
        estado, rows = run_soql(client, ds, soql)
        sample = str(rows[0])[:46] if rows else "∅"
        print(f"{name:<18} {estado:<10} {sample}")

    # ── 3. Semántica de valor ──────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("3. SEMÁNTICA DE VALOR (cobertura y magnitud por campo, universo Antioquia)")
    print("=" * 72)
    where = "departamento = 'Antioquia'"
    estado, rows = run_soql(client, ds, f"SELECT count(*) AS n WHERE {where}")
    universo = int(rows[0]["n"]) if estado == "OK" and rows else 0
    print(f"Universo (Antioquia): {universo:,} contratos\n")
    print(f"{'campo':<32} {'no-nulo':<18} {'suma'}")
    print("-" * 72)
    for f in VALUE_FIELDS:
        soql = (f"SELECT count({f}) AS n_nonnull, sum({f}) AS total "
                f"WHERE {where}")
        estado, rows = run_soql(client, ds, soql)
        if estado != "OK" or not rows:
            print(f"{f:<32} {estado}")
            continue
        nn = int(rows[0].get("n_nonnull", 0) or 0)
        total = float(rows[0].get("total", 0) or 0)
        pct = (nn / universo * 100) if universo else 0
        print(f"{f:<32} {nn:>9,} ({pct:4.0f}%)   ${total/1e12:.1f} B (10^12)")

    print("\nLectura: campo con alta cobertura (%) y suma coherente = candidato a "
          "semántica de valor. Baja cobertura = huecos de datos (mala UX si se usa).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
