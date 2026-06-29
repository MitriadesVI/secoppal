"""Análisis del perfil H4 vs H2 — gate de aceptación del asesor.

Mide: p50/p90/max, fallbacks intentados/exitosos/usados, filas recuperadas, inconclusas
sin filas (12→?), presentacional vs semántico, 0 falsos EMPTY, 0 rankings falsos, y la
tabla de inspección manual de TODOS los turnos que usaron fallback (SoQL principal vs
fallback, filtros preservados, respuesta visible).

Uso:
    .venv/bin/python scripts/analyze_h4_profile.py \
        artifacts/dogfood/dogfood_profile_h4_01.jsonl \
        artifacts/dogfood/dogfood_profile_h2_01.jsonl
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter


def load(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def pct(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    k = min(len(s) - 1, int(round((p / 100) * (len(s) - 1))))
    return s[k]


def where_of(soql):
    m = re.search(r" WHERE (.+?)(?: ORDER BY | LIMIT )", soql or "")
    return m.group(1) if m else ""


# Frases que delatan una afirmación de orden/ranking en la respuesta visible.
RANKING_CLAIMS = ["más recientes", "mas recientes", "de mayor valor", "más caro", "mas caro",
                  "el más", "el mas", "los primeros", "el mayor"]


def latency_block(turns, label):
    tot = [t["elapsed_ms"] / 1000 for t in turns if t.get("elapsed_ms")]
    over15 = sum(1 for v in tot if v > 15)
    print(f"\n## Latencia {label} (n={len(tot)})")
    print(f"  p50={pct(tot,50):.1f}s  p90={pct(tot,90):.1f}s  p95={pct(tot,95):.1f}s  "
          f"max={max(tot):.1f}s  turnos>15s={over15}")


def main():
    h4_path = sys.argv[1] if len(sys.argv) > 1 else "artifacts/dogfood/dogfood_profile_h4_01.jsonl"
    h2_path = sys.argv[2] if len(sys.argv) > 2 else "artifacts/dogfood/dogfood_profile_h2_01.jsonl"
    h4 = load(h4_path)
    h2 = load(h2_path)

    print(f"# H4 ({h4_path}) vs H2 ({h2_path})")
    latency_block(h2, "H2 (antes)")
    latency_block(h4, "H4 (después)")

    # ── Outcomes + inconclusas sin filas ─────────────────────────────────────
    def oc(turns):
        c = Counter((t["result"].get("outcome"), t["result"].get("outcome_reason")) for t in turns)
        return c
    print("\n## Outcomes")
    print("  H2:", dict(oc(h2)))
    print("  H4:", dict(oc(h4)))
    incon_h2 = sum(1 for t in h2 if (t["result"].get("outcome"), t["result"].get("outcome_reason")) == ("error", "fetch_timeout"))
    incon_h4 = sum(1 for t in h4 if (t["result"].get("outcome"), t["result"].get("outcome_reason")) == ("error", "fetch_timeout"))
    print(f"\n## Inconclusas sin filas (objetivo 12→≤4):  H2={incon_h2}  →  H4={incon_h4}")

    # ── Falsos EMPTY (filas perdidas marcadas como vacío) ────────────────────
    false_empty = sum(1 for t in h4 if t["result"].get("outcome") == "empty"
                      and (t["result"].get("result_row_count") or 0) > 0)
    print(f"## Falsos EMPTY H4 (debe ser 0): {false_empty}")

    # ── Telemetría de fallback ───────────────────────────────────────────────
    attempted = [t for t in h4 if t["result"].get("fallback_attempted")]
    used = [t for t in h4 if t["result"].get("fallback_used")]
    print(f"\n## Fallback H4:  intentados={len(attempted)}  usados/exitosos={len(used)}")
    sem = sum(1 for t in h4 if t["result"].get("resolved_params", {}).get("ordering_signal"))
    print(f"  turnos con orden semántico (ordering_signal presente): {sem}")
    # Invariante crítico: NINGÚN turno semántico debió usar fallback.
    sem_fallback = [t for t in h4 if t["result"].get("fallback_used")
                    and t["result"].get("resolved_params", {}).get("ordering_signal")]
    print(f"  ⚠️ semántico CON fallback (debe ser 0): {len(sem_fallback)}")

    # ── Rankings falsos en la respuesta visible (muestra presentada como orden) ─
    false_rank = []
    for t in h4:
        r = t["result"]
        if r.get("result_order_guarantee") == "unordered_sample":
            resp = (r.get("response") or "").lower()
            hits = [c for c in RANKING_CLAIMS if c in resp]
            if hits:
                false_rank.append((t["turn_index"], t["scenario_id"], hits))
    print(f"## Rankings falsos sobre muestra (debe ser 0): {len(false_rank)}")
    for ti, sc, hits in false_rank:
        print(f"   [{ti}] {sc}: {hits}")

    # ── Gate direct_lookup: ningún turno de referencia > 15s ─────────────────
    dl_turns = [t for t in h4 if str(t["result"].get("route_reason", "")).startswith("direct_lookup")
                or str(t["result"].get("route_reason", "")) == "entity_ref_disambiguation"]
    dl_over15 = [t for t in dl_turns if (t.get("elapsed_ms") or 0) / 1000 > 15]
    print(f"\n## direct_lookup: {len(dl_turns)} turnos | >15s (debe ser 0): {len(dl_over15)}")
    for t in dl_turns:
        r = t["result"]
        attempts = r.get("search_attempts") or []
        statuses = [a["status"] for a in attempts]
        cut = any(a.get("skipped_due_to_budget") for a in attempts)
        print(f"  [{t['turn_index']}] {t['scenario_id'][:22]:22} {t['elapsed_ms']/1000:5.1f}s "
              f"probes={len(attempts)} ({statuses.count('completed')}ok/"
              f"{statuses.count('failed')}fail/{statuses.count('skipped')}skip) "
              f"budget_cut={cut} status={r.get('direct_lookup_status')}/{r.get('direct_lookup_reason')}")
        print(f"       resp: {(r.get('response') or '')[:110]}")

    # ── Inspección manual: TODOS los turnos con fallback usado ───────────────
    print("\n## Inspección manual — turnos con fallback usado")
    for t in used:
        r = t["result"]
        primary = next((s for s in [r.get("soql_query")] if s and "ORDER BY" in s), None)
        wsame = None
        # el SoQL en el result ya es el del fallback (sin ORDER BY); comparamos su WHERE
        # contra el WHERE que tendría la principal (mismo params) no está aquí, así que
        # reportamos lo trazado.
        print(f"\n[{t['turn_index']}] {t['scenario_id']} — {t['query'][:60]}")
        print(f"   transformaciones: {r.get('transformations_applied')}")
        print(f"   filtros_sustantivos_preservados: {r.get('substantive_filters_preserved')}")
        print(f"   result_order_guarantee: {r.get('result_order_guarantee')}  "
              f"filas: {r.get('result_row_count')}  total: {r.get('total_count')} "
              f"(approx={r.get('total_count_approximate')})")
        print(f"   fallback_query_status: {r.get('fallback_query_status')}  "
              f"elapsed_ms: {r.get('fallback_elapsed_ms')}")
        print(f"   WHERE(fallback): {where_of(r.get('soql_query'))[:140]}")
        print(f"   respuesta: {(r.get('response') or '')[:160]}")


if __name__ == "__main__":
    main()
