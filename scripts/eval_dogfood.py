#!/usr/bin/env python3
"""Evalúa el JSONL de dogfood: recall de referencias, vacíos, latencias, clarificaciones."""
import json
import sys
from collections import defaultdict
from pathlib import Path

path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("artifacts/dogfood/dogfood_dogfood_20260627.jsonl")
recs = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]

by_suite = {}
empties = []
clarifs = []
slow = []
ref_checks = []
waterfalls = []
outcome_counts = {}
false_clarifs = []
stage_timings = defaultdict(list)
http_durations = []

for r in recs:
    s = r.get("summary", {})
    suite = r["suite_id"]
    by_suite.setdefault(suite, {"turns": 0, "empty": 0, "clarif": 0})
    by_suite[suite]["turns"] += 1

    total = s.get("total_count")
    ret = s.get("result_count_returned")
    clar = s.get("needs_clarification")
    ms = r.get("elapsed_ms", 0)

    # Etapa 4: outcome bidimensional. Si está presente, manda sobre needs_clarification.
    outcome = s.get("outcome") or r.get("outcome") or ""
    oreason = s.get("outcome_reason") or r.get("outcome_reason") or ""
    outcome_counts[outcome or "(sin outcome)"] = outcome_counts.get(outcome or "(sin outcome)", 0) + 1
    # Falsa clarificación: el sistema viejo lo mostraría como menú, pero es un vacío.
    if clar and outcome == "empty":
        false_clarifs.append((r, oreason))

    if clar:
        by_suite[suite]["clarif"] += 1
        clarifs.append(r)
    elif not total:
        by_suite[suite]["empty"] += 1
        empties.append(r)

    if ms and ms > 15000:
        slow.append((ms, r))

    kr = r.get("known_reference")
    if kr:
        blob = json.dumps(r.get("result", {}), ensure_ascii=False) + s.get("response", "")
        ref = kr.get("reference", "")
        uid = kr.get("notice_uid", "")
        hit_ref = ref and ref in blob
        hit_uid = uid and uid in blob
        ref_checks.append((r, ref, uid, hit_ref, hit_uid))

    # Etapa 5: timings por etapa + métricas HTTP (si la corrida los trae).
    res = r.get("result", {}) or {}
    tm = res.get("timings_ms") or s.get("timings_ms") or {}
    for k, v in tm.items():
        if isinstance(v, (int, float)):
            stage_timings[k].append(v)
    for m in (res.get("http_metrics") or []):
        if isinstance(m.get("duration_ms"), (int, float)):
            http_durations.append(m["duration_ms"])

    # Etapa 3: auditoría diferencial de recall. Se persiste cuando el path de
    # zero-results la engancha (Etapa 4); aquí solo se reporta si está presente.
    wf = s.get("predicate_waterfall") or r.get("predicate_waterfall")
    if wf:
        waterfalls.append((r, wf, s.get("killer_predicate") or r.get("killer_predicate")))

print("=" * 70)
print("RESUMEN POR SUITE")
print("=" * 70)
print(f"{'suite':28} {'turns':>5} {'vacíos':>7} {'clarif':>7}")
for k, v in by_suite.items():
    print(f"{k:28} {v['turns']:>5} {v['empty']:>7} {v['clarif']:>7}")

print("\n" + "=" * 70)
print("RECALL DE REFERENCIAS CONOCIDAS (known_reference)")
print("=" * 70)
for r, ref, uid, hr, hu in ref_checks:
    mark = "✅ HIT" if (hr or hu) else "❌ MISS"
    print(f"{mark}  ref={ref} uid={uid}")
    print(f"        query: {r['query'][:80]}")
    print(f"        total={r['summary'].get('total_count')} returned={r['summary'].get('result_count_returned')}")

print("\n" + "=" * 70)
print(f"RESULTADOS VACÍOS (total_count=0): {len(empties)}")
print("=" * 70)
for r in empties:
    print(f"  [{r['suite_id']}/{r['scenario_id']} t{r['turn_index']}] {r['query'][:75]}")
    if r.get("expected_notes"):
        print(f"        esperado: {r['expected_notes'][:90]}")

print("\n" + "=" * 70)
print(f"PIDIÓ CLARIFICACIÓN: {len(clarifs)}")
print("=" * 70)
for r in clarifs:
    print(f"  [{r['suite_id']}/{r['scenario_id']} t{r['turn_index']}] {r['query'][:75]}")

print("\n" + "=" * 70)
print(f"LATENCIA: turnos >15s = {len(slow)}")
print("=" * 70)
mss = sorted([r.get("elapsed_ms", 0) for r in recs])
n = len(mss)
print(f"  p50={mss[n//2]/1000:.1f}s  p90={mss[int(n*0.9)]/1000:.1f}s  max={mss[-1]/1000:.1f}s")
for ms, r in sorted(slow, reverse=True)[:8]:
    print(f"  {ms/1000:5.1f}s  [{r['suite_id']}/{r['scenario_id']} t{r['turn_index']}] {r['query'][:55]}")

print("\n" + "=" * 70)
print("INSTRUMENTACIÓN — timings por etapa (Etapa 5)")
print("=" * 70)
if not stage_timings:
    print("  (sin timings_ms — corrida previa a Etapa 5)")
for stage, vals in sorted(stage_timings.items()):
    v = sorted(vals)
    n = len(v)
    p50 = v[n // 2]
    p90 = v[min(int(n * 0.9), n - 1)]
    print(f"  {stage:22} p50={p50/1000:6.2f}s  p90={p90/1000:6.2f}s  max={v[-1]/1000:6.2f}s  n={n}")
if http_durations:
    plateaus = {f"~{b}s": sum(1 for d in http_durations if abs(d - b * 1000) < 2000) for b in (15, 30, 45, 60)}
    print(f"  HTTP ops={len(http_durations)}  mesetas (±2s, posible retry/timeout): "
          + " ".join(f"{k}={v}" for k, v in plateaus.items() if v))

print("\n" + "=" * 70)
print("OUTCOMES (Etapa 4: bidimensional)")
print("=" * 70)
for k, v in sorted(outcome_counts.items(), key=lambda kv: -kv[1]):
    print(f"  {k:14} {v}")
print(f"\n  Falsas clarificaciones (clar=True pero outcome=empty): {len(false_clarifs)}")
for r, oreason in false_clarifs:
    print(f"    [{r['suite_id']}/{r['scenario_id']} t{r['turn_index']}] {r['query'][:55]} → {oreason}")

print("\n" + "=" * 70)
print(f"AUDITORÍA DE RECALL (predicate waterfall): {len(waterfalls)}")
print("=" * 70)
if not waterfalls:
    print("  (sin waterfalls persistidos — se cablea en Etapa 4; usa build_predicate_waterfall on-demand)")
for r, wf, killer in waterfalls:
    print(f"  [{r['suite_id']}/{r['scenario_id']} t{r['turn_index']}] {r['query'][:60]}")
    for step in wf:
        mark = "✗ KILLER" if step.get("killer_predicate") else ("·" if step.get("exists") else "✗")
        cnt = f" count={step['count']}" if step.get("count") is not None else ""
        print(f"        {mark:9} {step.get('predicate'):10} exists={step.get('exists')}{cnt}")
    print(f"        → killer_predicate: {killer}")
