#!/usr/bin/env python3
"""Evalúa el JSONL de dogfood: recall de referencias, vacíos, latencias, clarificaciones."""
import json
import sys
from pathlib import Path

path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("artifacts/dogfood/dogfood_dogfood_20260627.jsonl")
recs = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]

by_suite = {}
empties = []
clarifs = []
slow = []
ref_checks = []

for r in recs:
    s = r.get("summary", {})
    suite = r["suite_id"]
    by_suite.setdefault(suite, {"turns": 0, "empty": 0, "clarif": 0})
    by_suite[suite]["turns"] += 1

    total = s.get("total_count")
    ret = s.get("result_count_returned")
    clar = s.get("needs_clarification")
    ms = r.get("elapsed_ms", 0)

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
