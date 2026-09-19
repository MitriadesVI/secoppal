#!/usr/bin/env python
"""Resumen compacto de un JSONL del probe: sin SQLs completos ni respuestas enteras."""
import json
import sys

path = sys.argv[1]
wanted = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else None
show_response = "--response" in sys.argv

for line in open(path, encoding="utf-8"):
    row = json.loads(line)
    if wanted and row["case_id"] not in wanted:
        continue
    print(f"== {row['case_id']} [{row['mode']}] {row['outcome']} :: {row['route_reason']}")
    for call in row.get("calls", []):
        soql = " ".join(call["soql"].split())
        print(f"   call {call['dataset']}: ...{soql[-160:]}")
    ranking = (row.get("view_suppliers") or {}).get("ranking")
    print(f"   ranking: {ranking}")
    if show_response:
        print("   response:")
        for text_line in row.get("response", "").splitlines():
            print(f"     | {text_line[:120]}")
