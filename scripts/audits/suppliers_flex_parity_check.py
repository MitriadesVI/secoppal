#!/usr/bin/env python
"""Paridad BASE vs AFTER sobre el corpus §3, por (case_id, mode).

Clasifica cada diferencia:
- EXPECTED_V1_1_DELTA  el corte interno pasa de 5 a 6 (testigo de empate §12);
- REGRESSION           outcome, ruta, llamadas o comprador cambian;
- HARNESS_DIFFERENCE   sólo cuando el propio registro lo demuestra.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys


def load(path: str) -> dict[tuple[str, str], dict]:
    rows = [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines()]
    return {(row["case_id"], row["mode"]): row for row in rows}


def limits(row: dict) -> list[str]:
    return [
        call["soql"].split("LIMIT ")[-1].split()[0]
        for call in row.get("calls", [])
        if "LIMIT" in call.get("soql", "")
    ]


def buyer(row: dict) -> str | None:
    return (row.get("view_header") or {}).get("display_name")


def main() -> int:
    base = load(sys.argv[1])
    after = load(sys.argv[2])
    deltas: list[tuple[str, str, str, str, str]] = []
    for key, before in sorted(base.items()):
        current = after.get(key)
        if current is None:
            # Un par de BASE sin contraparte en AFTER es una regresión, no una
            # nota: la paridad se mide entre pares (ZEUS-2 lo señaló).
            deltas.append((*key, "REGRESSION", "par presente", "sin contraparte"))
            continue
        if current["outcome"] != before["outcome"]:
            deltas.append((*key, "REGRESSION", before["outcome"], current["outcome"]))
        if current["route_reason"] != before["route_reason"]:
            deltas.append((*key, "REGRESSION", before["route_reason"], current["route_reason"]))
        before_calls, after_calls = len(before["calls"]), len(current["calls"])
        if after_calls != before_calls:
            if (before_calls, after_calls) == (2, 1):
                # Métrica explícita (§10): el ranking que no se pidió ya no se
                # consulta. Firma exacta del delta V1.1, en b04/b05.
                deltas.append((*key, "EXPECTED_V1_1_DELTA", f"{before_calls} calls", f"{after_calls} call"))
            else:
                deltas.append((*key, "REGRESSION", before_calls, after_calls))
        if before.get("view_header") is None:
            # El baseline viejo se grabó antes de que el probe registrara el
            # header; solo es una diferencia si el otro lado sí tiene algo.
            if buyer(current) is not None:
                deltas.append((*key, "HARNESS_DIFFERENCE", "sin header en base", buyer(current)))
        elif buyer(current) != buyer(before):
            deltas.append((*key, "REGRESSION", buyer(before), buyer(current)))
        before_limits, after_limits = sorted(limits(before)), sorted(limits(current))
        if before_limits != after_limits:
            if before_limits and after_limits and all(
                int(current_limit) == int(before_limit) + 1
                for before_limit, current_limit in zip(before_limits, after_limits)
            ):
                deltas.append((*key, "EXPECTED_V1_1_DELTA", before_limits, after_limits))
            else:
                deltas.append((*key, "REGRESSION", before_limits, after_limits))
    regressions = [delta for delta in deltas if delta[2] == "REGRESSION"]
    print(f"pares comparados: {len(base)}  |  deltas: {len(deltas)}")
    for case_id, mode, kind, before, after in deltas:
        print(f"  {case_id} {mode:<18} {kind:<20} base={before} after={after}")
    print(f"\nREGRESIONES: {len(regressions)}")
    print("PARITY: " + ("GO" if not regressions else "NO_GO"))
    return 0 if not regressions else 1


if __name__ == "__main__":
    raise SystemExit(main())
