#!/usr/bin/env python
"""Verificador del dogfood: hard errors §18 contra el corpus, sin interpretación.

Lee el JSONL del probe y el corpus del contrato, y adjudica —por caso— los ocho
ceros duros del frente. Un caso sin expectativa servible (veto de otra
autoridad) sólo comprueba su cero de llamadas y su outcome.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

APP_DIR = Path(__file__).resolve().parents[3]

# El comprador que la consulta nombra, por token: el display acreditado de la
# vista debe contenerlo cuando el turno responde.
BUYER_BY_TOKEN = {
    "galapa": "GALAPA",
    "barranquilla": "BARRANQUILLA",
    "soledad": "SOLEDAD",
    "baranoa": "BARANOA",
}


def expected_buyer(query: str) -> str | None:
    folded = query.casefold()
    return next(
        (name for token, name in BUYER_BY_TOKEN.items() if token in folded), None,
    )


def buyer_drift(case: dict, row: dict) -> str | None:
    """El comprador observado, cuando el turno respondió, debe ser el pedido."""
    if row.get("outcome") != "ANSWER":
        return None
    expected = expected_buyer(case["query"])
    if expected is None:
        return None
    observed = (row.get("view_header") or {}).get("display_name", "")
    if expected in observed.upper():
        return None
    return f"WRONG_BUYER {case['id']} esperado {expected} observado {observed!r}"


def load_corpus(path: Path) -> dict[str, dict]:
    corpus = json.loads(path.read_text(encoding="utf-8"))
    return {case["id"]: case for case in corpus["cases"]}


def check(rows: list[dict], corpus: dict[str, dict], *, mode: str) -> tuple[list[str], dict]:
    errors: list[str] = []
    counters = {
        "cases": 0, "served": 0, "clarified": 0, "preexisting_veto": 0,
        "WRONG_BUYER": 0, "WRONG_METRIC": 0, "WRONG_LIMIT": 0,
        "SILENT_FILTER_LOSS": 0, "ACTOR_STOLEN": 0,
        "UNSUPPORTED_EXECUTED": 0, "FALSE_COMPLETE_RANKING": 0,
    }
    for row in rows:
        if row.get("mode") != mode:
            continue
        case = corpus.get(row["case_id"])
        if case is None:
            errors.append(f"{row['case_id']}: caso sin corpus")
            continue
        counters["cases"] += 1
        expected = case["expect"]
        veto = case.get("veto")
        drift = buyer_drift(case, row)
        if drift is not None:
            counters["WRONG_BUYER"] += 1
            errors.append(drift)
        # Un veto PREEXISTENTE adjudicado (idéntico en BASE y candidato) no es
        # un error semántico de este frente: se cuenta aparte y se exige que
        # tampoco haya ejecutado.
        if case.get("preexisting_veto"):
            counters["preexisting_veto"] += 1
            if row.get("calls"):
                counters["SILENT_FILTER_LOSS"] += 1
                errors.append(
                    f"SILENT_FILTER_LOSS {case['id']} ejecutó pese al veto preexistente "
                    f"{case['preexisting_veto']}"
                )
            continue
        served = row["view_suppliers"] is not None or row["outcome"] == "ANSWER"
        if expected["limit"] is None or veto in ("ACTOR",):
            if row["outcome"] == "ANSWER" and row["view_focus"] == "suppliers":
                if veto == "ACTOR":
                    counters["ACTOR_STOLEN"] += 1
                    errors.append(f"ACTOR_STOLEN {case['id']} {case['query']!r}")
                else:
                    counters["UNSUPPORTED_EXECUTED"] += 1
                    errors.append(f"UNSUPPORTED_EXECUTED {case['id']} {case['query']!r}")
            else:
                counters["clarified"] += 1
            if row.get("calls"):
                counters["SILENT_FILTER_LOSS"] += 1
                errors.append(
                    f"SILENT_FILTER_LOSS {case['id']} ejecutó {len(row['calls'])} llamadas "
                    f"({case['query']!r})"
                )
            continue
        if veto:
            counters["clarified"] += 1
            if row.get("calls"):
                counters["SILENT_FILTER_LOSS"] += 1
                errors.append(
                    f"SILENT_FILTER_LOSS {case['id']} ejecutó pese al veto {veto}"
                )
            continue
        # servible
        ranking = (row.get("view_suppliers") or {}).get("ranking")
        if ranking is None:
            counters["WRONG_METRIC"] += 1
            errors.append(f"WRONG_METRIC {case['id']} sin ranking ejecutado ({row['outcome']})")
            continue
        counters["served"] += 1
        if ranking["metric"] != expected["metric"]:
            counters["WRONG_METRIC"] += 1
            errors.append(
                f"WRONG_METRIC {case['id']} esperado {expected['metric']} obtenido {ranking['metric']}"
            )
        if ranking["limit"] != expected["limit"]:
            counters["WRONG_LIMIT"] += 1
            errors.append(
                f"WRONG_LIMIT {case['id']} esperado {expected['limit']} obtenido {ranking['limit']}"
            )
        jobs = 2 if expected["metric"] == "both" else 1
        if len(row.get("calls", [])) != jobs:
            counters["SILENT_FILTER_LOSS"] += 1
            errors.append(
                f"WRONG_CALL_COUNT {case['id']} esperado {jobs} obtenido {len(row.get('calls', []))}"
            )
        if ranking.get("rows") == {} and row.get("calls"):
            # Todas las llamadas fallaron y el texto no debe afirmar un ranking.
            if "No encontré proveedores" in row.get("response", ""):
                counters["FALSE_COMPLETE_RANKING"] += 1
                errors.append(f"FALSE_COMPLETE_RANKING {case['id']}")
    return errors, counters


def main() -> int:
    rows = [json.loads(line) for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()]
    corpus = load_corpus(Path(sys.argv[2]))
    mode = sys.argv[3] if len(sys.argv) > 3 else "frame_on"
    errors, counters = check(rows, corpus, mode=mode)
    print(json.dumps(counters, indent=2, ensure_ascii=False))
    if errors:
        print("\nHARD ERRORS:")
        for error in errors:
            print(f"  - {error}")
        print(f"\nNO_GO: {len(errors)} hard errors")
        return 1
    print("\nGO: 0 hard errors")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
