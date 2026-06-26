#!/usr/bin/env python3
"""Benchmark OFFLINE de integridad semántica de retrieval (Commit 1).

Carga el snapshot inmutable (tests/fixtures/phrase_retrieval/*_v*.jsonl) y mide,
por separado en development y holdout, aplicando un PREDICADO de validación:

  - candidate_recall    = core_positivos detectados / core_positivos
  - validator_precision = positivos detectados / (positivos + negativos detectados)
  - critical-variant gate: cada match_type crítico (singular/plural/intercalated/
    dirty_newline) debe PASAR; cada anchor_false_positive debe quedar RECHAZADO.

Modos:
  (sin flags)        report-only, SIEMPRE exit 0 — no rompe la suite principal.
  --enforce-gates    exit 1 si el holdout no cumple gates o falla un crítico.

Hoy el único predicado es `baseline_tokens` (enfoque actual de SECOPPAL). Es ROJO
a propósito: marca el piso. Cuando exista phrase_validator.py (Commit 4) se registra
como nuevo predicado y se compara, sin tocar red.

REGLA DE ORO: el holdout NO se usa para diseñar el validador; ni siquiera se miran
sus filas individuales mientras se ajusta el patrón. Solo se ejecuta para reportar.
"""
from __future__ import annotations

import argparse
import json
import sys
import unicodedata
from pathlib import Path

FIXDIR = Path(__file__).resolve().parents[1] / "tests/fixtures/phrase_retrieval"
RECALL_GATE = 0.95
PRECISION_GATE = 0.95
CRITICAL_TYPES = ("exact_singular", "exact_plural", "intercalated", "dirty_newline")


def deaccent_upper(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).upper()


def blob(row: dict) -> str:
    return deaccent_upper(f"{row.get('objeto', '')} {row.get('descripcion', '')}")


# ---- predicados (True = la fila ES "manual de contratación") ----
def baseline_tokens(row: dict) -> bool:
    """Enfoque ACTUAL de SECOPPAL: token plural suelto."""
    return "MANUALES" in blob(row)


PREDICATES = {"baseline_tokens": baseline_tokens}


def active_snapshot() -> Path:
    snaps = sorted(FIXDIR.glob("manual_contratacion_2024_v*.jsonl"))
    if not snaps:
        raise SystemExit("Falta el snapshot. Corré: python3 scripts/build_phrase_corpus.py")
    return snaps[-1]  # mayor versión


def metrics(rows: list[dict], predicate, split: str) -> dict:
    sub = [r for r in rows if r["split"] == split]
    core = [r for r in sub if r["label"] == "relevant" and r["action_class"] == "core"]
    neg = [r for r in sub if r["label"] == "irrelevant"]
    tp = sum(1 for r in core if predicate(r))
    fp = sum(1 for r in neg if predicate(r))
    recall = tp / len(core) if core else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    return {"core": len(core), "neg": len(neg), "tp": tp, "fp": fp,
            "fn": len(core) - tp, "recall": recall, "precision": precision}


def critical_gate(rows: list[dict], predicate) -> list[tuple[str, str, int, int]]:
    """Devuelve [(match_type, estado, ok, total)] para tipos críticos + anchor_fp."""
    out = []
    for mt in CRITICAL_TYPES:
        items = [r for r in rows if r["match_type"] == mt]
        ok = sum(1 for r in items if predicate(r))
        out.append((mt, "PASS" if items and ok == len(items) else "FAIL", ok, len(items)))
    fps = [r for r in rows if r["match_type"] == "anchor_false_positive"]
    rejected = sum(1 for r in fps if not predicate(r))
    out.append(("anchor_false_positive", "PASS" if fps and rejected == len(fps) else "FAIL", rejected, len(fps)))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--enforce-gates", action="store_true",
                    help="exit 1 si el holdout no cumple gates o falla un crítico (para CI tras la solución)")
    args = ap.parse_args()

    snap = active_snapshot()
    rows = [json.loads(l) for l in snap.read_text(encoding="utf-8").splitlines() if l.strip()]
    print(f"Snapshot: {snap.name}  ({len(rows)} filas)\n")

    holdout_ok = True
    for name, pred in PREDICATES.items():
        print(f"=== predicado: {name} ===")
        for split in ("dev", "holdout"):
            m = metrics(rows, pred, split)
            ok = m["recall"] >= RECALL_GATE and m["precision"] >= PRECISION_GATE
            print(f"  {split:8} core={m['core']:3} neg={m['neg']:3} | TP={m['tp']:3} FP={m['fp']:3} FN={m['fn']:3} | "
                  f"recall={m['recall']:6.2%} precision={m['precision']:6.2%}  [{'PASS' if ok else 'FAIL'}]")
            if split == "holdout" and not ok:
                holdout_ok = False
        print("  critical-variant gate:")
        crit_ok = True
        for mt, status, ok, total in critical_gate(rows, pred):
            print(f"    {mt:24} {status}  ({ok}/{total})")
            if status == "FAIL":
                crit_ok = False
        print()

    print(f"Gates: recall>=%.0f%% y precision>=%.0f%% en holdout; crítics PASS." % (RECALL_GATE*100, PRECISION_GATE*100))
    passed = holdout_ok and crit_ok
    if not args.enforce_gates:
        print("Modo report-only (exit 0). El baseline es ROJO a propósito: es el piso a superar.")
        return 0
    print("PASS" if passed else "FAIL — gates incumplidos (modo --enforce-gates).")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
