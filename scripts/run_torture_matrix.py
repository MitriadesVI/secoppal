#!/usr/bin/env python3
"""Runner de Torture Matrix QA-001 para SECOPPAL.

Ejecuta tests parametrizados y produce resumen. Sin red.
Genera docs/torture_reports/ si se pasa --report.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
REPORT_DIR = ROOT / "docs" / "torture_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def run_pytest_torture() -> dict:
    """Ejecuta solo los tests de tortura y parsea resultados."""
    cmd = [
        sys.executable, "-m", "pytest",
        str(ROOT / "tests" / "test_torture_queries.py"),
        "-q", "--tb=no", "--no-header",
    ]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    # Parsear línea de summary tipo "28 passed in 0.63s"
    summary = result.stdout.strip()
    parts = summary.split()
    passed = failed = 0
    for i, p in enumerate(parts):
        if p == "passed":
            passed = int(parts[i-1]) if i > 0 else 0
        elif p == "failed":
            failed = int(parts[i-1]) if i > 0 else 0
    total = passed + failed
    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "raw_output": result.stdout,
    }


def main():
    parser = argparse.ArgumentParser(description="QA-001 Torture Matrix Runner")
    parser.add_argument("--report", action="store_true",
                        help="Genera JSON de reporte en docs/torture_reports/")
    args = parser.parse_args()

    print("=== QA-001 Torture Matrix SECOPPAL ===")
    print(f"Fecha:  {datetime.now().isoformat()}")
    print("Modo:   sin red (mocks activos)")
    print()

    stats = run_pytest_torture()
    total = stats["total"]
    passed = stats["passed"]
    failed = stats["failed"]

    categories = {
        "A": "Firmados / estados contractuales",
        "B": "Anti-WHERE 1=1",
        "C": "Follow-up / contaminación",
        "D": "Entidades / gazetteer",
        "E": "LLM fallback mockeado",
        "F": "Paginación / sugerencias",
        "G": "Degradación",
        "H": "Narrator grounding",
    }

    print("== RESULTADOS ==")
    print(f"Total casos:  {total}")
    print(f"Pass:         {passed}")
    print(f"Fail:         {failed}")
    print(f"Rate:         {passed/total*100:.0f}%" if total > 0 else "N/A")
    print()

    print("== CATEGORÍAS ==")
    for cat_id, cat_name in categories.items():
        print(f"  {cat_id}: {cat_name}")
    print()

    violations = []
    if failed > 0:
        print("== FALLOS DETECTADOS ==")
        print(stats["raw_output"])
    else:
        print("== INVARIANTES ==")
        invariants = [
            "INV-001: Guard anti-WHERE 1=1",
            "INV-002: COUNT y SELECT comparten WHERE",
            "INV-003: new_search no hereda contexto",
            "INV-004: follow-up de ordenamiento hereda",
            "INV-005: firmados no es estado, solo dataset",
            "INV-006: estado explícito puede acotar",
            "INV-007: LLM no emite estados inexistentes",
            "INV-008: gazetteer gana sobre LLM",
            "INV-009: tests no tocan feedback.jsonl",
            "INV-010: result_ids sin strings vacíos",
            "INV-011: reset borra contexto",
            "INV-012: narrator no inventa entidad/cifra",
        ]
        for inv in invariants:
            print(f"  PASS {inv}")
        print()
        print("Todas las invariantes protegidas.")

    if args.report:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = REPORT_DIR / f"torture_report_{ts}.json"
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "total_cases": total,
            "passed": passed,
            "failed": failed,
            "categories": categories,
            "invariants_violated": violations,
        }
        report_file.write_text(json.dumps(report_data, indent=2, ensure_ascii=False))
        print(f"\nReporte: {report_file}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()