#!/usr/bin/env python3
"""run_dialogue_corpus.py — Runner for dialogue_policy_corpus.yaml.

Validates QueryPlan output against expected_params, signals, and flags.
Usage:
    python scripts/run_dialogue_corpus.py --mode all
    python scripts/run_dialogue_corpus.py --category city_as_scope
    python scripts/run_dialogue_corpus.py --severity HIGH
    python scripts/run_dialogue_corpus.py --id CITY-ASOBJ-001
    python scripts/run_dialogue_corpus.py --mode all --json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

# Ensure project root is on sys.path for 'app' imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml

from app.core.query_plan import compute_query_plan

CORPUS_PATH = Path(__file__).parent.parent / "tests" / "fixtures" / "dialogue_policy_corpus.yaml"


def load_corpus() -> dict:
    return yaml.safe_load(CORPUS_PATH.read_text())


def validate_case(case: dict) -> dict:
    """Run a single corpus case through compute_query_plan and validate."""
    query = case["query"]
    should_execute = case["should_execute"]
    expected_params = case.get("expected_params", {})
    expected_signals = case.get("expected_signals", [])
    expected_signals_not = case.get("expected_signals_not", [])
    clarification_contains = case.get("clarification_contains")

    # Build minimal params from query so city/filler detection work.
    # The dialogue_policy's city_mention_as_topic and discourse_filler
    # detectors read from params["objeto"].
    # Normalize tildes like the pipeline does (normalize_text).
    import unicodedata
    def _norm(t: str) -> str:
        nfkd = unicodedata.normalize('NFKD', t.lower())
        return ''.join(c for c in nfkd if not unicodedata.combining(c))
    tokens = re.split(r"[\s,;.!¿?¡]+", query)
    tokens = [_norm(t) for t in tokens if t]
    params = {"objeto": tokens}

    # For negative tests: pre-set ciudad to prevent city_mention_as_topic.
    # Simulates query_router behavior which extracts cities before plan_query.
    from app.core.entity_resolver import GAZETTEER_CIUDADES
    # Only pre-set ciudad for negative tests (simulates query_router behavior)
    if case.get("category") == "negative":
        for token in tokens:
            if token in GAZETTEER_CIUDADES:
                params["ciudad"] = GAZETTEER_CIUDADES[token]
                break

    # For non_query_followup tests, inject prev_params + turn context
    ctx: dict = {"today": date(2026, 5, 21)}
    if case.get("category") == "non_query_followup":
        ctx["prev_params"] = {"objeto": ["mantenimiento"], "departamento_resolved": "Bolívar"}
        ctx["turn_index"] = 2

    # For number_phrase_scrub, inject a value so the signal fires
    if case.get("category") == "number_phrase_scrub":
        params["valor_max"] = 25_000_000

    if case["id"] == "NEG-001":
        import sys as _sys
    plan = compute_query_plan(
        params=params,
        raw_query=query,
        context=ctx,
        enabled_rules=None,
    )
    if case["id"] == "NEG-001":
        import sys as _sys

    failures: list[str] = []

    # Check should_execute
    if plan.should_execute != should_execute:
        failures.append(
            f"should_execute: expected {should_execute}, got {plan.should_execute}"
        )
        return {"id": case["id"], "status": "FAIL", "failures": failures}

    params = plan.params

    # Check expected_params
    if "ciudad" in expected_params:
        actual = params.get("ciudad")
        exp = expected_params["ciudad"]
        if actual != exp:
            failures.append(f"ciudad: expected {exp!r}, got {actual!r}")

    if "intent_type" in expected_params:
        actual = params.get("intent_type")
        exp = expected_params["intent_type"]
        if actual != exp:
            failures.append(f"intent_type: expected {exp!r}, got {actual!r}")

    if "estado_family" in expected_params:
        actual = params.get("estado_family")
        exp = expected_params["estado_family"]
        if actual != exp:
            failures.append(f"estado_family: expected {exp!r}, got {actual!r}")

    if "fecha_desde_starts_with" in expected_params:
        actual = params.get("fecha_desde", "")
        exp = expected_params["fecha_desde_starts_with"]
        if not (actual or "").startswith(exp):
            failures.append(f"fecha_desde: expected starts with {exp!r}, got {actual!r}")

    if "valor_max" in expected_params:
        actual = params.get("valor_max")
        exp = expected_params["valor_max"]
        if actual != exp:
            failures.append(f"valor_max: expected {exp}, got {actual}")

    if "valor_min" in expected_params:
        actual = params.get("valor_min")
        exp = expected_params["valor_min"]
        if actual != exp:
            failures.append(f"valor_min: expected {exp}, got {actual}")

    if "entidad_resolved" in expected_params:
        actual = params.get("entidad_resolved")
        exp = expected_params["entidad_resolved"]
        if actual != exp:
            failures.append(f"entidad_resolved: expected {exp!r}, got {actual!r}")

    # Check objeto_contains
    if "objeto_contains" in expected_params:
        objeto = [t.lower() for t in params.get("objeto", [])]
        for tok in expected_params["objeto_contains"]:
            if tok.lower() not in objeto:
                failures.append(f"objeto_contains: '{tok}' not in {objeto}")

    # Check objeto_not_contains
    if "objeto_not_contains" in expected_params:
        objeto = [t.lower() for t in params.get("objeto", [])]
        for tok in expected_params["objeto_not_contains"]:
            if tok.lower() in objeto:
                failures.append(f"objeto_not_contains: '{tok}' found in {objeto}")

    # Check clarification_contains
    if clarification_contains:
        cq = plan.clarification_question or ""
        if clarification_contains.lower() not in cq.lower():
            failures.append(
                f"clarification_contains: '{clarification_contains}' not in '{cq[:80]}'"
            )

    # Check expected_signals
    if expected_signals:
        signal_kinds = {s.kind for s in plan.signals}
        for sig in expected_signals:
            if sig not in signal_kinds:
                failures.append(f"expected_signal '{sig}' not in {signal_kinds}")

    # Check expected_signals_not
    if expected_signals_not:
        signal_kinds = {s.kind for s in plan.signals}
        for sig in expected_signals_not:
            if sig in signal_kinds:
                failures.append(f"unexpected_signal '{sig}' found in {signal_kinds}")

    if failures:
        return {"id": case["id"], "status": "FAIL", "failures": failures}
    return {"id": case["id"], "status": "PASS", "failures": []}


def main():
    parser = argparse.ArgumentParser(description="Run dialogue policy corpus.")
    parser.add_argument("--mode", default="all", choices=["all"])
    parser.add_argument("--category", default=None)
    parser.add_argument("--severity", default=None)
    parser.add_argument("--id", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    corpus = load_corpus()
    cases = corpus["cases"]

    if args.category:
        cases = [c for c in cases if c.get("category") == args.category]
    if args.severity:
        cases = [c for c in cases if c.get("severity") == args.severity]
    if args.id:
        cases = [c for c in cases if c["id"] == args.id]

    results = [validate_case(c) for c in cases]
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    total = len(results)

    # Category breakdown
    by_cat: dict[str, dict] = {}
    for c, r in zip(cases, results):
        cat = c.get("category", "unknown")
        if cat not in by_cat:
            by_cat[cat] = {"total": 0, "passed": 0, "failed": 0}
        by_cat[cat]["total"] += 1
        if r["status"] == "PASS":
            by_cat[cat]["passed"] += 1
        else:
            by_cat[cat]["failed"] += 1

    if args.json:
        output = {
            "total": total,
            "passed": passed,
            "failed": failed,
            "by_category": {
                cat: {
                    "total": v["total"],
                    "passed": v["passed"],
                    "failed": v["failed"],
                    "rate": round(v["passed"] / v["total"] * 100, 1) if v["total"] else 0,
                }
                for cat, v in sorted(by_cat.items())
            },
            "failures": [r for r in results if r["status"] == "FAIL"],
        }
        print(json.dumps(output, indent=2, ensure_ascii=False))
    else:
        print(f"Summary: {passed}/{total} PASS, {failed} FAIL")
        for cat, v in sorted(by_cat.items()):
            rate = round(v["passed"] / v["total"] * 100, 1) if v["total"] else 0
            print(f"  {cat}: {v['passed']}/{v['total']} ({rate}%)")
        for r in results:
            if r["status"] == "FAIL":
                print(f"\nFAIL {r['id']}:")
                for f in r["failures"]:
                    print(f"  - {f}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
