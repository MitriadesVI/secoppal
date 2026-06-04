#!/usr/bin/env python3
"""Eval harness over REAL rated queries (the precision ground truth).

Unlike ``run_query_corpus.py`` (synthetic queries → authored expectations),
this harness scores the pipeline against the queries users actually rated
👍/👎 in ``data/feedback.jsonl``. The labeled expectations live in
``tests/fixtures/rated_eval_v1.yaml`` and were authored by reading each
rating comment — they encode the *correct* behaviour, not the recorded one.

Two tiers:
    parser   — offline, fast, CI-safe. Checks parsed params (objeto purity,
               entity/dept resolution, value ranges, dataset, intent).
    pipeline — needs network / multi-turn context. Skipped unless --live.

The headline number is **parser-tier precision**: it goes up as bugs get
fixed and is guarded against regression. Pipeline-tier entries are tracked
but excluded from the offline score until labeled + run with --live.

Usage:
    python scripts/eval_rated.py                 # parser tier, all entries
    python scripts/eval_rated.py --tag range     # only one tag
    python scripts/eval_rated.py --regressions    # only entries we aim to fix
    python scripts/eval_rated.py --verbose        # show actual params on FAIL
    python scripts/eval_rated.py --sync           # refresh recorded snapshot from feedback.jsonl

Exit code: 0 = all selected parser-tier entries pass, 1 = at least one FAIL.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml not installed. `pip install pyyaml`.", file=sys.stderr)
    sys.exit(2)

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

EVAL_SET = REPO_ROOT / "tests" / "fixtures" / "rated_eval_v1.yaml"
# Fuente de verdad de ratings: SQLite (data/secoppal.db). El JSONL es un log
# append-only que NO se actualiza al calificar (rate() solo toca SQLite), por
# eso divergen — el .db tiene todas las calificadas y el .jsonl se queda atrás.
SECOPPAL_DB = REPO_ROOT / "data" / "secoppal.db"


# ── Param flattening / assertions (shared shape with run_query_corpus.py) ──

def _flatten(value: Any) -> list[str]:
    if value is None:
        return []
    out: list[str] = []
    if isinstance(value, (list, tuple)):
        for item in value:
            out.extend(_flatten(item))
    else:
        out.append(str(value).lower())
    return out


def _check_contains(actual: dict, key: str, needles: list[str]) -> list[str]:
    tokens = set(_flatten(actual.get(key)))
    # also expose composite tokens split on space/underscore
    for t in list(tokens):
        for part in t.replace("_", " ").split():
            tokens.add(part)
    fails = []
    for n in needles:
        nl = str(n).lower()
        if nl not in tokens and nl.replace(" ", "_") not in set(_flatten(actual.get(key))):
            fails.append(f"{key} should contain {n!r}, got {sorted(tokens)}")
    return fails


def _check_not_contains(actual: dict, key: str, blocklist: list[str]) -> list[str]:
    tokens = set(_flatten(actual.get(key)))
    fails = []
    for n in blocklist:
        if str(n).lower() in tokens:
            fails.append(f"{key} must NOT contain {n!r}, found in {sorted(tokens)}")
    return fails


def _check_eq(actual: dict, key: str, expected: Any) -> list[str]:
    got = actual.get(key)
    if got != expected:
        return [f"{key}: expected {expected!r}, got {got!r}"]
    return []


def _check_absent(actual: dict, keys: list[str]) -> list[str]:
    fails = []
    for k in keys:
        v = actual.get(k)
        if v not in (None, "", [], {}, False):
            fails.append(f"{k} should be absent, got {v!r}")
    return fails


def assert_expected(actual: dict, expected: dict) -> list[str]:
    """Evaluate one entry's ``expected`` block. Supported directives:

    <field>: value                — equality
    <field>_contains: [..]         — list membership (objeto, etc.)
    objeto_must_not_contain: [..]  — blocklist on objeto
    absent: [..]                   — these keys must be empty/missing
    """
    fails: list[str] = []
    for key, val in expected.items():
        if key == "objeto_must_not_contain":
            fails.extend(_check_not_contains(actual, "objeto", val))
        elif key == "absent":
            fails.extend(_check_absent(actual, val))
        elif key.endswith("_contains"):
            fails.extend(_check_contains(actual, key[: -len("_contains")], val))
        else:
            fails.extend(_check_eq(actual, key, val))
    return fails


# ── Result type ────────────────────────────────────────────────────────────

@dataclass
class EvalResult:
    eid: str
    tier: str
    tags: list[str]
    query: str
    passed: bool
    fails: list[str] = field(default_factory=list)
    actual: dict = field(default_factory=dict)
    recorded_rating: int | None = None
    is_regression: bool = False  # entry we aim to fix (was rated 0)


# ── Runner ──────────────────────────────────────────────────────────────────

def _compact(p: dict) -> dict:
    keep = {}
    for k in (
        "dataset", "entidad_resolved", "departamento_resolved", "ciudad",
        "objeto", "valor_min", "valor_max", "fecha_desde", "fecha_hasta",
        "intent_type", "estado_family", "ordering_signal",
    ):
        v = p.get(k)
        if v not in (None, "", [], {}, False):
            keep[k] = v
    return keep


def run_parser_entry(entry: dict, parser) -> EvalResult:
    query = entry["query"]
    parsed = parser.parse(query)
    actual = dict(parsed.params)
    fails = assert_expected(actual, entry.get("expected", {}) or {})
    return EvalResult(
        eid=entry["id"],
        tier=entry.get("tier", "parser"),
        tags=entry.get("tags", []) or [],
        query=query,
        passed=not fails,
        fails=fails,
        actual=_compact(actual),
        recorded_rating=entry.get("recorded_rating"),
        is_regression=entry.get("recorded_rating") == 0,
    )


# ── Reporting ────────────────────────────────────────────────────────────────

def print_report(results: list[EvalResult], verbose: bool) -> None:
    use_color = sys.stdout.isatty()

    def c(t, code):
        return f"\x1b[{code}m{t}\x1b[0m" if use_color else t

    failed = [r for r in results if not r.passed]
    passed = [r for r in results if r.passed]

    if failed:
        print("\nFAIL:")
        for r in failed:
            reg = c(" [regresión-objetivo]", "35") if r.is_regression else ""
            tags = f" {{{','.join(r.tags)}}}" if r.tags else ""
            print(f"  {c('FAIL', '31')} {r.eid}{tags}{reg} — {r.query[:70]!r}")
            for f in r.fails:
                print(f"      · {f}")
            if verbose:
                print(f"      actual: {json.dumps(r.actual, ensure_ascii=False, default=str)}")

    total = len(results)
    pct = (len(passed) / total * 100) if total else 0.0
    print(f"\nPrecisión (parser-tier): {len(passed)}/{total} = {pct:.0f}%")

    # Breakdown by tag
    by_tag: dict[str, list[int]] = {}
    for r in results:
        for t in r.tags:
            by_tag.setdefault(t, [0, 0])
            by_tag[t][1] += 1
            if r.passed:
                by_tag[t][0] += 1
    if by_tag:
        print("Por tag:")
        for t in sorted(by_tag):
            p, n = by_tag[t]
            mark = "✓" if p == n else " "
            print(f"  {mark} {t:<22} {p}/{n}")

    # Regression-target progress (entries that were rated 👎)
    regs = [r for r in results if r.is_regression]
    if regs:
        fixed = sum(1 for r in regs if r.passed)
        print(f"\nRegresiones-objetivo recuperadas (eran 👎): {fixed}/{len(regs)}")


# ── Sync: refresh recorded snapshot from feedback.jsonl ──────────────────────

def sync_recorded() -> int:
    """Print the rated queries from SQLite (source of truth) as YAML skeletons.

    Helper for authoring/extending the eval set — does NOT overwrite the
    curated file. Emits entries with id/query/recorded_rating/comment so a
    human can fill in the ``expected`` block. Ordered by rated_at so the
    RATED-NNN ids stay stable as new ratings are appended.
    """
    import sqlite3

    if not SECOPPAL_DB.exists():
        print(f"ERROR: {SECOPPAL_DB} not found", file=sys.stderr)
        return 2
    conn = sqlite3.connect(SECOPPAL_DB)
    rows = conn.execute(
        "SELECT user_query, followup, rating, rating_comment "
        "FROM traces WHERE rating IS NOT NULL ORDER BY rated_at"
    ).fetchall()
    conn.close()

    print(f"# {len(rows)} rated entries in {SECOPPAL_DB.name} (source of truth)\n")
    for i, (uq, followup, rating, comment) in enumerate(rows):
        q = (uq or "").replace("\t", " ").strip()
        print(f"  - id: RATED-{i:03d}")
        print(f"    query: {json.dumps(q, ensure_ascii=False)}")
        print(f"    recorded_rating: {rating}")
        if followup:
            print("    tier: pipeline   # follow-up: needs prev context")
        if comment:
            print(f"    comment: {json.dumps(comment.strip()[:160], ensure_ascii=False)}")
        print("    tags: []")
        print("    expected: {}")
        print()
    return 0


# ── Main ──────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Eval over real rated queries.")
    ap.add_argument("--tag", action="append", help="Filter by tag (repeatable).")
    ap.add_argument("--regressions", action="store_true", help="Only entries rated 👎 (fix targets).")
    ap.add_argument("--live", action="store_true", help="Also run pipeline-tier entries (network).")
    ap.add_argument("--verbose", action="store_true", help="Show actual params on FAIL.")
    ap.add_argument("--sync", action="store_true", help="Emit YAML skeletons from feedback.jsonl and exit.")
    ap.add_argument("--eval-set", type=Path, default=EVAL_SET)
    args = ap.parse_args(argv)

    if args.sync:
        return sync_recorded()

    if not args.eval_set.exists():
        print(f"ERROR: eval set not found at {args.eval_set}", file=sys.stderr)
        return 2

    data = yaml.safe_load(args.eval_set.read_text()) or {}
    entries = data.get("queries", []) or []

    # Tier filtering: parser tier by default; pipeline only with --live.
    selected = []
    for e in entries:
        tier = e.get("tier", "parser")
        if tier == "pipeline" and not args.live:
            continue
        if args.tag and not (set(args.tag) & set(e.get("tags", []) or [])):
            continue
        if args.regressions and e.get("recorded_rating") != 0:
            continue
        selected.append(e)

    if not selected:
        print("No entries matched the filters.")
        return 0

    from app.core.query_router import QueryRouter
    parser = QueryRouter()

    results = []
    for e in selected:
        if e.get("tier", "parser") == "pipeline":
            continue  # live pipeline checks: TODO (network/multi-turn)
        try:
            results.append(run_parser_entry(e, parser))
        except Exception as exc:
            results.append(EvalResult(
                eid=e.get("id", "?"), tier="parser", tags=e.get("tags", []),
                query=e.get("query", ""), passed=False,
                fails=[f"runner raised: {exc!r}"],
                recorded_rating=e.get("recorded_rating"),
                is_regression=e.get("recorded_rating") == 0,
            ))

    print_report(results, args.verbose)
    return 1 if any(not r.passed for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
