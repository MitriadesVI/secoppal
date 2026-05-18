#!/usr/bin/env python3
"""Regression harness for the SECOPPAL query corpus.

Reads tests/fixtures/query_corpus_v1.yaml, runs each entry against the
deterministic parser (and optionally the follow-up workflow), and reports
PASS/FAIL per entry.

Usage:
    python scripts/run_query_corpus.py [--mode {parser,workflow,all}]
                                       [--limit N]
                                       [--id CORPUS-R005]
                                       [--severity {critical,high,medium,smoke}]
                                       [--corpus path]
                                       [--verbose]
                                       [--show-pass]

Exit code:
    0 — all selected entries passed
    1 — at least one FAIL (used by CI)
    2 — usage / configuration error
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    print("ERROR: pyyaml not installed. `pip install pyyaml`.", file=sys.stderr)
    sys.exit(2)

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

DEFAULT_CORPUS = REPO_ROOT / "tests" / "fixtures" / "query_corpus_v1.yaml"

# ── Lazy imports (so --help works without app deps) ──────────────────────
def _load_parser():
    from app.core.query_router import QueryRouter
    return QueryRouter()


def _load_workflow_helpers():
    from app.core.query_router import QueryRouter
    from app.core.query_frame import frame_from_params
    from app.core.followup_engine import FollowupClassifier, FollowupMerger
    return QueryRouter(), frame_from_params, FollowupClassifier, FollowupMerger


# ── Result types ─────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    entry_id: str
    severity: str
    mode: str
    passed: bool
    fails: list[str] = field(default_factory=list)
    actual: dict = field(default_factory=dict)
    text: str = ""
    category: str = ""


# ── Assertion helpers ────────────────────────────────────────────────────

def _collect_topic_tokens(value: Any) -> list[str]:
    """Flatten a topic/objeto value (which may be list[str] or list[list[str]])."""
    if value is None:
        return []
    out: list[str] = []
    if isinstance(value, (list, tuple)):
        for item in value:
            out.extend(_collect_topic_tokens(item))
    else:
        out.append(str(value).lower())
    return out


def _normalize_actual_key(actual: dict, key: str) -> Any:
    """Return actual[key] with light normalization for assertion comparisons."""
    return actual.get(key)


def _assert_field(actual: dict, key: str, expected_value: Any) -> list[str]:
    """Standard equality assertion with collection-friendly comparisons."""
    fails: list[str] = []

    if key.endswith("_contains"):
        target_key = key[: -len("_contains")]
        # Map common alias: topic -> objeto in the parser params shape
        candidate_keys = [target_key]
        if target_key == "topic":
            candidate_keys = ["topic", "objeto"]
        elif target_key == "objeto":
            candidate_keys = ["objeto", "topic"]
        tokens: list[str] = []
        for k in candidate_keys:
            tokens.extend(_collect_topic_tokens(actual.get(k)))
        tokens_norm = [t.replace(" ", "_") for t in tokens]
        token_set = set(tokens_norm) | set(tokens)
        # Also expand composite tokens (e.g. "trampas_de_grasas") into parts
        # so a needle like "trampas" can match the compound.
        for tok in list(tokens_norm):
            for part in tok.replace(" ", "_").split("_"):
                if part:
                    token_set.add(part)
        bigrams = {
            f"{tokens_norm[i]}_{tokens_norm[i + 1]}"
            for i in range(len(tokens_norm) - 1)
        }
        for needle in expected_value:
            needle_norm = str(needle).lower().replace(" ", "_")
            if needle_norm in token_set or needle_norm in bigrams:
                continue
            # Bigram in reverse order (parser sometimes flips)
            if "_" in needle_norm:
                a, b = needle_norm.split("_", 1)
                if f"{b}_{a}" in bigrams or (a in token_set and b in token_set):
                    continue
            fails.append(
                f"expected {target_key} to contain {needle!r}, got {sorted(token_set) or list(tokens)}"
            )
        return fails

    if key == "not_present":
        for k in expected_value:
            v = actual.get(k)
            if v not in (None, "", [], {}, False):
                fails.append(f"expected key {k!r} absent, got {v!r}")
        return fails

    if key == "expected_objeto_must_not_contain":
        # Used only at top level of `expected` block; ignored when nested
        return fails

    actual_value = _normalize_actual_key(actual, key)
    if isinstance(expected_value, list):
        # require each element present in actual list
        actual_list = actual_value if isinstance(actual_value, list) else []
        for item in expected_value:
            if item not in actual_list:
                fails.append(f"{key}: expected element {item!r} in {actual_list!r}")
        return fails

    if actual_value != expected_value:
        fails.append(f"{key}: expected {expected_value!r}, got {actual_value!r}")
    return fails


def _assert_expected_block(actual: dict, expected: dict) -> list[str]:
    fails: list[str] = []
    for key, value in expected.items():
        fails.extend(_assert_field(actual, key, value))
    return fails


def _assert_objeto_must_not_contain(actual: dict, blocklist: list[str]) -> list[str]:
    tokens = {t for t in _collect_topic_tokens(actual.get("objeto"))}
    fails: list[str] = []
    for needle in blocklist:
        if str(needle).lower() in tokens:
            fails.append(f"objeto must not contain {needle!r}, found in {sorted(tokens)}")
    return fails


# ── Parser mode ──────────────────────────────────────────────────────────

def check_parser_entry(entry: dict, parser) -> CheckResult:
    eid = entry["id"]
    severity = entry.get("severity", "medium")
    text = entry["text"]
    expected = entry.get("expected", {}) or {}
    blocklist = entry.get("expected_objeto_must_not_contain", []) or []

    parsed = parser.parse(text)
    actual = dict(parsed.params)

    fails: list[str] = []
    fails.extend(_assert_expected_block(actual, expected))
    if blocklist:
        fails.extend(_assert_objeto_must_not_contain(actual, blocklist))

    return CheckResult(
        entry_id=eid,
        severity=severity,
        mode="parser",
        passed=not fails,
        fails=fails,
        actual=actual,
        text=text,
        category=entry.get("category", ""),
    )


# ── Workflow mode ────────────────────────────────────────────────────────

def check_workflow_entry(entry: dict, parser, frame_from_params, Classifier, Merger) -> CheckResult:
    eid = entry["id"]
    severity = entry.get("severity", "medium")
    turns = entry.get("turns", []) or []

    fails: list[str] = []
    prev_frame = None
    final_frame = None
    final_intent = None
    final_text = ""

    for idx, turn in enumerate(turns):
        text = turn["text"]
        final_text = text
        parsed = parser.parse(text)
        curr_frame = frame_from_params(parsed.params)

        if idx == 0:
            intent = "new_search"
        else:
            intent = Classifier.classify_followup(text, curr_frame, prev_frame)
        final_intent = intent

        expected_intent = turn.get("expected_intent")
        if expected_intent and expected_intent != intent:
            fails.append(f"turn{idx + 1} intent: expected {expected_intent!r}, got {intent!r}")

        # Build merged frame for downstream assertions on expected_frame
        if idx == 0 or intent == "new_search":
            merged = curr_frame
        else:
            try:
                merged = Merger.merge(prev_frame, curr_frame, intent)
            except Exception as exc:  # pragma: no cover
                fails.append(f"turn{idx + 1} merger raised: {exc!r}")
                merged = curr_frame
        final_frame = merged

        expected_frame = turn.get("expected_frame") or {}
        if expected_frame:
            actual_dict = _frame_to_dict(merged)
            fails.extend(
                [f"turn{idx + 1} {f}" for f in _assert_expected_block(actual_dict, expected_frame)]
            )

        prev_frame = merged

    return CheckResult(
        entry_id=eid,
        severity=severity,
        mode="workflow",
        passed=not fails,
        fails=fails,
        actual=_frame_to_dict(final_frame) if final_frame else {},
        text=final_text,
        category=entry.get("category", ""),
    )


def _frame_to_dict(frame) -> dict:
    if frame is None:
        return {}
    out: dict[str, Any] = {
        "dataset": getattr(frame, "dataset", None),
        "intent_type": getattr(frame, "intent_type", None),
        "estado_family": getattr(frame, "estado_family", None),
        "objeto": _collect_topic_tokens(getattr(frame, "topic", None)),
        "topic": _collect_topic_tokens(getattr(frame, "topic", None)),
    }
    modifiers = getattr(frame, "modifiers", {}) or {}
    out.update(modifiers)
    scope = getattr(frame, "scope", {}) or {}
    out.update(scope)
    raw = getattr(frame, "raw_params", {}) or {}
    for k in ("fecha_desde", "fecha_hasta", "valor_min", "valor_max",
              "departamento_resolved", "ciudad", "ordering_signal"):
        if k not in out or out[k] in (None, "", []):
            if raw.get(k) is not None:
                out[k] = raw[k]
    return out


# ── Filtering ────────────────────────────────────────────────────────────

def filter_entries(entries: list[dict], args: argparse.Namespace) -> list[dict]:
    out = entries
    if args.id:
        ids = set(args.id)
        out = [e for e in out if e.get("id") in ids]
    if args.severity:
        sev = set(args.severity)
        out = [e for e in out if e.get("severity") in sev]
    if args.mode != "all":
        out = [e for e in out if e.get("mode") == args.mode]
    if args.limit:
        out = out[: args.limit]
    return out


# ── Reporting ────────────────────────────────────────────────────────────

_SEV_RANK = {"critical": 0, "high": 1, "medium": 2, "smoke": 3}


def _color(text: str, code: str, enabled: bool) -> str:
    if not enabled:
        return text
    return f"\x1b[{code}m{text}\x1b[0m"


def print_report(results: list[CheckResult], args: argparse.Namespace) -> None:
    use_color = sys.stdout.isatty()
    passed = [r for r in results if r.passed]
    failed = [r for r in results if not r.passed]
    failed.sort(key=lambda r: (_SEV_RANK.get(r.severity, 9), r.entry_id))

    if failed:
        print("\nFAIL:")
        for r in failed:
            tag = _color("FAIL", "31", use_color)
            sev = _color(f"[{r.severity}]", "33", use_color)
            cat = f" {r.category}" if r.category else ""
            print(f"  {tag} {sev} {r.entry_id} ({r.mode}){cat} — {r.text!r}")
            for f in r.fails:
                print(f"      · {f}")
            if args.verbose:
                print(f"      actual: {_compact(r.actual)}")

    if args.show_pass and passed:
        print("\nPASS:")
        for r in passed:
            print(f"  PASS [{r.severity}] {r.entry_id} ({r.mode}) — {r.text!r}")

    total = len(results)
    print(
        "\n"
        f"Summary: {len(passed)}/{total} PASS, {len(failed)} FAIL "
        f"(mode={args.mode}, severity={args.severity or 'any'}, limit={args.limit or 'none'})"
    )
    if failed:
        by_sev: dict[str, int] = {}
        for r in failed:
            by_sev[r.severity] = by_sev.get(r.severity, 0) + 1
        breakdown = ", ".join(f"{k}={v}" for k, v in sorted(by_sev.items(), key=lambda kv: _SEV_RANK.get(kv[0], 9)))
        print(f"FAIL by severity: {breakdown}")


def _compact(d: dict) -> str:
    keep = {k: v for k, v in d.items() if v not in (None, "", [], {}, False)}
    # Drop noisy/derived keys for readability
    for k in ("departamento_resolution", "estado_field", "dataset_explicit"):
        keep.pop(k, None)
    return json.dumps(keep, ensure_ascii=False, default=str)


# ── Main ─────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the SECOPPAL query corpus.")
    parser.add_argument(
        "--mode",
        choices=["parser", "workflow", "all"],
        default="all",
        help="Which entry modes to run.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Stop after N entries.")
    parser.add_argument(
        "--id",
        action="append",
        help="Run only the given corpus entry id(s). Can be repeated.",
    )
    parser.add_argument(
        "--severity",
        action="append",
        choices=["critical", "high", "medium", "smoke"],
        help="Filter by severity. Can be repeated.",
    )
    parser.add_argument(
        "--corpus",
        type=Path,
        default=DEFAULT_CORPUS,
        help="Path to corpus YAML.",
    )
    parser.add_argument("--verbose", action="store_true", help="Show actual params on FAIL.")
    parser.add_argument("--show-pass", action="store_true", help="List PASS entries too.")

    args = parser.parse_args(argv)

    corpus_path: Path = args.corpus
    if not corpus_path.exists():
        print(f"ERROR: corpus not found at {corpus_path}", file=sys.stderr)
        return 2

    with corpus_path.open() as fh:
        data = yaml.safe_load(fh) or {}
    entries = data.get("queries", []) or []
    if not entries:
        print(f"ERROR: corpus {corpus_path} has no entries.", file=sys.stderr)
        return 2

    selected = filter_entries(entries, args)
    if not selected:
        print("No corpus entries matched the given filters.")
        return 0

    needs_workflow = any(e.get("mode") == "workflow" for e in selected)
    needs_parser = any(e.get("mode") == "parser" for e in selected) or needs_workflow

    parser_obj = _load_parser() if needs_parser else None
    workflow_bits = _load_workflow_helpers() if needs_workflow else None

    results: list[CheckResult] = []
    for entry in selected:
        mode = entry.get("mode", "parser")
        try:
            if mode == "workflow":
                router, frame_from_params, Classifier, Merger = workflow_bits  # type: ignore[misc]
                results.append(check_workflow_entry(entry, router, frame_from_params, Classifier, Merger))
            else:
                results.append(check_parser_entry(entry, parser_obj))
        except Exception as exc:  # pragma: no cover
            results.append(
                CheckResult(
                    entry_id=entry.get("id", "<unknown>"),
                    severity=entry.get("severity", "medium"),
                    mode=mode,
                    passed=False,
                    fails=[f"runner raised: {exc!r}"],
                    text=entry.get("text", ""),
                    category=entry.get("category", ""),
                )
            )

    print_report(results, args)
    return 1 if any(not r.passed for r in results) else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
