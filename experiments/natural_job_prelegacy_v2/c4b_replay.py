"""C4B — DEV + HOLDOUT replay of the RECORDED Alpha run with the candidate runtime.

The laboratory stays closed: this harness reads only frozen artifacts (corpus,
holdout, adjudications, recorded V3 payloads, the C4A.5 ordering simulation) and
runs the candidate runtime turn by turn. Nothing here tunes the prompt, the
contract or the gates.

Where B's labels come from is reported per case:

- ``recorded`` — the frozen C4A.5 transfer recorded the model's answer for the
  very same units, so that answer is reused (zero calls).
- ``live``     — the candidate hands B different units than the transfer did
  (C4B moved the weak actor guard after the verifier), so a recorded label would
  be an invented input/output pair: in ``--live`` the frozen contract is asked
  again, in ``--dry`` the case is reported as ``UNAVAILABLE`` and clarifies.

Run:
  python experiments/natural_job_prelegacy_v2/c4b_replay.py --dry
  DEEPSEEK_API_KEY=... python experiments/natural_job_prelegacy_v2/c4b_replay.py --live
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
import tempfile
import time
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

EXPERIMENT = ROOT / "experiments" / "natural_job_prelegacy_v2"
CORPUS = EXPERIMENT / "corpus_dev.json"
HOLDOUT = EXPERIMENT / "holdout.json"
PAYLOADS = EXPERIMENT / "alpha_payloads_v3.json"
TRANSFER = EXPERIMENT / "residual_role_transfer_b.json"
SIMULATION = EXPERIMENT / "residual_role_ordering_simulation.json"
ADJUDICATION = EXPERIMENT / "oracle_adjudication.json"
CORRECTIONS = EXPERIMENT / "oracle_corrections.json"
PROMPT_B = EXPERIMENT / "residual_role_prompt_b.txt"
OUT = EXPERIMENT / "c4b_runtime_replay.json"
TODAY = date(2026, 9, 17)
RUNS = 2
SILENT = "silent"
GATES = ("WRONG_CAPABILITY_READY", "WRONG_BUYER", "SILENT_FILTER_LOSS", "ACTOR_STOLEN",
         "NEGATED_EXECUTED", "UNSUPPORTED_CARDINALITY_EXECUTED", "MATERIAL_FALSE_NEGATIVE",
         "QUERY_ROUTER_CREDENTIALED", "LEGACY_FALLBACK_CREDENTIALED")


def _write(name: str, document: dict) -> None:
    (EXPERIMENT / name).write_text(
        json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def _sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _log(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")


# ── the frozen inputs ────────────────────────────────────────────────────────

def cases() -> dict:
    dev = json.loads(CORPUS.read_text(encoding="utf-8"))
    hold = json.loads(HOLDOUT.read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in dev + hold}


def datasets() -> dict:
    return {
        "dev": json.loads(CORPUS.read_text(encoding="utf-8")),
        "holdout": json.loads(HOLDOUT.read_text(encoding="utf-8")),
    }


def recorded_units() -> dict:
    """case_id -> the units the frozen transfer asked B about, with its answer."""
    document = json.loads(TRANSFER.read_text(encoding="utf-8"))
    out: dict[str, dict] = {}
    for run in document["runs"]:
        for case in run["cases"]:
            out.setdefault(case["case_id"], {
                "units": [unit["text"] for unit in case["units"]],
                "labels": case.get("labels"),
            })
    return out


def corrections() -> set[str]:
    document = json.loads(CORRECTIONS.read_text(encoding="utf-8"))
    return {entry["case_id"] for entry in document.get("corrections", [])
            if entry["correction"] == "ORACLE_ERROR"}


def supported_ids() -> set[str]:
    """The frozen product metric: SUPPLIERS/EXECUTE candidates inside SUPPORTED_V2."""
    adjudication = json.loads(ADJUDICATION.read_text(encoding="utf-8"))
    out_of_support = {entry["case_id"] for entry in adjudication.get("adjudications", [])
                      if entry["adjudication"] == "OUT_OF_SUPPORTED_V2"}
    return {case_id for case_id, case in cases().items()
            if case["owner"] == "SUPPLIERS" and case["outcome"] == "EXECUTE"
            and case_id not in out_of_support}


def measured_live_labels() -> dict:
    """Labels this harness already measured live, read back from its own artifact.

    A replay must not call the model again for units it has already measured: the
    live measurement is recorded evidence, and re-running the candidate runtime
    has to reuse it (otherwise the artifact would drift with non-determinism
    instead of showing the effect of the runtime change). Keyed by (case, run):
    an unstable answer is part of the measurement, never averaged away.
    """
    if not OUT.exists():
        return {}
    document = json.loads(OUT.read_text(encoding="utf-8"))
    out: dict[tuple[str, int], dict] = {}
    for row in document.get("rows", []):
        if row.get("residual_label_source") in ("live", "live_measured") and row.get("residual_labels"):
            out[(row["case_id"], row["run"])] = {
                "units": row["residual_units"], "labels": row["residual_labels"]}
    return out


# ── providers ────────────────────────────────────────────────────────────────

class RecordedAlpha:
    """The turn's one observation, served from the recorded V3 payload."""

    def __init__(self, payloads: dict):
        self._by_raw = payloads
        self.calls = 0

    def observe(self, call_key, request):
        from app.core.natural_alpha.provider import ProviderResult

        self.calls += 1
        raw = json.loads(request["input"])["text"]
        return ProviderResult(payload=self._by_raw.get(raw, ""), provider_calls=1)


class ResidualLabels:
    """B's answer: the frozen transfer's when its units match, else frozen-contract live.

    ``source`` per case is recorded by the harness, so a reused label is never
    presented as a fresh model answer and a case the recorded run cannot serve
    never silently passes. Three sources, in order: the C4A.5 transfer's recorded
    answer, this harness's own earlier live measurement, and — with ``--live`` —
    the frozen contract asked again.
    """

    def __init__(self, recorded: dict, *, live: bool, provider=None, measured: dict | None = None):
        self._recorded = recorded
        self._measured = measured or {}
        self._live = live
        self._provider = provider
        self.sources: dict[str, str] = {}
        self.calls = 0

    def observe(self, call_key, request):
        from app.core.natural_alpha.provider import ProviderResult

        body = json.loads(json.loads(request["input"])["text"].split("\n\nENTRADA\n", 1)[1])
        units = [line.split(":", 1)[0] for line in body["residual_units"]]
        texts = [line.split(":", 1)[1].strip().strip('"') for line in body["residual_units"]]
        case_id, run_index = getattr(self, "_current", ("", 0))
        for source, prior in (
            ("recorded", self._recorded.get(case_id, {"units": [], "labels": None})),
            ("live_measured", self._measured.get((case_id, run_index), {"units": [], "labels": None})),
        ):
            if prior["labels"] and prior["units"] == texts:
                self.sources[case_id] = source
                return ProviderResult(payload=json.dumps({"labels": [
                    {"id": units[index], "label": prior["labels"][units[index]]}
                    for index in range(len(units))]}), provider_calls=0)
        if not self._live or self._provider is None:
            self.sources[case_id] = "unavailable"
            return ProviderResult(error="RECORDED_LABELS_DO_NOT_COVER_THESE_UNITS")
        self.sources[case_id] = "live"
        self.calls += 1
        return self._provider.observe(call_key, request)


class StubSource:
    """No source is contacted: READY executes, and the replay measures the route."""

    timeout = 15

    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    def with_timeout(self, timeout):
        return self

    derived_for_timeout = with_timeout

    def query(self, dataset, soql):
        self.calls.append((dataset, soql))
        return []

    aggregate = query

    def count(self, dataset, soql):
        self.calls.append((dataset, soql))
        return 0


# ── one turn of the candidate runtime ────────────────────────────────────────

def _world(root: pathlib.Path):
    from app.config import Settings
    from app.core.conversation_store import ConversationStore
    from app.core.orchestrator import SecopalWorkflow

    settings = Settings(
        _env_file=None, deepseek_api_key=None,
        secoppal_natural_alpha_mode="alpha",
        secoppal_natural_alpha_email="alpha@example.test",
        secoppal_a2_suppliers_binding_enabled=True,
        secop_feedback_path=root / "traces.jsonl", secop_feedback_db_path=root / "feedback.db",
    )
    flow = SecopalWorkflow(settings)
    flow.conv_store = ConversationStore(root / "conversations")
    flow.narrator.narrate_with_grounding = lambda **kwargs: None
    flow.secop_client = StubSource()
    return flow


def _labels_of(result: dict) -> tuple[list[str], dict, str]:
    """The verifier's receipt as it travelled in the turn result."""
    units = [unit["text"] for unit in result.get("residual_units", [])]
    return units, dict(result.get("residual_labels", {})), result.get("residual_verifier_status", "")


def replay_case(flow, case, *, chat_id, labels: ResidualLabels, identity, run_index) -> dict:
    from app.core.accreditation.assembly import AssemblyStatus

    raw = case["raw_text"]
    labels._current = (case["case_id"], run_index)
    router_calls: list = []
    dispatch_calls: list = []
    real_parse, real_dispatch = flow.query_router.parse, flow._dispatch
    flow.query_router.parse = lambda query, **kwargs: (
        router_calls.append(query), real_parse(query, **kwargs))[1]
    flow._dispatch = lambda ctx, seed=None, **kwargs: (
        dispatch_calls.append(ctx.user_query), real_dispatch(ctx, seed, **kwargs))[1]
    try:
        result = flow.run_query(raw, channel="streamlit", user_identity=identity,
                                today=TODAY, chat_id=chat_id)
    except Exception as exc:  # noqa: BLE001 -- recorded, never hidden
        result = {"route_reason": f"HARNESS_ERROR:{type(exc).__name__}:{exc}", "results": []}
    finally:
        flow.query_router.parse, flow._dispatch = real_parse, real_dispatch
    units, label_map, verification_status = _labels_of(result)
    status = result.get("authority_status", "")
    return {
        "case_id": case["case_id"], "run": run_index, "owner": case["owner"],
        "outcome": case["outcome"], "family": case.get("family"), "raw": raw,
        "expected_buyer": case.get("buyer"), "forbid": list(case.get("forbid") or ()),
        "alpha_status": result.get("natural_alpha_status", ""),
        "credential_valid": "credential_capability" in result,
        "assembly_status": status, "assembly_reason": result.get("authority_reason", ""),
        "accredited_buyer": result.get("buyer_authority", ""),
        "residual_units": units, "residual_labels": label_map,
        "residual_label_source": labels.sources.get(case["case_id"], ""),
        "residual_verifier_status": verification_status,
        "weak_actor_guard_called": result.get("weak_actor_guard_called", None),
        "route_reason": result.get("route_reason", ""),
        "needs_clarification": bool(result.get("needs_clarification", False)),
        "executed": bool(flow.secop_client.calls),
        "ready": status == AssemblyStatus.READY.value,
        "query_router_calls": len(router_calls),
        "legacy_dispatch_calls": len(dispatch_calls),
    }


# ── gates (computed from the frozen oracle plus this run) ────────────────────

def gates(rows: list[dict], excluded: set[str]) -> dict:
    by_gate = {name: [] for name in GATES}
    by_gate["ORACLE_ERROR_EXCLUDED"] = sorted(
        {row["case_id"] for row in rows if row["case_id"] in excluded})
    for row in rows:
        if row["case_id"] in excluded:
            continue
        case_id, ready = row["case_id"], row["ready"]
        if ready and row["owner"] != "SUPPLIERS":
            by_gate["WRONG_CAPABILITY_READY"].append(case_id)
        if ready and ("SUPPLIERS_EXECUTED" in row["forbid"]
                      or "EXECUTE_WITHOUT_BUYER" in row["forbid"]):
            by_gate["WRONG_CAPABILITY_READY"].append(case_id)
        if ready and row["owner"] == "SUPPLIERS" and row["expected_buyer"]:
            if row["expected_buyer"].split()[-1].lower() not in (row["accredited_buyer"] or "").lower():
                by_gate["WRONG_BUYER"].append(case_id)
        if ready and row["owner"] == "SUPPLIERS" and row["outcome"] == "HONEST_CLARIFY":
            by_gate["SILENT_FILTER_LOSS"].append(case_id)
        if ready and "ACTOR_STOLEN" in row["forbid"]:
            by_gate["ACTOR_STOLEN"].append(case_id)
        if ready and "NEGATED_JOB_EXECUTED" in row["forbid"]:
            by_gate["NEGATED_EXECUTED"].append(case_id)
        if ready and "SILENT_CARDINALITY_SWAP" in row["forbid"]:
            by_gate["UNSUPPORTED_CARDINALITY_EXECUTED"].append(case_id)
        # B saw material and said job language: the verifier's own false negative.
        labels = row["residual_labels"] or {}
        if (row["residual_units"] and labels
                and all(label == "JOB_LANGUAGE" for label in labels.values())
                and row["outcome"] == "HONEST_CLARIFY" and row["owner"] == "SUPPLIERS"):
            by_gate["MATERIAL_FALSE_NEGATIVE"].append(case_id)
        if row["credential_valid"] and row["query_router_calls"]:
            by_gate["QUERY_ROUTER_CREDENTIALED"].append(case_id)
        if row["credential_valid"] and row["legacy_dispatch_calls"]:
            by_gate["LEGACY_FALLBACK_CREDENTIALED"].append(case_id)
    for name in GATES:
        by_gate[name] = sorted(set(by_gate[name]))
    by_gate["all_zero"] = all(not by_gate[name] for name in GATES)
    return by_gate


def ready_sets(rows: list[dict], dataset_cases: list[dict]) -> dict:
    """Supported candidates for THIS dataset, taken from the frozen adjudication."""
    supported = supported_ids()
    candidates = [case["case_id"] for case in dataset_cases
                  if case["owner"] == "SUPPLIERS" and case["outcome"] == "EXECUTE"]
    supported_here = [case_id for case_id in candidates if case_id in supported]
    ready = {row["case_id"] for row in rows if row["ready"]}
    return {
        "raw_ready": [case_id for case_id in candidates if case_id in ready],
        "raw_total": len(candidates),
        "supported_ready": [case_id for case_id in supported_here if case_id in ready],
        "supported_ready_count": len([case_id for case_id in supported_here if case_id in ready]),
        "supported_total": len(supported_here),
        "not_suppliers_ready": sorted(row["case_id"] for row in rows
                                      if row["ready"] and row["owner"] != "SUPPLIERS"),
    }


def compare_with_simulation(run_rows: list[dict]) -> dict:
    simulation = json.loads(SIMULATION.read_text(encoding="utf-8"))
    expected = set(simulation["B_BEFORE_WEAK_ACTOR_GUARD"]["supported_ready"])
    per_case = {row["case_id"]: row for row in run_rows}
    current = sorted({row["case_id"] for row in run_rows if row["ready"]} & supported_ids())
    observed = set(current)
    return {
        "reference": "C4A.5 residual_role_ordering_simulation.json",
        "expected_note": "referencia, no hardcode: ~15 supported READY",
        "simulation_supported_ready": sorted(expected),
        "simulation_supported_ready_count": len(expected),
        "observed_supported_ready": current,
        "observed_supported_ready_count": len(current),
        "only_in_simulation": sorted(expected - observed),
        "only_in_observed": sorted(observed - expected),
        "explanation": {
            case_id: {
                "simulation": "READY",
                "observed_status": per_case.get(case_id, {}).get("assembly_status", "NOT_IN_REPLAY"),
                "observed_reason": per_case.get(case_id, {}).get("assembly_reason", ""),
                "residual_units": per_case.get(case_id, {}).get("residual_units", []),
                "residual_labels": per_case.get(case_id, {}).get("residual_labels", {}),
                "label_source": per_case.get(case_id, {}).get("residual_label_source", ""),
            }
            for case_id in sorted((expected - observed) | (observed - expected))
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry", action="store_true",
                        help="recorded labels only; cases needing new units clarify")
    parser.add_argument("--live", action="store_true",
                        help="ask the frozen contract (same model) for the cases it cannot serve")
    args = parser.parse_args()
    if not (args.dry or args.live):
        parser.error("choose --dry or --live")
    from app.core.accreditation.residual_verifier import (
        MODEL, instruction_sha256, settings_residual_provider,
    )
    from tests.test_natural_alpha_contract import IDENTITY  # the same identity V3 was recorded with
    import os

    import burr.telemetry

    burr.telemetry.g_telemetry_enabled = False  # no outbound telemetry in a replay
    # The Frame is the route a READY turn takes; the flag is read from the
    # process settings, exactly like the C4B focal tests enable it.
    os.environ["SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED"] = "true"
    from app.config import get_settings

    get_settings.cache_clear()

    frozen = _sha256(PROMPT_B)
    live_provider = None
    if args.live:
        live_provider = settings_residual_provider(_settings_for_provider())
    labels = ResidualLabels(recorded_units(), live=args.live, provider=live_provider,
                            measured=measured_live_labels())
    payloads = json.loads(PAYLOADS.read_text(encoding="utf-8"))
    excluded = corrections()
    scratch = pathlib.Path(tempfile.mkdtemp(prefix="c4b-replay-"))
    starts = time.perf_counter()
    summaries, rows_by_dataset = {}, {}
    for dataset, dataset_cases in datasets().items():
        run_rows: list[dict] = []
        for run_index in range(RUNS):
            flow = _world(scratch / f"{dataset}-{run_index}")
            by_raw = {record["raw_text"]: record["provider_raw_response"]
                      for record in payloads[dataset] if record["run"] == run_index}
            flow.natural_alpha_provider = RecordedAlpha(by_raw)
            flow.residual_verifier_provider = labels
            for case in dataset_cases:
                if case["raw_text"] not in by_raw:
                    continue
                row = replay_case(flow, case, chat_id=f"{dataset}-{run_index}-{case['case_id']}",
                                  labels=labels, identity=IDENTITY, run_index=run_index)
                row["dataset"] = dataset
                run_rows.append(row)
        rows_by_dataset[dataset] = run_rows
        first_run = [row for row in run_rows if row["run"] == 0]
        summaries[dataset] = {
            "cases": len(run_rows),
            "gates": gates(run_rows, excluded),
            "ready": ready_sets(first_run, dataset_cases),
            "real_executions": len([row for row in run_rows if row["executed"]]),
            "residual_label_sources": {
                source: len([row for row in run_rows if row["residual_label_source"] == source])
                for source in ("recorded", "live_measured", "live", "unavailable")},
        }
    all_rows = [row for rows in rows_by_dataset.values() for row in rows]
    first_runs = [row for row in all_rows if row["run"] == 0]
    comparison = compare_with_simulation(first_runs)
    document = {
        "checkpoint": "C4B runtime replay",
        "mode": "live" if args.live else "dry",
        "prompt_b_sha256": frozen,
        "productive_instruction_sha256": instruction_sha256(),
        "model": MODEL,
        "runs": RUNS,
        "structural_spies": {
            "query_router": "flow.query_router.parse wrapped and delegating",
            "legacy_dispatch": "flow._dispatch wrapped and delegating",
            "source": "stub source; every call recorded, no network",
        },
        "datasets": summaries,
        "comparison_with_simulation": comparison,
        "replay_seconds": round(time.perf_counter() - starts, 1),
        "residual_verifier_calls_live": labels.calls,
        "rows": all_rows,
    }
    _write("c4b_runtime_replay.json", document)
    _log({"mode": document["mode"], "prompt_sha256": frozen,
          "instruction_sha256": instruction_sha256(),
          "gates": {name: summaries[name]["gates"] for name in summaries},
          "ready": {name: summaries[name]["ready"] for name in summaries},
          "comparison": comparison,
          "label_sources": {name: summaries[name]["residual_label_sources"] for name in summaries},
          "live_calls": labels.calls})


def _settings_for_provider():
    """Provider settings: the credential comes from the environment, never a file."""
    from app.config import Settings

    scratch = pathlib.Path(tempfile.mkdtemp(prefix="c4b-replay-provider-"))
    return Settings(
        _env_file=None, secoppal_a2_suppliers_binding_enabled=True,
        secop_feedback_path=scratch / "traces.jsonl",
        secop_feedback_db_path=scratch / "feedback.db",
    )


if __name__ == "__main__":
    main()
