"""C4A.2 B-F — Natural Alpha V3 real over DEV and HOLDOUT, then the dry run.

Two runs per dataset, each scored **on its own**; stability only compares them.
Three verdicts stay separate: classifier, evidence, control plane dry run
(credential -> spec -> assemble; no Frame, no source).

The prompt is read from the runtime module, whose hash must equal the frozen
copy: if they diverge the harness refuses to run.

The provider credential must be in the **environment** (``DEEPSEEK_API_KEY``):
the harness builds its settings with ``_env_file=None`` and never reads a secret
file on its own. Without the variable it refuses and says so; it never
fabricates labels.

Run:
  DEEPSEEK_API_KEY=... python experiments/natural_job_prelegacy_v2/alpha_v3_evaluation.py
  python experiments/natural_job_prelegacy_v2/alpha_v3_evaluation.py --stub   # plumbing only
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

from app.config import Settings  # noqa: E402
from app.core.accreditation import SUPPLIERS, capability_spec, credential_from_observation  # noqa: E402
from app.core.accreditation.assembly import AssemblyStatus, assemble  # noqa: E402
from app.core.natural_alpha.grounding import validate_and_ground  # noqa: E402
from app.core.natural_alpha.models import GroundedJob, Observation, Proposal  # noqa: E402
from app.core.natural_alpha.prompt import provider_request  # noqa: E402
from app.core.natural_alpha.provider import DeepSeekResponsesProvider  # noqa: E402

EXPERIMENT = ROOT / "experiments" / "natural_job_prelegacy_v2"
CORPUS = EXPERIMENT / "corpus_dev.json"
HOLDOUT = EXPERIMENT / "holdout.json"
ADJUDICATION = EXPERIMENT / "oracle_adjudication.json"
FROZEN = EXPERIMENT / "prompt_v3.frozen.txt"
TODAY = date(2026, 9, 17)
RUNS = 2
SUPPLIERS_JOB = "ANALYZE/SUPPLIERS"
GATE_NAMES = ("WRONG_CAPABILITY_REACHES_READY", "WRONG_BUYER", "SILENT_FILTER_LOSS",
              "ACTOR_STOLEN", "NEGATED_JOB_EXECUTED", "UNSUPPORTED_CARDINALITY_EXECUTED")
# Witnessed by the structural spies of the C3/C4A lanes, not by this harness.
STRUCTURAL_ZEROS = ("QUERY_ROUTER_CALLS", "LEGACY_GRAPH_ENTERED", "SOURCE_CALLS")


class _StubProvider:
    """Plumbing only: scripted payloads, never presented as the model's answer."""

    def __init__(self, payloads: dict | None = None, default: str | None = None):
        self._payloads = payloads or {}
        self._default = default or json.dumps({"jobs": [
            {"action": "ANALYZE", "aspect": "SUPPLIERS",
             "evidence_quote": "proveedores", "occurrence": 1}]})

    def observe(self, call_key, request):
        from app.core.natural_alpha.provider import ProviderResult

        dataset, run_index, case_id = call_key.split(":")[:3]
        payload = self._payloads.get((run_index, case_id),
                                     self._payloads.get(case_id, self._default))
        return ProviderResult(payload=payload, provider_calls=1)


def frozen_guard() -> str:
    frozen = hashlib.sha256(FROZEN.read_bytes()).hexdigest()
    live = hashlib.sha256((ROOT / "app" / "core" / "natural_alpha" / "prompt.py").read_bytes()).hexdigest()
    if frozen != live:
        raise SystemExit("PROMPT_DIVERGED_FROM_FROZEN_COPY: refusing to run")
    return frozen


def require_credential(settings) -> str:
    """The credential must come from the environment; no secret file is read."""
    key = settings.deepseek_api_key
    if not key:
        raise SystemExit(
            "PROVIDER_CREDENTIAL_ABSENT: this harness builds its settings with "
            "_env_file=None, so a project .env is NOT read. Export DEEPSEEK_API_KEY in the "
            "environment and rerun. It never fabricates labels.")
    return key


def _proposal_jobs(payload: str) -> list[dict]:
    """Observability only: the model's own proposal, checked against the schema."""
    try:
        proposal = Proposal.model_validate(json.loads(payload))
    except Exception:  # noqa: BLE001 -- unparsable proposal is recorded as absent
        return []
    return [{"action": job.action, "aspect": job.aspect, "evidence_quote": job.evidence_quote,
             "occurrence": job.occurrence} for job in proposal.jobs]


def collect(case: dict, provider, run_index: int, settings) -> dict:
    raw = case["raw_text"]
    started = time.perf_counter()
    try:
        response = provider.observe(f"c4a2:{run_index}:{case['case_id']}", provider_request(raw))
        error, payload = response.error, response.payload
    except Exception as exc:  # noqa: BLE001 -- recorded, never hidden
        error, payload = f"{type(exc).__name__}: {exc}", ""
    latency = round((time.perf_counter() - started) * 1000, 3)
    # validate_and_ground stays the only grounding authority; the parsed proposal is
    # joined only to report what the model actually wrote.
    jobs, grounding_error = (), ""
    if not error:
        try:
            jobs = validate_and_ground(raw, payload)
        except (ValueError, TypeError) as exc:
            grounding_error = str(exc)
    proposal = _proposal_jobs(payload) if payload else []
    grounded = [
        {"action": job.action, "aspect": job.aspect,
         "evidence_interval": [job.start, job.end],
         "evidence_quote": proposal[index]["evidence_quote"] if index < len(proposal) else "",
         "occurrence": proposal[index]["occurrence"] if index < len(proposal) else None}
        for index, job in enumerate(jobs)
    ]
    return {
        "case_id": case["case_id"], "run": run_index, "raw_text": raw,
        "provider_raw_response": payload, "provider_error": error,
        "parsed_jobs": proposal, "grounded_jobs": grounded,
        "latency_ms": latency, "grounding_error": grounding_error,
    }


def annotate(record: dict, case: dict) -> dict:
    """Label verdict against the corpus oracle for this record."""
    expected_suppliers = case["owner"] == "SUPPLIERS"
    observed = {f"{job['action']}/{job['aspect']}" for job in record["grounded_jobs"]}
    is_suppliers = SUPPLIERS_JOB in observed
    status = "INVALID" if record["grounding_error"] else ("MATCHED" if observed else "SKIPPED")
    if record["provider_error"]:
        status = "PROVIDER_ERROR"
    return {
        "case_id": case["case_id"], "owner": case["owner"], "observed": sorted(observed),
        "status": status,
        "suppliers_true_positive": bool(expected_suppliers and is_suppliers),
        "suppliers_false_positive": bool(not expected_suppliers and is_suppliers
                                         and case["owner"] != "MULTI_JOB"),
        "suppliers_false_negative": bool(expected_suppliers and not is_suppliers),
        "not_suppliers_correct": bool(not expected_suppliers and not is_suppliers
                                      and case["owner"] == "NOT_SUPPLIERS"),
        "multi_job_correct": bool(case["owner"] == "MULTI_JOB"
                                  and len(record["grounded_jobs"]) >= 2),
        "exact_job_match": bool(observed == {SUPPLIERS_JOB}) if expected_suppliers else None,
    }


def _no_credential(case_id: str, reason: str) -> dict:
    return {"case_id": case_id, "credential_valid": False, "reason": reason,
            "status": "NO_CREDENTIAL", "ready": False, "overreach": None,
            "evidence_consumes_residual": None, "residual_material": [],
            "buyer": None, "fecha_desde": None, "ordering_signal": None}


def dry_run(record: dict, case: dict, settings, entity_resolver, actor_resolver) -> dict:
    raw = record["raw_text"]
    spec = capability_spec(SUPPLIERS)
    if not record["grounded_jobs"]:
        return _no_credential(case["case_id"], "NO_CREDENTIAL")
    observation = Observation("alpha", "MATCHED", tuple(
        GroundedJob(job["action"], job["aspect"], job["evidence_interval"][0],
                    job["evidence_interval"][1]) for job in record["grounded_jobs"]),
        provider_calls=1)
    credential = credential_from_observation(observation, raw, settings=settings)
    if credential is None:
        return _no_credential(case["case_id"], "CREDENTIAL_REFUSED")
    assembly = assemble(raw, credential, spec, entity_resolver=entity_resolver, today=TODAY,
                        actor_resolver=actor_resolver)
    return {
        "case_id": case["case_id"], "credential_valid": True,
        "status": assembly.status.value, "reason": assembly.reason,
        "ready": assembly.status is AssemblyStatus.READY,
        "overreach": assembly.credential_overreach,
        "evidence_consumes_residual": assembly.evidence_consumes_residual,
        "residual_material": list(assembly.residual_material),
        "buyer": assembly.params.get("entidad_resolved"),
        "fecha_desde": assembly.params.get("fecha_desde"),
        "ordering_signal": assembly.params.get("ordering_signal"),
    }


def hard_gates(cases, annotated, dry) -> dict:
    """Computed from the frozen oracle plus the real dry run, never constants."""
    by_case = {verdict["case_id"]: verdict for verdict in annotated}
    gates = {name: [] for name in GATE_NAMES}
    for case, run in zip(cases, dry):
        if run.get("ready") is not True:
            continue  # a turn that clarifies executes nothing
        case_id = case["case_id"]
        forbid = set(case.get("forbid") or ())
        wrong_owner = (case["owner"] != "SUPPLIERS"
                       or "SUPPLIERS_EXECUTED" in forbid
                       or "EXECUTE_WITHOUT_BUYER" in forbid)
        if wrong_owner:
            gates["WRONG_CAPABILITY_REACHES_READY"].append(case_id)
        if case["owner"] == "SUPPLIERS":
            expected_buyer = case.get("buyer")
            if expected_buyer and expected_buyer.split()[-1] not in (run.get("buyer") or "").lower():
                gates["WRONG_BUYER"].append(case_id)
            if case["outcome"] == "HONEST_CLARIFY":
                gates["SILENT_FILTER_LOSS"].append(case_id)
        if "ACTOR_STOLEN" in forbid:
            gates["ACTOR_STOLEN"].append(case_id)
        if "NEGATED_JOB_EXECUTED" in forbid:
            gates["NEGATED_JOB_EXECUTED"].append(case_id)
        if "SILENT_CARDINALITY_SWAP" in forbid:
            gates["UNSUPPORTED_CARDINALITY_EXECUTED"].append(case_id)
        by_case[case_id]["gated"] = sorted(
            name for name in GATE_NAMES if case_id in gates[name])
    for name, ids in gates.items():
        gates[name] = sorted(set(ids))
    return gates


def candidates(cases, adjudication: dict) -> dict:
    """RAW keeps every SUPPLIERS/EXECUTE case; SUPPORTED_V2 drops the adjudicated."""
    out_of_support = {entry["case_id"] for entry in adjudication.get("adjudications", [])
                      if entry["adjudication"] == "OUT_OF_SUPPORTED_V2"}
    raw = [case["case_id"] for case in cases
           if case["owner"] == "SUPPLIERS" and case["outcome"] == "EXECUTE"]
    return {"raw": raw,
            "supported": [case_id for case_id in raw if case_id not in out_of_support],
            "out_of_supported": sorted(set(raw) & out_of_support)}


def summarise(cases, annotated, dry, adjudication: dict, dataset: str) -> dict:
    by_case = {verdict["case_id"]: verdict for verdict in annotated}
    runs = {run["case_id"]: run for run in dry}
    suppliers = [case for case in cases if case["owner"] == "SUPPLIERS"]
    credential_cases = [case_id for case_id, run in runs.items() if run["credential_valid"]]
    overreach = [case_id for case_id in credential_cases if runs[case_id]["overreach"]]
    picks = candidates(cases, adjudication)
    ready = {case_id for case_id, run in runs.items() if run["ready"]}
    gates = hard_gates(cases, annotated, dry)
    return {
        "dataset": dataset, "cases": len(cases),
        "classifier": {
            "suppliers_true_positive": len([v for v in by_case.values()
                                            if v["suppliers_true_positive"]]),
            "suppliers_total": len(suppliers),
            "suppliers_false_positive": sorted(case_id for case_id, v in by_case.items()
                                               if v["suppliers_false_positive"]),
            "suppliers_false_negative": sorted(case_id for case_id, v in by_case.items()
                                               if v["suppliers_false_negative"]),
            "not_suppliers_correct": len([v for v in by_case.values()
                                          if v["not_suppliers_correct"]]),
            "multi_job_correct": len([v for v in by_case.values() if v["multi_job_correct"]]),
            "invalid_or_other": sorted(case_id for case_id, v in by_case.items()
                                       if v["status"] in ("INVALID", "SKIPPED", "PROVIDER_ERROR")),
            "per_case": [by_case[case["case_id"]] for case in cases],
        },
        "evidence": {
            "credential_candidates": len(credential_cases),
            "overreach_rate": round(len(overreach) / len(credential_cases), 4)
                              if credential_cases else None,
            "overreach_case_ids": sorted(overreach),
            "invalid_grounding_case_ids": sorted(
                case["case_id"] for case in cases
                if runs[case["case_id"]]["status"] == "NO_CREDENTIAL"),
        },
        "dry_run": {
            "per_case": [runs[case["case_id"]] for case in cases],
            "hard_gates": gates,
            "hard_gates_pass": all(not ids for ids in gates.values()),
            "structural_zeros": {name: 0 for name in STRUCTURAL_ZEROS},
        },
        "ready": {
            "raw_ready": [case_id for case_id in picks["raw"] if case_id in ready],
            "raw_total": len(picks["raw"]),
            "supported_ready": [case_id for case_id in picks["supported"] if case_id in ready],
            "supported_total": len(picks["supported"]),
            "out_of_supported": picks["out_of_supported"],
            "not_suppliers_ready": sorted(case["case_id"] for case in cases
                                          if case["owner"] != "SUPPLIERS"
                                          and case["case_id"] in ready),
        },
    }


def stability(runs: list[dict]) -> dict:
    """Compares the runs; it never replaces their individual scores."""
    labels, intervals = [], []
    if len(runs) >= 2:
        first, *rest = runs
        for record in first["records"]:
            others = [other for run in rest for other in run["records"]
                      if other["case_id"] == record["case_id"]]
            if any(sorted(f"{j['action']}/{j['aspect']}" for j in other["grounded_jobs"])
                   != sorted(f"{j['action']}/{j['aspect']}" for j in record["grounded_jobs"])
                   for other in others):
                labels.append(record["case_id"])
            if any([j["evidence_interval"] for j in other["grounded_jobs"]]
                   != [j["evidence_interval"] for j in record["grounded_jobs"]]
                   for other in others):
                intervals.append(record["case_id"])
    return {"runs_compared": [run["run"] for run in runs],
            "unstable_labels": sorted(labels),
            "unstable_evidence_intervals": sorted(intervals)}


def run_dataset(dataset, cases, provider, settings, entity_resolver, actor_resolver,
                adjudication) -> dict:
    runs = []
    for run_index in range(RUNS):
        records = [collect(case, provider, run_index, settings) for case in cases]
        annotated = [annotate(record, case) for record, case in zip(records, cases)]
        dry = [dry_run(record, case, settings, entity_resolver, actor_resolver)
               for record, case in zip(records, cases)]
        runs.append({"run": run_index, "records": records,
                     "summary": summarise(cases, annotated, dry, adjudication, dataset)})
    return {"dataset": dataset, "runs": runs, "stability": stability(runs)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stub", action="store_true", help="plumbing only, never a result")
    args = parser.parse_args()

    prompt_hash = frozen_guard()
    # Never let a harness turn write into the product's real feedback data.
    scratch = pathlib.Path(tempfile.mkdtemp(prefix="c4a2-alpha-"))
    settings = Settings(
        _env_file=None, secoppal_a2_suppliers_binding_enabled=True,
        secop_feedback_path=scratch / "traces.jsonl",
        secop_feedback_db_path=scratch / "feedback.db",
    )
    from app.core.entity_resolver import EntityResolver
    from app.data.actor_alias_resolver import ActorNameResolver

    entity_resolver = EntityResolver(settings.secop_alias_db_path)
    actor_resolver = ActorNameResolver(settings.secop_actor_alias_index_path)
    adjudication = json.loads(ADJUDICATION.read_text(encoding="utf-8"))

    if args.stub:
        provider, suffix = _StubProvider(), "_stub"
    else:
        provider = DeepSeekResponsesProvider(
            api_key=require_credential(settings),
            timeout_seconds=settings.secoppal_natural_alpha_timeout_seconds)
        suffix = ""

    payloads, evaluation = {}, {}
    for dataset, path in (("dev", CORPUS), ("holdout", HOLDOUT)):
        cases = json.loads(path.read_text(encoding="utf-8"))
        result = run_dataset(dataset, cases, provider, settings, entity_resolver,
                             actor_resolver, adjudication)
        payloads[dataset] = [record for run in result["runs"] for record in run["records"]]
        evaluation[dataset] = result

    document = {
        "checkpoint": "C4A.2 B-F" + (" (STUB PLUMBING ONLY)" if args.stub else ""),
        "prompt": {"frozen_sha256": prompt_hash, "immutable": True,
                   "request": {key: value for key, value in provider_request("x").items()
                               if key != "instructions"}},
        "runs": RUNS,
        "dev": evaluation["dev"], "holdout": evaluation["holdout"],
    }
    (EXPERIMENT / f"alpha_payloads_v3{suffix}.json").write_text(
        json.dumps(payloads, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    (EXPERIMENT / f"alpha_v3_evaluation{suffix}.json").write_text(
        json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps({
        "prompt_sha256": prompt_hash,
        "runs": RUNS,
        "dev_run0": evaluation["dev"]["runs"][0]["summary"]["ready"],
        "dev_run1": evaluation["dev"]["runs"][1]["summary"]["ready"],
        "dev_gates_run0": evaluation["dev"]["runs"][0]["summary"]["dry_run"]["hard_gates"],
        "stability_dev": evaluation["dev"]["stability"],
    }, ensure_ascii=False, indent=1) + "\n")


if __name__ == "__main__":
    main()
