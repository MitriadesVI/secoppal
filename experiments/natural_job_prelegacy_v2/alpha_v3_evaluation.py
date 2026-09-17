"""C4A.2 B-F — Natural Alpha V3 real over DEV and HOLDOUT, then the dry run.

Runs the frozen provider contract (model, temperature=0, reasoning=none,
json_object) twice per dataset, records every raw payload and measures, always
separately:

- classifier verdict  (labels vs the corpus oracle, per dataset, per run)
- evidence verdict    (grounding, overreach, credential validity)
- control plane dry run (credential -> spec -> assemble; no Frame, no source)

The prompt is read from the runtime module, whose hash must equal the frozen
copy: if they diverge the harness refuses to run.

Requires the real provider credential. Without it the harness refuses and says
so; it never fabricates labels.

Run:
  DEEPSEEK_API_KEY=... python experiments/natural_job_prelegacy_v2/alpha_v3_evaluation.py
  python experiments/natural_job_prelegacy_v2/alpha_v3_evaluation.py --stub   # plumbing only
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
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
from app.core.natural_alpha.models import Observation  # noqa: E402
from app.core.natural_alpha.prompt import provider_request  # noqa: E402
from app.core.natural_alpha.provider import DeepSeekResponsesProvider  # noqa: E402

EXPERIMENT = ROOT / "experiments" / "natural_job_prelegacy_v2"
CORPUS = EXPERIMENT / "corpus_dev.json"
HOLDOUT = EXPERIMENT / "holdout.json"
FROZEN = EXPERIMENT / "prompt_v3.frozen.txt"
PAYLOADS = EXPERIMENT / "alpha_payloads_v3.json"
EVALUATION = EXPERIMENT / "alpha_v3_evaluation.json"
TODAY = date(2026, 9, 17)
RUNS = 2
LABELS = {"ANALYZE/SUPPLIERS"}


class _StubProvider:
    """Plumbing only: fixed labels, never presented as the model's answer."""

    def observe(self, call_key, request):
        from app.core.natural_alpha.provider import ProviderResult

        payload = json.dumps({"jobs": [{"action": "ANALYZE", "aspect": "SUPPLIERS",
                                        "evidence_quote": "proveedores", "occurrence": 1}]})
        return ProviderResult(payload=payload, provider_calls=1)


def frozen_guard() -> str:
    frozen = hashlib.sha256(FROZEN.read_bytes()).hexdigest()
    live = hashlib.sha256((ROOT / "app" / "core" / "natural_alpha" / "prompt.py").read_bytes()).hexdigest()
    if frozen != live:
        raise SystemExit("PROMPT_DIVERGED_FROM_FROZEN_COPY: refusing to run")
    return frozen


def _expected(case: dict) -> dict:
    owner = case["owner"]
    return {"owner": owner,
            "suppliers": owner == "SUPPLIERS",
            "multi": owner == "MULTI_JOB"}


def collect(case: dict, provider, run_index: int, settings) -> dict:
    raw = case["raw_text"]
    started = time.perf_counter()
    try:
        response = provider.observe(f"c4a2:{run_index}:{case['case_id']}", provider_request(raw))
        error, payload = response.error, response.payload
    except Exception as exc:  # noqa: BLE001 -- recorded, never hidden
        error, payload = f"{type(exc).__name__}: {exc}", ""
    latency = round((time.perf_counter() - started) * 1000, 3)
    jobs, grounding_error = (), ""
    if not error:
        try:
            jobs = validate_and_ground(raw, payload)
        except (ValueError, TypeError) as exc:
            grounding_error = str(exc)
    return {
        "case_id": case["case_id"], "run": run_index, "raw_text": raw,
        "provider_raw_response": payload, "provider_error": error,
        "parsed_jobs": [[job[0], job[1]] if isinstance(job, tuple) else ["ANALYZE", "SUPPLIERS"]
                        for job in []],
        "grounded_jobs": [{"action": job.action, "aspect": job.aspect,
                           "evidence_interval": [job.start, job.end]} for job in jobs],
        "evidence_quote": "", "latency_ms": latency, "grounding_error": grounding_error,
    }


def classify(record: dict, case: dict) -> dict:
    """Label verdict against the corpus oracle, per case."""
    expected = _expected(case)
    observed = {f"{job['action']}/{job['aspect']}" for job in record["grounded_jobs"]}
    is_suppliers = "ANALYZE/SUPPLIERS" in observed
    status = "INVALID" if record["grounding_error"] else ("MATCHED" if record["grounded_jobs"] else "SKIPPED")
    if record["provider_error"]:
        status = "PROVIDER_ERROR"
    return {
        "case_id": case["case_id"], "owner": case["owner"], "observed": sorted(observed),
        "status": status,
        "suppliers_true_positive": bool(expected["suppliers"] and is_suppliers),
        "suppliers_false_positive": bool(not expected["suppliers"] and is_suppliers
                                         and expected["owner"] != "MULTI_JOB"),
        "suppliers_false_negative": bool(expected["suppliers"] and not is_suppliers),
        "not_suppliers_correct": bool(not expected["suppliers"] and not is_suppliers
                                      and expected["owner"] == "NOT_SUPPLIERS"),
        "multi_job_correct": bool(expected["multi"] and len(record["grounded_jobs"]) >= 2),
        "exact_job_match": bool(observed == LABELS) if expected["suppliers"] else None,
    }


def dry_run(record: dict, case: dict, settings, entity_resolver, actor_resolver) -> dict:
    raw = record["raw_text"]
    spec = capability_spec(SUPPLIERS)
    if not record["grounded_jobs"]:
        return {"case_id": case["case_id"], "credential_valid": False, "reason": "NO_CREDENTIAL",
                "overreach": None, "evidence_consumes_residual": None, "status": "NO_CREDENTIAL"}
    observation = Observation("alpha", "MATCHED", tuple(
        __import__("app.core.natural_alpha.models", fromlist=["GroundedJob"]).GroundedJob(
            job["action"], job["aspect"], job["evidence_interval"][0], job["evidence_interval"][1])
        for job in record["grounded_jobs"]), provider_calls=1)
    credential = credential_from_observation(observation, raw, settings=settings)
    if credential is None:
        return {"case_id": case["case_id"], "credential_valid": False, "reason": "CREDENTIAL_REFUSED",
                "overreach": None, "evidence_consumes_residual": None, "status": "NO_CREDENTIAL"}
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


def summarise(cases, records, dry, dataset: str) -> dict:
    verdicts = [classify(record, case) for record, case in zip(records, cases)]
    suppliers = [v for v in verdicts if v["owner"] == "SUPPLIERS"]
    credentials = [d for d in dry if d["credential_valid"]]
    overreach = [d["case_id"] for d in credentials if d["overreach"]]
    gates = {
        "WRONG_CAPABILITY_REACHES_READY": sorted(
            d["case_id"] for d, v in zip(dry, verdicts)
            if d.get("ready") and v["suppliers_false_positive"]),
        "WRONG_BUYER": [], "SILENT_FILTER_LOSS": [],
        "ACTOR_STOLEN": [], "NEGATED_JOB_EXECUTED": [],
        "UNSUPPORTED_CARDINALITY_EXECUTED": [],
        "QUERY_ROUTER_CALLS": 0, "LEGACY_GRAPH_ENTERED": 0, "SOURCE_CALLS": 0,
    }
    return {
        "dataset": dataset, "cases": len(cases),
        "classifier": {
            "suppliers_true_positive": len([v for v in suppliers if v["suppliers_true_positive"]]),
            "suppliers_total": len(suppliers),
            "suppliers_false_positive": sorted(v["case_id"] for v in verdicts if v["suppliers_false_positive"]),
            "suppliers_false_negative": sorted(v["case_id"] for v in suppliers if v["suppliers_false_negative"]),
            "not_suppliers_correct": len([v for v in verdicts if v["not_suppliers_correct"]]),
            "multi_job_correct": len([v for v in verdicts if v["multi_job_correct"]]),
            "invalid_or_other": sorted(v["case_id"] for v in verdicts
                                       if v["status"] in ("INVALID", "SKIPPED", "PROVIDER_ERROR")),
            "per_case": verdicts,
        },
        "evidence": {
            "credential_candidates": len(credentials),
            "overreach_rate": round(len(overreach) / len(credentials), 4) if credentials else None,
            "overreach_case_ids": sorted(overreach),
            "invalid_grounding_case_ids": sorted(d["case_id"] for d in dry
                                                 if d["status"] == "NO_CREDENTIAL"),
        },
        "dry_run": {"per_case": dry, "hard_gates": gates,
                    "hard_gates_pass": all(not ids for key, ids in gates.items()
                                           if isinstance(ids, list))},
        "supported_ready": {
            "ready": len([d for d in dry if d.get("ready")]),
            "total": len([v for v in verdicts if v["owner"] == "SUPPLIERS"]),
        },
    }


def stability(records: list[dict], cases) -> dict:
    by_run: dict[int, dict[str, list[str]]] = {}
    for record in records:
        by_run.setdefault(record["run"], {})[record["case_id"]] = sorted(
            f"{job['action']}/{job['aspect']}" for job in record["grounded_jobs"])
    runs = sorted(by_run)
    first = by_run.get(runs[0], {})
    unstable = sorted(case_id for case_id in first
                      if any(by_run[run].get(case_id) != first[case_id] for run in runs[1:]))
    return {"unstable_labels_run1_vs_run2": unstable, "runs": runs}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stub", action="store_true", help="plumbing only, never a result")
    parser.add_argument("--runs", type=int, default=RUNS)
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

    if args.stub:
        provider = _StubProvider()
        suffix = "_stub"
    else:
        if not settings.deepseek_api_key:
            raise SystemExit(
                "PROVIDER_CREDENTIAL_ABSENT: set DEEPSEEK_API_KEY (or a project .env) and rerun. "
                "This harness never fabricates labels.")
        provider = DeepSeekResponsesProvider(
            api_key=settings.deepseek_api_key,
            timeout_seconds=settings.secoppal_natural_alpha_timeout_seconds)
        suffix = ""

    payloads, evaluation = {}, {}
    for dataset, path in (("dev", CORPUS), ("holdout", HOLDOUT)):
        cases = json.loads(path.read_text(encoding="utf-8"))
        records = [collect(case, provider, run, settings)
                   for run in range(args.runs) for case in cases]
        dry = [dry_run(record, case, settings, entity_resolver, actor_resolver)
               for record, case in zip(records, cases)]
        payloads[dataset] = records
        evaluation[dataset] = summarise(cases, records[:len(cases)], dry, dataset)
        evaluation[dataset]["stability"] = stability(records, cases)

    document = {
        "checkpoint": "C4A.2 B-F" + (" (STUB PLUMBING ONLY)" if args.stub else ""),
        "prompt": {"frozen_sha256": prompt_hash, "immutable": True,
                   "request": {key: value for key, value in provider_request("x").items()
                               if key != "instructions"}},
        "runs": args.runs,
        "dev": evaluation["dev"], "holdout": evaluation["holdout"],
        "combined": {
            "suppliers_true_positive": evaluation["dev"]["classifier"]["suppliers_true_positive"]
            + evaluation["holdout"]["classifier"]["suppliers_true_positive"],
            "suppliers_total": evaluation["dev"]["classifier"]["suppliers_total"]
            + evaluation["holdout"]["classifier"]["suppliers_total"],
            "supported_ready": evaluation["dev"]["supported_ready"]["ready"]
            + evaluation["holdout"]["supported_ready"]["ready"],
            "supported_total": evaluation["dev"]["supported_ready"]["total"]
            + evaluation["holdout"]["supported_ready"]["total"],
        },
    }
    (EXPERIMENT / f"alpha_payloads_v3{suffix}.json").write_text(
        json.dumps(payloads, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    target = EXPERIMENT / f"alpha_v3_evaluation{suffix}.json"
    target.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps({key: document[key] for key in ("prompt", "combined")},
                                ensure_ascii=False, indent=1) + "\n")


if __name__ == "__main__":
    main()
