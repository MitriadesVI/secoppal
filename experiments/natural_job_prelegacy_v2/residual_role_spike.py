"""C4A.4 — Residual role spike: B (LLM verifier) vs C (local linguistic verifier).

Spike only: nothing here is wired into the product runtime.

Preprocessing is frozen first (residual_role_inputs.json): every unit B and C see
comes from the C4A.3 credentialed lane -- an ANALYZE/SUPPLIERS credential, the
host authorities and the functional closed-class cleanup. Neither alternative may
resolve buyer, time, money, modality, geography or touch QueryRouter.

Gates are fixed before running (C4A.4 E):
  E1 MATERIAL_FALSE_NEGATIVE == 0  (any MATERIAL_PRESENT that obtains PASS rejects)
  E2 JOB_ONLY stable PASS rate: >= .80 VIABLE, .60-.79 WEAK, < .60 REJECT
Decision: safety always beats coverage.

Run:
  python experiments/natural_job_prelegacy_v2/residual_role_spike.py --build
  DEEPSEEK_API_KEY=... python experiments/natural_job_prelegacy_v2/residual_role_spike.py --run-b
  python experiments/natural_job_prelegacy_v2/residual_role_spike.py --run-c  [--spacy-python PATH]
  python experiments/natural_job_prelegacy_v2/residual_role_spike.py --decide
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

EXPERIMENT = ROOT / "experiments" / "natural_job_prelegacy_v2"
HOLDOUT = EXPERIMENT / "residual_role_holdout.json"
INPUTS = EXPERIMENT / "residual_role_inputs.json"
PROMPT_B = EXPERIMENT / "residual_role_prompt_b.txt"
RESULTS_B = EXPERIMENT / "residual_role_results_b.json"
RESULTS_C = EXPERIMENT / "residual_role_results_c.json"
RESULTS_C_RAW = EXPERIMENT / "residual_role_pos_observed.json"
DECISION = EXPERIMENT / "residual_role_decision.json"
MODEL = "deepseek-v4-flash"
RUNS = 2
LABELS = {"JOB_LANGUAGE", "MATERIAL", "UNKNOWN"}
# Frozen before evaluating (C4A.4 D).
POS_POLICY = {
    "MATERIAL": {"NOUN", "PROPN", "ADJ", "NUM"},
    "JOB_LANGUAGE": {"PRON", "DET", "ADP", "AUX", "ADV", "PART", "SCONJ", "CCONJ", "PUNCT"},
    "UNKNOWN": {"VERB"},
}
# MATERIAL wins over UNKNOWN wins over JOB_LANGUAGE.
SEVERITY = {"JOB_LANGUAGE": 0, "UNKNOWN": 1, "MATERIAL": 2}


def sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_cases() -> list[dict]:
    return json.loads(HOLDOUT.read_text(encoding="utf-8"))["cases"]


def build_inputs() -> dict:
    """One preprocessing for both alternatives: the C4A.3 residual units."""
    from app.config import Settings
    from app.core.accreditation import SUPPLIERS, capability_spec, credential_from_observation
    from app.core.accreditation.assembly import assemble
    from app.core.entity_resolver import EntityResolver
    from app.core.natural_alpha.models import GroundedJob, Observation
    from app.data.actor_alias_resolver import ActorNameResolver

    scratch = pathlib.Path(tempfile.mkdtemp(prefix="c4a4-"))
    settings = Settings(_env_file=None, secoppal_a2_suppliers_binding_enabled=True,
                        secop_feedback_path=scratch / "traces.jsonl",
                        secop_feedback_db_path=scratch / "feedback.db")
    entity_resolver = EntityResolver(settings.secop_alias_db_path)
    actor_resolver = ActorNameResolver(settings.secop_actor_alias_index_path)
    spec = capability_spec(SUPPLIERS)

    entries, host_failures = [], []
    for case in load_cases():
        raw = case["raw_text"]
        observation = Observation("alpha", "MATCHED",
                                  (GroundedJob("ANALYZE", "SUPPLIERS", 0, len(raw)),),
                                  provider_calls=1)
        credential = credential_from_observation(observation, raw, settings=settings)
        if credential is None:
            host_failures.append({"case_id": case["case_id"], "reason": "CREDENTIAL_REFUSED"})
            continue
        assembly = assemble(raw, credential, spec, entity_resolver=entity_resolver,
                            actor_resolver=actor_resolver)
        units = [{"id": f"R{index + 1}", "text": text}
                 for index, text in enumerate(assembly.residual_material)]
        entries.append({
            "case_id": case["case_id"], "group": case["group"], "gold": case["gold"],
            "raw": raw, "assembly_status": assembly.status.value,
            "assembly_reason": assembly.reason, "units": units,
        })
        if case["gold"] == "MATERIAL_PRESENT" and not units:
            # The host left nothing to judge: reported apart, never blamed on B/C.
            host_failures.append({"case_id": case["case_id"], "reason": "EMPTY_RESIDUAL"})
    document = {
        "checkpoint": "C4A.4 B/C preprocessing",
        "corpus_sha256": sha256(HOLDOUT),
        "prompt_b_sha256": sha256(PROMPT_B),
        "note": "Unidades = residual_material del carril credenciado (C4A.3). Ni B ni C vuelven a resolver nada.",
        "host_failures": host_failures,
        "cases": entries,
    }
    INPUTS.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return document


def _request(instructions: str, raw: str, units: list[dict]) -> dict:
    body = {
        "raw_query": raw,
        "human_job": "ANALYZE/SUPPLIERS",
        "residual_units": [f'{unit["id"]}: "{unit["text"]}"' for unit in units],
    }
    return {
        "model": MODEL,
        "temperature": 0,
        "reasoning": {"effort": "none"},
        "tools": [],
        "max_output_tokens": 512,
        "text": {"format": {"type": "json_object"}},
        "input": json.dumps({"text": instructions + "\n\nENTRADA\n" + json.dumps(body, ensure_ascii=False)},
                            ensure_ascii=False),
    }


def _validate(payload: str, units: list[dict]) -> tuple[dict | None, str]:
    """Host validation: exact ids, valid labels, nothing missing, nothing new."""
    try:
        parsed = json.loads(payload)
        labels = parsed["labels"]
    except Exception as exc:  # noqa: BLE001
        return None, f"MALFORMED:{type(exc).__name__}"
    seen: dict[str, str] = {}
    for entry in labels:
        if not isinstance(entry, dict) or "id" not in entry or "label" not in entry:
            return None, "MALFORMED:SHAPE"
        if entry["label"] not in LABELS:
            return None, f"INVALID_LABEL:{entry.get('label')}"
        if entry["id"] in seen:
            return None, f"DUPLICATE_ID:{entry['id']}"
        seen[entry["id"]] = entry["label"]
    expected = {unit["id"] for unit in units}
    if set(seen) - expected:
        return None, f"UNKNOWN_ID:{sorted(set(seen) - expected)}"
    if expected - set(seen):
        return None, f"MISSING_ID:{sorted(expected - set(seen))}"
    return seen, "OK"


def run_b(spacy_python: str | None = None) -> dict:
    del spacy_python
    from app.core.natural_alpha.provider import DeepSeekResponsesProvider

    instructions = PROMPT_B.read_text(encoding="utf-8")
    provider = DeepSeekResponsesProvider(api_key=os.environ.get("DEEPSEEK_API_KEY", ""),
                                         timeout_seconds=60)
    if not os.environ.get("DEEPSEEK_API_KEY"):
        raise SystemExit("PROVIDER_CREDENTIAL_ABSENT: export DEEPSEEK_API_KEY (spike B)")
    document = json.loads(INPUTS.read_text(encoding="utf-8"))
    runs = []
    latencies = []
    for run_index in range(RUNS):
        case_results = []
        for case in document["cases"]:
            units = case["units"]
            started = time.perf_counter()
            response = provider.observe(f"c4a4:{run_index}:{case['case_id']}",
                                        _request(instructions, case["raw"], units))
            latencies.append(round((time.perf_counter() - started) * 1000, 3))
            labels, validation = (None, "NO_UNITS") if not units else _validate(
                response.payload, units)
            if response.error:
                labels, validation = None, f"PROVIDER_ERROR:{response.error}"
            stable_pass = labels is not None and all(v == "JOB_LANGUAGE" for v in labels.values())
            case_results.append({
                "case_id": case["case_id"], "group": case["group"], "gold": case["gold"],
                "raw_payload": response.payload, "validation": validation,
                "labels": labels, "pass": bool(stable_pass),
                "blocked_reason": None if stable_pass else validation,
            })
        runs.append({"run": run_index, "cases": case_results})
    labels_by_run = [{c["case_id"]: c.get("labels") for c in run["cases"]} for run in runs]
    unstable_labels = sorted(
        case_id for case_id in labels_by_run[0]
        if any(labels_by_run[index].get(case_id) != labels_by_run[0].get(case_id)
               for index in range(1, len(labels_by_run))))
    unstable_pass = sorted(
        case["case_id"] for case in runs[0]["cases"]
        if any(other["pass"] != case["pass"] for run in runs[1:] for other in run["cases"]
               if other["case_id"] == case["case_id"]))
    result = {"alternative": "B_LLM", "model": MODEL, "runs": runs,
              "stability": {"unstable_unit_labels": unstable_labels,
                            "unstable_query_pass": unstable_pass},
              "cost": {"calls": len(latencies), "p50_ms": round(statistics.median(latencies), 1),
                       "p95_ms": round(sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)], 1),
                       "estimated_calls_per_turn": RUNS / RUNS}}
    RESULTS_B.write_text(json.dumps(result, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return result


POS_SCRIPT = r"""
import json, pathlib, sys
import spacy
nlp = spacy.load("es_core_news_sm")
document = json.loads(pathlib.Path(sys.argv[1]).read_text())
observed = {}
for case in document["cases"]:
    observed[case["case_id"]] = {}
    for unit in case["units"]:
        doc = nlp(unit["text"])
        observed[case["case_id"]][unit["id"]] = [
            {"text": token.text, "pos": token.pos_, "dep": token.dep_,
             "morph": str(token.morph), "lemma": token.lemma_} for token in doc]
print(json.dumps(observed, ensure_ascii=False))
"""


def run_c(spacy_python: str) -> dict:
    observed_raw = subprocess.run([spacy_python, "-c", POS_SCRIPT, str(INPUTS)],
                                  capture_output=True, text=True, check=True)
    observed = json.loads(observed_raw.stdout)
    RESULTS_C_RAW.write_text(json.dumps(observed, ensure_ascii=False, indent=1) + "\n",
                             encoding="utf-8")
    document = json.loads(INPUTS.read_text(encoding="utf-8"))
    case_results = []
    for case in document["cases"]:
        labels = {}
        for unit in case["units"]:
            tokens = observed.get(case["case_id"], {}).get(unit["id"], [])
            verdict = "JOB_LANGUAGE"
            for token in tokens:
                pos = token["pos"]
                role = next((name for name, kinds in POS_POLICY.items() if pos in kinds), "UNKNOWN")
                if SEVERITY[role] > SEVERITY[verdict]:
                    verdict = role
            labels[unit["id"]] = verdict
        stable_pass = bool(labels) and all(v == "JOB_LANGUAGE" for v in labels.values())
        case_results.append({"case_id": case["case_id"], "group": case["group"],
                             "gold": case["gold"], "labels": labels, "pass": stable_pass,
                             "blocked_reason": None if stable_pass else "NON_JOB_LABEL"})
    result = {"alternative": "C_LOCAL", "frozen_policy": {k: sorted(v) for k, v in POS_POLICY.items()},
              "runs": [{"run": 0, "cases": case_results}],
              "stability": {"unstable_unit_labels": [], "unstable_query_pass": [],
                            "note": "C es determinista: una corrida; se repite en --run-c para medir latencia"},
              "cost": {}}
    RESULTS_C.write_text(json.dumps(result, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return result


def gates(result: dict, passes_by_case: dict[str, bool]) -> dict:
    cases = load_cases()
    material_fn = sorted(case["case_id"] for case in cases
                         if case["gold"] == "MATERIAL_PRESENT" and passes_by_case.get(case["case_id"]))
    job_only = [case["case_id"] for case in cases if case["gold"] == "JOB_ONLY"]
    job_pass = sorted(case_id for case_id in job_only if passes_by_case.get(case_id))
    rate = len(job_pass) / len(job_only) if job_only else 0.0
    return {
        "alternative": result["alternative"],
        "MATERIAL_FALSE_NEGATIVE": len(material_fn),
        "material_false_negative_case_ids": material_fn,
        "SAFETY_PASS": len(material_fn) == 0,
        "JOB_ONLY_PASS": len(job_pass),
        "JOB_ONLY_TOTAL": len(job_only),
        "JOB_ONLY_RATE": round(rate, 4),
        "coverage_band": "VIABLE" if rate >= 0.80 else ("WEAK" if rate >= 0.60 else "REJECT"),
        "STATUS": ("REJECT" if material_fn else
                   ("VIABLE" if rate >= 0.80 else ("WEAK" if rate >= 0.60 else "COVERAGE_LOW"))),
    }


def decide() -> dict:
    cases = {case["case_id"]: case for case in load_cases()}
    verdicts = {}
    b = json.loads(RESULTS_B.read_text(encoding="utf-8"))
    b_pass = {case["case_id"]: all(
        other["pass"] for run in b["runs"] for other in run["cases"]
        if other["case_id"] == case["case_id"]) for case in cases.values()}
    verdicts["B_LLM"] = gates({"alternative": "B_LLM"}, b_pass)
    verdicts["B_LLM"]["stability"] = b["stability"]
    verdicts["B_LLM"]["cost"] = b["cost"]
    if RESULTS_C.exists():
        c = json.loads(RESULTS_C.read_text(encoding="utf-8"))
        c_pass = {case["case_id"]: case["pass"] for case in c["runs"][0]["cases"]}
        verdicts["C_LOCAL"] = gates({"alternative": "C_LOCAL"}, c_pass)
        verdicts["C_LOCAL"]["stability"] = c["stability"]
        verdicts["C_LOCAL"]["cost"] = c["cost"]
    b_ok = verdicts["B_LLM"]["SAFETY_PASS"] and verdicts["B_LLM"]["JOB_ONLY_RATE"] >= 0.80
    c_ok = bool(verdicts.get("C_LOCAL")) and verdicts["C_LOCAL"]["SAFETY_PASS"] \
        and verdicts["C_LOCAL"]["JOB_ONLY_RATE"] >= 0.80
    if verdicts["B_LLM"]["MATERIAL_FALSE_NEGATIVE"] == 0 and not c_ok and not b_ok:
        winner = "NONE_ABANDON_RESIDUAL_VERIFIER_PATH"
    elif verdicts["B_LLM"]["MATERIAL_FALSE_NEGATIVE"] > 0:
        winner = "C_LOCAL" if c_ok else "NONE_ABANDON_RESIDUAL_VERIFIER_PATH"
    elif b_ok and not c_ok:
        winner = "B_LLM"
    elif c_ok and not b_ok:
        winner = "C_LOCAL"
    elif b_ok and c_ok:
        difference = verdicts["B_LLM"]["JOB_ONLY_RATE"] - verdicts["C_LOCAL"]["JOB_ONLY_RATE"]
        winner = "B_LLM" if difference > 0.15 else "C_LOCAL"
    else:
        winner = "NONE_ABANDON_RESIDUAL_VERIFIER_PATH"
    document = {
        "checkpoint": "C4A.4 decision",
        "corpus_sha256": sha256(HOLDOUT), "inputs_sha256": sha256(INPUTS),
        "prompt_b_sha256": sha256(PROMPT_B),
        "frozen_before_evaluation": {
            "MATERIAL_FN_must_be": 0,
            "JOB_ONLY_rate_bands": {"VIABLE": ">=0.80", "WEAK": "0.60-0.79", "REJECT": "<0.60"},
            "tie_break": "si ambos cumplen gana C_LOCAL salvo >15 pp de cobertura",
        },
        "verdicts": verdicts,
        "winner": winner,
        "integration": "NOT_INTEGRATED (spike only)",
    }
    DECISION.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return document


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--run-b", action="store_true")
    parser.add_argument("--run-c", action="store_true")
    parser.add_argument("--decide", action="store_true")
    parser.add_argument("--spacy-python", default="/tmp/secoppal-c4a4-spacy/.venv/bin/python")
    args = parser.parse_args()
    if args.build:
        document = build_inputs()
        print(json.dumps({"cases": len(document["cases"]),
                          "host_failures": document["host_failures"]}, ensure_ascii=False))
    if args.run_b:
        print(json.dumps(run_b(), ensure_ascii=False)[:400])
    if args.run_c:
        started = time.perf_counter()
        run_c(args.spacy_python)
        print(json.dumps({"cold_run_ms": round((time.perf_counter() - started) * 1000, 1)}))
    if args.decide:
        print(json.dumps(decide(), ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
