"""C4A.5 — B transfer validation, actor collision audit and the two orderings.

Everything here is measurement: no runtime integration, no guard repair, no
prompt tuning. The frozen artifacts (corpus, prompt B, inputs, spike, manifest)
are read only.

Flags:
  --audit      actor_exact_guard collision audit over DEV + HOLDOUT
  --transfer   B over the real residuals of DEV + HOLDOUT (2 runs, frozen prompt)
  --simulate   CURRENT_ORDER vs B_BEFORE_WEAK_ACTOR_GUARD, using recorded labels
"""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

EXPERIMENT = ROOT / "experiments" / "natural_job_prelegacy_v2"
PROMPT_B = EXPERIMENT / "residual_role_prompt_b.txt"
REPLAY = EXPERIMENT / "alpha_v3_evaluation_replay.json"
AUDIT = EXPERIMENT / "actor_collision_audit.json"
TRANSFER = EXPERIMENT / "residual_role_transfer_b.json"
SIMULATION = EXPERIMENT / "residual_role_ordering_simulation.json"
RUNS = 2
MODEL = "deepseek-v4-flash"
LABELS = {"JOB_LANGUAGE", "MATERIAL", "UNKNOWN"}


def _world():
    from app.config import Settings
    from app.core.entity_resolver import EntityResolver
    from app.data.actor_alias_resolver import ActorNameResolver

    scratch = pathlib.Path(tempfile.mkdtemp(prefix="c4a5-"))
    settings = Settings(_env_file=None, secoppal_a2_suppliers_binding_enabled=True,
                        secop_feedback_path=scratch / "traces.jsonl",
                        secop_feedback_db_path=scratch / "feedback.db")
    return (settings, EntityResolver(settings.secop_alias_db_path),
            ActorNameResolver(settings.secop_actor_alias_index_path))


def _cases() -> dict:
    corpus = json.loads((EXPERIMENT / "corpus_dev.json").read_text(encoding="utf-8"))
    holdout = json.loads((EXPERIMENT / "holdout.json").read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in corpus + holdout}


def _replay_cases() -> dict:
    """Per-case dry result of the recorded real run (C4A.3 lane)."""
    document = json.loads(REPLAY.read_text(encoding="utf-8"))
    per_case: dict = {}
    for dataset in ("dev", "holdout"):
        for run in document[dataset]["runs"]:
            for result in run["summary"]["dry_run"]["per_case"]:
                current = per_case.setdefault(result["case_id"], result)
                if result["ready"]:
                    current["ready_run1"] = True
                if result["status"] == "READY":
                    current["ready_any"] = True
    return per_case


# ── actor collision audit ────────────────────────────────────────────────────

def audit() -> dict:
    from app.core.accreditation import authorities as A
    from app.core.accreditation import guards as G
    from app.core.accreditation.guards import actor_exact_guard

    settings, entity_resolver, actor_resolver = _world()
    corpus = _cases()
    triggers = []
    for case_id, case in sorted(corpus.items()):
        raw = case["raw_text"]
        # Same inputs the credentialed assembly gives the guard (assembly.py): the
        # spans every other authority already interpreted are protected.
        buyer = A.buyer_authority(raw, entity_resolver=entity_resolver)
        buy = tuple((s.start, s.end) for s in buyer.spans if raw[s.start:s.end] == raw[s.start:s.end])
        buy_spans = tuple(buyer.spans)
        protected = tuple((span.start, span.end) for span in buy_spans)
        buyer_span = protected[0] if protected else None
        time = A.time_authority(raw, today=None, protected_spans=protected)
        time_spans = tuple(time.spans)
        actor = A.actor_authority(raw)
        ranking = A.ranking_authority(raw, protected_spans=protected + tuple(
            (span.start, span.end) for span in time_spans))
        guard = A.supplier_guard(raw)
        buyer_surface = next((span.canonical_value for span in buy_spans if span.canonical_value), "")
        source = A.source_authority(raw, protected_surfaces=(buyer_surface,) if buyer_surface else ())
        modality = G.modality_guard(raw)
        money = G.money_guard(raw)
        geography = G.geography_guard(raw, buyer_span=buyer_span)
        interpreted = tuple(
            (span.start, span.end)
            for report in (buyer, time, actor, ranking, guard, source, modality, money, geography)
            for span in report.spans)
        captured: list = []
        original = actor_resolver.exact_aliases_in

        def spy(text, _original=original, _captured=captured):
            found = _original(text)
            _captured.extend(found)
            return found

        actor_resolver.exact_aliases_in = spy
        try:
            report = actor_exact_guard(raw, actor_resolver=actor_resolver,
                                       protected_spans=interpreted)
        finally:
            actor_resolver.exact_aliases_in = original
        if report.detail != "ACTOR_MENTION_PRESENT":
            continue  # the guard did not use any alias for this turn
        family = case.get("family")
        for span in report.spans:
            triggers.append({
                "case_id": case_id, "raw": raw, "alias": span.canonical_value,
                "span": [span.start, span.end],
                "alias_text_in_raw": raw[span.start:span.end],
                "identity_count_or_status": f"exact alias present in index; guard spans={len(report.spans)}",
                "guard_status": report.status, "guard_detail": report.detail,
                "aliases_matched_this_turn": [entry[0] for entry in captured],
                "family": family,
                "gold_actor_subject": family == "actor_subject",
                "classification": "TRUE_ACTOR_TRIGGER" if family == "actor_subject"
                else "FALSE_ACTOR_TRIGGER",
            })
    true_triggers = [t for t in triggers if t["classification"] == "TRUE_ACTOR_TRIGGER"]
    false_triggers = [t for t in triggers if t["classification"] == "FALSE_ACTOR_TRIGGER"]
    document = {
        "checkpoint": "C4A.5 actor collision audit",
        "index": str(settings.secop_actor_alias_index_path),
        "guard_modified": False,
        "trigger_total": len(triggers),
        "true_actor": len(true_triggers),
        "false_actor": len(false_triggers),
        "false_trigger_rate": round(len(false_triggers) / len(triggers), 4) if triggers else None,
        "triggers": triggers,
    }
    AUDIT.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return document


# ── B transfer over the real DEV/HOLDOUT residuals ──────────────────────────

def _key() -> str:
    import os
    if os.environ.get("DEEPSEEK_API_KEY"):
        return os.environ["DEEPSEEK_API_KEY"]
    for line in pathlib.Path("/Users/rodrigoortiz/Documents/secoppal/.env").read_text().splitlines():
        name, _, value = line.partition("=")
        if name.strip() == "DEEPSEEK_API_KEY":
            return value.strip().strip('"').strip("'")
    return ""


def transfer() -> dict:
    from app.core.natural_alpha.provider import DeepSeekResponsesProvider

    key = _key()
    if not key:
        raise SystemExit("PROVIDER_CREDENTIAL_ABSENT: no DEEPSEEK_API_KEY in env or project .env")
    instructions = PROMPT_B.read_text(encoding="utf-8")
    provider = DeepSeekResponsesProvider(api_key=key, timeout_seconds=60)
    corpus = _cases()
    replay = _replay_cases()
    eligible = [
        {"case_id": case_id, "raw": corpus[case_id]["raw_text"],
         "outcome": corpus[case_id].get("outcome"), "owner": corpus[case_id]["owner"],
         "family": corpus[case_id].get("family"),
         "units": [{"id": f"R{index + 1}", "text": text}
                   for index, text in enumerate(result["residual_material"])],
         "assembly_reason": result["reason"]}
        for case_id, result in sorted(replay.items())
        if result["credential_valid"] and result["residual_material"]
    ]
    runs, latencies = [], []
    for run_index in range(RUNS):
        results = []
        for case in eligible:
            from residual_role_spike import _request, _validate  # same frozen contract
            started = time.perf_counter()
            response = provider.observe(f"c4a5:{run_index}:{case['case_id']}",
                                        _request(instructions, case["raw"], case["units"]))
            latencies.append(round((time.perf_counter() - started) * 1000, 3))
            labels, validation = _validate(response.payload, case["units"])
            if response.error:
                labels, validation = None, f"PROVIDER_ERROR:{response.error}"
            results.append({**case, "labels": labels, "validation": validation,
                            "pass": labels is not None and all(v == "JOB_LANGUAGE" for v in labels.values()),
                            "raw_payload": response.payload})
        runs.append({"run": run_index, "cases": results})
    by_run = [{result["case_id"]: result for result in run["cases"]} for run in runs]
    unstable_labels = sorted(case["case_id"] for case in eligible
                             if any(by_run[index][case["case_id"]]["labels"]
                                    != by_run[0][case["case_id"]]["labels"]
                                    for index in range(1, len(by_run))))
    unstable_pass = sorted(case["case_id"] for case in eligible
                           if any(by_run[index][case["case_id"]]["pass"]
                                  != by_run[0][case["case_id"]]["pass"]
                                  for index in range(1, len(by_run))))
    material_cases = [case for case in eligible if case["outcome"] == "HONEST_CLARIFY"]
    clear_cases = [case for case in eligible if case["outcome"] == "EXECUTE"]
    material_fn = sorted(case["case_id"] for case in material_cases
                         if all(by_run[index][case["case_id"]]["pass"] for index in range(RUNS)))
    clear_pass = sorted(case["case_id"] for case in clear_cases
                        if all(by_run[index][case["case_id"]]["pass"] for index in range(RUNS)))
    attention = ("X01", "P01", "H05", "X03", "D01", "A01", "A02", "H15")
    document = {
        "checkpoint": "C4A.5 B transfer",
        "prompt_b_sha256_used": __import__("hashlib").sha256(PROMPT_B.read_bytes()).hexdigest(),
        "eligible": len(eligible), "runs": RUNS, "model": MODEL,
        "MATERIAL_FALSE_NEGATIVE": len(material_fn),
        "material_false_negative_case_ids": material_fn,
        "MATERIAL_FALSE_NEGATIVE_NOMINAL": sorted(
            case["case_id"] for case in material_cases
            if case["family"] in ("material_verbal_paraphrase",) ),
        "CLEAR_JOB_LANGUAGE_PASS": len(clear_pass),
        "CLEAR_JOB_LANGUAGE_TOTAL": len(clear_cases),
        "clear_pass_case_ids": clear_pass,
        "blocked_case_ids": sorted(case["case_id"] for case in eligible
                                   if not all(by_run[index][case["case_id"]]["pass"]
                                              for index in range(RUNS))),
        "unstable_unit_labels": unstable_labels, "unstable_query_pass": unstable_pass,
        "cost": {"calls": len(latencies), "p50_ms": round(statistics.median(latencies), 1),
                 "p95_ms": round(sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)], 1)},
        "attention_cases": {
            case_id: {"labels": by_run[0][case_id]["labels"],
                      "pass": by_run[0][case_id]["pass"],
                      "units": by_run[0][case_id]["units"],
                      "family": by_run[0][case_id]["family"],
                      "outcome": by_run[0][case_id]["outcome"]}
            for case_id in attention if case_id in by_run[0]},
        "runs": runs,
    }
    TRANSFER.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return document


# ── ordering simulation (no runtime change) ─────────────────────────────────

def simulate() -> dict:
    from app.core.accreditation import SUPPLIERS, capability_spec, credential_from_observation
    from app.core.accreditation.assembly import assemble
    from app.core.natural_alpha.models import GroundedJob, Observation

    settings, entity_resolver, actor_resolver = _world()
    corpus = _cases()
    replay = _replay_cases()
    transfer_doc = json.loads(TRANSFER.read_text(encoding="utf-8"))
    b_by_case = {case["case_id"]: case for case in
                 {c["case_id"]: c for c in transfer_doc["runs"][0]["cases"]}.values()}
    spec = capability_spec(SUPPLIERS)
    rows = []
    for case_id, case in sorted(corpus.items()):
        result = replay.get(case_id)
        if not result or not result["credential_valid"]:
            continue
        raw = case["raw_text"]
        observation = Observation("alpha", "MATCHED",
                                  (GroundedJob("ANALYZE", "SUPPLIERS", 0, len(raw)),), provider_calls=1)
        credential = credential_from_observation(observation, raw, settings=settings)
        if credential is None:
            continue
        current = assemble(raw, credential, spec, entity_resolver=entity_resolver,
                           actor_resolver=actor_resolver)
        weak_block = (current.reason == "ACTOR_MENTION_PRESENT")
        b_result = b_by_case.get(case_id)
        b_pass = bool(b_result and b_result["pass"])
        strong_actor = current.reason in ("ACTOR_MENTION_PRESENT",) and case.get("family") == "actor_subject"
        # Simulated ordering: authorities -> B -> weak exact actor guard.
        if strong_actor:
            simulated_status, simulated_reason = "OTHER_OWNER", "actor_subject"
        elif current.reason == "UNACCREDITED_RESIDUAL":
            if b_pass:
                simulated_status, simulated_reason = "READY", "accredited"
            else:
                simulated_status, simulated_reason = "CLARIFY", "residual_material"
        elif b_result is None:
            simulated_status, simulated_reason = current.status.value, current.reason
        elif not b_pass:
            simulated_status, simulated_reason = "CLARIFY", "residual_material"
        elif weak_block:
            simulated_status, simulated_reason = "CLARIFY", "ACTOR_MENTION_PRESENT"
        else:
            simulated_status, simulated_reason = current.status.value, current.reason
        rows.append({
            "case_id": case_id, "family": case.get("family"), "outcome": case.get("outcome"),
            "material": list(result["residual_material"]),
            "current_order": {"status": current.status.value, "reason": current.reason,
                              "ready": current.status.value == "READY",
                              "false_actor_block": bool(weak_block and case.get("family") != "actor_subject")},
            "b_says": (b_result["labels"] if b_result else None),
            "simulated_B_before_weak_actor_guard": {
                "status": simulated_status, "reason": simulated_reason,
                "ready": simulated_status == "READY",
                "false_actor_block": False},
        })
    current_ready = [row["case_id"] for row in rows if row["current_order"]["ready"]]
    simulated_ready = [row["case_id"] for row in rows if row["simulated_B_before_weak_actor_guard"]["ready"]]
    document = {
        "checkpoint": "C4A.5 ordering simulation (no runtime change)",
        "note": "Simulacion: el strong actor authority (detect_actor_name) no se mueve; solo cambia donde vive el guard exacto debil.",
        "cases": rows,
        "CURRENT_ORDER": {
            "supported_ready": current_ready, "supported_ready_count": len(current_ready),
            "false_actor_blocks": [row["case_id"] for row in rows
                                   if row["current_order"]["false_actor_block"]],
        },
        "B_BEFORE_WEAK_ACTOR_GUARD": {
            "supported_ready": simulated_ready, "supported_ready_count": len(simulated_ready),
            "false_actor_blocks": [],
        },
    }
    SIMULATION.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return document


def main() -> None:
    parser = argparse.ArgumentParser()
    for flag in ("--audit", "--transfer", "--simulate"):
        parser.add_argument(flag, action="store_true")
    args = parser.parse_args()
    if args.audit:
        document = audit()
        print(json.dumps({key: document[key] for key in
                          ("trigger_total", "true_actor", "false_actor", "false_trigger_rate")},
                         ensure_ascii=False))
        print(json.dumps([{k: t[k] for k in ("case_id", "alias", "span", "classification", "guard_status")}
                          for t in document["triggers"]], ensure_ascii=False, indent=1))
    if args.transfer:
        document = transfer()
        print(json.dumps({key: document[key] for key in
                          ("eligible", "MATERIAL_FALSE_NEGATIVE", "material_false_negative_case_ids",
                           "CLEAR_JOB_LANGUAGE_PASS", "CLEAR_JOB_LANGUAGE_TOTAL", "blocked_case_ids",
                           "unstable_unit_labels", "unstable_query_pass", "cost")}, ensure_ascii=False))
    if args.simulate:
        document = simulate()
        print(json.dumps({key: document[key] for key in
                          ("CURRENT_ORDER", "B_BEFORE_WEAK_ACTOR_GUARD")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
