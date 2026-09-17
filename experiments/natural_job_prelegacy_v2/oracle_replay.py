"""C4A.12 — two separate evaluations, never mixed.

A. CONTROL PLANE ORACLE (host only): the credential witnesses the host-free
   portion of the turn — what a correct citation would be. Measures WRONG_BUYER,
   SILENT_FILTER_LOSS, ACTOR_STOLEN, NEGATED_JOB_EXECUTED,
   UNSUPPORTED_CARDINALITY_EXECUTED. The structural zeros (QueryRouter, legacy
   graph, source) are witnessed by the real spies in the C4A tests.

B. EVIDENCE QUALITY (model): how many turns would reach READY if the credential
   cited the supplier mention, or the whole sentence as V1 measured. Reported
   apart: an overreaching citation is not a control-plane defect.

C. END-TO-END ALPHA (classifier): needs recorded V3 payloads; without them this
   section says so instead of inventing numbers.

Run: python experiments/natural_job_prelegacy_v2/oracle_replay.py
"""
from __future__ import annotations

import json
import pathlib
import sys
import tempfile
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.core.accreditation import SUPPLIERS, capability_spec, credential_from_observation  # noqa: E402
from app.core.accreditation.assembly import AssemblyStatus, assemble  # noqa: E402
from app.core.capability_binding.supplier_request_authority import assess_supplier_request  # noqa: E402
from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.core.natural_alpha.models import GroundedJob, Observation  # noqa: E402

CORPUS = ROOT / "experiments" / "natural_job_prelegacy_v2" / "corpus_dev.json"
OUT = ROOT / "experiments" / "natural_job_prelegacy_v2" / "oracle_replay.json"
TODAY = date(2026, 9, 17)
GATES = ("WRONG_BUYER", "SILENT_FILTER_LOSS", "ACTOR_STOLEN", "NEGATED_JOB_EXECUTED",
         "UNSUPPORTED_CARDINALITY_EXECUTED")
HOST_OWNED = ("BUYER", "TIME", "ACTOR", "SOURCE", "RANKING", "MONEY", "MODALITY",
              "GEOGRAPHY", "TOPIC")


def _world():
    root = pathlib.Path(tempfile.mkdtemp(prefix="c4a-replay-"))
    settings = Settings(
        _env_file=None, secoppal_a2_suppliers_binding_enabled=True,
        secop_feedback_path=root / "traces.jsonl", secop_feedback_db_path=root / "feedback.db",
    )
    from app.data.actor_alias_resolver import ActorNameResolver

    return (settings, EntityResolver(settings.secop_alias_db_path),
            ActorNameResolver(settings.secop_actor_alias_index_path))


def _credential(raw, settings, interval):
    start, end = interval
    job = GroundedJob("ANALYZE", "SUPPLIERS", start, end)
    observation = Observation("alpha", "MATCHED", (job,), provider_calls=1)
    return credential_from_observation(observation, raw, settings=settings)


def _oracle_interval(raw: str, host_spans) -> tuple[int, int]:
    """What a correct citation for this turn would be.

    The job phrase is the localised supplier mention when the turn has one
    (citing further would swallow the filters that must stay visible); a
    paraphrase without a literal noun cites the leftmost stretch no host
    authority claimed.
    """
    mention = _mention_interval(raw)
    if mention is not None:
        return mention
    blocked = sorted((span.start, span.end) for span in host_spans if span.dimension in HOST_OWNED)
    cursor = 0
    for start, end in [*blocked, (len(raw), len(raw))]:
        if start - cursor > 0 and raw[cursor:start].strip():
            return (cursor, start)
        cursor = max(cursor, end)
    return (0, len(raw))


def _assemble(raw, settings, resolver, interval, spec, actor_resolver=None):
    credential = _credential(raw, settings, interval)
    if credential is None:
        return None
    return assemble(raw, credential, spec, entity_resolver=resolver, today=TODAY,
                    actor_resolver=actor_resolver)


def _mention_interval(raw: str) -> tuple[int, int] | None:
    evidence = assess_supplier_request(raw)
    if not evidence.mentions:
        return None
    span = evidence.mentions[0].span.original
    return (span[0], span[1])


def evaluate(cases, settings, resolver, actor_resolver) -> dict:
    spec = capability_spec(SUPPLIERS)
    rows, violations = [], {gate: [] for gate in GATES}
    ready_clear, realistic = [], {"mention": 0, "whole": 0, "credentialed_turns": 0}
    for case in cases:
        case_id, raw = case["case_id"], case["raw_text"]
        forbid = set(case.get("forbid") or ())
        # Oracle: what a correct citation would be — the host-free portion.
        bootstrap = _assemble(raw, settings, resolver, (0, len(raw)), spec, actor_resolver)
        host_spans = bootstrap.spans if bootstrap is not None else ()
        oracle_interval = _oracle_interval(raw, host_spans)
        assembly = _assemble(raw, settings, resolver, oracle_interval, spec, actor_resolver)
        status = assembly.status.value if assembly is not None else "NO_CREDENTIAL"
        ready = assembly is not None and assembly.status is AssemblyStatus.READY
        row = {"case_id": case_id, "owner": case["owner"], "outcome": case["outcome"],
               "expected_buyer": case.get("buyer"), "oracle_status": status,
               "oracle_reason": assembly.reason if assembly else "",
               "oracle_buyer": (assembly.params.get("entidad_resolved") if assembly else None),
               "oracle_residual": list(assembly.residual_material) if assembly else []}
        if case["owner"] == "SUPPLIERS" and ready:
            expected = case.get("buyer")
            if expected and expected.split()[-1] not in (row["oracle_buyer"] or "").lower():
                violations["WRONG_BUYER"].append(case_id)
            if case["outcome"] == "HONEST_CLARIFY":
                violations["SILENT_FILTER_LOSS"].append(case_id)
        if case["owner"] == "NOT_SUPPLIERS" and ready:
            if "ACTOR_STOLEN" in forbid:
                violations["ACTOR_STOLEN"].append(case_id)
            if "NEGATED_JOB_EXECUTED" in forbid:
                violations["NEGATED_JOB_EXECUTED"].append(case_id)
            if "SILENT_CARDINALITY_SWAP" in forbid:
                violations["UNSUPPORTED_CARDINALITY_EXECUTED"].append(case_id)
        if case["owner"] == "SUPPLIERS" and case["outcome"] == "EXECUTE":
            if ready:
                ready_clear.append(case_id)
            for label, interval in (("mention", _mention_interval(raw)),
                                    ("whole", (0, len(raw)))):
                if label == "mention" and interval is None:
                    continue
                variant = _assemble(raw, settings, resolver, interval, spec, actor_resolver)
                realistic["credentialed_turns"] += 1
                if variant is not None and variant.status is AssemblyStatus.READY:
                    realistic[label] += 1
        rows.append(row)
    clear = [case for case in cases if case["owner"] == "SUPPLIERS" and case["outcome"] == "EXECUTE"]
    # Coverage gaps the mission ordered documented, never repaired here (P2).
    p2 = {"C02": "ranking surface 'principales' stays material",
          "P04": "bare municipality does not accredit as buyer"}
    ready = set(ready_clear)
    beyond_p2 = [case["case_id"] for case in clear if case["case_id"] not in p2]
    return {
        "oracle_credential": "evidence = correct citation (mention, else the leftmost "
                             "stretch no host authority claimed)",
        "cases": len(cases), "suppliers_execute_cases": len(clear),
        "gates": {gate: sorted(set(ids)) for gate, ids in violations.items()},
        "gates_pass": all(not ids for ids in violations.values()),
        "clear_suppliers_ready": sorted(ready),
        "clear_suppliers_ready_rate": round(len(ready) / len(clear), 4) if clear else None,
        "documented_coverage_gaps_p2": {case_id: p2[case_id] for case_id in sorted(p2)},
        "clear_suppliers_ready_rate_outside_p2": (
            round(len([case for case in beyond_p2 if case in ready]) / len(beyond_p2), 4)
            if beyond_p2 else None),
        "rows": rows,
    }, realistic


def classifier_section() -> dict:
    recorded = ROOT / "experiments" / "natural_job_prelegacy_v2" / "alpha_payloads_v3.json"
    if not recorded.exists():
        return {"status": "NOT_RUN",
                "reason": "no recorded V3 payloads in this checkpoint; classifier mistakes are "
                          "reported here only and never folded into the control-plane gates"}
    return {"status": "PENDING_HARNESS", "path": str(recorded.relative_to(ROOT))}


def main() -> None:
    cases = json.loads(CORPUS.read_text(encoding="utf-8"))
    settings, resolver, actor_resolver = _world()
    control, realistic = evaluate(cases, settings, resolver, actor_resolver)
    document = {
        "checkpoint": "C4A",
        "control_plane_oracle": control,
        "evidence_quality_model": {
            "definition": "how many SUPPLIERS/EXECUTE turns reach READY under each citation style",
            "mention_citations_ready": realistic["mention"],
            "whole_turn_citations_ready": realistic["whole"],
            "credentialed_variants": realistic["credentialed_turns"],
            "note": "an overreaching citation clarifying is the designed behavior (plan V2 6), "
                    "not a control-plane defect",
        },
        "end_to_end_alpha": classifier_section(),
    }
    OUT.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps({
        "gates": control["gates"], "gates_pass": control["gates_pass"],
        "clear_suppliers_ready": f"{len(control['clear_suppliers_ready'])}/"
                                  f"{control['suppliers_execute_cases']}",
        "evidence_quality": document["evidence_quality_model"],
        "end_to_end_alpha": document["end_to_end_alpha"]["status"],
    }, indent=1) + "\n")


if __name__ == "__main__":
    main()
