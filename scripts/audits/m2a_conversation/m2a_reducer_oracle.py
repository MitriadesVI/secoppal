"""M2A-8a0: the ORACLE of the M2a reducer, independent of M2A-3.

Principle (a rule of the project): GOLD decides MEANING, the CONTRACT decides mechanical invariants, and the oracle
verifies both with two independent pure functions over dicts. Neither imports the reducer or any registered M2a module.

* the GOLD oracle: ``audit(actuals)``, ``audit_classification(scopes)`` over ``load_cases()``: is the adjudicated meaning
  right (disposition, code, effective as a semantic projection, effects)? Everything expected comes from the sealed
  ``M2A_GOLD``; the replay can only coincide with it, never define it;
* the TRANSITION oracle: ``check_transition(base, patch, effective)`` (and ``audit_transitions`` over the Gold's cases): did
  the reducer arrive without falsifying identity or provenance? Its rules come from the contract
  (``TRANSITION_RULES``); M2A-3 also calls it over GENERATED bases.

Parts: ``m2a_oracle_gold`` (reading the sealed Gold, verifying the seal and the sealed §19 extraction),
``m2a_oracle_typing`` (``value_hint`` -> closed values, the dicts), ``m2a_oracle_applicability`` (the blocking
dependencies: §19 rows with impacto COMPOSICION plus two structural rules, each with its clause), ``m2a_oracle_expect`` (the cases), ``m2a_oracle_audit`` (the Gold audit) and ``m2a_oracle_transition`` (the
contract's invariants). This module is the facade, the artifact, the manifest and the CLI.

Applicability: ``APLICABLE_M2A3`` (typed input and a Gold-determined expectation: M2A-3 must reach 100 %),
``DEPENDIENTE(<owner>)`` (contract §19 says compose needs an input another lane produces, ``impacto_m2a3 = COMPOSICION``,
or a structural rule says so: red declared), ``NO_MEDIBLE_SIN_PRODUCTOR`` and ``NO_MEDIBLE(<motive>)`` (the Gold does not
allow typing or determining something without guessing). A §19 dependency with ``impacto_m2a3 = LECTURA`` does not block.

Use (M2A-3 iterates on DEV and measures TEST once, to freeze)::

    cases = select_partition(load_cases(), "DEV")             # synthetic; load_cases(real_dir) adds the private REAL slice
    inputs = {c["gold_id"]: c["inputs"] for c in cases if c["applicability"] == "APLICABLE_M2A3"}
    actuals = {gid: run(inputs[gid]) for gid in inputs}        # {disposition, code, effective, effects}
    audit(actuals, cases)["ok"] and audit_transitions(actuals, cases)["ok"]
    # in the Hypothesis properties, over GENERATED bases with every host field filled (never only the Gold's):
    check_transition(base, patch, effective)                  # the reducer executed: identity, provenance, what is refused
    check_transition(base, patch, None, refusal=code)         # the reducer refused: a cause must exist, with the top code
    expected_refusal(base, patch)                             # what the contract demands: the code, or None (an effective)
    refusal_causes(base, patch)                               # every cause in the closed precedence of §6 step 3b

CLI: ``build`` / ``check`` / ``report`` [--real-dir D]; the REAL section of the manifest is ids, counts and digests.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audits.m2a_conversation import m2a_oracle_gold as gold  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_applicability import rule_table  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_audit import (  # noqa: E402,F401
    ACTUAL_KEYS, LIMITED_GUARANTEES, PROJECTION_EXCLUSIONS, audit, audit_classification, audit_transitions,
    effective_equal, project_effective, realize_effective, reference_actuals,
)
from scripts.audits.m2a_conversation.m2a_oracle_expect import (  # noqa: E402,F401
    APPLICABLE, ECHO_EFFECTS, NO_PRODUCER, REDUCER_EFFECTS, NoProducer, build_case, dumps, load_cases, outcomes, p_facts,
    select_partition,
)
from scripts.audits.m2a_conversation.m2a_oracle_gold import OracleError, digest  # noqa: E402,F401
from scripts.audits.m2a_conversation.m2a_oracle_transition import (  # noqa: E402,F401
    TRANSITION_RULES, check_transition, expected_refusal, refusal_causes,
)

ORACLE_DIR = "docs/audits/m2a_conversation/oracle"
ARTIFACT = f"{ORACLE_DIR}/M2A_REDUCER_ORACLE_synthetic.jsonl"
MANIFEST = f"{ORACLE_DIR}/M2A_REDUCER_ORACLE_MANIFEST.json"
SCHEMA = "m2a-reducer-oracle/3"


# ---- artifact, manifest, CLI -----------------------------------------------------------------------------------------------
def _summary(cases: list[dict]) -> dict:
    applicable = [c for c in cases if c["applicability"] == APPLICABLE]
    count = collections.Counter
    return {"cases": len(cases), "by_applicability": dict(sorted(count(c["applicability"] for c in cases).items())),
            "dependent_by_any_owner": dict(sorted(count(o for c in cases for o in c["owners"]).items())),
            "applicable_by_partition": dict(sorted(count(c["partition"] for c in applicable).items())),
            "applicable_by_disposition": dict(sorted(count(c["expected"]["disposition"] for c in applicable).items())),
            "all_by_partition": dict(sorted(count(c["partition"] for c in cases).items())),
            "classification_measurable": sum(1 for c in applicable if c["expected"]["classification"]["measurable"]),
            "effects_not_measurable": sum(1 for c in applicable if c["expected"]["effects_status"] != "MEDIBLE")}


def _real_section(cases: list[dict]) -> dict:
    ids = collections.defaultdict(list)
    for c in cases:
        ids[c["applicability"]].append(c["gold_id"])
    return {"summary": _summary(cases), "ids_by_applicability": {k: sorted(v) for k, v in sorted(ids.items())},
            "case_sha256": {c["gold_id"]: digest(c) for c in cases}}


def declarations() -> dict:
    """What the oracle declares about itself (the manifest carries it): the rules that exclude cases with their clause,
    the transition rules with their clause and killing mutants, what the Gold projection leaves to the transition
    oracle, and the named limited guarantees."""
    killers = {r["id"]: r["killed_by"] for r in TRANSITION_RULES}
    delegated = []
    for entry in PROJECTION_EXCLUSIONS:
        rules = sorted({rid for rid in killers if rid in entry["to"]}, key=entry["to"].index)
        delegated.append({**entry, "rules": rules, "killed_by": sorted({m for rid in rules for m in killers[rid]})})
    return {"applicability_rules": rule_table(gold.s19()), "transition_rules": list(TRANSITION_RULES),
            "gold_projection_delegates_to_transition": delegated,
            "limited_guarantees": list(LIMITED_GUARANTEES),
            "effects_scope": {"reducer": sorted(REDUCER_EFFECTS), "echo": sorted(ECHO_EFFECTS)}}


def s19_section() -> dict:
    """The one authority of dependencies, as the manifest binds it (the seal binds the same file and the contract)."""
    s19 = gold.s19()
    return {"path": str(gold.S19_PATH.relative_to(gold.ROOT)), "sha256": hashlib.sha256(gold.S19_PATH.read_bytes()).hexdigest(),
            "schema": s19["schema"], "contract_sha256": s19["source"]["contract_sha256"], "impacts": s19["impacts"],
            "owners": s19["owners"],
            "dependencies": {d["dep_id"]: {"impacto_m2a3": d["impacto_m2a3"], "owners": d["owners"],
                                           "cases": d["cases_count"]} for d in s19["dependencies"]}}


def build_artifacts(real_dir: Path | None, previous: dict | None) -> tuple[str, dict]:
    synthetic = load_cases()
    text = "".join(dumps(c) + "\n" for c in synthetic)
    if real_dir is not None:
        real = _real_section([c for c in load_cases(real_dir) if c["source"] == "REAL"])
    elif previous is not None:
        real = previous["real"]
    else:
        raise SystemExit("build needs --real-dir the first time (the REAL section is private)")
    manifest = {"schema": SCHEMA, "gold_seal_sha256": gold.seal()["seal_sha256"], "seal_verified": True,
                "dependencies_authority": s19_section(),
                "artifact": {"path": ARTIFACT, "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                             "records": len(synthetic)},
                "declarations": declarations(), "synthetic": _summary(synthetic), "real": real,
                "policy": "texto REAL fuera de git; aquí sólo ids, conteos y digests"}
    return text, manifest


def real_manifest_problems(manifest: dict) -> list[str]:
    """What can be checked about the REAL section without the private directory: ids and coverage."""
    ids = sorted(i for group in manifest["real"]["ids_by_applicability"].values() for i in group)
    problems = []
    if ids != sorted(gold.seal()["slices"]["M2A_GOLD"]["REAL"]["ids"]):
        problems.append("the REAL ids of the manifest differ from the seal")
    if sorted(manifest["real"]["case_sha256"]) != ids:
        problems.append("the REAL digests do not cover the REAL ids")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("cmd", choices=("build", "check", "report"))
    ap.add_argument("--real-dir", type=Path)
    a = ap.parse_args(argv)
    artifact, manifest_path = ROOT / ARTIFACT, ROOT / MANIFEST
    previous = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else None
    text, manifest = build_artifacts(a.real_dir, previous)
    if a.cmd == "report":
        print(json.dumps({"synthetic": manifest["synthetic"], "real": manifest["real"]["summary"]},
                         ensure_ascii=False, indent=1, sort_keys=True))
        return 0
    if a.cmd == "build":
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(text, encoding="utf-8")
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1, sort_keys=True) + "\n",
                                 encoding="utf-8")
        print("oracle built", manifest["artifact"]["sha256"])
        return 0
    problems = []
    if not artifact.exists() or artifact.read_text(encoding="utf-8") != text:
        problems.append("the synthetic artifact differs from the recomputation")
    if previous != manifest:
        problems.append("the manifest differs from the recomputation")
    problems += real_manifest_problems(previous or manifest)
    for p in problems:
        print("ORACLE DRIFT:", p)
    if not problems:
        print("oracle OK", manifest["artifact"]["sha256"], "REAL",
              "recomputed" if a.real_dir else "carried (private dir absent)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
