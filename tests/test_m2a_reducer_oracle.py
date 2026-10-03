"""M2A-8a0 oracle A: the GOLD oracle is independent of M2A-3, faithful to the sealed Gold, and its audit catches errors.

GOLD decides meaning; the CONTRACT decides mechanical invariants; the oracle verifies both with two independent pure
functions over dicts. This file proves the first (the second is ``tests/test_m2a_transition_oracle.py``):

* INDEPENDENCE: no oracle module (nor these tests) imports the reducer or any registered M2a module; the pin has a mutant
  for each way of breaking it;
* FIDELITY: every expectation is a pure function of the sealed Gold (no label notes, no text, no privileged labeler); the
  seal, the section 19 extraction (the ONE authority of dependencies, U19) and the vocabulary are verified at run time,
  never fixed in the oracle; applicability comes from §19 (``COMPOSICION`` blocks, ``LECTURA`` does not) plus two structural
  rules that cite their clause, and moves a case when the extraction or the Gold field changes;
  an A/B disagreement is never resolved at will; the typing preserves the equalities the reducer needs;
* AUDIT: the Gold-faithful reference passes; a campaign of wrong outputs built from the expectation of every applicable
  case is caught in full; harmless variations are not; the comparison is not vacuous (neutralising one aspect of it lets
  exactly its mutants through); the coverage is declared.

Nothing about the Gold is fixed here (no counts, no id lists, no code lists, no artifact hash): the seal is an input
verified at run time, so a re-seal regenerates the oracle without touching any code. The REAL slice never enters git; with
``M2A_GOLD_REAL_DIR`` set to the private GOLD_CANDIDATE directory its checks run too.
"""

from __future__ import annotations

import ast
import collections
import copy
import functools
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from app.core.foundation_authority_values import decode_value  # closed value types: not M2a
from scripts.audits.m2a_conversation import m2a_oracle_applicability as applicability
from scripts.audits.m2a_conversation import m2a_oracle_audit as audit_mod
from scripts.audits.m2a_conversation import m2a_oracle_expect as expect_mod
from scripts.audits.m2a_conversation import m2a_oracle_gold as gold
from scripts.audits.m2a_conversation import m2a_oracle_transition as transition_module
from scripts.audits.m2a_conversation import m2a_oracle_typing as typing_
from scripts.audits.m2a_conversation import m2a_reducer_oracle as oracle
from tests.m2a_steps import registered_modules

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts/audits/m2a_conversation"
ORACLE_FILES = [SCRIPTS / f"{name}.py" for name in (
    "m2a_reducer_oracle", "m2a_oracle_gold", "m2a_oracle_typing", "m2a_oracle_applicability", "m2a_oracle_expect",
    "m2a_oracle_audit", "m2a_oracle_transition")]
THIS_FILE = Path(__file__).resolve()
TEST_FILES = [THIS_FILE, THIS_FILE.with_name("test_m2a_transition_oracle.py"),
              THIS_FILE.with_name("test_m2a_oracle_vs_contract_model.py")]
FAST = settings(max_examples=120, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
REAL_DIR = Path(os.environ["M2A_GOLD_REAL_DIR"]) if os.environ.get("M2A_GOLD_REAL_DIR") else None
needs_real = pytest.mark.skipif(REAL_DIR is None, reason="M2A_GOLD_REAL_DIR (private GOLD_CANDIDATE) not set")

SYN = {r["gold_id"]: r for _, r in gold.gold_records(None)}
CASES = oracle.load_cases()
BY_ID = {c["gold_id"]: c for c in CASES}
APPLICABLE = [c for c in CASES if c["applicability"] == oracle.APPLICABLE]
REFERENCE = oracle.reference_actuals(CASES)
S19 = gold.s19()


def rebuilt(gid: str, change) -> dict:
    """The case the oracle derives from the sealed record of ``gid`` after ``change`` edited a deep copy of it."""
    rec = copy.deepcopy(SYN[gid])
    change(rec)
    return oracle.build_case("SYNTHETIC", rec, BY_ID[gid]["partition"], S19)


def both(change):
    """Apply ``change(label)`` to both labels (a Gold fact has to hold for A and B)."""
    return lambda rec: [change(rec[name]) for name in ("label_A", "label_B")]


# ====================================================================================================================
# INDEPENDENCE
# ====================================================================================================================
FORBIDDEN_MODULE_NAMES = ("foundation_accepted", "foundation_delta", "foundation_turn_scope", "foundation_admission",
                          "foundation_runtime")
ALLOWED_APP_MODULES = frozenset({"app.core.foundation_authority_values", "app.core.foundation_geo_level"})
FORBIDDEN_IDENTIFIERS = frozenset({"compose", "classify_turn", "DeltaRefusal", "__import__", "import_module"})


def import_offenders(source: str, registered=()) -> list[str]:
    """Why a source is not independent of M2a: an import of any ``app`` module outside the two closed value types, of a
    registered M2a module or of the reducer, a dynamic import, or a use of the reducer's entry points."""
    out: list[str] = []

    def check(module: str) -> None:
        parts = module.split(".")
        if any(p in FORBIDDEN_MODULE_NAMES for p in parts) or module in registered:
            out.append(f"imports {module}")

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                check(alias.name)
                if alias.name.split(".")[0] == "app" and alias.name not in ALLOWED_APP_MODULES:
                    out.append(f"imports {alias.name}")
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            module = node.module or ""
            members = [f"{module}.{alias.name}" for alias in node.names]
            for name in [module, *members]:
                check(name)
            if module.split(".")[0] == "app" and module not in ALLOWED_APP_MODULES and not all(
                    m in ALLOWED_APP_MODULES for m in members):
                out.append(f"imports from {module}")
        names = []
        if isinstance(node, ast.Name):
            names = [node.id]
        elif isinstance(node, ast.Attribute):
            names = [node.attr]
        elif isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names = [node.name]
        elif isinstance(node, ast.alias):
            names = [node.name.split(".")[-1]]
        out += [f"uses {n}" for n in names if n in FORBIDDEN_IDENTIFIERS]
    return sorted(set(out))


def _registered() -> set[str]:
    return set(registered_modules())


@pytest.mark.parametrize("path", ORACLE_FILES + TEST_FILES, ids=lambda p: p.name)
def test_no_oracle_module_imports_m2a_or_the_reducer(path):
    assert import_offenders(path.read_text(encoding="utf-8"), _registered()) == []


@pytest.mark.parametrize("mutant", [
    "from app.core.foundation_accepted import AcceptedInterpretation\n",
    "import app.core.foundation_delta as delta\n",
    "from app.core import foundation_turn_scope\n",
    "from app.core.foundation_admission import accept\n",
    "from app.core.host_authorities.state import resolve\n",
    "from app.core.foundation_authority_registry import REGISTRY\n",
    "from app.core import host_authorities\n",
    "import app.core.query_router\n",
    "x = __import__('app.core.foundation_delta')\n",
    "from importlib import import_module\n",
    "result = compose(base, patch)\n",
    "scope = classify_turn(facts)\n",
    "refusal = DeltaRefusal\n",
    "import app.core.some_registered_module\n",
])
def test_the_independence_pin_catches_its_mutants(mutant):
    assert import_offenders(mutant, {"app.core.some_registered_module"}), mutant


def test_the_independence_pin_does_not_flag_what_the_oracle_may_use():
    ok = ("from app.core.foundation_authority_values import MoneyValue\nfrom app.core.foundation_geo_level import GeoLevel\n"
          "from app.core import foundation_geo_level\nimport importlib.util\n"
          "spec = importlib.util.spec_from_file_location('x', 'y')\n")
    assert import_offenders(ok, _registered()) == []


LOADED_APP_SCRIPT = """
import json, sys
sys.path.insert(0, {root!r})
{extra}
from scripts.audits.m2a_conversation import m2a_reducer_oracle as o
cases = o.load_cases(); reference = o.reference_actuals(cases)
o.audit(reference, cases); o.audit_transitions(reference, cases); o.check_transition(
    cases[0]["inputs"]["base"], cases[0]["inputs"]["patch"], None) if cases[0]["inputs"] else None
print(json.dumps(sorted(m for m in sys.modules if m == "app" or m.startswith("app."))))
"""


def loaded_app_modules(extra: str = "") -> set[str]:
    """The ``app`` modules a FRESH interpreter holds after the whole oracle ran (load, audit, transitions): the transitive truth
    the AST pin cannot see."""
    result = subprocess.run([sys.executable, "-c", LOADED_APP_SCRIPT.format(root=str(ROOT), extra=extra)], capture_output=True,
                            text=True, cwd=ROOT, timeout=300)
    assert result.returncode == 0, result.stderr[-500:]
    return set(json.loads(result.stdout.strip().splitlines()[-1]))


def m2a_modules_among(loaded: set[str]) -> list[str]:
    return sorted(m for m in loaded if m in _registered() or any(part in FORBIDDEN_MODULE_NAMES for part in m.split(".")))


def test_no_module_of_the_oracle_loads_m2a_not_even_transitively():
    """The independence is of the CODE of the oracle (scripts/): a test may load M2a only to generate domain, never for an
    expectation. After a full run in a fresh interpreter no registered M2a module (nor foundation_accepted, the reducer's
    modules...) is in ``sys.modules``: what is loaded is the closed value types and what the sealed checker reads."""
    loaded = loaded_app_modules()
    assert {"app.core.foundation_authority_values", "app.core.foundation_geo_level"} <= loaded
    assert m2a_modules_among(loaded) == []


def test_the_transitive_pin_sees_a_load_it_would_have_to_refuse():
    loaded = loaded_app_modules("import app.core.foundation_accepted")
    assert "app.core.foundation_accepted" in m2a_modules_among(loaded)


def test_the_oracle_only_reads_app_through_the_two_closed_value_modules():
    imported = set()
    for path in ORACLE_FILES:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("app"):
                imported.add(node.module)
    assert imported == ALLOWED_APP_MODULES


def test_the_only_dynamic_imports_load_the_sealed_checker_and_the_seal_script():
    source = (SCRIPTS / "m2a_oracle_gold.py").read_text(encoding="utf-8")
    assert source.count("spec_from_file_location") == 2
    assert "check_m2a_gold_contract.py" in source and "m2a_gold_seal.py" in source
    assert gold.checker().GOLD_DIR == gold.GOLD_DIR


# ====================================================================================================================
# THE GOLD IS READ AS SEALED
# ====================================================================================================================
def test_every_gold_case_has_exactly_one_case_and_the_frontier_never_enters():
    seal = gold.seal()
    assert [c["gold_id"] for c in CASES] == [r["gold_id"] for r in SYN.values()]
    assert sorted(BY_ID) == sorted(seal["slices"]["M2A_GOLD"]["SYNTHETIC"]["ids"])
    frontier = {r["gold_id"] for s in ("REAL", "SYNTHETIC") for r in seal["slices"]["FOLLOWUP_FRONTIER"][s]["records"]}
    assert not set(BY_ID) & frontier and all(c["source"] == "SYNTHETIC" for c in CASES)


def test_a_frontier_id_inside_the_gold_is_a_defect_not_a_case(monkeypatch):
    sealed = gold.seal()
    sealed["slices"]["FOLLOWUP_FRONTIER"]["SYNTHETIC"]["records"].append({"gold_id": "SF001"})
    monkeypatch.setattr(gold, "seal", lambda: sealed)
    with pytest.raises(gold.OracleError, match="FOLLOWUP_FRONTIER"):
        gold.gold_records(None)


def test_gold_ids_that_differ_from_the_seal_are_a_defect(monkeypatch):
    sealed = gold.seal()
    sealed["slices"]["M2A_GOLD"]["SYNTHETIC"]["ids"] = sealed["slices"]["M2A_GOLD"]["SYNTHETIC"]["ids"][:-1]
    monkeypatch.setattr(gold, "seal", lambda: sealed)
    with pytest.raises(gold.OracleError, match="differ from the seal"):
        gold.gold_records(None)


def test_the_partition_is_the_seals_and_dev_test_selection_is_exact():
    side = gold.partition_of(gold.seal())
    assert all(c["partition"] == side[c["gold_id"]] for c in CASES)
    dev, test = oracle.select_partition(CASES, "DEV"), oracle.select_partition(CASES, "TEST")
    assert len(dev) + len(test) == len(CASES) and not {c["gold_id"] for c in dev} & {c["gold_id"] for c in test}
    with pytest.raises(gold.OracleError):
        oracle.select_partition(CASES, "TRAIN")


def test_the_gold_is_still_representable_by_the_contract():
    assert gold.checker().run(None)["violations"] == []


# ====================================================================================================================
# THE SEAL, THE §19 EXTRACTION AND THE VOCABULARY ARE INPUTS VERIFIED AT RUN TIME
# ====================================================================================================================
def test_the_seal_is_verified_before_any_case_is_built(monkeypatch):
    calls = []
    monkeypatch.setattr(gold, "verify_seal", lambda real_dir=None: calls.append(real_dir))
    oracle.load_cases()
    assert calls and set(calls) == {None}  # (the §19 reader verifies it too: cheap, cached)


def test_a_broken_seal_stops_the_oracle(monkeypatch):
    class Broken:
        @staticmethod
        def main():
            print("SEAL BROKEN: the contract changed")
            return 1
    monkeypatch.setattr(gold, "seal_module", lambda: Broken)
    gold.invalidate_seal_cache()
    with pytest.raises(gold.OracleError, match="SEAL BROKEN: the contract changed"):
        oracle.load_cases()
    monkeypatch.undo()
    gold.invalidate_seal_cache()
    gold.verify_seal(None)  # and the real seal verifies


def test_a_file_the_seal_binds_that_is_missing_is_a_broken_seal_not_a_crash(monkeypatch):
    class Missing:
        @staticmethod
        def main():
            raise FileNotFoundError("docs/audits/m2a_conversation/gold/CONTRACT_S19_DEPENDENCIES.json")
    monkeypatch.setattr(gold, "seal_module", lambda: Missing)
    gold.invalidate_seal_cache()
    with pytest.raises(gold.OracleError, match="does not verify.*CONTRACT_S19_DEPENDENCIES"):
        oracle.load_cases()
    monkeypatch.undo()
    gold.invalidate_seal_cache()
    gold.verify_seal(None)


def test_the_oracle_runs_the_seals_own_verify_command_and_the_cache_dies_with_the_bound_files(monkeypatch):
    seen = []

    class Spy:
        @staticmethod
        def main():
            seen.append(list(sys.argv))
            return 0
    monkeypatch.setattr(gold, "seal_module", lambda: Spy)
    gold.invalidate_seal_cache()
    gold.verify_seal(None)
    gold.verify_seal(None)  # cached: the bound files did not change
    assert seen == [["m2a_gold_seal.py", "verify"]]
    monkeypatch.setattr(gold, "_fingerprint", lambda real_dir: ("a bound file changed",))
    gold.verify_seal(None)
    gold.verify_seal(Path("/private/dir"))
    assert seen[1:] == [["m2a_gold_seal.py", "verify"], ["m2a_gold_seal.py", "verify", "--real-dir", "/private/dir"]]
    monkeypatch.undo()
    gold.invalidate_seal_cache()


def test_the_fingerprint_changes_when_a_bound_file_changes(tmp_path, monkeypatch):
    before = gold._fingerprint(None)
    bound = ROOT / next(iter(gold.seal()["public_files"]))
    original = bound.stat()
    os.utime(bound, ns=(original.st_atime_ns, original.st_mtime_ns + 10**9))
    try:
        assert gold._fingerprint(None) != before
    finally:
        os.utime(bound, ns=(original.st_atime_ns, original.st_mtime_ns))
    assert gold._fingerprint(None) == before


def test_reason_codes_are_read_from_the_sealed_checker_not_from_the_oracle(monkeypatch):
    chk = gold.checker()
    for reason in sorted(chk.REASONS) + ["UNRESOLVED_VALUE:TIME", "OPERATION_CHANGE:COUNT", None]:
        gold.check_reason(reason)
    for bad in ("A_CODE_NOBODY_SEALED", "SMMLV_UNSUPPORTED", "unresolved_value:TIME"):
        with pytest.raises(gold.OracleError):
            gold.check_reason(bad)
    monkeypatch.setattr(chk, "REASONS", set(chk.REASONS) | {"A_NEW_CODE_OF_THE_NEXT_SEAL"})
    gold.check_reason("A_NEW_CODE_OF_THE_NEXT_SEAL")


def test_a_valid_reason_is_data_not_a_dependency():
    chk = gold.checker()
    facts = {gold.host_rules()[rule][1] for rule in expect_mod.HOST_RULE_FACTS}
    for reason in sorted(chk.PAIRS["CLARIFY"] & chk.REASONS):
        case = rebuilt("SF138", both(lambda L, r=reason: L.update(disposition_reason=r)))  # a CLARIFY case
        if reason in facts:
            assert case["applicability"].startswith("NO_MEDIBLE(HOST_FACT_NOT_IN_GOLD:"), reason
        else:
            assert case["applicability"] == oracle.APPLICABLE and case["expected"]["code"] == reason, reason


def test_a_reason_outside_the_sealed_vocabulary_is_a_defect_not_a_case():
    with pytest.raises(gold.OracleError):
        rebuilt("SF001", both(lambda L: L.update(disposition_reason="A_CODE_NOBODY_SEALED")))


def test_a_re_seal_with_a_new_case_and_a_new_code_is_regenerated_without_touching_code(monkeypatch):
    """The next seal adds a synthetic case that CLEARs ORDER and leaves the CARDINALITY, and a code for it."""
    new = copy.deepcopy(SYN["SF024"])  # prior with ORDER and CARDINALITY
    new["gold_id"] = "SF900"
    for name in ("label_A", "label_B"):
        label = new[name]
        label["gold_id"] = "SF900"
        label["ops"] = [o for o in label["ops"] if o["slot"] == "ORDER"]
        for op in label["ops"]:
            op["op"] = "CLEAR"
        label.update(expected_disposition="CLARIFY", disposition_reason="A_FAIL_CLOSED_CODE_OF_THE_NEXT_SEAL",
                     resulting_ir=None, effects=[], expected_echo=[])
    new.update(expected_disposition="CLARIFY")
    sealed = gold.seal()
    sealed["slices"]["M2A_GOLD"]["SYNTHETIC"]["ids"] = [*sealed["slices"]["M2A_GOLD"]["SYNTHETIC"]["ids"], "SF900"]
    sealed["partition"]["DEV"]["SYNTHETIC"] = [*sealed["partition"]["DEV"]["SYNTHETIC"], "SF900"]
    monkeypatch.setattr(gold, "seal", lambda: sealed)
    monkeypatch.setattr(gold, "verify_seal", lambda real_dir=None: None)
    monkeypatch.setattr(gold, "gold_records", lambda real_dir, gold_dir=gold.GOLD_DIR:
                        [("SYNTHETIC", r) for r in (*SYN.values(), new)])
    monkeypatch.setattr(gold.checker(), "REASONS", set(gold.checker().REASONS) | {"A_FAIL_CLOSED_CODE_OF_THE_NEXT_SEAL"})
    monkeypatch.setattr(gold.checker(), "PAIRS", {**gold.checker().PAIRS, "CLARIFY": gold.checker().PAIRS["CLARIFY"] | {
        "A_FAIL_CLOSED_CODE_OF_THE_NEXT_SEAL"}})  # annex A.3 of the next seal pairs it with CLARIFY
    with pytest.raises(gold.OracleError, match="vocabulary of refusal codes"):
        oracle.load_cases()  # a new A.3 code is a contract amendment: the transition oracle's vocabulary stops the build...
    monkeypatch.setattr(transition_module, "A3_PLAIN_CODES",
                        transition_module.A3_PLAIN_CODES | {"A_FAIL_CLOSED_CODE_OF_THE_NEXT_SEAL"})  # ...until it is extended
    cases = {c["gold_id"]: c for c in oracle.load_cases()}
    case = cases["SF900"]
    assert case["applicability"] == oracle.APPLICABLE and case["partition"] == "DEV"
    assert case["expected"]["disposition"] == "CLARIFY" and case["expected"]["code"] == "A_FAIL_CLOSED_CODE_OF_THE_NEXT_SEAL"
    assert case["expected"]["effective"] is None and [op["op"] for op in case["inputs"]["patch"]["ops"]] == ["CLEAR"]
    assert case["inputs"]["patch"]["ops"][0]["slot"] == "ORDER" and len(cases) == len(CASES) + 1
    everything = list(cases.values())
    with_new = {**REFERENCE, "SF900": oracle.reference_actuals([case])["SF900"]}
    assert oracle.audit(with_new, everything)["ok"]  # the new case is measured and everything else is untouched
    assert oracle.audit(REFERENCE, everything)["missing"] == ["SF900"]
    assert not oracle.audit({**with_new, "SF900": {**with_new["SF900"], "code": None}}, everything)["ok"]


# ---- ONE authority for dependencies (U19): the sealed extraction of contract section 19 ------------------------------------
def reroll(data: dict) -> dict:
    """The extraction with its per-case fields recomputed from its rows (what the generator would have written)."""
    rows = {d["dep_id"]: d for d in data["dependencies"]}
    cases = {}
    for dep in data["dependencies"]:
        for case in dep["cases"]:
            cases.setdefault(case, []).append(dep["dep_id"])
    data["case_dependencies"] = dict(sorted(cases.items()))
    data["case_owners"] = {c: sorted({o for d in ds for o in rows[d]["owners"]}) for c, ds in sorted(cases.items())}
    data["case_impacto_m2a3"] = {c: ("COMPOSICION" if any(rows[d]["impacto_m2a3"] == "COMPOSICION" for d in ds) else "LECTURA")
                                 for c, ds in sorted(cases.items())}
    return data


def s19_with(edit) -> dict:
    data = copy.deepcopy(S19)
    edit(data)
    return reroll(data)


def serve(monkeypatch, tmp_path, edit, *, bind=True):
    """Make ``gold.s19()`` read an edited copy of the extraction; ``bind`` makes the (patched) seal bind that copy, so that
    each failure is isolated from the sha256 one."""
    data = json.loads(gold.S19_PATH.read_text(encoding="utf-8"))
    edit(data)
    file = tmp_path / "CONTRACT_S19_DEPENDENCIES.json"
    file.write_text(json.dumps(data, ensure_ascii=False, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    sealed = gold.seal()
    if bind:
        sealed["public_files"][gold.S19_KEY] = {**sealed["public_files"][gold.S19_KEY],
                                                "sha256": hashlib.sha256(file.read_bytes()).hexdigest()}
    monkeypatch.setattr(gold, "S19_PATH", file)
    monkeypatch.setattr(gold, "seal", lambda: sealed)


def test_the_s19_extraction_is_the_only_source_the_seal_binds_it_and_it_reproduces_from_the_contract():
    sealed = gold.seal()
    assert hashlib.sha256(gold.S19_PATH.read_bytes()).hexdigest() == sealed["public_files"][gold.S19_KEY]["sha256"]
    assert S19["schema"] == gold.S19_SCHEMA and tuple(S19["impacts"]) == gold.KNOWN_IMPACTS
    assert S19["source"]["contract_sha256"] == sealed["interpreting_rules"][S19["source"]["contract"]]
    assert sealed["gates"]["contract_dependencies_reproduce"] is True  # the seal regenerates it from the contract text
    assert not (ROOT / "docs/audits/m2a_conversation/oracle/APPLICABILITY_DECISIONS.json").exists()
    assert not (ROOT / "docs/audits/m2a_conversation/oracle/USER_DECISIONS.jsonl").exists()  # retired: no local override


@pytest.mark.parametrize("edit,bind,message", [
    (lambda d: d.update(schema="m2a-contract-s19/3"), True, "has schema"),
    (lambda d: d.update(impacts=["LECTURA", "COMPOSICION", "OTRO"]), True, "declares impacts"),
    (lambda d: d["dependencies"][0].update(impacto_m2a3="DESCONOCIDO"), True, "unknown impact"),
    (lambda d: d["dependencies"][0].update(owners=["NADIE"]), True, "owner outside the closed list"),
    (lambda d: d["dependencies"][0].update(owners=[]), True, "owner outside the closed list"),
    (lambda d: d["case_impacto_m2a3"].update(SF195="COMPOSICION"), True, "roll-up"),
    (lambda d: d["case_owners"].update(SF195=["E-GEO"]), True, "roll-up"),
    (lambda d: d["case_dependencies"].update(SF999=["DEP-E-ORDER"]), True, "does not have"),
    (lambda d: d["case_dependencies"].update(SF195=["DEP-NO-SUCH-ROW"]), True, "not rows of the extraction"),
    (lambda d: d["source"].update(contract_sha256="0" * 64), True, "contract the seal does not bind"),
    (lambda d: d.update(gaps=[]), False, "does not match the sha256 the seal binds"),
])
def test_an_extraction_that_is_not_what_the_seal_binds_or_that_the_oracle_cannot_read_fails_the_build(
        monkeypatch, tmp_path, edit, bind, message):
    serve(monkeypatch, tmp_path, edit, bind=bind)
    with pytest.raises(gold.OracleError, match=message):
        oracle.load_cases()
    with pytest.raises(gold.OracleError, match=message):
        oracle.build_artifacts(None, json.loads((ROOT / oracle.MANIFEST).read_text(encoding="utf-8")))


def test_a_missing_extraction_fails_the_build_nothing_else_may_decide_a_dependency(monkeypatch, tmp_path):
    monkeypatch.setattr(gold, "S19_PATH", tmp_path / "nowhere.json")
    with pytest.raises(gold.OracleError, match="missing"):
        oracle.load_cases()


def test_an_extraction_with_a_new_owner_or_impact_in_its_own_closed_lists_is_refused_until_the_oracle_knows_it(
        monkeypatch, tmp_path):
    serve(monkeypatch, tmp_path, lambda d: (d["owners"].append("NEW_LANE"), d["dependencies"][0].update(owners=["NEW_LANE"]),
                                            reroll(d)))
    assert gold.s19()["owners"][-1] == "NEW_LANE"  # a new OWNER is data: the extraction's closed list is the authority
    serve(monkeypatch, tmp_path, lambda d: (d["impacts"].append("NUEVO"), d["dependencies"][0].update(impacto_m2a3="NUEVO")))
    with pytest.raises(gold.OracleError, match="declares impacts"):
        gold.s19()  # a new IMPACT has no treatment in the oracle: it never guesses one


def test_a_composicion_dependency_blocks_with_the_owner_of_section_19_and_a_lectura_one_never_does():
    rows = {d["dep_id"]: d for d in S19["dependencies"]}
    structural = {r.id for r in applicability.STRUCTURAL_RULES}
    seen = {"blocking": 0, "reading": 0}
    for c in CASES:
        facts = c["s19"]
        assert (facts is not None) == (c["gold_id"] in S19["case_dependencies"])
        if facts is not None:
            assert facts == {"dependencies": S19["case_dependencies"][c["gold_id"]],
                             "impacto_m2a3": S19["case_impacto_m2a3"][c["gold_id"]],
                             "owners": S19["case_owners"][c["gold_id"]]}
        composicion = [d for d in (facts or {"dependencies": []})["dependencies"] if rows[d]["impacto_m2a3"] == "COMPOSICION"]
        fired = set(c["rules"]) & structural
        if composicion or fired:
            assert c["applicability"].startswith("DEPENDIENTE(") and set(c["rules"]) == set(composicion) | fired, c["gold_id"]
            assert set(c["owners"]) == {o for d in composicion for o in rows[d]["owners"]} | {
                o for r in applicability.STRUCTURAL_RULES if r.id in fired for o in r.owners}
            seen["blocking"] += 1
        else:
            assert not c["applicability"].startswith("DEPENDIENTE("), c["gold_id"]  # LECTURA is read, never a red
            seen["reading"] += bool(facts)
    assert all(seen.values())


def test_the_extraction_decides_the_case_when_it_changes_in_memory():
    flipped = oracle.build_case("SYNTHETIC", copy.deepcopy(SYN["SF009"]), "DEV", s19_with(
        lambda d: next(x for x in d["dependencies"] if x["dep_id"] == "DEP-STATE-LITERALS").update(impacto_m2a3="COMPOSICION")))
    assert BY_ID["SF009"]["applicability"] == oracle.APPLICABLE and "DEP-STATE-LITERALS" in BY_ID["SF009"]["s19"]["dependencies"]
    assert flipped["applicability"] == "DEPENDIENTE(E-STATE)" and flipped["rules"] == ["DEP-STATE-LITERALS"]
    assert flipped["inputs"] is None and flipped["expected"] is None
    freed = oracle.build_case("SYNTHETIC", copy.deepcopy(SYN["SF075"]), "DEV", s19_with(
        lambda d: next(x for x in d["dependencies"] if x["dep_id"] == "DEP-O1B-OPPORTUNITIES").update(impacto_m2a3="LECTURA")))
    assert BY_ID["SF075"]["applicability"] == "DEPENDIENTE(FOUNDATION_O1B_EXPLICIT_TIME_PARITY)"
    assert not freed["applicability"].startswith("DEPENDIENTE(")  # no local override keeps it red once §19 stops saying so
    dropped = oracle.build_case("SYNTHETIC", copy.deepcopy(SYN["SF075"]), "DEV", s19_with(
        lambda d: next(x for x in d["dependencies"] if x["dep_id"] == "DEP-O1B-OPPORTUNITIES").update(cases=[], cases_count=0)))
    assert dropped["s19"] is None and not dropped["applicability"].startswith("DEPENDIENTE(")


def test_the_primary_owner_follows_the_structural_owners_then_the_order_of_the_closed_list_of_the_extraction():
    two = s19_with(lambda d: next(x for x in d["dependencies"] if x["dep_id"] == "DEP-STATE-LITERALS").update(
        impacto_m2a3="COMPOSICION", owners=["F", "E-STATE"]))  # written in the opposite order on purpose
    case = oracle.build_case("SYNTHETIC", copy.deepcopy(SYN["SF009"]), "DEV", two)
    assert case["owners"] == ["E-STATE", "F"] and case["applicability"] == "DEPENDIENTE(E-STATE)"  # the list's order
    pending = oracle.build_case("SYNTHETIC", rebuilt_record("SF009", both(lambda L: L["prior"].update(state="PENDING"))),
                                "DEV", two)
    assert pending["owners"][:2] == ["M2A-4", "E-STATE"]  # a structural owner precedes the extraction's


def rebuilt_record(gid: str, change) -> dict:
    rec = copy.deepcopy(SYN[gid])
    change(rec)
    return rec


def test_audit_refuses_cases_that_do_not_match_the_verified_extraction():
    freed = [{**c, "applicability": oracle.APPLICABLE, "owners": [], "rules": [], "s19": None} if c["gold_id"] == "SF075" else c
             for c in CASES]  # SF075 as if §19 had never made it red
    report = oracle.audit(REFERENCE, freed)
    assert not report["ok"] and any("SF075" in p for p in report["s19_problems"])
    stale = [{**c, "rules": [r for r in c["rules"] if not r.startswith("DEP-")]} if c["gold_id"] == "SF075" else c for c in CASES]
    assert any("SF075" in p for p in oracle.audit(REFERENCE, stale)["s19_problems"])
    read_only = [{**c, "s19": None} if c["gold_id"] == "SF009" else c for c in CASES]  # a LECTURA case that forgot what it reads
    problems = oracle.audit(REFERENCE, read_only)["s19_problems"]
    assert [p for p in problems if "SF009" in p] == ["SF009: the case and the verified §19 extraction disagree"]


# ====================================================================================================================
# APPLICABILITY IS MECHANICAL, CITED, TRACEABLE AND LIVE
# ====================================================================================================================
def test_every_case_has_one_applicability_of_the_closed_vocabulary_and_the_reds_never_get_an_expectation():
    for c in CASES:
        a = c["applicability"]
        assert a == oracle.APPLICABLE or a == oracle.NO_PRODUCER or re.fullmatch(r"(DEPENDIENTE|NO_MEDIBLE)\([^()]+\)", a)
        if a.startswith("DEPENDIENTE("):
            assert c["owners"] and a == f"DEPENDIENTE({c['owners'][0]})" and c["rules"]
        else:
            assert c["owners"] == []
        assert (c["inputs"] is not None and c["expected"] is not None) == (a == oracle.APPLICABLE)  # never a guess
    assert APPLICABLE and all(c["partition"] in ("DEV", "TEST") for c in CASES)


def test_a_case_whose_labels_disagree_on_the_result_is_never_measured():
    flagged = [c for c in CASES if "RESULT_IR_AB_DISAGREE" in c["flags"]]
    assert flagged and all(c["applicability"] != oracle.APPLICABLE for c in flagged)


def test_every_rule_cites_its_clause_and_the_s19_rows_say_whether_they_block():
    table = applicability.rule_table(S19)
    structural = [r for r in table if r["basis"] == "STRUCTURAL"]
    assert [r["id"] for r in structural] == ["PENDING_PRIOR", "EVENT_OR_PROTOCOL"]  # only what §19 does not cover
    for rule in structural:
        assert rule["clause"].strip() and rule["why"].strip() and "§" in rule["clause"] and rule["owners"], rule["id"]
        assert not {o for o in rule["owners"]} & {o for d in S19["dependencies"] for o in d["owners"]} - {"M2A-6"}
    rows = [r for r in table if r["basis"] == "§19"]
    assert [r["id"] for r in rows] == [d["dep_id"] for d in S19["dependencies"]]
    for rule in rows:
        assert rule["blocks"] == (rule["impacto_m2a3"] == "COMPOSICION") and rule["clause"].startswith("§19 DEP-")
    assert oracle.declarations()["applicability_rules"] == table


def _first_value_op(label):
    return next(o for o in label["ops"] if o["op"] in ("SET", "SET_ORDER"))


MOVES = [  # a Gold FACT changed in memory must move the case to the owner its structural rule names
    ("SF001", "PENDING_PRIOR", "DEPENDIENTE(M2A-4)", both(lambda L: L["prior"].update(state="PENDING"))),
    ("SF001", "ANSWER_OP", "DEPENDIENTE(M2A-4)", both(lambda L: _first_value_op(L).update(op="ANSWER"))),
    ("SF001", "CANCEL_PENDING_OP", "DEPENDIENTE(M2A-4)", both(lambda L: _first_value_op(L).update(op="CANCEL_PENDING"))),
    ("SF001", "EVENT", "DEPENDIENTE(M2A-6)", both(lambda L: L.update(input_kind="EVENT"))),
    ("SF001", "PROTOCOL_TURN", "DEPENDIENTE(M2A-6)", both(lambda L: L.update(turn_kind="PROTOCOL"))),
    ("SF001", "PROTOCOL_SCOPE", "DEPENDIENTE(M2A-6)", lambda rec: rec.update(m2a_scope="IN_PROTOCOL")),
]


@pytest.mark.parametrize("gid,name,expected,change", MOVES, ids=[f"{m[0]}-{m[1]}" for m in MOVES])
def test_a_gold_fact_moves_the_case_to_the_owner_its_structural_rule_names(gid, name, expected, change):
    assert BY_ID[gid]["applicability"] == oracle.APPLICABLE  # the unmutated case is measurable
    assert rebuilt(gid, change)["applicability"] == expected


def test_every_structural_rule_is_exercised_by_a_gold_fact_that_moves_a_case():
    exercised = set()
    for gid, _name, _expected, change in MOVES:
        exercised |= set(rebuilt(gid, change)["rules"])
    assert {r.id for r in applicability.STRUCTURAL_RULES} <= exercised


def test_the_retired_dependency_rules_are_gone_and_what_they_held_back_is_decided_by_section_19():
    """The rules PLAN/STRUCTURAL that overlapped §19 (derived view, SMMLV, unit and year inheritance, region, H6, states and
    modalities of the universe, unresolved authority values) no longer exist: the extraction says each case's impact."""
    assert not hasattr(applicability, "RULES") and not hasattr(applicability, "unresolved_owners")
    assert not hasattr(applicability, "OWNER_ORDER") and not hasattr(applicability, "USER_DECISION")
    inherited = [g for g, c in BY_ID.items() if c["s19"] and "DEP-INHERITANCE" in c["s19"]["dependencies"]]
    assert inherited and all(BY_ID[g]["applicability"] != "DEPENDIENTE(E-TIME)" for g in inherited)  # LECTURA: read, not red
    assert BY_ID["SF009"]["applicability"] == oracle.APPLICABLE  # a state SET over an unchanged universe reads no literals
    assert BY_ID["SF078"]["applicability"] == "NO_MEDIBLE(HOST_FACT_NOT_IN_GOLD:STATE_LITERALS_OF_THE_UNIVERSE)"  # H8 after a change
    assert BY_ID["SF075"]["applicability"] == "DEPENDIENTE(FOUNDATION_O1B_EXPLICIT_TIME_PARITY)"  # now §19's, not a user note


def test_an_unresolved_op_is_not_a_dependency_compose_refuses_it_or_it_cannot_be_typed():
    """«unresolved» marks the rejection of compose (F16 fail-closed, a KEEP of an empty slot, a slot conflict...) or a value the
    Gold does not type. It is never an owner by itself (a finding of ZEUS on SF194)."""
    f16 = BY_ID["SF194"]
    assert f16["applicability"] == oracle.APPLICABLE and f16["expected"]["code"] == "UNRESOLVED_VALUE:TOPIC"
    assert f16["s19"] is None and f16["owners"] == []
    flagged = [g for g, r in SYN.items() if any(o["unresolved"] for n in ("label_A", "label_B") for o in r[n]["ops"])]
    assert len(flagged) > 10
    structural = {r.id for r in applicability.STRUCTURAL_RULES}
    rows = {d["dep_id"]: d["impacto_m2a3"] for d in S19["dependencies"]}
    for gid in flagged:  # the class, whole: a red needs §19 (COMPOSICION) or a structural rule, never the mark
        case = BY_ID[gid]
        if case["applicability"].startswith("DEPENDIENTE("):
            assert all(r in structural or rows.get(r) == "COMPOSICION" for r in case["rules"]), gid
    measurable = [g for g in flagged if BY_ID[g]["applicability"] == oracle.APPLICABLE]
    assert {"SF193", "SF194", "SF196", "SF197", "SF198"} <= set(measurable)
    changed = rebuilt("SF001", both(lambda L: _first_value_op(L).update(unresolved=True)))
    assert changed["applicability"] == oracle.APPLICABLE and changed["rules"] == BY_ID["SF001"]["rules"]


def test_a_host_rule_that_reads_a_fact_the_gold_does_not_carry_is_no_medible_never_a_dependency():
    """H6/H7/H8 over a CLARIFY need a fact (the carried buyer's territory, the closed list of regions, the states of the
    universe) the Gold's IR does not carry. §19 says LECTURA, so they are not reds; the oracle cannot type the fact without
    guessing, so a case where the Gold expects the rule to FIRE is NO_MEDIBLE with the fact named. A control that executes
    stays measurable only when no rule's decision reads the missing fact (see ``test_h8_...``)."""
    host = gold.host_rules()
    assert set(expect_mod.HOST_RULE_FACTS) <= set(host)
    for rule, (fact, clause) in expect_mod.HOST_RULE_FACTS.items():
        assert host[rule][1] and "§" in clause and fact
        fired = [c for c in CASES if c["reason"] == host[rule][1] and c["disposition"] in ("CLARIFY", "INFORM")]
        assert fired, rule
        assert all(c["applicability"] == f"NO_MEDIBLE(HOST_FACT_NOT_IN_GOLD:{fact})" and c["owners"] == [] for c in fired)
    controls = [c for c in APPLICABLE if c["gold_id"] in ("SF009", "SF010")]
    assert len(controls) == 2 and all(c["expected"]["disposition"] == "EXECUTE" for c in controls)
    moved = rebuilt("SF009", lambda rec: (rec.update(expected_disposition="CLARIFY"), both(lambda L: L.update(
        expected_disposition="CLARIFY", disposition_reason=host["H8"][1], resulting_ir=None))(rec)))
    assert moved["applicability"] == "NO_MEDIBLE(HOST_FACT_NOT_IN_GOLD:STATE_LITERALS_OF_THE_UNIVERSE)"


H8_FACT = "NO_MEDIBLE(HOST_FACT_NOT_IN_GOLD:STATE_LITERALS_OF_THE_UNIVERSE)"


def _requests(*pairs):
    return {"requests": [{"subject": s, "universe_kind": k} for s, k in pairs]}


def _typed(slot, dimension):
    return {"slot": slot, "dimension": dimension}


def _op(kind, slot, cond_dimension=None, universe=None):
    return {"op": kind, "slot": slot, "cond": None if cond_dimension is None else {"dimension": cond_dimension}, "universe": universe}


@pytest.mark.parametrize("name,base,ops,reads", [
    ("state set after a change", [], [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["processes", "records"]),
                                      _op("SET", "STATE/POSITIVE", "STATE")], True),
    ("state carried after a change", [_typed("STATE/POSITIVE", "STATE")],
     [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["processes", "records"])], True),
    ("modality carried after a change", [_typed("MODALITY/POSITIVE", "MODALITY")],
     [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["processes", "records"])], True),
    ("modality set after a kind change", [], [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["contracts", "opportunities"]),
                                              _op("SET", "MODALITY/POSITIVE", "MODALITY")], True),
    ("negative state carried after a change", [_typed("STATE/NEGATIVE", "STATE")],
     [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["processes", "records"])], True),
    ("the state is cleared by the same patch", [_typed("STATE/POSITIVE", "STATE")],
     [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["processes", "records"]), _op("CLEAR", "STATE")], False),
    ("the state slot is replaced by a topic only", [_typed("STATE/POSITIVE", "STATE")],
     [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["processes", "records"]), _op("CLEAR", "STATE/POSITIVE")], False),
    ("a change with no state or modality", [_typed("TOPIC/POSITIVE", "TOPIC")],
     [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["processes", "records"])], False),
    ("a state next to an unchanged universe", [_typed("STATE/POSITIVE", "STATE")],
     [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["contracts", "records"])], False),
    ("a state set with no redirect", [], [_op("SET", "STATE/POSITIVE", "STATE")], False),
])
def test_h8_reads_the_literals_after_a_universe_change_with_a_state_or_a_modality_standing(name, base, ops, reads):
    assert expect_mod.h8_reads_the_literals(_requests(("contracts", "records")), base, ops) is reads, name


def test_a_fresh_acceptance_composes_nothing_so_h8_reads_nothing():
    """A NEW_SEARCH is built by from_acceptance (M2A-1b), not by compose: its STATE next to another universe is the
    authority's, and the case stays measurable."""
    assert BY_ID["SF124"]["applicability"] == oracle.APPLICABLE and BY_ID["SF124"]["expected"]["effective_scope"] == "FRESH_ACCEPTANCE"
    state = {"id": "c9", "dimension": "STATE", "operator": "is", "polarity": "POSITIVE", "level": None, "unit": None,
             "value_hint": "awarded"}
    with_state = rebuilt("SF124", both(lambda L: L["resulting_ir"]["conditions"].append(dict(state))))
    assert with_state["applicability"] == oracle.APPLICABLE
    assert {c["dimension"] for c in with_state["expected"]["effective"]["conditions"]} >= {"STATE"}


def _states_over_an_unchanged_universe(cases):
    """Applicable composed cases that SET or CARRY a STATE with no change of universe: measured on the assumption (declared in
    LG-H8-ONLY-AFTER-A-UNIVERSE-CHANGE) that the authority resolved the state against that dataset."""
    out = []
    for case in cases:
        if case["applicability"] != oracle.APPLICABLE or case["expected"]["effective_scope"] != "COMPOSED":
            continue
        base, patch = case["inputs"]["base"], case["inputs"]["patch"]
        held = {(r["subject"], r["universe_kind"]) for r in base["requests"]}
        moves = any((op["universe"]["subject"], op["universe"]["universe_kind"]) not in held for op in patch["ops"]
                    if op["op"] == "REPLACE_UNIVERSE")
        written = [op["slot"] for op in patch["ops"] if op["op"] in ("SET", "SET_ORDER", "CLEAR")]
        standing = [c for c in base["conditions"] if not any(transition_module._covered(w, c["slot"]) for w in written)]
        placed = [op["condition"] for op in patch["ops"] if op["condition"] is not None]
        if not moves and any(c["dimension"] == "STATE" for c in (*standing, *placed)):
            out.append(case["gold_id"])
    return out


def test_the_states_over_an_unchanged_universe_are_the_declared_h8_assumption():
    ids = _states_over_an_unchanged_universe(CASES)
    assert {"SF009", "SF010"} <= set(ids)  # the controls: a state set, the universe untouched
    declared = {g["id"]: g for g in oracle.declarations()["limited_guarantees"]}["LG-H8-ONLY-AFTER-A-UNIVERSE-CHANGE"]
    assert "UNCHANGED universe" in declared["what"] and declared["kind"] == "ORACLE_BLIND_SPOT"


def test_h8_reads_the_literals_of_a_dual_anchor_only_when_the_universe_is_not_held():
    dual = _requests(("processes", "records"), ("contracts", "records"))
    state = [_typed("STATE/POSITIVE", "STATE")]
    assert not expect_mod.h8_reads_the_literals(dual, state, [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["contracts", "records"])])
    assert expect_mod.h8_reads_the_literals(dual, state, [_op("REPLACE_UNIVERSE", "UNIVERSE", universe=["contracts", "opportunities"])])


def test_h8_decides_the_case_whatever_the_outcome_and_no_applicable_case_depends_on_a_host_fact_the_gold_does_not_type():
    """§14: «tras cambiar el universo, F27 (H8) comprueba STATE y MODALITY sobre el dataset resultante». A change of universe
    with a STATE or MODALITY standing is NO_MEDIBLE (EXECUTE or not: SF078, REAL R0094). This test checks that no applicable
    case (composed) has such a change, and that none expects a H6, H7 or H8 outcome. A territory next to a carried buyer is
    ``test_h6_has_no_executed_control_in_this_gold_and_says_so``'s."""
    assert BY_ID["SF078"]["applicability"] == H8_FACT and BY_ID["SF079"]["applicability"] == H8_FACT
    hidden = []
    for case in APPLICABLE:
        base, patch = case["inputs"]["base"], case["inputs"]["patch"]
        if case["expected"]["effective_scope"] == "FRESH_ACCEPTANCE":
            continue
        held = {(r["subject"], r["universe_kind"]) for r in base["requests"]}
        moves = any((op["universe"]["subject"], op["universe"]["universe_kind"]) not in held for op in patch["ops"]
                    if op["op"] == "REPLACE_UNIVERSE")
        written = [op["slot"] for op in patch["ops"] if op["op"] in ("SET", "SET_ORDER", "CLEAR")]
        standing = [c for c in base["conditions"] if not any(transition_module._covered(w, c["slot"]) for w in written)]
        placed = [op["condition"] for op in patch["ops"] if op["condition"] is not None]
        if moves and any(c["dimension"] in ("STATE", "MODALITY") for c in (*standing, *placed)):
            hidden.append(case["gold_id"])
    assert hidden == []
    host = gold.host_rules()
    assert not [c["gold_id"] for c in APPLICABLE if c["expected"]["code"] in {host[r][1] for r in expect_mod.HOST_RULE_FACTS}]
    carried = rebuilt("SF073", both(lambda L: L["prior"]["ir"]["conditions"].append(
        {"id": "c9", "dimension": "STATE", "operator": "is", "polarity": "POSITIVE", "level": None, "unit": None,
         "value_hint": "awarded"})))
    assert BY_ID["SF073"]["applicability"] == oracle.APPLICABLE and carried["applicability"] == H8_FACT  # the change carries it


def test_a_lectura_dependency_that_the_gold_types_is_measurable_regions_included():
    """§19 DEP-REGION is LECTURA: compose receives the typed region, so the cases with a region are measured (a region carries
    one synthetic member: LG-REGION-MEMBERS-SYNTHETIC). The case where the Gold says the region is NOT in the list is the one
    that stays NO_MEDIBLE (H7 reads the list, which the Gold does not carry)."""
    regions = [g for g, c in BY_ID.items() if c["s19"] and "DEP-REGION" in c["s19"]["dependencies"]]
    assert {"SF003", "SF004", "SF124"} <= set(regions)
    assert all(BY_ID[g]["applicability"] == oracle.APPLICABLE for g in ("SF003", "SF004", "SF124"))
    assert BY_ID["SF159"]["applicability"] == "NO_MEDIBLE(HOST_FACT_NOT_IN_GOLD:REGION_CLOSED_LIST)"
    assert all(BY_ID[g]["owners"] == [] for g in regions)
    for gid in ("SF003", "SF004"):
        op = next(o for o in BY_ID[gid]["inputs"]["patch"]["ops"] if o["condition"] and o["condition"]["dimension"] == "GEOGRAPHY")
        assert decode_value(tuple(op["condition"]["value"])).level.value == "REGION"


def test_an_explicit_relative_window_on_which_both_labels_agree_is_measurable_and_only_a_disagreement_excludes_it():
    """The labels of SF027, SF028 and SF029 agree byte for byte on a window the hint states («últimos 3 meses
    (2026-07-01..2026-10-01)»): the case is measured, with the typed interval. Where the labels give different ends (SF025,
    SF026, SF030, SF036, SF161) the case is NO_MEDIBLE with the true motive: they disagree on the ops."""
    agree = ("SF027", "SF028", "SF029")
    for gid in agree:
        assert json.dumps(SYN[gid]["label_A"]["ops"], sort_keys=True) == json.dumps(SYN[gid]["label_B"]["ops"], sort_keys=True)
        assert BY_ID[gid]["applicability"] == oracle.APPLICABLE and BY_ID[gid]["owners"] == []
        time = next(c for c in BY_ID[gid]["expected"]["effective"]["conditions"] if c["dimension"] == "TIME")
        assert time["operator"] == "relative_period" and time["value"][0] == "time@1"
    window = next(o["condition"]["value"] for o in BY_ID["SF028"]["inputs"]["patch"]["ops"] if o["condition"])
    assert window[1:3] == ["start=2026-07-01", "end_exclusive=2026-10-02"]  # the end day is inclusive, as in between_dates
    disagree = [g for g in ("SF025", "SF026", "SF030", "SF036", "SF161")]
    for gid in disagree:
        ends = {re.search(r"\.\.(\d{4}-\d{2}-\d{2})", o["value_hint"]).group(1) for n in ("label_A", "label_B")
                for o in SYN[gid][n]["ops"] if o["operator"] == "relative_period"}
        assert len(ends) == 2, gid  # the windows really end on different days
        assert BY_ID[gid]["applicability"] == "NO_MEDIBLE(OPS_AB_DISAGREE)" and BY_ID[gid]["inputs"] is None, gid
    not_explicit = rebuilt("SF028", both(lambda L: [o.update(value_hint="últimos 3 meses") for o in L["ops"]]
                                         + [c.update(value_hint="últimos 3 meses") for c in L["resulting_ir"]["conditions"]
                                            if c["dimension"] == "TIME"]))
    assert not_explicit["applicability"] == "NO_MEDIBLE(TIME_RELATIVE_WINDOW_NOT_EXPLICIT)"


def test_surface_forms_of_value_are_measurable_only_when_both_labels_agree_on_the_operation():
    flagged = [c for c in CASES if "SURFACE_FORM_READ_BY_E_VALUE" in c["flags"]]
    assert flagged and [c for c in flagged if c["applicability"] == oracle.APPLICABLE]
    for c in flagged:
        if c["applicability"] == oracle.APPLICABLE:
            sig = lambda L: sorted((o["op"], o["slot"], o["operator"], o["polarity"]) for o in L["ops"])  # noqa: E731
            assert sig(SYN[c["gold_id"]]["label_A"]) == sig(SYN[c["gold_id"]]["label_B"])
    gid = next(c["gold_id"] for c in flagged if c["applicability"] == oracle.APPLICABLE)
    changed = rebuilt(gid, lambda rec: (rec["label_B"]["ops"][0].update(operator="less_than"),
                                        rec["label_B"].update(gap_codes=["B5RANGE"])))
    assert changed["applicability"] == "NO_MEDIBLE(SURFACE_FORM_AB_DISAGREE)"


def test_the_h5_control_executes_and_the_ambiguous_case_is_not_measured():
    assert BY_ID["SF164"]["applicability"] == oracle.APPLICABLE and BY_ID["SF164"]["expected"]["disposition"] == "EXECUTE"
    assert BY_ID["SF163"]["applicability"] == "NO_MEDIBLE(OPS_AB_DISAGREE)" and BY_ID["SF163"]["owners"] == []


def test_the_new_op_shapes_of_the_seal_never_break_the_oracle():
    """KEEP with ``inherited: null``, CLEAR / ANSWER with a null polarity, the OUT_UNSUPPORTED_PROPOSAL scope and the codes
    of annex A.3 (KEEP_ON_EMPTY_SLOT, DELTA_SLOT_CONFLICT, DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER,
    UNSUPPORTED_PROPOSAL:<motivo>, UNRESOLVED_VALUE:<DIM>) are read as data."""
    shapes = collections.Counter()
    for rec in SYN.values():
        for name in ("label_A", "label_B"):
            for op in rec[name]["ops"]:
                for key, value in op.items():
                    shapes[(op["op"], key, value is None)] += 1
    assert shapes[("KEEP", "inherited", True)] and shapes[("CLEAR", "polarity", True)] and shapes[("ANSWER", "polarity", True)]
    assert any(r["m2a_scope"] == "OUT_UNSUPPORTED_PROPOSAL" for r in SYN.values())
    assert {c["reason"] for c in CASES} >= {"KEEP_ON_EMPTY_SLOT", "DELTA_SLOT_CONFLICT",
                                            "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER", "UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL",
                                            "UNRESOLVED_VALUE:TOPIC"}
    assert all(c["applicability"] for c in CASES) and len(CASES) == len(SYN)


def test_the_reason_vocabulary_checks_the_tail_and_the_pair_the_contract_declares():
    chk = gold.checker()
    gold.check_reason("UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL", "INFORM")
    gold.check_reason("UNRESOLVED_VALUE:TOPIC", "CLARIFY")
    gold.check_reason("DELTA_SLOT_CONFLICT", "INFORM")  # an INFORM-capable CLARIFY code
    for reason, disposition in (("UNRESOLVED_VALUE:NOT_A_DIMENSION", "CLARIFY"), ("UNSUPPORTED_PROPOSAL:NO_SUCH_MOTIVE", "INFORM"),
                                ("UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL", "CLARIFY"), ("OPTION_NOT_CHOSEN", "EXECUTE"),
                                ("CLEAR_ON_EMPTY_SLOT", "CLARIFY")):
        with pytest.raises(gold.OracleError):
            gold.check_reason(reason, disposition)
    assert "UNSUPPORTED_PROPOSAL:" in chk.REASON_PREFIXES and chk.REASON_TAILS["UNSUPPORTED_PROPOSAL:"]


@pytest.mark.parametrize("gid", ["SF001", "SF021", "SF091", "SF113", "SF128", "SF138", "SF025", "SF047"])
def test_notes_text_and_confidence_never_reach_an_expectation(gid):
    def scrub(label):
        label.update(notes="x" * 40, followup_text="otro texto cualquiera", confidence="LOW", labeler="ZZ")
    assert rebuilt(gid, both(scrub)) == BY_ID[gid]


def test_no_labeler_is_privileged_and_the_order_of_the_ops_never_matters():
    for gid in SYN:
        swapped = rebuilt(gid, lambda rec: rec.update(label_A=rec["label_B"], label_B=rec["label_A"]))
        assert swapped == BY_ID[gid], gid
    for case in APPLICABLE:
        if len(case["inputs"]["patch"]["ops"]) >= 2:
            assert rebuilt(case["gold_id"], both(lambda L: L["ops"].reverse())) == case


# ---- NO_MEDIBLE is declared, never chosen at will ---------------------------------------------------------------------------
def test_labels_that_disagree_on_the_result_are_not_resolved_at_will():
    changed = rebuilt("SF001", lambda rec: rec["label_B"]["resulting_ir"]["conditions"][0].update(value_hint="otro"))
    assert changed["applicability"] == "NO_MEDIBLE(RESULT_AB_DISAGREE)"
    assert "RESULT_IR_AB_DISAGREE" in changed["flags"] and changed["expected"] is None


def _b_reads_another_topic(rec):  # label B is self-consistent (op and result agree) but reads another value than A
    label = rec["label_B"]
    label["ops"][0]["value_hint"] = label["resulting_ir"]["conditions"][2]["value_hint"] = "otro"


@pytest.mark.parametrize("gid,change,motive", [
    ("SF001", lambda rec: rec["label_B"]["prior"]["ir"]["conditions"][0].update(value_hint="otro"), "PRIOR_AB_DISAGREE"),
    ("SF001", lambda rec: rec["label_B"]["prior"].update(gap_seconds=9999), "PRIOR_AB_DISAGREE"),
    ("SF001", lambda rec: rec["label_B"]["prior"].update(state="NOT_EXECUTED"), "PRIOR_AB_DISAGREE"),
    ("SF001", lambda rec: rec["label_B"]["prior"].update(owner="LEGACY_SEARCH"), "PRIOR_AB_DISAGREE"),
    ("SF001", _b_reads_another_topic, "OPS_AB_DISAGREE"),
    ("SF138", lambda rec: rec["label_B"].update(disposition_reason="NO_ANCHOR"), "LABELS_DISAGREE_ON_REASON_OR_TURN"),
    ("SF001", lambda rec: rec["label_B"].update(turn_kind="NEW_SEARCH"), "LABELS_DISAGREE_ON_REASON_OR_TURN"),
    ("SF001", lambda rec: rec["label_B"].update(m2a_scope="IN_NEW_SEARCH"), "LABELS_DISAGREE_ON_REASON_OR_TURN"),
])
def test_an_ab_disagreement_without_adjudication_is_no_medible(gid, change, motive):
    assert rebuilt(gid, change)["applicability"] == f"NO_MEDIBLE({motive})"


@pytest.mark.parametrize("change,motive", [
    (both(lambda L: _first_value_op(L).update(value_hint="   ")), "HINT_EMPTY"),
    (both(lambda L: [ir["conditions"].append({  # a carried STATE the closed classes do not know
        "dimension": "STATE", "id": "c9", "level": None, "operator": "is", "polarity": "POSITIVE", "unit": None,
        "value_hint": "not a state class"}) for ir in (L["prior"]["ir"], L["resulting_ir"])]), "STATE_CLASS"),
    (both(lambda L: L["resulting_ir"]["conditions"][2].update(value_hint="otra frase")), "OP_DISAGREES_WITH_RESULT"),
    (both(lambda L: L["resulting_ir"].update(logic={"operator": "OR"})), "IR_LOGIC"),
])
def test_what_cannot_be_typed_or_does_not_match_the_result_is_no_medible(change, motive):
    assert rebuilt("SF001", change)["applicability"] == f"NO_MEDIBLE({motive})"


def test_a_new_condition_without_an_op_on_its_slot_is_no_medible():
    def extra(label):
        label["resulting_ir"]["conditions"].append({"dimension": "CONTRACT_TYPE_OR_NATURE", "id": "c9", "level": None,
                                                    "operator": "is", "polarity": "POSITIVE", "unit": None,
                                                    "value_hint": "obra"})
    assert rebuilt("SF001", both(extra))["applicability"] == "NO_MEDIBLE(NEW_CONDITION_WITHOUT_OP)"


def test_a_carried_condition_lost_without_an_op_or_a_declared_substitution_is_no_medible():
    def lose(label):
        label["resulting_ir"]["conditions"] = [c for c in label["resulting_ir"]["conditions"] if c["dimension"] != "BUYER"]
    assert rebuilt("SF001", both(lose))["applicability"] == "NO_MEDIBLE(CARRIED_CONDITION_LOST_WITHOUT_OP)"


def test_a_lost_cardinality_is_never_attributed_to_a_polarity_inversion():
    def drop_cardinality(label):  # SET_ORDER leaves the CARDINALITY (F10): a result that loses it cites no op
        label["resulting_ir"]["cardinality"] = None
    assert rebuilt("SF018", both(drop_cardinality))["applicability"] == "NO_MEDIBLE(CARRIED_CONDITION_LOST_WITHOUT_OP)"


def test_a_bound_lost_without_collision_is_not_an_a8_1_replacement():
    def lost_without_collision(label):
        _first_value_op(label).update(value_hint="hasta 800 millones")
        for c in label["resulting_ir"]["conditions"]:
            if c["dimension"] == "VALUE" and c["operator"] == "less_or_equal":
                c["value_hint"] = "800 millones"
        label["resulting_ir"]["conditions"] = [c for c in label["resulting_ir"]["conditions"]
                                               if not (c["dimension"] == "VALUE" and c["operator"].startswith("greater"))]
    assert rebuilt("SF091", both(lost_without_collision))["applicability"] == \
        "NO_MEDIBLE(A8_1_BOUND_LOST_BUT_INTERVAL_NOT_EMPTY)"


def test_a_positive_lost_among_several_is_not_a_polarity_inversion_it_is_the_fail_closed_branch():
    """F16 inverts the SINGLE positive of a slot (§6.5): with another positive standing, a result that dropped the negated one
    is not that substitution, and a Gold that executes it contradicts the contract: NO_MEDIBLE, never POLARITY_INVERTED."""
    def two_positives(label):
        label["prior"]["ir"]["conditions"].append({"id": "c9", "dimension": "GEOGRAPHY", "operator": "is", "polarity": "POSITIVE",
                                                   "level": "MUNICIPALITY", "unit": None, "value_hint": "Medellín"})
        label["resulting_ir"]["conditions"].append({"id": "c9", "dimension": "GEOGRAPHY", "operator": "is",
                                                    "polarity": "POSITIVE", "level": "MUNICIPALITY", "unit": None,
                                                    "value_hint": "Medellín"})
    assert BY_ID["SF011"]["expected"]["effects"] == ["POLARITY_INVERTED"]  # the single positive: the inversion is declared
    for label in ("label_A", "label_B"):
        assert SYN["SF011"][label]["prior"]["ir"]["conditions"][1]["level"] == "MUNICIPALITY"
    assert rebuilt("SF011", both(two_positives))["applicability"] == "NO_MEDIBLE(CARRIED_CONDITION_LOST_WITHOUT_OP)"


def test_a_universe_that_moves_without_an_op_is_not_measured():
    """§5.2: only REPLACE_UNIVERSE changes the universe; a Gold whose result universe differs from the prior with no such op
    is not a determined expectation."""
    moved = rebuilt("SF001", both(lambda L: L["resulting_ir"].update(universe="PROCESSES" if L["prior"]["ir"]["universe"] != "PROCESSES" else "CONTRACTS")))
    assert BY_ID["SF001"]["applicability"] == oracle.APPLICABLE
    assert moved["applicability"] == "NO_MEDIBLE(UNIVERSE_CHANGED_WITHOUT_OP)"


def test_a_gold_whose_facts_determine_another_scope_than_its_own_is_declared_not_followed():
    def not_new(label):  # a NEW_SEARCH whose result brings no new TOPIC/ACTOR: §7.2 (b) says DELTA
        for c in label["resulting_ir"]["conditions"]:
            same = next(p for p in label["prior"]["ir"]["conditions"] if p["dimension"] == c["dimension"]) \
                if c["dimension"] in ("TOPIC", "BUYER") else None
            if same:
                c["value_hint"] = same["value_hint"]
    assert rebuilt("SF128", both(not_new))["applicability"] == "NO_MEDIBLE(GOLD_CONTRADICTS_SECTION_7_2)"


def test_a_new_search_the_facts_leave_undetermined_needs_the_producer(monkeypatch):
    monkeypatch.setattr(expect_mod, "outcomes", lambda facts: {"DELTA", "NEW_SEARCH"})
    assert rebuilt("SF128", lambda rec: None)["applicability"] == oracle.NO_PRODUCER


# ====================================================================================================================
# THE EFFECTS
# ====================================================================================================================
def test_value_range_replaced_is_required_of_the_reducer_only_when_a81_fires():
    a81 = rebuilt("SF091", lambda rec: None)  # a new bound that collides with the carried opposite bound
    assert a81["expected"]["effects"] == ["VALUE_RANGE_REPLACED"]
    explicit = BY_ID["SF100"]  # CLEAR(VALUE) then SET: an obligation of ECHO (§11), not an effect of the reducer
    assert explicit["expected"]["effects"] == [] and "VALUE_RANGE_REPLACED" in explicit["expected"]["effects_gold"]
    assert explicit["expected"]["effects_status"] == "MEDIBLE"
    assert oracle.audit({**REFERENCE, "SF100": {**REFERENCE["SF100"], "effects": ["VALUE_RANGE_REPLACED"]}}, CASES)["ok"]
    assert oracle.audit(REFERENCE, CASES)["ok"]  # and it is not required


def test_the_effect_the_reducer_owes_is_derived_from_the_typed_values_not_copied_from_the_checker():
    def checker_says_nothing(label):
        label["effects"] = []
    case = rebuilt("SF091", both(checker_says_nothing))
    assert case["expected"]["effects"] == ["VALUE_RANGE_REPLACED"]


def test_a_stored_effect_nobody_can_corroborate_makes_the_effects_no_medible():
    stored = rebuilt("SF091", lambda rec: rec["label_B"].update(effects=["VALUE_RANGE_REPLACED", "POLARITY_INVERTED"]))
    assert stored["applicability"] == oracle.APPLICABLE
    assert stored["expected"]["effects_status"] == "NO_MEDIBLE(EFFECTS_NOT_CORROBORATED)" and stored["expected"]["effects"] == []
    only_one = rebuilt("SF041", lambda rec: rec["label_B"].update(effects=[]))
    assert only_one["expected"]["effects"] == ["TOPIC_REPLACED"]  # derived by §6.5 even when no label stored it


def test_a_turn_that_does_not_execute_has_no_declared_substitution(monkeypatch):
    chk = gold.checker()
    real = chk.derive_effects
    monkeypatch.setattr(chk, "derive_effects", lambda label: real(label) | {"VALUE_RANGE_REPLACED"})
    case = oracle.build_case("SYNTHETIC", copy.deepcopy(SYN["SF138"]), "DEV", S19)  # CLARIFY CONTEXT_EXPIRED_CONFIRM
    assert case["applicability"] == oracle.APPLICABLE
    assert case["expected"]["effects"] == [] and case["expected"]["effects_status"] == "NO_MEDIBLE(EFFECTS_ON_NON_EXECUTION)"


def test_a_new_effect_after_a_re_seal_is_refused_until_someone_decides_whose_it_is(monkeypatch):
    chk = gold.checker()
    monkeypatch.setattr(chk, "EFFECTS", set(chk.EFFECTS) | {"CARDINALITY_DROPPED"})
    with pytest.raises(gold.OracleError, match="CARDINALITY_DROPPED"):
        oracle.load_cases()  # the sealed vocabulary has an effect the oracle has no scope decision for
    monkeypatch.setattr(chk, "EFFECTS", set(chk.EFFECTS) - {"CARDINALITY_DROPPED", "TOPIC_REPLACED"})
    with pytest.raises(gold.OracleError, match="TOPIC_REPLACED"):
        oracle.load_cases()  # and the other way round: the oracle scopes an effect the seal no longer lists


def test_an_effect_a_label_lists_that_has_no_scope_decision_is_refused_not_ignored(monkeypatch):
    chk = gold.checker()
    real = chk.derive_effects
    monkeypatch.setattr(chk, "derive_effects", lambda L: real(L) | ({"CARDINALITY_DROPPED"} if L["gold_id"] == "SF023" else set()))
    with pytest.raises(gold.OracleError, match="no scope decision"):
        oracle.build_case("SYNTHETIC", copy.deepcopy(SYN["SF023"]), "DEV", S19)


def test_the_reducer_and_echo_effects_partition_the_sealed_vocabulary():
    assert set(gold.checker().EFFECTS) == oracle.REDUCER_EFFECTS | oracle.ECHO_EFFECTS
    assert not oracle.REDUCER_EFFECTS & oracle.ECHO_EFFECTS


# ====================================================================================================================
# TYPING PRESERVES THE EQUALITIES THE REDUCER USES
# ====================================================================================================================
TEXT = st.text(alphabet=st.characters(blacklist_categories=("Cs",)), min_size=1, max_size=40).filter(lambda t: t.strip())


@pytest.mark.parametrize("dimension,operator", [("TOPIC", "contains"), ("BUYER", "is"), ("ACTOR", "is"),
                                                  ("MODALITY", "is"), ("MODALITY", "family"),
                                                  ("CONTRACT_TYPE_OR_NATURE", "is")])
@FAST
@given(a=TEXT, b=TEXT)
def test_opaque_dimensions_same_hint_same_value_and_distinct_hints_distinct_values(dimension, operator, a, b):
    ta = typing_.type_value(dimension, operator, "POSITIVE", None, None, a)
    assert ta == typing_.type_value(dimension, operator, "POSITIVE", None, None, a)
    same = typing_.nfc(a) == typing_.nfc(b)
    assert (ta == typing_.type_value(dimension, operator, "POSITIVE", None, None, b)) == same
    assert ta != typing_.type_value(dimension, operator, "NEGATIVE", None, None, a)  # polarity is part of the value


@FAST
@given(a=TEXT, b=TEXT)
def test_an_actor_is_identified_by_a_synthetic_document_derived_from_the_hint(a, b):
    va, vb = (decode_value(typing_.type_value("ACTOR", "is", "POSITIVE", None, None, h)) for h in (a, b))
    assert (va.document == vb.document) == (typing_.nfc(a) == typing_.nfc(b))
    assert re.fullmatch(r"H[0-9A-F]{15}", va.document) and va.display == typing_.nfc(a)


@FAST
@given(a=TEXT, b=TEXT)
def test_geography_distinguishes_hints_and_levels(a, b):
    dep = typing_.type_value("GEOGRAPHY", "is", "POSITIVE", "DEPARTMENT", None, a)
    assert (dep == typing_.type_value("GEOGRAPHY", "is", "POSITIVE", "DEPARTMENT", None, b)) == (
        typing_.nfc(a) == typing_.nfc(b))
    assert dep != typing_.type_value("GEOGRAPHY", "is", "POSITIVE", "MUNICIPALITY", None, a)
    region = typing_.type_value("GEOGRAPHY", "is", "POSITIVE", "REGION", None, a)  # members: synthetic, derived from the hint
    assert region != dep and (region == typing_.type_value("GEOGRAPHY", "is", "POSITIVE", "REGION", None, b)) == (
        typing_.nfc(a) == typing_.nfc(b))
    decoded = decode_value(region)
    assert len(decoded.members) == 1 and decoded.members[0].startswith("oracle-member-") and decoded.canonical == typing_.nfc(a)
    with pytest.raises(typing_.Untypeable):
        typing_.type_value("GEOGRAPHY", "is", "POSITIVE", "UNKNOWN", None, a)  # a level nobody resolved is not guessed


def test_a_smmlv_amount_is_converted_with_the_sealed_catalogue_at_the_single_year_of_its_ir():
    catalogue = gold.checker().SMMLV_COP_BY_YEAR  # the catalogue of D10 the sealed checker uses: never copied here
    assert catalogue and 2023 not in catalogue
    for year, salary in catalogue.items():
        value = typing_.type_value("VALUE", "greater_or_equal", "POSITIVE", None, "SMMLV", "500 SMMLV", year)
        assert value == ("money@1", f"amount_cop={500 * salary}", "comparator=>=")
    even = next(y for y, s in catalogue.items() if s % 2 == 0)
    assert typing_.type_value("VALUE", "less_than", "POSITIVE", None, "SMMLV", "2,5 SMMLV", even)[1] == (
        f"amount_cop={catalogue[even] * 5 // 2}")
    odd = [y for y, s in catalogue.items() if s % 2]
    if odd:  # half a peso is never rounded
        with pytest.raises(typing_.Untypeable, match="VALUE_NOT_WHOLE_PESOS"):
            typing_.type_value("VALUE", "less_than", "POSITIVE", None, "SMMLV", "2,5 SMMLV", odd[0])
    for year, motive in ((None, "SMMLV_YEAR_NOT_SINGLE"), (2023, "SMMLV_YEAR_NOT_CATALOGUED"), (2099, "SMMLV_YEAR_NOT_CATALOGUED")):
        with pytest.raises(typing_.Untypeable) as error:
            typing_.type_value("VALUE", "greater_or_equal", "POSITIVE", None, "SMMLV", "500 SMMLV", year)
        assert error.value.motive == motive  # nothing is extrapolated, no clock is read
    with pytest.raises(typing_.Untypeable) as error:
        typing_.type_value("VALUE", "greater_or_equal", "POSITIVE", None, "SMMLV", "500 millones", max(catalogue))
    assert error.value.motive == "VALUE_UNIT_WORD_DISAGREES"


def test_the_smmlv_catalogue_is_read_from_the_sealed_checker_not_kept_in_the_oracle(monkeypatch):
    year = max(gold.checker().SMMLV_COP_BY_YEAR)
    monkeypatch.setattr(gold.checker(), "SMMLV_COP_BY_YEAR", {year: 1_000_000, 2023: 1_160_000})
    assert typing_.type_value("VALUE", "greater_or_equal", "POSITIVE", None, "SMMLV", "7 SMMLV", year)[1] == "amount_cop=7000000"
    assert typing_.type_value("VALUE", "greater_or_equal", "POSITIVE", None, "SMMLV", "7 SMMLV", 2023)[1] == "amount_cop=8120000"


def test_the_year_of_an_ir_is_its_one_positive_time_in_one_civil_year():
    def ir(*times):
        return {"conditions": [{"dimension": "TIME", "operator": op, "polarity": pol, "level": None, "unit": None,
                                "value_hint": hint, "id": f"t{n}"} for n, (op, pol, hint) in enumerate(times)]}
    assert typing_.ir_year(ir(("during_year", "POSITIVE", "2025"))) == 2025
    assert typing_.ir_year(ir(("during_month", "POSITIVE", "2024-03"))) == 2024
    assert typing_.ir_year(ir(("between_dates", "POSITIVE", "2025-02-01..2025-06-30"))) == 2025
    assert typing_.ir_year(ir(("during_year", "POSITIVE", "2025"), ("during_year", "NEGATIVE", "2024"))) == 2025
    for none in (ir(), ir(("between_dates", "POSITIVE", "2023-01-01..2025-12-31")), ir(("between_dates", "POSITIVE", "2025-01-01..")),
                 ir(("during_year", "POSITIVE", "2024"), ("during_year", "POSITIVE", "2025")), ir(("during_year", "NEGATIVE", "2025"))):
        assert typing_.ir_year(none) is None
    assert typing_.ir_year(None) is None


def test_the_smmlv_cases_are_h3s_over_the_closed_catalogue_and_not_a_dependency():
    """§19 (U23): SF166, SF168 and R0303 carry SMMLV and are NOT a dependency (H3 decides them over the catalogue 2024-2026);
    SF165, SF167 and R0301 are LECTURA, so they are measured. SF168 is a declared invalid persisted state (D1)."""
    for gid in ("SF166", "SF168"):
        assert BY_ID[gid]["s19"] is None and BY_ID[gid]["owners"] == []
    for gid in ("SF165", "SF167"):
        assert BY_ID[gid]["s19"]["impacto_m2a3"] == "LECTURA" and "DEP-SMMLV-READ" in BY_ID[gid]["s19"]["dependencies"]
    for gid, disposition in (("SF165", "EXECUTE"), ("SF166", "CLARIFY"), ("SF167", "EXECUTE")):
        assert BY_ID[gid]["applicability"] == oracle.APPLICABLE and BY_ID[gid]["expected"]["disposition"] == disposition, gid
    assert BY_ID["SF166"]["expected"]["code"] == "SMMLV_YEAR_AMBIGUOUS"  # U1: 2023 is outside the catalogue (the control)
    assert BY_ID["SF168"]["applicability"] == "NO_MEDIBLE(SMMLV_YEAR_NOT_SINGLE)" and SYN["SF168"].get("invalid_persisted_state")
    salary = gold.checker().SMMLV_COP_BY_YEAR[2025]
    base = BY_ID["SF165"]["inputs"]["base"]["conditions"]
    bound = next(c for c in base if c["dimension"] == "VALUE")
    assert bound["value"][1] == f"amount_cop={500 * salary}"
    assert bound["provenance"]["unit"] == "SMMLV" and bound["provenance"]["source_amount"] == "500" and bound["provenance"]["year"] == 2025
    placed = next(o["condition"] for o in BY_ID["SF167"]["inputs"]["patch"]["ops"] if o["condition"])
    assert placed["value"][1] == f"amount_cop={800 * salary}" and placed["provenance"]["unit_source"] == "PREVIOUS_ACCEPTED_VALUE"
    assert (placed["provenance"]["unit"], placed["provenance"]["source_amount"], placed["provenance"]["year"]) == ("SMMLV", "800", 2025)
    assert all(c["provenance"]["source_amount"] is None and c["provenance"]["year"] is None
               for c in BY_ID["SF091"]["inputs"]["base"]["conditions"])  # only a SMMLV amount carries them


def test_a_smmlv_case_whose_year_is_not_one_catalogued_year_is_not_measured():
    def two_years(label):
        label["prior"]["ir"]["conditions"] = [c for c in label["prior"]["ir"]["conditions"] if c["dimension"] != "TIME"]
        label["prior"]["ir"]["conditions"].append({"id": "t9", "dimension": "TIME", "operator": "between_dates",
                                                   "polarity": "POSITIVE", "level": None, "unit": None,
                                                   "value_hint": "2024-01-01..2025-12-31"})
        if label["resulting_ir"]:
            label["resulting_ir"]["conditions"] = [c for c in label["resulting_ir"]["conditions"] if c["dimension"] != "TIME"]
            label["resulting_ir"]["conditions"].append(dict(label["prior"]["ir"]["conditions"][-1]))
    assert rebuilt("SF165", both(two_years))["applicability"] == "NO_MEDIBLE(SMMLV_YEAR_NOT_SINGLE)"

    def out_of_catalogue(label):
        for ir in (label["prior"]["ir"], label["resulting_ir"]):
            for c in ir["conditions"]:
                if c["dimension"] == "TIME":
                    c["value_hint"] = "2023"
    assert rebuilt("SF165", both(out_of_catalogue))["applicability"] == "NO_MEDIBLE(SMMLV_YEAR_NOT_CATALOGUED)"


@FAST
@given(n=st.integers(min_value=1, max_value=999_999))
def test_value_amounts_are_canonical_across_units_and_spellings(n):
    millions = typing_.type_value("VALUE", "greater_or_equal", "POSITIVE", None, "MILLONES", f"{n} millones")
    pesos = typing_.type_value("VALUE", "greater_or_equal", "POSITIVE", None, "PESOS",
                               f"{n * 10**6:,}".replace(",", ".") + " pesos")
    thousands = typing_.type_value("VALUE", "greater_or_equal", "POSITIVE", None, "MILES", f"{n * 1000} mil pesos")
    assert millions == pesos == thousands == ("money@1", f"amount_cop={n * 10**6}", "comparator=>=")
    assert typing_.type_value("VALUE", "greater_or_equal", "POSITIVE", None, "MILLONES", f"mínimo {n} millones") == millions
    assert typing_.type_value("VALUE", "greater_than", "POSITIVE", None, "MILLONES", f"{n} millones") != millions


def test_value_comparator_side_and_negation_follow_the_typed_comparator():
    for operator, slot in (("greater_than", "VALUE/lower"), ("greater_or_equal", "VALUE/lower"),
                           ("less_than", "VALUE/upper"), ("less_or_equal", "VALUE/upper")):
        value = typing_.type_value("VALUE", operator, "POSITIVE", None, "MILLONES", "10 millones")
        assert typing_.slot_key("VALUE", value) == slot
    negated = typing_.type_value("VALUE", "greater_than", "NEGATIVE", None, "MILLONES", "10 millones")
    assert negated == typing_.type_value("VALUE", "less_or_equal", "POSITIVE", None, "MILLONES", "10 millones")


@pytest.mark.parametrize("unit,hint,motive", [
    ("SMMLV", "500 SMMLV", "SMMLV_YEAR_NOT_SINGLE"), (None, "400", "VALUE_UNIT_NOT_PESO_SCALE"),
    ("MILLONES", "2.5 millones", "VALUE_NUMBER_AMBIGUOUS"), ("MILLONES", "muchos millones", "VALUE_NUMBER_COUNT"),
    ("MILLONES", "de 50 a 100 millones", "VALUE_NUMBER_COUNT"), ("MILLONES", "50 mil millones", "VALUE_UNIT_WORD_DISAGREES"),
    ("MILLONES", "50 billones", "VALUE_UNIT_WORD_DISAGREES"), ("PESOS", "0,5 pesos", "VALUE_NOT_WHOLE_PESOS"),
])
def test_a_value_that_would_be_guessed_is_untypeable(unit, hint, motive):
    with pytest.raises(typing_.Untypeable) as error:
        typing_.type_value("VALUE", "greater_than", "POSITIVE", None, unit, hint)
    assert error.value.motive == motive
    assert typing_.type_value("VALUE", "greater_than", "POSITIVE", None, "MILLONES", "1,5 millones")[1] == "amount_cop=1500000"


@FAST
@given(year=st.integers(min_value=1990, max_value=2100), month=st.integers(min_value=1, max_value=12))
def test_time_spellings_of_the_same_interval_give_the_same_value(year, month):
    names = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre",
             "noviembre", "diciembre"]
    iso = typing_.type_value("TIME", "during_month", "POSITIVE", None, None, f"{year}-{month:02d}")
    assert iso == typing_.type_value("TIME", "during_month", "POSITIVE", None, None, f"{names[month - 1]} de {year}")
    assert iso == typing_.type_value("TIME", "during_month", "POSITIVE", None, None, f"{names[month - 1].upper()} DE {year}")
    last = (date(year + (month == 12), month % 12 + 1, 1) - timedelta(days=1)).isoformat()
    assert iso == typing_.type_value("TIME", "between_dates", "POSITIVE", None, None, f"{year}-{month:02d}-01..{last}")
    assert typing_.type_value("TIME", "during_year", "POSITIVE", None, None, str(year)) == typing_.type_value(
        "TIME", "between_dates", "POSITIVE", None, None, f"{year}-01-01..{year}-12-31")
    assert typing_.type_value("TIME", "during_year", "NEGATIVE", None, None, str(year)) != typing_.type_value(
        "TIME", "during_year", "POSITIVE", None, None, str(year))


@pytest.mark.parametrize("operator,hint", [
    ("relative_period", "este año"), ("relative_period", "este año (2026-01-01..)"), ("during_year", "20x5"), ("during_month", "marzo"),
    ("during_month", "2025-13"), ("between_dates", "2025-02-30..2025-03-01"), ("between_dates", ".."),
    ("between_dates", "2025-05-01..2025-04-01"), ("before", "2025"),
])
def test_a_time_hint_that_is_not_an_interval_is_untypeable(operator, hint):
    with pytest.raises(typing_.Untypeable):
        typing_.type_value("TIME", operator, "POSITIVE", None, None, hint)


def test_open_ended_ranges_are_half_open_intervals():
    assert typing_.type_value("TIME", "between_dates", "POSITIVE", None, None, "2025-03-01..")[1:3] == (
        "start=2025-03-01", "end_exclusive=!")
    assert typing_.type_value("TIME", "between_dates", "POSITIVE", None, None, "..2025-03-31")[1:3] == (
        "start=!", "end_exclusive=2025-04-01")


def test_state_must_be_a_closed_class_and_order_cardinality_are_enumerations():
    with pytest.raises(typing_.Untypeable):
        typing_.type_value("STATE", "is", "POSITIVE", None, None, "abierto")
    assert typing_.type_value("STATE", "is", "NEGATIVE", None, None, "awarded")[-1] == "excluded=1"
    assert typing_.tcond("ORDER", "value_desc", "POSITIVE", None, None, None)["value"] == [
        "order@1", "key=value", "direction=desc"]
    with pytest.raises(typing_.Untypeable):
        typing_.tcond("ORDER", "sideways", "POSITIVE", None, None, None)
    capped = typing_.tcond("CARDINALITY", "top_n", "POSITIVE", None, None, None, n_requested=250)["value"]
    assert capped == ["cardinality@2", "n=100", "basis=figure", "n_requested=250"]
    assert typing_.tcond("CARDINALITY", "top_n", "POSITIVE", None, None, None, n_requested="1")["value"][2] == \
        "basis=singular"
    with pytest.raises(typing_.Untypeable):
        typing_.tcond("CARDINALITY", "top_n", "POSITIVE", None, None, None, n_requested="muchos")


def test_the_year_source_of_a_time_op_is_the_golds_and_reaches_the_typed_patch():
    seen = collections.Counter()
    for case in APPLICABLE:
        if case["expected"]["effective_scope"] != "COMPOSED":
            continue
        golds = sorted(str((o["inherited"] or {}).get("year_source")) for o in SYN[case["gold_id"]]["label_A"]["ops"]
                       if o["op"] == "SET" and o["slot"] == "TIME")
        typed = sorted(str(o["condition"]["provenance"]["year_source"]) for o in case["inputs"]["patch"]["ops"]
                       if o["condition"] is not None and o["condition"]["dimension"] == "TIME")
        assert golds == typed, case["gold_id"]
        seen.update(typed)
    assert {"CLOCK_INFERRED", "PREVIOUS_ACCEPTED_TIME", "STATED"} <= set(seen)


def test_a_universe_op_of_a_turn_that_does_not_execute_is_typed_from_its_hint_and_never_guessed():
    def with_hint(hint):
        def change(label):
            label.update(disposition_reason="UNIT_AMBIGUOUS")  # a host-rule code that needs no fact the Gold lacks
            label["ops"] = [op for op in label["ops"] if op["op"] == "SET_UNIVERSE"]  # no STATE: H8 reads nothing here
            for op in label["ops"]:
                op["value_hint"] = hint
        return both(change)
    typed = rebuilt("SF079", with_hint("CONTRACTS"))
    redirect = next(o for o in typed["inputs"]["patch"]["ops"] if o["op"] == "REPLACE_UNIVERSE")
    assert typed["applicability"] == oracle.APPLICABLE and redirect["universe"] == {"subject": "contracts", "universe_kind": "records"}
    opportunities = rebuilt("SF079", with_hint("PROCESSES (oportunidades)"))
    assert next(o for o in opportunities["inputs"]["patch"]["ops"] if o["op"] == "REPLACE_UNIVERSE")["universe"] == {
        "subject": "processes", "universe_kind": "opportunities"}
    assert rebuilt("SF079", with_hint("cualquier cosa"))["applicability"] == "NO_MEDIBLE(UNIVERSE_TARGET)"
    # an executed turn: the typed result must name the same universe the hint names
    drop_state = lambda L: L.update(ops=[o for o in L["ops"] if o["op"] == "SET_UNIVERSE"])  # noqa: E731
    assert BY_ID["SF078"]["applicability"].startswith("NO_MEDIBLE(HOST_FACT")  # SF078 changes the universe with a state
    only_universe = rebuilt("SF078", both(lambda L: (drop_state(L), L["resulting_ir"]["conditions"].pop())))
    assert only_universe["applicability"] == oracle.APPLICABLE
    wrong = rebuilt("SF078", both(lambda L: (drop_state(L), L["resulting_ir"]["conditions"].pop(),
                                             [o.update(value_hint="CONTRACTS") for o in L["ops"]])))
    assert wrong["applicability"] == "NO_MEDIBLE(UNIVERSE_TARGET)"
    kind = rebuilt("SF078", both(lambda L: (drop_state(L), L["resulting_ir"]["conditions"].pop(),
                                            [o.update(value_hint="PROCESSES (oportunidades)") for o in L["ops"]])))
    assert kind["applicability"] == "NO_MEDIBLE(UNIVERSE_TARGET_KIND)"


def test_a_clear_names_the_whole_dimension_unless_the_gold_gives_it_a_polarity():
    def op(kind, slot, polarity):
        return {"op": kind, "slot": slot, "polarity": polarity, "operator": None, "value_hint": None, "level": None,
                "unresolved": False, "depends_on_prior_value": False, "refers_to_result_set": False,
                "inherited": {"unit_source": None, "year_source": None}}

    def slot(kind, name, polarity):
        label = {"resulting_ir": None, "ops": [op(kind, name, polarity)]}
        return typing_.typed_ops(label)[0]["slot"]
    assert slot("CLEAR", "TOPIC", None) == "TOPIC"
    assert slot("CLEAR", "TOPIC", "NEGATIVE") == "TOPIC/NEGATIVE" and slot("CLEAR", "TOPIC", "POSITIVE") == "TOPIC/POSITIVE"
    assert slot("CLEAR", "VALUE", "POSITIVE") == "VALUE"  # VALUE is not polarized: the noise of the label is ignored
    assert slot("CLEAR", "ORDER", None) == "ORDER" and slot("KEEP", "TIME", "POSITIVE") == "TIME/POSITIVE"


def test_every_typed_value_of_every_applicable_case_is_a_canonical_closed_value_with_its_slot():
    seen = set()
    for case in APPLICABLE:
        conditions = list(case["inputs"]["base"]["conditions"])
        conditions += [op["condition"] for op in case["inputs"]["patch"]["ops"] if op["condition"]]
        conditions += case["expected"]["effective"]["conditions"] if case["expected"]["effective"] else []
        for c in conditions:
            value = tuple(c["value"])
            assert decode_value(value).encode() == value, (case["gold_id"], value)
            assert typing_.slot_key(c["dimension"], value) == c["slot"]
            seen.add(c["dimension"])
    assert seen >= {"TOPIC", "BUYER", "TIME", "VALUE", "ORDER", "CARDINALITY", "GEOGRAPHY"}


# ====================================================================================================================
# THE EXPECTATION COMES FROM THE GOLD
# ====================================================================================================================
def _gold_result_conditions(rec):
    ir = rec["label_A"]["resulting_ir"]
    return len(ir["conditions"]) + (1 if ir["order"] else 0) + (1 if ir["cardinality"] else 0)


def test_the_expected_effective_has_exactly_the_conditions_of_the_gold_result():
    for case in APPLICABLE:
        eff = case["expected"]["effective"]
        if eff is None:
            assert case["expected"]["disposition"] in ("CLARIFY", "REASK", "INFORM")
            continue
        assert len(eff["conditions"]) == _gold_result_conditions(SYN[case["gold_id"]]), case["gold_id"]
        assert eff["requests"] == sorted(map(list, [(r["subject"], r["universe_kind"]) for r in
                                                    oracle.reference_actuals([case])[case["gold_id"]]["effective"]["requests"]]))


def test_the_gold_projection_holds_meaning_only_identity_and_provenance_are_the_contracts():
    for case in APPLICABLE:
        eff = case["expected"]["effective"]
        if eff is None:
            continue
        assert set(eff) == {"requests", "logic", "conditions"}
        for c in eff["conditions"]:
            assert set(c) == {"dimension", "slot", "operator", "polarity", "level", "value", "unit", "order_direction_source"}


def test_expected_disposition_code_and_scope_are_the_golds():
    for case in APPLICABLE:
        rec, exp = SYN[case["gold_id"]], case["expected"]
        assert exp["disposition"] == rec["expected_disposition"] == rec["label_A"]["expected_disposition"]
        assert exp["code"] == rec["label_A"]["disposition_reason"] == rec["label_B"]["disposition_reason"]
        assert exp["scope"] == rec["m2a_scope"]


def _amount(condition) -> int:
    return int(next(i for i in condition["value"] if i.startswith("amount_cop=")).partition("=")[2])


def test_a8_1_the_replaced_bound_is_numerically_empty_or_degenerate_and_a_kept_one_never_is():
    """The reducer's A8.1 effect against the TYPED amounts (an independent numeric reading of the typing)."""
    replaced = kept = 0
    for case in APPLICABLE:
        patch, base = case["inputs"]["patch"], case["inputs"]["base"]
        sets = {op["slot"]: op["condition"] for op in patch["ops"] if op["op"] == "SET" and op["slot"].startswith("VALUE/")}
        clears = [op for op in patch["ops"] if op["op"] == "CLEAR" and op["slot"].startswith("VALUE")]
        if len(sets) != 1 or clears or case["expected"]["effective"] is None:
            continue
        (side, new), = sets.items()
        old = next((c for c in base["conditions"] if c["slot"] == ("VALUE/upper" if side == "VALUE/lower" else "VALUE/lower")), None)
        if old is None:
            continue
        lo, hi = (_amount(new), _amount(old)) if side == "VALUE/lower" else (_amount(old), _amount(new))
        if "VALUE_RANGE_REPLACED" in case["expected"]["effects"]:
            assert lo >= hi, case["gold_id"]
            replaced += 1
        else:
            assert lo < hi, case["gold_id"]
            kept += 1
    assert replaced >= 1 and kept >= 1


def test_every_input_has_the_same_shape_so_the_shape_never_says_what_the_scope_rule_concludes():
    shapes = {(frozenset(c["inputs"]), frozenset(c["inputs"]["patch"]), frozenset(c["inputs"]["p_facts"]),
               frozenset(c["inputs"]["context"]), frozenset(c["inputs"]["base"]), type(c["inputs"]["patch"]["ops"]))
              for c in APPLICABLE}
    assert len(shapes) == 1
    for c in APPLICABLE:
        assert c["inputs"]["patch"]["ops"] and all(set(op) == {"op", "slot", "condition", "universe", "origin"}
                                                   for op in c["inputs"]["patch"]["ops"])
    fresh = [c for c in APPLICABLE if c["expected"]["effective_scope"] == "FRESH_ACCEPTANCE"]
    assert fresh and all(any(op["op"] == "REPLACE_UNIVERSE" for op in c["inputs"]["patch"]["ops"]) for c in fresh)  # P read as the patch it would be
    delta = [c for c in APPLICABLE if c["expected"]["effective_scope"] == "COMPOSED"]
    assert {frozenset(op) for c in fresh for op in c["inputs"]["patch"]["ops"]} == \
        {frozenset(op) for c in delta for op in c["inputs"]["patch"]["ops"]}


# ---- §7.2 over the facts of P ------------------------------------------------------------------------------------------------
FACTS = {"anchor": "SEARCH", "pending": False, "continuation": False, "new": True, "frame": True, "admissible_alone": True}


@pytest.mark.parametrize("override,expected", [
    ({"continuation": True}, {"DELTA"}),                                    # (a) the marker wins
    ({"new": False, "frame": True, "admissible_alone": True}, {"DELTA"}),   # (b) A9.1 even if admissible alone
    ({}, {"NEW_SEARCH"}),                                                   # (c)
    ({"frame": False}, {"DELTA"}),                                          # (d) F22
    ({"admissible_alone": False}, {"UNDEFINED"}),                           # marco without admissible: not in §7.2
    ({"continuation": None}, {"DELTA", "NEW_SEARCH"}),
    ({"frame": None, "admissible_alone": None}, {"DELTA", "NEW_SEARCH", "UNDEFINED"}),
    ({"new": False, "frame": None, "admissible_alone": None, "continuation": None}, {"DELTA"}),
])
def test_the_scope_rule_is_three_valued_and_never_guesses(override, expected):
    assert oracle.outcomes({**FACTS, **override}) == expected


def test_a_case_is_classification_measurable_only_when_the_facts_leave_one_outcome_that_is_the_golds():
    for case in APPLICABLE:
        c, facts, kind = case["expected"]["classification"], case["inputs"]["p_facts"], \
            SYN[case["gold_id"]]["label_A"]["turn_kind"]
        assert set(c["possible"]) == oracle.outcomes(facts) and facts["pending"] is False
        assert c["measurable"] == (set(c["possible"]) == {kind})
        assert c["expected"] == (kind if c["measurable"] else None)
        assert (c["motive"] is None) == c["measurable"]
        if not c["measurable"]:
            assert c["motive"] == "NO_MEDIBLE(FACTS_OF_P_UNDETERMINED)" and len(c["possible"]) > 1  # never a single guess
    # the lone content condition (F22) is the unmeasurable kind: «frame» is the verb of request, which the Gold lacks
    assert all(c["inputs"]["p_facts"]["frame"] is None for c in APPLICABLE if not c["expected"]["classification"]["measurable"])


def test_p_facts_read_the_typed_ops_of_every_case_the_same_way():
    for case in APPLICABLE:
        facts, ops = case["inputs"]["p_facts"], case["inputs"]["patch"]["ops"]
        assert facts["own_conditions"] == sum(1 for op in ops if op["condition"])
        assert facts["universe_evidence"] == any(op["op"] == "REPLACE_UNIVERSE" for op in ops)


def test_scope_audit_accepts_the_gold_and_catches_each_wrong_scope():
    scopes = {c["gold_id"]: c["expected"]["scope"] for c in APPLICABLE if c["expected"]["classification"]["measurable"]}
    report = oracle.audit_classification(scopes, CASES)
    assert report["ok"] and report["no_medible_for_classification"]
    for gid in scopes:
        wrong = {**scopes, gid: "IN_NEW_SEARCH" if scopes[gid] == "IN" else "IN"}
        assert not oracle.audit_classification(wrong, CASES)["ok"], gid
    assert not oracle.audit_classification({k: v for k, v in scopes.items() if k != next(iter(scopes))}, CASES)["ok"]
    undetermined = report["no_medible_for_classification"][0]
    assert not oracle.audit_classification({**scopes, undetermined: "IN"}, CASES)["ok"]  # no verdict invented


# ====================================================================================================================
# THE ARTIFACT AND THE MANIFEST
# ====================================================================================================================
def test_the_committed_artifact_and_manifest_are_the_recomputation():
    artifact, manifest_path = ROOT / oracle.ARTIFACT, ROOT / oracle.MANIFEST
    text, manifest = oracle.build_artifacts(None, json.loads(manifest_path.read_text(encoding="utf-8")))
    assert artifact.read_text(encoding="utf-8") == text
    assert json.loads(manifest_path.read_text(encoding="utf-8")) == manifest
    assert hashlib.sha256(text.encode("utf-8")).hexdigest() == manifest["artifact"]["sha256"]
    assert manifest["gold_seal_sha256"] == gold.seal()["seal_sha256"] and manifest["seal_verified"] is True
    assert manifest["dependencies_authority"] == oracle.s19_section()
    assert manifest["dependencies_authority"]["sha256"] == gold.seal()["public_files"][gold.S19_KEY]["sha256"]
    assert "applicability_decisions" not in manifest  # U19: there is no other authority, no local override
    assert oracle.real_manifest_problems(manifest) == []
    assert oracle.main(["check"]) == 0


def test_the_manifest_declares_the_exclusions_the_transition_rules_and_the_limited_guarantees_by_name():
    declared = json.loads((ROOT / oracle.MANIFEST).read_text(encoding="utf-8"))["declarations"]
    assert declared == json.loads(json.dumps(oracle.declarations()))
    assert [r["id"] for r in declared["applicability_rules"]] == [
        *(r.id for r in applicability.STRUCTURAL_RULES), *(d["dep_id"] for d in gold.s19()["dependencies"])]
    ids = {g["id"] for g in declared["limited_guarantees"]}
    assert ids >= {"LG-BASES-WITHOUT-HOST-FIELDS", "LG-CLASSIFICATION-CERTIFIED-BY-8B", "GOLD-GAP-F16-ANY-VALUE",
                   "GOLD-GAP-H6-OVERFIRE", "LG-HOST-FACTS-NOT-IN-GOLD", "LG-LECTURA-WITHOUT-TYPED-VALUE",
                   "LG-TARGET-NOT-ADDRESSABLE", "LG-AUTHORITY-REFUSALS", "LG-F16-CONTAINED-VALUE", "LG-REGION-MEMBERS-SYNTHETIC",
                   "LG-H8-ONLY-AFTER-A-UNIVERSE-CHANGE",
                   "LG-HOST-RULES-NOT-DECIDED-HERE",
                   "GOLD-GAP-SET-CLEARS-WHOLE-DIMENSION", "GOLD-GAP-CLEAR-DIMENSION-ONLY-POSITIVE",
                   "GOLD-GAP-REPLACE-UNIVERSE-KEEPS-OLD-KIND", "LG-DATASET-VALUE", "LG-REQUEST-POLICIES", "LG-FRESH-ACCEPTANCE"}
    assert all(g["kind"] in ("ORACLE_BLIND_SPOT", "GOLD_GAP", "SCOPE") and g["what"] and g["closed_by"]
               for g in declared["limited_guarantees"])
    assert declared["effects_scope"] == {"reducer": sorted(oracle.REDUCER_EFFECTS), "echo": sorted(oracle.ECHO_EFFECTS)}
    for entry in declared["gold_projection_delegates_to_transition"]:  # every exclusion has its rule and its killing mutants
        assert entry["to"] and entry["rules"] and entry["killed_by"], entry["field"]


def test_the_gold_gaps_are_closed_by_the_named_transition_mutants():
    rules = {r["id"]: set(r["killed_by"]) for r in oracle.TRANSITION_RULES}
    killers = set().union(*rules.values())
    for guarantee in oracle.LIMITED_GUARANTEES:
        if guarantee["kind"] == "GOLD_GAP":
            named = {w for w in re.findall(r"killed by ([a-z0-9_]+)", guarantee["closed_by"])}
            assert named <= killers and (named or "Gold case" in guarantee["closed_by"]), guarantee["id"]


def test_the_artifact_is_synthetic_only_and_carries_no_text():
    blob = (ROOT / oracle.ARTIFACT).read_text(encoding="utf-8")
    rows = [json.loads(line) for line in blob.splitlines()]
    assert [r["gold_id"] for r in rows] == [c["gold_id"] for c in CASES]
    assert all(r["gold_id"].startswith("SF") and r["source"] == "SYNTHETIC" for r in rows)
    assert not re.search(r'"(followup_text|notes|text|prior_text|quote)"', blob)
    manifest = json.loads((ROOT / oracle.MANIFEST).read_text(encoding="utf-8"))
    assert all(re.fullmatch(r"R\d{4}", i) for ids in manifest["real"]["ids_by_applicability"].values() for i in ids)
    assert all(re.fullmatch(r"[0-9a-f]{64}", d) for d in manifest["real"]["case_sha256"].values())
    assert manifest["real"]["summary"]["cases"] == len(gold.seal()["slices"]["M2A_GOLD"]["REAL"]["ids"])
    assert "inputs" not in json.dumps(manifest["real"])


def test_a_change_of_expectation_changes_the_artifact_the_manifest_binds(monkeypatch):
    """The manifest binds the artifact by sha256: the oracle cannot be adapted silently."""
    changed = copy.deepcopy(SYN)
    changed["SF138"]["label_A"]["disposition_reason"] = changed["SF138"]["label_B"]["disposition_reason"] = "NO_ANCHOR"
    monkeypatch.setattr(gold, "gold_records", lambda real_dir, gold_dir=gold.GOLD_DIR: [("SYNTHETIC", r) for r in changed.values()])
    text, _ = oracle.build_artifacts(None, json.loads((ROOT / oracle.MANIFEST).read_text(encoding="utf-8")))
    bound = json.loads((ROOT / oracle.MANIFEST).read_text(encoding="utf-8"))["artifact"]["sha256"]
    assert hashlib.sha256(text.encode("utf-8")).hexdigest() != bound


def test_building_without_a_manifest_needs_the_private_directory():
    with pytest.raises(SystemExit):
        oracle.build_artifacts(None, None)


@pytest.fixture
def scratch_repo(tmp_path, monkeypatch):
    """The artifact paths of the CLI redirected to a scratch tree that holds a copy of the committed manifest."""
    (tmp_path / oracle.ORACLE_DIR).mkdir(parents=True)
    shutil.copy(ROOT / oracle.MANIFEST, tmp_path / oracle.MANIFEST)
    monkeypatch.setattr(oracle, "ROOT", tmp_path)
    return tmp_path


def test_the_cli_builds_checks_and_detects_drift_in_the_artifact_and_the_manifest(scratch_repo, capsys):
    assert oracle.main(["check"]) == 1  # no artifact yet
    assert oracle.main(["build"]) == 0 and oracle.main(["check"]) == 0
    assert (scratch_repo / oracle.ARTIFACT).read_text(encoding="utf-8") == (ROOT / oracle.ARTIFACT).read_text(encoding="utf-8")
    artifact = scratch_repo / oracle.ARTIFACT
    artifact.write_text(artifact.read_text(encoding="utf-8").replace("SF001", "SF00X", 1), encoding="utf-8")
    assert oracle.main(["check"]) == 1 and "artifact differs" in capsys.readouterr().out
    assert oracle.main(["build"]) == 0
    manifest = scratch_repo / oracle.MANIFEST
    stored = json.loads(manifest.read_text(encoding="utf-8"))
    stored["real"]["ids_by_applicability"]["APLICABLE_M2A3"].append("R9999")
    manifest.write_text(json.dumps(stored), encoding="utf-8")
    assert oracle.main(["check"]) == 1
    assert "REAL ids of the manifest differ from the seal" in capsys.readouterr().out


def test_the_manifest_cannot_drop_or_invent_real_ids_nor_their_digests():
    manifest = json.loads((ROOT / oracle.MANIFEST).read_text(encoding="utf-8"))
    assert oracle.real_manifest_problems(manifest) == []
    dropped = copy.deepcopy(manifest)
    dropped["real"]["ids_by_applicability"]["APLICABLE_M2A3"].pop()
    assert oracle.real_manifest_problems(dropped)
    no_digest = copy.deepcopy(manifest)
    no_digest["real"]["case_sha256"].pop(next(iter(no_digest["real"]["case_sha256"])))
    assert oracle.real_manifest_problems(no_digest)


def test_the_report_command_prints_the_counts(capsys):
    assert oracle.main(["report"]) == 0
    printed = json.loads(capsys.readouterr().out)
    counted = {}
    for c in CASES:
        counted[c["applicability"]] = counted.get(c["applicability"], 0) + 1
    assert printed["synthetic"]["by_applicability"] == dict(sorted(counted.items()))


# ====================================================================================================================
# THE GOLD AUDIT: the reference passes, the coverage is declared, every mutant is caught
# ====================================================================================================================
def run_audit(case, actual, **kwargs):
    return oracle.audit({case["gold_id"]: actual}, [case], **kwargs)


def test_the_gold_faithful_reference_passes_and_the_report_declares_the_reds():
    report = oracle.audit(REFERENCE, CASES)
    dependent = [c for c in CASES if c["applicability"].startswith("DEPENDIENTE(")]
    assert report["ok"] and report["expected"] == report["actual"] == len(APPLICABLE) and not report["mismatches"]
    assert [r["gold_id"] for r in report["declared_red"]] == [c["gold_id"] for c in dependent]
    assert {r["owner"] for r in report["declared_red"]} == {c["owners"][0] for c in dependent}
    assert len(report["no_medible"]) == sum(1 for c in CASES if c["applicability"].startswith("NO_MEDIBLE"))
    assert report["unverified"] == {} and oracle.audit(REFERENCE)["ok"]  # the default cases are the synthetic Gold


def test_the_audit_declares_its_coverage_and_is_not_complete_when_it_covers_only_part_of_the_seal():
    report = oracle.audit(REFERENCE, CASES)
    sealed = {s: len(v["ids"]) for s, v in gold.seal()["slices"]["M2A_GOLD"].items()}
    assert report["coverage"]["sealed_m2a_gold"] == dict(sorted(sealed.items()))
    assert report["coverage"]["loaded"] == {"SYNTHETIC": sealed["SYNTHETIC"]}
    assert report["coverage"]["applicable_expected"] == {"SYNTHETIC": len(APPLICABLE)}
    assert report["coverage"]["full_sources"] == ["SYNTHETIC"] and report["complete"] is False  # REAL was not audited
    dev = oracle.select_partition(CASES, "DEV")
    ids = {c["gold_id"] for c in dev if c["applicability"] == oracle.APPLICABLE}
    partial = oracle.audit({k: v for k, v in REFERENCE.items() if k in ids}, dev)
    assert partial["ok"] and not partial["complete"] and partial["coverage"]["partitions"] == {"DEV": len(dev)}
    assert not oracle.audit(REFERENCE, dev)["ok"]  # TEST ids are extra when only DEV is audited


def test_asking_for_real_with_no_real_case_loaded_fails():
    report = oracle.audit(REFERENCE, CASES, require_sources=("SYNTHETIC", "REAL"))
    assert not report["ok"] and report["source_problems"] == ["REAL: required but no case is loaded"]
    assert oracle.audit(REFERENCE, CASES, require_sources=())["ok"]


def test_an_effective_of_a_fresh_acceptance_that_is_not_supplied_is_declared_or_a_failure():
    case = next(c for c in APPLICABLE if c["expected"]["effective_scope"] == "FRESH_ACCEPTANCE")
    absent = {**REFERENCE[case["gold_id"]], "effective": None}
    report = run_audit(case, absent)
    assert report["ok"] and report["unverified"] == {"fresh_acceptance_effective": [case["gold_id"]]}
    assert not report["complete"]
    strict = run_audit(case, absent, require_fresh=True)
    assert not strict["ok"] and strict["mismatches"][0]["field"] == "effective"
    wrong = copy.deepcopy(REFERENCE[case["gold_id"]])
    wrong["effective"]["conditions"].pop()
    assert not run_audit(case, wrong)["ok"]


def test_effects_that_are_no_medible_are_declared_unverified_and_not_compared():
    case = rebuilt("SF091", lambda rec: rec["label_B"].update(effects=["VALUE_RANGE_REPLACED", "POLARITY_INVERTED"]))
    assert case["expected"]["effects_status"].startswith("NO_MEDIBLE")
    ref = oracle.reference_actuals([case])["SF091"]
    report = run_audit(case, {**ref, "effects": ["whatever"]})
    assert report["ok"] and report["unverified"] == {"effects_not_measurable": ["SF091"]} and not report["complete"]
    assert not run_audit(case, {**ref, "disposition": "CLARIFY"})["ok"]


def test_the_ids_are_exactly_the_applicable_ones():
    victim = APPLICABLE[3]["gold_id"]
    without = oracle.audit({k: v for k, v in REFERENCE.items() if k != victim}, CASES)
    assert not without["ok"] and without["missing"] == [victim]
    dependent = next(c["gold_id"] for c in CASES if c["applicability"].startswith("DEPENDIENTE"))
    extra = oracle.audit({**REFERENCE, dependent: REFERENCE[victim]}, CASES)
    assert not extra["ok"] and extra["extra"] == [dependent]  # a dependent case is never measured, not even by mistake
    assert oracle.audit({**REFERENCE, "NOPE": REFERENCE[victim]}, CASES)["extra"] == ["NOPE"]
    for gid in REFERENCE:
        assert not oracle.audit({k: v for k, v in REFERENCE.items() if k != gid}, CASES)["ok"]


# ---- the campaign of mutants (the Gold's meaning; the contract's mechanics are test_m2a_transition_oracle) ----------------------
def _is_carried(case, condition) -> bool:
    written = [op["slot"] for op in case["inputs"]["patch"]["ops"] if op["op"] in ("SET", "SET_ORDER", "CLEAR")]
    return any(b["slot"] == condition["slot"] and b["value"] == condition["value"] for b in case["inputs"]["base"]["conditions"]) \
        and not any(expect_mod._covered(w, condition["slot"]) for w in written)


def _flip_side(condition):
    swap = {">=": "<=", "<=": ">=", ">": "<", "<": ">"}
    op = {"greater_or_equal": "less_or_equal", "less_or_equal": "greater_or_equal", "greater_than": "less_than",
          "less_than": "greater_than"}
    value = list(condition["value"])
    value[2] = "comparator=" + swap[value[2].partition("=")[2]]
    condition.update(value=value, operator=op[condition["operator"]],
                     slot="VALUE/upper" if condition["slot"] == "VALUE/lower" else "VALUE/lower")


def _invert_polarity(condition):
    value = [("excluded=" + ("0" if i == "excluded=1" else "1")) if i.startswith("excluded=") else i
             for i in condition["value"]]
    pol = "POSITIVE" if condition["polarity"] == "NEGATIVE" else "NEGATIVE"
    condition.update(value=value, polarity=pol, slot=f"{condition['dimension']}/{pol}")


def _extra_condition(effective):
    t = typing_.tcond("TOPIC", "contains", "POSITIVE", None, None, "una condición que nadie pidió")
    return typing_.condition_dict(t, "zz", [r["request_id"] for r in effective["requests"]],
                                  typing_.origin("t1", "0" * 64, 99))


def _restore_slot(case, effective, slot):
    """The effective as if the op on ``slot`` had not been applied: its new condition out, the base's back."""
    rids = [r["request_id"] for r in effective["requests"]]
    patch_values = [op["condition"]["value"] for op in case["inputs"]["patch"]["ops"] if op["condition"] and op["slot"] == slot]
    kept = [c for c in effective["conditions"] if not expect_mod._covered(slot, c["slot"]) or c["value"] not in patch_values]
    for b in case["inputs"]["base"]["conditions"]:
        if expect_mod._covered(slot, b["slot"]) and not any(b["slot"] == c["slot"] and b["value"] == c["value"] for c in kept):
            kept.append({**copy.deepcopy(b), "request_ids": sorted(rids)})
    effective["conditions"] = kept


DISPOSITIONS = ("EXECUTE", "NEW_SEARCH", "CLARIFY", "REASK", "INFORM")


def mutants(case, ref):
    """Wrong outputs built from the expectation of one case: ``(operator, actual)``. A mutant whose semantic projection
    equals the expectation (an equivalent one) is never produced."""
    exp = case["expected"]
    out = []

    def add(name, actual_change=None, effective_change=None):
        actual = copy.deepcopy(ref)
        if effective_change is not None:
            effective_change(actual["effective"])
            try:
                if oracle.project_effective(actual["effective"]) == exp["effective"]:
                    return
            except ValueError:
                pass
        if actual_change is not None:
            actual_change(actual)
        out.append((name, actual))

    effective = ref["effective"]
    for i, c in enumerate(effective["conditions"] if effective else []):
        if _is_carried(case, c):
            add("drop_carried", effective_change=lambda e, i=i: e["conditions"].pop(i))
        add("drop_condition", effective_change=lambda e, i=i: e["conditions"].pop(i))
        add("duplicate_condition", effective_change=lambda e, i=i: e["conditions"].append(copy.deepcopy(e["conditions"][i])))
        if c["dimension"] == "VALUE":
            add("flip_value_side", effective_change=lambda e, i=i: _flip_side(e["conditions"][i]))
            add("change_value", effective_change=lambda e, i=i: e["conditions"][i].update(
                value=[e["conditions"][i]["value"][0], "amount_cop=1", e["conditions"][i]["value"][2]]))
            add("wrong_unit", effective_change=lambda e, i=i: e["conditions"][i]["provenance"].update(unit="BILLONES"))
        if c["dimension"] in typing_.POLARIZED:
            add("invert_polarity", effective_change=lambda e, i=i: _invert_polarity(e["conditions"][i]))
        if c["dimension"] == "TOPIC":
            add("change_value", effective_change=lambda e, i=i: e["conditions"][i].update(
                value=[e["conditions"][i]["value"][0], "phrase_folded=otro", *e["conditions"][i]["value"][2:]]))
        if c["dimension"] == "ORDER" and c["provenance"]["order_direction_source"]:
            flipped = "DEFAULTED" if c["provenance"]["order_direction_source"] == "STATED" else "STATED"
            add("wrong_direction_source", effective_change=lambda e, i=i, f=flipped:
                e["conditions"][i]["provenance"].update(order_direction_source=f))
    if effective:
        add("add_condition", effective_change=lambda e: e["conditions"].append(_extra_condition(e)))
        add("logic_present", effective_change=lambda e: e.update(logic={"operator": "OR", "condition_id": None, "children": []}))
        add("swap_universe", effective_change=lambda e: e["requests"][0].update(
            subject="contracts" if e["requests"][0]["subject"] == "processes" else "processes"))
        add("swap_universe_kind", effective_change=lambda e: e["requests"][0].update(
            universe_kind="opportunities" if e["requests"][0]["universe_kind"] == "records" else "records"))
        add("malformed_effective", effective_change=lambda e: e.pop("conditions"))
        if exp["effective_scope"] == "COMPOSED":
            add("effective_missing", actual_change=lambda a: a.update(effective=None))
            ops = [op for op in case["inputs"]["patch"]["ops"] if op["op"] in ("SET", "SET_ORDER", "CLEAR")]
            if len(ops) >= 2:  # partial application: one op applied, another not
                for op in ops:
                    add("partial_application", effective_change=lambda e, s=op["slot"]: _restore_slot(case, e, s))
    else:
        add("effective_on_non_execution", actual_change=lambda a: a.update(effective=copy.deepcopy(case["inputs"]["base"])))
    for other in DISPOSITIONS:
        if other != exp["disposition"]:
            add("wrong_disposition", actual_change=lambda a, o=other: a.update(disposition=o))
    add("wrong_code", actual_change=lambda a: a.update(code="NO_ANCHOR" if exp["code"] != "NO_ANCHOR" else "PENDING_UNANSWERED"))
    if exp["code"] is not None:
        add("code_missing", actual_change=lambda a: a.update(code=None))
    add("unknown_code", actual_change=lambda a: a.update(code="A_CODE_NOBODY_DECLARED"))
    for effect in exp["effects"]:
        add("omit_effect", actual_change=lambda a, e=effect: a.update(effects=[x for x in a["effects"] if x != e]))
    for effect in sorted(oracle.REDUCER_EFFECTS - set(exp["effects_gold"])):
        add("invent_effect", actual_change=lambda a, e=effect: a.update(effects=[*a["effects"], e]))
    add("invent_foreign_effect", actual_change=lambda a: a.update(effects=[*a["effects"], "NOT_AN_EFFECT"]))
    if "UNIVERSE_CHANGED" not in exp["effects_gold"]:
        add("invent_echo_effect", actual_change=lambda a: a.update(effects=[*a["effects"], "UNIVERSE_CHANGED"]))
    add("duplicate_effect", actual_change=lambda a: a.update(effects=[*a["effects"], *(a["effects"][:1] or ["CONTEXT_RESET"] * 2)]))
    add("effects_not_a_collection", actual_change=lambda a: a.update(effects="CONTEXT_RESET"))
    add("shape_missing_key", actual_change=lambda a: a.pop("effects"))
    add("shape_extra_key", actual_change=lambda a: a.update(extra=1))
    return out


MUTANT_OPERATORS = {
    "drop_carried", "drop_condition", "duplicate_condition", "flip_value_side", "change_value", "wrong_unit",
    "invert_polarity", "wrong_direction_source", "add_condition", "logic_present", "swap_universe", "swap_universe_kind",
    "malformed_effective", "effective_missing", "partial_application", "effective_on_non_execution",
    "wrong_disposition", "wrong_code", "code_missing", "unknown_code", "omit_effect", "invent_effect",
    "invent_foreign_effect", "invent_echo_effect", "duplicate_effect", "effects_not_a_collection", "shape_missing_key",
    "shape_extra_key"}


@functools.cache
def all_mutants():
    return [(case["gold_id"], name, actual) for case in APPLICABLE for name, actual in mutants(case, REFERENCE[case["gold_id"]])]


def id_mutants():
    """Completeness mutants: an applicable id omitted, and an extra id (a dependent case, or one that does not exist)."""
    omitted = [("omitted_id", {k: v for k, v in REFERENCE.items() if k != gid}) for gid in REFERENCE]
    dependent = [c["gold_id"] for c in CASES if c["applicability"].startswith("DEPENDIENTE")]
    victim = next(iter(REFERENCE))
    extra = [("extra_id", {**REFERENCE, gid: REFERENCE[victim]}) for gid in [*dependent, "SF999"]]
    return omitted + extra


def test_every_mutant_of_every_applicable_case_is_caught():
    survivors, caught = [], 0
    for gid, name, actual in all_mutants():
        if run_audit(BY_ID[gid], actual)["ok"]:
            survivors.append((gid, name))
        else:
            caught += 1
    for name, actuals in id_mutants():
        if oracle.audit(actuals, CASES)["ok"]:
            survivors.append((name, "ids"))
        else:
            caught += 1
    total = len(all_mutants()) + len(id_mutants())
    by_operator = {}
    for _, name, _ in all_mutants():
        by_operator[name] = by_operator.get(name, 0) + 1
    print(f"\nM2A-8a0 mutation of actuals (Gold oracle): {caught}/{total} caught; by operator {dict(sorted(by_operator.items()))}")
    assert survivors == [] and caught == total > 0
    assert len(id_mutants()) == len(APPLICABLE) + sum(1 for c in CASES if c["applicability"].startswith("DEPENDIENTE")) + 1


def test_every_mutation_operator_is_exercised_by_the_campaign():
    assert {name for _, name, _ in all_mutants()} == MUTANT_OPERATORS


def test_the_required_error_classes_hit_several_cases_each():
    for operator in ("drop_carried", "flip_value_side", "invert_polarity", "add_condition", "partial_application",
                     "wrong_disposition", "wrong_code", "omit_effect"):
        assert len({g for g, n, _ in all_mutants() if n == operator}) >= 2, operator


# ---- harmless variations are not errors; what is the contract's is not the Gold oracle's -------------------------------------
def _rename(effective):
    """Rename every condition and request id, and reverse both collections: the reducer chooses ids and order."""
    effective = copy.deepcopy(effective)
    rq = {r["request_id"]: f"q{n}" for n, r in enumerate(reversed(effective["requests"]), 1)}
    for r in effective["requests"]:
        r["request_id"] = rq[r["request_id"]]
    effective["requests"].reverse()
    for n, c in enumerate(effective["conditions"]):
        c["condition_id"] = f"renamed-{n}"
        c["request_ids"] = sorted(rq[i] for i in c["request_ids"])
    effective["conditions"].reverse()
    return effective


def test_order_ids_and_every_identity_or_provenance_change_leave_the_gold_verdict_alone():
    """The Gold oracle judges meaning: ids, order, origins, authorities, requests' ids and datasets are the contract's."""
    for case in APPLICABLE:
        ref = REFERENCE[case["gold_id"]]
        if ref["effective"] is None:
            continue
        tweaked = copy.deepcopy(ref["effective"])
        for c in tweaked["conditions"]:
            c.update(authority="reducer@9", policies=["x"], receipt_rules=["unit.sibling"],
                     origin=typing_.origin("t7", "1" * 64, 5))
        for r in tweaked["requests"]:
            r.update(dataset="elsewhere", universe_origin=typing_.origin("t7", "1" * 64, 5), scope_id="other")
        tweaked.update(base_trace_id="elsewhere", root_trace_id="root", policies=[])
        for variant in (_rename(ref["effective"]), tweaked):
            assert run_audit(case, {**ref, "effective": variant})["ok"], case["gold_id"]
        assert run_audit(case, {**ref, "effects": tuple(reversed(ref["effects"]))})["ok"]
        assert run_audit(case, {**ref, "effects": set(ref["effects"])})["ok"]


def test_an_echo_level_effect_the_gold_lists_is_allowed_but_never_required():
    case = BY_ID["SF021"]  # the Gold lists TOPN_CAPPED (echo-level, M2A-7)
    assert "TOPN_CAPPED" in case["expected"]["effects_gold"] and "TOPN_CAPPED" not in case["expected"]["effects"]
    assert run_audit(case, {**REFERENCE["SF021"], "effects": ["TOPN_CAPPED"]})["ok"]
    assert run_audit(case, REFERENCE["SF021"])["ok"]


@FAST
@given(seed=st.integers(min_value=0, max_value=10**6), index=st.integers(min_value=0, max_value=len(APPLICABLE) - 1))
def test_the_projection_is_invariant_under_any_reordering_and_renaming(seed, index):
    import random
    case = APPLICABLE[index]
    effective = REFERENCE[case["gold_id"]]["effective"]
    if effective is None:
        return
    shuffled = copy.deepcopy(effective)
    random.Random(seed).shuffle(shuffled["conditions"])
    random.Random(seed).shuffle(shuffled["requests"])
    for n, c in enumerate(shuffled["conditions"]):
        c["condition_id"] = f"id-{seed}-{n}"
        random.Random(seed).shuffle(c["request_ids"])
    assert oracle.project_effective(shuffled) == case["expected"]["effective"]


# ---- the comparison is not vacuous: neutralise one aspect and exactly its mutants get through ----------------------------------
def _map_conditions(projection, fn):
    if projection is None or "conditions" not in projection:
        return projection
    return {**projection, "conditions": sorted((fn(dict(c)) for c in projection["conditions"]), key=gold.canon)}


def _no_polarity(c):
    return {**c, "polarity": None, "slot": c["slot"].replace("/NEGATIVE", "/POSITIVE"),
            "value": [i.replace("excluded=1", "excluded=0") for i in c["value"]]}


def _no_value_side(c):
    return {**c, "operator": None, "slot": "VALUE", "value": c["value"][:2]} if c["dimension"] == "VALUE" else c


def _as_a_set(p):
    return p if p is None or "conditions" not in p else {**p, "conditions": sorted(
        {gold.canon(c): c for c in p["conditions"]}.values(), key=gold.canon)}


COMPARISON_ASPECTS = {  # aspect -> (a weakened projection, the mutants that depend on exactly that aspect)
    "conditions": (lambda p: p if p is None or "conditions" not in p else {**p, "conditions": []},
                   {"drop_carried", "drop_condition", "add_condition", "flip_value_side", "invert_polarity", "change_value",
                    "partial_application", "wrong_unit", "wrong_direction_source", "duplicate_condition",
                    "effective_missing"}),
    "requests": (lambda p: p if p is None or "requests" not in p else {**p, "requests": []},
                 {"swap_universe", "swap_universe_kind"}),
    "logic": (lambda p: p if p is None or "logic" not in p else {**p, "logic": None}, {"logic_present"}),
    "polarity": (lambda p: _map_conditions(p, _no_polarity), {"invert_polarity"}),
    "value_side": (lambda p: _map_conditions(p, _no_value_side), {"flip_value_side"}),
    "value": (lambda p: _map_conditions(p, lambda c: {**c, "value": c["value"][:1]}), {"change_value", "partial_application"}),
    "unit": (lambda p: _map_conditions(p, lambda c: {**c, "unit": None}), {"wrong_unit"}),
    "direction_source": (lambda p: _map_conditions(p, lambda c: {**c, "order_direction_source": None}), {"wrong_direction_source"}),
    "multiplicity": (_as_a_set, {"duplicate_condition"}),
}


@pytest.mark.parametrize("aspect", sorted(COMPARISON_ASPECTS))
def test_neutralising_an_aspect_of_the_comparison_lets_exactly_its_mutants_through(aspect, monkeypatch):
    weakened, operators = COMPARISON_ASPECTS[aspect]
    monkeypatch.setattr(audit_mod, "effective_equal", lambda have, want: weakened(have) == weakened(want))
    survived = {name for gid, name, actual in all_mutants() if run_audit(BY_ID[gid], actual)["ok"]}
    assert survived, f"{aspect}: no mutant depends on it, so the check is vacuous"
    assert survived <= operators, (aspect, sorted(survived - operators))


def test_with_the_comparison_intact_none_of_those_mutants_gets_through():
    assert all(not run_audit(BY_ID[gid], actual)["ok"] for gid, name, actual in all_mutants())


# ---- the disposition, code and effects comparisons are exact ---------------------------------------------------------------------
def test_disposition_code_and_effects_are_compared_exactly():
    case, ref = BY_ID["SF091"], REFERENCE["SF091"]
    assert case["expected"]["effects"] == ["VALUE_RANGE_REPLACED"]
    for field, wrong in (("disposition", "execute"), ("code", ""), ("effects", [])):
        assert not run_audit(case, {**ref, field: wrong})["ok"]
    report = run_audit(case, {**ref, "effects": []})
    assert report["mismatches"][0]["field"] == "effects" and report["mismatches"][0]["expected"] == ["VALUE_RANGE_REPLACED"]


def test_an_effective_for_a_turn_that_does_not_execute_is_a_failure():
    case = BY_ID["SF113"]  # INFORM CLEAR_ON_EMPTY_SLOT: the anchor stays, there is no effective
    assert case["expected"]["effective"] is None and case["expected"]["code"] == "CLEAR_ON_EMPTY_SLOT"
    assert run_audit(case, REFERENCE["SF113"])["ok"]
    assert not run_audit(case, {**REFERENCE["SF113"], "effective": case["inputs"]["base"]})["ok"]


def test_a_report_about_a_real_case_carries_digests_not_content():
    case = copy.deepcopy(BY_ID["SF021"])
    case["source"] = "REAL"
    item = oracle.audit({"SF021": {**REFERENCE["SF021"], "code": "NO_ANCHOR"}}, [case])["mismatches"][0]
    assert set(item) == {"gold_id", "field", "expected_sha256", "actual_sha256"}
    assert "NO_ANCHOR" not in json.dumps(item)


# ---- the independent review's regressions (Gold side) ------------------------------------------------------------------------------
def test_h_rule_overfiring_reducers_die_on_the_measurable_controls():
    """h5_overfire: «con X» after an ACTOR anchor answered with a clarification although the Gold executes (SF164)."""
    control = BY_ID["SF164"]
    assert control["applicability"] == oracle.APPLICABLE
    overfire = {**REFERENCE["SF164"], "disposition": "CLARIFY", "code": "BUYER_OR_PLACE_AFTER_ACTOR", "effective": None}
    report = run_audit(control, overfire)
    assert not report["ok"] and {m["field"] for m in report["mismatches"]} >= {"disposition", "code", "effective"}


def test_h6_has_no_executed_control_in_this_gold_and_says_so():
    controls = [c for c in APPLICABLE if c["expected"]["disposition"] == "EXECUTE"
                and any(op["op"] == "SET" and op["slot"].startswith("GEOGRAPHY") for op in c["inputs"]["patch"]["ops"])
                and any(b["dimension"] == "BUYER" for b in c["inputs"]["base"]["conditions"])]
    assert controls == []  # a Gold gap, not hidden: no EXECUTE case discriminates an over-firing H6
    h6 = [c for c in CASES if c["reason"] == gold.host_rules()["H6"][1]]
    assert h6 and all(c["applicability"] == "NO_MEDIBLE(HOST_FACT_NOT_IN_GOLD:CARRIED_BUYER_TERRITORY)" for c in h6)


def test_the_swap_of_labels_leaves_every_case_unchanged_on_the_synthetic_gold():
    test_no_labeler_is_privileged_and_the_order_of_the_ops_never_matters()


# ====================================================================================================================
# THE REAL SLICE (private; skipped without the directory)
# ====================================================================================================================
@needs_real
def test_real_slice_is_typed_audited_and_its_digests_match_the_manifest():
    cases = oracle.load_cases(REAL_DIR)
    real = [c for c in cases if c["source"] == "REAL"]
    totals = gold.seal()["totals"]["M2A_GOLD"]
    assert len(cases) == totals["TOTAL"] and len(real) == totals["REAL"]
    manifest = json.loads((ROOT / oracle.MANIFEST).read_text(encoding="utf-8"))
    assert {c["gold_id"]: gold.digest(c) for c in real} == manifest["real"]["case_sha256"]
    assert oracle._real_section(real) == manifest["real"]
    assert oracle.main(["check", "--real-dir", str(REAL_DIR)]) == 0
    reference = oracle.reference_actuals(cases)
    report = oracle.audit(reference, cases, require_sources=("SYNTHETIC", "REAL"))
    assert report["ok"] and report["complete"] and report["coverage"]["full_sources"] == ["REAL", "SYNTHETIC"]
    assert oracle.audit_transitions(reference, cases)["ok"]
    scopes = {c["gold_id"]: c["expected"]["scope"] for c in cases
              if c["applicability"] == oracle.APPLICABLE and c["expected"]["classification"]["measurable"]}
    assert oracle.audit_classification(scopes, cases)["ok"]
    assert sum(len(ids) for ids in manifest["real"]["ids_by_applicability"].values()) == totals["REAL"]


@needs_real
def test_the_synthetic_audit_of_real_actuals_declares_what_it_did_not_cover():
    cases = oracle.load_cases(REAL_DIR)
    reference = oracle.reference_actuals(cases)
    partial = oracle.audit(reference, [c for c in cases if c["source"] == "SYNTHETIC"])
    assert not partial["ok"] and partial["extra"]  # REAL ids with no REAL case loaded
    only_synthetic = oracle.audit({k: v for k, v in reference.items() if not k.startswith("R")}, cases)
    assert not only_synthetic["ok"] and only_synthetic["missing"]  # REAL cases loaded but not answered


@needs_real
def test_the_real_states_over_an_unchanged_universe_are_covered_by_the_declared_h8_assumption():
    ids = _states_over_an_unchanged_universe([c for c in oracle.load_cases(REAL_DIR) if c["source"] == "REAL"])
    assert ids and all(i.startswith("R") for i in ids)


@needs_real
def test_the_real_smmlv_cases_follow_section_19_too():
    cases = {c["gold_id"]: c for c in oracle.load_cases(REAL_DIR)}
    assert cases["R0301"]["applicability"] == oracle.APPLICABLE and cases["R0301"]["expected"]["disposition"] == "EXECUTE"
    assert cases["R0303"]["applicability"] == oracle.APPLICABLE and cases["R0303"]["expected"]["code"] == "SMMLV_YEAR_AMBIGUOUS"
    assert cases["R0303"]["s19"] is None and "DEP-SMMLV-READ" in cases["R0301"]["s19"]["dependencies"]
    assert cases["R0094"]["applicability"] == H8_FACT  # a change of universe with a state


@needs_real
def test_swapping_the_labels_leaves_every_real_case_unchanged():
    partition, s19 = gold.partition_of(gold.seal()), gold.s19(REAL_DIR)
    for source, record in gold.gold_records(REAL_DIR):
        if source != "REAL":
            continue
        swapped = copy.deepcopy(record)
        swapped["label_A"], swapped["label_B"] = swapped["label_B"], swapped["label_A"]
        original = oracle.build_case(source, copy.deepcopy(record), partition[record["gold_id"]], s19)
        assert oracle.build_case(source, swapped, partition[record["gold_id"]], s19) == original, record["gold_id"]


@needs_real
def test_labels_that_disagree_on_the_prior_instant_or_state_are_no_medible_in_real():
    cases = {c["gold_id"]: c for c in oracle.load_cases(REAL_DIR)}
    disagree = [g for g, c in cases.items() if c["applicability"] == "NO_MEDIBLE(PRIOR_AB_DISAGREE)"]
    assert disagree and all(g.startswith("R") for g in disagree)


@needs_real
def test_every_mutant_of_every_applicable_real_case_is_caught():
    cases = [c for c in oracle.load_cases(REAL_DIR) if c["source"] == "REAL"]
    reference = oracle.reference_actuals(cases)
    total = caught = 0
    for case in (c for c in cases if c["applicability"] == oracle.APPLICABLE):
        for _, actual in mutants(case, reference[case["gold_id"]]):
            total += 1
            caught += not run_audit(case, actual)["ok"]
    print(f"\nM2A-8a0 mutation of REAL actuals (Gold oracle): {caught}/{total} caught")
    assert total and caught == total


@needs_real
def test_no_real_utterance_actor_or_number_reaches_the_public_files():
    spec = importlib.util.spec_from_file_location("privacy_scan_public_tree", SCRIPTS / "privacy_scan_public_tree.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with tempfile.TemporaryDirectory() as tmp:
        for path in [ROOT / oracle.ARTIFACT, ROOT / oracle.MANIFEST, *TEST_FILES, *ORACLE_FILES]:
            shutil.copy(path, Path(tmp) / path.name)
        assert module.scan(REAL_DIR, Path(tmp))["blocking"] == {}
