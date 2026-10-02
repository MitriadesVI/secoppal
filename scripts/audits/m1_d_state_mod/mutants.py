"""Mutation gate of M1-D-STATE/MOD: apply a mutant, run the focal tests, restore, report N/N killed.

Usage: ``PYTHONPATH=. python scripts/audits/m1_d_state_mod/mutants.py``.  Every mutant replaces ONE exact
occurrence of ``old`` by ``new`` in one file (aborting if ``old`` is not found exactly once), runs the
focal tests and requires a non-zero exit; the file is restored byte for byte in ``finally``.
"""
from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FOCAL = ["tests/test_host_authority_state.py", "tests/test_host_authority_modality.py"]
STATE = "app/core/host_authorities/state.py"
CATALOG = "app/core/host_authorities/state_catalog.py"
MODALITY = "app/core/host_authorities/modality.py"

MUTANTS = [
    # ---- STATE
    ("S01 excluded ignores the polarity", STATE, "excluded = evidence.polarity is Polarity.NEGATIVE\n        if excluded and any",
     "excluded = False\n        if excluded and any"),
    ("S02 negation inside a POSITIVE cite is executed", STATE, "if negations and evidence.polarity is Polarity.POSITIVE:",
     "if False:"),
    ("S03 only the first token of the cite is read (STATE-036)", STATE, "tokens = window_tokens(window.quote)",
     "tokens = window_tokens(window.quote)[:1]"),
    ("S04 S3 accepts any gap", STATE, 'window.gaps == (("o",),)', "True"),
    ("S05 S3 loses its ADR-100 receipt", STATE, "policies=(FUNNEL_POLICY,))", "policies=())"),
    ("S06 the operator is not compared with the evidence (C4)", STATE, "if reading.operator != evidence.operator:",
     "if False:"),
    ("S07 D6 withdrawal removed", STATE, "if reading.names_event and _anchors_a_date(window.right):", "if False:"),
    ("S08 literals of every dataset for every dataset", CATALOG, "return next((columns for name, columns in self.literals if name == logical), None)",
     "return self.literals[0][1]"),
    ("S09 «adjudicado» points at another literal", CATALOG, '_reading("awarded", "is", processes=_process("adjudicado"))',
     '_reading("awarded", "is", processes=_process("evaluacion"))'),
    ("S10 a two-column state is negated anyway", STATE, "if excluded and any(len(columns) > 1 for columns in literals.values()):",
     "if False:"),
    ("S11 the unrepresentable states are not known", CATALOG, 'KNOWN_UNREPRESENTABLE = frozenset({\n    ("liquidado",),',
     'KNOWN_UNREPRESENTABLE = frozenset({\n    ("__never__",),'),
    ("S12 «cerrado» in contratos points at «terminado»", CATALOG, 'contracts=_contract("cerrado")',
     'contracts=_contract("terminado")'),
    ("S13 literals keyed by the logical name, not the context id", STATE,
     "literals = {name: dict(columns) for name, logical in datasets.items()", "literals = {logical: dict(columns) for name, logical in datasets.items()"),
    ("S14 no inflection by lemma", CATALOG, "    return tuple(dict.fromkeys(found))", "    return (token,)"),
    ("S15 the negation scaffold is not counted", CATALOG, "return tokens[index:], sum(token in NEGATION_TOKENS for token in tokens[:index])",
     "return tokens[index:], 0"),
    ("S16 a negative state carries no policy", STATE, "policies = (*policies, *sorted({EXCLUSION_POLICIES[datasets[name]] for name in literals}))",
     "policies = policies"),
    ("S17 S3 collapses a different pair to the funnel", STATE, 'and _is_funnel_pair(*(window_tokens(segment) for segment in window.segments))):',
     "):"),
    ("S18 the open funnel loses its opening column", CATALOG, '("estado_de_apertura_del_proceso", (_PROCESS_LEXICON["abierto"],)),\n', ""),
    ("S19 deictics are not the open family (C3)", CATALOG, "    if phrase in DEICTICS:\n        return OPEN\n", ""),
    ("S20 a deictic after any state is accepted", CATALOG, "if head is OPEN or head is LIVE:", "if head is not None:"),
    ("S21 S3 accepts every pair", STATE, "return (one is catalog.OPEN and (other is catalog.OPEN or other is catalog.PUBLISHED)) or (\n        one is catalog.PUBLISHED and other is catalog.OPEN)",
     "return True"),
    ("S22 «en curso» is not in the open family (C4)", CATALOG, '("en", "curso"): LIVE,  # C4: open family', ""),
    # ---- MODALITY
    ("M01 colloquial «licitación» becomes a legal filter (O6)", MODALITY, "if match.strength is MatchStrength.COLLOQUIAL or match.assumption_code:",
     "if False:"),
    ("M02 polarity guard removed", MODALITY, "if negated and evidence.polarity is Polarity.POSITIVE:", "if False:"),
    ("M03 residue allowed", MODALITY, "if any(token not in SCAFFOLD_TOKENS for token in residue):", "if False:"),
    ("M04 unaccredited families executed", MODALITY, "if accredited is None or frozenset(ids) != accredited[1]:",
     "if False:"),
    ("M05 excluded ignores the polarity", MODALITY, "excluded = evidence.polarity is Polarity.NEGATIVE\n",
     "excluded = False\n"),
    ("M06 strength gate removed", MODALITY, "if match.strength not in _EXECUTABLE:", "if False:"),
    ("M07 is over a family is executed", MODALITY, 'if evidence.operator == "is":\n                return _reject("OPERATOR_EVIDENCE_MISMATCH:MODALITY"',
     'if False:\n                return _reject("OPERATOR_EVIDENCE_MISMATCH:MODALITY"'),
    ("M08 two matches in one cite keep the first", MODALITY, "if len(matches) > 1:", "if False:"),
    ("M09 resolver issues ignored", MODALITY, "if resolution.issues:", "if False:"),
    ("M10 an unbound negation is accepted", MODALITY, "if negated and not (resolution.excludes and not resolution.includes):",
     "if False:"),
    ("M11 receipt rule not sanitised", MODALITY, 're.sub(r"[^A-Za-z0-9_.:@-]", "_", rule_id)[:64]', "rule_id"),
    ("M12 negative modality without its policy", MODALITY, "policies = (*policies, EXCLUSION_POLICY)", "policies = policies"),
    ("M13 a colloquial negation is ignored", MODALITY, "if excluded:\n                return _reject(\"NEGATION_UNSUPPORTED:MODALITY\"",
     "if False:\n                return _reject(\"NEGATION_UNSUPPORTED:MODALITY\""),
    # ---- the authority owns A2 STATE-015/016/017 (estado_families stays byte-identical to main: legacy freeze)
    ("E01 the authority does not fold accents/case of the cite", STATE, "tokens = window_tokens(window.quote)\n        phrase, negations",
     "tokens = tuple(window.quote.split())\n        phrase, negations"),
    ("E02 «adjudicado» widens to the evaluation family", CATALOG,
     '("adjudicado",): _reading("awarded", "is", processes=_process("adjudicado")),',
     '("adjudicado",): _reading("awarded", "is", processes=(("estado_del_procedimiento", tuple(\n'
     '        ESTADO_FAMILIES["en_evaluacion"][PROCESSES]["estado_del_procedimiento"])),)),'),
    ("E03 the catalog key of «en evaluación» is not folded", CATALOG, '("en", "evaluacion"): _reading("in_evaluation"',
     '("en", "evaluación"): _reading("in_evaluation"'),
]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    survivors, killed = [], 0
    for label, relative, old, new in MUTANTS:
        path = ROOT / relative
        original = path.read_bytes()
        text = original.decode("utf-8")
        if text.count(old) != 1:
            print(f"ABORT {label}: pattern found {text.count(old)} times in {relative}")
            return 2
        before = _sha(path)
        try:
            path.write_text(text.replace(old, new), encoding="utf-8")
            result = subprocess.run([sys.executable, "-m", "pytest", *FOCAL, "-q", "-x", "--tb=no",
                                     "-p", "no:cacheprovider"], cwd=ROOT, capture_output=True, text=True)
        finally:
            path.write_bytes(original)
        assert _sha(path) == before, f"{relative} was not restored"
        status = "KILLED" if result.returncode != 0 else "SURVIVED"
        killed += result.returncode != 0
        if result.returncode == 0:
            survivors.append(label)
        print(f"{status:8} {label}")
    print(f"mutants killed {killed}/{len(MUTANTS)}")
    return 0 if not survivors else 1


if __name__ == "__main__":
    raise SystemExit(main())
