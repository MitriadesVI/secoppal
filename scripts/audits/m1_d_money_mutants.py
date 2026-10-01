"""M1-D VALUE mutants: each edit re-introduces a defect; the focal tests must kill it.

Run: python scripts/audits/m1_d_money_mutants.py <scratch_dir>
Works on a COPY of app/ and tests/ under <scratch_dir>; the worktree is never edited.
"""
import os
import shutil
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRATCH = sys.argv[1]
CMP, AUTH, DIR, POL = ("app/core/money_comparators.py", "app/core/value_authority.py",
                       "app/core/foundation_value_direction.py", "app/core/money_polarity.py")
MUTANTS = [
 ("M01_negation_ignored", CMP, 'family = negate(family) if (head.negatable and not match.group("eq")) else None',
  "pass"),
 ("M02_negation_keeps_inclusivity", CMP, "_NEGATED = {GT: LTE, GTE: LT, LT: GTE, LTE: GT}",
  "_NEGATED = {GT: LT, GTE: GT, LT: GT, LTE: LT}"),
 ("M03_guard_blind_to_inclusive_operators", DIR, "    if expected is None:\n        return None",
  "    if expected not in (GT, LT):\n        return None"),
 ("M04_gt_accepted_for_gte", DIR, "    if family == expected:\n        return None",
  "    if family == expected or family[0] == expected[0]:\n        return None"),
 ("M05_range_converted_as_one_bound", AUTH,
  'if (scan.between or {"min", "max"} <= bounds\n            or any(d.relation is MoneyRelation.RANGE for d in decisions)):', "if False:"),
 ("M06_no_noise_floor", AUTH, "    if not is_admissible_amount(amount, amount_text):", "    if False:"),
 ("M07_negative_polarity_accepted", AUTH, 'if str(getattr(polarity, "value", polarity)).upper() != "POSITIVE":', "if False:"),
 ("M08_context_negation_ignored", AUTH,
  "if resolved.decision is ValueDecision.RESOLVED and _negation_just_before(left):", "if False:"),
 ("M09_unread_negation_ignored", DIR, " or scan.unread_negation", ""),
 ("M10_polarity_lexicon_drifts", POL, "_UP = UP_PATTERN", '_UP = r"(?:m[aá]s|mayor(?:es)?)"'),
 ("M11_inclusive_flag_inverted", AUTH,
  "ValueResolution(ValueDecision.RESOLVED, bound_of(family), is_inclusive(family), amount, None)",
  "ValueResolution(ValueDecision.RESOLVED, bound_of(family), not is_inclusive(family), amount, None)"),
 ("M12_suffix_comparators_dropped", CMP, "for match in _SUFFIX_RE.finditer(leftover):", "for match in []:"),
 ("M13_symbol_gte_read_as_gt", CMP, '">=": GTE,', '">=": GT,'),
 ("M14_negated_inclusive_phrase_guessed", CMP, '(head.negatable and not match.group("eq"))', "(not match.group('eq'))"),
 ("M15_context_window_unclipped", AUTH, "[-WINDOW_TOKENS:]", "[:]"),
 ("M16_por_lo_menos_back_to_upper", CMP, '"minimo|minima|al menos|por lo menos|desde|', '"minimo|minima|al menos|desde|'),
 ("M17_bare_comparator_word_without_link", CMP,
  'if head.needs_link and not match.group("link") and not match.group("eq"):\n            continue', "if False:\n            continue"),
 ("M18_money_value_decode_accepts_non_ascii", AUTH, " or not encoded[2].isascii()", ""),
 ("M19_range_with_two_figures_converted", AUTH,
  "if sum(len(d.amount_spans) for d in decisions) > 1:", "if False:"),
 ("M20_negative_amount_sign_lost", AUTH, "    if _NEGATIVE_SIGN.search(scan.folded):", "    if False:"),
]
TESTS = ["tests/test_value_authority_m1d.py", "tests/test_foundation_silent_paths.py"]


def main():
    if os.path.exists(SCRATCH):
        shutil.rmtree(SCRATCH)
    os.makedirs(SCRATCH)
    for name in ("app", "tests"):
        shutil.copytree(os.path.join(ROOT, name), os.path.join(SCRATCH, name))
    shutil.copy(os.path.join(ROOT, "pytest.ini"), SCRATCH)
    env = {**os.environ, "PYTHONPATH": SCRATCH}
    baseline = subprocess.run([sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *TESTS],
                              cwd=SCRATCH, env=env, capture_output=True, text=True)
    print("BASELINE", "green" if baseline.returncode == 0 else "RED\n" + baseline.stdout[-800:])
    survivors = 0
    for name, rel, old, new in MUTANTS:
        path = os.path.join(SCRATCH, rel)
        original = open(path).read()
        assert old in original, f"{name}: pattern not found"
        open(path, "w").write(original.replace(old, new, 1))
        try:
            run = subprocess.run([sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *TESTS],
                                 cwd=SCRATCH, env=env, capture_output=True, text=True)
        finally:
            open(path, "w").write(original)
        killed = run.returncode != 0
        survivors += not killed
        failed = [line for line in run.stdout.splitlines() if line.startswith("FAILED")]
        print(f"{'KILLED  ' if killed else 'SURVIVED'} {name}  {failed[0][:110] if failed else ''}")
    print(f"{len(MUTANTS) - survivors}/{len(MUTANTS)} killed")
    sys.exit(1 if survivors else 0)


if __name__ == "__main__":
    main()
