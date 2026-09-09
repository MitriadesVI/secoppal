"""F2.1 mutation harness: the fix must be load-bearing, killed by product behaviour."""
import json, re, subprocess, sys
from pathlib import Path

ROOT = Path(sys.argv[1])
PY = '/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python'
ARGS = ['-m', 'pytest', '-p', 'tests.contract_universe_offline_guard',
        '-p', 'tests.contract_universe_sql_dates', '-q', '--tb=no']
KILLERS = ['tests/test_contract_universe_f2_1_symmetry.py',
           'tests/test_cu_v11_audit_findings.py',
           'tests/test_cu_v11_audit_f1_f5.py']

CURRENT = """            conflict = len(states) > 1 or not composition.representable"""

MUTATIONS = [
  ('M1_tail_only_detection',
   'app/core/contract_universe_intent.py',
   CURRENT,
   """            conflict = len(states) > 1 or bool(re.match(
                r'\\s+y\\s+(?:(?:registros?|contratos?)\\s+)?(?:suspendidos?|prorrogados?|aprobados?|cerrados?|terminados?|celebrados?|firmados?)\\b',
                normalized.text[match.end():]))""",
   'Restores V1.1: a second condition is only seen in the tail after the match.'),

  ('M2_first_match_wins',
   'app/core/contract_universe_intent.py',
   CURRENT,
   """            conflict = False""",
   'Pure first-match-wins: the first selectable state is executed, whatever else was asked.'),

  ('M3_composition_ignored_only_two_excluded_counted',
   'app/core/contract_universe_intent.py',
   CURRENT,
   """            conflict = len(states) > 1""",
   'Only two adjacent excluded states count; an admitted one before them is dropped.'),

  ('M4_representable_ignores_universe_frontier',
   'app/core/contract_conditions.py',
   """        excluded = {condition.surface for condition in self.excluded}
        if len(excluded) > 1:
            return False
        return not (excluded and self.admitted)""",
   """        return len({condition.surface for condition in self.excluded}) <= 1""",
   'Crossing the universe frontier (admitted + excluded) stops being a conflict.'),

  ('M5_reader_drops_clause_ownership',
   'app/core/contract_conditions.py',
   """            if not _is_request_framing(_consume(prefix, clause, occurrences, consumed)):
                continue""",
   """            pass""",
   'Ownership guard removed: a word inside an object or a name counts as a condition.'),
]


def run(targets):
    out = subprocess.run([PY, *ARGS, *targets], cwd=ROOT, capture_output=True, text=True)
    lines = [l for l in (out.stdout + out.stderr).strip().splitlines() if l.strip()]
    return out.returncode, lines[-1] if lines else ''


results = []
for name, rel, old, new, intent in MUTATIONS:
    path = ROOT / rel
    original = path.read_text()
    if old not in original:
        results.append(dict(mutation=name, applied=False, note='anchor not found'))
        continue
    try:
        path.write_text(original.replace(old, new, 1))
        code, summary = run(KILLERS)
        failed = int(m.group(1)) if (m := re.search(r'(\d+) failed', summary)) else 0
        errors = int(m.group(1)) if (m := re.search(r'(\d+) error', summary)) else 0
        results.append(dict(mutation=name, file=rel, intent=intent, applied=True,
                            exit_code=code, summary=summary, failed=failed, errors=errors,
                            killed_by_product_assertion=bool(failed and not errors)))
    finally:
        path.write_text(original)

# The worktree is a disposable copy of the base with the fix applied on top, so
# git would report the fix itself as "dirty". What must hold is that every
# mutated file came back byte for byte to its post-fix content.
import hashlib
restored = {
    rel: hashlib.sha256((ROOT / rel).read_bytes()).hexdigest()
    == hashlib.sha256(Path(sys.argv[2], rel).read_bytes()).hexdigest()
    for rel in sorted({m[1] for m in MUTATIONS})
}
baseline_code, baseline = run(KILLERS)
print(json.dumps(dict(baseline=baseline, baseline_exit=baseline_code,
                      every_mutated_file_restored=all(restored.values()),
                      restored=restored, killers=KILLERS, results=results),
                 ensure_ascii=False, indent=2))
