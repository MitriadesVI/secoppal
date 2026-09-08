"""Mutate a throwaway export; this worktree runtime is never written."""  # noqa: T201
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
SCRATCH = Path('/private/tmp/claude-501/-Users-rodrigoortiz-Documents-secoppal/'
               '45fd252a-fb1b-41d7-b15e-b20431913cf6/scratchpad/b3-mutants')
BIN = Path('/Users/rodrigoortiz/Documents/secoppal/.venv/bin')
SHA = '4edc248e60610efdbeebb3cb8010bc7b59445cad'

MUTANTS = {
    'drop-blocked-prefix-capture': (
        'app/core/workflow/publication_io.py',
        "    blocked_prefix = barrier.exists()\n",
        "    blocked_prefix = False\n",
    ),
    'marker-before-arm-instead-of-after': (
        'app/core/workflow/publication_io.py',
        "    blocked_prefix = barrier.exists()\n    offset = path.stat().st_size if path.exists() else 0\n    _arm(barrier)\n",
        "    offset = path.stat().st_size if path.exists() else 0\n    _arm(barrier)\n    blocked_prefix = barrier.exists()\n",
    ),
    'marker-without-trailing-newline': (
        'app/core/workflow/publication_io.py',
        "handle.write(b'\\n{\"publication_incomplete\":true}\\n')",
        "handle.write(b'')",
    ),
    'ranking-shared-amount-condition': (
        'app/core/entity_profile/supplier_rankings.py',
        "        counts if ranking == 'count' else amounts,\n",
        "        counts and amounts,\n",
    ),
    'ranking-render-recomputes-from-amounts': (
        'app/core/entity_profile/render.py',
        "    if by_value and section_ranking(profile.suppliers, 'value').accredited:\n",
        "    if by_value and all(item.total_contract_value is not None for item in by_value):\n",
    ),
    'ranking-warning-generalised-to-value': (
        'app/core/entity_profile/supplier_rankings.py',
        "    label = 'cantidad' if ranking == 'count' else 'valor'\n",
        "    label = 'valor'\n",
    ),
}


def export():
    if SCRATCH.exists():
        shutil.rmtree(SCRATCH)
    SCRATCH.mkdir(parents=True)
    subprocess.run(f'git archive {SHA} | tar -x -C {SCRATCH}', shell=True, cwd=ROOT, check=True)
    shutil.copytree(OUT, SCRATCH / 'docs/audits/entity_profile_frame_v1_2_b3_independent',
                    dirs_exist_ok=True)
    return {p: hashlib.sha256((SCRATCH / p).read_bytes()).hexdigest()
            for p, *_ in MUTANTS.values()}


def run(targets):
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(str(SCRATCH / p) for p in (
        '.', 'tests', 'scripts/audits/entity_profile_frame_v1_2_b_independent',
        'docs/audits/entity_profile_frame_v1_2_b2_independent',
        'docs/audits/entity_profile_frame_v1_2_b3_remediation',
        'docs/audits/entity_profile_frame_v1_2_b3_independent')),
        PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false',
        PYTHONDONTWRITEBYTECODE='1')
    result = subprocess.run([str(BIN / 'python'), '-m', 'pytest', '-q', *targets, '--tb=line'],
                            cwd=SCRATCH, env=env, capture_output=True, text=True)
    return result.returncode, result.stdout


def main():
    targets = sys.argv[2:] or ['docs/audits/entity_profile_frame_v1_2_b3_independent']
    names = [sys.argv[1]] if sys.argv[1] != 'all' else list(MUTANTS)
    report = []
    for name in names:
        hashes = export()
        path, anchor, replacement = MUTANTS[name]
        target = SCRATCH / path
        source = target.read_text()
        assert source.count(anchor) == 1, f'{name}: anchor not unique ({source.count(anchor)})'
        control_code, control_out = run(targets)
        target.write_text(source.replace(anchor, replacement))
        assert hashlib.sha256(target.read_bytes()).hexdigest() != hashes[path]
        mutant_code, mutant_out = run(targets)
        target.write_text(source)
        assert hashlib.sha256(target.read_bytes()).hexdigest() == hashes[path], 'restore failed'
        entry = dict(mutant=name, file=path, control_exit=control_code, mutant_exit=mutant_code,
                     control_tail=control_out.strip().splitlines()[-1],
                     mutant_tail=mutant_out.strip().splitlines()[-1],
                     mutant_failures=[l for l in mutant_out.splitlines() if l.startswith('FAILED')][:8],
                     restored=True)
        report.append(entry)
        print(json.dumps(entry, ensure_ascii=False))  # noqa: T201
    (OUT / f'mutations_{"all" if len(names) > 1 else names[0]}.json').write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + '\n')
    # Worktree runtime untouched.
    for path, *_ in MUTANTS.values():
        print('WORKTREE_UNCHANGED', path, subprocess.run(  # noqa: T201
            ['git', 'diff', '--quiet', '--', path], cwd=ROOT).returncode == 0)


if __name__ == '__main__':
    main()
