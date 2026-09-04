"""Retain focal, corpus and lint logs, and compare independent offline outputs."""
from pathlib import Path
import subprocess
import sys
import tempfile

from scripts.audits.m2b.io import ROOT, digest, save, read, protected_hashes, byte_identity


def validate(out):
    out = Path(out)
    baseline = {p: v['before'] for p, v in read(out / 'byte_identity.json')['files'].items()}
    commands = {
        'focal_tests': [sys.executable, '-m', 'pytest', 'tests/test_municipal_m2b.py', 'tests/test_municipal_m2a.py', '-q'],
        'corpus_89': [sys.executable, 'scripts/run_query_corpus.py', '--mode', 'all'],
        'ruff': [str(ROOT / '.venv/bin/ruff'), 'check', 'app/', 'tests/', 'scripts/'],
        'lint_core': ['make', 'lint-core'],
        'diff_check': ['git', 'diff', '--check'],
        'staged_diff_check': ['git', 'diff', '--cached', '--check'],
    }
    logs = out / 'validation'
    logs.mkdir(parents=True, exist_ok=True)
    results = {}
    for name, argv in commands.items():
        run = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True)
        log = logs / f'{name}.log'
        log.write_text('$ ' + ' '.join(argv) + '\n' + run.stdout + run.stderr)
        results[name] = dict(status='PASS' if run.returncode == 0 else 'FAIL', exit_code=run.returncode,
                             command=argv, log=f'validation/{name}.log', log_sha256=digest(log))
        if name == 'corpus_89':
            # Verify the expected corpus denominator, not only the exit code.
            results[name]['expected_cases'] = 89
            results[name]['observed_summary'] = [line for line in run.stdout.splitlines() if '89' in line]
            if 'Summary: 89/89 PASS, 0 FAIL' not in run.stdout:
                results[name]['status'] = 'FAIL'
        print(name, results[name]['status'], flush=True)
    with tempfile.TemporaryDirectory(prefix='municipal-m2b-replay-') as directory:
        run = subprocess.run([sys.executable, str(ROOT / 'scripts/audits/audit_municipal_m2b.py'), '--output', directory],
                             cwd=ROOT, text=True, capture_output=True)
        (logs / 'replay.log').write_text(run.stdout + run.stderr)
        replay = Path(directory)
        names = sorted(p.name for p in replay.iterdir() if p.is_file())
        comparison = {name: dict(original=digest(out / name), replay=digest(replay / name)) for name in names}
        results['deterministic_replay'] = dict(status='PASS' if run.returncode == 0 and len(names) == 13 and
            all(v['original'] == v['replay'] for v in comparison.values()) else 'FAIL', files=comparison, exit_code=run.returncode)
    proof = byte_identity(baseline)
    save(out / 'byte_identity.json', proof)
    results['post_validation_byte_identity'] = dict(status=proof['status'], files=len(protected_hashes()), changed=proof['changed'])
    required = proof['status'] != 'PASS'
    results['full_suite'] = dict(required=required, executed=required,
        reason='Explicit user gate: full suite required only if protected bytes change.')
    if required:
        run = subprocess.run([sys.executable, '-m', 'pytest', '-q'], cwd=ROOT, capture_output=True, text=True)
        (logs / 'full_suite.log').write_text(run.stdout + run.stderr)
        results['full_suite'].update(exit_code=run.returncode, status='PASS' if run.returncode == 0 else 'FAIL')
    code = [ROOT / 'scripts/audits/audit_municipal_m2b.py', ROOT / 'tests/test_municipal_m2b.py',
            *sorted((ROOT / 'scripts/audits/m2b').glob('*.py'))]
    results['code_sha256'] = {str(p.relative_to(ROOT)): digest(p) for p in code}
    results['status'] = 'PASS' if all(v.get('status', 'PASS') == 'PASS' for v in results.values() if isinstance(v, dict)) else 'FAIL'
    save(out / 'validation.json', results)
    if results['status'] != 'PASS':
        raise ValueError('M2B validation failed; inspect validation.json and logs')
    return results
