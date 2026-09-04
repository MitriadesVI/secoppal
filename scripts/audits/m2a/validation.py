"""Offline validation, retained logs and independent deterministic replay."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile

from scripts.audits.m2a.io import ROOT, digest, save


def validate(out):
    commands = {
        'focal_tests': [sys.executable, '-m', 'pytest', 'tests/test_municipal_m2a.py', '-q'],
        'corpus_89': [sys.executable, 'scripts/run_query_corpus.py', '--mode', 'all'],
        'ruff': [str(ROOT / '.venv/bin/ruff'), 'check', 'app/', 'tests/', 'scripts/'],
        'lint_core': ['make', 'lint-core'],
        'diff_check': ['git', 'diff', '--check'],
    }
    results = {}
    logs = out / 'validation'
    logs.mkdir(parents=True, exist_ok=True)
    for name, argv in commands.items():
        run = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True)
        log = logs / f'{name}.log'
        log.write_text('$ ' + ' '.join(argv) + '\n' + run.stdout + run.stderr)
        results[name] = dict(status='PASS' if run.returncode == 0 else 'FAIL', exit_code=run.returncode,
                             command=argv, log=f'validation/{name}.log', log_sha256=digest(log))
        print(name, results[name]['status'], flush=True)
    with tempfile.TemporaryDirectory(prefix='municipal-m2a-replay-') as directory:
        run = subprocess.run([sys.executable, str(ROOT / 'scripts/audits/audit_municipal_m2a.py'),
                              '--output', directory], cwd=ROOT, text=True, capture_output=True)
        (logs / 'replay.log').write_text(run.stdout + run.stderr)
        replay = Path(directory)
        files = sorted(p.name for p in replay.iterdir() if p.is_file())
        comparison = {name: dict(original=digest(out / name), replay=digest(replay / name)) for name in files}
        results['deterministic_replay'] = dict(
            status='PASS' if run.returncode == 0 and files and all(v['original'] == v['replay'] for v in comparison.values()) else 'FAIL',
            files=comparison, exit_code=run.returncode)
    source_files = [ROOT / 'scripts/audits/audit_municipal_m2a.py', ROOT / 'tests/test_municipal_m2a.py',
                    *sorted((ROOT / 'scripts/audits/m2a').glob('*.py'))]
    proof = json.loads((out / 'byte_identity.json').read_bytes())
    results['full_suite'] = dict(required=proof['status'] != 'PASS', executed=False,
                                 reason='All protected production and frozen evidence bytes unchanged.')
    results['code_sha256'] = {str(p.relative_to(ROOT)): digest(p) for p in source_files}
    results['status'] = ('PASS' if not results['full_suite']['required'] and
                         all(v.get('status', 'PASS') == 'PASS' for v in results.values() if isinstance(v, dict)) else 'FAIL')
    save(out / 'validation.json', results)
    if results['status'] != 'PASS':
        raise ValueError('M2A validation failed; inspect validation.json and logs')
    return results
