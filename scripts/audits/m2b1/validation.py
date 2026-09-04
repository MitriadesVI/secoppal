"""Retain M2B.1 gates and prove an independent offline replay is byte exact."""
from pathlib import Path
import subprocess
import sys
import tempfile

from scripts.audits.m2b1.io import ROOT, byte_identity, digest, protected_hashes, read, save


def validate(out):
    out = Path(out)
    baseline = {path: value['before']
                for path, value in read(out / 'byte_identity.json')['files'].items()}
    commands = {
        'focal_tests': [sys.executable, '-m', 'pytest', 'tests/test_municipal_m2b1.py',
                        'tests/test_municipal_m2b.py', 'tests/test_municipal_m2a.py', '-q'],
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
        results[name] = dict(status='PASS' if run.returncode == 0 else 'FAIL',
                             exit_code=run.returncode, command=argv,
                             log=f'validation/{name}.log', log_sha256=digest(log))
        if name == 'corpus_89':
            results[name]['expected_cases'] = 89
            results[name]['observed_summary'] = [line for line in run.stdout.splitlines()
                                                 if line.startswith('Summary:')]
            if 'Summary: 89/89 PASS, 0 FAIL' not in run.stdout:
                results[name]['status'] = 'FAIL'
        print(name, results[name]['status'], flush=True)

    with tempfile.TemporaryDirectory(prefix='municipal-m2b1-replay-') as directory:
        argv = [sys.executable, str(ROOT / 'scripts/audits/audit_municipal_m2b1.py'),
                '--output', directory]
        run = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True)
        log = logs / 'replay.log'
        log.write_text('$ ' + ' '.join(argv) + '\n' + run.stdout + run.stderr)
        replay = Path(directory)
        names = sorted(path.name for path in replay.iterdir() if path.is_file())
        comparison = {name: dict(original=digest(out / name), replay=digest(replay / name))
                      for name in names}
        status = ('PASS' if run.returncode == 0 and len(names) == 13
                  and all(value['original'] == value['replay']
                          for value in comparison.values()) else 'FAIL')
        results['deterministic_replay'] = dict(status=status, files=comparison,
                                                file_count=len(names), exit_code=run.returncode,
                                                log='validation/replay.log',
                                                log_sha256=digest(log))
        print('deterministic_replay', status, flush=True)

    proof = byte_identity(baseline)
    save(out / 'byte_identity.json', proof)
    results['post_validation_byte_identity'] = dict(
        status=proof['status'], files=len(protected_hashes()), changed=proof['changed'])
    required = proof['status'] != 'PASS'
    results['full_suite'] = dict(required=required, executed=required,
                                 reason='Full suite is required only if protected bytes change.')
    if required:
        argv = [sys.executable, '-m', 'pytest', '-q']
        run = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True)
        log = logs / 'full_suite.log'
        log.write_text('$ ' + ' '.join(argv) + '\n' + run.stdout + run.stderr)
        results['full_suite'].update(exit_code=run.returncode,
                                     status='PASS' if run.returncode == 0 else 'FAIL',
                                     log='validation/full_suite.log', log_sha256=digest(log))
    code = [ROOT / 'scripts/audits/audit_municipal_m2b1.py',
            ROOT / 'tests/test_municipal_m2b1.py',
            *sorted((ROOT / 'scripts/audits/m2b1').glob('*.py'))]
    results['code_sha256'] = {str(path.relative_to(ROOT)): digest(path) for path in code}
    checked = [value for value in results.values() if isinstance(value, dict)
               and 'status' in value]
    results['status'] = ('PASS' if all(value['status'] == 'PASS' for value in checked)
                         else 'FAIL')
    save(out / 'validation.json', results)
    if results['status'] != 'PASS':
        raise ValueError('M2B.1 validation failed; inspect validation.json and logs')
    return results
