"""Replay frozen commands while redirecting every output into this audit lane."""
import os
from pathlib import Path
import runpy
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_independent'
AUTHOR = ROOT / 'scripts/audits/entity_profile_frame_v1_2_b1_remediation'


def main():
    phase = sys.argv[1]
    if phase in ('replay', 'mutations'):
        package = runpy.run_path(str(AUTHOR / (phase + '.py')))
        package['main'].__globals__['OUT'] = OUT
        return package['main']() or 0
    env = dict(os.environ, PYTHONPATH=str(ROOT / 'scripts/audits/entity_profile_frame_v1_2_b_independent'),
               PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false')
    if phase == 'acceptance':
        import fnmatch
        frozen = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only',
            'af8f0a3407102dcb807089e87e470d754b835d61', '--', 'tests'], cwd=ROOT, text=True).splitlines()
        targets = [path for path in frozen if any(fnmatch.fnmatch(path, pattern) for pattern in
                   ('tests/test_v12b_independent*.py', 'tests/test_v12b1_*.py'))]
        ports = runpy.run_path(str(AUTHOR / 'acceptance.py'))['PORTS']
        targets.extend('--deselect=' + port for port in ports)
    elif phase == 'originals':
        targets = sorted(str(p.relative_to(ROOT)) for p in (ROOT / 'tests').glob('test_v12b_independent*.py'))
    else:
        targets = sys.argv[2:]
    args = [sys.executable, '-m', 'pytest', '-q', '--tb=short', *targets]
    with (OUT / (phase + '.log')).open('w') as output:
        output.write(repr(args) + '\n')
        output.flush()
        result = subprocess.run(args, cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
        output.write(f'\nEXIT_CODE={result.returncode}\n')
    print(phase, result.returncode, flush=True)
    return result.returncode


if __name__ == '__main__':
    sys.exit(main())
