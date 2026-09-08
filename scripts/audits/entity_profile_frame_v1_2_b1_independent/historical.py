"""Offline historical controls in disposable Git exports only."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_independent'
TEST = 'tests/test_v12b1_independent_supplier_overflow.py'


def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.glob('app/**/*.py')}


def main():
    results = []
    for name, sha in [('runtime_b', '1b2a85f89e5d556f1de392e11b9da437da6540d1'),
                      ('before_b', '4c86d5bafaa3296b73c6523465b33efa4311910a')]:
        with tempfile.TemporaryDirectory(prefix='secoppal-b1-independent-history-') as temporary:
            root = Path(temporary)
            archive = root / 'source.tar'
            with archive.open('wb') as output:
                subprocess.run(['git', 'archive', sha], cwd=ROOT, stdout=output, check=True)
            with tarfile.open(archive) as source:
                source.extractall(root, filter='data')
            before = hashes(root)
            shutil.copyfile(ROOT / TEST, root / TEST)
            env = dict(os.environ,
                PYTHONPATH=str(ROOT / 'scripts/audits/entity_profile_frame_v1_2_b_independent'),
                PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false')
            args = [sys.executable, '-m', 'pytest', '-q', '--tb=short', TEST]
            result = subprocess.run(args, cwd=root, env=env, capture_output=True, text=True, check=False)
            (OUT / ('supplier_' + name + '.log')).write_text(repr(args) + '\n' + result.stdout + result.stderr)
            if name == 'runtime_b':
                # Semgrep path exclusions are rooted differently outside a Git repo.
                subprocess.run(['git', 'init', '-q'], cwd=root, check=True)
                actual = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], cwd=root, text=True).strip()
                assert Path(actual).resolve() == root.resolve()
                args = ['/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep', '--config',
                        '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off', '--json']
                scan = subprocess.run(args, cwd=root, capture_output=True, text=True, check=True)
                (OUT / 'semgrep_base.json').write_text(scan.stdout)
                (OUT / 'semgrep_base.log').write_text(scan.stderr)
            entry = dict(name=name, sha=sha, exit=result.returncode, runtime_unchanged=before == hashes(root),
                         outcome=[line for line in result.stdout.splitlines() if 'passed' in line and 'failed' in line])
            assert entry['runtime_unchanged']
            results.append(entry)
            print(entry, flush=True)
    (OUT / 'historical_controls.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    main()
