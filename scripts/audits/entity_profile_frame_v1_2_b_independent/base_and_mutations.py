"""Disposable exact Git exports for BASE comparison and author mutation replay."""
from pathlib import Path
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b_independent'
BASE = '4c86d5bafaa3296b73c6523465b33efa4311910a'
CANDIDATE = '1b2a85f89e5d556f1de392e11b9da437da6540d1'
ENV = dict(os.environ, PYTHONPATH=str(Path(__file__).parent), PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false')
TARGET = 'tests/test_actor_hardening_v2_1_remediation.py::test_actor_region_and_buyer_together_still_resolve'


def run(name, args, cwd, env=ENV):
    with (OUT / f'{name}.log').open('w') as log:
        result = subprocess.run(args, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
        log.write(f'\nEXIT_CODE={result.returncode}\n')
    print(name, result.returncode, flush=True)


def export(sha, path):
    archive = subprocess.check_output(['git', 'archive', sha], cwd=ROOT)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(path, filter='data')


def hashes(path):
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest() for p in (path / 'app').rglob('*.py')}


def main():
    with tempfile.TemporaryDirectory(prefix='v12b-independent-base-') as directory:
        base = Path(directory)
        export(BASE, base)
        subprocess.run(['git', 'init', '-q'], cwd=base, check=True)
        subprocess.run(['git', 'add', '.'], cwd=base, check=True)
        run('semgrep_base', ['/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep', '--config', '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off', '--json'], base)
        run('base_xfail', [sys.executable, '-m', 'pytest', TARGET, '-q', '-rx'], base)
        run('base_runxfail', [sys.executable, '-m', 'pytest', TARGET, '-q', '--runxfail', '--tb=short'], base)
    run('candidate_xfail', [sys.executable, '-m', 'pytest', TARGET, '-q', '-rx'], ROOT)
    run('candidate_runxfail', [sys.executable, '-m', 'pytest', TARGET, '-q', '--runxfail', '--tb=short'], ROOT)
    with tempfile.TemporaryDirectory(prefix='v12b-independent-mutations-') as directory:
        candidate = Path(directory)
        export(CANDIDATE, candidate)
        before = hashes(candidate)
        run('mutations_replayed', [sys.executable, 'scripts/audits/entity_profile_frame_v1_2_b/mutation_campaign.py'], candidate)
        after = hashes(candidate)
        (OUT / 'mutation_runtime_restoration.json').write_text(json.dumps({'candidate': CANDIDATE, 'before': before, 'after': after, 'identical': before == after}, indent=2) + '\n')
        shutil.copyfile(candidate / 'docs/audits/entity_profile_frame_v1_2_b/mutation_campaign.json', OUT / 'mutation_replay.json')


if __name__ == '__main__':
    main()
