"""Reproduce frozen targets without writing implementer or historical evidence."""
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
from audit_output import emit

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
AUTHOR = ROOT / 'docs/audits/entity_profile_frame_v1_2_b2_remediation'
BIN = Path('/Users/rodrigoortiz/Documents/secoppal/.venv/bin')
CANDIDATE = '5d715ba9e7d67aa05f703d2a280fdf9e2efde3b1'
ENV = dict(os.environ, PYTHONPATH=os.pathsep.join([str(ROOT), str(ROOT/'tests'), str(ROOT/'scripts/audits/entity_profile_frame_v1_2_b_independent')]), PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false', PYTHONDONTWRITEBYTECODE='1')


def run(name, args):
    env = dict(ENV, AUDIT_FLAG_LOG=str(OUT / f'{name}_flags.json'))
    if '-m' in args and 'pytest' in args:
        args += ['--junitxml=' + str(OUT / f'{name}.xml')]
    with (OUT / f'{name}.txt').open('w') as log:
        log.write(shlex.join(args) + '\n'); log.flush()
        result = subprocess.run(args, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
        log.write(f'\nEXIT_CODE={result.returncode}\n')
    emit(name, result.returncode, flush=True)


def load_author(name):
    spec = importlib.util.spec_from_file_location('author_' + name, AUTHOR / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.OUT = OUT
    return module


def preserve():
    files = subprocess.check_output(['git', 'ls-tree', '-rz', '--name-only', CANDIDATE], cwd=ROOT).decode().split('\0')
    hashes = {p: hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in files if p}
    existing = OUT/'candidate_sha256.json'
    if existing.exists():
        assert hashes == json.loads(existing.read_text()), 'candidate files changed'
    else:
        existing.write_text(json.dumps(hashes, indent=2) + '\n')
    for p in files:
        if not p:
            continue
        content = subprocess.check_output(['git', 'show', f'{CANDIDATE}:{p}'], cwd=ROOT)
        assert hashlib.sha256(content).hexdigest() == hashes[p], p
    emit('PRESERVED', len(hashes), flush=True)


def main():
    mode = sys.argv[1]
    if mode == 'preserve':
        preserve()
    elif mode in ('regressions', 'historical_current', 'final_main'):
        module = load_author('run_checks'); module.run = run; module.main()
    elif mode == 'mutate':
        module = load_author('mutate'); module.SHA = CANDIDATE; module.main()
    elif mode == 'acceptance':
        command = gzip.decompress((AUTHOR/'final_acceptance.txt.gz').read_bytes()).decode().splitlines()[0]
        args = [a for a in shlex.split(command) if not a.startswith('--junitxml')]
        run(mode, args)
    elif mode == 'gates':
        run('ruff', [str(BIN/'ruff'), 'check', 'app/', 'tests/', 'scripts/'])
        run('semgrep', [str(BIN/'semgrep'), '--config', '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off'])
        run('semgrep_json', [str(BIN/'semgrep'), '--config', '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off', '--json', '--output', str(OUT/'semgrep.json')])
        run('lint_core', ['make', 'lint-core'])
        run('diff_check', ['git', 'diff', '--check', 'af3d0c7..HEAD'])
    else:
        run(mode, [str(BIN/'python'), '-m', 'pytest', '-q', *sys.argv[2:]])


if __name__ == '__main__':
    main()
