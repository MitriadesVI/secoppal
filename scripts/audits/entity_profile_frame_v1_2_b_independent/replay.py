"""Replay documented focused commands with a network tripwire and raw logs."""
from pathlib import Path
import glob
import os
import re
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b_independent'
VENV = Path('/Users/rodrigoortiz/Documents/secoppal/.venv/bin')


def run(name, args, env=None):
    with (OUT / f'{name}.log').open('w') as log:
        log.write(shlex.join(args) + '\n')
        log.flush()
        result = subprocess.run(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, env=env, check=False)
        log.write(f'\nEXIT_CODE={result.returncode}\n')
    print(name, result.returncode, flush=True)


def main():
    run('ruff_initial', [str(VENV / 'ruff'), 'check', 'app/', 'tests/', 'scripts/'])
    run('semgrep_candidate', [str(VENV / 'semgrep'), '--config', '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off', '--json'])
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).parent), PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false')
    doc = (ROOT / 'docs/audits/entity_profile_frame_v1_2_b/REPRODUCE.md').read_text()
    commands = re.findall(r'```bash\n(.*?)```', doc, re.S)
    for i, command in enumerate(commands):
        if ' -m pytest' not in command:
            continue
        tokens = shlex.split(command.replace('\\\n', ' '))
        expanded = [p for token in tokens for p in (sorted(glob.glob(str(ROOT / token))) if '*' in token else [token])]
        run(f'author_{i}', expanded, env)


if __name__ == '__main__':
    main()
