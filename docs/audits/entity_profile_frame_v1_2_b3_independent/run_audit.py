"""Independent B.3 reproduction: own OUT, never writes implementer evidence."""
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
B2 = ROOT / 'docs/audits/entity_profile_frame_v1_2_b2_independent'
B3 = ROOT / 'docs/audits/entity_profile_frame_v1_2_b3_remediation'
PLUGIN = ROOT / 'scripts/audits/entity_profile_frame_v1_2_b_independent'
BIN = Path('/Users/rodrigoortiz/Documents/secoppal/.venv/bin')

PATHS = [ROOT, ROOT / 'tests', PLUGIN, B2, B3, OUT]
ENV = dict(
    os.environ,
    PYTHONPATH=os.pathsep.join(str(p) for p in PATHS),
    PYTEST_ADDOPTS='-p offline_guard',
    SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false',
    PYTHONDONTWRITEBYTECODE='1',
)


def run(name, args, env_extra=None):
    env = dict(ENV, AUDIT_FLAG_LOG=str(OUT / f'{name}_flags.json'), **(env_extra or {}))
    log = OUT / f'{name}.txt'
    with log.open('w') as handle:
        handle.write(shlex.join(args) + '\n')
        handle.flush()
        result = subprocess.run(args, cwd=ROOT, env=env, stdout=handle,
                                stderr=subprocess.STDOUT, check=False)
        handle.write(f'\nEXIT_CODE={result.returncode}\n')
    tail = log.read_text().strip().splitlines()[-6:]
    print(json.dumps(dict(target=name, exit=result.returncode, tail=tail), ensure_ascii=False))  # noqa: T201
    return result.returncode


def pytest(name, *args):
    return run(name, [str(BIN / 'python'), '-m', 'pytest', '-q', *args,
                      '--junitxml=' + str(OUT / f'{name}.xml')])


if __name__ == '__main__':
    pytest(sys.argv[1], *sys.argv[2:])
