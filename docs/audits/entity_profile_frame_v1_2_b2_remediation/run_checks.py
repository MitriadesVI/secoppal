"""Directed frozen targets; never a global pytest invocation."""
import glob
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
VENV = Path('/Users/rodrigoortiz/Documents/secoppal/.venv/bin')
ENV = dict(os.environ, PYTHONPATH=str(ROOT / 'scripts/audits/entity_profile_frame_v1_2_b_independent'),
           PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false')


def run(name, args):
    with (OUT / (name + '.txt')).open('w') as log:
        log.write(shlex.join(args) + '\n')
        log.flush()
        done = subprocess.run(args, cwd=ROOT, env=ENV, stdout=log, stderr=subprocess.STDOUT, check=False)
        log.write(f'\nEXIT_CODE={done.returncode}\n')
    sys.stdout.write(f'{name} {done.returncode}\n')
    sys.stdout.flush()


def main():
    if sys.argv[1] in ('regressions', 'final_main'):
        doc = (ROOT / 'docs/audits/entity_profile_frame_v1_2_b/REPRODUCE.md').read_text()
        for i, command in enumerate(re.findall(r'```bash\n(.*?)```', doc, re.S)):
            if ' -m pytest' not in command:
                continue
            if sys.argv[1] == 'final_main' and 'test_query_router_source_occurrence' not in command:
                continue
            tokens = shlex.split(command.replace('\\\n', ' '))
            args = [p for token in tokens for p in (sorted(glob.glob(str(ROOT / token))) if '*' in token else [token])]
            run(f'{sys.argv[1]}_{i}', args)
    elif sys.argv[1] == 'historical_current':
        import json
        manifest = json.loads((ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_independent/test_reconciliation.json').read_text())
        nodes = manifest['original_directly_current'] + manifest['four_green_controls_executed']
        ports = sorted(str(p.relative_to(ROOT)) for p in (ROOT / 'tests').glob('test_v12b1_*.py')
                       if not p.name.startswith('test_v12b1_independent'))
        run('historical_current', [str(VENV / 'python'), '-m', 'pytest', '-q', *nodes, *ports])
    else:
        run(sys.argv[1], [str(VENV / 'python'), '-m', 'pytest', '-q', *sys.argv[2:]])


if __name__ == '__main__':
    main()
