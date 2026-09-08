"""Explicit original/port partition. Full originals are also run and reported."""
from pathlib import Path
import os
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_remediation'
PORTS = (
    'tests/test_v12b_independent_publication.py::test_reservation_serializes_execution_not_only_append',
    'tests/test_v12b_independent_publication.py::test_two_processes_both_enter_when_fcntl_is_missing',
    'tests/test_v12b_independent_boundaries.py::test_current_actor_api_token_reaches_actor_handler',
)


def main():
    env = dict(os.environ, PYTHONPATH=str(ROOT / 'scripts/audits/entity_profile_frame_v1_2_b_independent'),
               PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false')
    targets = [str(p.relative_to(ROOT)) for pattern in ('test_v12b_independent*.py', 'test_v12b1_*.py')
               for p in sorted((ROOT / 'tests').glob(pattern))]
    args = [sys.executable, '-m', 'pytest', '-q', '--tb=short', *targets,
            *(f'--deselect={target}' for target in PORTS)]
    with (OUT / 'acceptance_final.log').open('w') as output:
        output.write('Explicit ports (also executed intact in independent_final.log):\n' + '\n'.join(PORTS) + '\n')
        output.write('COMMAND: ' + repr(args) + '\n')
        output.flush()
        result = subprocess.run(args, cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
        output.write(f'\nEXIT_CODE={result.returncode}\n')
    print(result.returncode)
    return result.returncode


if __name__ == '__main__':
    sys.exit(main())
