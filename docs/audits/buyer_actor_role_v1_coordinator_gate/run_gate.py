"""Buyer gate adapter; reuse the frozen Frame runner without rewriting it."""
import os
import json
from pathlib import Path
import subprocess
import sys
import tempfile

OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]
FRAME = ROOT / 'docs/audits/entity_profile_frame_v1_2_coordinator_gate'
sys.path.insert(0, str(FRAME))
import gate_support as support  # noqa: E402

support.BASE = '52a6bcc68efc50e06ba9aa0f60ff8d30f8cf1386'
support.RUNTIME = 'aab5d41d45e58f21b727929d4ec454f8fbbbf9cd'
original_environment = support.environment


def environment(scratch, name, network, *, output=OUT):
    env = original_environment(scratch, name, network, output=output)
    env.update(BURR_TELEMETRY_ENABLED='false', SECOPPAL_ACTOR_PROFILE_ENABLED='true',
               BUYER_INDEPENDENT_EVIDENCE=str(output / name / 'traces'))
    env['PYTHONPATH'] += os.pathsep + str(OUT)
    env['PYTEST_PLUGINS'] = 'gate_checks'
    env['PYTEST_DEBUG_TEMPROOT'] = str(scratch)
    env['TMPDIR'] = str(scratch)
    env.pop('BUYER_ACTOR_EVIDENCE', None)
    env.pop('BUYER_ACTOR_LOCAL_INDEX', None)
    if name.endswith(('buyer_acceptance', 'buyer_local', 'b2_independent',
                      'b3_remediation', 'b3_independent')) or 'buyer_local' in name \
            or os.environ.get('BUYER_GATE_STRICT') == '1':
        env['PYTEST_PLUGINS'] += ',tests.buyer_actor_independent_guard'
    # Substring, not suffix: a focal rerun of this component is named
    # focal_buyer_local_focused, and without the index it would silently skip
    # the eight opt-in cases instead of running them.
    if 'buyer_local' in name:
        env['BUYER_ACTOR_LOCAL_INDEX'] = str(support.BIN.parents[1] / 'data/actor_alias_index.sqlite')
    return env


support.environment = environment
import run_gate as frame_runner  # noqa: E402

frame_runner.OUT = OUT
frame_runner.BLOCKS.update(
    buyer_acceptance=['tests/test_buyer_actor_independent_product.py',
                      'tests/test_buyer_actor_independent_boundaries.py'],
    buyer_local=['tests/test_buyer_actor_role_local_v1.py',
                 'tests/test_buyer_actor_independent_local.py'],
)

def main():
    extra = []
    if len(sys.argv) > 1 and sys.argv[1] == 'full':
        if (OUT / 'full_results.json').exists() or (OUT / 'full_ruff_active.log').exists():
            raise SystemExit('Full gate already started; preserve its evidence')
        with tempfile.TemporaryDirectory(prefix='buyer-gate-lint-') as temporary:
            command = [str(support.BIN / 'ruff'), 'check', str(FRAME)]
            result = subprocess.run(command, cwd=ROOT,
                                    env=environment(Path(temporary), 'full_ruff_active', 'offline'),
                                    capture_output=True)
            (OUT / 'full_ruff_active.log').write_bytes(result.stdout + result.stderr)
            extra.append({'component': 'ruff_active_frame', 'command': command,
                          'exit': result.returncode, 'pass': result.returncode == 0})
    result = frame_runner.main()
    if len(sys.argv) > 1 and sys.argv[1] == 'full':
        rows = json.loads((OUT / 'full_results.json').read_text()) + extra
        passed = result == 0 and all(row['pass'] for row in rows)
        (OUT / 'execution_manifest.json').write_text(json.dumps(
            {'components': rows, 'pass': passed}, indent=2) + '\n')
        return 0 if passed else 1
    return result


if __name__ == '__main__':
    raise SystemExit(main())
