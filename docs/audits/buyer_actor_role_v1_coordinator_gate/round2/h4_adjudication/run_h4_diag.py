"""Run the H4 diagnostic under the gate adapter's exact environment."""
import subprocess
import sys
import tempfile
from pathlib import Path

GATE = Path('/Users/rodrigoortiz/Documents/secoppal-buyer-actor-role-v1-gate/'
            'docs/audits/buyer_actor_role_v1_coordinator_gate')
sys.path.insert(0, str(GATE))
sys.path.insert(0, str(GATE.parent / 'entity_profile_frame_v1_2_coordinator_gate'))
import run_gate as buyer_adapter  # noqa: E402  (installs support.environment override)
import gate_support as support  # noqa: E402

SCRATCHPAD = Path(sys.argv[1]).parent
ROOT = support.ROOT

with tempfile.TemporaryDirectory(prefix='h4-diagnostic-', dir=str(SCRATCHPAD)) as temp:
    scratch = Path(temp)
    env = buyer_adapter.environment(scratch, 'h4_adjudication', 'offline', output=GATE)
    env['TMPDIR'] = str(scratch)
    command = [str(support.BIN / 'python'), str(SCRATCHPAD / 'h4_diagnose.py'), sys.argv[1]]
    result = subprocess.run(command, cwd=ROOT, env=env)
    print('COMMAND:', ' '.join(command))  # noqa: T201
    print('EXIT:', result.returncode)  # noqa: T201
    raise SystemExit(result.returncode)
