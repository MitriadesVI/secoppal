"""Replay the implementer's frozen targets in THIS worktree, own OUT only."""
import gzip
from pathlib import Path
import shlex
import sys

import run_audit

SRC = run_audit.ROOT / 'docs/audits/entity_profile_frame_v1_2_b3_remediation'
THEIRS = '/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b3-remediation'

for name in sys.argv[1:]:
    command = gzip.decompress((SRC / f'{name}.txt.gz').read_bytes()).decode().splitlines()[0]
    args = [a.replace(THEIRS, str(run_audit.ROOT)) for a in shlex.split(command)
            if not a.startswith('--junitxml')]
    run_audit.run('replay_' + name, args + ['--junitxml=' + str(
        run_audit.OUT / f'replay_{name}.xml')])
