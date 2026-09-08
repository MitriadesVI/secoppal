"""Ports A/B with independent processes and pipe/event handshakes."""
import os
from pathlib import Path
import select
import subprocess
import sys

import pytest

from app.core.conversation_store import ConversationStore
from entity_profile_frame_b_harness import frame_v12  # noqa: F401


def line(process):
    assert select.select([process.stdout], [], [], 10)[0], 'worker handshake failed'
    return process.stdout.readline().strip()


def test_without_fcntl_every_process_refuses_before_entry(tmp_path):
    """Port B drops the invalid precondition that the first writer gets HELD."""
    script = '''
import sys
from pathlib import Path
sys.modules['fcntl'] = None
from app.core.store_lock import conversation_lock
try:
    with conversation_lock(Path(sys.argv[1])):
        print('HELD')
except (ImportError, RuntimeError):
    print('REFUSED')
'''
    for _ in range(2):
        result = subprocess.run([sys.executable, '-c', script, str(tmp_path / 'chat')],
                                text=True, capture_output=True, timeout=10, check=True)
        assert result.stdout.strip() == 'REFUSED'


_WORKER = '''
import sys
from pathlib import Path
sys.path.insert(0, 'tests')
from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from test_entity_profile_workflow import WorkflowClient
from datetime import date
base = Path(sys.argv[1])
flow = SecopalWorkflow(Settings(deepseek_api_key=None,
    secop_feedback_path=base / (sys.argv[2] + '.jsonl'),
    secop_feedback_db_path=base / (sys.argv[2] + '.sqlite')))
class Client(WorkflowClient):
    def aggregate(self, dataset, sql):
        with self._lock:
            pause = sys.argv[2] == 'first' and not getattr(self, 'paused', False)
            self.paused = True
        if pause:
            print('ENTER', flush=True)
            sys.stdin.readline()
        return super().aggregate(dataset, sql)
flow.secop_client = Client()
flow.narrator.narrate_with_grounding = lambda **kw: None
import app.core.workflow.turn_coordination as coordination
original = coordination.conversation_lock
def announce(*args, **kwargs):
    print('LOCK_REQUEST', flush=True)
    return original(*args, **kwargs)
coordination.conversation_lock = announce
query = ('analiza contratos de alcaldia de baranoa en agosto'
         if sys.argv[2] == 'first' else 'proveedores')
result = flow.run_query(query, chat_id='b1-process', today=date(2026, 9, 7))
print('RESULT', result['conversation_revision'], result['route_reason'], len(flow.secop_client.calls), flush=True)
'''


def test_real_workflows_serialize_across_processes_and_reload(frame_v12, tmp_path):
    env = dict(os.environ, SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='true')
    args = [sys.executable, '-u', '-c', _WORKER, str(tmp_path)]
    first = subprocess.Popen([*args, 'first'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, env=env)
    second = None
    try:
        assert line(first) == 'LOCK_REQUEST'
        assert line(first) == 'ENTER'
        # The execution lock is held at the actual external boundary.
        probe = subprocess.run([sys.executable, '-c', '''
from pathlib import Path
import sys
from app.core.store_lock import conversation_lock, ConversationLockTimeout
try:
    with conversation_lock(Path(sys.argv[1]), timeout=.05):
        print('HELD')
except ConversationLockTimeout:
    print('BUSY')
''', str(ConversationStore()._envelope_path_for('b1-process').with_suffix('.turn'))],
            capture_output=True, text=True, check=True, timeout=10)
        assert probe.stdout.strip() == 'BUSY', 'execution ran outside serial exclusion'
        second = subprocess.Popen([*args, 'second'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True, env=env)
        assert line(second) == 'LOCK_REQUEST'
        first.stdin.write('release\n')
        first.stdin.flush()
        assert line(first) == 'RESULT 1 entity_profile_overview 4'
        assert line(second) == 'RESULT 2 entity_profile_suppliers 2'
        first.wait(10)
        second.wait(10)
        assert first.returncode == second.returncode == 0
        read = ConversationStore().load_envelope('b1-process')
        assert read.revision == 2
        assert read.envelope.profile_state['confirmed_frame']['focus'] == 'suppliers'
    finally:
        for process in (first, second):
            if process is not None:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=5)
