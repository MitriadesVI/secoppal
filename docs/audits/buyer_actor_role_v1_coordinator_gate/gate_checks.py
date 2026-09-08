"""Check external HTTP ledger after product execution and record Actor reader."""
import json
import os
from pathlib import Path

import pytest

FLAGS = {}


@pytest.fixture(autouse=True)
def external_attempt_ledger():
    import gate_plugin
    start = len(gate_plugin.DATA['network'])
    yield
    denied = [row for row in gate_plugin.DATA['network'][start:] if not row['allowed']]
    assert not denied, denied


def pytest_runtest_call(item):
    from app.config import actor_profile_enabled, entity_profile_frame_enabled
    FLAGS[item.nodeid] = {'actor': actor_profile_enabled(), 'frame': entity_profile_frame_enabled()}


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    import gate_plugin
    denied = [row for row in gate_plugin.DATA['network'] if not row['allowed']]
    if denied:
        session.exitstatus = 1
    out = Path(os.environ['COORDINATOR_OUTPUT'])
    (out / (os.environ['COORDINATOR_COMPONENT'] + '_external.json')).write_text(
        json.dumps({'flags': FLAGS, 'denied': denied, 'exitstatus': int(session.exitstatus)}, indent=2) + '\n')
