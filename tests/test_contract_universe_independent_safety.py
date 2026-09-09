"""A swallowed external attempt is a failing safety gate, even outside workflow."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from app.core.secop_client import SecopClient
from tests.contract_universe_independent_support import record


@pytest.mark.parametrize('method', ['with_timeout', 'derived_for_timeout'])
def test_actual_derived_client_keeps_injected_transport(method):
    calls = []
    class Transport:
        name = 'independent'

        def aggregate(self, dataset, sql, **kwargs):
            calls.append((dataset, sql, kwargs))
            return [{'n': 7}]

    transport = Transport()
    original = SecopClient('example.invalid', None, transport=transport)
    derived = getattr(original, method)(2)
    assert derived is not original
    assert derived.transport is transport
    assert derived.aggregate('jbjy-vk9h', 'SELECT count(*) AS n WHERE 1=1') == [{'n': 7}]
    assert calls[0][2]['timeout_seconds'] == 2
    assert original.timeout == 30


@pytest.mark.parametrize('method', ['with_timeout', 'derived_for_timeout'])
def test_ledger_rejects_swallowed_aggregate_attempt_outside_workflow(tmp_path, method):
    test = tmp_path / 'test_safety_canary.py'
    test.write_text('''import socket
from app.core.secop_client import SecopClient
def test_swallowed_external_attempt():
    class Transport:
        name = 'canary'
        def aggregate(self, *args, **kwargs):
            try:
                socket.create_connection(('127.0.0.1', 9))
            except AssertionError:
                return [{'n': 0}]
    client = SecopClient('example.invalid', None, transport=Transport())
    assert client.METHOD(2).aggregate('jbjy-vk9h', 'SELECT count(*) AS n') == [{'n': 0}]
'''.replace('METHOD', method))
    cmd = [sys.executable, '-m', 'pytest', '-p', 'tests.contract_universe_offline_guard',
           str(test), '-q', '--tb=short']
    result = subprocess.run(cmd, env={**os.environ, 'PYTHONPATH': str(Path.cwd()),
                                      'BURR_TELEMETRY_ENABLED': 'false'},
                            capture_output=True, text=True, timeout=30)
    record('safety-canary-' + method, dict(command=cmd, exit_code=result.returncode,
                                         stdout=result.stdout, stderr=result.stderr))
    assert result.returncode == 1
    assert '1 passed, 1 error' in result.stdout
    assert 'assert not attempts' in result.stdout
    assert 'ImportError' not in result.stdout


def test_repeated_mutations_require_green_and_product_difference():
    raw = json.loads(Path('docs/audits/contract_universe_v1_independent/mutations_repeated.json').read_text())
    assert raw['baseline']['exit_code'] == 0
    for name, failure in raw['mutations'].items():
        assert failure['exit_code'] == 1
        assert failure['killed_by_product']
        assert 'ImportError' not in failure['output'] and 'ERROR ' not in failure['output']
        assert '63 == 8' in failure['output'] if name in ('count', 'frame') else (
            'VALID-1' in failure['output'] if name == 'limit' else '8 == 63' in failure['output'])
