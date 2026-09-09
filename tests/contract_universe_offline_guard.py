"""Offline validation ledger: caught exceptions do not erase attempted I/O."""
import socket
import traceback
import pytest


@pytest.fixture(autouse=True)
def forbid_external_network(monkeypatch):
    import burr.telemetry
    monkeypatch.setattr(burr.telemetry, 'g_telemetry_enabled', False)
    attempts = []
    def forbidden(*args, **kwargs):
        attempts.append(''.join(traceback.format_stack(limit=12)))
        raise AssertionError('Contract Universe offline validation attempted network')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    yield
    assert not attempts, attempts
