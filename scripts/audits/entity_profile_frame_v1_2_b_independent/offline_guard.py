"""Audit-only pytest plugin: fail any attempted IP socket connection."""
import socket

_original_connect = socket.socket.connect


def _connect(sock, address):
    if sock.family in (socket.AF_INET, socket.AF_INET6):
        raise AssertionError(f"AUDIT_NETWORK_FORBIDDEN: {address}")
    return _original_connect(sock, address)


def pytest_sessionstart(session):
    socket.socket.connect = _connect


_flags = {}


def pytest_runtest_call(item):
    import os
    if os.environ.get('AUDIT_FLAG_LOG'):
        from app.config import entity_profile_frame_enabled
        _flags[item.nodeid] = entity_profile_frame_enabled()


def pytest_sessionfinish(session, exitstatus):
    import json
    import os
    from pathlib import Path
    destination = os.environ.get('AUDIT_FLAG_LOG')
    if destination:
        Path(destination).write_text(json.dumps(_flags, indent=2) + '\n')
