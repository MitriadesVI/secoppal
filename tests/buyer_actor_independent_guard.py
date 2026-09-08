"""Offline audit: record attempted network even if product swallows errors."""
import socket
import traceback
import os
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def independent_network_fence(monkeypatch):
    attempts = []

    def forbidden(*args, **kwargs):
        attempts.append(''.join(traceback.format_stack(limit=35)))
        raise AssertionError(attempts[-1])

    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    yield
    if attempts and os.environ.get('BUYER_INDEPENDENT_EVIDENCE'):
        path = Path(os.environ['BUYER_INDEPENDENT_EVIDENCE'])
        path.mkdir(parents=True, exist_ok=True)
        with (path / 'network-attempts.txt').open('a') as stream:
            stream.write('\n'.join(attempts))
    assert not attempts, attempts
