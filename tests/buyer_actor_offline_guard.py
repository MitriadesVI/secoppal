"""Optional pytest plugin: enforce the authorized offline validation boundary."""
import socket
import pytest


@pytest.fixture(autouse=True)
def forbid_external_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Buyer-Actor V1 validation forbids external network")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
