"""Pytest plugin: blocks non-loopback network and writes one outcome per nodeid.

NETBLOCK_LEDGER=<path> receives JSON lines {nodeid, outcome}; blocked attempts are
counted in <path>.blocked. Outcome = failed if any phase failed, error if setup/
teardown failed, skipped/xfailed/xpassed as reported, passed otherwise.
"""
import ipaddress
import json
import os
import socket

_LEDGER = os.environ.get("NETBLOCK_LEDGER")
_blocked = 0
_outcomes = {}


def _loopback(host):
    if host in (None, "", "localhost"):
        return True
    if isinstance(host, bytes):
        host = host.decode()
    try:
        return ipaddress.ip_address(host.split("%")[0]).is_loopback
    except ValueError:
        return False


def _guard(address):
    global _blocked
    if isinstance(address, (str, bytes)):  # AF_UNIX
        return
    if not _loopback(address[0]):
        _blocked += 1
        raise OSError(f"network blocked by netblock_ledger: {address[0]}")


_connect, _connect_ex, _gai = socket.socket.connect, socket.socket.connect_ex, socket.getaddrinfo


def _patched_connect(self, address):
    _guard(address)
    return _connect(self, address)


def _patched_connect_ex(self, address):
    _guard(address)
    return _connect_ex(self, address)


def _patched_gai(host, *args, **kwargs):
    global _blocked
    if not _loopback(host):
        _blocked += 1
        raise socket.gaierror(f"network blocked by netblock_ledger: {host}")
    return _gai(host, *args, **kwargs)


socket.socket.connect, socket.socket.connect_ex, socket.getaddrinfo = (
    _patched_connect, _patched_connect_ex, _patched_gai)


def pytest_runtest_logreport(report):
    current = _outcomes.get(report.nodeid, "passed")
    if hasattr(report, "wasxfail"):
        outcome = "xpassed" if report.passed else "xfailed"
    elif report.failed:
        outcome = "failed" if report.when == "call" else "error"
    elif report.skipped:
        outcome = "skipped"
    else:
        outcome = "passed"
    rank = ["passed", "skipped", "xfailed", "xpassed", "failed", "error"]
    _outcomes[report.nodeid] = max(current, outcome, key=rank.index)


def pytest_collectreport(report):
    if report.failed:
        _outcomes[f"COLLECT::{report.nodeid}"] = "error"


def pytest_sessionfinish(session, exitstatus):
    if not _LEDGER:
        return
    with open(_LEDGER, "w", encoding="utf-8") as stream:
        for nodeid, outcome in sorted(_outcomes.items()):
            stream.write(json.dumps({"nodeid": nodeid, "outcome": outcome}) + "\n")
    with open(_LEDGER + ".blocked", "w", encoding="utf-8") as stream:
        stream.write(str(_blocked))
