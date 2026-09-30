"""pytest plugin: block non-loopback network and log attempts per node id (Foundation promotion gates)."""
import json
import os
import socket

_LOG = os.environ.get("NETBLOCK_LOG")
_orig_connect = socket.socket.connect
_orig_connect_ex = socket.socket.connect_ex
_orig_gai = socket.getaddrinfo
_current = {"nodeid": None}

def _is_local(addr):
    if not isinstance(addr, tuple):
        return True  # AF_UNIX
    host = str(addr[0])
    return host in ("127.0.0.1", "::1", "localhost") or host.startswith("127.")

def _record(kind, target):
    if _LOG:
        with open(_LOG, "a") as fh:
            fh.write(json.dumps({"nodeid": _current["nodeid"], "kind": kind, "target": str(target)}) + "\n")

def _connect(self, addr):
    if not _is_local(addr):
        _record("connect", addr)
        raise OSError("NETBLOCK: network disabled for promotion gates")
    return _orig_connect(self, addr)

def _connect_ex(self, addr):
    if not _is_local(addr):
        _record("connect_ex", addr)
        raise OSError("NETBLOCK: network disabled for promotion gates")
    return _orig_connect_ex(self, addr)

def _gai(host, *a, **k):
    if host not in (None, "localhost", "127.0.0.1", "::1") and not str(host).startswith("127."):
        _record("getaddrinfo", host)
        raise socket.gaierror("NETBLOCK: DNS disabled for promotion gates")
    return _orig_gai(host, *a, **k)

socket.socket.connect = _connect
socket.socket.connect_ex = _connect_ex
socket.getaddrinfo = _gai

def pytest_runtest_setup(item):
    _current["nodeid"] = item.nodeid
