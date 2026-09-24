"""Ledger de red del Product Contract: ningún socket externo se abre (plan P3, H10).

Un intento bloqueado queda registrado aunque el runtime atrape la excepción: los
reintentos de ``SecopClient`` y el observer lo hacen, y un test que sólo mirara la
excepción quedaría verde habiendo intentado salir. La verificación mira el ledger.

``AF_UNIX`` sigue permitido: no sale de la máquina y lo usan librerías locales.
"""
from __future__ import annotations

import socket
import threading
import traceback
from dataclasses import dataclass, field


class NetworkForbiddenError(ConnectionRefusedError):
    """Conexión externa bloqueada por el harness de Product Contract."""


class DnsForbiddenError(socket.gaierror):
    """Resolución DNS bloqueada por el harness de Product Contract."""


@dataclass
class NetworkLedger:
    attempts: list[dict] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record(self, operation: str, target: object) -> None:
        with self._lock:
            self.attempts.append({
                "operation": operation,
                "target": repr(target)[:200],
                "stack": "".join(traceback.format_stack(limit=25)),
            })

    def take(self) -> list[dict]:
        """Consume intentos provocados a propósito por un autotest del harness."""
        with self._lock:
            taken, self.attempts = self.attempts, []
        return taken

    def verify(self) -> None:
        if not self.attempts:
            return
        shown = "\n".join(f"- {a['operation']} {a['target']}\n{a['stack']}" for a in self.attempts[:3])
        raise AssertionError(
            f"Product Contract attempted {len(self.attempts)} external network operation(s):\n{shown}"
        )


def _is_unix(sock: socket.socket) -> bool:
    return hasattr(socket, "AF_UNIX") and sock.family == socket.AF_UNIX


def install(monkeypatch, ledger: NetworkLedger) -> NetworkLedger:
    """Parchea las puertas de salida (TCP, datagramas, DNS); ``monkeypatch`` las restaura."""
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_sendto = socket.socket.sendto

    def sendto(self, *args):
        if _is_unix(self):
            return original_sendto(self, *args)
        ledger.record("socket.sendto", args[-1])
        raise NetworkForbiddenError(f"external datagram forbidden: {args[-1]!r}")

    def connect(self, address):
        if _is_unix(self):
            return original_connect(self, address)
        ledger.record("socket.connect", address)
        raise NetworkForbiddenError(f"external connect forbidden: {address!r}")

    def connect_ex(self, address):
        if _is_unix(self):
            return original_connect_ex(self, address)
        ledger.record("socket.connect_ex", address)
        raise NetworkForbiddenError(f"external connect forbidden: {address!r}")

    def create_connection(address, *args, **kwargs):
        ledger.record("socket.create_connection", address)
        raise NetworkForbiddenError(f"external connection forbidden: {address!r}")

    def getaddrinfo(host, *args, **kwargs):
        ledger.record("socket.getaddrinfo", host)
        raise DnsForbiddenError(socket.EAI_NONAME, f"DNS resolution forbidden: {host!r}")

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket.socket, "sendto", sendto)
    monkeypatch.setattr(socket, "create_connection", create_connection)
    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    return ledger
