"""Exclusión POR CONVERSACIÓN entre hilos y entre procesos.

El diseño lo dice sin rodeos: «el RLock de un objeto de buffer no acredita esta
garantía». Un `threading.RLock` vive dentro de una instancia; dos procesos
—dos workers, la UI y un webhook, dos pruebas— comparten el mismo directorio de
conversaciones y no comparten ese candado. La revisión esperada validada bajo un
candado que el otro escritor no ve no valida nada.

Aquí se componen las dos exclusiones que hacen falta y ninguna más:

* `flock` sobre un archivo de bloqueo por chat, que sí cruza procesos;
* un candado en memoria por ruta, porque `flock` es por archivo abierto y dos
  hilos del mismo proceso con descriptores distintos no se excluyen entre sí.

Es exclusión por CHAT, no un candado global: dos conversaciones distintas se
publican en paralelo. Una instalación con escritores en almacenes que no
comparten sistema de archivos necesita otra garantía; esto no la finge.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import errno
import threading
import time


LOCK_SUFFIX = ".lock"

DEFAULT_TIMEOUT_SECONDS = 10.0

_PROCESS_LOCKS: dict[str, threading.Lock] = {}
_REGISTRY_GUARD = threading.Lock()


class ConversationLockTimeout(TimeoutError):
    """Nadie publica «a la fuerza»: si no se obtuvo el turno, no se escribe."""


def _process_lock(key: str) -> threading.Lock:
    with _REGISTRY_GUARD:
        lock = _PROCESS_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _PROCESS_LOCKS[key] = lock
        return lock


@contextmanager
def conversation_lock(
    path: Path, *, timeout: float = DEFAULT_TIMEOUT_SECONDS,
    poll: float = 0.01,
):
    """Toma la exclusión de ESTE chat; la suelta pase lo que pase."""
    lock_path = Path(f"{path}{LOCK_SUFFIX}")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    key = str(lock_path.resolve())
    in_process = _process_lock(key)
    deadline = time.monotonic() + timeout
    if not in_process.acquire(timeout=max(0.0, deadline - time.monotonic())):
        raise ConversationLockTimeout(f"conversation busy: {path.name}")
    handle = None
    try:
        handle = lock_path.open("a+")
        _acquire_file_lock(handle, deadline, poll, path)
        yield
    finally:
        if handle is not None:
            try:
                _release_file_lock(handle)
            finally:
                handle.close()
        in_process.release()


def _acquire_file_lock(handle, deadline: float, poll: float, path: Path) -> None:
    try:
        import fcntl
    except ImportError as exc:
        raise RuntimeError("cross-process conversation locking requires fcntl") from exc
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
                raise
            if time.monotonic() >= deadline:
                raise ConversationLockTimeout(
                    f"conversation busy: {path.name}",
                ) from exc
            time.sleep(poll)


def _release_file_lock(handle) -> None:
    try:
        import fcntl
    except ImportError:  # pragma: no cover - plataformas sin fcntl
        return
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


__all__ = [
    "ConversationLockTimeout",
    "DEFAULT_TIMEOUT_SECONDS",
    "LOCK_SUFFIX",
    "conversation_lock",
]
