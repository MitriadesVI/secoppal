"""Local publication barrier: visible JSON alone is never a confirmation.

Callers hold the existing per-chat lock. A barrier surviving a failed write
blocks every reader, including a new process. Old epochs are inert after reset.
"""
from pathlib import Path
import os

from app.core.workflow.store_epoch import read_epoch


class PublicationRecoveryError(RuntimeError):
    """Storage could neither fence nor roll back; no publication result is safe."""


def barrier_path(path: Path) -> Path:
    return path.with_name(f'{path.name}.pending-{read_epoch(path)}')


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _arm(barrier: Path) -> None:
    with barrier.open('w', encoding='utf-8') as handle:
        handle.write('publication incomplete\n')
        handle.flush()
        os.fsync(handle.fileno())
    _sync_directory(barrier.parent)


def append_confirmed(path: Path, line: str) -> None:
    barrier = barrier_path(path)
    blocked_prefix = barrier.exists()
    offset = path.stat().st_size if path.exists() else 0
    _arm(barrier)
    separator = ''
    if offset:
        with path.open('rb') as previous:
            previous.seek(-1, os.SEEK_END)
            if previous.read(1) != b'\n':
                separator = '\n'
    # Errors before confirmation leave the durable barrier in place. No
    # rollback of other revisions or chats, and no best-effort promotion.
    with path.open('a', encoding='utf-8') as handle:
        handle.write(separator + line + '\n')
        handle.flush()
        os.fsync(handle.fileno())
    _sync_directory(path.parent)
    try:
        barrier.unlink()
    except OSError:
        # unlink can report failure after visibility. Re-fence before the
        # caller can return continuation_published=False.
        try:
            _arm(barrier)
        except OSError:
            try:
                with path.open('r+b') as handle:
                    handle.truncate(offset)
                    if blocked_prefix:
                        # The offset delimits our append, NOT a confirmed prefix.
                        # Preserve the earlier barrier's rejection in our own
                        # suffix when that barrier cannot be recreated. The
                        # strict envelope reader rejects this incomplete record.
                        handle.seek(offset)
                        handle.write(b'\n{"publication_incomplete":true}\n')
                    handle.flush()
                    os.fsync(handle.fileno())
                _sync_directory(path.parent)
            except OSError as exc:
                raise PublicationRecoveryError('cannot fence or roll back publication') from exc
        raise
