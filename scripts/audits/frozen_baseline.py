"""Digests of a frozen commit's blobs, read from Git rather than the worktree."""
from __future__ import annotations

import hashlib
from pathlib import Path
import subprocess
from typing import Iterable


def blob_digests(root: Path, commit: str, paths: Iterable[str]) -> dict[str, str]:
    """Return each path's SHA-256 digest as committed at ``commit``.

    An unreadable path is an error, never an omission: a gate that silently
    skips what it cannot read stops being a gate.
    """
    requested_paths = list(paths)
    if not requested_paths:
        return {}
    request = ''.join(f'{commit}:{path}\n' for path in requested_paths).encode()
    batch = subprocess.run(
        ['git', 'cat-file', '--batch'],
        cwd=root,
        input=request,
        capture_output=True,
        check=False,
    )
    if batch.returncode != 0:
        message = batch.stderr.decode().strip()
        raise RuntimeError(f'STOP: git cat-file failed: {message}')
    stream = batch.stdout
    digests = {}
    cursor = 0
    for path in requested_paths:
        end = stream.index(b'\n', cursor)
        header = stream[cursor:end].decode()
        if header.split()[-1] in {'missing', 'ambiguous'}:
            raise ValueError(f'STOP: {commit}:{path} is not readable ({header})')
        size = int(header.split()[2])
        content_start = end + 1
        content_end = content_start + size
        digests[path] = hashlib.sha256(stream[content_start:content_end]).hexdigest()
        cursor = content_end + 1
    return digests
