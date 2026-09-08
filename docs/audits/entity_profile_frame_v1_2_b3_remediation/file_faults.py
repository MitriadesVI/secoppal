"""Fail only real filesystem operations of one envelope/epoch; record boundaries."""
from contextlib import contextmanager
import json
import os
from pathlib import Path

from app.core.workflow.publication_io import barrier_path


@contextmanager
def file_fault(monkeypatch, target, kind, *, other=None):
    barrier = barrier_path(target)
    original_open, original_unlink, original_fsync = Path.open, Path.unlink, os.fsync
    reached, visible = [], []

    class Writer:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def write(self, text):
            self.handle.write(text[:len(text) // 2] if kind == 'partial' else text)
            self.handle.flush()
            reached.append(kind)
            raise OSError('B3 injected real append error')

    def open_path(path, mode='r', *args, **kwargs):
        if path == barrier and mode == 'w' and 'unlink_visible' in reached and kind == 'rearm':
            reached.append('rearm_failed')
            raise OSError('B3 rearm failed before open')
        if path == target and mode == 'r+b':
            reached.append('rollback_open')
        handle = original_open(path, mode, *args, **kwargs)
        return Writer(handle) if path == target and mode == 'a' and kind in ('partial', 'complete') else handle

    def unlink(path, *args, **kwargs):
        if path == barrier and not reached and kind in ('before_unlink', 'after_unlink', 'rearm'):
            visible.append(json.loads(target.read_text().splitlines()[-1]))
            if kind != 'before_unlink':
                original_unlink(path, *args, **kwargs)
                assert not barrier.exists()
                reached.append('unlink_visible')
            else:
                reached.append('unlink_before')
            if other:
                other()
            raise OSError('B3 confirmation failed')
        return original_unlink(path, *args, **kwargs)

    def fsync(fd):
        if target.exists() and os.fstat(fd).st_ino == target.stat().st_ino:
            if kind == 'fsync' and not reached:
                visible.append(json.loads(target.read_text().splitlines()[-1]))
                reached.append('envelope_fsync')
                raise OSError('B3 envelope fsync failed')
            if 'rollback_open' in reached:
                reached.append('rollback_fsync')
        elif 'rollback_fsync' in reached and os.fstat(fd).st_ino == target.parent.stat().st_ino:
            reached.append('rollback_directory_fsync')
        return original_fsync(fd)

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'open', open_path)
        patch.setattr(Path, 'unlink', unlink)
        patch.setattr(os, 'fsync', fsync)
        yield reached, visible
    expected = {
        'partial': ['partial'], 'complete': ['complete'], 'fsync': ['envelope_fsync'],
        'before_unlink': ['unlink_before'], 'after_unlink': ['unlink_visible'],
        'rearm': ['unlink_visible', 'rearm_failed', 'rollback_open', 'rollback_fsync', 'rollback_directory_fsync'],
    }
    assert reached == expected[kind], (kind, reached)
