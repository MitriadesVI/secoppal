"""Durable reset generation, protected by the envelope publication lock."""
import os
from pathlib import Path


def epoch_path(envelope_path):
    return Path(str(envelope_path) + '.epoch')


def read_epoch(envelope_path):
    path = epoch_path(envelope_path)
    return int(path.read_text()) if path.exists() else 0


def advance_epoch(envelope_path):
    path = epoch_path(envelope_path)
    value = read_epoch(envelope_path) + 1
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as handle:
        handle.write(str(value))
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
