"""Literal audit observations without runtime logging changes."""
import sys


def emit(*parts, flush=False):
    sys.stdout.write(' '.join(str(part) for part in parts) + '\n')
    if flush:
        sys.stdout.flush()
