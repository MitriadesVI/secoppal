"""Generate the legacy-closure sha manifest (ADR-103 enm. 4, D1(b)/D1(d), C1_2_FRONTIER_SPEC v3 §3.1).

``python scripts/audits/m1_search/legacy_closure_manifest.py --base 88653752 [--out PATH]``

The manifest is ``{path: sha256}`` of EVERY versioned file at ``--base`` (``git ls-tree -r``; ``__pycache__``
excluded): the ``.py`` files and everything else, ``app/data/**`` and ``artifacts/**`` included. Blobs are read with
``git cat-file``; the working tree is never consulted. It is the frozen reference of
``tests/test_m1_wave1_parity_pins.py``, which never calls git: the pin decides which entries the legacy closure
reads (code by import graph, data by the literals it names, resolved against this very listing) and compares them.
Regenerating the fixture needs a declared reason in the commit and a fresh ZEUS review.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT = ROOT / "tests" / "fixtures" / "legacy_closure_manifest_88653752.json"


def tracked(path: str) -> bool:
    """Whether a repository path belongs to the manifest: every versioned file but compiled caches."""
    return "__pycache__" not in path.split("/")


def _listing(base: str) -> list[tuple[str, str]]:
    raw = subprocess.run(["git", "-C", str(ROOT), "ls-tree", "-r", "-z", base],
                         check=True, capture_output=True).stdout.decode("utf-8")
    out = []
    for entry in raw.split("\0"):
        if not entry:
            continue
        meta, path = entry.split("\t", 1)
        mode, kind, blob = meta.split()
        if kind == "blob" and tracked(path):
            out.append((path, blob))
    return sorted(out)


def manifest(base: str) -> dict[str, str]:
    listing = _listing(base)
    proc = subprocess.Popen(["git", "-C", str(ROOT), "cat-file", "--batch"], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE)
    assert proc.stdin is not None and proc.stdout is not None
    out: dict[str, str] = {}
    for path, blob in listing:
        proc.stdin.write(blob.encode("ascii") + b"\n")
        proc.stdin.flush()
        _sha, kind, size = proc.stdout.readline().split()
        if kind != b"blob":
            raise RuntimeError(f"{path}: unexpected object kind {kind!r}")
        data = proc.stdout.read(int(size))
        proc.stdout.read(1)  # the newline that closes each object
        out[path] = hashlib.sha256(data).hexdigest()
    proc.stdin.close()
    proc.wait()
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="the main commit the legacy closure is frozen against")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(argv)
    data = manifest(args.base)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(data, indent=0, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    print(f"{len(data)} entries from {args.base} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
