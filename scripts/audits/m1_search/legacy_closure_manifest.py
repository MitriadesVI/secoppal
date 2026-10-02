"""Generate the legacy-closure sha manifest (ADR-103 enm. 4, D1(b)/D1(d)).

``python scripts/audits/m1_search/legacy_closure_manifest.py --base 88653752 [--out PATH]``

The manifest is ``{path: sha256}`` of every ``app/**/*.py``, ``streamlit_app.py``, ``scripts/**/*.py`` and EVERY
file under ``app/data/**`` as they are at ``--base`` (read with ``git cat-file``; ``__pycache__`` excluded). It is the
frozen reference of ``tests/test_m1_wave1_parity_pins.py``; the test never calls git. Regenerating the fixture
needs a declared reason in the commit and a fresh ZEUS review.

``EXTRA_FROZEN_DATA`` adds the versioned non-``.py`` files OUTSIDE ``app/data`` that a module of the legacy closure
names (``territorial_knowledge.EXPECTED_SOURCES``): every versioned data file the closure names is frozen.
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
EXTRA_FROZEN_DATA = (
    "artifacts/audits/municipal_m2a/logical_units.json",
    "artifacts/audits/municipal_m2a/physical_profiles.json",
    "artifacts/audits/municipal_m2b/adjudicated_candidates.json",
    "artifacts/audits/municipal_m2b/pending_identity_profiles.json",
    "artifacts/audits/municipal_m2b_1/adjudicated_delta.json",
    "artifacts/audits/municipal_m2b_1/gates.json",
    "artifacts/audits/municipal_m2b_1/impact.json",
)


def tracked(path: str) -> bool:
    """Whether a repository path belongs to the manifest (code roots and the whole ``app/data`` tree)."""
    parts = path.split("/")
    if "__pycache__" in parts:
        return False
    if path.startswith("app/data/") or path in EXTRA_FROZEN_DATA:
        return True
    return path.endswith(".py") and (path == "streamlit_app.py" or parts[0] in {"app", "scripts"})


def manifest(base: str) -> dict[str, str]:
    listing = subprocess.run(["git", "-C", str(ROOT), "ls-tree", "-r", "-z", "--name-only", base],
                             check=True, capture_output=True).stdout.decode("utf-8")
    out: dict[str, str] = {}
    for path in sorted(item for item in listing.split("\0") if item and tracked(item)):
        blob = subprocess.run(["git", "-C", str(ROOT), "cat-file", "blob", f"{base}:{path}"],
                              check=True, capture_output=True).stdout
        out[path] = hashlib.sha256(blob).hexdigest()
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
