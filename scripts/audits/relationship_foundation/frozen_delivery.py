"""Exact Git authority for historical Foundation replay, never future checkout bytes."""
import subprocess

from scripts.audits.relationship_foundation.edge_evidence import ROOT

BASE = "c79f8460c84ab26631c2534af106405cb16803f2"
# Frozen Foundation delivery: later features are not Foundation product effects.
FOUNDATION_HEAD = "d608336a32553ac11a69924d663491ce852e1446"

LOCAL_SOURCES = (
    "app/core/result_identity.py", "app/core/actor_identity.py", "app/core/territorial_knowledge.py",
    "app/data/territorial_knowledge_v0.json", "app/data/buyer_groups.json",
    "app/data/buyer_source_names.json", "app/data/aliases_db.json",
)


def read_source_blob(path: str) -> bytes:
    """Read an accredited local source at the fixed delivery; Git errors propagate."""
    if path not in LOCAL_SOURCES:
        raise ValueError(f"not a Foundation local source: {path}")
    return subprocess.check_output(["git", "show", f"{FOUNDATION_HEAD}:{path}"], cwd=ROOT)
