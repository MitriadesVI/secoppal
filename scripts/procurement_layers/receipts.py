"""Read only frozen, manifest-verified receipts and local authority files."""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

from scripts.audits.relationship_foundation.edge_evidence import render
from scripts.procurement_layers.model import Evidence

ROOT = Path(__file__).resolve().parents[2]
AUDIT = Path("artifacts/audits/relationship_foundation_v0")


@dataclass(frozen=True)
class SourceRow:
    row: dict
    evidence: Evidence


class FrozenSources:
    def __init__(self, root=ROOT):
        self.root = Path(root)
        path = self.root / AUDIT / "observation_manifest.json"
        self.manifest_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        self.receipts = json.loads(path.read_text())["receipts"]
        self.used = {}

    def rows(self, label, dataset, *, require_uncapped=False):
        name = label + ".json"
        if name not in self.receipts or Path(name).name != name:
            raise ValueError("receipt absent from frozen manifest")
        path = AUDIT / "observations" / name
        raw = (self.root / path).read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if digest != self.receipts[name]:
            raise ValueError("receipt differs from frozen manifest: " + label)
        receipt = json.loads(raw)
        if receipt["status"] != "MATERIALIZED" or receipt["error"] is not None:
            raise ValueError("non-affirmative source observation")
        if receipt["endpoint"] != f"https://www.datos.gov.co/resource/{dataset}.json":
            raise ValueError("receipt dataset mismatch")
        payload = receipt["payload"]
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise ValueError("source rows must be objects")
        if hashlib.sha256(render(payload)).hexdigest() != receipt["sha256"]:
            raise ValueError("corrupt source payload")
        if require_uncapped and len(payload) >= int(receipt["params"]["$limit"]):
            raise ValueError("capped lookup cannot accredit uniqueness")
        self.used[str(path)] = digest
        return [SourceRow(row, Evidence(dataset, str(path), digest, f"/payload/{index}",
                                       receipt["observed_at"])) for index, row in enumerate(payload)]

    def matching(self, prefix, dataset, *, require_uncapped=False):
        rows = []
        for name in sorted(self.receipts):
            if name.startswith(prefix) and name.endswith(".json"):
                rows.extend(self.rows(name[:-5], dataset, require_uncapped=require_uncapped))
        if not any(name.startswith(prefix) for name in self.receipts):
            raise ValueError("missing receipt family: " + prefix)
        return rows

    def local(self, relative_path, source, locator):
        path = self.root / relative_path
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self.used[relative_path] = digest
        return Evidence(source, relative_path, digest, locator, "LOCAL_VERSIONED_SNAPSHOT")
