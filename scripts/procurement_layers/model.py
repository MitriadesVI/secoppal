"""Typed, evidence-bearing instances of the accepted shadow contract; no I/O."""
from dataclasses import asdict, dataclass
import json
import re

from app.core.procurement_relationships import EDGE_ENDPOINTS, EDGE_SOURCES, NODE_FORMATS

LAYERS = (
    (1, "Dónde", ("TERRITORY",)),
    (2, "Quién compra", ("INSTITUTION", "BUYER_GROUP")),
    (3, "Qué tramita y contrata", ("PROCESS", "CONTRACT")),
    (4, "Con qué registro participa en SECOP", ("SECOP_SUPPLIER", "SUPPLIER_GROUP")),
    (5, "Identidad documental", ("ACTOR",)),
    (6, "Qué planea comprar", ("PAA_ITEM",)),
)


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True, order=True)
class Evidence:
    source: str
    path: str
    sha256: str
    locator: str
    observed_at: str

    def __post_init__(self):
        if not all(isinstance(v, str) and v.strip() for v in asdict(self).values()):
            raise ValueError("evidence requires source, path, hash, locator and observation time")
        if not re.fullmatch(r"[0-9a-f]{64}", self.sha256):
            raise ValueError("invalid evidence digest")


@dataclass(frozen=True)
class Node:
    kind: str
    id: str
    label: str

    def __post_init__(self):
        if self.kind not in NODE_FORMATS or self.kind == "PAA_ITEM":
            raise ValueError("unknown or deferred node type")
        prefix = NODE_FORMATS[self.kind].split("<", 1)[0]
        if not isinstance(self.id, str) or not self.id.startswith(prefix):
            raise ValueError("node namespace does not match type")
        key = self.id[len(prefix):]
        if not key.strip() or not isinstance(self.label, str) or not self.label.strip():
            raise ValueError("empty node identity or label")
        if self.kind in {"SECOP_SUPPLIER", "SUPPLIER_GROUP"}:
            if not re.fullmatch(r"[0-9]+", key) or set(key) == {"0"}:
                raise ValueError("platform identity requires exact nonzero ASCII code")
        if self.kind == "TERRITORY" and not re.fullmatch(r"[0-9]{5}", key):
            raise ValueError("territory identity requires DIVIPOLA")
        if self.kind == "INSTITUTION" and not re.fullmatch(r"(?:logical|m2b-logical|m2b1-logical):[0-9a-f]{20}", key):
            raise ValueError("institution requires an accredited logical ID, never a name or NIT")
        if self.kind == "ACTOR" and not re.fullmatch(r"(?:NIT|CC|UNKNOWN):[0-9]+", key):
            raise ValueError("actor requires typed documentary identity")


class LayerGraph:
    """No transitive identity, inferred relations, promotion or completeness claims.

    Evidence references establish provenance, not source truth. Adapters must
    validate each relation against its source row before calling add_edge.
    Conflicting observations remain separate; none silently wins.
    """

    def __init__(self):
        self._nodes = {}
        self._observations = {}
        self._edges = {}
        self._issues = set()

    def add_node(self, node, evidence, attributes=None):
        if not isinstance(node, Node) or not isinstance(evidence, Evidence):
            raise ValueError("typed node and evidence required")
        existing = self._nodes.get(node.id)
        if existing and existing.kind != node.kind:
            raise ValueError("identity collision")
        self._nodes[node.id] = node if existing is None or node.label < existing.label else existing
        observation = canonical({"label": node.label, "attributes": attributes or {},
                                 "evidence": asdict(evidence)})
        self._observations.setdefault(node.id, set()).add(observation)
        return node.id

    def add_edge(self, kind, source, target, evidence, attributes=None):
        if kind not in EDGE_ENDPOINTS:
            raise ValueError("unknown, deferred or derived relation cannot be materialized")
        if source not in self._nodes or target not in self._nodes:
            raise ValueError("both endpoints must exist")
        if (self._nodes[source].kind, self._nodes[target].kind) != EDGE_ENDPOINTS[kind]:
            raise ValueError("wrong endpoint types")
        refs = tuple(sorted(set(evidence)))
        if not refs or not all(isinstance(ref, Evidence) for ref in refs):
            raise ValueError("edge evidence is required")
        if not EDGE_SOURCES[kind] <= {ref.source for ref in refs}:
            raise ValueError("missing required source evidence")
        if kind == "HAS_RECORDED_CONTRACT" and any(
            other_kind == kind and other_target == target and other_source != source
            for other_kind, other_source, other_target, _ in self._edges
        ):
            raise ValueError("contract cannot have multiple accredited process parents")
        attrs = canonical(attributes or {})
        key = (kind, source, target, attrs)
        self._edges.setdefault(key, set()).update(refs)

    def issue(self, code, detail):
        self._issues.add(canonical({"code": code, "detail": detail}))

    def export(self):
        nodes = [{**asdict(node), "observations": [json.loads(v) for v in sorted(self._observations[key])]}
                 for key, node in sorted(self._nodes.items())]
        edges = [{"kind": kind, "from": source, "to": target,
                  "attributes": json.loads(attrs), "assertion_status": "SUPPORTED",
                  "coverage_status": "PARTIAL", "runtime_status": "SHADOW",
                  "evidence": [asdict(ref) for ref in sorted(refs)]}
                 for (kind, source, target, attrs), refs in sorted(self._edges.items())]
        return {"schema_version": 1, "state": "SHADOW", "nodes": nodes, "edges": edges,
                "layers": [{"number": n, "label": label, "node_types": list(kinds),
                            "node_count": sum(node.kind in kinds for node in self._nodes.values()),
                            "status": "DEFERRED" if n == 6 else "OBSERVED" if any(
                                node.kind in kinds for node in self._nodes.values()) else "NO_OBSERVATIONS"}
                           for n, label, kinds in LAYERS],
                "deferred_relations": ["REPRESENTS_ACTOR", "PLANNED_AS"],
                "coverage_status": "PARTIAL", "absence_means_zero": False,
                "issues": [json.loads(v) for v in sorted(self._issues)]}

    def neighborhood(self, node_id, *, hops=1):
        """Return an induced view of observed edges; traversal never creates facts."""
        if type(hops) is not int or not 0 <= hops <= 3:
            raise ValueError("hops must be an integer between 0 and 3")
        if node_id not in self._nodes:
            raise ValueError("unknown node identity")
        selected = {node_id}
        for _ in range(hops):
            adjacent = {endpoint for _, source, target, _ in self._edges
                        if source in selected or target in selected for endpoint in (source, target)}
            selected.update(adjacent)
        result = self.export()
        result["nodes"] = [node for node in result["nodes"] if node["id"] in selected]
        result["edges"] = [edge for edge in result["edges"] if edge["from"] in selected and edge["to"] in selected]
        for layer in result["layers"]:
            layer["node_count"] = sum(node["kind"] in layer["node_types"] for node in result["nodes"])
            if layer["number"] != 6:
                layer["status"] = "OBSERVED" if layer["node_count"] else "NO_OBSERVATIONS"
        result["view"] = {"root": node_id, "hops": hops, "counts_scope": "VIEW_ONLY"}
        return result
