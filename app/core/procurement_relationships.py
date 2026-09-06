"""Pure shadow relationship-contract reader. No network or product integration."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

DATA_PATH = Path(__file__).resolve().parents[1] / "data/procurement_relationship_registry_v0.json"
NODE_FORMATS = {
    "TERRITORY": "territory:<divipola>",
    "INSTITUTION": "institution:<logical_id>",
    "BUYER_GROUP": "buyer_group:<group_id>",
    "PROCESS": "process:p6dx-8zbt:<stable_key>",
    "CONTRACT": "contract:jbjy-vk9h:<stable_key>",
    "SECOP_SUPPLIER": "secop_supplier:<platform_code>",
    "SUPPLIER_GROUP": "supplier_group:<group_code>",
    "ACTOR": "actor:<document_type>:<base_number>",
    "PAA_ITEM": "paa_item:<future_authority>",
}
NODE_AUTHORITIES = {
    "TERRITORY": "app/data/territorial_knowledge_v0.json:DIVIPOLA",
    "INSTITUTION": "app/core/territorial_knowledge.py:Institution.institution_id",
    "BUYER_GROUP": "app/data/buyer_groups.json:declared id",
    "PROCESS": "app/core/result_identity.py:result_identity",
    "CONTRACT": "app/core/result_identity.py:result_identity",
    "SECOP_SUPPLIER": "official SECOP II platform codes; scoped exact-code witnesses",
    "SUPPLIER_GROUP": "ceth-n4bn:codigo_grupo",
    "ACTOR": "app/core/actor_identity.py:IdentityKey",
    "PAA_ITEM": "DEFERRED_TO_PAA_AUDIT",
}
EDGE_ENDPOINTS = {
    "IN_TERRITORY": ("INSTITUTION", "TERRITORY"),
    "SELECTED_IN_BUYER_GROUP": ("INSTITUTION", "BUYER_GROUP"),
    "PUBLISHED": ("INSTITUTION", "PROCESS"),
    "PUBLISHED_CONTRACT": ("INSTITUTION", "CONTRACT"),
    "HAS_RECORDED_CONTRACT": ("PROCESS", "CONTRACT"),
    "HAS_CONTRACTOR": ("CONTRACT", "SECOP_SUPPLIER"),
    "BID_ON": ("SECOP_SUPPLIER", "PROCESS"),
    "MEMBER_OF_SUPPLIER_GROUP": ("SECOP_SUPPLIER", "SUPPLIER_GROUP"),
}
EDGE_SOURCES = {
    "IN_TERRITORY": {"territorial"},
    "SELECTED_IN_BUYER_GROUP": {"buyer_groups"},
    "PUBLISHED": {"p6dx-8zbt", "buyer_bindings"},
    "PUBLISHED_CONTRACT": {"jbjy-vk9h", "buyer_bindings"},
    "HAS_RECORDED_CONTRACT": {"p6dx-8zbt", "jbjy-vk9h"},
    "HAS_CONTRACTOR": {"jbjy-vk9h"},
    "BID_ON": {"hgi6-6wh3", "p6dx-8zbt"},
    "MEMBER_OF_SUPPLIER_GROUP": {"ceth-n4bn"},
}
ASSERTIONS = frozenset({"SUPPORTED", "PROBABLE", "UNSUPPORTED"})
COVERAGE = frozenset({"COMPLETE", "PARTIAL", "UNKNOWN"})
RUNTIME = frozenset({"PRODUCTIVE", "SHADOW", "DEFERRED"})
IDENTITY_GATES = (
    "TERRITORY_AS_BUYER", "NIT_AS_BUYER_UNIT_ID", "NIT_AS_SECOP_SUPPLIER_ID",
    "SUPPLIER_CODE_AS_ACTOR_ID", "NAME_AS_PRIMARY_NODE_ID", "BUYER_GROUP_INFERRED_MEMBERSHIP",
    "PROCESS_UNSTABLE_IDS_AS_NODES", "CONTRACT_UNSTABLE_IDS_AS_NODES",
)
PRODUCT_GATES = (
    "RUNTIME_IMPORTS", "QUERY_EFFECT", "SOQL_EFFECT", "ENTITY_ANALYSIS_EFFECT",
    "ACTOR_PROFILE_EFFECT", "RADAR_EFFECT",
)
DEFERRED = {"REPRESENTS_ACTOR": ("SECOP_SUPPLIER", "ACTOR"),
            "PLANNED_AS": ("PAA_ITEM", "PROCESS")}
DERIVED = frozenset({"COMPETES_WITH", "PARTNERED_WITH", "BUYER_CONTRACTED_WITH",
                     "WON_AGAINST", "FREQUENT_PARTNER", "SAME_MARKET", "RELATED_TO"})


def _require(condition, message):
    if not condition:
        raise ValueError("invalid procurement relationship contract: " + message)


def _edge(edge, endpoints, sources):
    required = {"from_type", "to_type", "assertion_status", "coverage_status", "runtime_status",
                "source", "identity_authority", "cardinality", "attributes", "known_caveats"}
    _require(isinstance(edge, dict) and required <= edge.keys(), "edge fields")
    _require((edge["from_type"], edge["to_type"]) == endpoints, "typed endpoints")
    for field, vocabulary in (("assertion_status", ASSERTIONS), ("coverage_status", COVERAGE),
                              ("runtime_status", RUNTIME)):
        _require(isinstance(edge[field], str) and edge[field] in vocabulary, field)
    for field in ("source", "identity_authority", "known_caveats"):
        _require(isinstance(edge[field], list) and bool(edge[field])
                 and all(isinstance(x, str) and x.strip() for x in edge[field]), field)
    _require(set(edge["source"]) <= sources, "unknown source authority")
    _require(isinstance(edge["attributes"], dict), "attributes")
    _require(isinstance(edge["cardinality"], dict)
             and set(edge["cardinality"]) == {"out", "in"}, "cardinality")
    _require(all(x in {"0..1", "0..n"} for x in edge["cardinality"].values()), "cardinality values")
    _require(edge["assertion_status"] == "SUPPORTED" or edge["runtime_status"] != "PRODUCTIVE",
             "unsupported productive edge")


def validate(payload):
    """Reject vocabulary drift, authority conflation and accidental promotion."""
    fields = {"schema_version", "data_version", "state", "node_types", "edge_types",
              "derived_relations", "deferred_relations", "source_authorities", "gates"}
    _require(isinstance(payload, dict) and set(payload) == fields, "root fields")
    _require(type(payload["schema_version"]) is int and payload["schema_version"] == 0, "schema")
    _require(payload["data_version"] == "PROCUREMENT_RELATIONSHIP_FOUNDATION_V0", "version")
    _require(payload["state"] == "SHADOW_FOUNDATION", "shadow state")
    nodes = payload["node_types"]
    _require(isinstance(nodes, dict) and set(nodes) == set(NODE_FORMATS), "nine node types")
    for name, template in NODE_FORMATS.items():
        _require(isinstance(nodes[name], dict) and nodes[name].get("id_format") == template,
                 "node identity " + name)
        _require(nodes[name].get("authority") == NODE_AUTHORITIES[name], "node authority")
        _require(isinstance(nodes[name].get("attributes"), list), "node attributes")
        _require(nodes[name].get("materializable") is (name != "PAA_ITEM"), "materialization status")
        if name in {"ACTOR", "SECOP_SUPPLIER", "SUPPLIER_GROUP"}:
            _require(not {"participation_pct", "is_leader"} & set(nodes[name]["attributes"]), "membership attributes belong on edge")
    _require(nodes["ACTOR"].get("document_types") == ["NIT", "CC", "UNKNOWN"], "Actor types")
    _require(nodes["ACTOR"].get("authority") == "app/core/actor_identity.py:IdentityKey", "Actor authority")
    _require(nodes["PAA_ITEM"].get("materializable") is False, "PAA deferred")
    for name in ("PROCESS", "CONTRACT"):
        _require(nodes[name].get("authority") == "app/core/result_identity.py:result_identity"
                 and nodes[name].get("stable_only") is True, "transaction authority")
    sources = payload["source_authorities"]
    _require(isinstance(sources, dict) and bool(sources), "sources")
    edges = payload["edge_types"]
    _require(isinstance(edges, dict) and set(edges) == set(EDGE_ENDPOINTS), "primary edge set")
    for name, endpoints in EDGE_ENDPOINTS.items():
        _edge(edges[name], endpoints, set(sources))
        _require(set(edges[name]["source"]) == EDGE_SOURCES[name], "edge source authority")
        _require(edges[name]["runtime_status"] == "SHADOW", "V0 has no runtime graph edges")
        _require(edges[name]["coverage_status"] == ("COMPLETE" if name == "SELECTED_IN_BUYER_GROUP" else "PARTIAL"), "V0 coverage scope")
    for name in ("PUBLISHED", "PUBLISHED_CONTRACT"):
        _require(edges[name].get("institution_status_required") == "PRODUCT_RECOGNIZED", "buyer status")
    _require(set(edges["MEMBER_OF_SUPPLIER_GROUP"]["attributes"]) == {"participation_pct", "is_leader"}, "membership attributes")
    deferred = payload["deferred_relations"]
    _require(isinstance(deferred, dict) and set(deferred) == set(DEFERRED), "deferred set")
    for name, endpoints in DEFERRED.items():
        _edge(deferred[name], endpoints, set(sources))
        _require(deferred[name]["runtime_status"] == "DEFERRED"
                 and deferred[name]["assertion_status"] == "UNSUPPORTED", "deferred promotion")
    derived = payload["derived_relations"]
    _require(isinstance(derived, dict) and set(derived) == DERIVED, "derived set")
    _require(all(isinstance(d, dict) and d.get("primary") is False for d in derived.values()), "derived authority")
    gates = payload["gates"]
    _require(isinstance(gates, dict) and set(gates) == set(IDENTITY_GATES + PRODUCT_GATES), "gates")
    _require(all(type(x) is int and x == 0 for x in gates.values()), "closed gates")
    return deepcopy(payload)


def load(path=DATA_PATH):
    return validate(json.loads(Path(path).read_text(encoding="utf-8")))


def get_node_type(name, registry=None):
    return deepcopy((registry if registry is not None else load())["node_types"].get(name))


def get_edge_type(name, registry=None):
    data = registry if registry is not None else load()
    return deepcopy(data["edge_types"].get(name, data["deferred_relations"].get(name)))


def supported_edges(registry=None):
    """Supported source assertions, including shadow and partial-coverage edges."""
    data = registry if registry is not None else load()
    return deepcopy({k: v for k, v in data["edge_types"].items() if v["assertion_status"] == "SUPPORTED"})


def deferred_edges(registry=None):
    return deepcopy((registry if registry is not None else load())["deferred_relations"])
