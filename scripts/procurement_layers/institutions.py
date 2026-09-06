"""Project accredited local authorities without resolving names or promoting buyers."""
import json

from app.core.territorial_knowledge import load
from scripts.audits.relationship_foundation.identity_spine import buyer_names
from scripts.procurement_layers.model import Node


def add_institution(graph, sources, canonical):
    registry = load(sources.root / "app/data/territorial_knowledge_v0.json")
    matches = [i for i in registry.institutions if i.product_buyer_unit == canonical
               and i.knowledge_status == "PRODUCT_RECOGNIZED"]
    if len(matches) != 1:
        graph.issue("BUYER_LOGICAL_ID_UNAVAILABLE", {"canonical": canonical, "matches": len(matches)})
        return None
    institution = matches[0]
    iid = "institution:" + institution.institution_id
    ref = sources.local("app/data/territorial_knowledge_v0.json", "territorial", institution.institution_id)
    graph.add_node(Node("INSTITUTION", iid, institution.display_name), ref,
                   {"product_buyer_unit": canonical, "knowledge_status": institution.knowledge_status})
    territory = registry.territory_index[institution.territory_divipola]
    tid = "territory:" + territory.divipola
    graph.add_node(Node("TERRITORY", tid, territory.municipality), ref,
                   {"department": territory.department})
    graph.add_edge("IN_TERRITORY", iid, tid, [ref], {"meaning": "ACCREDITED_AFFILIATION"})
    groups = json.loads((sources.root / "app/data/buyer_groups.json").read_text())["groups"]
    for group in groups:
        if any(m["canonical_name"] == canonical for m in group["members"]):
            group_ref = sources.local("app/data/buyer_groups.json", "buyer_groups", group["id"])
            gid = "buyer_group:" + group["id"]
            graph.add_node(Node("BUYER_GROUP", gid, group["id"]), group_ref,
                           {"version": group["version"]})
            graph.add_edge("SELECTED_IN_BUYER_GROUP", iid, gid, [group_ref])
    return iid


def binding(sources, canonical, dataset):
    names, authority = buyer_names(canonical, dataset)
    return set(names), sources.local(authority.split(":", 1)[0], "buyer_bindings", canonical)
