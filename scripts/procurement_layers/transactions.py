"""Preserve source identities and publish only exact, unambiguous portfolio joins."""
from collections import defaultdict

from scripts.audits.relationship_foundation.identity_spine import platform_code, transaction_id
from scripts.audits.relationship_foundation.process_contract import analyze
from scripts.procurement_layers.institutions import binding
from scripts.procurement_layers.model import Node

P = "p6dx-8zbt"
C = "jbjy-vk9h"


def add_transactions(graph, sources, canonical, iid, processes, contracts, parents):
    for dataset, observations, field, kind, relation in (
        (P, processes + parents, "entidad", "PROCESS", "PUBLISHED"),
        (C, contracts, "nombre_entidad", "CONTRACT", "PUBLISHED_CONTRACT"),
    ):
        names, ref = binding(sources, canonical, dataset)
        for observation in observations:
            row, evidence = observation.row, observation.evidence
            identifier = transaction_id(row, dataset)
            if identifier is None:
                graph.issue("UNSTABLE_TRANSACTION", {"source": evidence.path, "row": evidence.locator})
                continue
            graph.add_node(Node(kind, identifier, identifier.split(":", 2)[2]), evidence, row)
            if iid and row.get(field) in names:
                graph.add_edge(relation, iid, identifier, [evidence, ref])
            elif row.get(field) not in names:
                graph.issue("BUYER_BINDING_MISMATCH", {"id": identifier, "reported_buyer": row.get(field)})
    summary = analyze([o.row for o in contracts], [o.row for o in parents], "id_del_portafolio")
    process_refs, contract_refs = defaultdict(set), defaultdict(set)
    for observation in parents:
        process_refs[transaction_id(observation.row, P)].add(observation.evidence)
    accounts = defaultdict(set)
    for observation in contracts:
        row, evidence = observation.row, observation.evidence
        cid = transaction_id(row, C)
        if not cid:
            continue
        contract_refs[cid].add(evidence)
        code = platform_code(row.get("codigo_proveedor"))
        if code is None:
            graph.issue("CONTRACTOR_ACCOUNT_UNAVAILABLE", {"id": cid, "row": evidence.locator})
            continue
        sid = "secop_supplier:" + code
        graph.add_node(Node("SECOP_SUPPLIER", sid, code), evidence)
        graph.add_edge("HAS_CONTRACTOR", cid, sid, [evidence],
                       {"meaning": "REPORTED_CONTRACTOR", "contract_state": row.get("estado_contrato"),
                        "signature_date": row.get("fecha_de_firma")})
        accounts[code].add(evidence)
    for edge in summary["edges"]:
        graph.add_edge("HAS_RECORDED_CONTRACT", edge["from"], edge["to"],
                       process_refs[edge["from"]] | contract_refs[edge["to"]],
                       {"portfolio": edge["join_key"], "meaning": "RECORDED_ASSOCIATION"})
    for key in ("ambiguous_rows", "orphans", "null_or_unstable_rows"):
        for row in summary[key]:
            graph.issue("PROCESS_CONTRACT_" + key.upper(), row)
    return accounts
