"""Observed participation and supplier-group composition, never inferred Actor identity."""
from collections import defaultdict

from scripts.audits.relationship_foundation.group_membership import membership
from scripts.audits.relationship_foundation.identity_spine import platform_code, transaction_id
from scripts.procurement_layers.model import Node


def add_bids(graph, bids, processes, accounts):
    known = defaultdict(list)
    for observation in processes:
        pid = observation.row.get("id_del_proceso")
        if isinstance(pid, str) and pid.strip():
            known[pid].append(observation)
    for observation in bids:
        row, ref = observation.row, observation.evidence
        pid, code = row.get("id_procedimiento"), platform_code(row.get("codigo_proveedor"))
        candidates = known.get(pid, []) if isinstance(pid, str) else []
        identities = {transaction_id(o.row, "p6dx-8zbt") for o in candidates}
        owners = {o.row.get("entidad") for o in candidates}
        if not code or len(identities) != 1 or len(owners) != 1 or None in identities:
            graph.issue("BID_ENDPOINT_UNACCREDITED", {"row": ref.locator, "path": ref.path})
            continue
        process_id = next(iter(identities))
        sid = "secop_supplier:" + code
        graph.add_node(Node("SECOP_SUPPLIER", sid, code), ref)
        graph.add_edge("BID_ON", sid, process_id, [ref, *(o.evidence for o in candidates)],
                       {"meaning": "OBSERVED_PARTICIPATION", "lot_identity": None})
        accounts[code].add(ref)


def add_memberships(graph, observations, accounts):
    selected_groups = {o.row.get("codigo_grupo") for o in observations
                       if o.row.get("codigo_grupo") in accounts or o.row.get("codigo_participante") in accounts}
    for observation in observations:
        row, ref = observation.row, observation.evidence
        if row.get("codigo_grupo") not in selected_groups:
            continue
        edge = membership(row)
        if edge is None:
            graph.issue("MEMBERSHIP_ENDPOINT_UNACCREDITED", {"row": ref.locator, "path": ref.path})
            continue
        group_code = row["codigo_grupo"]
        attributes = {"active_raw": row.get("esta_activo"),
                      "creation_date": row.get("fecha_creaci_n_grupo"),
                      "group_type": None, "membership_coverage": "PARTIAL"}
        # An exact cross-source witness is a reference, never node equality.
        if group_code in accounts:
            attributes["supplier_ref"] = "secop_supplier:" + group_code
            attributes["supplier_ref_evidence"] = [
                {"source": r.source, "path": r.path, "sha256": r.sha256,
                 "locator": r.locator, "observed_at": r.observed_at}
                for r in sorted(accounts[group_code] | {ref})]
        graph.add_node(Node("SUPPLIER_GROUP", edge["to"], group_code), ref, attributes)
        graph.add_node(Node("SECOP_SUPPLIER", edge["from"], row["codigo_participante"]), ref)
        graph.add_edge("MEMBER_OF_SUPPLIER_GROUP", edge["from"], edge["to"], [ref],
                       {k: v for k, v in edge.items() if k not in {"from", "to"}} | {
                           "valid_from": None, "valid_to": None})
