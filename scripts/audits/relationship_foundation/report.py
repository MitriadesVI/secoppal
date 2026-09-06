"""Evidence canaries and concise status matrix; no product formatting."""
from scripts.audits.relationship_foundation.edge_evidence import in_values
from scripts.audits.relationship_foundation.identity_spine import buyer_names, platform_code, transaction_id


def canaries(probe, spine, populations, contracts, join, bids):
    published, territorial, process_rows, contractor_pairs = [], [], [], []
    for label, institution in spine["institutions"].items():
        if institution is None:
            continue
        iid = "institution:" + institution["institution_id"]
        territorial.append({"from": iid, "to": "territory:" + institution["territory_divipola"],
                            "institution": institution["product_buyer_unit"], "source": "ADR-080"})
        for row in populations[label][:10]:
            if (pid := transaction_id(row, "p6dx-8zbt")):
                published.append({"from": iid, "to": pid, "receipt": label + "_processes"})
                process_rows.append(row)
    for label, rows in contracts.items():
        for row in rows:
            cid = transaction_id(row, "jbjy-vk9h")
            code = platform_code(row.get("codigo_proveedor"))
            if cid and code:
                contractor_pairs.append(({"from": cid, "to": "secop_supplier:" + code,
                                          "receipt": label + "_contracts"}, row))
    names = sorted({n for member in spine["buyer_group"]["members"]
                    for n in buyer_names(member["canonical_name"], "p6dx-8zbt")[0]})
    bogota = probe.get("bogota_group_processes", "p6dx-8zbt", {
        "$select": "id_del_proceso,referencia_del_proceso,entidad",
        "$where": in_values("entidad", names)
                  + " AND fecha_de_publicacion_del >= '2026-08-01T00:00:00' AND fecha_de_publicacion_del < '2026-09-01T00:00:00'",
        "$order": "id_del_proceso,entidad", "$limit": 10000})
    return {"territorial": territorial, "published": published, "process_rows": process_rows,
            "buyer_group": [{"from": m["institution_id"], "to": spine["buyer_group"]["id"]}
                            for m in spine["buyer_group"]["members"] if m["institution_id"]],
            "bogota_transaction_control": {"rows": len(bogota), "capped": len(bogota) == 10000,
                                           "physical_buyers": sorted({r["entidad"] for r in bogota}),
                                           "scope": "declared 38 members, August 2026; exact dataset-aware OR"},
            "process_contract": [e for cohort in join["cohorts"].values() for e in cohort["edges"][:10]],
            "contractor_pairs": contractor_pairs[:20],
            "bid_multi": [c for summary in bids.values() for c in summary["multi_bidder_canaries"]],
            "bid_single": [c for summary in bids.values() for c in summary["single_bidder_canaries"]],
            "sampling": "Deterministic bounded canaries, never national prevalence estimates."}


def matrix(registry):
    return {name: {k: edge[k] for k in ("from_type", "to_type", "assertion_status", "coverage_status", "runtime_status")}
            for name, edge in (registry["edge_types"] | registry["deferred_relations"]).items()}
