"""Observed bidder edges and cross-source platform-code witnesses."""
from collections import Counter, defaultdict

from scripts.audits.relationship_foundation.edge_evidence import in_values
from scripts.audits.relationship_foundation.identity_spine import COHORTS, buyer_names, platform_code
from scripts.audits.relationship_foundation.process_contract import P, exact_processes, percent

H = "hgi6-6wh3"


def bid_summary(rows, population, rechecked):
    population_ids = {r["id_del_proceso"] for r in population if r.get("id_del_proceso")}
    known = population_ids | {r["id_del_proceso"] for r in rechecked if r.get("id_del_proceso")}
    keys = [(r.get("id_procedimiento"), platform_code(r.get("codigo_proveedor"))) for r in rows]
    valid = [(p, s) for p, s in keys if isinstance(p, str) and p.strip() and s]
    bids = {p for p, _ in valid}
    counts = Counter(valid)
    distinct = set(valid)
    bidder_counts = Counter(p for p, _ in distinct if p in known)
    matches = bids & known
    return {"physical_rows": len(rows), "valid_rows": len(valid), "invalid_rows": len(rows) - len(valid),
            "distinct_pairs": len(distinct), "duplicate_pair_excess": len(valid) - len(distinct),
            "duplicate_pair_rate_pct": percent(len(valid) - len(distinct), len(valid)),
            "duplicate_pair_examples": [{"process": p, "supplier": s, "rows": n}
                                        for (p, s), n in counts.items() if n > 1],
            "bid_processes": len(bids), "matched_processes": len(matches),
            "process_join_rate_pct": percent(len(matches), len(bids)),
            "process_population": len(population_ids),
            "matched_inside_population": len(bids & population_ids),
            "process_coverage_pct": percent(len(bids & population_ids), len(population_ids)),
            "orphans": sorted(bids - known),
            "outside_population_exact_matches": sorted(matches - population_ids),
            "multi_bidder_canaries": [{"process": p, "suppliers": sorted(s for q, s in distinct if q == p)}
                                      for p, n in sorted(bidder_counts.items()) if n > 1][:10],
            "single_bidder_canaries": sorted(p for p, n in bidder_counts.items() if n == 1)[:10],
            "lot_level": "UNSUPPORTED", "absence_means_zero": False}


def audit(probe, populations):
    summaries, all_rows = {}, []
    for label, canonical in COHORTS.items():
        names, _ = buyer_names(canonical, P)
        rows = probe.get(label + "_bids", H, {
            "$select": "id_procedimiento,codigo_proveedor,entidad_compradora",
            "$where": in_values("entidad_compradora", names)
                      + " AND fecha_publicaci_n >= '2025-01-01T00:00:00'"
                      + " AND fecha_publicaci_n < '2026-09-07T00:00:00'",
            "$order": "id_procedimiento,codigo_proveedor", "$limit": 10000})
        known = {r.get("id_del_proceso") for r in populations[label]}
        missing = sorted({r["id_procedimiento"] for r in rows if r.get("id_procedimiento")} - known)
        suffix = "_bid_recheck_full_population" if len(populations[label]) > 10000 else "_bid_recheck"
        rechecked = exact_processes(probe, label + suffix, missing)
        summary = bid_summary(rows, populations[label], rechecked)
        summary["population_capped"] = len(populations[label]) >= 20000 or len(rows) == 10000
        summary["scope"] = "exact buyer source names; 2025-01-01 inclusive to 2026-09-07 exclusive"
        summaries[label] = summary
        all_rows.extend(rows)
    return summaries, all_rows


def namespace_witnesses(bids, contracts, memberships):
    bid_codes = {platform_code(r.get("codigo_proveedor")) for r in bids} - {None}
    contract_codes = {platform_code(r.get("codigo_proveedor")) for r in contracts} - {None}
    groups = {platform_code(r.get("codigo_grupo")) for r in memberships} - {None}
    members = {platform_code(r.get("codigo_participante")) for r in memberships} - {None}
    witnesses = {
        "hgi6_supplier_equals_jbjy_supplier": sorted(bid_codes & contract_codes),
        "ceth_group_equals_hgi6_supplier": sorted(groups & bid_codes),
        "ceth_participant_equals_hgi6_supplier": sorted(members & bid_codes),
        "ceth_participant_equals_jbjy_supplier": sorted(members & contract_codes),
    }
    return {"assertion": "EXACT_OBSERVED_CODE_COMPATIBILITY", "witnesses": witnesses,
            "universal_namespace_equivalence_proven": False,
            "normalization": "ASCII integer strings verbatim; no float/int conversion or zero stripping",
            "group_reference": "PLATFORM_SUPPLIER_CODE; resolve secop_supplier:<code> only with exact ceth/hgi6 witness",
            "group_reference_canaries": [{"group_id": "supplier_group:" + code,
                                          "PLATFORM_SUPPLIER_CODE": code,
                                          "supplier_ref": "secop_supplier:" + code,
                                          "authority": "exact ceth.codigo_grupo = hgi6.codigo_proveedor"}
                                         for code in sorted(groups & bid_codes)[:20]],
            "type_collapse": False, "supplier_actor_link": "DEFERRED"}
