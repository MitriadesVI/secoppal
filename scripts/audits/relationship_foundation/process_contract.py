"""Exact contract/process joins with bounded, named denominators."""
from __future__ import annotations

from collections import Counter, defaultdict

from scripts.audits.relationship_foundation.edge_evidence import in_values
from scripts.audits.relationship_foundation.identity_spine import (
    COHORTS, buyer_names, platform_code, transaction_id,
)

P = "p6dx-8zbt"
C = "jbjy-vk9h"
WINDOW = "fecha_de_publicacion_del >= '2025-01-01T00:00:00' AND fecha_de_publicacion_del < '2026-09-07T00:00:00'"


def percent(n, denominator):
    return round(100 * n / denominator, 6) if denominator else None


def exact_processes(probe, label, keys):
    rows = []
    for offset in range(0, len(keys), 100):
        batch = probe.get(f"{label}_{offset}", P, {
            "$select": "id_del_proceso,referencia_del_proceso,entidad",
            "$where": in_values("id_del_proceso", keys[offset:offset + 100]),
            "$order": "id_del_proceso,entidad", "$limit": 10000})
        if len(batch) == 10000:
            raise ValueError("capped exact lookup cannot prove endpoint uniqueness")
        rows.extend(batch)
    return rows


def analyze(contracts, processes, join_field="id_del_proceso"):
    indexed = defaultdict(list)
    for row in processes:
        if transaction_id(row, P) and row.get(join_field):
            indexed[row[join_field]].append(row)
    edges, orphans, nulls, ambiguous = [], [], [], []
    owners = {key: {r.get("entidad") for r in rows} for key, rows in indexed.items()}
    identities = {key: {transaction_id(r, P) for r in rows} for key, rows in indexed.items()}
    contract_parents = defaultdict(set)
    for row in contracts:
        cid = transaction_id(row, C)
        pid = row.get("proceso_de_compra")
        if cid and isinstance(pid, str) and pid.strip():
            contract_parents[cid].add(pid)
    conflicting = {cid for cid, parents in contract_parents.items() if len(parents) > 1}
    for row in contracts:
        cid, pid = transaction_id(row, C), row.get("proceso_de_compra")
        if not cid or not isinstance(pid, str) or not pid.strip():
            nulls.append(row)
        elif (cid in conflicting or len(owners.get(pid, set())) > 1
              or len(identities.get(pid, set())) > 1):
            ambiguous.append(row)
        elif pid not in indexed:
            orphans.append(row)
        else:
            edges.append({"from": transaction_id(indexed[pid][0], P), "to": cid,
                          "process_key": indexed[pid][0].get("id_del_proceso"),
                          "join_key": pid, "contract_key": row.get("id_contrato"),
                          "supplier_code": platform_code(row.get("codigo_proveedor"))})
    unique = {(e["from"], e["to"]): e for e in edges}
    counts = Counter(e["from"] for e in unique.values())
    return {"sample_contract_rows": len(contracts), "matched_rows": len(edges),
            "join_rate_pct": percent(len(edges), len(contracts)),
            "distinct_matched_contracts": len({e["to"] for e in edges}),
            "distinct_matched_processes": len(counts),
            "one_to_many_processes": {p: n for p, n in counts.items() if n > 1},
            "many_to_one_anomalies": sorted(conflicting),
            "join_key_to_multiple_process_ids": {k: sorted(v) for k, v in identities.items() if len(v) > 1},
            "ambiguous_rows": ambiguous, "orphans": orphans, "null_or_unstable_rows": nulls,
            "edges": sorted(unique.values(), key=lambda e: (e["from"], e["to"])),
            "contractor_code_missing_rows": sum(platform_code(r.get("codigo_proveedor")) is None
                                                 for r in contracts)}


def audit(probe):
    result, all_processes, all_contracts = {}, {}, {}
    for label, canonical in COHORTS.items():
        pnames, pauth = buyer_names(canonical, P)
        cnames, cauth = buyer_names(canonical, C)
        processes = probe.get(label + "_processes", P, {
            "$select": "id_del_proceso,referencia_del_proceso,entidad",
            "$where": in_values("entidad", pnames) + " AND " + WINDOW,
            "$order": "id_del_proceso,entidad", "$limit": 10000})
        if len(processes) == 10000:
            processes = processes + probe.get(label + "_processes_page2", P, {
                "$select": "id_del_proceso,referencia_del_proceso,entidad",
                "$where": in_values("entidad", pnames) + " AND " + WINDOW,
                "$order": "id_del_proceso,entidad", "$offset": 10000, "$limit": 10000})
        contracts = probe.get(label + "_contracts", C, {
            "$select": "id_contrato,referencia_del_contrato,proceso_de_compra,nombre_entidad,codigo_proveedor,estado_contrato,fecha_de_firma",
            "$where": in_values("nombre_entidad", cnames),
            "$order": "id_contrato", "$limit": 250})
        keys = sorted({r["proceso_de_compra"] for r in contracts if r.get("proceso_de_compra")})
        parents = exact_processes(probe, label + "_contract_parents", keys)
        direct = analyze(contracts, parents)
        portfolios = []
        for start in range(0, len(keys), 100):
            batch = probe.get(f"{label}_portfolio_parents_{start}", P, {
                "$select": "id_del_proceso,referencia_del_proceso,id_del_portafolio,entidad",
                "$where": in_values("id_del_portafolio", keys[start:start + 100]),
                "$order": "id_del_portafolio,id_del_proceso,entidad", "$limit": 10000})
            if len(batch) == 10000:
                raise ValueError("capped portfolio lookup cannot prove a unique process")
            portfolios.extend(batch)
        summary = analyze(contracts, portfolios, "id_del_portafolio")
        summary["rejected_direct_id_join"] = {k: direct[k] for k in ("sample_contract_rows", "matched_rows", "join_rate_pct")}
        summary.update(buyer_unit=canonical, buyer_authorities=[pauth, cauth],
                       contract_sampling="first 250 ordered id_contrato, all source dates; not prevalence",
                       process_window=WINDOW, process_population_capped=len(processes) >= 20000,
                       buyer_mismatch_rows=[r for r in portfolios if r.get("entidad") not in pnames])
        result[label], all_processes[label], all_contracts[label] = summary, processes, contracts
    return {"join": "jbjy.proceso_de_compra = p6dx.id_del_portafolio -> result_identity(p6dx)",
            "condition": "portfolio must resolve to exactly one stable process ID; ambiguous rows excluded",
            "cohorts": result}, all_processes, all_contracts
