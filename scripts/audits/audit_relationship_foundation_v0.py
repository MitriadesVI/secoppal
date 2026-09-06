"""Offline by default. --capture permits capped official-source requests; --check writes nothing."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.procurement_relationships import validate
from scripts.audits.relationship_foundation import (
    build_registry, gates, group_membership, identity_spine, process_contract, report, supplier_codes,
)
from scripts.audits.relationship_foundation.edge_evidence import OUT, Probe, metadata, render, save


def run(capture=False):
    probe = Probe(live=capture)
    schemas = metadata(probe)
    spine = identity_spine.spine()
    joins, populations, contracts = process_contract.audit(probe)
    bids, bid_rows = supplier_codes.audit(probe, populations)
    groups, membership_rows = group_membership.audit(probe, {
        row["codigo_proveedor"] for row in bid_rows if identity_spine.platform_code(row.get("codigo_proveedor"))})
    namespace = supplier_codes.namespace_witnesses(
        bid_rows, [r for rows in contracts.values() for r in rows], membership_rows)
    canaries = report.canaries(probe, spine, populations, contracts, joins, bids)
    gate_result = gates.evaluate(spine, contracts, groups, canaries, build_registry.nodes())
    gate_result["evidence"].update(
        BID_MULTI_CANARIES_10=len(canaries["bid_multi"]) >= 10,
        BID_SINGLE_CANARIES=bool(canaries["bid_single"]),
        BID_POPULATIONS_NOT_CAPPED=not any(b["population_capped"] for b in bids.values()),
        GROUP_NAMESPACE_WITNESSES_10=len(namespace["witnesses"]["ceth_group_equals_hgi6_supplier"]) >= 10,
        BOGOTA_MULTIPLE_BUYERS=len(canaries["bogota_transaction_control"]["physical_buyers"]) >= 2,
    )
    gate_result["pass"] = not any(gate_result["identity_and_product"].values()) and all(gate_result["evidence"].values())
    evidence = {"process_join": joins, "bid_on": bids,
                "process_contract_canaries": len({e["from"] for e in canaries["process_contract"]}),
                "semantics": {ds: {c["fieldName"]: c.get("description") for c in schema["columns"]
                                   if c["fieldName"] in {"id_del_proceso", "id_del_portafolio", "proceso_de_compra",
                                                          "nombre_entidad", "entidad", "codigo_proveedor", "proveedor_adjudicado",
                                                          "codigo_participante", "codigo_grupo", "participacion", "es_lider_del_grupo"}}
                              for ds, schema in schemas.items()},
                "accuracy": "No independent UI accuracy estimate; assertions describe published source rows."}
    registry = build_registry.build(evidence, gate_result["identity_and_product"])
    validate(registry)
    receipts = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((OUT / "observations").glob("*.json"))}
    gate_result["verdict"] = "PASS" if gate_result["pass"] else "PARTIAL"
    return {"node_identity_contract.json": {"node_types": registry["node_types"], "spine": spine},
            "edge_evidence.json": evidence, "process_contract_join.json": joins,
            "supplier_namespace.json": namespace, "group_membership.json": groups,
            "edge_status_matrix.json": report.matrix(registry), "canaries.json": canaries,
            "gates.json": gate_result, "bid_on.json": bids,
            "observation_manifest.json": {"receipts": receipts, "request_budget_per_capture": 120,
                                          "row_cap_per_request": 10000, "byte_cap_per_response": 8000000,
                                          "timeout_seconds": 35, "warehouse": False,
                                          "historical_audit_sha": "b936d0f91974e40114c777cf041ea55fe3849955",
                                          "historical_audit_role": "hypothesis only; new receipts are authority"}}, registry


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.capture and args.check:
        parser.error("--check cannot perform live capture")
    artifacts, registry = run(args.capture)
    outputs = {OUT / name: data for name, data in artifacts.items()}
    outputs[ROOT / "app/data/procurement_relationship_registry_v0.json"] = registry
    if args.check:
        drift = [str(p.relative_to(ROOT)) for p, data in outputs.items()
                 if not p.exists() or p.read_bytes() != render(data)]
        if drift:
            raise SystemExit("BYTE_REPLAY_DRIFT: " + ", ".join(drift))
    else:
        for path, data in outputs.items():
            save(path, data)
    print(json.dumps({"verdict": artifacts["gates.json"]["verdict"], "byte_exact": args.check,
                      "runtime_effect": "ZERO", "graph_database": "NONE"}, sort_keys=True))
    return 0 if artifacts["gates.json"]["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
