"""Computed identity/product isolation checks over actual evidence and source tree."""
import ast
import subprocess

from app.core.procurement_relationships import IDENTITY_GATES, PRODUCT_GATES
from scripts.audits.relationship_foundation.edge_evidence import ROOT
from scripts.audits.relationship_foundation.identity_spine import platform_code, transaction_id

BASE = "c79f8460c84ab26631c2534af106405cb16803f2"


def isolation(root=ROOT):
    imports = []
    for path in (root / "app").rglob("*.py"):
        if path.name == "procurement_relationships.py":
            continue
        text = path.read_text()
        ast.parse(text)
        if "procurement_relationship" in text:
            imports.append(str(path.relative_to(root)))
    changed = subprocess.check_output(["git", "diff", BASE, "--name-only"], cwd=root, text=True).splitlines()
    forbidden = [p for p in changed if p.startswith("app/") and p not in {
        "app/core/procurement_relationships.py", "app/data/procurement_relationship_registry_v0.json"}]
    owned = [root / "app/core/procurement_relationships.py",
             root / "scripts/audits/audit_relationship_foundation_v0.py",
             *(root / "scripts/audits/relationship_foundation").glob("*.py")]
    dependencies = set()
    for path in owned:
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                dependencies.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                dependencies.add(node.module.split(".")[0])
    forbidden_dependencies = sorted(dependencies & {"neo4j", "gqlalchemy", "pymgclient", "mgclient", "duckdb", "sqlite3", "pyarrow"})
    database_files = [str(p.relative_to(root)) for p in (root / "artifacts/audits/relationship_foundation_v0").rglob("*")
                      if p.suffix in {".db", ".sqlite", ".sqlite3", ".parquet", ".duckdb"}]
    return {"runtime_references": imports, "changed_existing_app_files": forbidden,
            "forbidden_storage_dependencies": forbidden_dependencies,
            "database_files": database_files,
            "base_sha": BASE}


def evaluate(spine, contracts, memberships, canaries, nodes):
    values = dict.fromkeys(IDENTITY_GATES + PRODUCT_GATES, 0)
    institution_rows = [r for r in spine["institutions"].values() if r]
    values["TERRITORY_AS_BUYER"] = sum(c["from"].startswith("territory:") for c in canaries["published"])
    values["NIT_AS_BUYER_UNIT_ID"] = sum(not r["institution_id"].startswith(("logical:", "m2b-logical:", "m2b1-logical:")) for r in institution_rows)
    values["NAME_AS_PRIMARY_NODE_ID"] = sum(r["institution_id"] == r["product_buyer_unit"] for r in institution_rows)
    values["NIT_AS_SECOP_SUPPLIER_ID"] = sum(e["to"] != "secop_supplier:" + r["codigo_proveedor"]
                                              for e, r in canaries["contractor_pairs"])
    values["SUPPLIER_CODE_AS_ACTOR_ID"] = int(nodes["ACTOR"]["authority"] != "app/core/actor_identity.py:IdentityKey")
    declared = {(m["institution_id"], spine["buyer_group"]["id"]) for m in spine["buyer_group"]["members"]}
    values["BUYER_GROUP_INFERRED_MEMBERSHIP"] = sum((e["from"], e["to"]) not in declared for e in canaries["buyer_group"])
    values["PROCESS_UNSTABLE_IDS_AS_NODES"] = sum(transaction_id(r, "p6dx-8zbt") is None for r in canaries["process_rows"])
    values["CONTRACT_UNSTABLE_IDS_AS_NODES"] = sum(transaction_id(r, "jbjy-vk9h") is None for _, r in canaries["contractor_pairs"])
    state = isolation()
    values["RUNTIME_IMPORTS"] = len(state["runtime_references"])
    for key in PRODUCT_GATES[1:]:
        values[key] = len(state["changed_existing_app_files"])
    evidence_gates = {
        "TERRITORIES_1103": spine["territories"] == 1103,
        "GROUP_LOGICAL_MAPPING_COMPLETE": spine["unmapped_group_members"] == 0,
        "PROCESS_CONTRACT_CANARIES_10": len({e["from"] for e in canaries["process_contract"]}) >= 10,
        "CONTRACTOR_CANARIES_10": len(canaries["contractor_pairs"]) >= 10,
        "MEMBERSHIP_GROUP_CANARIES_10": len(memberships["canaries"]) >= 10,
        "GROUPS_NOT_CAPPED": not memberships["canary_capture_capped"],
        "CONTRACT_CODES_REJECT_ZERO": platform_code("0") is None,
        "NO_GRAPH_DATABASE": not state["forbidden_storage_dependencies"] and not state["database_files"],
    }
    return {"identity_and_product": values, "evidence": evidence_gates,
            "isolation": state, "pass": not any(values.values()) and all(evidence_gates.values())}
