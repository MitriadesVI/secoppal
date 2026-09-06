"""Adapt existing identity authorities; do not resolve or promote institutions."""
from __future__ import annotations

from dataclasses import asdict
import json
import re

from app.core.actor_identity import IdentityKey
from app.core.buyer_source_names import project_buyer_names
from app.core.result_identity import result_identity
from app.core.territorial_knowledge import load
from scripts.audits.relationship_foundation.edge_evidence import ROOT

COHORTS = {
    "baranoa": "ALCALDIA MUNICIPAL DE BARANOA",
    "barranquilla": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
    "bucaramanga": "MUNICIPIO DE BUCARAMANGA",
    "yumbo": "MUNICIPIO DE YUMBO VALLE",
    "atlantico": "DEPARTAMENTO DEL ATLANTICO**",
}


def transaction_id(row, dataset):
    identity = result_identity(row, dataset)
    if identity.identity_status != "stable":
        return None
    prefix = {"p6dx-8zbt": "process", "jbjy-vk9h": "contract"}[dataset]
    return f"{prefix}:{dataset}:{identity.stable_key}"


def platform_code(value):
    """Exact ASCII integer text only; no numeric coercion, trim or NIT fallback."""
    return (value if isinstance(value, str) and re.fullmatch(r"[0-9]+", value)
            and set(value) != {"0"} else None)


def actor_id(key: IdentityKey):
    if not isinstance(key, IdentityKey):
        raise ValueError("Actor identity requires IdentityKey")
    return f"actor:{key.document_type}:{key.base_number}"


def buyer_names(canonical, dataset):
    projected = project_buyer_names(canonical, dataset)
    if projected:
        return list(projected), "app/data/buyer_source_names.json"
    aliases = json.loads((ROOT / "app/data/aliases_db.json").read_text())
    physical = {"p6dx-8zbt": "procesos", "jbjy-vk9h": "contratos"}[dataset]
    for item in aliases:
        if item["official_name"] == canonical and physical in item.get("source_datasets", []):
            return [canonical], "app/data/aliases_db.json:exact_dataset_canonical"
    raise ValueError(f"no exact dataset authority: {canonical}: {dataset}")


def spine():
    registry = load()
    group = next(g for g in json.loads((ROOT / "app/data/buyer_groups.json").read_text())["groups"]
                 if g["id"] == "bogota_executive")
    by_product = {i.product_buyer_unit: i for i in registry.institutions if i.product_buyer_unit}
    institutions = {label: asdict(by_product[name]) if name in by_product else None
                    for label, name in COHORTS.items()}
    members = [{"canonical_name": member["canonical_name"],
                "institution_id": ("institution:" + by_product[member["canonical_name"]].institution_id
                                   if member["canonical_name"] in by_product else None),
                "evidence": member["evidence"]} for member in group["members"]]
    return {"territories": len(registry.territories), "institutions": institutions,
            "buyer_group": {"id": "buyer_group:" + group["id"], "version": group["version"],
                            "members": members, "declared_count": len(members)},
            "unmapped_group_members": sum(m["institution_id"] is None for m in members),
            "rule": "Absent logical IDs remain absent; canonical names never become node IDs."}
