"""Deterministic schema compiler from adjudicated, bounded evidence."""
from __future__ import annotations

import hashlib

from app.core.procurement_relationships import DERIVED, EDGE_ENDPOINTS, NODE_FORMATS
from scripts.audits.relationship_foundation.edge_evidence import OUT
from scripts.audits.relationship_foundation.frozen_delivery import LOCAL_SOURCES, read_source_blob


def nodes():
    authorities = {
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
    attributes = {
        "TERRITORY": ["municipality", "department", "macro_region"],
        "INSTITUTION": ["knowledge_status", "product_buyer_unit", "territory_divipola", "family"],
        "BUYER_GROUP": ["version", "label"], "PROCESS": ["identity_field", "reason_codes"],
        "CONTRACT": ["identity_field", "reason_codes", "estado_contrato", "fecha_de_firma"],
        "SECOP_SUPPLIER": ["platform_code", "source", "observed_at"],
        "SUPPLIER_GROUP": ["active", "creation_date", "PLATFORM_SUPPLIER_CODE", "platform_reference_evidence"],
        "ACTOR": ["document_type", "base_number", "identity_provenance"], "PAA_ITEM": [],
    }
    result = {name: {"id_format": template, "authority": authorities[name],
                     "attributes": attributes[name], "materializable": name != "PAA_ITEM"}
              for name, template in NODE_FORMATS.items()}
    result["ACTOR"]["document_types"] = ["NIT", "CC", "UNKNOWN"]
    result["ACTOR"]["caveat"] = "UNKNOWN remains unknown document type; not legal-person certification. No supplier links."
    for name in ("PROCESS", "CONTRACT"):
        result[name]["stable_only"] = True
        result[name]["caveat"] = "Preserve stable_key verbatim, including reference fallback. Do not strengthen existing authority."
    result["INSTITUTION"]["caveat"] = "Only PRODUCT_RECOGNIZED may originate future productive transactional edges; other knowledge remains shadow."
    result["SECOP_SUPPLIER"]["caveat"] = "Exact nonzero ASCII integer text; no document fallback. Equality witnesses are bounded, not universal namespace proof."
    result["SUPPLIER_GROUP"]["caveat"] = "PLATFORM_SUPPLIER_CODE references secop_supplier:<code> only with exact ceth/hgi6 evidence; no type collapse."
    return result


def edge(endpoints, source, authority, attributes=None, coverage="PARTIAL", assertion="SUPPORTED"):
    return {"from_type": endpoints[0], "to_type": endpoints[1], "assertion_status": assertion,
            "coverage_status": coverage, "runtime_status": "SHADOW", "source": source,
            "identity_authority": authority, "cardinality": {"out": "0..n", "in": "0..n"},
            "attributes": attributes or {}, "known_caveats": ["Source observation with dated evidence; never infer absent edges."]}


def edges(evidence):
    definitions = {
        "IN_TERRITORY": (["territorial"], ["ADR-075", "ADR-080", "logical institution id -> DIVIPOLA"]),
        "SELECTED_IN_BUYER_GROUP": (["buyer_groups"], ["ADR-074", "exact versioned declared membership -> existing logical institution id"]),
        "PUBLISHED": (["p6dx-8zbt", "buyer_bindings"], ["exact dataset-aware buyer binding", "result_identity(p6dx-8zbt)"]),
        "PUBLISHED_CONTRACT": (["jbjy-vk9h", "buyer_bindings"], ["exact dataset-aware buyer binding", "result_identity(jbjy-vk9h)"]),
        "HAS_RECORDED_CONTRACT": (["p6dx-8zbt", "jbjy-vk9h"], [evidence["process_join"]["join"], "exactly one stable process endpoint per portfolio"]),
        "HAS_CONTRACTOR": (["jbjy-vk9h"], ["result_identity(contract) -> codigo_proveedor; recorded provider of this contract"]),
        "BID_ON": (["hgi6-6wh3", "p6dx-8zbt"], ["hgi6.id_procedimiento = p6dx.id_del_proceso", "hgi6.codigo_proveedor"]),
        "MEMBER_OF_SUPPLIER_GROUP": (["ceth-n4bn"], ["codigo_participante -> codigo_grupo in the same row"]),
    }
    result = {name: edge(EDGE_ENDPOINTS[name], *definition) for name, definition in definitions.items()}
    for name, definition in result.items():
        definition["assertion_kind"] = ("EXACT_SOURCE_JOIN" if name in {"BID_ON", "HAS_RECORDED_CONTRACT"}
                                        else "SOURCE_ROW_WITH_AUTHORITY_PROJECTION")
        definition["evidence_ref"] = "artifacts/audits/relationship_foundation_v0/edge_evidence.json"
    result["IN_TERRITORY"]["cardinality"]["out"] = "0..1"
    result["IN_TERRITORY"]["known_caveats"] = ["Affiliation, not headquarters or place of contract execution.", "ADR-080 SHADOW_NO_RUNTIME prevails; PRODUCTIVE_METADATA_ONLY is not a runtime status."]
    group = result["SELECTED_IN_BUYER_GROUP"]
    group["coverage_status"] = "COMPLETE"
    group["coverage_scope"] = "Declared versioned group; node projection requires existing logical IDs."
    group["existing_product_capability"] = "PRODUCTIVE declared BuyerGroup selection; this graph schema remains SHADOW."
    for name in ("PUBLISHED", "PUBLISHED_CONTRACT"):
        result[name]["institution_status_required"] = "PRODUCT_RECOGNIZED"
        result[name]["cardinality"]["in"] = "0..1"
        result[name]["known_caveats"] = ["Only unique exact accredited buyer bindings; exclude owner conflicts.", "Unmapped institutions cannot receive fabricated graph IDs."]
    result["PUBLISHED_CONTRACT"]["known_caveats"].append("nombre_entidad officially describes publisher, not signature; draft/cancelled rows exist.")
    pc = result["HAS_RECORDED_CONTRACT"]
    pc["cardinality"]["in"] = "0..1"
    pc["known_caveats"] = ["Portfolio reference is not process identity; reject portfolio mapping to multiple stable process IDs.", "Recorded association, not causal proof of award, signature or execution.", "Direct BDOS-to-REQ equality was falsified; no string replacement."]
    pc["assertion_status"] = "SUPPORTED" if evidence["process_contract_canaries"] >= 10 else "UNSUPPORTED"
    contractor = result["HAS_CONTRACTOR"]
    contractor["cardinality"]["out"] = "0..1"
    contractor["attributes"] = {"contract_status": "estado_contrato verbatim", "signature_date": "fecha_de_firma optional"}
    contractor["known_caveats"] = ["Provider recorded on a contract, including draft/cancelled records; not proof of signed obligation.", "Reject null, blank, noninteger and all-zero provider codes; Actor remains separate."]
    result["BID_ON"]["known_caveats"] = ["Observed process-level proponent, not necessarily a valid/evaluated offer; lot-level unsupported.", "Deduplicate process/code pairs; coverage uses explicit scoped population.", "No independent UI accuracy estimate is claimed."]
    result["MEMBER_OF_SUPPLIER_GROUP"]["attributes"] = {
        "participation_pct": "participacion; decimal [0,100], otherwise null with raw preserved",
        "is_leader": "es_lider_del_grupo; Verdadero/Falso, otherwise null with raw preserved"}
    result["MEMBER_OF_SUPPLIER_GROUP"]["known_caveats"] = ["Snapshot membership; no effective-time history inferred.", "Missing participant code cannot be replaced by NIT/name.", "Active and creation_date belong to group; membership percentage is not attributed contract revenue."]
    return result


def sources():
    result = {
        "territorial": {"path": "app/data/territorial_knowledge_v0.json", "authority": "ADR-075 / ADR-080"},
        "buyer_groups": {"path": "app/data/buyer_groups.json", "authority": "ADR-074"},
        "buyer_bindings": {"paths": ["app/data/buyer_source_names.json", "app/data/aliases_db.json"], "authority": "exact dataset projection"},
        "future_identity": {"authority": "SUPPLIER_TO_ACTOR_IDENTITY_AUTHORITY_REQUIRED"},
        "future_paa": {"authority": "DEFERRED_TO_PAA_AUDIT"},
    }
    result["local_digests"] = {path: hashlib.sha256(read_source_blob(path)).hexdigest() for path in LOCAL_SOURCES}
    for dataset in ("p6dx-8zbt", "jbjy-vk9h", "hgi6-6wh3", "ceth-n4bn"):
        path = OUT / "observations" / f"schema_{dataset}.json"
        result[dataset] = {"url": f"https://www.datos.gov.co/api/views/{dataset}.json",
                           "metadata_receipt_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    return result


def build(evidence, gate_values):
    deferred = {}
    for name, endpoint, source, debt in (
        ("REPRESENTS_ACTOR", ("SECOP_SUPPLIER", "ACTOR"), "future_identity", "SUPPLIER_TO_ACTOR_IDENTITY_AUTHORITY_REQUIRED"),
        ("PLANNED_AS", ("PAA_ITEM", "PROCESS"), "future_paa", "DEFERRED_TO_PAA_AUDIT"),
    ):
        deferred[name] = edge(endpoint, [source], [debt], coverage="UNKNOWN", assertion="UNSUPPORTED")
        deferred[name]["runtime_status"] = "DEFERRED"
    return {"schema_version": 0, "data_version": "PROCUREMENT_RELATIONSHIP_FOUNDATION_V0",
            "state": "SHADOW_FOUNDATION", "node_types": nodes(), "edge_types": edges(evidence),
            "derived_relations": {name: {"primary": False, "requirements": ["explicit temporal window", "metric definition", "coverage", "provenance"],
                                         "caveat": "Shared observed process/group is a structural co-occurrence; competition or partnership interpretation needs scope."}
                                  for name in sorted(DERIVED)},
            "deferred_relations": deferred, "source_authorities": sources(), "gates": gate_values}
