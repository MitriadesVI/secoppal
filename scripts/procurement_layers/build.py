"""Compose one bounded cohort from frozen receipts; never call SECOP or Settings."""
from app.core.procurement_relationships import load
from scripts.audits.relationship_foundation.identity_spine import COHORTS
from scripts.procurement_layers.institutions import add_institution
from scripts.procurement_layers.model import LayerGraph
from scripts.procurement_layers.participation import add_bids, add_memberships
from scripts.procurement_layers.receipts import FrozenSources
from scripts.procurement_layers.transactions import C, P, add_transactions


def build(cohort="baranoa"):
    if cohort not in COHORTS:
        raise ValueError("cohort has no frozen source authority")
    sources = FrozenSources()
    load(sources.root / "app/data/procurement_relationship_registry_v0.json")
    graph = LayerGraph()
    canonical = COHORTS[cohort]
    iid = add_institution(graph, sources, canonical)
    processes = sources.rows(cohort + "_processes", P)
    if cohort + "_processes_page2.json" in sources.receipts:
        processes += sources.rows(cohort + "_processes_page2", P)
    parents = sources.matching(cohort + "_portfolio_parents_", P, require_uncapped=True)
    rechecks = [name[:-5] for name in sources.receipts if name.startswith(cohort + "_bid_recheck_")]
    for label in sorted(rechecks):
        processes += sources.rows(label, P, require_uncapped=True)
    contracts = sources.rows(cohort + "_contracts", C)
    bids = sources.rows(cohort + "_bids", "hgi6-6wh3")
    accounts = add_transactions(graph, sources, canonical, iid, processes, contracts, parents)
    add_bids(graph, bids, processes, accounts)
    memberships = sources.matching("bid_groups_", "ceth-n4bn")
    memberships += sources.rows("group_canaries_complete", "ceth-n4bn")
    add_memberships(graph, memberships, accounts)
    graph.issue("SUPPLIER_TO_ACTOR_IDENTITY_AUTHORITY_REQUIRED", {"status": "DEFERRED"})
    graph.issue("PAA_IDENTITY_AUTHORITY_REQUIRED", {"status": "DEFERRED"})
    scope = {"cohort": cohort, "buyer_unit": canonical,
             "contract_sampling": "First 250 ordered by id_contrato; all dates",
             "process_window": "2025-01-01 inclusive to 2026-09-07 exclusive, plus exact parent/recheck lookups",
             "group_sampling": "Only captured groups involving observed accounts; partial membership",
             "identity_scope": "Recorded source identities; no independent legal certification",
             "manifest_sha256": sources.manifest_sha256,
             "inputs": dict(sorted(sources.used.items()))}
    return graph, scope
