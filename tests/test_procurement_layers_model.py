"""Deterministic domain invariants: identity, evidence, cardinality and navigation."""
from copy import deepcopy

from hypothesis import given, settings, strategies as st
import pytest

from app.core.actor_identity import IdentityKey, IdentityMatch
from scripts.procurement_layers.actors import add_actor
from scripts.procurement_layers.model import Evidence, LayerGraph, Node


def evidence(source="jbjy-vk9h", row="/payload/0"):
    return Evidence(source, "observations.json", "a" * 64, row, "2026-09-06T00:00:00+00:00")


def contract_graph():
    graph = LayerGraph()
    graph.add_node(Node("CONTRACT", "contract:jbjy-vk9h:C1", "C1"), evidence())
    graph.add_node(Node("SECOP_SUPPLIER", "secop_supplier:123", "123"), evidence())
    return graph


@settings(max_examples=50, derandomize=True)
@given(code=st.text(alphabet="0123456789", min_size=1, max_size=25).filter(lambda s: set(s) != {"0"}))
def test_platform_types_never_collapse_or_lose_leading_zeros(code):
    supplier = Node("SECOP_SUPPLIER", "secop_supplier:" + code, code)
    group = Node("SUPPLIER_GROUP", "supplier_group:" + code, code)
    actor = Node("ACTOR", "actor:NIT:" + code, code)
    assert len({supplier.id, group.id, actor.id}) == 3
    assert supplier.id.removeprefix("secop_supplier:") == code


@pytest.mark.parametrize("code", ["0", "000", " 123", "123 ", "1.0", "1e8", "١٢٣", "", "-1"])
def test_invalid_platform_ids_are_not_materializable(code):
    with pytest.raises(ValueError):
        Node("SECOP_SUPPLIER", "secop_supplier:" + code, "label")


@pytest.mark.parametrize("kind", ["REPRESENTS_ACTOR", "PLANNED_AS", "COMPETES_WITH", "SIGNED"])
def test_deferred_derived_or_invented_relations_are_rejected(kind):
    with pytest.raises(ValueError, match="cannot be materialized"):
        contract_graph().add_edge(kind, "contract:jbjy-vk9h:C1", "secop_supplier:123", [evidence()])


def test_paa_is_declared_but_cannot_be_fabricated():
    with pytest.raises(ValueError, match="deferred"):
        Node("PAA_ITEM", "paa_item:123", "planned")
    snapshot = LayerGraph().export()
    assert len(snapshot["layers"]) == 6
    assert snapshot["layers"][-1]["status"] == "DEFERRED"
    assert snapshot["layers"][4]["status"] == "NO_OBSERVATIONS"
    assert snapshot["absence_means_zero"] is False


def test_edges_require_source_evidence_and_typed_existing_endpoints():
    graph = contract_graph()
    with pytest.raises(ValueError, match="source evidence"):
        graph.add_edge("HAS_CONTRACTOR", "contract:jbjy-vk9h:C1", "secop_supplier:123", [evidence("territorial")])
    with pytest.raises(ValueError, match="both endpoints"):
        graph.add_edge("HAS_CONTRACTOR", "contract:jbjy-vk9h:C1", "secop_supplier:999", [evidence()])
    with pytest.raises(ValueError, match="endpoint types"):
        graph.add_edge("HAS_CONTRACTOR", "secop_supplier:123", "contract:jbjy-vk9h:C1", [evidence()])


@settings(max_examples=40, derandomize=True)
@given(rows=st.lists(st.integers(min_value=0, max_value=10), min_size=1, max_size=30))
def test_duplicate_and_permuted_observations_preserve_identity_and_provenance(rows):
    def make(sequence):
        graph = contract_graph()
        for index in sequence:
            graph.add_edge("HAS_CONTRACTOR", "contract:jbjy-vk9h:C1", "secop_supplier:123",
                           [evidence(row=f"/payload/{index}")])
        return graph.export()
    result = make(rows)
    assert result == make(list(reversed(rows)) + rows)
    assert len(result["nodes"]) == 2 and len(result["edges"]) == 1
    assert len(result["edges"][0]["evidence"]) == len(set(rows))


def test_one_process_many_contracts_but_never_two_accredited_parents():
    graph = contract_graph()
    for code in ("P1", "P2"):
        graph.add_node(Node("PROCESS", "process:p6dx-8zbt:" + code, code), evidence("p6dx-8zbt"))
    graph.add_node(Node("CONTRACT", "contract:jbjy-vk9h:C2", "C2"), evidence())
    refs = [evidence(), evidence("p6dx-8zbt")]
    for code in ("C1", "C2"):
        graph.add_edge("HAS_RECORDED_CONTRACT", "process:p6dx-8zbt:P1", "contract:jbjy-vk9h:" + code, refs)
    with pytest.raises(ValueError, match="multiple accredited"):
        graph.add_edge("HAS_RECORDED_CONTRACT", "process:p6dx-8zbt:P2", "contract:jbjy-vk9h:C1", refs)
    assert len(graph.export()["edges"]) == 2


def test_conflicting_attributes_are_preserved_and_exports_do_not_mutate_graph():
    graph = contract_graph()
    attrs = {"is_leader": None, "raw": ["unknown"]}
    graph.add_edge("HAS_CONTRACTOR", "contract:jbjy-vk9h:C1", "secop_supplier:123", [evidence()], attrs)
    before = deepcopy(graph.export())
    attrs["raw"].append("changed")
    snapshot = graph.export()
    snapshot["edges"][0]["attributes"]["raw"].append("also changed")
    assert graph.export() == before


@pytest.mark.parametrize("hops", [-1, 4, True, 1.5, "1"])
def test_navigation_is_bounded_and_exact(hops):
    with pytest.raises(ValueError, match="hops"):
        contract_graph().neighborhood("secop_supplier:123", hops=hops)


def test_navigation_returns_observed_edges_without_inferring_new_relations():
    graph = contract_graph()
    graph.add_edge("HAS_CONTRACTOR", "contract:jbjy-vk9h:C1", "secop_supplier:123", [evidence()])
    snapshot = graph.neighborhood("secop_supplier:123", hops=0)
    assert len(snapshot["nodes"]) == 1 and snapshot["edges"] == []
    assert snapshot["layers"][2]["node_count"] == 0
    assert snapshot["layers"][2]["status"] == "NO_OBSERVATIONS"
    assert len(graph.neighborhood("secop_supplier:123")["edges"]) == 1
    with pytest.raises(ValueError, match="unknown node"):
        graph.neighborhood("123")


@settings(max_examples=40, derandomize=True)
@given(number=st.text(alphabet="0123456789", min_size=1, max_size=15))
def test_actor_adapter_preserves_document_types_and_never_links_accounts(number):
    graph = contract_graph()
    for document_type in ("NIT", "CC", "UNKNOWN"):
        match = IdentityMatch(IdentityKey(document_type, number), None, number,
                              "document_source", "undetermined")
        assert add_actor(graph, match, evidence("document_source")) == f"actor:{document_type}:{number}"
    result = graph.export()
    assert result["layers"][4]["node_count"] == 3
    assert result["edges"] == []


def test_actor_unresolved_and_source_mismatch_never_become_identity():
    graph = LayerGraph()
    match = IdentityMatch(None, None, "unknown", "document_source", "undetermined")
    assert add_actor(graph, match, evidence("document_source")) is None
    with pytest.raises(ValueError, match="same source"):
        add_actor(graph, match, evidence("different_source"))
    assert graph.export()["nodes"] == []


@pytest.mark.parametrize("key", ["BARANOA", "08078", "890100000", "logical:123"])
def test_buyer_identity_cannot_be_replaced_with_a_name_territory_or_nit(key):
    with pytest.raises(ValueError, match="accredited logical ID"):
        Node("INSTITUTION", "institution:" + key, "buyer")
