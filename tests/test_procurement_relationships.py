"""Offline authority, ambiguity, isolation and byte-replay invariants."""
from copy import deepcopy
from decimal import Decimal
import json
import shutil

from hypothesis import given, settings, strategies as st
import pytest

from app.core.actor_identity import IdentityKey
from app.core.procurement_relationships import (
    ASSERTIONS, COVERAGE, DATA_PATH, NODE_FORMATS, RUNTIME, deferred_edges,
    get_edge_type, get_node_type, load, supported_edges, validate,
)
from app.core.result_identity import result_identity
from scripts.audits.audit_relationship_foundation_v0 import run
from scripts.audits.relationship_foundation import build_registry, gates
from scripts.audits.relationship_foundation.edge_evidence import OUT, Probe, render
from scripts.audits.relationship_foundation.group_membership import membership
from scripts.audits.relationship_foundation.identity_spine import actor_id, platform_code, transaction_id
from scripts.audits.relationship_foundation.process_contract import analyze, exact_processes
from scripts.audits.relationship_foundation.supplier_codes import bid_summary


def test_schema_and_queries():
    registry = load()
    assert set(registry["node_types"]) == set(NODE_FORMATS)
    assert len(supported_edges(registry)) == 8
    assert set(deferred_edges(registry)) == {"REPRESENTS_ACTOR", "PLANNED_AS"}
    assert get_node_type("PAA_ITEM", registry)["materializable"] is False
    assert get_edge_type("REPRESENTS_ACTOR", registry)["runtime_status"] == "DEFERRED"
    assert get_node_type("NOT_A_NODE", registry) is None
    assert get_edge_type("RELATED_TO", registry) is None
    changed = get_node_type("ACTOR", registry)
    changed["document_types"].append("PLATFORM_CODE")
    assert registry["node_types"]["ACTOR"]["document_types"] == ["NIT", "CC", "UNKNOWN"]


@pytest.mark.parametrize("node", list(NODE_FORMATS))
def test_identity_template_cannot_be_replaced_by_name_or_nit(node):
    registry = load()
    registry["node_types"][node]["id_format"] = "nit:<name>"
    with pytest.raises(ValueError):
        validate(registry)


@pytest.mark.parametrize("field,vocabulary", [("assertion_status", ASSERTIONS), ("coverage_status", COVERAGE), ("runtime_status", RUNTIME)])
@settings(max_examples=30, derandomize=True)
@given(value=st.text(max_size=30))
def test_unknown_edge_status_never_defaults(field, vocabulary, value):
    if value in vocabulary:
        return
    registry = load()
    registry["edge_types"]["BID_ON"][field] = value
    with pytest.raises(ValueError):
        validate(registry)


@pytest.mark.parametrize("edge", ["REPRESENTS_ACTOR", "PLANNED_AS"])
def test_deferred_links_cannot_be_promoted(edge):
    registry = load()
    registry["deferred_relations"][edge]["assertion_status"] = "SUPPORTED"
    with pytest.raises(ValueError):
        validate(registry)


def test_platform_never_actor_and_membership_attributes_stay_on_edge():
    registry = load()
    registry["edge_types"]["HAS_CONTRACTOR"]["to_type"] = "ACTOR"
    with pytest.raises(ValueError):
        validate(registry)
    assert not {"participation_pct", "is_leader"} & set(load()["node_types"]["SECOP_SUPPLIER"]["attributes"])


@pytest.mark.parametrize("mutation", ["institution_authority", "coverage", "runtime", "edge_source", "membership_attribute", "gate_bool"])
def test_authority_and_promotion_mutations_fail_closed(mutation):
    registry = load()
    if mutation == "institution_authority":
        registry["node_types"]["INSTITUTION"]["authority"] = "NIT"
    elif mutation == "coverage":
        registry["edge_types"]["BID_ON"]["coverage_status"] = "COMPLETE"
    elif mutation == "runtime":
        registry["edge_types"]["IN_TERRITORY"]["runtime_status"] = "PRODUCTIVE"
    elif mutation == "edge_source":
        registry["edge_types"]["HAS_CONTRACTOR"]["source"] = ["future_identity"]
    elif mutation == "membership_attribute":
        registry["node_types"]["SECOP_SUPPLIER"]["attributes"].append("participation_pct")
    else:
        registry["gates"]["RUNTIME_IMPORTS"] = False
    with pytest.raises(ValueError):
        validate(registry)


@settings(max_examples=100, derandomize=True)
@given(value=st.one_of(st.none(), st.integers(), st.booleans(), st.text(max_size=30)), dataset=st.sampled_from(["p6dx-8zbt", "jbjy-vk9h", "unknown"]))
def test_graph_transaction_identity_exactly_delegates_to_authority(value, dataset):
    row = {"id_del_proceso": value, "id_contrato": value}
    authority = result_identity(row, dataset)
    node = transaction_id(row, dataset)
    if authority.identity_status == "unstable":
        assert node is None
    else:
        assert node.endswith(":" + authority.stable_key)
        assert dataset in node


def test_fallback_key_is_verbatim_and_namespaced():
    row = {"referencia_del_proceso": "  REF:ABC  ", "referencia_del_contrato": "  REF:ABC  "}
    assert transaction_id(row, "p6dx-8zbt") == "process:p6dx-8zbt:  REF:ABC  "
    assert transaction_id(row, "jbjy-vk9h") == "contract:jbjy-vk9h:  REF:ABC  "


@settings(max_examples=80, derandomize=True)
@given(code=st.text(alphabet="0123456789", min_size=1, max_size=20))
def test_platform_codes_preserve_text_and_document_types_remain_distinct(code):
    assert platform_code(code) == (None if set(code) == {"0"} else code)
    assert platform_code(" " + code) is None
    assert platform_code(int(code)) is None
    assert actor_id(IdentityKey("NIT", code)) != actor_id(IdentityKey("CC", code))
    assert actor_id(IdentityKey("UNKNOWN", code)) != "secop_supplier:" + code


@settings(max_examples=80, derandomize=True)
@given(percent=st.decimals(min_value=-1000, max_value=1000, allow_nan=False, allow_infinity=False))
def test_membership_percent_is_evidence_not_identity(percent):
    row = {"codigo_grupo": "123", "codigo_participante": "456", "participacion": str(percent)}
    edge = membership(row)
    assert (edge["from"], edge["to"]) == ("secop_supplier:456", "supplier_group:123")
    assert (edge["participation_pct"] is not None) == (0 <= percent <= 100)
    if edge["participation_pct"] is not None:
        assert Decimal(edge["participation_pct"]) == percent
    assert membership({**row, "codigo_participante": None, "nit_participante": "456"}) is None


def test_portfolio_ambiguity_abstains_and_one_to_many_is_retained():
    contracts = [{"id_contrato": "C1", "proceso_de_compra": "BDOS", "codigo_proveedor": "1"},
                 {"id_contrato": "C2", "proceso_de_compra": "BDOS", "codigo_proveedor": "2"}]
    process = {"id_del_proceso": "REQ1", "id_del_portafolio": "BDOS", "entidad": "buyer"}
    result = analyze(contracts, [process], "id_del_portafolio")
    assert result["matched_rows"] == 2
    assert list(result["one_to_many_processes"].values()) == [2]
    duplicate = analyze(contracts, [process, deepcopy(process)], "id_del_portafolio")
    assert duplicate["edges"] == result["edges"]
    conflict = analyze(contracts, [process, {**process, "id_del_proceso": "REQ2"}], "id_del_portafolio")
    assert not conflict["edges"] and len(conflict["ambiguous_rows"]) == 2
    assert analyze(contracts, [process])["matched_rows"] == 0
    two_parents = contracts + [{**contracts[0], "proceso_de_compra": "OTHER"}]
    conflict = analyze(two_parents, [process, {**process, "id_del_portafolio": "OTHER"}], "id_del_portafolio")
    assert conflict["many_to_one_anomalies"] == ["contract:jbjy-vk9h:C1"]
    assert all(e["to"] != "contract:jbjy-vk9h:C1" for e in conflict["edges"])


def test_bid_duplicates_do_not_inflate_competition_or_suppress_orphans():
    row = {"id_procedimiento": "P1", "codigo_proveedor": "123"}
    summary = bid_summary([row, row, {**row, "id_procedimiento": "orphan"}], [{"id_del_proceso": "P1"}], [])
    assert summary["duplicate_pair_excess"] == 1
    assert summary["orphans"] == ["orphan"]
    assert summary["single_bidder_canaries"] == ["P1"]
    assert summary["absence_means_zero"] is False


def test_capped_exact_lookup_cannot_prove_uniqueness():
    class CappedProbe:
        def get(self, *args, **kwargs):
            return [{"id_del_proceso": "P"}] * 10000
    with pytest.raises(ValueError, match="capped exact lookup"):
        exact_processes(CappedProbe(), "test", ["P"])


def test_missing_or_tampered_receipt_fails_closed(tmp_path):
    probe = Probe(directory=tmp_path)
    with pytest.raises(ValueError, match="missing frozen"):
        probe.get("missing", "p6dx-8zbt")
    receipt = json.loads((OUT / "observations/baranoa_processes.json").read_text())
    receipt["payload"] = []
    (tmp_path / "tampered.json").write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="corrupt observation"):
        probe.get("tampered", "p6dx-8zbt", receipt["params"])


def test_complete_offline_replay_canaries_and_runtime_isolation(monkeypatch):
    def no_network(*args, **kwargs):
        raise AssertionError("offline replay attempted network")
    monkeypatch.setattr("requests.get", no_network)
    artifacts, registry = run()
    assert render(registry) == DATA_PATH.read_bytes()
    for name, payload in artifacts.items():
        assert render(payload) == (OUT / name).read_bytes(), name
    assert artifacts["gates.json"]["pass"]
    assert not any(artifacts["gates.json"]["identity_and_product"].values())
    territorial = artifacts["canaries.json"]["territorial"]
    baranoa = next(e for e in territorial if e["institution"] == "ALCALDIA MUNICIPAL DE BARANOA")
    assert baranoa["to"] == "territory:08078" and baranoa["from"] != baranoa["to"]
    assert len(artifacts["canaries.json"]["buyer_group"]) == 38


def test_foundation_effects_use_exact_delivery_range_not_later_entity_changes(monkeypatch):
    assert gates.BASE == "c79f8460c84ab26631c2534af106405cb16803f2"
    assert gates.FOUNDATION_HEAD == "d608336a32553ac11a69924d663491ce852e1446"
    git_output = gates.subprocess.check_output
    later = git_output(
        ["git", "diff", gates.FOUNDATION_HEAD, "--name-only"],
        cwd=gates.ROOT, text=True,
    ).splitlines()
    assert "app/core/entity_profile/scope.py" in later
    assert "app/core/workflow/actions/execution.py" in later
    commands = []

    def record(command, **kwargs):
        commands.append(command)
        return git_output(command, **kwargs)

    monkeypatch.setattr(gates.subprocess, "check_output", record)
    assert gates.isolation()["changed_existing_app_files"] == []
    assert commands == [["git", "diff", gates.BASE, gates.FOUNDATION_HEAD, "--name-only"]]


@pytest.mark.parametrize("statement", [
    "import app.core.procurement_relationships",
    "from app.core.procurement_relationships import load",
])
def test_frozen_history_does_not_hide_a_current_runtime_consumer(tmp_path, monkeypatch, statement):
    # Mutate only a temporary current tree; historical Git reads use the real repo.
    for relative in (
        "app/core/procurement_relationships.py",
        "scripts/audits/audit_relationship_foundation_v0.py",
    ):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(gates.ROOT / relative, target)
    shutil.copytree(
        gates.ROOT / "scripts/audits/relationship_foundation",
        tmp_path / "scripts/audits/relationship_foundation",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    git_output = gates.subprocess.check_output

    def read_history(command, **kwargs):
        allowed = [["git", "diff", gates.BASE, gates.FOUNDATION_HEAD, "--name-only"]]
        allowed += [["git", "show", f"{gates.FOUNDATION_HEAD}:{path}"]
                    for path in build_registry.LOCAL_SOURCES]
        assert command in allowed
        return git_output(command, **{**kwargs, "cwd": gates.ROOT})

    monkeypatch.setattr(gates.subprocess, "check_output", read_history)
    current_isolation = gates.isolation
    monkeypatch.setattr(gates, "isolation", lambda: current_isolation(root=tmp_path))
    assert gates.isolation()["runtime_references"] == []
    consumer = tmp_path / "app/core/current_consumer.py"
    consumer.write_text(statement + "\n", encoding="utf-8")
    state = gates.isolation()
    assert state["changed_existing_app_files"] == []
    assert state["runtime_references"] == ["app/core/current_consumer.py"]
    with pytest.raises(ValueError, match="invalid procurement relationship contract: closed gates"):
        run()
