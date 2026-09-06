"""Receipt integrity and frozen-cohort integration, including negative evidence."""
from collections import Counter
import hashlib
import json

import pytest
import requests

from scripts.explore_procurement_layers import main
from scripts.procurement_layers.build import build
from scripts.procurement_layers.receipts import AUDIT, FrozenSources, ROOT


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline model attempted network I/O")
    monkeypatch.setattr(requests.sessions.Session, "request", forbidden)


@pytest.fixture
def sources(tmp_path):
    audit = tmp_path / AUDIT
    (audit / "observations").mkdir(parents=True)
    name = "baranoa_contracts.json"
    raw = (ROOT / AUDIT / "observations" / name).read_bytes()
    (audit / "observations" / name).write_bytes(raw)
    (audit / "observation_manifest.json").write_text(json.dumps({"receipts": {
        name: hashlib.sha256(raw).hexdigest()}}))
    return FrozenSources(tmp_path)


def test_manifest_and_source_hash_verified_before_materialization(sources):
    observations = sources.rows("baranoa_contracts", "jbjy-vk9h")
    assert len(observations) == 250
    assert observations[0].evidence.locator == "/payload/0"
    path = sources.root / AUDIT / "observations/baranoa_contracts.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="differs from frozen manifest"):
        sources.rows("baranoa_contracts", "jbjy-vk9h")


def test_unknown_dataset_missing_receipt_and_capped_uniqueness_fail_closed(sources):
    with pytest.raises(ValueError, match="dataset mismatch"):
        sources.rows("baranoa_contracts", "p6dx-8zbt")
    with pytest.raises(ValueError, match="absent from frozen manifest"):
        sources.rows("../../absent", "jbjy-vk9h")
    with pytest.raises(ValueError, match="capped lookup"):
        sources.rows("baranoa_contracts", "jbjy-vk9h", require_uncapped=True)
    with pytest.raises(ValueError, match="missing receipt family"):
        sources.matching("unavailable_", "jbjy-vk9h")


@pytest.mark.parametrize("field,value,message", [
    ("status", "FAILED", "non-affirmative"),
    ("error", "HTTP 403", "non-affirmative"),
    ("sha256", "0" * 64, "corrupt source payload"),
])
def test_nonaffirmative_or_corrupt_payload_is_rejected_even_with_matching_file_hash(sources, field, value, message):
    path = sources.root / AUDIT / "observations/baranoa_contracts.json"
    receipt = json.loads(path.read_text())
    receipt[field] = value
    path.write_text(json.dumps(receipt))
    sources.receipts[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match=message):
        sources.rows("baranoa_contracts", "jbjy-vk9h")


@pytest.mark.parametrize("cohort", ["baranoa", "barranquilla", "bucaramanga", "yumbo", "atlantico"])
def test_all_frozen_cohorts_keep_audited_contract_join_and_no_legal_inference(cohort):
    graph, scope = build(cohort)
    result = graph.export()
    nodes = {n["id"]: n for n in result["nodes"]}
    summary = json.loads((ROOT / AUDIT / "process_contract_join.json").read_text())["cohorts"][cohort]
    actual = {(e["from"], e["to"]) for e in result["edges"] if e["kind"] == "HAS_RECORDED_CONTRACT"}
    assert actual == {(e["from"], e["to"]) for e in summary["edges"]}
    assert all(e["from"] in nodes and e["to"] in nodes for e in result["edges"])
    assert all(e["runtime_status"] == "SHADOW" for e in result["edges"])
    assert not {"ACTOR", "PAA_ITEM"} & {n["kind"] for n in nodes.values()}
    assert scope["inputs"] and len(scope["manifest_sha256"]) == 64
    assert not any(e["kind"] in result["deferred_relations"] for e in result["edges"])
    if cohort == "baranoa":
        assert "territory:08078" in nodes
        assert len(actual) == 248
        assert Counter(e["kind"] for e in result["edges"])["BID_ON"] == 102
    if cohort == "atlantico":
        assert not any(n["kind"] == "INSTITUTION" for n in nodes.values())
        assert any(i["code"] == "BUYER_LOGICAL_ID_UNAVAILABLE" for i in result["issues"])


def test_cli_exact_neighborhood_output_and_no_overwrite(tmp_path, capsys):
    output = tmp_path / "layers.json"
    arguments = ["--cohort", "baranoa", "--node", "territory:08078", "--hops", "1", "--output", str(output)]
    assert main(arguments) == 0
    result = json.loads(output.read_text())
    assert len(result["nodes"]) == 2
    assert result["edges"][0]["kind"] == "IN_TERRITORY"
    assert result["view"]["counts_scope"] == "VIEW_ONLY"
    with pytest.raises(FileExistsError):
        main(arguments)
    assert json.loads(capsys.readouterr().out)["state"] == "SHADOW"


def test_unknown_cohort_never_falls_back_to_baranoa():
    with pytest.raises(ValueError, match="no frozen source authority"):
        build("unknown")


def test_supplier_relations_have_exact_row_witnesses_without_document_or_name_fallback():
    graph, _ = build("baranoa")
    payloads = {}
    for edge in graph.export()["edges"]:
        if edge["kind"] not in {"HAS_CONTRACTOR", "BID_ON", "MEMBER_OF_SUPPLIER_GROUP"}:
            continue
        for ref in edge["evidence"]:
            if ref["source"] == "p6dx-8zbt":
                continue
            if ref["path"] not in payloads:
                payloads[ref["path"]] = json.loads((ROOT / ref["path"]).read_text())["payload"]
            row = payloads[ref["path"]][int(ref["locator"].rsplit("/", 1)[1])]
            if edge["kind"] == "HAS_CONTRACTOR":
                assert edge["to"] == "secop_supplier:" + row["codigo_proveedor"]
                assert edge["from"] == "contract:jbjy-vk9h:" + row["id_contrato"]
            elif edge["kind"] == "BID_ON":
                assert edge["from"] == "secop_supplier:" + row["codigo_proveedor"]
                assert edge["to"] == "process:p6dx-8zbt:" + row["id_procedimiento"]
            else:
                assert edge["from"] == "secop_supplier:" + row["codigo_participante"]
                assert edge["to"] == "supplier_group:" + row["codigo_grupo"]
                assert edge["attributes"]["valid_from"] is None
