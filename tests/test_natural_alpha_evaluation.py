import json
from collections import Counter
from copy import deepcopy
from unittest.mock import Mock

import pytest

from app.core.natural_alpha.catalog import ACTION_ASPECTS
from app.core.natural_alpha.prompt import provider_request
from app.core.natural_alpha.provider import ProviderResult
from experiments.natural_alpha_v1.evaluation import (
    DIRECTORY, ROOT, evaluate, load_frozen, metrics, sha256,
)
from experiments.natural_alpha_v1.replay import RecordedProvider, ReplayError


def _synthetic_records(cases, gold):
    # Fabricated model state to exercise replay/evaluator, never live evidence.
    return [{
        "call_key": case["case_id"] + "::NATURAL_ALPHA_V1",
        "request": provider_request(case["raw_text"]),
        "payload": json.dumps({"jobs": [
            {"action": action, "aspect": aspect, "evidence_quote": case["raw_text"], "occurrence": 1}
            for action, aspect in gold["expected_jobs"][case["case_id"]]
        ]}),
        "provider_error": "",
    } for case in cases]


def test_corpus_catalog_and_frozen_artifacts():
    cases, gold, manifest = load_frozen()
    assert len(cases) == 40
    assert Counter(case["family"] for case in cases) == {
        "direct_search": 10, "analysis": 8, "actor_entity": 5, "result_follow": 7, "composite": 10,
    }
    for pairs in gold["expected_jobs"].values():
        assert 1 <= len(pairs) <= 3
        assert all(aspect in ACTION_ASPECTS[action] for action, aspect in pairs)
    assert manifest["timeout_seconds"] == 10
    assert gold["oracle_status"] == "PROVISIONAL_ORACLE"


def test_superseded_artifact_names_a_live_successor_that_pins_it():
    manifest = json.loads((DIRECTORY / "manifest.json").read_text())
    for name, note in manifest.get("superseded", {}).items():
        assert name in manifest["sha256"]
        assert note["reason"], f"MISSING_REASON:{name}"
        if not note.get("successor_manifest"):
            continue
        successor = ROOT / note["successor_manifest"]
        assert successor.is_file(), f"MISSING_SUCCESSOR:{note['successor_manifest']}"
        successor_manifest = json.loads(successor.read_text())
        pins = successor_manifest.get("sha256")
        if pins is None:
            # Historical V2 prompt-only schema remains readable.
            pins = {successor_manifest["alpha_prompt_file"]:
                    successor_manifest["alpha_prompt_file_sha256"]}
        assert pins[name] == sha256(ROOT / name), f"SUCCESSOR_SHA_STALE:{name}"
        frozen = successor_manifest.get("frozen_copies", {}).get(name)
        if frozen is not None:
            assert sha256(ROOT / frozen["path"]) == frozen["sha256"] == pins[name]


def test_fabricated_perfect_replay_zero_calls_and_all_records_consumed():
    cases = json.loads((DIRECTORY / "corpus.json").read_text())
    gold = json.loads((DIRECTORY / "gold.json").read_text())
    provider = RecordedProvider(_synthetic_records(cases, gold))
    report = evaluate(cases, gold, provider)
    provider.assert_consumed()
    assert report["provider_calls"] == 0
    assert report["completed"] == 40
    assert report["metrics"]["single_accuracy"] == 1
    assert report["metrics"]["multi_detection_recall"] == 1
    assert report["metrics"]["gates_pass"]


def test_request_exact_replay_rejects_prompt_and_text_drift_even_with_same_raw_text():
    record = {"call_key": "a", "request": provider_request("same raw"), "payload": '{"jobs": []}'}
    provider = RecordedProvider([record])
    changed = deepcopy(record["request"])
    changed["instructions"] += " drift"
    with pytest.raises(ReplayError, match="REQUEST_MISMATCH"):
        provider.observe("a", changed)
    with pytest.raises(ReplayError, match="MISSING_CALL_KEY"):
        provider.observe("b", record["request"])
    with pytest.raises(ReplayError, match="UNUSED_RECORDS"):
        provider.assert_consumed()
    assert provider.observe("a", record["request"]).provider_calls == 0
    with pytest.raises(ReplayError, match="REPEATED_CALL_KEY"):
        provider.observe("a", record["request"])
    with pytest.raises(ReplayError, match="DUPLICATE_CALL_KEY"):
        RecordedProvider([record, record])


def test_provider_contract_incompatible_stops_after_first_call():
    cases = json.loads((DIRECTORY / "corpus.json").read_text())
    gold = json.loads((DIRECTORY / "gold.json").read_text())
    provider = Mock(observe=Mock(return_value=ProviderResult(
        error="PROVIDER_CONTRACT_INCOMPATIBLE", provider_calls=1)))
    report = evaluate(cases, gold, provider)
    assert report["completed"] == report["provider_calls"] == 1
    assert report["stopped_reason"] == "PROVIDER_CONTRACT_INCOMPATIBLE"
    assert not report["metrics"]["gates_pass"]
    provider.observe.assert_called_once()


def test_metric_negative_controls_do_not_hide_false_blocks_or_missing_work():
    cases = json.loads((DIRECTORY / "corpus.json").read_text())
    gold = json.loads((DIRECTORY / "gold.json").read_text())
    provider = RecordedProvider(_synthetic_records(cases, gold))
    report = evaluate(cases, gold, provider)
    rows = deepcopy(report["results"])
    rows[0]["actual"].append(["FOLLOW", "SAVE"])
    rows[0]["exact"] = False
    assert not metrics(rows, complete=True)["gates"]["single_false_multi_at_most_2_5pct"]
    for row in rows[-2:]:
        row["actual"] = row["actual"][:1]
        row["exact"] = False
    assert not metrics(rows, complete=True)["gates"]["multi_recall_at_least_90pct"]
    rows[0]["status"] = "PROVIDER_ERROR"
    assert not metrics(rows, complete=True)["gates"]["provider_errors_zero"]
    assert not metrics(rows[:1], complete=False)["gates_pass"]
