"""C4A.2 harness checks: the adjudication is consistent and the frozen files did not move."""
import hashlib
import json
from pathlib import Path

EXPERIMENT = Path(__file__).resolve().parents[1] / "experiments" / "natural_job_prelegacy_v2"


def _artifact(name):
    return json.loads((EXPERIMENT / name).read_text(encoding="utf-8"))


def test_adjudication_is_documented_and_the_corpus_kept_its_original_oracle():
    adjudication = _artifact("oracle_adjudication.json")
    corpus = {case["case_id"]: case for case in _artifact("corpus_dev.json")}
    entries = {entry["case_id"]: entry for entry in adjudication["adjudications"]}

    assert entries["P04"]["adjudication"] == "OUT_OF_SUPPORTED_V2"
    assert entries["P04"]["original_oracle"] == "EXECUTE"
    assert entries["P04"]["runtime_behaviour"] == "CLARIFY / buyer_not_accredited"
    # The corpus was not edited: P04 still carries its original oracle.
    assert corpus["P04"]["outcome"] == "EXECUTE"
    assert corpus["P04"]["raw_text"] == entries["P04"]["raw"]


def test_raw_and_supported_metrics_are_reported_separately():
    metrics = _artifact("oracle_adjudication.json")["metrics"]

    raw, supported = metrics["RAW_CLEAR_READY"], metrics["SUPPORTED_V2_READY"]
    assert (raw["ready"], raw["total"], raw["rate"]) == (11, 13, 0.8462)
    assert (supported["ready"], supported["total"], supported["rate"]) == (11, 12, 0.9167)
    assert raw["total"] - supported["total"] == 1  # exactly the adjudicated case
    assert "P04" in raw["misses"] and "P04" not in supported["misses"]
    assert supported["misses"] == ["C02"]
    assert metrics["C02"]["repaired_in_this_checkpoint"] is False


def test_frozen_artifacts_match_the_manifest_hashes():
    manifest = _artifact("manifest.json")
    expected = {
        "corpus_dev.json": manifest["corpus_dev"]["sha256"],
        "prompt_v3.frozen.txt": manifest["alpha_prompt_file_sha256"],
        "holdout.json": manifest["holdout"]["sha256"],
    }
    for name, digest in expected.items():
        assert hashlib.sha256((EXPERIMENT / name).read_bytes()).hexdigest() == digest, name
