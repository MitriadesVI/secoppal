"""C4A.2a — harness integrity: per-run scoring, computed gates, honest metrics.

No network, no real API: a scripted provider plus synthetic inputs for the gate
functions. Each hard gate must be able to go red, and the utility metric must not
be inflatable by a wrong capability reaching READY.
"""
import importlib.util
import json
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
EXPERIMENT = ROOT / "experiments" / "natural_job_prelegacy_v2"


def _harness():
    spec = importlib.util.spec_from_file_location(
        "c4a2_alpha_v3", EXPERIMENT / "alpha_v3_evaluation.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


harness = _harness()


def _case(case_id, owner, outcome="EXECUTE", buyer=None, forbid=(), raw=None):
    return {"case_id": case_id, "owner": owner, "outcome": outcome, "buyer": buyer,
            "forbid": list(forbid), "raw_text": raw or f"{case_id} proveedores de galapa"}


def _verdict(case_id, owner, suppliers=False):
    return {"case_id": case_id, "owner": owner, "observed": ["ANALYZE/SUPPLIERS"] if suppliers else [],
            "status": "MATCHED" if suppliers else "SKIPPED",
            "suppliers_true_positive": suppliers and owner == "SUPPLIERS",
            "suppliers_false_positive": suppliers and owner != "SUPPLIERS",
            "suppliers_false_negative": not suppliers and owner == "SUPPLIERS",
            "not_suppliers_correct": not suppliers and owner == "NOT_SUPPLIERS",
            "multi_job_correct": False,
            "exact_job_match": suppliers if owner == "SUPPLIERS" else None}


def _dry(case_id, ready, buyer=None, credential=True):
    return {"case_id": case_id, "credential_valid": credential, "status": "READY" if ready else "CLARIFY",
            "reason": "accredited" if ready else "x", "ready": ready, "overreach": False,
            "evidence_consumes_residual": True, "residual_material": [], "buyer": buyer,
            "fecha_desde": None, "ordering_signal": None}


# ── 1. runs are scored separately ────────────────────────────────────────────

def test_each_run_is_scored_on_its_own(tmp_path):
    cases = [_case("U01", "SUPPLIERS", buyer="galapa")]
    good = json.dumps({"jobs": [{"action": "ANALYZE", "aspect": "SUPPLIERS",
                                 "evidence_quote": "proveedores", "occurrence": 1}]})
    bad = json.dumps({"jobs": [{"action": "SEARCH", "aspect": "CONTRACTS",
                                "evidence_quote": "proveedores", "occurrence": 1}]})
    provider = harness._StubProvider(payloads={("0", "U01"): good, ("1", "U01"): bad})
    settings = _settings(tmp_path)
    result = harness.run_dataset("dev", cases, provider, settings, *_resolvers(settings), _adjudication())

    assert [run["run"] for run in result["runs"]] == [0, 1]
    assert result["runs"][0]["summary"]["classifier"]["suppliers_true_positive"] == 1
    assert result["runs"][1]["summary"]["classifier"]["suppliers_false_negative"] == ["U01"]
    assert result["runs"][1]["summary"]["classifier"]["suppliers_true_positive"] == 0
    assert result["stability"]["unstable_labels"] == ["U01"]


def test_unstable_evidence_intervals_are_reported_separately(tmp_path):
    cases = [_case("U01", "SUPPLIERS", buyer="galapa")]
    first = json.dumps({"jobs": [{"action": "ANALYZE", "aspect": "SUPPLIERS",
                                  "evidence_quote": "galapa", "occurrence": 1}]})
    second = json.dumps({"jobs": [{"action": "ANALYZE", "aspect": "SUPPLIERS",
                                   "evidence_quote": "proveedores", "occurrence": 1}]})
    provider = harness._StubProvider(payloads={("0", "U01"): first, ("1", "U01"): second})
    settings = _settings(tmp_path)
    result = harness.run_dataset("dev", cases, provider, settings, *_resolvers(settings), _adjudication())

    assert result["stability"]["unstable_labels"] == []
    assert result["stability"]["unstable_evidence_intervals"] == ["U01"]


# ── 2. each hard gate can go red (synthetic inputs, no resolver needed) ───────

def test_wrong_capability_reaches_ready_gate():
    cases = [_case("O02", "NOT_SUPPLIERS")]
    gates = harness.hard_gates(cases, [_verdict("O02", "NOT_SUPPLIERS")], [_dry("O02", True)])
    assert gates["WRONG_CAPABILITY_REACHES_READY"] == ["O02"]
    quiet = harness.hard_gates(cases, [_verdict("O02", "NOT_SUPPLIERS")], [_dry("O02", False)])
    assert quiet["WRONG_CAPABILITY_REACHES_READY"] == []


@pytest.mark.parametrize("case,run,gate", [
    (_case("U01", "SUPPLIERS", buyer="galapa"), _dry("U01", True, buyer="MUNICIPIO DE BARRANQUILLA"),
     "WRONG_BUYER"),
    (_case("D01", "SUPPLIERS", outcome="HONEST_CLARIFY", buyer="galapa"),
     _dry("D01", True, buyer="MUNICIPIO DE GALAPA"), "SILENT_FILTER_LOSS"),
    (_case("A01", "NOT_SUPPLIERS", forbid=["ACTOR_STOLEN"]),
     _dry("A01", True, buyer="MUNICIPIO DE GALAPA"), "ACTOR_STOLEN"),
    (_case("N01", "NOT_SUPPLIERS", forbid=["NEGATED_JOB_EXECUTED"]),
     _dry("N01", True, buyer="MUNICIPIO DE GALAPA"), "NEGATED_JOB_EXECUTED"),
    (_case("C01", "SUPPLIERS", buyer="galapa", forbid=["SILENT_CARDINALITY_SWAP"]),
     _dry("C01", True, buyer="MUNICIPIO DE GALAPA"), "UNSUPPORTED_CARDINALITY_EXECUTED"),
])
def test_every_execution_gate_can_go_red(case, run, gate):
    gates = harness.hard_gates([case], [_verdict(case["case_id"], case["owner"], suppliers=True)],
                               [run])
    assert gates[gate] == [case["case_id"]], gates
    assert gates["WRONG_BUYER"] == [] if gate != "WRONG_BUYER" else True
    # A clarifying turn trips nothing: gates only describe executions.
    assert harness.hard_gates([case], [_verdict(case["case_id"], case["owner"])],
                              [_dry(case["case_id"], False)])[gate] == []


# ── 3. READY metric: P04 out of SUPPORTED_V2 only, C02 inside ────────────────

def test_p04_leaves_supported_but_stays_visible_in_raw():
    cases = json.loads((EXPERIMENT / "corpus_dev.json").read_text(encoding="utf-8"))
    picks = harness.candidates(cases, _adjudication())

    assert "P04" in picks["raw"] and "P04" not in picks["supported"]
    assert picks["out_of_supported"] == ["P04"]
    assert "C02" in picks["supported"] and "C02" in picks["raw"]
    assert len(picks["raw"]) - len(picks["supported"]) == 1


def test_not_suppliers_ready_never_inflates_supported_ready():
    cases = [_case("U07", "SUPPLIERS", buyer="galapa"), _case("O02", "NOT_SUPPLIERS")]
    annotated = [_verdict("U07", "SUPPLIERS", suppliers=True), _verdict("O02", "NOT_SUPPLIERS")]
    dry = [_dry("U07", True, buyer="MUNICIPIO DE GALAPA"), _dry("O02", True, buyer="MUNICIPIO DE GALAPA")]
    summary = harness.summarise(cases, annotated, dry, _adjudication(), "dev")

    assert summary["ready"]["supported_ready"] == ["U07"]
    assert summary["ready"]["supported_total"] == 1
    assert summary["ready"]["not_suppliers_ready"] == ["O02"]
    assert summary["dry_run"]["hard_gates"]["WRONG_CAPABILITY_REACHES_READY"] == ["O02"]


# ── 4. the model's own proposal is recorded ──────────────────────────────────

def test_parsed_jobs_report_the_payload_quote(tmp_path):
    cases = [_case("U01", "SUPPLIERS", buyer="galapa", raw="proveedores de galapa")]
    payload = json.dumps({"jobs": [{"action": "ANALYZE", "aspect": "SUPPLIERS",
                                    "evidence_quote": "de galapa", "occurrence": 1}]})
    settings = _settings(tmp_path)
    record = harness.collect(cases[0], harness._StubProvider(payloads={"U01": payload}), 0, settings)

    assert record["parsed_jobs"] == [{"action": "ANALYZE", "aspect": "SUPPLIERS",
                                      "evidence_quote": "de galapa", "occurrence": 1}]
    assert record["grounded_jobs"][0]["evidence_quote"] == "de galapa"
    assert record["grounded_jobs"][0]["evidence_interval"] == [12, 21]  # normalized span of "de galapa"


# ── 6. credential fail-closed ────────────────────────────────────────────────

def test_missing_credential_is_fail_closed(tmp_path):
    settings = _settings(tmp_path)
    assert settings.deepseek_api_key is None
    with pytest.raises(SystemExit, match="PROVIDER_CREDENTIAL_ABSENT"):
        harness.require_credential(settings)


def _settings(tmp_path):
    from app.config import Settings

    return Settings(_env_file=None, secoppal_a2_suppliers_binding_enabled=True,
                    secop_feedback_path=tmp_path / "traces.jsonl",
                    secop_feedback_db_path=tmp_path / "feedback.db")


def _resolvers(settings):
    from app.core.entity_resolver import EntityResolver

    return EntityResolver(settings.secop_alias_db_path), None  # actor resolver not needed here


def _adjudication():
    return json.loads((EXPERIMENT / "oracle_adjudication.json").read_text(encoding="utf-8"))
