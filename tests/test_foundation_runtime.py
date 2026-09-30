"""Foundation composition controls at the actual source-call boundary."""

from __future__ import annotations

from dataclasses import replace
from copy import deepcopy
from datetime import datetime

import pytest
from hypothesis import given, strategies as st

from app.core.entity_resolver import EntityResolver
from app.core.foundation_admission import admit_proposal
from app.core.foundation_guard import ExecutionGuard, FoundationGuardError, execution_guard
from app.core.foundation_producers import LinguisticProducer, configured_producer, decode_proposal, host_candidates
from app.core.foundation_projection import project_accepted
from app.core.interpretation_contract import AcceptanceStatus
from app.core.opportunity_retrieval_policy import REFERENCE_INSTANT_PARAM
from app.core.source_call_receipts import begin_source_calls, end_source_calls, last_call_id, source_call_scope
from app.core.soql_builder import SoQLBuilder
from tests.product_contract.frozen_source import FrozenTransport, InjectedFailure, frozen_client, soql_to_sql
from tests.product_contract.harness import ProductHarness


RAW = "Busca procesos de aseo"
NOW = datetime.fromisoformat("2026-09-26T12:00:00-05:00")
DATASET = "p6dx-8zbt"
ROWS = {DATASET: [{"id_del_proceso": "P1", "nombre_del_procedimiento": "aseo",
                   "descripci_n_del_procedimiento": None, "fecha_de_publicacion_del": "2026-09-01",
                   "precio_base": "100", "estado_del_procedimiento": "Publicado"}]}
ROWS[DATASET][0]["justificaci_n_modalidad_de"] = None  # Explicit nullable column in this synthetic source.


def payload(raw=RAW):
    noun, topic = raw.index("procesos"), raw.index("aseo")
    return {"requests": [{"request_id": "q1", "scope_id": "q1", "operation": "SEARCH",
                          "subject": "processes", "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                          "universe_evidence": {"start": noun, "end": noun + 8, "quote": "procesos"},
                          "condition_expression": {"operator": "ATOM", "condition_id": "q1.c1"}}],
            "conditions": [{"condition_id": "q1.c1", "request_ids": ["q1"], "dimension": "TOPIC",
                            "operator": "contains", "polarity": "POSITIVE",
                            "evidence": [{"start": topic, "end": topic + 4, "quote": "aseo"}]}]}


def as_b(body, raw=RAW):
    """The same meaning in the B wire shape: every span becomes its host candidate id."""
    ids = {(c["start"], c["end"]): c["candidate_id"] for c in host_candidates(raw)}
    body = deepcopy(body)
    for request in body["requests"]:
        request["candidate_ids"] = [ids[(s["start"], s["end"])] for s in request.pop("evidence")]
        span = request.pop("universe_evidence")
        request["universe_evidence_candidate_ids"] = [ids[(span["start"], span["end"])]]
    for condition in body["conditions"]:
        condition["candidate_ids"] = [ids[(s["start"], s["end"])] for s in condition.pop("evidence")]
    return body


def accepted(raw=RAW):
    return admit_proposal(decode_proposal(payload(raw), raw, arm="C"), raw,
                          entity_resolver=EntityResolver("app/data/aliases_db.json"))


def test_foundation_publication_preserves_exact_source_fields_and_legacy_normalization(tmp_path, monkeypatch):
    rows = {DATASET: [{**ROWS[DATASET][0], "precio_base": "1000000001"}]}
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows,
                             reference_instant=NOW, today=NOW.date(),
                             foundation_producer=lambda raw: accepted(raw),
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    turn = harness.say(RAW)
    source = harness.transport.snapshot()[0]["result"]
    assert turn.result["groups"][0]["rows"] == turn.result["rows"] == turn.result["results"] == source
    assert source[0]["precio_base"] == "1000000001"
    assert "urlproceso" not in source[0]
    legacy = frozen_client(FrozenTransport(rows)).query(DATASET, "SELECT id_del_proceso, precio_base")
    assert legacy[0]["precio_base"] == 1000000001.0
    assert legacy[0]["urlproceso"]


def scoped(manifest, **extra):
    return source_call_scope(request_id=manifest.request_id, scope_id=manifest.scope_id,
                             interpretation_hash=manifest.interpretation_hash,
                             plan_hash=manifest.plan_hash, **extra)


@pytest.mark.parametrize("mutation", ["dataset", "soql", "scope", "hash"])
def test_changed_effective_binding_blocks_before_transport(mutation):
    outcome = project_accepted(accepted(), reference_instant=NOW)
    guard, transport = ExecutionGuard(outcome, RAW, reference_instant=NOW), FrozenTransport(ROWS)
    query = outcome.manifest[0]
    dataset, soql = query.dataset, query.soql
    if mutation == "dataset":
        dataset = "jbjy-vk9h"
    elif mutation == "soql":
        soql = soql.replace(" WHERE ", " WHERE precio_base = 100 AND ")
    values = {"scope_id": query.scope_id, "plan_hash": query.plan_hash}
    if mutation in {"scope", "hash"}:
        values["scope_id" if mutation == "scope" else "plan_hash"] = "foreign"
    with execution_guard(guard), pytest.raises(FoundationGuardError, match="BINDING_MISMATCH"):
        with guard.permit(query, reason="search:query"):
            with source_call_scope(request_id=query.request_id, reason="search:query",
                                   interpretation_hash=query.interpretation_hash, **values):
                frozen_client(transport).query(dataset, soql)
    assert transport.calls == []


def test_rehashing_a_mutated_plan_does_not_admit_it():
    outcome = project_accepted(accepted(), reference_instant=NOW)
    changed = replace(outcome, manifest=(replace(outcome.manifest[0], soql="SELECT 1", plan_hash="rehashed"),
                                         *outcome.manifest[1:]))
    with pytest.raises(FoundationGuardError, match="PLAN_BINDING_MISMATCH"):
        ExecutionGuard(changed, RAW, reference_instant=NOW)


def test_count_before_page_and_h4_before_timeout_are_blocked():
    outcome = project_accepted(accepted(), reference_instant=NOW)
    guard = ExecutionGuard(outcome, RAW, reference_instant=NOW)
    for manifest, fallback, reason in [(outcome.manifest[1], False, "COUNT_PRECONDITION"),
                                       (outcome.manifest[0], True, "H4_PRECONDITION")]:
        with pytest.raises(FoundationGuardError, match=reason):
            with guard.permit(manifest, reason="test", parent_call_id="foreign", fallback=fallback):
                pytest.fail("grant unexpectedly admitted")


def test_snapshot_keeps_full_source_rows_independent_of_runtime_mutation():
    transport = FrozenTransport(ROWS)
    tokens = begin_source_calls("rawhash")
    try:
        returned = frozen_client(transport).query(DATASET, "SELECT id_del_proceso WHERE precio_base = 100")
        returned[0]["id_del_proceso"] = "mutated"
        capture = transport.snapshot()
        capture[0]["result"][0]["id_del_proceso"] = "mutated-again"
        assert transport.snapshot()[0]["result"] == [{"id_del_proceso": "P1"}]
        assert transport.calls[0]["call_id"] == "call-1"
        assert transport.calls[0]["attempts"] == 1
    finally:
        native = end_source_calls(tokens)
    assert native[0]["rows_received"] == 1


@given(st.sampled_from(["::text", "::number", "ORDER BY", "WHERE", "LIMIT 50"]))
def test_emulator_rewrites_only_cast_syntax_not_quoted_data(literal):
    sql = soql_to_sql(f"SELECT nombre_del_procedimiento::text WHERE nombre_del_procedimiento = '{literal}'", "source")
    assert f"'{literal}'" in sql
    assert "nombre_del_procedimiento::text" not in sql


@pytest.mark.parametrize("field", ["sql", "dataset", "columns", "value", "policy"])
def test_producer_cannot_supply_executable_authority(field):
    body = as_b(payload())
    body["conditions"][0][field] = "candidate supplied authority"
    producer = LinguisticProducer(lambda system, prompt: body)
    rejected = producer.produce(RAW)
    assert rejected.status is AcceptanceStatus.INVALID_PROPOSAL


def test_b_adjudicates_only_host_candidates_and_uses_same_admission():
    inventory = host_candidates(RAW)
    ids = {candidate["quote"]: candidate["candidate_id"] for candidate in inventory}
    body = payload()
    request, condition = body["requests"][0], body["conditions"][0]
    request.pop("evidence")
    request.pop("universe_evidence")
    condition.pop("evidence")
    request.update(candidate_ids=[ids[RAW]], universe_evidence_candidate_ids=[ids["procesos"]])
    condition["candidate_ids"] = [ids["aseo"]]
    proposal = LinguisticProducer(lambda system, prompt: body).produce(RAW)
    admission = admit_proposal(proposal, RAW, entity_resolver=EntityResolver("app/data/aliases_db.json"))
    assert admission.status is AcceptanceStatus.READY
    assert admission.resolutions == accepted().resolutions
    condition["candidate_ids"] = ["source-row-ID"]
    assert LinguisticProducer(lambda system, prompt: body).produce(RAW).status is AcceptanceStatus.INVALID_PROPOSAL


@pytest.mark.parametrize("failure_type,expected_attempts", [(None, 1), (TimeoutError, 2), (ValueError, 1)])
def test_run_query_foundation_preserves_source_error_and_only_one_h4(tmp_path, monkeypatch, failure_type, expected_attempts):
    failures = [InjectedFailure(lambda: failure_type("source failure"), operation="query")] if failure_type else []
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=ROWS, failures=failures,
                             reference_instant=NOW, foundation_producer=accepted,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    turn = harness.say(RAW)
    assert len(turn.transport_calls) == expected_attempts
    assert len(turn.result["source_call_receipts"]) == expected_attempts
    assert turn.result["foundation_plan_decision"] == "READY"
    if failure_type:
        assert turn.result["foundation_terminal"] == "SOURCE_ERROR"
        assert turn.result["state"] == "source_error"
        assert turn.result["total_count"] is None
        assert all(call["operation"] == "query" for call in turn.transport_calls)
    else:
        assert turn.result["groups"][0]["row_ids"] == ["P1"]
        assert turn.result["groups"][0]["total_quality"] == "exact"
    if failure_type is TimeoutError:
        first, fallback = turn.transport_calls
        assert " ORDER BY " in first["soql"] and " ORDER BY " not in fallback["soql"]
        assert fallback["parent_call_id"] == first["call_id"]
        assert fallback["reason"] == "h4_fallback:query"


def test_legacy_mode_does_not_invoke_foundation_producer(tmp_path, monkeypatch):
    def forbidden(raw):
        pytest.fail("legacy called Foundation producer")
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=ROWS, foundation_producer=forbidden)
    turn = harness.say(RAW)
    assert "foundation_mode" not in turn.result


def test_invalid_mode_is_rejected_before_workflow(tmp_path):
    from app.config import Settings
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(secop_feedback_path=tmp_path / "feedback.jsonl", secop_feedback_db_path=tmp_path / "db",
                 secoppal_foundation_mode="winner_by_default")


def test_o1b_is_a_real_nonempty_predicate_without_dummy_state_or_global_optin():
    params = {"intent_type": "opportunity_search", REFERENCE_INSTANT_PARAM: NOW.isoformat(),
              "valor_min_exclusive": 1000000000}
    soql = SoQLBuilder().build(DATASET, params, allow_opportunity_only=True)
    assert "precio_base > 1000000000" in soql
    assert "estado_de_apertura_del_proceso = 'Abierto'" in soql
    assert "case(fecha_de_recepcion_de IS NULL" in soql
    count = SoQLBuilder().build_count(DATASET, params, allow_opportunity_only=True)
    assert count.split(" WHERE ")[1] == soql.split(" WHERE ")[1].split(" ORDER BY ")[0]


@pytest.mark.parametrize("build", ["build", "build_count"])
def test_without_the_foundation_optin_the_legacy_global_guard_is_unchanged(build):
    """Flag-OFF parity: main refuses an opportunity search with no other WHERE key."""
    from app.core.soql_builder import UnsafeGlobalQueryError

    params = {"intent_type": "opportunity_search", REFERENCE_INSTANT_PARAM: NOW.isoformat()}
    with pytest.raises(UnsafeGlobalQueryError):
        getattr(SoQLBuilder(), build)(DATASET, params)


@pytest.mark.parametrize("count_failure", [False, True])
def test_full_page_count_is_optional_quality_and_rows_are_faithful(tmp_path, monkeypatch, count_failure):
    rows = {DATASET: [{**ROWS[DATASET][0], "id_del_proceso": f"P{at:03d}"} for at in range(60)]}
    failures = [InjectedFailure(lambda: TimeoutError("count timeout"), operation="count")] if count_failure else []
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows, failures=failures,
                             reference_instant=NOW, foundation_producer=accepted,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    turn = harness.say(RAW)
    assert [call["operation"] for call in turn.transport_calls] == ["query", "count"]
    group = turn.result["groups"][0]
    assert len(group["rows"]) == 50
    assert group["total"] == (50 if count_failure else 60)
    assert group["total_quality"] == ("lower_bound" if count_failure else "exact")
    assert turn.result["foundation_terminal"] == "READY"
    if count_failure:
        assert "COUNT_UNAVAILABLE" in group["warnings"]
        assert "al menos 50" in turn.result["response"]


@pytest.mark.parametrize("payload", [[], [{}], [{"count": None}], [{"count": True}],
                                    [{"count": 50.0}], [{"count": "50.0"}], [{"count": -1}],
                                    [{"count": "-1"}], [{"count": "５０"}],
                                    [{"count": "50", "other": "50"}],
                                    [{"count": "50"}, {"count": "50"}], None])
def test_foundation_malformed_count_preserves_full_page_as_lower_bound(tmp_path, monkeypatch, payload):
    rows = {DATASET: [{**ROWS[DATASET][0], "id_del_proceso": f"P{at:03d}"} for at in range(60)]}
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows,
                             reference_instant=NOW, foundation_producer=accepted,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    observed_payloads = []

    def malformed_count(*args, **kwargs):
        observed_payloads.append(deepcopy(payload))
        return deepcopy(payload)

    monkeypatch.setattr(harness.transport, "count", malformed_count)
    turn = harness.say(RAW)
    group = turn.result["groups"][0]
    assert observed_payloads == [payload]
    assert len(group["rows"]) == group["total"] == turn.result["total_count"] == 50
    assert group["total_quality"] == "lower_bound"
    assert "COUNT_UNAVAILABLE" in group["warnings"]
    assert "al menos 50" in turn.result["response"]
    assert turn.result["foundation_terminal"] == "READY"
    native = turn.result["source_call_receipts"]
    assert native[-1]["operation"] == "count" and native[-1]["status"] == "error"
    assert native[-1]["count_value"] is None
    assert [item["reason"] for item in native] == ["search:query", "search:count"]


@pytest.mark.parametrize("scalar", [60, "60"])
def test_foundation_valid_full_page_count_scalar_is_exact(tmp_path, monkeypatch, scalar):
    rows = {DATASET: [{**ROWS[DATASET][0], "id_del_proceso": f"P{at:03d}"} for at in range(60)]}
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows,
                             reference_instant=NOW, foundation_producer=accepted,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    monkeypatch.setattr(harness.transport, "count", lambda *a, **k: [{"count": scalar}])
    turn = harness.say(RAW)
    assert turn.result["groups"][0]["total"] == int(scalar)
    assert turn.result["groups"][0]["total_quality"] == "exact"
    assert "COUNT_UNAVAILABLE" not in turn.result["groups"][0]["warnings"]


@pytest.mark.parametrize("scalar", [0, "0", 49, "49"])
def test_foundation_count_below_received_page_is_source_error_not_exact_total(tmp_path, monkeypatch, scalar):
    rows = {DATASET: [{**ROWS[DATASET][0], "id_del_proceso": f"P{at:03d}"} for at in range(60)]}
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows,
                             reference_instant=NOW, foundation_producer=accepted,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})

    class InconsistentCount:
        def step(self):
            pass

        def finalize(self):
            return scalar

    # Corrupt only the frozen source's COUNT aggregate. The real transport
    # executes and records the emitted COUNT and its successful raw response.
    harness.transport._conn.create_aggregate("count", 0, InconsistentCount)
    turn = harness.say(RAW)
    query, count = turn.transport_calls
    assert [query["operation"], count["operation"]] == ["query", "count"]
    assert count["soql"].split(" WHERE ")[1] == query["soql"].split(" WHERE ")[1].split(" ORDER BY ")[0]
    assert count["status"] == "success" and count["result"] == [{"count(*)": str(scalar)}]
    assert count.get("error_type") is None
    assert len(query["result"]) == 50
    group = turn.result["groups"][0]
    assert len(group["rows"]) == group["total"] == turn.result["total_count"] == 50
    assert group["total_quality"] == "lower_bound" and "COUNT_UNAVAILABLE" in group["warnings"]
    native = turn.result["source_call_receipts"][-1]
    assert native["status"] == "error" and native["error_type"] == "ValueError"
    assert native["count_value"] is None and native["parent_call_id"] == query["call_id"]
    assert "al menos 50" in turn.result["response"]


@given(scalar=st.integers(min_value=0, max_value=49))
def test_count_never_credits_less_than_its_recorded_parent_page(scalar):
    outcome = project_accepted(accepted(), reference_instant=NOW)
    guard = ExecutionGuard(outcome, RAW, reference_instant=NOW)
    rows = {DATASET: [{**ROWS[DATASET][0], "id_del_proceso": f"P{at:03d}"} for at in range(60)]}
    transport = FrozenTransport(rows)

    class InconsistentCount:
        def step(self):
            pass

        def finalize(self):
            return scalar

    transport._conn.create_aggregate("count", 0, InconsistentCount)
    query, count = outcome.manifest
    tokens = begin_source_calls("rawhash")
    try:
        with execution_guard(guard):
            with guard.permit(query, reason="search:query"), scoped(query, reason="search:query"):
                page = frozen_client(transport).query(DATASET, query.soql)
            parent = last_call_id()
            guard.query_result(query.request_id, page, call_id=parent)
            assert len(page) == 50
            with guard.permit(count, reason="search:count", parent_call_id=parent):
                with scoped(count, reason="search:count", parent_call_id=parent):
                    with pytest.raises(RuntimeError) as failure:
                        frozen_client(transport).count(DATASET, count.soql)
        assert type(failure.value.__cause__) is ValueError
        assert "FOUNDATION_COUNT_BELOW_PAGE_SIZE" in str(failure.value.__cause__)
    finally:
        native = end_source_calls(tokens)
    assert [frame["operation"] for frame in transport.snapshot()] == ["query", "count"]
    assert transport.snapshot()[1]["status"] == "success"
    assert native[1]["status"] == "error" and native[1]["count_value"] is None


def test_foundation_true_empty_query_is_exact_zero_without_count(tmp_path, monkeypatch):
    raw = "Busca procesos de obra"
    meaning = payload()
    meaning["conditions"][0]["evidence"] = [{"start": raw.index("obra"), "end": len(raw), "quote": "obra"}]
    meaning["requests"][0]["evidence"] = [{"start": 0, "end": len(raw), "quote": raw}]
    admitted = admit_proposal(decode_proposal(meaning, raw, arm="C"), raw,
                             entity_resolver=EntityResolver("app/data/aliases_db.json"))
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=ROWS,
                             reference_instant=NOW, foundation_producer=lambda text: admitted,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    turn = harness.say(raw)
    assert [call["operation"] for call in turn.transport_calls] == ["query"]
    assert turn.transport_calls[0]["result"] == []
    group = turn.result["groups"][0]
    assert group["rows"] == [] and group["total"] == turn.result["total_count"] == 0
    assert group["total_quality"] == "exact" and "COUNT_UNAVAILABLE" not in group["warnings"]


@pytest.mark.parametrize("payload", [[], [{"count": 0}], [{"count": "0"}]])
def test_legacy_count_empty_payload_still_returns_existing_zero(monkeypatch, payload):
    transport = FrozenTransport(ROWS)
    monkeypatch.setattr(transport, "count", lambda *a, **k: payload)
    assert frozen_client(transport).count(DATASET, "SELECT count(*)") == 0


def test_successful_h4_returns_source_sample_preserving_filters(tmp_path, monkeypatch):
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=ROWS,
                             failures=[InjectedFailure(lambda: TimeoutError("primary timeout"),
                                                       operation="query", times=1)],
                             reference_instant=NOW, foundation_producer=accepted,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    turn = harness.say(RAW)
    assert turn.result["state"] == "ok_order_sample"
    assert turn.result["foundation_terminal"] == "READY"
    assert turn.result["groups"][0]["row_ids"] == ["P1"]
    primary, fallback = turn.transport_calls
    assert primary["status"] == "error" and fallback["status"] == "success"
    assert primary["soql"].split(" WHERE ")[1].split(" ORDER BY ")[0] == fallback["soql"].split(" WHERE ")[1].split(" LIMIT ")[0]


def test_provider_error_is_not_an_accredited_zero(tmp_path, monkeypatch):
    def unavailable(system, prompt):
        raise TimeoutError("provider unavailable")
    producer = LinguisticProducer(unavailable)
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=ROWS, reference_instant=NOW,
                             foundation_producer=producer,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    # A query outside the basic-fallback contract (a negation) makes no call and claims no total.
    turn = harness.say("Busca procesos de aseo sin vigilancia")
    assert turn.transport_calls == ()
    assert turn.result["foundation_terminal"] == "PROVIDER_ERROR"
    assert turn.result["foundation_plan_decision"] == "PROVIDER_ERROR"
    assert turn.result["total_count"] is None
    assert turn.result["total_quality"] == "not_applicable"
    assert turn.result["foundation_fallback"]["fallback_outcome"] == "BASIC_FALLBACK_NOT_SAFE"


@pytest.mark.parametrize("columns", [("foreign",), ("precio_base",), ("id_del_proceso", "id_del_proceso"), ["id_del_proceso"]])
def test_select_boundary_rejects_foreign_missing_identity_duplicate_or_mutable_columns(columns):
    with pytest.raises(ValueError, match="INVALID_SOURCE_SELECT_FIELDS"):
        SoQLBuilder().build(DATASET, {"objeto": ["aseo"]}, select_fields=columns)


def test_select_boundary_uses_known_source_columns_without_changing_legacy_default():
    builder = SoQLBuilder()
    fields = ("id_del_proceso", "justificaci_n_modalidad_de", "nombre_del_procedimiento")
    selected = builder.build(DATASET, {"objeto": ["aseo"]}, select_fields=fields)
    legacy = builder.build(DATASET, {"objeto": ["aseo"]})
    assert selected.split(" WHERE ")[0] == "SELECT " + ", ".join(fields)
    assert "fase" in legacy.split(" WHERE ")[0]
    assert selected.split(" WHERE ")[1] == legacy.split(" WHERE ")[1]


def test_configured_producer_uses_real_handler_interface_without_network(monkeypatch):
    from types import SimpleNamespace
    import app.core.llm_handler as llm

    calls = []
    def create(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(model="reported-by-provider",
                               choices=[SimpleNamespace(message=SimpleNamespace(content=__import__("json").dumps(as_b(payload()))))])
    def client(**kwargs):
        assert kwargs["api_key"] == "synthetic-test-key"
        assert kwargs["max_retries"] == 0
        return SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    monkeypatch.setattr(llm, "OpenAI", client)
    workflow = SimpleNamespace(llm_handler=SimpleNamespace(api_key="synthetic-test-key", base_url="https://api.deepseek.com/v1"),
                               settings=SimpleNamespace(deepseek_model="deepseek-flash"))
    producer = configured_producer(workflow)
    proposal = producer.produce(RAW)
    assert proposal.producer == "foundation-B"
    assert producer.receipt.provider_model_reported == "reported-by-provider"
    assert len(calls) == 1 and calls[0]["model"] == "deepseek-flash"
