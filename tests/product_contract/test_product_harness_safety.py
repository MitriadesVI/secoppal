"""F2.5 — Product Harness Safety: el harness demuestra sus garantías antes de F3.

Cada garantía tiene un test que falla si la garantía se retira (mutación explícita
donde aplica). Ninguno es un escenario de producto: son autotests del harness.
"""
from __future__ import annotations

import hashlib
import json
import socket
from datetime import date

import pytest
from hypothesis import given, strategies as st

from app.config import Settings, get_settings
from app.core.actor_name import ActorResolutionStatus
from app.core.orchestrator import SecopalWorkflow
from app.core.secop_client import SecopClient
from app.core.query_router import QueryRouter
from app.data.actor_alias_resolver import ActorNameResolver
from tests.product_contract import scenario_schema
from tests.product_contract.actor_pack import build_pack_index, load_pack
from tests.product_contract.assertions import ORACLES, evaluate, row_ids
from tests.product_contract.frozen_source import (
    FrozenSourceError,
    FrozenTransport,
    InjectedFailure,
    frozen_client,
    load_rows_jsonl,
    soql_to_sql,
)
from tests.product_contract.harness import ProductHarness

FIXTURES = scenario_schema.FIXTURE_DIR
CANARY = FIXTURES / "source_rows" / "harness_canary.jsonl"
PACK = FIXTURES / "actor_alias_rows.jsonl"


# ── Red prohibida por construcción ───────────────────────────────────────


def test_a_swallowed_connection_attempt_still_fails_the_ledger(network_ledger):
    try:
        socket.create_connection(("www.datos.gov.co", 443), timeout=1)
    except OSError:
        pass  # el runtime atrapa; el ledger no olvida
    with pytest.raises(AssertionError, match="external network"):
        network_ledger.verify()
    assert network_ledger.take()


def test_dns_raw_connect_and_datagrams_are_all_recorded(network_ledger):
    with pytest.raises(socket.gaierror):
        socket.getaddrinfo("www.datos.gov.co", 443)
    stream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    datagram = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        with pytest.raises(ConnectionRefusedError):
            stream.connect(("127.0.0.1", 9))
        with pytest.raises(ConnectionRefusedError):
            datagram.sendto(b"x", ("127.0.0.1", 9))
    finally:
        stream.close()
        datagram.close()
    assert [a["operation"] for a in network_ledger.take()] == [
        "socket.getaddrinfo", "socket.connect", "socket.sendto"]


@pytest.mark.parametrize("host", ["www.datos.gov.co", "api.deepseek.com", "app.posthog.com"])
def test_socrata_llm_and_burr_endpoints_are_blocked(network_ledger, host):
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection((host, 443), timeout=1)
    attempts = network_ledger.take()
    assert len(attempts) == 1
    assert host in attempts[0]["target"]


def test_local_unix_sockets_are_not_false_positives(network_ledger):
    left, right = socket.socketpair()
    try:
        left.sendall(b"ok")
        assert right.recv(2) == b"ok"
    finally:
        left.close()
        right.close()
    assert network_ledger.attempts == []


def test_an_unmocked_production_workflow_is_caught_reaching_the_network(network_ledger, tmp_path, monkeypatch):
    """H10 demostrado: sin fuente congelada el workflow sale a la red y el ledger lo ve."""
    get_settings.cache_clear()
    flow = SecopalWorkflow(Settings(
        deepseek_api_key=None,
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "secoppal.db",
    ))
    flow.run_query("contratos de mantenimiento vial en 2026", chat_id="escape")
    attempts = network_ledger.take()
    assert attempts, "the production client must be caught attempting the network"


# ── Fuente congelada ─────────────────────────────────────────────────────


def test_soql_is_evaluated_not_canned():
    sql = soql_to_sql("SELECT id_contrato WHERE upper(x) LIKE '%ORDER BY%' ORDER BY id_contrato LIMIT 5", "t")
    assert sql == "SELECT id_contrato FROM t WHERE upper(x) LIKE '%ORDER BY%' ORDER BY id_contrato LIMIT 5"
    transport = FrozenTransport(load_rows_jsonl(CANARY))
    rows = transport.query("jbjy-vk9h", "SELECT id_contrato WHERE upper(objeto_del_contrato) LIKE '%VIAL%' "
                                         "ORDER BY id_contrato")
    assert [r["id_contrato"] for r in rows] == ["PC-CANARY-001", "PC-CANARY-002"]
    count = transport.count("jbjy-vk9h", "SELECT count(*) AS total WHERE valor_del_contrato >= 500000000")
    assert count == [{"total": "2"}]


@given(
    deadline=st.one_of(st.none(), st.dates(min_value=date(2026, 9, 6), max_value=date(2026, 9, 10))),
    published=st.dates(min_value=date(2026, 6, 8), max_value=date(2026, 9, 10)),
)
def test_frozen_opportunity_case_keeps_deadline_and_null_semantics(deadline, published):
    row = {
        "_dataset": "p6dx-8zbt", "id_del_proceso": "synthetic-opportunity",
        "fecha_de_recepcion_de": f"{deadline.isoformat()}T00:00:00.000" if deadline else None,
        "fecha_de_publicacion_del": f"{published.isoformat()}T00:00:00.000",
    }
    transport = FrozenTransport({"p6dx-8zbt": [row]})
    soql = ("SELECT id_del_proceso WHERE case(fecha_de_recepcion_de IS NULL, "
            "fecha_de_publicacion_del >= '2026-06-10T00:00:00.000', true, "
            "fecha_de_recepcion_de >= '2026-09-08T00:00:00.000')")
    got = transport.query("p6dx-8zbt", soql)
    expected = (deadline >= date(2026, 9, 8) if deadline else published >= date(2026, 6, 10))
    assert bool(got) is expected


def test_unevaluable_soql_and_unfrozen_datasets_fail_loudly():
    transport = FrozenTransport(load_rows_jsonl(CANARY))
    with pytest.raises(FrozenSourceError):
        transport.query("jbjy-vk9h", "SELECT columna_inexistente WHERE 1 = 1")
    with pytest.raises(FrozenSourceError):
        transport.query("p6dx-8zbt", "SELECT id_del_proceso WHERE 1 = 1")
    assert len(transport.errors) == 2


def test_timeout_derivation_keeps_the_frozen_wire():
    transport = FrozenTransport(load_rows_jsonl(CANARY))
    client = frozen_client(transport)
    for derived in (client.with_timeout(3), client.derived_for_timeout(2)):
        assert derived is not client and derived.transport is transport


def test_injected_failures_surface_through_the_real_client():
    transport = FrozenTransport(load_rows_jsonl(CANARY),
                                failures=[InjectedFailure(lambda: TimeoutError("frozen"), operation="query")])
    client = frozen_client(transport)
    with pytest.raises(RuntimeError) as raised:
        client.query("jbjy-vk9h", "SELECT id_contrato WHERE 1 = 1")
    assert isinstance(raised.value.__cause__, TimeoutError)
    assert transport.calls[-1]["injected"] == "TimeoutError"
    assert client.count("jbjy-vk9h", "SELECT count(*) AS total WHERE 1 = 1") == 3


# ── Harness completo: workflow real + fuente congelada ───────────────────


def test_the_real_workflow_runs_on_the_frozen_source_without_network(network_ledger, tmp_path, monkeypatch):
    harness = ProductHarness(tmp_path / "scenario", monkeypatch, rows_by_dataset=load_rows_jsonl(CANARY))
    turn = harness.say("contratos de mantenimiento vial en 2026", chat_id="canary")
    assert turn.source_calls, "the workflow must consume the frozen source"
    assert {c["dataset"] for c in turn.source_calls} == {"jbjy-vk9h"}
    assert not harness.transport.errors
    ids = row_ids(turn.result)
    assert "PC-CANARY-001" in ids and "PC-DECOY-001" not in ids, (ids, turn.result.get("response"))
    assert network_ledger.attempts == []


def test_persistence_is_confined_to_the_scenario_root(tmp_path, monkeypatch):
    root = tmp_path / "scenario"
    harness = ProductHarness(root, monkeypatch, rows_by_dataset=load_rows_jsonl(CANARY))
    assert harness.unconfined_paths() == []
    harness.say("contratos de mantenimiento vial en 2026", chat_id="persisted")
    written = [p for p in root.rglob("*") if p.is_file()]
    assert written and all(p.resolve().is_relative_to(root.resolve()) for p in written)
    harness.flow.conv_store.base_path = tmp_path.parent / "escaped"
    assert harness.unconfined_paths(), "a store pointing outside the scenario root must be reported"


def test_a_followup_turn_carries_the_previous_params(tmp_path, monkeypatch):
    harness = ProductHarness(tmp_path / "scenario", monkeypatch, rows_by_dataset=load_rows_jsonl(CANARY))
    first = harness.say("contratos de mantenimiento vial en 2026", chat_id="fu")
    second = harness.say("muéstrame más", chat_id="fu")
    assert first.previous_params is None
    assert second.previous_params == first.params


def test_public_lineage_opt_in_is_observed_without_changing_execution(tmp_path, monkeypatch):
    rows = load_rows_jsonl(CANARY)
    traced = ProductHarness(tmp_path / "traced", monkeypatch, rows_by_dataset=rows)
    plain = ProductHarness(tmp_path / "plain", monkeypatch, rows_by_dataset=rows)
    plain.flow.query_router.parse = QueryRouter.parse.__get__(plain.flow.query_router)
    query = "contratos de mantenimiento vial de más de 500 millones"
    observed = traced.say(query, chat_id="traced")
    control = plain.say(query, chat_id="plain")
    assert observed.lineage_views and control.lineage_views == ()
    assert observed.lineage_views[0].raw_text == query
    observed.lineage_views[0].verify_integrity()
    assert observed.params == control.params
    assert [c["soql"] for c in observed.source_calls] == [c["soql"] for c in control.source_calls]
    assert row_ids(observed.result) == row_ids(control.result)
    assert observed.result["outcome"] == control.result["outcome"]
    observed.lineage_views = ()
    assert any(v.startswith("lineage.present") for v in observed.violations({"lineage": {"raw_text": query}}))


# ── Pack de colisiones de actores ────────────────────────────────────────


def test_the_actor_pack_builds_through_the_real_index_builder(tmp_path):
    rows = load_pack(PACK)
    resolver = ActorNameResolver(build_pack_index(rows, tmp_path / "actors.sqlite",
                                                  source_dataset="product-contract-v1-harness"))
    assert resolver.resolve("FUNDACION ALFA SINTETICA").status == ActorResolutionStatus.RESOLVED
    assert resolver.resolve("LUCIA MARGARITA QUINTERO SINTETICA").status != ActorResolutionStatus.RESOLVED


def test_derived_buyer_collision_pack_keeps_only_the_accredited_shape():
    provenance = json.loads((FIXTURES / "actor_barranquilla_provenance.json").read_text())
    rows = load_pack(FIXTURES / provenance["fixture"])
    assert provenance["observed_status"] == "AMBIGUOUS"
    assert provenance["observed_candidate_count"] == 4
    assert provenance["observed_document_types"] == {"CC": 4}
    assert len(rows) == 2
    assert {row["tipodocproveedor"] for row in rows} == {"CC"}
    assert {row["documento_proveedor"] for row in rows} == {"70070801", "70070802"}
    assert {row["proveedor_adjudicado"] for row in rows} == {"ALCALDIA DE BARRANQUILLA"}


def test_pack_rows_must_declare_their_collision_shape(tmp_path):
    bad = tmp_path / "bad.jsonl"
    record = {k: v for k, v in json.loads(PACK.read_text().splitlines()[0]).items() if k != "_shape"}
    bad.write_text(json.dumps(record) + "\n")
    with pytest.raises(ValueError, match="_shape"):
        load_pack(bad)


# ── Oráculos ─────────────────────────────────────────────────────────────


def test_every_violated_oracle_is_reported_not_only_the_first():
    result = {
        "analytical_intent": "actor_profile", "route_reason": "actor_profile",
        "resolved_params": {"actor_scope": {"base_number": "1"}, "valor_max": 5},
        "response": "Elige una persona de la lista", "results": [{"id_contrato": "X"}],
    }
    calls = [{"dataset": "jbjy-vk9h", "soql": "SELECT * WHERE documento_proveedor = '1'"}]
    violations = evaluate(result, calls, {
        "route": "entity_profile", "forbidden_routes": ["actor_profile"],
        "absent_params": ["actor_scope", "valor_max"], "source_calls": {"exact": 0},
        "soql": {"not_contains": ["documento_proveedor"]},
        "response": {"not_contains": ["elige una PERSONA"]},
        "results": {"must_exclude_ids": ["X"]}, "surprise": True,
    })
    labels = {v.split(":")[0] for v in violations}
    assert labels == {"unknown oracle 'surprise'", "route", "forbidden_routes", "absent_params.actor_scope",
                      "absent_params.valor_max", "source_calls.exact", "soql.not_contains",
                      "response.not_contains", "results.must_exclude_ids"}


@given(st.text(min_size=1).filter(lambda key: key not in ORACLES))
def test_an_unknown_expectation_never_suppresses_other_violations(key):
    violations = evaluate({}, [], {key: True, "source_calls": {"exact": 1}})
    assert any(v.startswith("unknown oracle") for v in violations)
    assert any(v.startswith("source_calls.exact") for v in violations)


def test_a_satisfied_contract_reports_nothing_and_followup_is_checked():
    previous = {"entidad_resolved": "X", "actor_scope": {"base_number": "1"}}
    result = {"analytical_intent": "entity_profile", "resolved_params": {"entidad_resolved": "X"}}
    assert evaluate(result, [], {"route": "entity_profile",
                                 "followup": {"preserves": ["entidad_resolved"], "clears": ["actor_scope"]}},
                    previous) == []
    assert evaluate(result, [], {"followup": {"preserves": ["valor_min"]}}, previous)


# ── Corpus, cuarentena y procedencia ─────────────────────────────────────


def test_fixture_manifest_hashes_match_the_committed_files():
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    assert manifest["contains_personal_data"] is False
    for relative, digest in manifest["sha256"].items():
        assert hashlib.sha256((FIXTURES / relative).read_bytes()).hexdigest() == digest, relative
    tracked = {p.relative_to(FIXTURES).as_posix() for p in FIXTURES.rglob("*")
               if p.is_file() and p.name != "manifest.json"}
    assert tracked == set(manifest["sha256"])


def test_the_design_corpus_is_valid_and_covers_the_twenty_mandatory_items():
    document = scenario_schema.load(scenario_schema.SCENARIOS)
    assert scenario_schema.validate_scenarios(document) == []
    scenarios = document["scenarios"]
    assert sorted(s["mandatory_item"] for s in scenarios) == list(range(1, 21))
    assert {s["status"] for s in scenarios} == {"DESIGN", "READY"}
    assert sum(s["status"] == "READY" for s in scenarios) == 10


def test_known_product_defects_are_quarantined_and_never_green():
    scenarios = scenario_schema.load(scenario_schema.SCENARIOS)["scenarios"]
    quarantine = scenario_schema.load(scenario_schema.QUARANTINE)
    ids = {s["id"] for s in scenarios}
    assert scenario_schema.validate_quarantine(quarantine, ids) == []
    assert quarantine["open_product_gaps"] and quarantine["harness_debt"]
    tampered = {"open_product_gaps": [dict(quarantine["open_product_gaps"][0], counted_green=True)]}
    assert scenario_schema.validate_quarantine(tampered, ids)
