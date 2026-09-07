"""Independent evidence and boundary falsification with synthetic source replies."""
from __future__ import annotations

from dataclasses import asdict

import pytest
from hypothesis import given, settings, strategies as st

from app.core.entity_profile.aspects.activity import parse_process_activity
from app.core.entity_profile.aspects.temporal import merge_periods
from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.render import render_entity_profile
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.soql_builder import SoQLBuilder
from test_entity_profile_independent_review import BUYER, ROUTER, TODAY, observe, record
from test_entity_profile_queries import FakeClient
from test_entity_profile_workflow import _workflow


class EvidenceClient(FakeClient):
    def __init__(self, mode):
        super().__init__()
        self.mode = mode

    def aggregate(self, dataset, soql):
        self._record("aggregate", dataset, soql)
        if "GROUP BY modalidad_de_contratacion" in soql and self.mode.startswith("process-"):
            raw = {"process-unknown": "unknown", "process-fraction": "0.5",
                   "process-negative": "-1", "process-zero": "0"}[self.mode]
            return [{"process_count": raw, "modalidad_de_contratacion": "Directa"}]
        if "AS contract_count" in soql and "GROUP BY" not in soql:
            return [{"contract_count": "0" if self.mode == "contract-zero" else "4",
                     "total_contract_value": None, "average_contract_value": None}]
        if "GROUP BY documento_proveedor" in soql and self.mode == "empty-suppliers":
            return []
        if "date_trunc_y" in soql and dataset == "jbjy-vk9h":
            return [{"period": "2026-01-01T00:00:00.000", "row_count": "4",
                     "contract_value": None}]
        # Base implementation also records: source_calls, not this debugging list,
        # is the execution authority in this test.
        return super().aggregate(dataset, soql)


@pytest.mark.parametrize("mode", [
    "process-unknown", "process-fraction", "process-negative", "process-zero",
    "missing-value", "contract-zero", "empty-suppliers", "temporal",
])
def test_evidence_truth_independent(mode):
    query = f"analiza {'evolucion de ' if mode == 'temporal' else ''}{BUYER} en 2026"
    turn = classify_entity_profile_turn(query, ROUTER.parse(query, today=TODAY).params)
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params, focus=turn.focus,
        plan=turn.plan, secop_client=EvidenceClient(mode), soql_builder=SoQLBuilder(),
    )
    rendered = render_entity_profile(profile)
    record(f"evidence-{mode}", {"profile": asdict(profile), "render": rendered})
    assert all(c.status == "SUCCESS" for c in profile.source_calls.calls)
    if mode.startswith("process-"):
        assert profile.activity.fact("process_count") is None
        assert "Publicó 0 procesos" not in rendered
    elif mode == "contract-zero":
        assert profile.activity.fact("contract_count") is None
        assert "Se observan 0 contratos" not in rendered
        assert "contradictorios" in rendered
    elif mode == "temporal":
        row = profile.temporal.fact("periods")[-1]
        assert row.contract_count == 4 and row.contract_value is None
        assert "$0" not in rendered
    else:
        assert profile.activity.fact("contract_count") == 4
        assert profile.values.fact("total_contract_value") is None
        assert "$0" not in rendered and "no contratos" not in rendered.lower()


@settings(max_examples=20, derandomize=True)
@given(numerator=st.integers(min_value=1, max_value=99).filter(lambda n: n % 10 != 0))
def test_nonintegral_source_count_never_becomes_integer_fact(numerator):
    raw = str(numerator / 10)
    assert parse_process_activity([{"process_count": raw}]).process_count is None
    assert merge_periods([{"period": "2026", "row_count": raw}], None)[0].process_count is None


# ADJUDICADO en V1.1: "procesos publicados y contratos firmados en agosto" sale
# de esta lista y pasa al control de abajo.
#
# Los dos artefactos de la auditoría se contradicen en ESA frase. El texto de F1
# fija como criterio de aceptación «ambos órdenes conservan ambos eventos **o**
# ambos aclaran», y esta parametrización exigía sólo la segunda mitad. La
# petición es enteramente representable —procesos por publicación, contratos por
# firma, un único periodo— y aclararla sería negarse a contestar una pregunta
# bien formada, que es una regresión de utilidad, no una protección.
#
# Las otras dos frases se conservan estrictas: en ellas una fuente explícita NO
# puede honrar la interpretación, y ahí la aclaración sí es la respuesta.
@pytest.mark.parametrize("phrase", [
    "contratos terminados en agosto y actualmente activos",
    "procesos y contratos publicados en agosto",
])
def test_full_product_workflow_rejects_partial_temporal_interpretation(phrase):
    workflow, client = _workflow()
    result = workflow.run_query(f"analiza {phrase} de {BUYER}", today=TODAY)
    record("workflow-" + phrase.split()[0] + "-" + str(len(phrase)), {
        "response": result["response"], "params": result["resolved_params"],
        "needs_clarification": result["needs_clarification"],
        "route": result["route_reason"], "calls": client.calls,
    })
    assert result["needs_clarification"] and not client.calls


@pytest.mark.parametrize("phrase", [
    "procesos publicados y contratos firmados en agosto",
    "contratos firmados y procesos publicados en agosto",
])
def test_full_product_workflow_honors_two_representable_events(phrase):
    """El criterio de aceptación de F1, en su primera mitad y de extremo a extremo."""
    workflow, client = _workflow()
    result = workflow.run_query(f"analiza {phrase} de {BUYER}", today=TODAY)
    assert not result["needs_clarification"]
    assert {call[1] for call in client.calls} == {"p6dx-8zbt", "jbjy-vk9h"}
    for _, dataset, soql in client.calls:
        where = soql.split(" WHERE ", 1)[1]
        expected = (
            "fecha_de_publicacion_del >=" if dataset == "p6dx-8zbt"
            else "fecha_de_firma >="
        )
        assert expected in where


def test_purity_keeps_metadata_in_its_source_schema():
    observation = observe(f"analiza procesos y contratos de {BUYER}", {
        "estado_contrato": ["terminado"], "estado_del_procedimiento": ["Publicado"],
        "estado": "Publicado", "estado_family": "publicado", "intent_type": "opportunity_search",
        "estado_field": "estado_del_procedimiento",
    })
    record("purity-injected", observation)
    contract = observation["contract_plan"]["params"]
    assert "estado_del_procedimiento" not in contract
    assert contract.get("estado_field") in (None, "estado_contrato")


def test_injected_contract_state_is_removed_from_processes():
    observation = observe(f"analiza procesos y contratos de {BUYER}", {
        "estado_contrato": ["terminado"],
    })
    assert observation["parsed"]["estado_contrato"] == ["terminado"]
    assert "estado_contrato" not in observation["process_plan"]["params"]


@pytest.mark.parametrize("phrase", [
    "procesos y contratos publicados en agosto",
    "proveedores de procesos y contratos",
    "montos de procesos y contratos",
])
def test_workflow_has_no_execution_authority_after_profile_plan(phrase):
    query = f"analiza {phrase} de {BUYER}"
    planned = observe(query)
    workflow, client = _workflow()
    result = workflow.run_query(query, today=TODAY)
    record("workflow-extra-source-" + phrase.split()[0], {
        "planned_calls": planned["calls"], "actual_calls": client.calls,
        "response": result["response"], "params": result["resolved_params"],
        "profile_calls_reported": result["timings_ms"].get("entity_profile_calls_total"),
    })
    assert len(client.calls) == len(planned["calls"])
    assert {c[1] for c in client.calls} == {c[1] for c in planned["calls"]}


@pytest.mark.parametrize("phrase,expected_event,topic", [
    ("contratos de firma digital", "NONE", "firma"),
    ("contratos que firmo digitalmente", "CONTRACT_SIGNATURE", "digitalmente"),
    ("contratos de publicacion de avisos", "NONE", "avisos"),
    ("procesos que publico avisos", "PROCESS_PUBLICATION", "avisos"),
])
def test_nominal_verbal_pairs(phrase, expected_event, topic):
    observation = observe(f"analiza {phrase} de {BUYER} en agosto")
    record(f"pair-{expected_event}-{topic}", observation)
    assert observation["request"]["event"] == expected_event
    assert any(topic in value for value in observation["request"]["topic"])
