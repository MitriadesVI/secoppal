"""SUPPLIERS 1.1 — la ruta real de primer turno (§17).

Entrypoint real (``SecopalWorkflow.run_query``) con la frontera SECOP y los dos
providers LLM stubbeados, y CERO mocks dentro de la capability: autoridad de
cardinalidad, binding, admisión, plan de ejecución y presentación corren como
en el producto. Cada caso comprueba ruta, llamadas a la fuente, el corte del
SoQL, la métrica/corte de la vista y el texto visible.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.test_natural_alpha_runtime import make_flow, run  # noqa: F401
from tests.test_natural_job_c4b_suppliers_runtime import accredited_flow, frame_on  # noqa: F401


CORPUS = json.loads(
    Path("tests/fixtures/suppliers_request_flex_v1_corpus.json").read_text(encoding="utf-8"),
)["cases"]
BY_ID = {case["id"]: case for case in CORPUS}

# Filas sintéticas: el 2º y 3º empatan a 10 contratos (testigo de corte).
ROWS = [
    {"documento_proveedor": str(900100000 + index), "tipodocproveedor": "NIT",
     "supplier_name": f"PROVEEDOR {index + 1}", "contract_count": count,
     "total_contract_value": float(count * 1_000_000)}
    for index, count in enumerate((12, 10, 10, 8, 6, 4, 1))
]


class RowsSource:
    """La frontera SECOP con filas: registra cada llamada y responde el agregado."""

    timeout = 15

    def __init__(self, rows):
        self.calls = []
        self.rows = list(rows)

    def with_timeout(self, timeout):
        return self

    derived_for_timeout = with_timeout

    def aggregate(self, dataset, soql, **kwargs):
        self.calls.append((dataset, soql))
        return list(self.rows) if "GROUP BY" in soql else []

    query = aggregate

    def count(self, dataset, soql, **kwargs):
        self.calls.append((dataset, soql))
        return 0


# Empate deliberado en el primer lugar (12 y 12 contratos).
TIE_ROWS = [
    {"documento_proveedor": "900200001", "tipodocproveedor": "NIT",
     "supplier_name": "EMPATE A", "contract_count": 12, "total_contract_value": 12000000.0},
    {"documento_proveedor": "900200002", "tipodocproveedor": "NIT",
     "supplier_name": "EMPATE B", "contract_count": 12, "total_contract_value": 11000000.0},
    {"documento_proveedor": "900200003", "tipodocproveedor": "NIT",
     "supplier_name": "TERCERO", "contract_count": 8, "total_contract_value": 8000000.0},
]

# Un caso representativo por bucket servible; el corpus completo se verifica en
# el módulo determinista (mismo fixture, cero coste).
SERVED = ("d01", "d02", "c01", "c02", "c03", "v03", "s01", "s02", "e04", "e07", "e06", "y01", "p02")
# Los vetos de OTRAS autoridades, cada uno con su cero de llamadas.
VETOED = {
    "o03": "unsupported_cardinality",
    "a01": "unsupported_cardinality",
    "m01": "MONEY",
    "u01": "MODALITY",
    "g01": "intent_witness_not_positive",
}


@pytest.mark.parametrize("case_id", SERVED)
def test_a_served_case_executes_exactly_its_view(frame_on, make_flow, case_id):
    case = BY_ID[case_id]
    metric, limit = case["expect"]["metric"], case["expect"]["limit"]
    flow, _verifier = accredited_flow(make_flow, frame_on, case["quote"])

    result = run(flow, case["query"], chat_id=f"flex-{case_id}")

    assert result["route_reason"] == "entity_profile_suppliers", result.get("authority_reason")
    assert result["needs_clarification"] is False
    calls = flow.secop_client.calls
    assert len(calls) == (2 if metric == "both" else 1), case["query"]
    assert all(dataset == "jbjy-vk9h" for dataset, _ in calls)
    for _, soql in calls:
        assert f"LIMIT {limit + 1}" in soql, soql
        if metric == "count":
            assert "ORDER BY contract_count DESC" in soql
        if metric == "value":
            assert "ORDER BY total_contract_value DESC" in soql
    view = result["entity_profile_view"]["suppliers"]["ranking"]
    assert (view["metric"], view["limit"]) == (metric, limit)
    response = result["response"]
    if metric == "count" and limit == 1:
        assert "Proveedor con más contratos" in response
    elif metric == "value" and limit == 1:
        assert "Proveedor con mayor valor contratado" in response
    elif metric == "both":
        assert "Principales contratistas por número de" in response
    else:
        label = "más contratos" if metric == "count" else "mayor valor contratado"
        assert f"Proveedores con {label} (top {limit})" in response


def test_the_default_case_runs_the_two_rankings_and_keeps_the_v1_view(frame_on, make_flow):
    """Paridad V1 en la ruta real: BOTH, 5, las dos listas y sus dos llamadas."""
    case = BY_ID["d01"]
    flow, _verifier = accredited_flow(make_flow, frame_on, case["quote"])
    flow.secop_client = RowsSource(ROWS)

    result = run(flow, case["query"], chat_id="flex-parity")

    assert len(flow.secop_client.calls) == 2
    assert all(f"LIMIT {5 + 1}" in soql for _, soql in flow.secop_client.calls)
    response = result["response"]
    assert "Principales contratistas por número de" in response
    assert "Principales contratistas por valor contratado" in response
    # 5 visibles por ranking: la fila testigo del SQL nunca se publica.
    assert response.count("• ") == 10
    assert "PROVEEDOR 6" not in response and "PROVEEDOR 7" not in response
    ranking = result["entity_profile_view"]["suppliers"]["ranking"]
    assert (ranking["metric"], ranking["limit"], ranking["ties"]) == ("both", 5, [])
    assert set(ranking["rows"]) == {"count", "value"}
    assert len(ranking["rows"]["count"]) == 5
    assert len(ranking["rows"]["value"]) == 5


@pytest.mark.parametrize("case_id,reason", sorted(VETOED.items()))
def test_an_unserved_or_guarded_case_stops_with_zero_calls(frame_on, make_flow, case_id, reason):
    case = BY_ID[case_id]
    flow, _verifier = accredited_flow(make_flow, frame_on, case["quote"])

    result = run(flow, case["query"], chat_id=f"flex-veto-{case_id}")

    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []
    if reason == "MONEY":
        assert result["authority_reason"] == "UNSUPPORTED_MONEY"
    elif reason == "MODALITY":
        assert result["authority_reason"] == "UNSUPPORTED_MODALITY"
    elif reason == "intent_witness_not_positive":
        assert result["authority_reason"] == "intent_witness_not_positive"
    else:
        assert reason in (result.get("route_reason", "") + result.get("authority_reason", ""))


def test_a_first_place_tie_is_never_presented_as_a_unique_leader(frame_on, make_flow):
    """§12: empate en el primer lugar — se nombra el valor y el empate, no un líder."""
    case = BY_ID["s02"]
    flow, _verifier = accredited_flow(make_flow, frame_on, case["quote"])
    flow.secop_client = RowsSource(TIE_ROWS)

    result = run(flow, case["query"], chat_id="flex-tie")

    response = result["response"]
    assert "existe empate en el primer lugar" in response
    assert "EMPATE A" not in response, "un empate no se presenta como líder único"
    ranking = result["entity_profile_view"]["suppliers"]["ranking"]
    assert ranking["ties"] == [
        {"ranking": "count", "position": 1, "boundary": "12 contratos"},
    ]


def test_a_both_with_a_single_cup_and_a_tie_never_names_a_leader(frame_on, make_flow):
    """ZEUS §11: «1 proveedor …» es BOTH/1 — el escalar cubre ambas métricas.

    Con estas filas sólo empata el ranking por conteo (12,12); por valor hay
    líder único (12M contra 11M) y nombrarlo es correcto. Lo que no puede pasar
    es que el ranking EMPATADO nombre a alguien.
    """
    flow, _verifier = accredited_flow(make_flow, frame_on, "proveedor")
    flow.secop_client = RowsSource(TIE_ROWS)

    result = run(flow, "1 proveedor de la alcaldía de Barranquilla", chat_id="flex-both1-tie")

    response = result["response"]
    assert "existe empate en el primer lugar" in response
    assert "EMPATE A: 12 contratos" not in response, "el ranking empatado no nombra"
    assert "EMPATE B: 12 contratos" not in response
    ranking = result["entity_profile_view"]["suppliers"]["ranking"]
    assert (ranking["metric"], ranking["limit"]) == ("both", 1)
    assert [tie["ranking"] for tie in ranking["ties"]] == ["count"]


def test_a_failed_single_ranking_never_keeps_an_affirming_title(frame_on, make_flow):
    """ZEUS §11: sin cifra acreditable el título escalar no afirma un primer puesto."""
    class FailingSource(RowsSource):
        def aggregate(self, dataset, soql, **kwargs):
            self.calls.append((dataset, soql))
            raise TimeoutError("SECOP no respondió")

    case = BY_ID["s05"]
    flow, _verifier = accredited_flow(make_flow, frame_on, case["quote"])
    flow.secop_client = FailingSource([])

    result = run(flow, case["query"], chat_id="flex-scalar-fail")

    response = result["response"]
    assert "Proveedor con mayor valor contratado" not in response
    assert "no acreditado" in response


def test_a_boundary_tie_in_a_top_n_is_declared_without_deciding_the_order(frame_on, make_flow):
    case = BY_ID["e01"]  # "2 proveedores de la alcaldía de Barranquilla"
    flow, _verifier = accredited_flow(make_flow, frame_on, case["quote"])
    flow.secop_client = RowsSource(ROWS)

    result = run(flow, case["query"], chat_id="flex-tie-cut")

    assert "Empate en el corte del top 2 por número de contratos" in result["response"]
    # Las filas sintéticas empatan en ambos rankings (los totales siguen a los
    # conteos), y cada ranking ejecutado declara su propio empate.
    ranking = result["entity_profile_view"]["suppliers"]["ranking"]
    assert ranking["ties"] == [
        {"ranking": "count", "position": 2, "boundary": "10 contratos"},
        {"ranking": "value", "position": 2, "boundary": "$10 millones"},
    ]


def test_a_failed_ranking_is_never_presented_as_an_empty_one(frame_on, make_flow):
    """§11: TIMEOUT ≠ empty — un ranking que no respondió no se afirma como cero."""
    class FailingSource(RowsSource):
        def aggregate(self, dataset, soql, **kwargs):
            self.calls.append((dataset, soql))
            raise TimeoutError("SECOP no respondió")

    case = BY_ID["c02"]  # count/3
    flow, _verifier = accredited_flow(make_flow, frame_on, case["quote"])
    flow.secop_client = FailingSource([])

    result = run(flow, case["query"], chat_id="flex-failure")

    response = result["response"]
    assert "No encontré proveedores" not in response
    # El fallo total nombra la causa y su cláusula impide leerlo como un cero.
    assert "No interpreto ese silencio" in response
    assert result["entity_profile_view"]["suppliers"]["ranking"]["rows"] == {}


def test_an_actor_subject_is_never_stolen_by_suppliers(frame_on, make_flow):
    """ACTOR_STOLEN = 0: el turno del NIT no es atendido como vista de proveedores.

    La credencial puede existir (el modelo propuso el trabajo); lo que no puede
    ocurrir es que la capability ejecute el turno de un actor concreto.
    """
    case = BY_ID["ac01"]
    flow, _verifier = accredited_flow(make_flow, frame_on, case["quote"])

    result = run(flow, case["query"], chat_id="flex-actor")

    assert result["route_reason"] != "entity_profile_suppliers"
    assert result.get("admission_reason") == "other_owner_pending"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []
