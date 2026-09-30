"""ADR-102 §13: product gates for the promoted Foundation route (regression corpus, not held-out)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from app.core.foundation_producers import decode_proposal
from app.core.foundation_projection import SOURCE_SELECT_FIELDS
from app.core.soql_builder import SoQLBuilder
from tests.test_foundation_messages import assert_public
from tests.test_foundation_silent_paths import NOW

CORPUS = yaml.safe_load((Path(__file__).parent / "fixtures" / "foundation_v3_regression_corpus.yaml").read_text())
CANONICAL = "contratos de primera infancia de la alcaldía de barranquilla"
BARRANQUILLA = "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"


def _rows():
    contract = dict.fromkeys({*SoQLBuilder.SPECS[SoQLBuilder.CONTRATOS_DATASET].select_fields,
                              *SOURCE_SELECT_FIELDS[SoQLBuilder.CONTRATOS_DATASET]})
    process = dict.fromkeys({*SoQLBuilder.SPECS[SoQLBuilder.PROCESOS_DATASET].select_fields,
                             *SOURCE_SELECT_FIELDS[SoQLBuilder.PROCESOS_DATASET]})
    return {
        SoQLBuilder.CONTRATOS_DATASET: [
            {**contract, "id_contrato": "K-PI", "nombre_entidad": BARRANQUILLA, "estado_contrato": "En ejecución",
             "objeto_del_contrato": "ATENCION INTEGRAL A LA PRIMERA INFANCIA", "fecha_de_firma": "2025-03-01",
             "valor_del_contrato": "400000000"},
            # Would match a LIKE '%contratos%' reading and must never be returned for the canonical case.
            {**contract, "id_contrato": "K-ADM", "nombre_entidad": BARRANQUILLA, "estado_contrato": "En ejecución",
             "objeto_del_contrato": "ADMINISTRACION DE CONTRATOS DE PERSONAL", "fecha_de_firma": "2025-02-01",
             "valor_del_contrato": "10000000"},
        ],
        SoQLBuilder.PROCESOS_DATASET: [
            {**process, "id_del_proceso": "P-PI", "entidad": BARRANQUILLA,
             "nombre_del_procedimiento": "PRIMERA INFANCIA", "fecha_de_publicacion_del": "2026-09-20",
             "estado_del_procedimiento": "Publicado", "estado_de_apertura_del_proceso": "Abierto"},
        ],
    }


def _span(raw, quote):
    text, _, nth = quote.partition("#")
    at = -1
    for _ in range(int(nth or 1)):
        at = raw.index(text, at + 1)
    return {"start": at, "end": at + len(text), "quote": text}


def _proposal(case):
    raw = case["raw"]
    requests, conditions = [], []
    by_request = {request["id"]: [] for request in case["requests"]}
    for at, item in enumerate(case["conditions"]):
        dimension, operator, quotes, polarity, *owners = item
        owners = owners[0] if owners else [case["requests"][0]["id"]]
        cid = f"c{at}"
        conditions.append({"condition_id": cid, "request_ids": owners, "dimension": dimension, "operator": operator,
                           "polarity": polarity, "evidence": [_span(raw, quote) for quote in quotes]})
        for owner in owners:
            by_request[owner].append(cid)
    scopes = {}
    for request in case["requests"]:
        atoms = [{"operator": "ATOM", "condition_id": cid} for cid in by_request[request["id"]]]
        expression = (None if not atoms else atoms[0] if len(atoms) == 1 else {"operator": "AND", "children": atoms})
        scope = request.get("scope", request["id"])
        scopes.setdefault(scope, []).append(request["id"])
        requests.append({"request_id": request["id"], "scope_id": scope, "operation": "SEARCH",
                         "subject": request["subject"], "universe_kind": request.get("kind", "records"),
                         "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                         "universe_evidence": _span(raw, request["universe"]),
                         **({"condition_expression": expression} if expression else {})})
    relations = [{"kind": "SHARES_SCOPE", "members": members} for members in scopes.values() if len(members) > 1]
    return decode_proposal({"requests": requests, "conditions": conditions, "relations": relations}, raw, arm="C")


def _harness(tmp_path, monkeypatch, producer, mode="foundation_v3", failures=()):
    from tests.product_contract.harness import ProductHarness

    return ProductHarness(tmp_path, monkeypatch, rows_by_dataset=_rows(), failures=list(failures),
                          reference_instant=NOW, today=NOW.date(), foundation_producer=producer,
                          settings_overrides={"secoppal_foundation_mode": mode})


def _host_values(result, dimension):
    acceptance = result.get("foundation_acceptance") or {}
    conditions = {c["condition_id"]: c for c in (acceptance.get("proposal") or {}).get("conditions", [])}
    return [value for resolution in acceptance.get("resolutions", [])
            if conditions.get(resolution["target_id"], {}).get("dimension") == dimension
            for value in resolution["resolved_ids"]]


@pytest.mark.parametrize("case", CORPUS["cases"], ids=[case["id"] for case in CORPUS["cases"]])
def test_regression_corpus_through_the_real_route(tmp_path, monkeypatch, case):
    expect = case["expect"]
    producer = Mock(side_effect=lambda raw: _proposal(case))
    harness = _harness(tmp_path, monkeypatch, producer)
    turn = harness.say(case["raw"])
    result = turn.result
    if expect["route"] == "legacy":
        assert producer.call_count == 0 and "foundation_mode" not in result
        return
    assert producer.call_count == 1 and result["foundation_mode"] == "foundation_v3"
    assert result["route_reason"] != "heuristic_only"
    assert result["foundation_terminal"] == expect["terminal"], result.get("foundation_internal_reason")
    if "reason" in expect:
        assert result["foundation_internal_reason"] == expect["reason"]
    assert_public(result["response"])
    queries = [call["soql"].lower() for call in turn.transport_calls if call["operation"] == "query"]
    assert len(turn.transport_calls) == expect["calls"]
    if "datasets" in expect:
        assert [group["dataset"] for group in result["groups"]] == expect["datasets"]
    if "topics" in expect:
        assert _host_values(result, "TOPIC") == expect["topics"]
    if "buyer" in expect:
        assert _host_values(result, "BUYER") == [expect["buyer"]]
    for word in expect.get("objeto_not_contains", []):
        assert all(word not in topic for topic in _host_values(result, "TOPIC")), word
    for fragment in expect.get("soql_contains", []):
        assert all(fragment.lower() in soql for soql in queries), fragment
    for fragment in expect.get("soql_not_contains", []):
        assert all(fragment.lower() not in soql for soql in queries), fragment
    if "row_ids" in expect:
        assert [row_id for group in result["groups"] for row_id in group["row_ids"]] == expect["row_ids"]


def _canonical_case():
    return next(case for case in CORPUS["cases"] if case["id"] == "FI-001")


def test_first_infancy_gate_flag_off_keeps_legacy(tmp_path, monkeypatch):
    producer = Mock(side_effect=lambda raw: _proposal(_canonical_case()))
    turn = _harness(tmp_path, monkeypatch, producer, mode="legacy").say(CANONICAL)
    assert producer.call_count == 0
    assert "foundation_mode" not in turn.result and "source_call_receipts" not in turn.result


def test_first_infancy_gate_never_reads_a_technical_zero_as_no_contracts(tmp_path, monkeypatch):
    from tests.product_contract.frozen_source import InjectedFailure

    failure = InjectedFailure(lambda: TimeoutError("source timeout"), operation="query")
    turn = _harness(tmp_path, monkeypatch, lambda raw: _proposal(_canonical_case()),
                    failures=[failure, InjectedFailure(lambda: TimeoutError("again"), operation="query")]).say(CANONICAL)
    result = turn.result
    assert result["foundation_terminal"] == "SOURCE_ERROR" and result["total_count"] is None
    assert "No afirmo que no existan resultados" in result["response"]
    assert "0 filas" not in result["response"] and "total 0" not in result["response"]
    assert_public(result["response"])
