from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

from app.core.analytics.contract_activity import handle_contract_activity
from app.core.analytics.contract_activity.models import DecisionKind
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter


_FROZEN_SHA256 = "b872a0ef206dfafd41d575f5a6a456032c494308867e57818ea0b046002fdc13"
_FROZEN_ACCEPTANCE = '''{"record_type":"meta","contract":"CONTRACT_ACTIVITY_V1_NARROW","version":2,"supersedes_sha256":"a09ad535f762eea12c7110a3dd6381268f9a293568f07228edabe1ce27df5439","corrections":{"NAR-003":"explicit BuyerUnit; bare Galapa was an invalid READY oracle","NAR-007":"explicit BuyerUnit; corpus may not inject absent buyer authority"},"case_count":30,"frozen_before_repair_writer":true}
{"id":"NAR-001","family":"TOTAL_TIME","raw":"¿Cuántos contratos firmó Galapa en agosto de 2026?","expected":"READY","operation":"TOTAL","calls":2}
{"id":"NAR-002","family":"TOTAL_TIME","raw":"Dime el total de contratos de Galapa durante 2025.","expected":"READY","operation":"TOTAL","calls":2}
{"id":"NAR-003","family":"TOTAL_NO_TIME","raw":"¿Cuántos contratos tiene el Municipio de Galapa?","expected":"READY","operation":"TOTAL","calls":2}
{"id":"NAR-004","family":"MODALITY_FILTER","raw":"¿Cuántos contratos por contratación directa firmó Galapa en 2026?","expected":"READY","operation":"TOTAL","calls":2}
{"id":"NAR-005","family":"GROUP_MODALITY","raw":"Desglosa los contratos de Galapa por modalidad.","expected":"READY","operation":"BY_MODALITY","calls":3}
{"id":"NAR-006","family":"GROUP_MODALITY","raw":"Distribuye por modalidad los contratos firmados por Galapa en 2025.","expected":"READY","operation":"BY_MODALITY","calls":3}
{"id":"NAR-007","family":"GROUP_STATE","raw":"Desglosa por estado reportado los contratos del Municipio de Galapa.","expected":"READY","operation":"BY_REPORTED_STATE","calls":3}
{"id":"NAR-008","family":"GROUP_STATE","raw":"Distribuye los contratos firmados por Galapa en 2025 según su estado actual.","expected":"READY","operation":"BY_REPORTED_STATE","calls":3}
{"id":"NAR-009","family":"MONEY_STRICT","raw":"¿Cuántos contratos superiores a 200 millones firmó Galapa en 2026?","expected":"READY","operation":"TOTAL","calls":2,"where_contains":"valor_del_contrato > 200000000"}
{"id":"NAR-010","family":"MONEY_INCLUSIVE","raw":"¿Cuántos contratos de al menos 200 millones firmó Galapa en 2026?","expected":"READY","operation":"TOTAL","calls":2,"where_contains":"valor_del_contrato >= 200000000"}
{"id":"NAR-011","family":"MONEY_UPPER","raw":"¿Cuántos contratos inferiores a 80 millones firmó Galapa en 2025?","expected":"READY","operation":"TOTAL","calls":2,"where_contains":"valor_del_contrato < 80000000"}
{"id":"NAR-012","family":"CURRENT_STATE_FILTER","raw":"¿Cuántos contratos de Galapa figuran actualmente suspendidos?","expected":"READY","operation":"TOTAL","calls":2}
{"id":"NAR-013","family":"AMBIGUOUS_UNIT","raw":"¿Cuántas contrataciones hizo Galapa en 2026?","expected":"CLARIFY","calls":0}
{"id":"NAR-014","family":"PROCESS_NEIGHBOR","raw":"¿Cuántos procesos publicó Galapa en 2026?","expected":"HANDOFF","calls":0}
{"id":"NAR-015","family":"SEARCH_NEIGHBOR","raw":"Lista los contratos de Galapa firmados en 2026.","expected":"HANDOFF","calls":0}
{"id":"NAR-016","family":"TOTAL_VALUE_NEIGHBOR","raw":"¿Cuánto suman los contratos de Galapa firmados en 2026?","expected":"HANDOFF","calls":0}
{"id":"NAR-017","family":"SUPPLIERS_NEIGHBOR","raw":"¿Qué proveedores contrataron con Galapa en 2026?","expected":"HANDOFF","calls":0}
{"id":"NAR-018","family":"MULTI_OPERATION","raw":"Cuenta los contratos de Galapa en 2026 y enuméralos.","expected":"CLARIFY","calls":0}
{"id":"NAR-019","family":"MULTI_OPERATION","raw":"Cuenta los contratos de Galapa en 2026 y agrúpalos por modalidad.","expected":"CLARIFY","calls":0}
{"id":"NAR-020","family":"NEGATED_OPERATION","raw":"No los agrupe por modalidad; cuente los contratos de Galapa en 2026.","expected":"CLARIFY","calls":0}
{"id":"NAR-021","family":"TWO_GROUPS","raw":"Desglosa los contratos de Galapa por modalidad y estado reportado.","expected":"CLARIFY","calls":0}
{"id":"NAR-022","family":"RANKING","raw":"Ordena las dos modalidades con más contratos de Galapa en 2026.","expected":"CLARIFY","calls":0}
{"id":"NAR-023","family":"HISTORICAL_STATE","raw":"¿Cuántos contratos de Galapa estaban suspendidos el 10 de octubre de 2024?","expected":"CLARIFY","calls":0}
{"id":"NAR-024","family":"UNKNOWN_MODALITY","raw":"¿Cuántos contratos de modalidad relámpago firmó Galapa en 2026?","expected":"CLARIFY","calls":0}
{"id":"NAR-025","family":"PARTIAL_TIME","raw":"¿Cuántos contratos firmó Galapa desde enero hasta?","expected":"CLARIFY","calls":0}
{"id":"NAR-026","family":"SHARED_DAY_RANGE_FOUNDATION","raw":"¿Cuántos contratos firmó Galapa desde el 28 hasta el 30 de junio de 2025?","expected":"CLARIFY","calls":0}
{"id":"NAR-027","family":"MONEY_FOUNDATION","raw":"¿Cuántos contratos por lo menos de 200 millones firmó Galapa en 2026?","expected":"CLARIFY","calls":0}
{"id":"NAR-028","family":"NO_BUYER","raw":"¿Cuántos contratos se firmaron en 2026?","expected":"CLARIFY","calls":0}
{"id":"NAR-029","family":"IDENTITY_FAILURE","raw":"¿Cuántos contratos firmó la alcaldía desconocida en 2026?","expected":"CLARIFY","calls":0}
{"id":"NAR-030","family":"MONEY_CONFLICT","raw":"¿Cuántos contratos mayores y menores a 200 millones firmó Galapa?","expected":"CLARIFY","calls":0}
'''

BUYER_PARAMS = {
    "entidad_resolved": "MUNICIPIO DE GALAPA.",
    "entidad_resolution": {
        "value": "MUNICIPIO DE GALAPA.",
        "method": "frozen_corpus_adapter",
        "kind": "buyer_entity",
        "confidence": "high",
        "like_value": None,
        "metadata": {},
    },
}


class CorpusClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def with_timeout(self, _seconds: int):
        return self

    def aggregate(self, dataset: str, sql: str) -> list[dict]:
        self.calls.append((dataset, sql))
        if "HAVING" in sql:
            return []
        if "GROUP BY modalidad_de_contratacion" in sql:
            return [
                {"group_label": "Contratación directa", "row_count": "2", "id_count": "2", "distinct_contract_count": "2"},
                {"row_count": "1", "id_count": "1", "distinct_contract_count": "1"},
            ]
        if "GROUP BY estado_contrato" in sql:
            return [
                {"group_label": "Suspendido", "row_count": "2", "id_count": "2", "distinct_contract_count": "2"},
                {"group_label": "", "row_count": "1", "id_count": "1", "distinct_contract_count": "1"},
            ]
        row = {"row_count": "3", "id_count": "3", "distinct_contract_count": "3"}
        if "count(modalidad_de_contratacion) AS group_non_null_count" in sql:
            row.update(group_non_null_count="2", distinct_group_count="1")
        elif "count(estado_contrato) AS group_non_null_count" in sql:
            row.update(group_non_null_count="3", distinct_group_count="2")
        return [row]


def _cases() -> list[dict]:
    assert hashlib.sha256(_FROZEN_ACCEPTANCE.encode()).hexdigest() == _FROZEN_SHA256
    records = [json.loads(line) for line in _FROZEN_ACCEPTANCE.splitlines()]
    meta, *cases = records
    assert meta == {
        "record_type": "meta",
        "contract": "CONTRACT_ACTIVITY_V1_NARROW",
        "version": 2,
        "supersedes_sha256": "a09ad535f762eea12c7110a3dd6381268f9a293568f07228edabe1ce27df5439",
        "corrections": {
            "NAR-003": "explicit BuyerUnit; bare Galapa was an invalid READY oracle",
            "NAR-007": "explicit BuyerUnit; corpus may not inject absent buyer authority",
        },
        "case_count": 30,
        "frozen_before_repair_writer": True,
    }
    assert len(cases) == meta["case_count"]
    return cases


def _adapt_params(case: dict) -> dict:
    return dict(BUYER_PARAMS) if "galapa" in case["raw"].lower() else {}


def test_frozen_acceptance_corpus() -> None:
    failures: list[str] = []
    for case in _cases():
        client = CorpusClient()
        decision = handle_contract_activity(case["raw"], _adapt_params(case), client)
        actual = "READY" if decision.kind is DecisionKind.ANSWER else decision.kind.value
        observations = {
            "expected": actual,
            "calls": len(client.calls),
            "operation": decision.request.operation.value if decision.request else None,
        }
        for field in ("expected", "calls", "operation"):
            if field in case and observations[field] != case[field]:
                failures.append(
                    f"{case['id']} {field}: {observations[field]!r} != {case[field]!r}"
                )
        if case.get("where_contains") and (
            decision.plan is None or case["where_contains"] not in decision.plan.where
        ):
            failures.append(f"{case['id']} missing WHERE fragment")
    assert failures == []


def test_full_v2_corpus_through_product_query_router() -> None:
    router = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))
    failures: list[str] = []
    for case in _cases():
        params = router.parse(case["raw"], today=date(2026, 9, 20)).params
        client = CorpusClient()
        decision = handle_contract_activity(case["raw"], params, client)
        actual = "READY" if decision.kind is DecisionKind.ANSWER else decision.kind.value
        observations = {
            "expected": actual,
            "calls": len(client.calls),
            "operation": decision.request.operation.value if decision.request else None,
        }
        for field in ("expected", "calls", "operation"):
            if field in case and observations[field] != case[field]:
                failures.append(
                    f"{case['id']} {field}: {observations[field]!r} != {case[field]!r}"
                )
        if case.get("where_contains") and (
            decision.plan is None or case["where_contains"] not in decision.plan.where
        ):
            failures.append(f"{case['id']} missing WHERE fragment")
    assert failures == []
