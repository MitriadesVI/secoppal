"""registry.py — orquestador genérico de la capa analítica.

REGISTRY es la lista ORDENADA de analyzers (el orden = precedencia ante colisión).
maybe_handle_analytical_query itera el registry: el primer analyzer cuyo detect()
matchea gobierna el turno. La lógica de scope-guard / ejecución / error vive aquí
una sola vez (_run), no duplicada por analyzer.

Añadir un análisis nuevo = importar su Analyzer y añadirlo a REGISTRY. Nada más.
"""

from __future__ import annotations

from datetime import date as Date
from typing import Any

from app.core.analytics.aggregate_sum import AggregateSumAnalyzer
from app.core.analytics.base import BaseAnalyzer, has_analytics_scope
from app.core.analytics.contract_activity import (
    as_analytics_payload,
    handle_contract_activity,
)
from app.core.analytics.execution import ExecutionAnalyzer
from app.core.analytics.ranking import RankingAnalyzer
from app.core.analytics.temporal import TemporalAnalyzer
from app.core.analytics.value_stats import ValueStatsAnalyzer

# Orden = precedencia. Analyzers más específicos primero; aggregate_sum (la suma
# general de contratación) va al final como caso más amplio. Los detect son
# disjuntos (tests de no-colisión lo congelan), así que el orden es defensivo.
REGISTRY: list[BaseAnalyzer] = [
    ExecutionAnalyzer(),
    ValueStatsAnalyzer(),
    RankingAnalyzer(),
    TemporalAnalyzer(),
    AggregateSumAnalyzer(),
]

LEGACY_CONTRACT_ACTIVITY_DIRECT = object()


def detect_analytical_intent(query: str, params: dict | None = None) -> str | None:
    """Devuelve el intent del primer analyzer que reconoce la query, o None."""
    p = params or {}
    for analyzer in REGISTRY:
        if analyzer.detect(query, p):
            return analyzer.intent
    return None


def _run(
    analyzer: BaseAnalyzer,
    params: dict,
    dataset_id: str,
    secop_client: Any,
    soql_builder: Any,
) -> dict:
    """Ejecuta UN analyzer: scope-guard → build → aggregate → format, con manejo
    uniforme de clarificación y error. route_reason = analytics_{intent}[_sufijo]."""
    intent = analyzer.intent

    if analyzer.requires_scope and not has_analytics_scope(params):
        return {
            "needs_clarification": True,
            "clarification_reason": analyzer.scope_hint,
            "analytical_intent": intent,
            "route_reason": f"analytics_{intent}_no_scope",
        }

    soql = ""
    try:
        soql = analyzer.build(soql_builder, dataset_id, params)
        rows = secop_client.with_timeout(10).aggregate(dataset_id, soql)
        response = analyzer.format(rows, params, dataset_id)
        return {
            "response": response,
            "soql_query": soql,
            "analytical_intent": intent,
            "route_reason": f"analytics_{intent}",
            "dataset_id": dataset_id,
            "results": [],
            "total_count": analyzer.total_count(rows),
            "needs_clarification": False,
        }
    except Exception:
        return {
            "needs_clarification": True,
            "clarification_reason": analyzer.error_hint,
            "soql_query": soql,
            "analytical_intent": intent,
            "route_reason": f"analytics_{intent}_error",
        }


def maybe_handle_analytical_query(
    user_query: str,
    params: dict,
    dataset_id: str,
    secop_client: Any,
    soql_builder: Any,
    channel: str = "whatsapp",
    *,
    capability_binding: object = LEGACY_CONTRACT_ACTIVITY_DIRECT,
    today: Date | None = None,
    semantic_evidence: object = None,
) -> dict | None:
    """Punto de entrada controlado para queries analíticas.

    No infla run_query. Retorna None si ningún analyzer reconoce la query.
    """
    from app.core.capability_binding.contract_activity_binding import (
        ContractActivityBinding,
    )

    if (
        (capability_binding is LEGACY_CONTRACT_ACTIVITY_DIRECT and semantic_evidence is not None)
        or isinstance(capability_binding, ContractActivityBinding)
    ):
        contract_activity = handle_contract_activity(
            user_query,
            params,
            secop_client,
            binding=(
                capability_binding
                if isinstance(capability_binding, ContractActivityBinding)
                else None
            ),
            today=today,
            semantic_evidence=semantic_evidence,
        )
        contract_activity_payload = as_analytics_payload(contract_activity)
        if contract_activity_payload is not None:
            return contract_activity_payload
    for analyzer in REGISTRY:
        if analyzer.detect(user_query, params):
            return _run(analyzer, params, dataset_id, secop_client, soql_builder)
    return None
