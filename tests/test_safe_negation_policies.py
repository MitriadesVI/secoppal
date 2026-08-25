"""HCA-R4-S3: fail-closed policies for unsupported bound negations."""
from __future__ import annotations

import pytest

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.negation_binding import (
    NegationDimension,
    NegationRequest,
    NegationVerdict,
)
from app.core.negation_policy import apply_negation_policy
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter


def _request(dimension: NegationDimension, value: str) -> NegationRequest:
    return NegationRequest(
        dimension=dimension,
        value=value,
        value_start=0,
        value_end=len(value),
        marker="menos",
        marker_start=0,
        marker_end=5,
        verdict=NegationVerdict.DEFER,
    )


def _workflow(tmp_path):
    workflow = SecopalWorkflow(Settings())
    calls: list[tuple[str, str]] = []

    def query(dataset_id, soql_query, **_):
        calls.append(("query", dataset_id))
        return [{
            "nombre_del_procedimiento": "Aseo integral",
            "objeto_del_contrato": "Aseo integral",
            "entidad": "ENTIDAD CONTROL",
            "nombre_entidad": "ENTIDAD CONTROL",
            "precio_base": "1000000",
            "valor_del_contrato": "1000000",
            "estado_de_apertura_del_proceso": "Abierto",
            "estado_contrato": "Activo",
            "fecha_de_publicacion_del": "2026-01-01T00:00:00.000",
            "fecha_de_firma": "2026-01-01T00:00:00.000",
            "referencia_del_proceso": "R4-S3-1",
            "id_contrato": "R4-S3-C1",
        }]

    def count(dataset_id, soql_query, **_):
        calls.append(("count", dataset_id))
        return 1

    workflow.secop_client.query = query  # type: ignore[method-assign]
    workflow.secop_client.count = count  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **_: None  # type: ignore[method-assign]
    workflow.conv_store = ConversationStore(tmp_path / "conversations")
    return workflow, calls


@pytest.mark.parametrize(
    "query,dimension",
    [
        *[
            (f"contratos de aseo {marker} barranquilla", NegationDimension.CITY)
            for marker in (
                "que no sean de", "que no sea de", "menos", "excepto",
                "salvo", "excluyendo", "pero no", "sin",
            )
        ],
        *[
            (f"contratos de aseo {marker} 2025", NegationDimension.TIME)
            for marker in (
                "que no sean de", "que no sea de", "menos", "excepto",
                "salvo", "excluyendo", "pero no", "sin",
            )
        ],
        *[
            (
                f"contratos {marker} la alcaldia de barranquilla",
                NegationDimension.BUYER,
            )
            for marker in (
                "que no sean de", "menos", "excepto", "salvo",
                "excluyendo", "pero no", "sin",
            )
        ],
        *[
            (
                f"contratos donde el proveedor {marker} union temporal aseo",
                NegationDimension.ACTOR,
            )
            for marker in (
                "no sea", "menos", "excepto", "salvo", "excluyendo",
                "pero no", "sin",
            )
        ],
    ],
)
def test_adversarial_marker_forms_bind_to_safe_dimension(query, dimension) -> None:
    parsed = QueryRouter().parse(query)
    request = parsed.negation_requests[0]
    assert request.dimension is dimension
    assert request.verdict is NegationVerdict.CLARIFY
    assert not any(parsed.params.get(key) for key in {
        "ciudad", "ciudad_resolved", "entidad", "entidad_resolved",
        "fecha_hasta", "actor_scope", "actor_resolution", "actor_pending",
        "contratista",
    })


@pytest.mark.parametrize(
    "query,dimension,forbidden",
    [
        (
            "contratos de aseo que no sean de barranquilla",
            NegationDimension.CITY,
            {"ciudad", "ciudad_resolved", "entidad_resolved", "entidad_ecosystem"},
        ),
        (
            "contratos desde 2024 menos los de 2025",
            NegationDimension.TIME,
            {"fecha_hasta"},
        ),
        (
            "contratos que no sean de la alcaldia de barranquilla",
            NegationDimension.BUYER,
            {"entidad", "entidad_resolved"},
        ),
        (
            "contratos donde el proveedor no sea union temporal aseo",
            NegationDimension.ACTOR,
            {"actor_pending", "actor_scope", "actor_resolution", "contratista"},
        ),
        (
            "contratos donde el proveedor pero no union temporal aseo",
            NegationDimension.ACTOR,
            {
                "actor_pending", "actor_scope", "actor_resolution",
                "contratista", "objeto_exclude",
            },
        ),
        (
            "excepto contratos de obra",
            NegationDimension.CONTRACT_TYPE,
            {"contract_type_include"},
        ),
        (
            "procesos que no sean abiertos",
            NegationDimension.STATE,
            {"estado", "estado_family", "estado_del_procedimiento"},
        ),
    ],
)
def test_unsupported_negations_clarify_without_source_or_positive_leak(
    tmp_path, query, dimension, forbidden,
) -> None:
    workflow, calls = _workflow(tmp_path)

    # These collaborators are upstream of QueryPlan and must also remain idle.
    workflow.llm_handler.parse = lambda *_args, **_kwargs: pytest.fail(  # type: ignore[method-assign]
        "unsupported negation called the LLM"
    )
    workflow.actor_name_resolver.resolve = lambda *_args, **_kwargs: pytest.fail(  # type: ignore[method-assign]
        "unsupported negation opened actor candidates"
    )
    workflow.actor_name_resolver.resolve_exact = lambda *_args, **_kwargs: pytest.fail(  # type: ignore[method-assign]
        "unsupported negation probed the actor directory"
    )

    result = workflow.run_query(query, channel="streamlit")

    assert result["needs_clarification"] is True
    assert result["query_plan"]["reason"] == "unsupported_negation"
    assert result["soql_query"] == ""
    assert calls == []
    assert "Entendí" in result["response"]
    assert all(not result["resolved_params"].get(key) for key in forbidden)

    parsed = QueryRouter().parse(query)
    request = parsed.negation_requests[0]
    assert request.dimension is dimension
    assert request.verdict is NegationVerdict.CLARIFY


def test_time_direct_contradiction_is_blocked_without_positive_range(tmp_path) -> None:
    workflow, calls = _workflow(tmp_path)
    result = workflow.run_query(
        "contratos de aseo en 2025 menos los de 2025",
        channel="streamlit",
    )

    assert result["needs_clarification"] is True
    assert result["query_plan"]["reason"] == "dimensional_negation_contradiction"
    assert result["parsed_params"].get("fecha_desde") is None
    assert result["parsed_params"].get("fecha_hasta") is None
    assert calls == []
    assert "incluye y excluye" in result["response"]


def test_city_direct_contradiction_scrubs_only_city_derived_entity(tmp_path) -> None:
    workflow, calls = _workflow(tmp_path)
    result = workflow.run_query(
        "contratos de aseo en barranquilla menos barranquilla",
        channel="streamlit",
    )
    assert result["needs_clarification"] is True
    assert result["query_plan"]["reason"] == "dimensional_negation_contradiction"
    assert not any(result["resolved_params"].get(key) for key in {
        "ciudad", "ciudad_resolved", "entidad", "entidad_resolved",
        "entidad_resolution", "entidad_ecosystem",
    })
    assert calls == []


def test_city_contradiction_preserves_independent_buyer() -> None:
    request = _request(NegationDimension.CITY, "barranquilla")
    params = {
        "ciudad": "Barranquilla",
        "entidad": "Gobernacion del Atlantico",
        "entidad_resolved": "GOBERNACION DEL ATLANTICO",
        "entidad_resolution": {"method": "rewrite_gobernacion"},
    }
    result = apply_negation_policy((request,), params)
    assert result.requests[0].verdict is NegationVerdict.CONTRADICTORY
    assert "ciudad" not in result.params
    assert result.params["entidad_resolved"] == "GOBERNACION DEL ATLANTICO"


def test_department_subtracts_only_from_existing_canonical_set(tmp_path) -> None:
    workflow, calls = _workflow(tmp_path)
    result = workflow.run_query(
        "contratos de aseo en la costa caribe menos atlantico",
        channel="streamlit",
    )

    assert result["needs_clarification"] is False
    assert calls
    assert result["parsed_params"]["departamentos_in"] == [
        "Bolívar", "Cesar", "Córdoba", "La Guajira", "Magdalena", "Sucre",
    ]
    assert "departamento IN (" in result["soql_query"]
    assert "NOT IN" not in result["soql_query"]
    assert "Atlántico" not in result["soql_query"]


def test_department_without_positive_set_clarifies_without_source(tmp_path) -> None:
    workflow, calls = _workflow(tmp_path)
    result = workflow.run_query("contratos excepto atlantico", channel="streamlit")
    assert result["needs_clarification"] is True
    assert result["query_plan"]["reason"] == "unsupported_negation"
    assert calls == []


def test_department_direct_contradiction_leaves_no_territory_operation(tmp_path) -> None:
    workflow, calls = _workflow(tmp_path)
    result = workflow.run_query(
        "contratos de aseo en atlantico menos atlantico",
        channel="streamlit",
    )
    assert result["needs_clarification"] is True
    assert result["query_plan"]["reason"] == "dimensional_negation_contradiction"
    assert not any(result["resolved_params"].get(key) for key in {
        "departamento", "departamento_resolved", "departamento_resolution",
        "departamentos_in", "region", "actor_conversation_operation",
        "actor_conversation_construction",
    })
    assert calls == []


def test_policy_rolls_back_department_subtraction_when_another_request_blocks() -> None:
    original = {
        "departamentos_in": ["Atlántico", "Bolívar", "Magdalena"],
        "objeto": ["aseo"],
    }
    result = apply_negation_policy(
        (
            _request(NegationDimension.DEPARTMENT, "atlantico"),
            _request(NegationDimension.CITY, "barranquilla"),
        ),
        original,
    )
    assert result.blocks_execution is True
    assert result.params == original
    assert result.requests[0].verdict is NegationVerdict.EXECUTE
    assert result.requests[1].verdict is NegationVerdict.CLARIFY


@pytest.mark.parametrize(
    "first,negative",
    [
        ("contratos de aseo en atlantico", "menos barranquilla"),
        ("contratos desde 2024", "menos los de 2025"),
    ],
)
def test_followup_clarification_persists_exact_confirmed_context(
    tmp_path, first, negative,
) -> None:
    workflow, calls = _workflow(tmp_path)
    chat_id = "transactional-negation"
    invalidations: list[str] = []
    original_invalidate = workflow.conv_store.invalidate_pagination_buffers

    def record_invalidation(current_chat_id):
        invalidations.append(current_chat_id)
        return original_invalidate(current_chat_id)

    workflow.conv_store.invalidate_pagination_buffers = record_invalidation  # type: ignore[method-assign]

    workflow.run_query(first, channel="streamlit", chat_id=chat_id)
    confirmed = dict(workflow.conv_store.get_history(chat_id, last_n=1)[0].parsed_params)
    source_calls_before = len(calls)
    invalidations.clear()

    result = workflow.run_query(negative, channel="streamlit", chat_id=chat_id)
    persisted = workflow.conv_store.get_history(chat_id, last_n=1)[0].parsed_params

    assert result["needs_clarification"] is True
    assert result["parsed_params"] == confirmed
    assert result["resolved_params"] == confirmed
    assert persisted == confirmed
    assert len(calls) == source_calls_before
    assert invalidations == []


def test_department_followup_subtracts_confirmed_region_set(tmp_path) -> None:
    workflow, calls = _workflow(tmp_path)
    chat_id = "department-set"
    workflow.run_query(
        "contratos de aseo en la costa caribe",
        channel="streamlit",
        chat_id=chat_id,
    )
    source_calls_before = len(calls)

    result = workflow.run_query(
        "menos atlantico",
        channel="streamlit",
        chat_id=chat_id,
    )

    assert result["needs_clarification"] is False
    assert len(calls) > source_calls_before
    assert "Atlántico" not in result["resolved_params"]["departamentos_in"]
    assert "NOT IN" not in result["soql_query"]


def test_supported_object_and_modality_keep_execution_authority(tmp_path) -> None:
    workflow, calls = _workflow(tmp_path)
    object_result = workflow.run_query("vigilancia sin alimentacion", channel="streamlit")
    modality_result = workflow.run_query(
        "procesos de obra sin contratacion directa", channel="streamlit",
    )
    assert object_result["needs_clarification"] is False
    assert object_result["parsed_params"]["objeto_exclude"] == ["alimentacion"]
    assert modality_result["needs_clarification"] is False
    assert modality_result["parsed_params"]["modalidad_exclude_procedure_ids"]
    assert calls


@pytest.mark.parametrize(
    "query,forbidden",
    [
        (
            "contratos de aseo por mas de 500 millones menos barranquilla",
            {"ciudad", "entidad_resolved", "entidad_ecosystem"},
        ),
        (
            "procesos de obra sin contratacion directa menos barranquilla",
            {"ciudad", "entidad_resolved", "entidad_ecosystem"},
        ),
        (
            "contratos de aseo en atlantico menos barranquilla",
            {"ciudad", "entidad_resolved", "entidad_ecosystem"},
        ),
        (
            "contratos de aseo sin transporte menos los de 2025",
            {"fecha_desde", "fecha_hasta"},
        ),
        (
            "contratos del proveedor union temporal aseo menos barranquilla",
            {"ciudad", "entidad_resolved", "actor_pending"},
        ),
    ],
)
def test_unsupported_negation_blocks_orthogonal_partial_execution(
    tmp_path, query, forbidden,
) -> None:
    workflow, calls = _workflow(tmp_path)
    result = workflow.run_query(query, channel="streamlit")
    assert result["needs_clarification"] is True
    assert result["query_plan"]["reason"] == "unsupported_negation"
    assert calls == []
    assert result["soql_query"] == ""
    assert all(not result["resolved_params"].get(key) for key in forbidden)
