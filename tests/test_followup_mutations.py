"""Etapa 2: mutaciones conversacionales tipadas y trazables."""

from __future__ import annotations

import re

import pytest

from app.core.followup_engine import detect_and_merge
from app.core.followup_mutations import MUTATION_POLICIES, MutationPolicy, gate_delta
from app.core.query_frame import frame_from_params
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter()


def _followup(
    router: QueryRouter,
    query: str,
    previous: dict,
) -> tuple[dict, dict]:
    trace: dict = {}
    _intent, merged = detect_and_merge(
        text=query,
        current_params=router.parse(query).params,
        previous_frame=frame_from_params(previous),
        mutation_trace=trace,
    )
    return merged, trace


def _where(soql: str) -> str:
    return re.search(r"\bWHERE\s+(.*?)\s+ORDER BY\b", soql, re.I | re.S).group(1)


def test_policy_has_distinct_set_clear_and_transition_cleanup() -> None:
    policy = MUTATION_POLICIES["change_dataset"]
    assert isinstance(policy, MutationPolicy)
    assert "dataset" in policy.allowed_set
    assert "fecha_desde" in policy.allowed_clear
    assert policy.transition_cleanup == "normalize_dataset_transition"


@pytest.mark.parametrize(
    ("query", "expected_order"),
    [
        ("ponlos del más caro al más barato", "valor_desc"),
        ("déjame solo los recientes", "fecha_desc"),
    ],
)
def test_change_order_preserves_where_and_rejects_control_topic(
    router: QueryRouter,
    query: str,
    expected_order: str,
) -> None:
    previous = {
        "dataset": "contratos",
        "objeto": ["mantenimiento", "ascensores"],
        "ciudad": "Medellin",
        "valor_min": 10_000_000,
    }
    builder = SoQLBuilder()
    dataset_id = builder.dataset_id_for(previous["dataset"])
    before = builder.build(dataset_id, previous)

    merged, trace = _followup(router, query, previous)
    after = builder.build(dataset_id, merged)

    assert merged["objeto"] == previous["objeto"]
    assert merged["ordering_signal"] == expected_order
    assert _where(before) == _where(after)  # INV-1
    assert "objeto" in trace["requested_fields"] or not router.parse(query).params.get("objeto")
    if router.parse(query).params.get("objeto"):
        assert "objeto" in trace["rejected_fields"]
        assert trace["rejection_reasons"]["objeto"] == "field_not_allowed_for_intent"


def test_change_dataset_cleans_incompatible_fields_before_apply(router: QueryRouter) -> None:
    previous = {
        "dataset": "procesos",
        "objeto": ["atencion", "integral"],
        "fecha_desde": "2026-01-01",
        "fecha_hasta": "2026-12-31",
        "estado_de_apertura_del_proceso": ["Abierto"],
        "offset": 20,
    }
    merged, trace = _followup(router, "regresa a los contratos", previous)

    assert merged["dataset"] == "contratos"
    assert merged["objeto"] == previous["objeto"]
    for field in ("fecha_desde", "fecha_hasta", "estado_de_apertura_del_proceso", "offset"):
        assert field not in merged
        assert field in trace["transition_cleared_fields"]
    assert trace["accepted_fields"] == ["dataset", "dataset_explicit"]
    assert trace["rejected_fields"] == ["objeto"]


def test_remove_range_clears_values_without_replacing_topic(router: QueryRouter) -> None:
    previous = {
        "dataset": "procesos",
        "objeto": ["mantenimiento", "ascensores"],
        "valor_min": 100_000_000,
        "valor_max": 150_000_000,
    }
    merged, trace = _followup(router, "elimina ese rango", previous)

    assert merged["objeto"] == previous["objeto"]
    assert "valor_min" not in merged
    assert "valor_max" not in merged
    assert trace["accepted_fields"] == []
    assert trace["transition_cleared_fields"] == ["valor_max", "valor_min"]
    assert "objeto" in trace["rejected_fields"]


def test_pagination_control_does_not_mutate_topic(router: QueryRouter) -> None:
    previous = {
        "dataset": "procesos",
        "objeto": ["dotacion", "hospitalaria"],
        "offset": 0,
        "limit": 10,
    }
    merged, trace = _followup(router, "muéstrame los siguientes", previous)

    assert merged["objeto"] == previous["objeto"]
    assert merged["offset"] == 10
    assert "objeto" in trace["rejected_fields"]


def test_exclude_topic_is_own_field_and_builds_negative_where(router: QueryRouter) -> None:
    previous = {
        "dataset": "procesos",
        "objeto": ["mantenimiento", "ascensores"],
        "departamento_resolved": "Antioquia",
    }
    merged, trace = _followup(router, "ahora excluye interventorías", previous)

    assert merged["objeto"] == previous["objeto"]
    assert merged["objeto_exclude"] == ["interventoria"]
    frame = frame_from_params(merged)
    assert frame.objeto_exclude == ["interventoria"]
    assert "objeto_exclude" in trace["accepted_fields"]
    soql = SoQLBuilder().build(SoQLBuilder.PROCESOS_DATASET, merged)
    assert "NOT (" in soql
    assert "INTERVENTORIA" in soql.upper()


def test_legitimate_topic_replacement_is_accepted(router: QueryRouter) -> None:
    previous = {
        "dataset": "procesos",
        "objeto": ["dotacion", "hospitalaria"],
        "departamento_resolved": "Antioquia",
        "valor_max": 50_000_000,
    }
    merged, trace = _followup(router, "ahora busca mantenimiento de ascensores", previous)

    assert merged["objeto"] == ["mantenimiento", "ascensores"]
    assert merged["departamento_resolved"] == "Antioquia"
    assert "valor_max" not in merged
    assert trace["accepted_fields"] == ["objeto"]
    assert trace["transition_cleared_fields"] == ["valor_max"]


def test_state_family_is_normalized_to_inherited_dataset(router: QueryRouter) -> None:
    previous = {"dataset": "contratos", "objeto": ["mantenimiento"]}
    merged, trace = _followup(router, "solo los activos", previous)

    assert merged["estado_family"] == "contrato_activo"
    assert merged["estado_field"] == "estado_contrato"
    assert merged["estado_contrato"] == ["En ejecución", "Modificado", "Prorrogado"]
    assert "estado_de_apertura_del_proceso" not in merged
    assert "estado_del_procedimiento" not in merged
    assert "estado_contrato" in trace["accepted_fields"]


def test_double_defense_rejects_control_topic_even_under_topic_intent() -> None:
    gated = gate_delta(
        intent="contextual_requery",
        text="ordénalos por mayor valor",
        delta={"objeto": ["ordenalos"], "ordering_signal": "valor_desc"},
        previous_params={"dataset": "procesos", "objeto": ["adulto_mayor"]},
    )
    trace = gated.trace("contextual_requery")
    assert gated.cleaned_previous["objeto"] == ["adulto_mayor"]
    assert "objeto" not in gated.accepted_delta
    assert "objeto" in trace["rejected_fields"]
    assert trace["rejection_reasons"]["objeto"] == "control_only_without_new_domain_topic"


def test_unknown_intent_preserves_topic_conservatively() -> None:
    gated = gate_delta(
        intent="unclear",
        text="eso otro",
        delta={"objeto": ["otro"]},
        previous_params={"dataset": "procesos", "objeto": ["mantenimiento"]},
    )
    assert gated.accepted_delta == {}
    assert gated.cleaned_previous["objeto"] == ["mantenimiento"]
    assert gated.rejection_reasons == {"objeto": "field_not_allowed_for_intent"}


@pytest.mark.parametrize(
    "control_query",
    ["ordénalos por mayor valor", "regresa a los contratos", "elimina ese rango"],
)
def test_rejected_control_topic_does_not_persist_three_turns(
    router: QueryRouter,
    control_query: str,
) -> None:
    first = {"dataset": "procesos", "objeto": ["mantenimiento", "ascensores"]}
    second, trace = _followup(router, control_query, first)
    third, _ = _followup(router, "ahora en Medellín", second)

    assert second["objeto"] == first["objeto"]
    assert third["objeto"] == first["objeto"]
    assert not ({"ordenalos", "regresa", "elimina", "rango"} & set(third["objeto"]))
    assert "objeto" in trace["rejected_fields"]
