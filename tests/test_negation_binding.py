"""HCA-R4-S1: dimensional negation binding and positive-span isolation."""
from __future__ import annotations

import inspect

import pytest

from app.core import modality_alternatives, modality_composition, modality_scope
from app.core.negation_binding import (
    NegationDimension,
    NegationVerdict,
)
from app.core.negation_markers import (
    ANTI_NEGATION_PATTERNS,
    EXTERNAL_NEGATIVE_OPERATORS,
    has_external_negative_operator,
)
from app.core.query_router import QueryRouter


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter()


def _requests(router: QueryRouter, query: str):
    return router.parse(query).negation_requests


def _request(router: QueryRouter, query: str):
    requests = _requests(router, query)
    assert len(requests) == 1
    return requests[0]


def test_shared_markers_are_the_modality_source_of_truth() -> None:
    assert EXTERNAL_NEGATIVE_OPERATORS == (
        "que no sean", "que no sea", "diferentes de", "diferente de",
        "distintos de", "distinto de", "excluyendo", "excepto", "salvo",
        "no", "sin",
    )
    assert ANTI_NEGATION_PATTERNS == (
        "no solo", "sin descartar", "sin excluir",
        "no quiero excluir", "no busco excluir",
    )
    assert has_external_negative_operator(" pero sin ") is True
    assert has_external_negative_operator(" sin descartar ") is False
    alternatives_source = inspect.getsource(modality_alternatives)
    composition_source = inspect.getsource(modality_composition)
    scope_source = inspect.getsource(modality_scope)
    assert "_EXTERNAL_NEGATIVE_OPERATORS =" not in alternatives_source
    assert "_ANTI_NEGATION_PATTERNS =" not in alternatives_source
    assert "_REFINEMENT_BLOCKERS =" not in composition_source
    assert "_NEGATIVE_OPERATORS =" not in scope_source
    assert "_ANTI_NEGATION_PATTERNS =" not in scope_source


def test_modality_external_negation_remains_behavior_equivalent(router: QueryRouter) -> None:
    parsed = router.parse("procesos de obra sin contratacion directa")
    assert parsed.params["modalidad_exclude_procedure_ids"] == [
        "contratacion_directa",
        "contratacion_directa_con_ofertas",
    ]
    assert parsed.params["objeto"] == ["obra"]
    request = parsed.negation_requests[0]
    assert request.dimension is NegationDimension.MODALITY
    assert request.verdict is NegationVerdict.EXECUTE


def test_modality_internal_alias_is_not_generic_negation(router: QueryRouter) -> None:
    parsed = router.parse("contratacion directa sin ofertas")
    assert parsed.params["modalidad_include_procedure_ids"] == ["contratacion_directa"]
    assert "modalidad_exclude_procedure_ids" not in parsed.params
    assert parsed.negation_requests == ()


@pytest.mark.parametrize(
    "query,expected",
    [
        ("contratos de vigilancia por menos de 500 millones", 500_000_000),
        ("contratos de obra no mayores de 500 millones", 500_000_000),
    ],
)
def test_money_polarity_is_not_dimensional_exclusion(
    router: QueryRouter, query: str, expected: int,
) -> None:
    parsed = router.parse(query)
    assert parsed.params["valor_max"] == expected
    assert "valor_min" not in parsed.params
    assert parsed.negation_requests == ()


@pytest.mark.parametrize(
    "query",
    [
        "contratos con entidades sin animo de lucro",
        "contratos sin formula de reajuste para elementos de aseo",
        "contratos sin perjuicio de la vigilancia",
    ],
)
def test_domain_anti_negations_do_not_create_requests(
    router: QueryRouter, query: str,
) -> None:
    parsed = router.parse(query)
    assert parsed.negation_requests == ()
    assert "objeto_exclude" not in parsed.params


def test_object_binding_never_becomes_positive(router: QueryRouter) -> None:
    parsed = router.parse("contratos sin alimentacion")
    request = parsed.negation_requests[0]
    assert (request.dimension, request.value, request.marker) == (
        NegationDimension.OBJECT,
        "alimentacion",
        "sin",
    )
    assert request.verdict is NegationVerdict.EXECUTE
    assert "objeto" not in parsed.params
    assert parsed.params["objeto_exclude"] == ["alimentacion"]


def test_positive_and_negative_object_are_separated(router: QueryRouter) -> None:
    parsed = router.parse("contratos de alimentacion sin transporte")
    assert parsed.params["objeto"] == ["alimentacion"]
    assert parsed.params["objeto_exclude"] == ["transporte"]
    request = parsed.negation_requests[0]
    assert request.dimension is NegationDimension.OBJECT
    assert request.value == "transporte"
    assert "transporte" not in parsed.params["objeto"]


@pytest.mark.parametrize(
    "query",
    [
        "sin alimentacion ni transporte",
        "sin alimentacion, ni transporte",
    ],
)
def test_ni_propagates_negative_polarity(router: QueryRouter, query: str) -> None:
    parsed = router.parse(query)
    assert [(request.dimension, request.value) for request in parsed.negation_requests] == [
        (NegationDimension.OBJECT, "alimentacion"),
        (NegationDimension.OBJECT, "transporte"),
    ]
    assert "objeto" not in parsed.params
    assert parsed.params["objeto_exclude"] == ["alimentacion", "transporte"]
    assert parsed.negation_requests[1].context == "ni"


def test_sino_closes_negation_and_opens_positive_span(router: QueryRouter) -> None:
    parsed = router.parse("no de alimentacion sino de transporte")
    assert parsed.params["objeto"] == ["transporte"]
    assert parsed.params["objeto_exclude"] == ["alimentacion"]
    request = parsed.negation_requests[0]
    assert request.dimension is NegationDimension.OBJECT
    assert request.value == "alimentacion"


def test_city_binding_disables_city_and_city_to_entity(router: QueryRouter) -> None:
    parsed = router.parse("contratos de aseo que no sean de barranquilla")
    assert parsed.params["objeto"] == ["aseo"]
    assert "ciudad" not in parsed.params
    assert "entidad_resolved" not in parsed.params
    request = parsed.negation_requests[0]
    assert request.dimension is NegationDimension.CITY
    assert request.value == "barranquilla"


def test_city_binding_can_keep_a_positive_department_anchor(router: QueryRouter) -> None:
    parsed = router.parse("contratos en atlantico menos barranquilla")
    assert parsed.params["departamento_resolved"] == "Atlántico"
    assert "ciudad" not in parsed.params
    request = parsed.negation_requests[0]
    assert request.dimension is NegationDimension.CITY
    assert request.scope_anchor is not None
    assert request.scope_anchor.dimension is NegationDimension.DEPARTMENT
    assert request.scope_anchor.value == "Atlántico"


def test_time_binding_never_creates_positive_range(router: QueryRouter) -> None:
    parsed = router.parse("menos los de 2025")
    assert "fecha_desde" not in parsed.params
    assert "fecha_hasta" not in parsed.params
    request = parsed.negation_requests[0]
    assert request.dimension is NegationDimension.TIME
    assert request.value == "2025"


def test_buyer_binding_never_creates_positive_entity(router: QueryRouter) -> None:
    parsed = router.parse("contratos que no sean de la alcaldia de barranquilla")
    assert "entidad" not in parsed.params
    assert "entidad_resolved" not in parsed.params
    request = parsed.negation_requests[0]
    assert request.dimension is NegationDimension.BUYER
    assert request.value == "alcaldia de barranquilla"


def test_actor_binding_never_creates_positive_actor(router: QueryRouter) -> None:
    parsed = router.parse(
        "contratos donde el proveedor no sea union temporal aseo"
    )
    assert "actor_scope" not in parsed.params
    assert "actor_resolution" not in parsed.params
    assert "contratista" not in parsed.params
    request = parsed.negation_requests[0]
    assert request.dimension is NegationDimension.ACTOR
    assert request.marker == "no sea"
    assert request.value == "union temporal aseo"


@pytest.mark.parametrize(
    "query,dimension",
    [
        ("excepto contratos de obra", NegationDimension.CONTRACT_TYPE),
        ("procesos que no sean abiertos", NegationDimension.STATE),
        ("contratos excepto atlantico", NegationDimension.DEPARTMENT),
        ("excepto contratacion directa", NegationDimension.MODALITY),
    ],
)
def test_closed_dimensions_are_structurally_distinct(
    router: QueryRouter, query: str, dimension: NegationDimension,
) -> None:
    assert _request(router, query).dimension is dimension


@pytest.mark.parametrize(
    "query,marker",
    [
        ("contratos sin alimentacion", "sin"),
        ("contratos pero sin alimentacion", "sin"),
        ("contratos que no sean alimentacion", "que no sean"),
        ("contratos que no sea alimentacion", "que no sea"),
        ("contratos excepto alimentacion", "excepto"),
        ("contratos salvo alimentacion", "salvo"),
        ("contratos excluyendo alimentacion", "excluyendo"),
        ("contratos que excluyan alimentacion", "que excluyan"),
        ("contratos menos alimentacion", "menos"),
    ],
)
def test_adversarial_marker_forms_bind_without_positive_leak(
    router: QueryRouter, query: str, marker: str,
) -> None:
    parsed = router.parse(query)
    request = parsed.negation_requests[0]
    assert request.dimension is NegationDimension.OBJECT
    assert request.marker == marker
    assert request.value == "alimentacion"
    assert request.verdict is NegationVerdict.EXECUTE
    assert parsed.params["objeto_exclude"] == ["alimentacion"]
    assert "alimentacion" not in parsed.params.get("objeto", [])


def test_contrastive_connector_does_not_leak_as_positive_object(router: QueryRouter) -> None:
    parsed = router.parse("contratos pero sin alimentacion")
    assert "objeto" not in parsed.params


def test_negative_object_stops_before_positive_city(router: QueryRouter) -> None:
    parsed = router.parse("contratos sin alimentacion en barranquilla")
    assert parsed.negation_requests[0].value == "alimentacion"
    assert parsed.params["ciudad"] == "Barranquilla"


def test_legacy_object_exclusion_remains_executable(router: QueryRouter) -> None:
    parsed = router.parse("contratos excluye alimentacion")
    assert parsed.params["objeto_exclude"] == ["alimentacion"]
    assert parsed.negation_requests[0].verdict is NegationVerdict.EXECUTE


def test_anti_metamorphic_city_frames_do_not_collapse(router: QueryRouter) -> None:
    negative = router.parse("contratos de aseo que no sean de barranquilla")
    positive = router.parse("contratos de aseo en barranquilla")
    assert negative.params != positive.params
    assert negative.negation_requests
    assert not positive.negation_requests
    assert "ciudad" not in negative.params
    assert positive.params["ciudad"] == "Barranquilla"


def test_anti_metamorphic_object_polarities_do_not_collapse(router: QueryRouter) -> None:
    mixed = router.parse("contratos de alimentacion sin transporte")
    all_negative = router.parse("sin alimentacion ni transporte")
    mixed_view = (mixed.params.get("objeto"), [r.value for r in mixed.negation_requests])
    negative_view = (
        all_negative.params.get("objeto"),
        [r.value for r in all_negative.negation_requests],
    )
    assert mixed_view != negative_view
