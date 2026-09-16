"""Actor documents are structural boundaries, never negation identity evidence."""
from __future__ import annotations

import pytest
from burr.core import State
from hypothesis import given, settings, strategies as st

from app.core.capability_binding.supplier_contract import Outcome, decide
from app.core.negation_binding import NegationDimension, NegationVerdict
from app.core.query_router import QueryRouter
from app.core.workflow.actions.planning import plan_query


router = QueryRouter()


class CountingSidecar:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, *_args, **_kwargs):
        self.calls += 1
        raise AssertionError("an actor-owned turn reached the A2 sidecar")


@pytest.mark.parametrize(
    "query",
    [
        "menos de 5 proveedores del proveedor NIT 900711074",
        "diferente de 5 proveedores del proveedor NIT 900711074",
        "distinto de 5 proveedores del proveedor NIT 900711074",
        "mas o menos 5 proveedores del proveedor NIT 900711074",
        "5 proveedores en adelante del proveedor NIT 900711074",
    ],
)
def test_free_cardinality_before_document_keeps_the_actor_positive(query: str) -> None:
    parsed = router.parse(query)

    assert parsed.params["actor_scope"]["base_number"] == "900711074"
    assert parsed.params["actor_scope"]["document_type"] == "NIT"
    assert parsed.params["actor_resolution"] == "explicit_document"
    assert parsed.params["contratista"] == ["900711074"]
    assert len(parsed.positive_text) == len(query.lower())

    sidecar = CountingSidecar()
    decision = decide(query, parsed.params, polarity_veto=sidecar)
    assert decision.outcome is Outcome.NOT_BOUND
    assert decision.reason == "ACTOR_SUBJECT"
    assert sidecar.calls == 0


def test_approximation_is_not_dimensional_negation() -> None:
    parsed = router.parse(
        "más o menos 5 proveedores del proveedor NIT 900711074"
    )
    assert parsed.negation_requests == ()
    assert parsed.params["actor_scope"]["base_number"] == "900711074"


@pytest.mark.parametrize(
    "query",
    [
        "contratos excepto proveedor NIT 900711074",
        "contratos sin el proveedor NIT 900711074",
        "contratos donde el proveedor no sea NIT 900711074",
        "contratos donde el proveedor pero no NIT 900711074",
    ],
)
def test_genuinely_negated_actor_never_becomes_positive(query: str) -> None:
    parsed = router.parse(query)

    assert parsed.params.get("actor_scope") is None
    assert parsed.params.get("actor_resolution") is None
    assert parsed.params.get("contratista") is None
    assert len(parsed.negation_requests) == 1
    request = parsed.negation_requests[0]
    assert request.dimension is NegationDimension.ACTOR
    assert request.verdict is NegationVerdict.CLARIFY

    state = State({
        "parsed_params": parsed.params,
        "raw_query": query,
        "prev_params": None,
        "turn_index": 0,
        "modality_interpretation": parsed.modality_interpretation,
        "negation_requests": [request.to_state()],
        "followup": False,
        "followup_intent_type": None,
    })
    planned = plan_query(state)
    assert planned["should_execute_decision"] is False
    assert planned["query_plan"]["reason"] == "unsupported_negation"


@settings(max_examples=36, deadline=None)
@given(
    marker=st.sampled_from(("menos de", "diferente de", "distinto de")),
    cardinal=st.integers(min_value=1, max_value=99),
    document=st.sampled_from((
        ("NIT", "900711074", "NIT"),
        ("CC", "79482763", "CC"),
        ("cedula", "1140835135", "CC"),
    )),
    soft_gap=st.sampled_from((" ", "  ", ", ", " ( ")),
)
def test_free_constituent_boundary_survives_spacing_punctuation_and_document_type(
    marker: str,
    cardinal: int,
    document: tuple[str, str, str],
    soft_gap: str,
) -> None:
    label, number, expected_type = document
    query = (
        f"{marker}{soft_gap}{cardinal} proveedores{soft_gap}"
        f"del proveedor {label} {number}"
    )
    parsed = router.parse(query)

    assert parsed.params["actor_scope"]["base_number"] == number
    assert parsed.params["actor_scope"]["document_type"] == expected_type
    assert parsed.params["actor_resolution"] == "explicit_document"
    assert parsed.params["contratista"] == [number]

    negated = router.parse(
        f"contratos sin{soft_gap}el proveedor {label} {number}"
    )
    assert negated.params.get("actor_scope") is None
    assert negated.params.get("actor_resolution") is None
    assert negated.params.get("contratista") is None
    assert negated.negation_requests[0].dimension is NegationDimension.ACTOR
    assert negated.negation_requests[0].verdict is NegationVerdict.CLARIFY


@pytest.mark.parametrize(
    ("query", "assertion"),
    [
        ("menos de 5 proveedores", "no_actor"),
        ("contratos sin alimentacion", "object"),
        ("contratos por menos de 500 millones", "money"),
        ("procesos sin contratacion directa", "modality"),
        ("contratos de aseo en barranquilla", "geography"),
    ],
)
def test_absent_actor_evidence_leaves_other_authorities_unchanged(
    query: str, assertion: str,
) -> None:
    parsed = router.parse(query)
    if assertion == "no_actor":
        assert parsed.params.get("actor_scope") is None
    elif assertion == "object":
        assert parsed.params["objeto_exclude"] == ["alimentacion"]
    elif assertion == "money":
        assert parsed.params["valor_max"] == 500_000_000
        assert parsed.negation_requests == ()
    elif assertion == "modality":
        assert parsed.params["modalidad_exclude_procedure_ids"]
    else:
        assert parsed.params["ciudad"] == "Barranquilla"
