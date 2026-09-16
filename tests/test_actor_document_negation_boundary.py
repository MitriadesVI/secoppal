"""Actor documents are structural boundaries, never negation identity evidence."""
from __future__ import annotations

import re

import pytest
from burr.core import State
from hypothesis import given, settings, strategies as st

from app.config import Settings
from app.core import negation_binding as negation_module
from app.core.capability_binding.supplier_contract import Outcome, decide
from app.core.conversation_store import ConversationStore
from app.core.negation_binding import (
    NegationDimension,
    NegationVerdict,
    bind_negations,
)
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import _CITY_PATTERN_RE, QueryRouter
from app.core.workflow.actions.planning import plan_query


router = QueryRouter()


class CountingSidecar:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, *_args, **_kwargs):
        self.calls += 1
        raise AssertionError("an actor-owned turn reached the A2 sidecar")


def _recording_workflow(tmp_path):
    settings = Settings(
        _env_file=None,
        deepseek_api_key=None,
        secop_feedback_path=tmp_path / "traces.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.db",
    )
    workflow = SecopalWorkflow(settings)
    workflow.conv_store = ConversationStore(tmp_path / "conversations")
    calls: list[tuple[str, str]] = []

    def record(operation):
        def inner(dataset_id, _soql_query, **_kwargs):
            calls.append((operation, dataset_id))
            return [] if operation != "count" else 0

        return inner

    workflow.secop_client.query = record("query")
    workflow.secop_client.count = record("count")
    workflow.secop_client.aggregate = record("aggregate")
    return workflow, calls


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


_ATOMIC_ACTOR_SURFACES = (
    "el proveedor con NIT 900711074",
    "el proveedor, NIT 900711074",
    "el proveedor: NIT 900711074",
    "el proveedor (NIT 900711074)",
    "el proveedor - NIT 900711074",
    "el proveedor identificado con NIT 900711074",
    "el proveedor número NIT 900711074",
    "el proveedor con cédula 79482763",
)


@pytest.mark.parametrize("surface", _ATOMIC_ACTOR_SURFACES)
def test_atomic_negated_actor_variants_block_before_every_source_call(
    tmp_path, surface: str,
) -> None:
    workflow, calls = _recording_workflow(tmp_path)
    result = workflow.run_query(f"contratos sin {surface}", channel="streamlit")

    assert result["needs_clarification"] is True
    assert result["query_plan"]["reason"] == "unsupported_negation"
    assert result["soql_query"] == ""
    assert calls == []
    assert all(not result["resolved_params"].get(key) for key in (
        "actor_scope", "actor_resolution", "contratista",
    ))


def test_mutation_narrowing_atomic_grammar_reopens_exactly_eight_source_calls(
    tmp_path, monkeypatch,
) -> None:
    immediate_only = re.compile(
        r"(?P<mention>\b(?:proveedor|contratista)\s+(?:nit|cc))\s*$",
        re.IGNORECASE,
    )
    monkeypatch.setattr(
        negation_module, "_ACTOR_DOCUMENT_PREFIX_RE", immediate_only,
    )
    workflow, calls = _recording_workflow(tmp_path)
    result = workflow.run_query(
        "contratos sin el proveedor con NIT 900711074",
        channel="streamlit",
    )
    assert result["route_reason"] == "actor_profile"
    assert len(calls) == 8


@settings(max_examples=24, deadline=None)
@given(
    reverse=st.booleans(),
    cardinal=st.integers(min_value=1, max_value=99),
    negative_document=st.sampled_from((
        ("NIT", "900711074"),
        ("CC", "79482763"),
    )),
    positive_document=st.sampled_from((
        ("NIT", "800197268", "NIT"),
        ("cedula", "1140835135", "CC"),
    )),
)
def test_each_marker_uses_its_own_document_boundary_across_permutations(
    reverse: bool,
    cardinal: int,
    negative_document: tuple[str, str],
    positive_document: tuple[str, str, str],
) -> None:
    negative_label, negative_number = negative_document
    positive_label, positive_number, positive_type = positive_document
    negative = f"contratos sin proveedor {negative_label} {negative_number}"
    positive = (
        f"menos de {cardinal} proveedores del proveedor "
        f"{positive_label} {positive_number}"
    )
    query = f"{positive}; {negative}" if reverse else f"{negative}; {positive}"
    parsed = router.parse(query)

    assert parsed.params["actor_scope"]["base_number"] == positive_number
    assert parsed.params["actor_scope"]["document_type"] == positive_type
    assert parsed.params["contratista"] == [positive_number]
    assert negative_number not in parsed.params["contratista"]
    assert any(
        request.dimension is NegationDimension.ACTOR
        and negative_number in request.value
        for request in parsed.negation_requests
    )


def test_mutation_passing_only_first_document_loses_the_independent_actor(
    monkeypatch,
) -> None:
    real_find = QueryRouter._find_identifier_spans
    monkeypatch.setattr(
        QueryRouter,
        "_find_identifier_spans",
        staticmethod(lambda text: real_find(text)[:1]),
    )
    parsed = QueryRouter().parse(
        "contratos sin proveedor NIT 900711074; "
        "menos de 5 proveedores del proveedor NIT 800197268"
    )
    assert parsed.params.get("actor_scope") is None
    assert parsed.params.get("contratista") is None


@pytest.mark.parametrize(
    "foreign_spans",
    [
        ((-1, 8),),
        ((14, 17),),
        ((14, 26),),
        ((100, 109),),
        ((True, 26),),
    ],
)
def test_direct_binding_ignores_foreign_or_non_document_spans(
    foreign_spans: tuple[tuple[int, int], ...],
) -> None:
    binding = bind_negations(
        "contratos sin alimentacion",
        city_pattern=_CITY_PATTERN_RE,
        actor_document_spans=foreign_spans,
    )
    assert binding.requests[0].dimension is NegationDimension.OBJECT
    assert binding.requests[0].value == "alimentacion"


def test_direct_binding_rejects_a_partial_document_coordinate() -> None:
    text = "contratos sin proveedor NIT 900711074"
    document_start = text.index("900711074")
    binding = bind_negations(
        text,
        city_pattern=_CITY_PATTERN_RE,
        actor_document_spans=((document_start + 1, len(text)),),
    )
    assert binding.requests[0].dimension is NegationDimension.OBJECT


def test_direct_binding_accepts_an_exact_document_coordinate() -> None:
    text = "contratos sin proveedor NIT 900711074"
    document_start = text.index("900711074")
    binding = bind_negations(
        text,
        city_pattern=_CITY_PATTERN_RE,
        actor_document_spans=((document_start, len(text)),),
    )
    assert binding.requests[0].dimension is NegationDimension.ACTOR
    assert "900711074" not in binding.masked_text


def test_mutation_without_span_validation_misclassifies_arbitrary_text(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        negation_module,
        "_validated_actor_document_spans",
        lambda _text, spans: tuple(spans),
    )
    binding = bind_negations(
        "contratos sin alimentacion",
        city_pattern=_CITY_PATTERN_RE,
        actor_document_spans=((14, 26),),
    )
    assert binding.requests[0].dimension is NegationDimension.ACTOR


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
