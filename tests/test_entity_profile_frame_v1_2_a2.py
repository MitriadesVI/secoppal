"""A2 counterexamples: immutable plans, R3 ownership, and legacy state."""
from __future__ import annotations

from collections.abc import Mapping

from hypothesis import given, settings, strategies as st
import pytest

from app.core.entity_profile.frame import (
    BuyerScope,
    BuyerScopeKind,
    FrameProvenance,
    SourceSelection,
)
from app.core.entity_profile.frame_compiler import ProfilePlan
from app.core.entity_profile.frame_migration import (
    MigrationStatus,
    migrate_legacy_context,
)
from app.core.entity_profile.frame_values import (
    Selection,
    SourceKind,
    SourceOrigin,
    StateExpression,
    StatePredicate,
    TopicAll,
    TopicTerm,
)
from app.core.entity_profile.frame_render import (
    build_compiled_scope,
    render_compiled_scope,
)
from app.core.entity_profile.immutability import deep_freeze
from app.core.entity_profile.query_runner import QueryJob, execute_jobs
from app.core.query_router import QueryRouter
from app.core.source_occurrence import source_head_occurrences
from app.utils.money import normalize_text
from entity_profile_frame_harness import BUYER, observe
from test_entity_profile_frame_migration import BUYER_PARAMS, LEGACY_BASE


SETTINGS = settings(max_examples=24, derandomize=True, deadline=None)


def _plain(value):
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _plan_snapshot(plan):
    return {
        "scope": render_compiled_scope(build_compiled_scope(plan)),
        "source": plan.source_plans[SourceKind.CONTRACTS],
        "sql": tuple(job.sql for job in plan.jobs),
        "fingerprint": plan.source_plans[SourceKind.CONTRACTS].fingerprint,
        "plan_id": plan.plan_id,
        "frame_ref": plan.frame_ref,
        "jobs": plan.jobs,
    }


@pytest.mark.parametrize(
    "operation",
    [
        lambda target, replacement: dict.__setitem__(
            target, SourceKind.CONTRACTS, replacement,
        ),
        lambda target, replacement: dict.update(
            target, {SourceKind.CONTRACTS: replacement},
        ),
        lambda target, replacement: target.__init__(
            {SourceKind.CONTRACTS: replacement},
        ),
    ],
    ids=("dict-setitem", "dict-update", "reinitialize"),
)
def test_real_plan_cannot_swap_july_scope_for_august(operation):
    july = observe(f"analiza contratos firmados en julio de {BUYER}").decision.plan
    august = observe(f"analiza contratos firmados en agosto de {BUYER}").decision.plan
    before = _plan_snapshot(july)

    with pytest.raises((TypeError, AttributeError)):
        operation(
            july.source_plans,
            august.source_plans[SourceKind.CONTRACTS],
        )

    assert _plan_snapshot(july) == before
    assert any("Julio" in line for line in before["scope"])
    assert all("2026-07-01" in sql for sql in before["sql"])


@SETTINGS
@given(values=st.lists(st.text(min_size=1, max_size=12), min_size=1, max_size=6))
def test_snapshot_is_independent_of_nested_mutable_inputs(values):
    original = {"nested": {"values": list(values)}}
    frozen = deep_freeze(original)
    before = _plain(frozen)

    original["nested"]["values"].append("foreign")
    original["nested"] = {"values": ["replaced"]}

    assert _plain(frozen) == before
    assert isinstance(frozen["nested"]["values"], tuple)


def test_query_builder_receives_an_independent_mutable_materialization():
    frozen = deep_freeze({"nested": {"values": ["authorized"]}})

    def build(_builder, params):
        assert isinstance(params, dict)
        assert isinstance(params["nested"], dict)
        assert isinstance(params["nested"]["values"], list)
        params["nested"]["values"].append("builder-local")
        return "SELECT count(*)"

    class Client:
        def derived_for_timeout(self, _timeout):
            return self

        def query(self, _dataset, _sql):
            return []

    job = QueryJob(
        "a2_materialization", "test", "synthetic", "query", build,
        params=frozen,
    )
    rows, calls = execute_jobs((job,), Client(), object())

    assert rows == {"a2_materialization": []}
    assert calls["a2_materialization"].status == "SUCCESS"
    assert frozen["nested"]["values"] == ("authorized",)


def test_public_plan_and_frame_values_snapshot_sequence_inputs():
    compiled = observe(f"analiza contratos de software de {BUYER}").decision.plan
    jobs = list(compiled.jobs)
    frame_ref = list(compiled.frame_ref)
    limitations = ["known"]
    plan = ProfilePlan(
        plan_id=compiled.plan_id,
        frame_ref=frame_ref,
        catalog_version=compiled.catalog_version,
        focus=compiled.focus,
        source_plans=dict(compiled.source_plans),
        jobs=jobs,
        limitations=limitations,
    )
    buyers = ["ALCALDIA DE BARANOA"]
    requested = {SourceKind.CONTRACTS}
    values = ["Suspendido"]
    predicates = [StatePredicate("estado_contrato", values)]
    children = [TopicTerm("software")]
    includes = ["licitacion_publica"]
    claim_ids = ["claim-1"]

    buyer = BuyerScope(
        kind=BuyerScopeKind.BUYER_UNIT,
        display_name="ALCALDIA DE BARANOA",
        canonical_buyers=buyers,
        authority="test",
        authority_version="1",
    )
    selection = SourceSelection(requested, SourceOrigin.EXPLICIT)
    state = StateExpression(predicates)
    topic = TopicAll(children)
    modality = Selection(includes, ())
    provenance = FrameProvenance(claim_ids=claim_ids)

    jobs.clear()
    frame_ref.clear()
    limitations.clear()
    buyers.clear()
    requested.clear()
    values.clear()
    predicates.clear()
    children.clear()
    includes.clear()
    claim_ids.clear()

    assert plan.jobs and len(plan.frame_ref) == 2 and plan.limitations == ("known",)
    assert buyer.canonical_buyers == ("ALCALDIA DE BARANOA",)
    assert selection.requested == frozenset({SourceKind.CONTRACTS})
    assert state.values == ("Suspendido",)
    assert topic.children == (TopicTerm("software"),)
    assert modality.include == ("licitacion_publica",)
    assert provenance.claim_ids == ("claim-1",)


@SETTINGS
@given(
    framing=st.sampled_from((
        "", "busca ", "buscar ", "cuales ", "por favor ",
        "¿CUÁLES ", "POR FAVOR, BUSCA ",
    )),
    topic=st.sampled_from(("software", "mantenimiento", "automatizacion")),
)
def test_request_framing_preserves_contract_source_and_topic(framing, topic):
    query = f"{framing}contratos de {topic}"
    parsed = QueryRouter().parse(query).params
    assert parsed["dataset"] == "contratos"
    assert "dataset_scope" not in parsed
    assert parsed["objeto"] == [topic]
    assert len(source_head_occurrences(normalize_text(query))) == 1


@pytest.mark.parametrize(
    "query,topic",
    [
        ("¿CUÁLES CONTRATOS DE SOFTWARE?", "software"),
        ("Por favor, busca contratos de gestión", "gestion"),
        ("BUSCAR CONTRATOS DE AUTOMATIZACIÓN", "automatizacion"),
    ],
)
def test_case_accents_and_punctuation_do_not_change_the_request(query, topic):
    params = QueryRouter().parse(query).params
    assert params["dataset"] == "contratos"
    assert "dataset_scope" not in params
    assert params["objeto"] == [topic]


@pytest.mark.parametrize(
    "phrase,source,search_dataset,terms",
    [
        (
            "contratos de gestion de procesos y contratos",
            SourceKind.CONTRACTS,
            "contratos",
            ("gestion", "procesos", "contratos"),
        ),
        (
            "procesos de administracion de contratos",
            SourceKind.PROCESSES,
            "contratos",
            ("administracion", "contratos"),
        ),
        (
            "contratos de gestion de procesos y administracion de contratos",
            SourceKind.CONTRACTS,
            "contratos",
            ("gestion", "procesos", "administracion", "contratos"),
        ),
        (
            "contratos de gestion de procesos, administracion de contratos",
            SourceKind.CONTRACTS,
            "contratos",
            ("gestion", "procesos", "administracion", "contratos"),
        ),
        (
            "contratos de automatizacion de procesos o administracion de contratos",
            SourceKind.CONTRACTS,
            "contratos",
            ("automatizacion", "procesos", "administracion", "contratos"),
        ),
    ],
)
def test_thematic_coordination_keeps_every_source_word_in_the_frame(
    phrase,
    source,
    search_dataset,
    terms,
):
    parsed = QueryRouter().parse(phrase).params
    # Search keeps its historical scalar-dataset policy.  Frame source
    # ownership is decided independently from the accredited clause head.
    assert parsed["dataset"] == search_dataset
    assert "dataset_scope" not in parsed

    outcome = observe(f"analiza {phrase} de {BUYER}")
    assert outcome.ready, outcome.issues
    assert outcome.sources == frozenset({source})
    fingerprint = outcome.predicates(source).topic.fingerprint()
    assert all(term in fingerprint for term in terms)
    assert all(any(term in line for line in outcome.scope_lines) for term in terms)
    assert all(any(term in sql for sql in outcome.sql) for term in terms)


@pytest.mark.parametrize(
    "phrase",
    [
        "procesos publicados en julio y contratos firmados en agosto",
        "contratos firmados en agosto y procesos publicados en julio",
    ],
)
def test_qualified_source_coordination_remains_both_and_ready(phrase):
    outcome = observe(f"analiza {phrase} de {BUYER}")
    assert outcome.ready, outcome.issues
    assert outcome.sources == frozenset(SourceKind)
    assert outcome.params["dataset_scope"] == ["procesos", "contratos"]
    assert all(outcome.sql_for(source) for source in SourceKind)


@pytest.mark.parametrize(
    "query,positive_key",
    [
        ("contratos excepto obra", "objeto_exclude"),
        ("contratos de manual de contratacion", "objeto_phrases"),
        ("contratos excepto contratacion directa", "modalidad_exclude_procedure_ids"),
    ],
)
def test_exclusions_and_catalog_phrases_do_not_reenter_positive_topic(
    query,
    positive_key,
):
    params = QueryRouter().parse(query).params
    assert params.get(positive_key)
    assert not params.get("objeto")


@pytest.mark.parametrize(
    "sources,foreign_field",
    [
        ("CONTRACTS", "estado_del_procedimiento"),
        ("PROCESSES", "estado_contrato"),
    ],
)
def test_foreign_physical_state_never_disappears_during_migration(
    sources,
    foreign_field,
):
    request = {
        **LEGACY_BASE,
        "sources": sources,
        "event": "NONE",
        "temporal_claims": [],
    }
    outcome = migrate_legacy_context(
        request,
        {**BUYER_PARAMS, foreign_field: ["Publicado"]},
    )
    assert outcome.status is MigrationStatus.INVALID_CONTEXT
    assert outcome.frame is None
    assert outcome.reason == "STATE_FIELD_FOREIGN_TO_SOURCE"


def test_compatible_params_only_and_both_source_states_are_preserved():
    contracts = migrate_legacy_context(
        LEGACY_BASE,
        {**BUYER_PARAMS, "estado_contrato": ["Suspendido"]},
    )
    assert contracts.status is MigrationStatus.MIGRATED
    assert contracts.frame.predicates_for(
        SourceKind.CONTRACTS,
    ).current_state.values == ("Suspendido",)

    both_request = {**LEGACY_BASE, "sources": "BOTH"}
    both = migrate_legacy_context(
        both_request,
        {
            **BUYER_PARAMS,
            "estado_del_procedimiento": ["Publicado"],
            "estado_contrato": ["Suspendido"],
        },
    )
    assert both.status is MigrationStatus.MIGRATED
    assert all(
        not both.frame.predicates_for(source).current_state.is_true
        for source in SourceKind
    )

    duplicate = migrate_legacy_context(
        LEGACY_BASE,
        {
            **BUYER_PARAMS,
            "estado": ["Suspendido"],
            "estado_field": "estado_contrato",
            "estado_contrato": ["Suspendido"],
            "estado_family": "no_procede",
        },
    )
    assert duplicate.status is MigrationStatus.MIGRATED
    assert duplicate.frame.predicates_for(
        SourceKind.CONTRACTS,
    ).current_state.values == ("Suspendido",)


@pytest.mark.parametrize(
    "params_delta,reason",
    [
        ({"estado": [], "estado_field": "estado_contrato"}, "EMPTY_STATE_VALUES"),
        ({"estado_contrato": []}, "EMPTY_STATE_VALUES"),
        ({"estado": ["Suspendido"], "estado_field": "estado_desconocido"},
         "UNKNOWN_STATE_FIELD"),
        ({"estado_field": "estado_contrato"}, "STATE_FIELD_WITHOUT_VALUES"),
        ({"estado_family": ""}, "EMPTY_STATE_FAMILY"),
        ({"estado_family": "desconocida", "estado_contrato": ["Suspendido"]},
         "UNKNOWN_STATE_FAMILY"),
        ({"estado_family": "no_procede"}, "STATE_FAMILY_WITHOUT_VALUES"),
        ({"estado_family": "oferta_abierta", "estado_contrato": ["Suspendido"]},
         "STATE_FAMILY_CONFLICT"),
    ],
)
def test_migration_rejects_unaccredited_state_shapes(params_delta, reason):
    outcome = migrate_legacy_context(LEGACY_BASE, {**BUYER_PARAMS, **params_delta})
    assert outcome.status is MigrationStatus.INVALID_CONTEXT
    assert outcome.frame is None and outcome.reason == reason


@SETTINGS
@given(
    source=st.sampled_from(tuple(SourceKind)),
    value=st.sampled_from(("Suspendido", "Cancelado")),
    compatible=st.booleans(),
)
def test_migration_preserves_owned_states_and_rejects_foreign_ones(
    source,
    value,
    compatible,
):
    own_field = (
        "estado_del_procedimiento"
        if source is SourceKind.PROCESSES
        else "estado_contrato"
    )
    foreign_field = (
        "estado_contrato"
        if source is SourceKind.PROCESSES
        else "estado_del_procedimiento"
    )
    field = own_field if compatible else foreign_field
    request = {
        **LEGACY_BASE,
        "sources": source.value,
        "event": "NONE",
        "temporal_claims": [],
    }
    outcome = migrate_legacy_context(request, {**BUYER_PARAMS, field: [value]})
    expected = MigrationStatus.MIGRATED if compatible else MigrationStatus.INVALID_CONTEXT
    assert outcome.status is expected
    if compatible:
        state = outcome.frame.predicates_for(source).current_state
        assert state.predicates == (StatePredicate(field, (value,)),)
    else:
        assert outcome.frame is None
