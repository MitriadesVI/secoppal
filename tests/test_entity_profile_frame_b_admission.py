"""La tabla de transiciones de la sección 10, y que sus predicados no compiten.

Dos cosas distintas se comprueban aquí:

1. **Que el clasificador es total y de valor único.** Burr elige la primera
   condición verdadera; eso no demuestra por sí solo que las condiciones sean
   disjuntas. Aquí la ruta se calcula UNA vez y las transiciones comparan ese
   campo, así que la disjunción es del clasificador — y se prueba sobre el
   producto cartesiano de formas de entrada y dueños posibles.

2. **Que cada fila de la tabla de producto hace lo que dice**, de extremo a
   extremo: una continuación inválida termina sin consultas y no cae por
   defecto al parser tabular.
"""
from __future__ import annotations

import itertools

import pytest

from app.core.conversation_envelope import EnvelopeStatus
from app.core.entity_profile.frame_values import SourceKind
from app.core.workflow.admission_rules import (
    CONTINUATION_REF_KEY,
    AdmissionRoute,
    InputShape,
    classify,
    input_shape,
)
from app.core.workflow.continuation import (
    ConfirmedProfileRef,
    ContinuationContract,
    ContinuationOwner,
    PendingProfileRef,
    ResultKind,
    TabularGroup,
)
from entity_profile_frame_b_harness import (  # noqa: F401 — fixture por nombre
    BUYER,
    Conversation,
    frame_v12,
)


def _contract(owner: ContinuationOwner, *, groups=None) -> ContinuationContract:
    kind = {
        ContinuationOwner.TABULAR: ResultKind.TABULAR_SEARCH,
        ContinuationOwner.ENTITY_PROFILE: ResultKind.ENTITY_PROFILE,
        ContinuationOwner.ACTOR: ResultKind.ACTOR_PROFILE,
        ContinuationOwner.RADAR: ResultKind.RADAR,
        ContinuationOwner.NONE: ResultKind.OTHER,
    }[owner]
    kwargs = {}
    if owner is ContinuationOwner.TABULAR and groups is not False:
        kwargs["tabular_groups"] = {"procesos": TabularGroup(
            "procesos", "c1", "fp", "exact_order", "g1",
        )}
    if owner is ContinuationOwner.ENTITY_PROFILE:
        kwargs["confirmed_profile"] = ConfirmedProfileRef(("f", 1), "p", "e")
    return ContinuationContract(
        conversation_revision=1, visible_turn_id="t1", result_id="r1",
        result_kind=kind, owner=owner, **kwargs,
    )


_SHAPES = tuple(InputShape)
_OWNERS = tuple(ContinuationOwner)
_STATUSES = (EnvelopeStatus.OK, EnvelopeStatus.EMPTY, EnvelopeStatus.CORRUPT)

_QUERY_FOR_SHAPE = {
    InputShape.RESET: "/reset",
    InputShape.SEED_REFINEMENT: "🎚️ Estado",
    InputShape.PAGINATION: "muéstrame más",
    InputShape.ORDINAL: "1",
    InputShape.AFFIRMATIVE: "sí",
    InputShape.TEXT: "analiza la alcaldía de baranoa",
}


def _classify(shape, owner, status, *, owns_text=False, has_options=False, ref=None):
    seed = None
    if shape is InputShape.SEED_REFINEMENT:
        seed = {"estado": "activo"}
        if ref is not None:
            seed[CONTINUATION_REF_KEY] = ref
    return classify(
        user_query=_QUERY_FOR_SHAPE[shape],
        seed_resolved_params=seed,
        envelope_status=status,
        contract=_contract(owner) if owner is not ContinuationOwner.NONE else None,
        conversation_revision=1,
        profile_owns_text=owns_text,
        profile_has_options=has_options,
    )


# ── 1. Totalidad y valor único ─────────────────────────────────────────────

@pytest.mark.parametrize(
    "shape,owner,status", itertools.product(_SHAPES, _OWNERS, _STATUSES),
)
def test_every_combination_yields_exactly_one_route(shape, owner, status):
    decision = _classify(shape, owner, status)
    assert isinstance(decision.route, AdmissionRoute)
    assert decision.reason
    # La forma observada es la que se pidió: el clasificador no reinterpreta la
    # entrada a mitad de camino.
    assert decision.shape is shape


def test_input_shape_is_single_valued_for_every_probe():
    for shape, query in _QUERY_FOR_SHAPE.items():
        seed = {"estado": "activo"} if shape is InputShape.SEED_REFINEMENT else None
        assert input_shape(query, seed_resolved_params=seed) is shape


def test_a_clarifying_route_always_carries_its_answer():
    for shape, owner, status in itertools.product(_SHAPES, _OWNERS, _STATUSES):
        decision = _classify(shape, owner, status)
        if decision.route is AdmissionRoute.CLARIFY:
            assert decision.response, (shape, owner, status)


def test_the_burr_graph_reaches_the_terminal_named_by_the_decision():
    from app.core.workflow.admission import build_admission

    for shape, owner, status in itertools.product(_SHAPES, _OWNERS, _STATUSES):
        decision = _classify(shape, owner, status)
        application = build_admission(decision)
        *_, state = application.run(
            halt_after=["admit_profile", "admit_delegated", "admit_clarification"],
        )
        assert state["route"] == decision.route.value


# ── 2. Filas concretas de la tabla ─────────────────────────────────────────

def test_more_over_a_profile_is_not_paginable():
    decision = _classify(
        InputShape.PAGINATION, ContinuationOwner.ENTITY_PROFILE, EnvelopeStatus.OK,
    )
    assert decision.route is AdmissionRoute.CLARIFY
    assert decision.reason == "profile_is_not_paginable"


def test_more_over_a_tabular_result_keeps_its_handler():
    decision = _classify(
        InputShape.PAGINATION, ContinuationOwner.TABULAR, EnvelopeStatus.OK,
    )
    assert decision.route is AdmissionRoute.DELEGATED


def test_more_without_a_referent_clarifies_instead_of_inventing_an_offset():
    decision = _classify(
        InputShape.PAGINATION, ContinuationOwner.NONE, EnvelopeStatus.OK,
    )
    assert decision.route is AdmissionRoute.CLARIFY
    assert decision.reason == "no_referent_to_continue"


def test_an_unreadable_queue_never_reactivates_an_old_cursor():
    for shape in (
        InputShape.PAGINATION, InputShape.ORDINAL, InputShape.AFFIRMATIVE,
        InputShape.SEED_REFINEMENT,
    ):
        decision = _classify(shape, ContinuationOwner.TABULAR, EnvelopeStatus.CORRUPT)
        assert decision.route is AdmissionRoute.CLARIFY, shape
        assert decision.reason == "unreadable_continuation"


def test_a_reset_keeps_its_own_route_even_with_a_corrupt_queue():
    decision = _classify(
        InputShape.RESET, ContinuationOwner.ENTITY_PROFILE, EnvelopeStatus.CORRUPT,
    )
    assert decision.route is AdmissionRoute.DELEGATED
    assert decision.reason == "explicit_reset"


def test_a_number_over_a_profile_without_options_does_not_hunt_old_suggestions():
    decision = _classify(
        InputShape.ORDINAL, ContinuationOwner.ENTITY_PROFILE, EnvelopeStatus.OK,
        has_options=False,
    )
    assert decision.route is AdmissionRoute.CLARIFY
    assert decision.reason == "profile_has_no_options"


def test_a_number_with_actor_candidates_is_delegated_to_its_handler():
    decision = _classify(
        InputShape.ORDINAL, ContinuationOwner.ACTOR, EnvelopeStatus.OK,
    )
    assert decision.route is AdmissionRoute.DELEGATED
    assert decision.reason == "delegated_selection"


def test_a_number_over_a_pending_profile_belongs_to_the_profile():
    decision = _classify(
        InputShape.ORDINAL, ContinuationOwner.ENTITY_PROFILE, EnvelopeStatus.OK,
        has_options=True,
    )
    assert decision.route is AdmissionRoute.PROFILE


def test_a_tabular_seed_never_acquires_authority_over_a_profile_frame():
    decision = _classify(
        InputShape.SEED_REFINEMENT, ContinuationOwner.ENTITY_PROFILE,
        EnvelopeStatus.OK,
    )
    assert decision.route is AdmissionRoute.CLARIFY
    assert decision.reason == "seed_over_profile"


def test_a_seed_that_names_an_old_result_is_refused():
    stale = {"result_id": "another", "conversation_revision": 1}
    decision = _classify(
        InputShape.SEED_REFINEMENT, ContinuationOwner.TABULAR, EnvelopeStatus.OK,
        ref=stale,
    )
    assert decision.route is AdmissionRoute.CLARIFY
    assert decision.reason == "stale_seed_reference"


def test_a_seed_that_names_the_visible_result_keeps_its_route():
    fresh = {"result_id": "r1", "conversation_revision": 1}
    decision = _classify(
        InputShape.SEED_REFINEMENT, ContinuationOwner.TABULAR, EnvelopeStatus.OK,
        ref=fresh,
    )
    assert decision.route is AdmissionRoute.DELEGATED


def test_a_new_unambiguous_query_is_a_profile_turn_when_the_profile_owns_it():
    decision = _classify(
        InputShape.TEXT, ContinuationOwner.TABULAR, EnvelopeStatus.OK, owns_text=True,
    )
    assert decision.route is AdmissionRoute.PROFILE


def test_text_the_profile_does_not_own_goes_to_the_normal_flow():
    decision = _classify(
        InputShape.TEXT, ContinuationOwner.ENTITY_PROFILE, EnvelopeStatus.OK,
        owns_text=False,
    )
    assert decision.route is AdmissionRoute.DELEGATED
    assert decision.reason == "new_query"


# ── 3. Extremo a extremo: la admisión también corre para la interfaz ───────

def test_a_ui_refinement_over_a_profile_runs_no_query(frame_v12):
    chat = Conversation("b-adm-seed")
    chat.say(f"analiza contratos de {BUYER} en agosto")
    seed = {"entidad_resolved": "ALCALDIA MUNICIPAL DE BARANOA", "estado": "activo"}
    refined = chat.say("🎚️ Estado: activo", seed_resolved_params=seed)
    assert refined.clarifies
    assert refined.calls == ()
    assert refined.route == "admission_seed_over_profile"


def test_an_explicit_reset_invalidates_the_published_continuation(frame_v12):
    chat = Conversation("b-adm-reset")
    chat.say(f"analiza contratos de {BUYER} en agosto")
    assert chat.contract() is not None
    chat.say("/reset")
    assert chat.contract() is None
    assert not chat.profile_state().has_pending
    assert chat.profile_state().confirmed_frame is None


def test_a_new_buyer_starts_its_own_universe(frame_v12):
    chat = Conversation("b-adm-newbuyer")
    chat.say(f"analiza contratos de {BUYER} en agosto")
    other = chat.say("analiza la alcaldia de galapa")
    assert other.owns_profile and not other.clarifies
    confirmed = chat.profile_state().confirmed_frame
    assert confirmed.buyer_scope.canonical_buyers == ("MUNICIPIO DE GALAPA.",)
    # NEW con resolución propia: ni el periodo ni la fuente del universo
    # anterior se heredan.
    assert confirmed.source_selection.requested == frozenset(
        {SourceKind.PROCESSES, SourceKind.CONTRACTS},
    )
    assert other.planned_jobs == 6
    for soql in other.sql:
        assert "2026-08-01" not in soql
