"""Propiedades de lo que V1.2-B añade: estado durable, contrato y publicación.

Las invariantes del núcleo ya viven en `test_entity_profile_frame_properties.py`
y no se repiten. Aquí se comprueban las que sólo existen cuando el universo
sobrevive a un proceso: que un pendiente viaje entero, que no toque lo
confirmado, que un cursor de otro resultado se rechace y que las revisiones
publicadas formen una secuencia sin huecos.

Deterministas, sin red y sin `Settings()` fuera de las fixtures.
"""
from __future__ import annotations

import json

from hypothesis import given, settings, strategies as st
import pytest

from app.core.conversation_store import ConversationStore, StaleRevisionError
from app.core.entity_profile.frame import (
    EntityProfileFrame,
    SourcePredicates,
    SourceSelection,
    TemporalPredicate,
)
from app.core.entity_profile.frame_values import SourceKind, SourceOrigin
from app.core.entity_profile.patch import (
    EntityProfilePatch,
    PatchOp,
    PatchOperation,
    PatchTarget,
    TargetKind,
    apply_patch,
)
from app.core.entity_profile.patch_codec import patch_from_dict, patch_to_dict
from app.core.entity_profile.profile_state import (
    Alternative,
    PendingIssue,
    PendingProfile,
    ProfileConversationState,
    ProfileStateError,
)
from app.core.workflow.admission_rules import CONTINUATION_REF_KEY, classify
from app.core.conversation_envelope import EnvelopeStatus
from app.core.workflow.continuation import (
    ConfirmedProfileRef,
    ContinuationContract,
    ContinuationOwner,
    ResultKind,
    TabularGroup,
)
from app.core.workflow.admission_rules import AdmissionRoute, InputShape
from test_entity_profile_frame_properties import (
    BUYER,
    focuses,
    frames,
    periods,
    source_sets,
    topics,
)


SETTINGS = settings(max_examples=25, derandomize=True, deadline=None)


def _patch(frame, *operations) -> EntityProfilePatch:
    return EntityProfilePatch(
        patch_id="p", turn_id="t", operations=tuple(operations),
        base_frame_ref=(frame.frame_id, frame.revision) if frame else None,
    )


def _focus_op(focus: str) -> PatchOp:
    return PatchOp(
        PatchTarget(TargetKind.FOCUS), PatchOperation.SET, focus,
    )


# 1 — el parche viaja entero o no viaja
@SETTINGS
@given(frame=frames(), focus=focuses, topic=topics())
def test_a_patch_survives_serialization_with_every_typed_value(frame, focus, topic):
    operations = [_focus_op(focus)]
    for source in frame.sources:
        operations.append(PatchOp(
            PatchTarget(TargetKind.TOPIC, source), PatchOperation.SET, topic,
        ))
        operations.append(PatchOp(
            PatchTarget(TargetKind.TEMPORAL, source), PatchOperation.CLEAR,
        ))
    patch = _patch(frame, *operations)
    revived = patch_from_dict(json.loads(json.dumps(patch_to_dict(patch))))
    assert revived.operations == patch.operations
    assert revived.base_frame_ref == patch.base_frame_ref


# 2 — el estado completo viaja entero
@SETTINGS
@given(frame=frames(), focus=focuses)
def test_the_conversation_state_survives_a_full_json_round_trip(frame, focus):
    pending = PendingProfile(
        pending_id="pnd", patch=_patch(frame, _focus_op(focus)),
        question="¿sobre qué fuente?",
        base_frame_ref=(frame.frame_id, frame.revision),
        issues=(PendingIssue("EXPLICIT_SOURCE_INCOMPLETE", "detalle"),),
        alternatives=(Alternative("a1", "una salida", (_focus_op(focus),)),),
    )
    state = ProfileConversationState(confirmed_frame=frame, pending=pending)
    revived = ProfileConversationState.from_dict(
        json.loads(json.dumps(state.to_dict())),
    )
    assert revived.confirmed_frame.semantic_equal(frame)
    assert revived.pending.pending_id == "pnd"
    assert revived.pending.patch.operations == pending.patch.operations
    assert revived.pending.alternatives == pending.alternatives


# 3 — un pendiente NUNCA modifica lo confirmado
@SETTINGS
@given(frame=frames(), focus=focuses)
def test_holding_a_pending_never_changes_the_confirmed_frame(frame, focus):
    state = ProfileConversationState(confirmed_frame=frame)
    pending = PendingProfile(
        pending_id="p", patch=_patch(frame, _focus_op(focus)), question="?",
        base_frame_ref=(frame.frame_id, frame.revision),
    )
    held = state.hold(pending)
    assert held.confirmed_frame is frame
    assert held.confirmed_frame.fingerprint() == frame.fingerprint()
    assert held.cancel_pending().confirmed_frame is frame


# 4 — un pendiente anclado a otra revisión se rechaza
@SETTINGS
@given(frame=frames(), focus=focuses, offset=st.integers(min_value=1, max_value=5))
def test_a_pending_anchored_elsewhere_is_refused(frame, focus, offset):
    pending = PendingProfile(
        pending_id="p", patch=_patch(frame, _focus_op(focus)), question="?",
        base_frame_ref=(frame.frame_id, frame.revision + offset),
    )
    with pytest.raises(ProfileStateError):
        ProfileConversationState(confirmed_frame=frame, pending=pending)


# 5 — cambiar de fuente y cambiar el periodo NO conmutan, y eso es correcto
@SETTINGS
@given(period=periods, sources=source_sets)
def test_source_change_and_period_change_are_not_required_to_commute(period, sources):
    frame = EntityProfileFrame(
        frame_id="f", revision=1, buyer_scope=BUYER, focus="overview",
        source_selection=SourceSelection(
            frozenset({SourceKind.PROCESSES}), SourceOrigin.EXPLICIT,
        ),
        source_predicates={
            SourceKind.PROCESSES: SourcePredicates(SourceKind.PROCESSES),
        },
    )
    switch = PatchOp(
        PatchTarget(TargetKind.SOURCE_SELECTION), PatchOperation.SET,
        SourceSelection(sources, SourceOrigin.EXPLICIT),
    )
    set_period = PatchOp(
        PatchTarget(TargetKind.TEMPORAL, SourceKind.PROCESSES),
        PatchOperation.SET, TemporalPredicate(period=period),
    )
    period_first = apply_patch(frame, _patch(frame, set_period))
    assert period_first.frame is not None
    then_switch = apply_patch(period_first.frame, _patch(period_first.frame, switch))
    switch_first = apply_patch(frame, _patch(frame, switch))
    assert switch_first.frame is not None and then_switch.frame is not None

    # La fuente que ENTRA nace limpia: heredar el periodo de la que sale
    # contestaría otra pregunta. Por eso el orden importa aquí, y la prueba lo
    # documenta en vez de exigir una conmutatividad falsa.
    if SourceKind.CONTRACTS in sources:
        assert switch_first.frame.predicates_for(
            SourceKind.CONTRACTS,
        ).temporal.period.is_unbounded
        assert then_switch.frame.predicates_for(
            SourceKind.CONTRACTS,
        ).temporal.period.is_unbounded
    # La fuente que SIGUE conserva lo suyo, venga el periodo antes o después.
    if SourceKind.PROCESSES in sources:
        assert then_switch.frame.predicates_for(
            SourceKind.PROCESSES,
        ).temporal.period == period


# 6 — un cursor de otro resultado nunca valida
@SETTINGS
@given(
    mine=st.text(min_size=1, max_size=8),
    other=st.text(min_size=1, max_size=8),
    revision=st.integers(min_value=1, max_value=50),
)
def test_a_reference_to_another_result_is_always_refused(mine, other, revision):
    contract = ContinuationContract(
        conversation_revision=revision, visible_turn_id="t", result_id=mine,
        result_kind=ResultKind.TABULAR_SEARCH, owner=ContinuationOwner.TABULAR,
        tabular_groups={"procesos": TabularGroup(
            "procesos", "c", "fp", "exact_order", "g",
        )},
    )
    seed = {
        "estado": "activo",
        CONTINUATION_REF_KEY: {
            "result_id": other, "conversation_revision": revision,
        },
    }
    decision = classify(
        user_query="🎚️ Estado", seed_resolved_params=seed,
        envelope_status=EnvelopeStatus.OK, contract=contract,
        conversation_revision=revision, profile_owns_text=False,
        profile_has_options=False,
    )
    expected = (
        AdmissionRoute.DELEGATED if other == mine else AdmissionRoute.CLARIFY
    )
    assert decision.route is expected
    assert decision.shape is InputShape.SEED_REFINEMENT


# 7 — un perfil publicado nunca declara capacidad de paginar
@SETTINGS
@given(revision=st.integers(min_value=1, max_value=50))
def test_a_profile_contract_never_declares_pagination_capacity(revision):
    contract = ContinuationContract(
        conversation_revision=revision, visible_turn_id="t", result_id="r",
        result_kind=ResultKind.ENTITY_PROFILE, owner=ContinuationOwner.ENTITY_PROFILE,
        confirmed_profile=ConfirmedProfileRef(("f", 1), "plan", "ev"),
    )
    assert contract.can_paginate is False
    revived = ContinuationContract.from_dict(json.loads(json.dumps(contract.to_dict())))
    assert revived.can_paginate is False
    assert revived.owner is ContinuationOwner.ENTITY_PROFILE


# 8 — las revisiones publicadas forman una secuencia sin huecos
@SETTINGS
@given(turns=st.integers(min_value=1, max_value=6))
def test_published_revisions_are_a_gapless_sequence(turns, tmp_path_factory):
    store = ConversationStore(base_path=tmp_path_factory.mktemp("conv"))
    published = []
    for index in range(turns):
        reservation = store.reserve_turn("chat")
        envelope = store.publish_turn(
            reservation, user_query=f"q{index}", response=f"r{index}",
        )
        published.append(envelope.revision)
    assert published == list(range(1, turns + 1))
    read = store.load_envelope("chat")
    assert read.status is EnvelopeStatus.OK
    assert read.revision == turns
    assert read.envelope.response == f"r{turns - 1}"


# 9 — una reserva vieja nunca gana a una publicación posterior
@SETTINGS
@given(extra=st.integers(min_value=1, max_value=4))
def test_a_stale_reservation_never_overwrites(extra, tmp_path_factory):
    store = ConversationStore(base_path=tmp_path_factory.mktemp("conv"))
    stale = store.reserve_turn("chat")
    for index in range(extra):
        store.publish_turn(
            store.reserve_turn("chat"), user_query=f"q{index}", response=f"r{index}",
        )
    with pytest.raises(StaleRevisionError):
        store.publish_turn(stale, user_query="stale", response="STALE")
    assert store.load_envelope("chat").envelope.response == f"r{extra - 1}"
