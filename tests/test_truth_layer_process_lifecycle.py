"""T1B-2 — Process stage + resolution tests.

Behavioural unit tests for ``app/core/truth_layer/process_lifecycle``.

Goals (binding spec):

* the stage enum is frozen at nine values; ``selected_or_awarded`` and any
  offerability vocabulary are absent;
* ``estado_del_procedimiento`` is the primary signal and
  ``id_estado_del_procedimiento`` never arbitrates;
* ``estado_de_apertura_del_proceso="Abierto"`` never determines the stage —
  it is a DIFFERENT field from ``estado_del_procedimiento="Abierto"``, which
  does take part in the precedence yielding ``published``;
* award flag, award identity and a linked contract never rewrite the stage;
* award and contract-link are orthogonal signals, and the derived status
  never substitutes them;
* an ``AwardId`` only comes from an observed, ``meaningful``, non-empty
  string — never from ``"No Adjudicado"`` nor from a blank value;
* a timeout is ``source_error``, never an absence;
* conflicts are preserved, pick no winner, and forbid a ``consistent``
  quality;
* anti-bypass: for the two cataloged fields the resolver MUST attach the
  sentinel assessment; for uncataloged fields evidence without a sentinel is
  correct;
* purity and negative scope: no clock, I/O, network, filesystem, parser,
  formatter, Radar, workflow, bid readiness, temporality, modality
  offerability or lot types;
* ``opportunity_stage.py`` keeps its legacy behaviour untouched.
"""
from __future__ import annotations

import ast
import dataclasses
import inspect

import pytest

from app.core.opportunity_stage import (
    CLASSIFIER_VERSION,
    OpportunityStage,
    classify_opportunity_stage,
)
from app.core.truth_layer import (
    AssessmentConflict,
    AwardIdentityExtraction,
    ContractLookupResult,
    Evidence,
    FieldObservation,
    ProcessLifecycleAssessment,
    ProcessLifecycleSignals,
    ProcessResolutionAssessment,
    ResolutionQuality,
    SentinelAssessment,
    SourceError,
    assess_sentinel,
    build_resolution_quality,
    extract_award_identity,
    get_field_sentinel_policy,
    resolve_process_lifecycle,
    resolve_process_resolution,
)
import app.core.opportunity_stage as legacy_mod
import app.core.truth_layer as truth_layer_pkg
import app.core.truth_layer.process_lifecycle as process_mod


PROCESSES = "p6dx-8zbt"
CONTRACTS = "jbjy-vk9h"

_ALL_STAGES = {
    "draft",
    "approval",
    "published",
    "offer_phase_observed",
    "evaluation",
    "selected",
    "cancelled",
    "suspended",
    "unknown",
}


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _obs(value: object) -> FieldObservation:
    return FieldObservation("observed", value)  # type: ignore[arg-type]


def _missing() -> FieldObservation:
    return FieldObservation("missing", None)


def _not_observed() -> FieldObservation:
    return FieldObservation("not_observed", None)


def _signals(
    *,
    state: object = None,
    state_id: object = None,
    opening: object = None,
    phase: object = None,
    summary: object = None,
    adjudicated: object = None,
    adjudication_date: object = None,
    adjudication_id: object = None,
    absent: FieldObservation | None = None,
) -> ProcessLifecycleSignals:
    """Build T1A transport signals; ``None`` means the field is missing."""
    blank = absent if absent is not None else _missing()

    def field(value: object) -> FieldObservation:
        if isinstance(value, FieldObservation):
            return value
        return blank if value is None else _obs(value)

    return ProcessLifecycleSignals(
        raw_procedure_state=field(state),
        raw_procedure_state_id=field(state_id),
        raw_opening_state=field(opening),
        raw_phase=field(phase),
        raw_summary_state=field(summary),
        raw_adjudicated=field(adjudicated),
        raw_adjudication_date=field(adjudication_date),
        raw_adjudication_id=field(adjudication_id),
        raw_portfolio_id=blank,
        raw_modality=field(None),
    )


LINK_FIELD = "proceso_de_compra"

_DEFAULT_LOOKUP_OBSERVATION = {
    "observed": lambda: _obs("CO1.PCCNTR.9999"),
    "absent_after_completed_lookup": _missing,
    "not_queried": _not_observed,
    "source_error": _not_observed,
}


def _contract_evidence(
    raw: str,
    *,
    code: str = "CONTRACT_LINK_CANDIDATE",
    dataset: str = CONTRACTS,
    field: str = LINK_FIELD,
) -> Evidence:
    return Evidence(
        code=code,
        dataset=dataset,
        field=field,
        raw_observation=_obs(raw),
        sentinel_assessment=None,
        normalized_value=None,
        role="supports",
    )


def _canonical(evidences: tuple[Evidence, ...]) -> tuple[Evidence, ...]:
    return tuple(sorted(evidences, key=process_mod._evidence_sort_key))


def _contract_conflict(
    *, evidences: tuple[Evidence, ...] | None = None
) -> AssessmentConflict:
    """A contradiction the contract lookup itself found.

    Two contract rows claim the link for the same process, so the lookup can
    neither pick one nor report a clean link. The evidence is entirely
    contractual: no process-side evidence props it up.
    """
    if evidences is None:
        evidences = (
            _contract_evidence("CO1.PCCNTR.1"),
            _contract_evidence("CO1.PCCNTR.2"),
        )
    return AssessmentConflict(
        code="CONTRACT_LOOKUP_AMBIGUOUS_LINK",
        evidences=_canonical(evidences),
        consequence="CONTRACT_LINK_UNDECIDED",
        severity="material",
    )


def _lookup(
    outcome: str,
    error: SourceError | None = None,
    *,
    observation: FieldObservation | None = None,
    dataset_id: str = CONTRACTS,
    field_name: str = LINK_FIELD,
    conflict: AssessmentConflict | None = None,
    default_conflict: bool = True,
) -> ContractLookupResult:
    if observation is None:
        factory = _DEFAULT_LOOKUP_OBSERVATION.get(outcome)
        observation = factory() if factory is not None else None
    if conflict is None and outcome == "conflicting" and default_conflict:
        conflict = _contract_conflict()
    return ContractLookupResult(
        outcome=outcome,  # type: ignore[arg-type]
        dataset_id=dataset_id,
        field_name=field_name,
        observation=observation,
        source_error=error,
        conflict=conflict,
    )


def _timeout(dataset: str = CONTRACTS) -> SourceError:
    return SourceError(code="CONTRACT_LOOKUP_TIMEOUT", dataset=dataset, kind="timeout")


def _executable_source(module: object) -> str:
    """Module source with docstrings and comments stripped.

    Scope checks must read what the module DOES, not what its documentation
    says it refuses to do — otherwise a docstring citing the binding contract
    trips the very check that guards it.
    """
    tree = ast.parse(inspect.getsource(module))  # type: ignore[arg-type]
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef)):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                node.body = body[1:] or [ast.Pass()]
    return ast.unparse(tree)


# ---------------------------------------------------------------------------
# 1. Stage — the frozen enum and the binding precedence.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "state,expected",
    [
        ("Cancelado", "cancelled"),
        ("Suspendido", "suspended"),
        ("Seleccionado", "selected"),
        ("Evaluación", "evaluation"),
        ("Borrador", "draft"),
        ("En aprobación", "approval"),
        ("Aprobado", "approval"),
        ("Publicado", "published"),
        ("Abierto", "published"),
    ],
)
def test_every_recognized_procedure_state_maps_to_its_stage(
    state: str, expected: str
) -> None:
    assessment = resolve_process_lifecycle(_signals(state=state))
    assert assessment.stage == expected
    assert assessment.stage in _ALL_STAGES


@pytest.mark.parametrize("state", ["Publicado", "Abierto"])
def test_published_state_with_offer_phase_becomes_offer_phase_observed(
    state: str,
) -> None:
    assessment = resolve_process_lifecycle(
        _signals(state=state, phase="Presentación de oferta")
    )
    assert assessment.stage == "offer_phase_observed"


def test_offer_phase_without_state_still_yields_offer_phase_observed() -> None:
    assessment = resolve_process_lifecycle(_signals(phase="Presentación de oferta"))
    assert assessment.stage == "offer_phase_observed"


def test_precedence_puts_cancelled_above_everything_else() -> None:
    assessment = resolve_process_lifecycle(
        _signals(state="Cancelado", phase="Presentación de oferta", opening="Abierto")
    )
    assert assessment.stage == "cancelled"


def test_unrecognized_state_is_unknown_not_guessed() -> None:
    assessment = resolve_process_lifecycle(_signals(state="Estado Inventado 2031"))
    assert assessment.stage == "unknown"
    assert assessment.quality.completeness == "insufficient"


def test_missing_and_not_observed_signals_yield_unknown() -> None:
    assert resolve_process_lifecycle(_signals()).stage == "unknown"
    assert (
        resolve_process_lifecycle(_signals(absent=_not_observed())).stage == "unknown"
    )


def test_missing_state_is_reported_as_a_missing_field() -> None:
    assessment = resolve_process_lifecycle(_signals(phase="Presentación de oferta"))
    assert "estado_del_procedimiento" in assessment.quality.missing_fields
    assert assessment.quality.missing_fields == tuple(
        sorted(assessment.quality.missing_fields)
    )


def test_missing_and_not_observed_produce_different_evidence_roles() -> None:
    from_missing = resolve_process_lifecycle(_signals(state="Publicado"))
    from_not_observed = resolve_process_lifecycle(
        _signals(state="Publicado", absent=_not_observed())
    )
    roles_missing = {e.field: e.role for e in from_missing.evidence}
    roles_not_obs = {e.field: e.role for e in from_not_observed.evidence}
    assert roles_missing["fase"] == "missing"
    assert roles_not_obs["fase"] == "not_observed"
    # not_observed is never reported as a missing field.
    assert from_not_observed.quality.missing_fields == ()


# ---------------------------------------------------------------------------
# 2. The two "Abierto" are different fields.
# ---------------------------------------------------------------------------


def test_opening_state_open_never_determines_the_stage() -> None:
    assessment = resolve_process_lifecycle(_signals(opening="Abierto"))
    assert assessment.stage == "unknown"
    assert assessment.stage != "published"


def test_opening_state_open_does_not_upgrade_a_draft() -> None:
    assessment = resolve_process_lifecycle(
        _signals(state="Borrador", opening="Abierto")
    )
    assert assessment.stage == "draft"


def test_opening_state_is_kept_as_corroborating_evidence() -> None:
    assessment = resolve_process_lifecycle(
        _signals(state="Publicado", opening="Abierto")
    )
    opening = {e.field: e for e in assessment.evidence}[
        "estado_de_apertura_del_proceso"
    ]
    assert opening.raw_observation.raw_value == "Abierto"
    assert assessment.stage == "published"


def test_procedure_state_open_and_opening_state_open_are_not_interchangeable() -> None:
    by_state = resolve_process_lifecycle(_signals(state="Abierto"))
    by_opening = resolve_process_lifecycle(_signals(opening="Abierto"))
    assert by_state.stage == "published"
    assert by_opening.stage == "unknown"


def test_no_stage_named_open_or_expired_exists() -> None:
    for forbidden in ("open", "expired", "active", "bid_ready", "selected_or_awarded"):
        assert forbidden not in _ALL_STAGES


# ---------------------------------------------------------------------------
# 3. The state ID never arbitrates.
# ---------------------------------------------------------------------------


def test_state_id_does_not_override_the_textual_state() -> None:
    # 70 was observed live as "Seleccionado"; the text still wins.
    assessment = resolve_process_lifecycle(_signals(state="Publicado", state_id=70))
    assert assessment.stage == "published"


def test_state_id_alone_never_produces_a_stage() -> None:
    assert resolve_process_lifecycle(_signals(state_id=70)).stage == "unknown"


def test_state_id_is_preserved_as_evidence_with_a_capability_note() -> None:
    assessment = resolve_process_lifecycle(_signals(state="Publicado", state_id=70))
    evidence = {e.field: e for e in assessment.evidence}["id_estado_del_procedimiento"]
    assert evidence.raw_observation.raw_value == 70
    assert (
        "process_state_id_mapping",
        "unavailable",
    ) in assessment.quality.capabilities


def test_no_per_row_unmapped_reason_code_is_emitted() -> None:
    assessment = resolve_process_lifecycle(_signals(state="Publicado", state_id=70))
    assert "PROCESS_STATE_ID_UNMAPPED" not in assessment.quality.reason_codes


# ---------------------------------------------------------------------------
# 4. Award and contract never rewrite the stage.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "state,expected",
    [("Evaluación", "evaluation"), ("Publicado", "published"), ("Cancelado", "cancelled")],
)
def test_award_signals_do_not_rewrite_the_stage(state: str, expected: str) -> None:
    with_award = resolve_process_lifecycle(
        _signals(state=state, adjudicated="Si", adjudication_id="CO1.AWD.123")
    )
    without_award = resolve_process_lifecycle(_signals(state=state))
    assert with_award.stage == expected == without_award.stage


def test_lifecycle_assessment_does_not_carry_award_or_contract_fields() -> None:
    names = {f.name for f in dataclasses.fields(ProcessLifecycleAssessment)}
    assert not names & {
        "raw_adjudicated",
        "raw_adjudication_id",
        "raw_adjudication_date",
        "award_signal",
        "award_id",
        "contract_link_signal",
        "resolution_status",
    }


def test_stage_and_resolution_are_independent_axes() -> None:
    signals = _signals(
        state="Evaluación", adjudicated="Si", adjudication_id="CO1.AWD.123"
    )
    lifecycle = resolve_process_lifecycle(signals)
    resolution = resolve_process_resolution(
        signals, contract_lookup=_lookup("observed")
    )
    assert lifecycle.stage == "evaluation"
    assert resolution.resolution_status == "resolved"
    # Resolving one axis does not alter the other.
    assert resolve_process_lifecycle(signals).stage == "evaluation"


# ---------------------------------------------------------------------------
# 5. Award identity extraction.
# ---------------------------------------------------------------------------


def test_meaningful_award_id_is_extracted_verbatim() -> None:
    extraction = extract_award_identity(_obs("CO1.AWD.123"))
    assert extraction.status == "extracted"
    assert extraction.award_id == "CO1.AWD.123"
    assert extraction.raw_observation.raw_value == "CO1.AWD.123"
    assert extraction.sentinel_assessment is not None
    assert extraction.sentinel_assessment.status == "meaningful"


@pytest.mark.parametrize("raw", ["No Adjudicado", "no adjudicado", "  NO ADJUDICADO  "])
def test_sentinel_absence_never_produces_an_award_id(raw: str) -> None:
    extraction = extract_award_identity(_obs(raw))
    assert extraction.status == "absent"
    assert extraction.award_id is None
    assert extraction.sentinel_assessment is not None
    assert extraction.sentinel_assessment.status == "sentinel_absence"
    # The raw is preserved; absence is never a grouping key.
    assert extraction.raw_observation.raw_value == raw


@pytest.mark.parametrize("raw", ["", "   ", "\t\n"])
def test_blank_award_id_does_not_produce_an_identity(raw: str) -> None:
    """The catalog classifies "" as meaningful; emptiness is excluded here."""
    extraction = extract_award_identity(_obs(raw))
    assert extraction.sentinel_assessment is not None
    assert extraction.sentinel_assessment.status == "meaningful"
    assert extraction.status == "unknown"
    assert extraction.award_id is None
    assert "AWARD_ID_EMPTY_STRING" in extraction.reason_codes


@pytest.mark.parametrize("raw", [12345, 3.5, True])
def test_non_string_award_id_does_not_produce_an_identity(raw: object) -> None:
    """A type the comparator cannot handle must not crash the resolver.

    T1B-0A deliberately raises rather than collapsing a type mismatch into a
    sentinel. A resolver cannot die on an unexpected column type, so the
    mismatch surfaces as "unknown" with an explicit reason code — never as a
    silent sentinel, an absence, or an identity.
    """
    extraction = extract_award_identity(_obs(raw))
    assert extraction.status == "unknown"
    assert extraction.award_id is None
    assert extraction.sentinel_assessment is None
    assert "AWARD_ID_NOT_A_STRING" in extraction.reason_codes


@pytest.mark.parametrize("raw", [12345, 3.5, True])
def test_non_string_cataloged_field_does_not_crash_the_resolver(raw: object) -> None:
    resolution = resolve_process_resolution(_signals(adjudication_id=raw))
    assert resolution.award_signal == "unknown"
    assert resolution.award_id is None
    evidence = {e.field: e for e in resolution.evidence}["id_adjudicacion"]
    assert evidence.sentinel_assessment is None
    assert evidence.raw_observation.raw_value == raw
    assert "AWARD_ID_NOT_A_STRING" in resolution.quality.reason_codes


@pytest.mark.parametrize("observation", [_missing(), _not_observed()])
def test_unobserved_award_id_is_unknown_not_absent(
    observation: FieldObservation,
) -> None:
    extraction = extract_award_identity(observation)
    assert extraction.status == "unknown"
    assert extraction.status != "absent"
    assert extraction.award_id is None
    assert extraction.sentinel_assessment is None


def test_award_identity_rejects_an_award_id_without_extracted_status() -> None:
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=_obs("CO1.AWD.1"),
            sentinel_assessment=None,
            status="absent",
            award_id="CO1.AWD.1",  # type: ignore[arg-type]
            reason_codes=(),
        )
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=_obs("CO1.AWD.1"),
            sentinel_assessment=None,
            status="extracted",
            award_id=None,
            reason_codes=(),
        )


def test_two_processes_sharing_no_adjudicado_are_not_a_collision() -> None:
    first = extract_award_identity(_obs("No Adjudicado"))
    second = extract_award_identity(_obs("No Adjudicado"))
    assert first.award_id is second.award_id is None
    # Encoded absence never becomes an identity that could be grouped.
    assert first.status == second.status == "absent"


def test_genuine_repeated_award_id_stays_a_relational_fact() -> None:
    first = extract_award_identity(_obs("CO1.AWD.777"))
    second = extract_award_identity(_obs("CO1.AWD.777"))
    assert first.award_id == second.award_id == "CO1.AWD.777"
    assert first.status == "extracted"
    # Nothing in this layer dedupes, groups or asserts uniqueness.
    assert not hasattr(process_mod, "group_by_award_id")
    assert not hasattr(process_mod, "dedupe_awards")


# ---------------------------------------------------------------------------
# 6. Contract link input.
# ---------------------------------------------------------------------------


def test_no_lookup_means_not_queried() -> None:
    resolution = resolve_process_resolution(_signals(adjudicated="Si"))
    assert resolution.contract_link_signal == "not_queried"


def test_source_error_requires_a_source_error_payload() -> None:
    with pytest.raises(ValueError):
        _lookup("source_error")
    with pytest.raises(ValueError):
        _lookup("observed", _timeout())


def test_source_error_must_belong_to_the_queried_dataset() -> None:
    # Provenance metadata may never contradict itself.
    with pytest.raises(ValueError):
        _lookup("source_error", _timeout(dataset="some-other-dataset"))
    # The matching dataset is accepted.
    assert _lookup("source_error", _timeout()).source_error is not None


def test_contract_lookup_rejects_invalid_outcome_dataset_and_field() -> None:
    with pytest.raises(ValueError):
        ContractLookupResult(
            outcome="maybe", dataset_id=CONTRACTS, field_name=LINK_FIELD  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError):
        ContractLookupResult(
            outcome="not_queried", dataset_id="", field_name=LINK_FIELD
        )
    with pytest.raises(ValueError):
        ContractLookupResult(
            outcome="not_queried", dataset_id=CONTRACTS, field_name=""
        )


def test_observed_link_requires_the_observation_that_supports_it() -> None:
    # Asserting a contract link without showing what was seen is a claim,
    # not evidence.
    with pytest.raises(ValueError):
        ContractLookupResult(
            outcome="observed", dataset_id=CONTRACTS, field_name=LINK_FIELD
        )


@pytest.mark.parametrize(
    "outcome,wrong",
    [
        ("observed", _missing()),
        ("absent_after_completed_lookup", _obs("CO1.PCCNTR.1")),
        ("not_queried", _obs("CO1.PCCNTR.1")),
        ("source_error", _missing()),
    ],
)
def test_lookup_outcome_and_presence_must_agree(
    outcome: str, wrong: FieldObservation
) -> None:
    with pytest.raises(ValueError):
        _lookup(
            outcome,
            _timeout() if outcome == "source_error" else None,
            observation=wrong,
        )


def test_timeout_is_source_error_never_absence() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="No Adjudicado"),
        contract_lookup=_lookup("source_error", _timeout()),
    )
    assert resolution.contract_link_signal == "source_error"
    assert resolution.contract_link_signal != "absent_after_completed_lookup"
    assert resolution.resolution_status == "unknown"
    assert resolution.resolution_status != "unresolved"
    assert len(resolution.quality.source_errors) == 1
    assert resolution.quality.source_errors[0].kind == "timeout"


def test_not_queried_and_completed_empty_lookup_are_different() -> None:
    signals = _signals(adjudicated="No", adjudication_id="No Adjudicado")
    not_queried = resolve_process_resolution(signals)
    completed = resolve_process_resolution(
        signals, contract_lookup=_lookup("absent_after_completed_lookup")
    )
    assert not_queried.contract_link_signal == "not_queried"
    assert completed.contract_link_signal == "absent_after_completed_lookup"
    assert not_queried.resolution_status == "unknown"
    assert completed.resolution_status == "unresolved"


def test_resolver_performs_no_lookup_itself() -> None:
    source = _executable_source(process_mod)
    for forbidden in ("requests", "httpx", "urlopen", "socket", "sqlite3", "execute"):
        assert forbidden not in source, f"lookup machinery leaked: {forbidden}"


# ---------------------------------------------------------------------------
# 7. The binding resolution matrix.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,signals,lookup,award,link,status",
    [
        (
            "evaluation + award + contract",
            _signals(state="Evaluación", adjudicated="Si", adjudication_id="CO1.AWD.1"),
            _lookup("observed"),
            "observed",
            "observed",
            "resolved",
        ),
        (
            "offer phase + award, no lookup",
            _signals(
                state="Publicado",
                phase="Presentación de oferta",
                adjudicated="Si",
                adjudication_id="CO1.AWD.1",
            ),
            None,
            "observed",
            "not_queried",
            "resolved",
        ),
        (
            "selected + adjudicado No + contract",
            _signals(
                state="Seleccionado",
                adjudicated="No",
                adjudication_id="No Adjudicado",
            ),
            _lookup("observed"),
            "not_indicated",
            "observed",
            "conflicting",
        ),
        (
            "cancelled + contract observed",
            _signals(state="Cancelado", adjudicated="Si", adjudication_id="CO1.AWD.9"),
            _lookup("observed"),
            "observed",
            "observed",
            "resolved",
        ),
        (
            "no award + completed empty lookup",
            _signals(adjudicated="No", adjudication_id="No Adjudicado"),
            _lookup("absent_after_completed_lookup"),
            "not_indicated",
            "absent_after_completed_lookup",
            "unresolved",
        ),
        (
            "no award + timeout",
            _signals(adjudicated="No", adjudication_id="No Adjudicado"),
            _lookup("source_error", _timeout()),
            "not_indicated",
            "source_error",
            "unknown",
        ),
        (
            "award observed + contract source error",
            _signals(adjudicated="Si", adjudication_id="CO1.AWD.5"),
            _lookup("source_error", _timeout()),
            "observed",
            "source_error",
            "resolved",
        ),
    ],
)
def test_binding_resolution_matrix(
    name: str,
    signals: ProcessLifecycleSignals,
    lookup: ContractLookupResult | None,
    award: str,
    link: str,
    status: str,
) -> None:
    resolution = resolve_process_resolution(signals, contract_lookup=lookup)
    assert resolution.award_signal == award, name
    assert resolution.contract_link_signal == link, name
    assert resolution.resolution_status == status, name


def test_award_and_contract_may_both_be_observed() -> None:
    resolution = resolve_process_resolution(
        _signals(state="Evaluación", adjudicated="Si", adjudication_id="CO1.AWD.1"),
        contract_lookup=_lookup("observed"),
    )
    assert resolution.award_signal == "observed"
    assert resolution.contract_link_signal == "observed"


def test_derived_status_does_not_substitute_the_two_signals() -> None:
    resolution = resolve_process_resolution(
        _signals(state="Seleccionado", adjudicated="No", adjudication_id="No Adjudicado"),
        contract_lookup=_lookup("observed"),
    )
    assert resolution.resolution_status == "conflicting"
    # Both signals survive the contradiction untouched.
    assert resolution.award_signal == "not_indicated"
    assert resolution.contract_link_signal == "observed"


def test_award_id_always_comes_from_the_identity_extraction() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1")
    )
    assert resolution.award_id == resolution.award_identity.award_id == "CO1.AWD.1"
    with pytest.raises(ValueError):
        dataclasses.replace(resolution, award_id="CO1.AWD.OTHER")  # type: ignore[arg-type]


def test_award_flag_negative_with_award_identity_is_conflicting() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="CO1.AWD.1")
    )
    assert resolution.award_signal == "conflicting"
    assert resolution.resolution_status == "conflicting"
    assert resolution.award_id == "CO1.AWD.1"


# ---------------------------------------------------------------------------
# 8. Conflicts.
# ---------------------------------------------------------------------------


def test_conflicts_are_preserved_and_pick_no_winner() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="CO1.AWD.1")
    )
    assert resolution.quality.conflicts
    conflict = resolution.quality.conflicts[0]
    assert len(conflict.evidences) >= 2
    fields = {e.field for e in conflict.evidences}
    assert fields == {"adjudicado", "id_adjudicacion"}
    assert not {f.name for f in dataclasses.fields(conflict)} & {"winner", "chosen"}


def test_quality_is_never_consistent_while_a_conflict_stands() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="CO1.AWD.1")
    )
    assert resolution.quality.consistency == "conflicting"
    assert resolution.quality.consistency != "consistent"


def test_conflicting_contract_link_is_never_reported_as_consistent() -> None:
    """A lookup that reports a contradiction must not yield clean quality."""
    lookup = _lookup("conflicting")
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1"),
        contract_lookup=lookup,
    )
    assert resolution.contract_link_signal == "conflicting"
    assert resolution.resolution_status == "conflicting"
    assert resolution.quality.consistency == "conflicting"
    assert resolution.quality.consistency != "consistent"
    assert resolution.quality.conflicts
    # The lookup's own conflict is preserved verbatim; nothing is re-described
    # and no substitute conflict is manufactured on its behalf.
    assert lookup.conflict in resolution.quality.conflicts
    codes = {c.code for c in resolution.quality.conflicts}
    assert codes == {"CONTRACT_LOOKUP_AMBIGUOUS_LINK"}
    assert "PROCESS_CONTRACT_LINK_REPORTED_CONFLICTING" not in codes
    assert "PROCESS_CONTRACT_LINK_REPORTED_CONFLICTING" not in _executable_source(
        process_mod
    )


@pytest.mark.parametrize(
    "adjudicated,adjudication_id",
    [("Si", "CO1.AWD.1"), ("No", "No Adjudicado"), ("No", "CO1.AWD.1"), (None, None)],
)
@pytest.mark.parametrize(
    "outcome",
    [
        "observed",
        "absent_after_completed_lookup",
        "not_queried",
        "source_error",
        "unknown",
        "conflicting",
    ],
)
def test_no_public_state_ever_reports_a_conflict_as_consistent(
    adjudicated: object, adjudication_id: object, outcome: str
) -> None:
    """Exhaustive sweep over the public award x contract-link state space.

    A green suite proves nothing about states no test constructs, so every
    reachable combination is asserted against the transversal invariants.
    """
    lookup = _lookup(outcome, _timeout() if outcome == "source_error" else None)
    resolution = resolve_process_resolution(
        _signals(adjudicated=adjudicated, adjudication_id=adjudication_id),
        contract_lookup=lookup,
    )
    if resolution.resolution_status == "conflicting":
        assert resolution.quality.consistency == "conflicting"
    if resolution.quality.conflicts:
        assert resolution.quality.consistency == "conflicting"
    if "conflicting" in {
        resolution.award_signal,
        resolution.contract_link_signal,
    }:
        assert resolution.quality.consistency == "conflicting"
    # The two signals always survive into the assessment.
    assert resolution.contract_link_signal == outcome
    assert resolution.contract_lookup is lookup


def test_award_versus_contract_conflict_carries_both_sides() -> None:
    """A conflict that names two axes must preserve evidence from both."""
    lookup = _lookup("observed")
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="No Adjudicado"),
        contract_lookup=lookup,
    )
    conflict = {c.code: c for c in resolution.quality.conflicts}[
        "PROCESS_AWARD_ABSENT_VS_CONTRACT_LINK"
    ]
    datasets = {e.dataset for e in conflict.evidences}
    assert datasets == {PROCESSES, CONTRACTS}, "the contract side is missing"
    fields = {e.field for e in conflict.evidences}
    assert LINK_FIELD in fields
    assert "id_adjudicacion" in fields


def test_contract_lookup_provenance_survives_in_the_assessment() -> None:
    lookup = _lookup("observed")
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1"),
        contract_lookup=lookup,
    )
    assert resolution.contract_lookup is lookup
    assert resolution.contract_lookup.dataset_id == CONTRACTS
    contract_evidence = {
        e.field: e for e in resolution.evidence if e.dataset == CONTRACTS
    }
    assert LINK_FIELD in contract_evidence
    assert (
        contract_evidence[LINK_FIELD].raw_observation.raw_value == "CO1.PCCNTR.9999"
    )


def test_contract_source_error_is_attached_to_the_contract_evidence() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="No Adjudicado"),
        contract_lookup=_lookup("source_error", _timeout()),
    )
    contract_evidence = [e for e in resolution.evidence if e.dataset == CONTRACTS]
    assert len(contract_evidence) == 1
    evidence = contract_evidence[0]
    assert evidence.source_error is not None
    assert evidence.source_error.kind == "timeout"
    # A source failure is not_observed, never a missing field.
    assert evidence.role == "not_observed"
    assert LINK_FIELD not in resolution.quality.missing_fields


def test_no_lookup_means_no_contract_evidence_and_no_provenance_invented() -> None:
    resolution = resolve_process_resolution(_signals(adjudicated="Si"))
    assert resolution.contract_lookup is None
    assert resolution.contract_link_signal == "not_queried"
    assert not [e for e in resolution.evidence if e.dataset != PROCESSES]


def test_assessment_rejects_a_signal_that_contradicts_its_lookup() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1"),
        contract_lookup=_lookup("observed"),
    )
    with pytest.raises(ValueError):
        dataclasses.replace(resolution, contract_link_signal="not_queried")
    with pytest.raises(ValueError):
        dataclasses.replace(resolution, contract_lookup=None)


def test_resolution_quality_carries_no_lifecycle_capability() -> None:
    """Capabilities belong to the axis they describe.

    ``process_state_id_mapping`` is a lifecycle concern; leaking it into the
    resolution assessment weakens the orthogonality this module maintains.
    """
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1")
    )
    keys = {key for key, _ in resolution.quality.capabilities}
    assert "process_state_id_mapping" not in keys
    lifecycle = resolve_process_lifecycle(_signals(state="Publicado"))
    assert ("process_state_id_mapping", "unavailable") in lifecycle.quality.capabilities


# ---------------------------------------------------------------------------
# 8b. The award identity boundary cannot be forged.
# ---------------------------------------------------------------------------


def _real_award_assessment(raw: str = "CO1.AWD.1"):
    from app.core.truth_layer import assess_sentinel

    observation = _obs(raw)
    policy = get_field_sentinel_policy(PROCESSES, "id_adjudicacion")
    return observation, assess_sentinel(observation, policy)


def test_extracted_identity_requires_an_observed_scalar() -> None:
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=_missing(),
            sentinel_assessment=None,
            status="extracted",
            award_id="FORGED",  # type: ignore[arg-type]
            reason_codes=("FORGED_CODE",),
        )


def test_extracted_identity_requires_its_sentinel_assessment() -> None:
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=_obs("CO1.AWD.1"),
            sentinel_assessment=None,
            status="extracted",
            award_id="CO1.AWD.1",  # type: ignore[arg-type]
            reason_codes=(),
        )


def test_extracted_identity_requires_a_meaningful_classification() -> None:
    observation, assessment = _real_award_assessment("No Adjudicado")
    assert assessment.status == "sentinel_absence"
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="extracted",
            award_id="No Adjudicado",  # type: ignore[arg-type]
            reason_codes=(),
        )


def test_extracted_identity_requires_the_very_assessed_observation() -> None:
    _, assessment = _real_award_assessment("CO1.AWD.1")
    twin = _obs("CO1.AWD.1")
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=twin,
            sentinel_assessment=assessment,
            status="extracted",
            award_id="CO1.AWD.1",  # type: ignore[arg-type]
            reason_codes=(),
        )


def test_extracted_identity_must_come_from_the_award_id_field() -> None:
    from app.core.truth_layer import assess_sentinel

    observation = _obs("Cualquier Cosa")
    policy = get_field_sentinel_policy(PROCESSES, "estado_resumen")
    assessment = assess_sentinel(observation, policy)
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="extracted",
            award_id="Cualquier Cosa",  # type: ignore[arg-type]
            reason_codes=(),
        )


def test_extracted_identity_must_equal_the_untransformed_raw() -> None:
    observation, assessment = _real_award_assessment("CO1.AWD.1")
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="extracted",
            award_id="co1.awd.1",  # normalized, not the raw
            reason_codes=(),
        )


def test_non_extracted_status_cannot_contradict_its_sentinel() -> None:
    observation, assessment = _real_award_assessment("No Adjudicado")
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="unrecognized",  # the sentinel says absence
            award_id=None,
            reason_codes=(),
        )


def test_a_hand_built_assessment_cannot_smuggle_an_identity() -> None:
    """The strongest forgery vector: a hand-built ``SentinelAssessment``.

    ``SentinelAssessment`` does not itself require ``presence="observed"``
    (only ``assess_sentinel`` does), so a caller can construct one over a
    non-observation. The identity boundary must still refuse it.
    """
    from app.core.truth_layer import SentinelAssessment

    missing = _missing()
    forged_assessment = SentinelAssessment(
        dataset_id=PROCESSES,
        field_name="id_adjudicacion",
        raw_observation=missing,
        status="meaningful",
        sentinel_code=None,
        normalized_comparison_value="FORGED",
    )
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=missing,
            sentinel_assessment=forged_assessment,
            status="extracted",
            award_id="FORGED",  # type: ignore[arg-type]
            reason_codes=(),
        )


@pytest.mark.parametrize(
    "presence,sentinel_status,identity_status",
    [
        ("not_observed", "sentinel_absence", "absent"),
        ("missing", "sentinel_absence", "absent"),
        ("not_observed", "unrecognized", "unrecognized"),
        ("missing", "sentinel_unknown", "unknown"),
    ],
)
def test_no_sentinel_classification_can_stand_on_a_non_observation(
    presence: str,
    sentinel_status: str,
    identity_status: str,
) -> None:
    observation = FieldObservation(presence, None)  # type: ignore[arg-type]
    assessment = _hand_assessment(
        observation,
        status=sentinel_status,
        code=(
            "FORGED_SENTINEL"
            if sentinel_status in {"sentinel_absence", "sentinel_unknown"}
            else None
        ),
    )
    with pytest.raises(ValueError, match="requires an observed scalar"):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status=identity_status,  # type: ignore[arg-type]
            award_id=None,
            reason_codes=(),
        )


def test_a_genuine_extraction_still_constructs() -> None:
    observation, assessment = _real_award_assessment("CO1.AWD.1")
    extraction = AwardIdentityExtraction(
        dataset_id=PROCESSES,
        raw_observation=observation,
        sentinel_assessment=assessment,
        status="extracted",
        award_id="CO1.AWD.1",  # type: ignore[arg-type]
        reason_codes=("AWARD_ID_EXTRACTED",),
    )
    assert extraction.award_id == "CO1.AWD.1"


# ---------------------------------------------------------------------------
# 8c. Award identity provenance — enforced for EVERY status.
# ---------------------------------------------------------------------------


def _hand_assessment(
    observation: FieldObservation,
    *,
    status: str,
    dataset_id: str = PROCESSES,
    field_name: str = "id_adjudicacion",
    code: str | None = None,
) -> SentinelAssessment:
    """A hand-built classification — the forgery vector the boundary must resist."""
    return SentinelAssessment(
        dataset_id=dataset_id,
        field_name=field_name,
        raw_observation=observation,
        status=status,  # type: ignore[arg-type]
        sentinel_code=code,
        normalized_comparison_value=str(observation.raw_value).strip().casefold(),
    )


def test_identity_requires_a_non_empty_dataset_id() -> None:
    observation, assessment = _real_award_assessment("CO1.AWD.1")
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id="",
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="extracted",
            award_id="CO1.AWD.1",  # type: ignore[arg-type]
            reason_codes=(),
        )


def test_the_extractor_records_the_dataset_it_classified_against() -> None:
    extraction = extract_award_identity(_obs("CO1.AWD.1"))
    assert extraction.dataset_id == PROCESSES
    assert extraction.sentinel_assessment is not None
    assert extraction.sentinel_assessment.dataset_id == PROCESSES
    # A dataset with no authorized policy keeps its provenance and claims
    # nothing: no policy means no classification, never "meaningful".
    elsewhere = extract_award_identity(_obs("CO1.AWD.1"), dataset_id="not-a-dataset")
    assert elsewhere.dataset_id == "not-a-dataset"
    assert elsewhere.status == "unknown"
    assert elsewhere.award_id is None
    assert elsewhere.sentinel_assessment is None
    assert "AWARD_ID_SENTINEL_POLICY_UNAVAILABLE" in elsewhere.reason_codes


def test_absent_cannot_borrow_the_assessment_of_another_observation() -> None:
    """Provenance is identity, for a non-extracted status too."""
    assessed, assessment = _real_award_assessment("No Adjudicado")
    twin = _obs("No Adjudicado")  # equal value, different observation
    assert twin is not assessed
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=twin,
            sentinel_assessment=assessment,
            status="absent",
            award_id=None,
            reason_codes=(),
        )
    # The very observation it classified is accepted.
    accepted = AwardIdentityExtraction(
        dataset_id=PROCESSES,
        raw_observation=assessed,
        sentinel_assessment=assessment,
        status="absent",
        award_id=None,
        reason_codes=(),
    )
    assert accepted.status == "absent"


def test_no_status_accepts_a_classification_of_another_field() -> None:
    """``estado_resumen`` says nothing about ``id_adjudicacion``."""
    observation = _obs("No Definido")
    policy = get_field_sentinel_policy(PROCESSES, "estado_resumen")
    assessment = assess_sentinel(observation, policy)
    # The sentinel status alone would be admissible for "unknown"; only the
    # field name makes this classification foreign.
    assert assessment.status == "sentinel_unknown"
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="unknown",
            award_id=None,
            reason_codes=(),
        )


@pytest.mark.parametrize("status", ["absent", "extracted"])
def test_no_status_accepts_a_classification_of_another_dataset(status: str) -> None:
    raw = "No Adjudicado" if status == "absent" else "CO1.AWD.1"
    observation = _obs(raw)
    foreign = _hand_assessment(
        observation,
        dataset_id=CONTRACTS,
        status="sentinel_absence" if status == "absent" else "meaningful",
        code="PROCESS_AWARD_ID_NO_AWARD" if status == "absent" else None,
    )
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=foreign,
            status=status,  # type: ignore[arg-type]
            award_id=raw if status == "extracted" else None,  # type: ignore[arg-type]
            reason_codes=(),
        )
    # The check is agreement with the extraction's own dataset, not a hardcoded
    # allowlist: the same classification is admissible in its own dataset.
    accepted = AwardIdentityExtraction(
        dataset_id=CONTRACTS,
        raw_observation=observation,
        sentinel_assessment=foreign,
        status=status,  # type: ignore[arg-type]
        award_id=raw if status == "extracted" else None,  # type: ignore[arg-type]
        reason_codes=(),
    )
    assert accepted.dataset_id == CONTRACTS


@pytest.mark.parametrize("raw", ["", "   ", "\t\n"])
def test_extracted_cannot_stand_on_a_blank_raw_value(raw: str) -> None:
    """The catalog calls "" meaningful; whitespace is still not an identity."""
    observation, assessment = _real_award_assessment(raw)
    assert assessment.status == "meaningful"
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="extracted",
            award_id=raw,  # type: ignore[arg-type]
            reason_codes=(),
        )


def test_extracted_cannot_stand_on_a_non_string_raw_value() -> None:
    observation = _obs(12345)
    assessment = _hand_assessment(observation, status="meaningful")
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="extracted",
            award_id=12345,  # type: ignore[arg-type]
            reason_codes=(),
        )


@pytest.mark.parametrize(
    "status,sentinel_status,code",
    [
        ("extracted", "sentinel_absence", "PROCESS_AWARD_ID_NO_AWARD"),
        ("extracted", "sentinel_unknown", "PROCESS_AWARD_ID_UNDEFINED"),
        ("extracted", "unrecognized", None),
        ("absent", "meaningful", None),
        ("absent", "sentinel_unknown", "PROCESS_AWARD_ID_UNDEFINED"),
        ("absent", "unrecognized", None),
        ("unrecognized", "sentinel_unknown", "PROCESS_AWARD_ID_UNDEFINED"),
        ("unrecognized", "meaningful", None),
        ("unrecognized", "sentinel_absence", "PROCESS_AWARD_ID_NO_AWARD"),
        ("unknown", "sentinel_absence", "PROCESS_AWARD_ID_NO_AWARD"),
        ("unknown", "unrecognized", None),
    ],
)
def test_status_may_not_contradict_the_sentinel_that_backs_it(
    status: str, sentinel_status: str, code: str | None
) -> None:
    observation = _obs("CO1.AWD.1")
    assessment = _hand_assessment(observation, status=sentinel_status, code=code)
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status=status,  # type: ignore[arg-type]
            award_id="CO1.AWD.1" if status == "extracted" else None,  # type: ignore[arg-type]
            reason_codes=(),
        )


@pytest.mark.parametrize(
    "status,sentinel_status,code",
    [
        ("extracted", "meaningful", None),
        ("absent", "sentinel_absence", "PROCESS_AWARD_ID_NO_AWARD"),
        ("unrecognized", "unrecognized", None),
        ("unknown", "sentinel_unknown", "PROCESS_AWARD_ID_UNDEFINED"),
        ("unknown", "meaningful", None),
        ("unknown", "not_applicable", "PROCESS_AWARD_ID_NOT_APPLICABLE"),
    ],
)
def test_each_status_accepts_the_classification_that_backs_it(
    status: str, sentinel_status: str, code: str | None
) -> None:
    raw = (
        "   "
        if status == "unknown" and sentinel_status == "meaningful"
        else "CO1.AWD.1"
    )
    observation = _obs(raw)
    assessment = _hand_assessment(observation, status=sentinel_status, code=code)
    extraction = AwardIdentityExtraction(
        dataset_id=PROCESSES,
        raw_observation=observation,
        sentinel_assessment=assessment,
        status=status,  # type: ignore[arg-type]
        award_id=raw if status == "extracted" else None,  # type: ignore[arg-type]
        reason_codes=(),
    )
    assert extraction.status == status


def test_meaningful_non_blank_raw_cannot_be_hidden_as_unknown() -> None:
    observation = _obs("CO1.AWD.123")
    assessment = _hand_assessment(observation, status="meaningful")
    with pytest.raises(ValueError, match="cannot hide or invent an AwardId"):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=assessment,
            status="unknown",
            award_id=None,
            reason_codes=(),
        )


def test_meaningful_whitespace_raw_may_remain_unknown() -> None:
    observation = _obs(" \t ")
    assessment = _hand_assessment(observation, status="meaningful")
    extraction = AwardIdentityExtraction(
        dataset_id=PROCESSES,
        raw_observation=observation,
        sentinel_assessment=assessment,
        status="unknown",
        award_id=None,
        reason_codes=(),
    )
    assert extraction.raw_observation.raw_value == " \t "
    assert extract_award_identity(observation).status == extraction.status


def test_constructor_and_extractor_require_verbatim_extraction_for_meaningful_id() -> (
    None
):
    observation = _obs("  CO1.AWD.123  ")
    assessment = _hand_assessment(observation, status="meaningful")
    constructed = AwardIdentityExtraction(
        dataset_id=PROCESSES,
        raw_observation=observation,
        sentinel_assessment=assessment,
        status="extracted",
        award_id="  CO1.AWD.123  ",  # type: ignore[arg-type]
        reason_codes=(),
    )
    extracted = extract_award_identity(observation)
    assert constructed.status == extracted.status == "extracted"
    assert constructed.award_id == extracted.award_id == "  CO1.AWD.123  "


@pytest.mark.parametrize("status", ["extracted", "absent", "unrecognized"])
def test_a_sentinel_verdict_requires_the_assessment_that_produced_it(
    status: str,
) -> None:
    observation = _obs("CO1.AWD.1")
    with pytest.raises(ValueError):
        AwardIdentityExtraction(
            dataset_id=PROCESSES,
            raw_observation=observation,
            sentinel_assessment=None,
            status=status,  # type: ignore[arg-type]
            award_id="CO1.AWD.1" if status == "extracted" else None,  # type: ignore[arg-type]
            reason_codes=(),
        )


@pytest.mark.parametrize(
    "observation", [_missing(), _not_observed(), _obs(12345), _obs("")]
)
def test_unknown_may_stand_without_any_assessment(
    observation: FieldObservation,
) -> None:
    """``unknown`` covers non-observation, unavailable policy, type mismatch
    and a blank string alike — none of which can produce a classification."""
    extraction = AwardIdentityExtraction(
        dataset_id=PROCESSES,
        raw_observation=observation,
        sentinel_assessment=None,
        status="unknown",
        award_id=None,
        reason_codes=(),
    )
    assert extraction.status == "unknown"
    assert extraction.award_id is None


def test_every_extractor_outcome_keeps_its_provenance() -> None:
    """The extractor never emits a state its own dataclass would refuse."""
    for raw in ("CO1.AWD.1", "No Adjudicado", "", "   ", 12345, True):
        extraction = extract_award_identity(_obs(raw))
        assert extraction.dataset_id == PROCESSES
        if extraction.sentinel_assessment is not None:
            assert extraction.sentinel_assessment.dataset_id == PROCESSES
            assert extraction.sentinel_assessment.field_name == "id_adjudicacion"
            assert (
                extraction.sentinel_assessment.raw_observation
                is extraction.raw_observation
            )


# ---------------------------------------------------------------------------
# 8d. A contract conflict is contractual — evidence included.
# ---------------------------------------------------------------------------


def test_conflicting_lookup_requires_its_own_conflict() -> None:
    with pytest.raises(ValueError):
        _lookup("conflicting", default_conflict=False)


def test_conflicting_lookup_forbids_a_source_error() -> None:
    with pytest.raises(ValueError):
        _lookup("conflicting", _timeout())


def test_a_contract_conflict_may_not_lean_on_foreign_evidence() -> None:
    """Process evidence may not pad a contract conflict to the minimum of two."""
    mixed = (
        _contract_evidence("CO1.PCCNTR.1"),
        _contract_evidence(
            "CO1.AWD.1",
            code="AWARD_ID_EVIDENCE",
            dataset=PROCESSES,
            field="id_adjudicacion",
        ),
    )
    with pytest.raises(ValueError):
        _lookup("conflicting", conflict=_contract_conflict(evidences=mixed))


@pytest.mark.parametrize(
    "outcome",
    ["observed", "absent_after_completed_lookup", "not_queried", "unknown"],
)
def test_a_non_conflicting_outcome_cannot_carry_a_conflict(outcome: str) -> None:
    with pytest.raises(ValueError):
        _lookup(outcome, conflict=_contract_conflict())


def test_a_contract_conflict_is_preserved_exactly_as_the_lookup_found_it() -> None:
    conflict = _contract_conflict()
    lookup = _lookup("conflicting", conflict=conflict)
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1"),
        contract_lookup=lookup,
    )
    preserved = {c.code: c for c in resolution.quality.conflicts}[
        "CONTRACT_LOOKUP_AMBIGUOUS_LINK"
    ]
    assert preserved is conflict
    assert preserved.evidences == conflict.evidences
    assert {e.dataset for e in preserved.evidences} == {CONTRACTS}
    # Its evidence reaches the general evidence set, so the conflict can be
    # audited from the assessment alone.
    assert set(conflict.evidences) <= set(resolution.evidence)


def test_no_process_evidence_is_introduced_into_a_contract_conflict() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1"),
        contract_lookup=_lookup("conflicting"),
    )
    cited = [e for c in resolution.quality.conflicts for e in c.evidences]
    assert cited, "the contractual conflict disappeared"
    assert {e.dataset for e in cited} == {CONTRACTS}
    assert "id_adjudicacion" not in {e.field for e in cited}
    assert not [e for e in cited if e.raw_observation.presence != "observed"]


def test_award_observed_with_a_conflicting_lookup_keeps_both_axes() -> None:
    lookup = _lookup("conflicting")
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1"),
        contract_lookup=lookup,
    )
    assert resolution.award_signal == "observed"
    assert resolution.award_id == "CO1.AWD.1"
    assert resolution.contract_link_signal == "conflicting"
    assert resolution.resolution_status == "conflicting"
    assert resolution.contract_lookup is lookup
    # The internal contradiction stays contractual; the award axis is not
    # dragged into it.
    assert {c.code for c in resolution.quality.conflicts} == {
        "CONTRACT_LOOKUP_AMBIGUOUS_LINK"
    }


def test_a_genuine_cross_axis_conflict_still_names_both_datasets() -> None:
    """The redesign removes the fabricated conflict, not the authentic one."""
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="No Adjudicado"),
        contract_lookup=_lookup("observed"),
    )
    conflict = {c.code: c for c in resolution.quality.conflicts}[
        "PROCESS_AWARD_ABSENT_VS_CONTRACT_LINK"
    ]
    assert {e.dataset for e in conflict.evidences} == {PROCESSES, CONTRACTS}
    # The contract side of a cross-axis conflict is a real observation.
    contract_side = [e for e in conflict.evidences if e.dataset == CONTRACTS]
    assert all(e.raw_observation.presence == "observed" for e in contract_side)


def test_award_absent_with_a_conflicting_lookup_keeps_both_axes() -> None:
    lookup = _lookup("conflicting")
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="No Adjudicado"),
        contract_lookup=lookup,
    )
    assert resolution.award_signal == "not_indicated"
    assert resolution.contract_link_signal == "conflicting"
    assert {c.code for c in resolution.quality.conflicts} == {
        "CONTRACT_LOOKUP_AMBIGUOUS_LINK"
    }


# ---------------------------------------------------------------------------
# 8e. A sentinel type mismatch is visible, never silent.
# ---------------------------------------------------------------------------


def test_numeric_summary_state_never_yields_complete_or_high_quality() -> None:
    assessment = resolve_process_lifecycle(
        _signals(state="Publicado", phase="Cerrada", summary=123)
    )
    evidence = {e.field: e for e in assessment.evidence}["estado_resumen"]
    # The raw survives; only the classification is missing, and it says so.
    assert evidence.sentinel_assessment is None
    assert evidence.raw_observation.raw_value == 123
    assert "PROCESS_SUMMARY_SENTINEL_TYPE_MISMATCH" in assessment.quality.reason_codes
    assert assessment.quality.completeness in {"partial", "insufficient"}
    assert assessment.quality.completeness != "complete"
    assert assessment.quality.confidence in {"medium", "low"}
    assert assessment.quality.confidence != "high"


def test_the_same_signals_with_a_classifiable_summary_are_complete() -> None:
    """The control: only the unclassifiable type degrades the quality."""
    assessment = resolve_process_lifecycle(
        _signals(state="Publicado", phase="Cerrada", summary="No Definido")
    )
    assert assessment.quality.completeness == "complete"
    assert assessment.quality.confidence == "high"
    assert (
        "PROCESS_SUMMARY_SENTINEL_TYPE_MISMATCH"
        not in assessment.quality.reason_codes
    )


def test_an_unavailable_summary_policy_is_equally_visible() -> None:
    assessment = resolve_process_lifecycle(
        _signals(state="Publicado", phase="Cerrada", summary="No Definido"),
        dataset_id="not-a-dataset",
    )
    evidence = {e.field: e for e in assessment.evidence}["estado_resumen"]
    assert evidence.sentinel_assessment is None
    assert (
        "PROCESS_SUMMARY_SENTINEL_POLICY_UNAVAILABLE"
        in assessment.quality.reason_codes
    )
    assert assessment.quality.completeness != "complete"
    assert assessment.quality.confidence != "high"


def test_the_defined_summary_case_still_attaches_its_sentinel() -> None:
    assessment = resolve_process_lifecycle(
        _signals(state="Publicado", summary="No Definido")
    )
    evidence = {e.field: e for e in assessment.evidence}["estado_resumen"]
    assert evidence.sentinel_assessment is not None
    assert evidence.sentinel_assessment.status == "sentinel_unknown"
    for code in (
        "PROCESS_SUMMARY_SENTINEL_TYPE_MISMATCH",
        "PROCESS_SUMMARY_SENTINEL_POLICY_UNAVAILABLE",
    ):
        assert code not in assessment.quality.reason_codes


def test_award_id_type_mismatch_keeps_only_the_extractor_codes() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id=123)
    )
    codes = resolution.quality.reason_codes
    assert "AWARD_ID_NOT_A_STRING" in codes
    assert "PROCESS_SUMMARY_SENTINEL_TYPE_MISMATCH" not in codes
    assert len(codes) == len(set(codes))
    elsewhere = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1"),
        dataset_id="not-a-dataset",
    )
    assert "AWARD_ID_SENTINEL_POLICY_UNAVAILABLE" in elsewhere.quality.reason_codes


# ---------------------------------------------------------------------------
# 8f. Public constructors cannot mint an incoherent assessment.
# ---------------------------------------------------------------------------


def test_lifecycle_stage_cannot_be_replaced_with_an_incompatible_one() -> None:
    assessment = resolve_process_lifecycle(_signals(state="Evaluación"))
    assert assessment.stage == "evaluation"
    with pytest.raises(ValueError):
        dataclasses.replace(assessment, stage="cancelled")
    for stage in sorted(_ALL_STAGES - {"evaluation"}):
        with pytest.raises(ValueError):
            dataclasses.replace(assessment, stage=stage)


def test_lifecycle_stage_follows_the_raw_signals_it_is_given() -> None:
    assessment = resolve_process_lifecycle(_signals(state="Evaluación"))
    # Swapping the decisive raw signal without swapping the stage is refused.
    with pytest.raises(ValueError):
        dataclasses.replace(assessment, raw_procedure_state=_obs("Cancelado"))
    coherent = dataclasses.replace(
        assessment, raw_procedure_state=_obs("Cancelado"), stage="cancelled"
    )
    assert coherent.stage == "cancelled"


def test_lifecycle_stage_is_recomputed_only_from_state_and_phase() -> None:
    """Opening state, award, contract, modality and dates stay out of it."""
    assessment = resolve_process_lifecycle(
        _signals(state="Publicado", opening="Abierto", summary="No Definido")
    )
    assert assessment.stage == "published"
    # Replacing a non-decisive raw signal cannot invalidate the stage.
    assert (
        dataclasses.replace(assessment, raw_opening_state=_obs("Cerrado")).stage
        == "published"
    )
    assert (
        dataclasses.replace(assessment, raw_summary_state=_obs("Adjudicado")).stage
        == "published"
    )
    # Replacing the phase, which IS decisive, does invalidate it.
    with pytest.raises(ValueError):
        dataclasses.replace(assessment, raw_phase=_obs("Presentación de oferta"))


def test_lifecycle_rejects_a_non_canonical_or_foreign_evidence_set() -> None:
    assessment = resolve_process_lifecycle(_signals(state="Publicado"))
    with pytest.raises(ValueError):
        dataclasses.replace(assessment, evidence=tuple(reversed(assessment.evidence)))
    with pytest.raises(TypeError):
        dataclasses.replace(assessment, evidence=("estado_del_procedimiento",))
    with pytest.raises(TypeError):
        dataclasses.replace(assessment, evidence=list(assessment.evidence))
    with pytest.raises(TypeError):
        dataclasses.replace(assessment, quality="complete")


def test_resolution_status_cannot_be_replaced_with_an_incompatible_one() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="CO1.AWD.1")
    )
    assert resolution.resolution_status == "conflicting"
    for status in ("resolved", "unresolved", "unknown"):
        with pytest.raises(ValueError):
            dataclasses.replace(resolution, resolution_status=status)


def test_award_signal_cannot_be_replaced_with_an_incompatible_one() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="No Adjudicado")
    )
    assert resolution.award_signal == "not_indicated"
    for signal in ("observed", "unknown", "conflicting"):
        with pytest.raises(ValueError):
            dataclasses.replace(resolution, award_signal=signal)


def test_contract_link_signal_cannot_be_replaced_with_an_incompatible_one() -> None:
    lookup = _lookup("absent_after_completed_lookup")
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="No Adjudicado"),
        contract_lookup=lookup,
    )
    for signal in ("observed", "not_queried", "source_error", "unknown", "conflicting"):
        with pytest.raises(ValueError):
            dataclasses.replace(resolution, contract_link_signal=signal)


def test_quality_may_not_call_a_conflicting_signal_consistent() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="CO1.AWD.1")
    )
    assert resolution.award_signal == "conflicting"
    # A conflict-free quality passes ResolutionQuality's own check, so only
    # the assessment can catch the contradiction.
    clean = build_resolution_quality(
        completeness="partial", consistency="consistent", confidence="medium"
    )
    with pytest.raises(ValueError):
        dataclasses.replace(resolution, quality=clean)


def test_quality_may_not_drop_the_lookup_source_error() -> None:
    lookup = _lookup("source_error", _timeout())
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="No Adjudicado"),
        contract_lookup=lookup,
    )
    assert resolution.quality.source_errors == (lookup.source_error,)
    stripped = build_resolution_quality(
        completeness=resolution.quality.completeness,
        consistency=resolution.quality.consistency,
        confidence=resolution.quality.confidence,
        reason_codes=resolution.quality.reason_codes,
        missing_fields=resolution.quality.missing_fields,
    )
    with pytest.raises(ValueError):
        dataclasses.replace(resolution, quality=stripped)
    kept = build_resolution_quality(
        completeness=resolution.quality.completeness,
        consistency=resolution.quality.consistency,
        confidence=resolution.quality.confidence,
        reason_codes=resolution.quality.reason_codes,
        missing_fields=resolution.quality.missing_fields,
        source_errors=resolution.quality.source_errors,
    )
    assert dataclasses.replace(resolution, quality=kept).quality.source_errors == (
        lookup.source_error,
    )


def test_quality_may_not_drop_the_lookup_conflict() -> None:
    lookup = _lookup("conflicting")
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1"),
        contract_lookup=lookup,
    )
    stripped = build_resolution_quality(
        completeness=resolution.quality.completeness,
        consistency="conflicting",
        confidence=resolution.quality.confidence,
        reason_codes=resolution.quality.reason_codes,
        missing_fields=resolution.quality.missing_fields,
    )
    with pytest.raises(ValueError):
        dataclasses.replace(resolution, quality=stripped)


def test_resolution_rejects_a_non_canonical_or_foreign_evidence_set() -> None:
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1"),
        contract_lookup=_lookup("observed"),
    )
    with pytest.raises(ValueError):
        dataclasses.replace(resolution, evidence=tuple(reversed(resolution.evidence)))
    with pytest.raises(ValueError):
        dataclasses.replace(
            resolution, evidence=resolution.evidence + resolution.evidence[:1]
        )
    with pytest.raises(TypeError):
        dataclasses.replace(resolution, evidence=list(resolution.evidence))


def test_offer_phase_against_post_offer_state_is_a_preserved_conflict() -> None:
    assessment = resolve_process_lifecycle(
        _signals(state="Seleccionado", phase="Presentación de oferta")
    )
    # Precedence still applies; the contradiction is kept, not dropped.
    assert assessment.stage == "selected"
    codes = {c.code for c in assessment.quality.conflicts}
    assert "PROCESS_OFFER_PHASE_VS_POST_OFFER_STATE" in codes
    assert assessment.quality.consistency == "conflicting"


def test_offer_phase_with_closed_opening_is_a_preserved_conflict() -> None:
    assessment = resolve_process_lifecycle(
        _signals(state="Publicado", phase="Presentación de oferta", opening="Cerrado")
    )
    assert assessment.stage == "offer_phase_observed"
    codes = {c.code for c in assessment.quality.conflicts}
    assert "PROCESS_OFFER_PHASE_VS_CLOSED_OPENING" in codes


def test_critical_conflict_coexists_with_a_preserved_stage() -> None:
    signals = _signals(
        state="Evaluación", adjudicated="No", adjudication_id="CO1.AWD.1"
    )
    lifecycle = resolve_process_lifecycle(signals)
    resolution = resolve_process_resolution(signals)
    assert lifecycle.stage == "evaluation"
    assert any(c.severity == "critical" for c in resolution.quality.conflicts)


# ---------------------------------------------------------------------------
# 9. Anti-bypass: cataloged fields must carry their sentinel assessment.
# ---------------------------------------------------------------------------


def test_summary_state_evidence_carries_its_sentinel_assessment() -> None:
    assert get_field_sentinel_policy(PROCESSES, "estado_resumen") is not None
    assessment = resolve_process_lifecycle(
        _signals(state="Publicado", summary="No Definido")
    )
    evidence = {e.field: e for e in assessment.evidence}["estado_resumen"]
    assert evidence.sentinel_assessment is not None
    assert evidence.sentinel_assessment.status == "sentinel_unknown"
    assert evidence.raw_observation.raw_value == "No Definido"


def test_award_id_evidence_carries_its_sentinel_assessment() -> None:
    assert get_field_sentinel_policy(PROCESSES, "id_adjudicacion") is not None
    resolution = resolve_process_resolution(
        _signals(adjudicated="No", adjudication_id="No Adjudicado")
    )
    evidence = {e.field: e for e in resolution.evidence}["id_adjudicacion"]
    assert evidence.sentinel_assessment is not None
    assert evidence.sentinel_assessment.status == "sentinel_absence"


def test_every_cataloged_observed_field_gets_an_assessment() -> None:
    signals = _signals(
        state="Publicado",
        summary="No Definido",
        adjudicated="Si",
        adjudication_id="CO1.AWD.1",
        adjudication_date="2025-01-01",
    )
    produced = list(resolve_process_lifecycle(signals).evidence) + list(
        resolve_process_resolution(signals).evidence
    )
    for evidence in produced:
        cataloged = get_field_sentinel_policy(evidence.dataset, evidence.field)
        observed = evidence.raw_observation.presence == "observed"
        if cataloged is not None and observed:
            assert evidence.sentinel_assessment is not None, (
                f"{evidence.field} bypassed the sentinel layer"
            )
        else:
            assert evidence.sentinel_assessment is None


def test_uncataloged_fields_produce_evidence_without_a_sentinel() -> None:
    assessment = resolve_process_lifecycle(
        _signals(state="Evaluación", phase="Presentación de oferta", opening="Abierto")
    )
    by_field = {e.field: e for e in assessment.evidence}
    for field in ("estado_del_procedimiento", "fase", "estado_de_apertura_del_proceso"):
        assert get_field_sentinel_policy(PROCESSES, field) is None
        assert by_field[field].sentinel_assessment is None
        assert by_field[field].normalized_value is None


def test_module_does_not_reimplement_sentinel_semantics() -> None:
    tree = ast.parse(inspect.getsource(process_mod))
    defined = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
    }
    assert not defined & {
        "SentinelAssessment",
        "SentinelRule",
        "FieldSentinelPolicy",
        "assess_sentinel",
        "Evidence",
        "AssessmentConflict",
        "ResolutionQuality",
        "SourceError",
    }


# ---------------------------------------------------------------------------
# 10. Structure and determinism.
# ---------------------------------------------------------------------------


def test_assessments_are_frozen_slotted_and_hashable() -> None:
    signals = _signals(state="Publicado", adjudicated="Si", adjudication_id="CO1.AWD.1")
    lifecycle = resolve_process_lifecycle(signals)
    resolution = resolve_process_resolution(signals)
    identity = resolution.award_identity
    lookup = _lookup("observed")
    for instance in (lifecycle, resolution, identity, lookup):
        assert dataclasses.is_dataclass(instance)
        assert instance.__dataclass_params__.frozen  # type: ignore[attr-defined]
        assert "__dict__" not in dir(instance)
        assert isinstance(hash(instance), int)
    with pytest.raises(dataclasses.FrozenInstanceError):
        lifecycle.stage = "cancelled"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        resolution.award_signal = "observed"  # type: ignore[misc]


def test_resolution_is_deterministic() -> None:
    signals = _signals(
        state="Evaluación",
        phase="Presentación de oferta",
        summary="No Definido",
        adjudicated="Si",
        adjudication_id="CO1.AWD.1",
    )
    a_life = resolve_process_lifecycle(signals)
    b_life = resolve_process_lifecycle(signals)
    a_res = resolve_process_resolution(signals, contract_lookup=_lookup("observed"))
    b_res = resolve_process_resolution(signals, contract_lookup=_lookup("observed"))
    assert a_life == b_life and hash(a_life) == hash(b_life)
    assert a_res == b_res and hash(a_res) == hash(b_res)
    assert a_life.quality.reason_codes == b_life.quality.reason_codes


def test_resolvers_reject_wrong_input_types() -> None:
    with pytest.raises(TypeError):
        resolve_process_lifecycle({"estado_del_procedimiento": "Publicado"})  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        resolve_process_resolution(_signals(), contract_lookup="observed")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        extract_award_identity("CO1.AWD.1")  # type: ignore[arg-type]


def test_assessments_reuse_t1b_1_classes_without_duplicating_them() -> None:
    signals = _signals(state="Publicado", adjudicated="Si", adjudication_id="CO1.AWD.1")
    lifecycle = resolve_process_lifecycle(signals)
    assert isinstance(lifecycle.quality, ResolutionQuality)
    assert all(isinstance(e, Evidence) for e in lifecycle.evidence)
    assert process_mod.Evidence is Evidence
    assert process_mod.ResolutionQuality is ResolutionQuality


def test_raw_observations_are_preserved_by_identity() -> None:
    state = _obs("Publicado")
    signals = _signals(state=state, adjudication_id=_obs("CO1.AWD.1"))
    lifecycle = resolve_process_lifecycle(signals)
    assert lifecycle.raw_procedure_state is state
    by_field = {e.field: e for e in lifecycle.evidence}
    assert by_field["estado_del_procedimiento"].raw_observation is state


# ---------------------------------------------------------------------------
# 11. Exports.
# ---------------------------------------------------------------------------


_T1B_2_EXPORTS = {
    "PROCESS_DATASET_ID",
    "ProcessLifecycleStage",
    "AwardResolutionSignal",
    "ContractLinkSignal",
    "ProcessResolutionStatus",
    "AwardIdentityStatus",
    "AwardId",
    "AwardIdentityExtraction",
    "ContractLookupResult",
    "ProcessLifecycleAssessment",
    "ProcessResolutionAssessment",
    "extract_award_identity",
    "resolve_process_lifecycle",
    "resolve_process_resolution",
}


def test_process_module_keeps_exactly_its_public_exports() -> None:
    assert set(process_mod.__all__) == _T1B_2_EXPORTS
    assert len(process_mod.__all__) == len(_T1B_2_EXPORTS)


def test_t1b_2_api_is_published_and_earlier_blocks_preserved() -> None:
    published = set(truth_layer_pkg.__all__)
    assert _T1B_2_EXPORTS <= published
    earlier = {
        "FieldObservation",
        "ProcessLifecycleSignals",
        "RawScalar",
        "SentinelAssessment",
        "assess_sentinel",
        "get_field_sentinel_policy",
        "Evidence",
        "AssessmentConflict",
        "ResolutionQuality",
        "SourceError",
        "build_resolution_quality",
        "validate_evidence_set",
    }
    assert earlier <= published
    names = list(truth_layer_pkg.__all__)
    assert len(names) == len(set(names))


# ---------------------------------------------------------------------------
# 12. Purity and negative scope.
# ---------------------------------------------------------------------------


def test_module_imports_are_pure() -> None:
    tree = ast.parse(inspect.getsource(process_mod))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module is not None:
                imported.add(node.module.split(".")[0])
    forbidden = {
        "datetime",
        "time",
        "os",
        "sys",
        "subprocess",
        "pathlib",
        "urllib",
        "requests",
        "http",
        "socket",
        "json",
        "logging",
        "sqlite3",
        "asyncio",
        "random",
        "uuid",
    }
    assert not (imported & forbidden), f"impure imports: {imported & forbidden}"
    assert imported <= {"app", "dataclasses", "typing", "unicodedata", "__future__"}


def test_module_does_not_touch_runtime_consumers() -> None:
    source = _executable_source(process_mod)
    for forbidden in (
        "opportunity_stage",
        "formatter",
        "soql_builder",
        "QueryFrame",
        "radar",
        "workflow",
        "orchestrator",
        "query_router",
        "entity_resolver",
        "secop_client",
    ):
        assert forbidden not in source, f"runtime consumer referenced: {forbidden}"


def test_module_has_no_clock_or_environment_access() -> None:
    source = inspect.getsource(process_mod)
    for forbidden in ("datetime.now", "time.time", "os.environ", "open(", "random."):
        assert forbidden not in source, f"forbidden runtime access: {forbidden}"


def test_no_offerability_temporality_modality_or_lot_scope() -> None:
    tree = ast.parse(inspect.getsource(process_mod))
    defined = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
    }
    assert not defined & {
        "OpportunityRoleAssessment",
        "TemporalAssessment",
        "PublicationVisibilityAssessment",
        "ContractLifecycleAssessment",
        "ContractPerformanceAssessment",
        "LotStructureEvidence",
        "LotIdentityEvidence",
        "ContractLotLinkageEvidence",
    }
    source = _executable_source(process_mod)
    for forbidden in ("bid_ready", "offerab", "numero_de_lotes", "Lot", "lote"):
        assert forbidden not in source, f"out-of-scope concept: {forbidden}"


def test_modality_is_carried_but_never_used_to_decide() -> None:
    competitive = _signals(state="Publicado", adjudicated="Si")
    assert resolve_process_lifecycle(competitive).stage == "published"
    # The modality observation exists in T1A but never reaches a decision.
    assert "raw_modality" not in _executable_source(process_mod)


def test_module_has_no_clock_or_environment_access_in_code() -> None:
    source = _executable_source(process_mod)
    for forbidden in ("datetime", "time.", "os.", "random", "uuid"):
        assert forbidden not in source, f"forbidden runtime access: {forbidden}"


# ---------------------------------------------------------------------------
# 13. Opportunity shadow compatibility — legacy stays untouched.
# ---------------------------------------------------------------------------


def test_legacy_classifier_behaviour_is_unchanged() -> None:
    assert CLASSIFIER_VERSION == "0.1"
    cases = [
        ({"estado_del_procedimiento": "Publicado", "fase": "Presentación de oferta"},
         "offer_phase_indicated"),
        ({"estado_del_procedimiento": "Borrador"}, "unpublished_draft_signal"),
        ({"estado_del_procedimiento": "Evaluación"}, "post_offer_signal"),
        ({"estado_del_procedimiento": "Publicado",
          "fase": "Presentación de oferta",
          "estado_de_apertura_del_proceso": "Cerrado"}, "conflicting_signals"),
        ({}, "unknown"),
    ]
    for row, expected in cases:
        assert classify_opportunity_stage(row).stage == expected


def test_legacy_and_truth_layer_vocabularies_stay_distinct() -> None:
    from typing import get_args

    legacy_stages = set(get_args(OpportunityStage))
    # "unknown" is the one deliberately shared fallback; every other value of
    # each vocabulary is exclusive, so no legacy stage can be mistaken for a
    # Truth Layer stage.
    assert (legacy_stages & _ALL_STAGES) == {"unknown"}
    assert "selected_or_awarded" not in legacy_stages | _ALL_STAGES
    # The legacy classifier never emits a Truth Layer stage.
    assert "offer_phase_observed" not in legacy_stages
    assert "offer_phase_indicated" not in _ALL_STAGES


def test_legacy_module_does_not_import_the_truth_layer() -> None:
    source = inspect.getsource(legacy_mod)
    assert "truth_layer" not in source
    assert "process_lifecycle" not in source


# ---------------------------------------------------------------------------
# 14. Cartesian sweep of the public state space.
# ---------------------------------------------------------------------------

# Inputs that legitimately produce each award signal. No mock forces a signal:
# every one is derived by the resolver from real T1A observations.
_AWARD_INPUTS: dict[str, dict[str, object]] = {
    "observed": {"adjudicated": "Si", "adjudication_id": "CO1.AWD.1"},
    "not_indicated": {"adjudicated": "No", "adjudication_id": "No Adjudicado"},
    "unknown": {},
    "conflicting": {"adjudicated": "No", "adjudication_id": "CO1.AWD.1"},
}

_LINK_OUTCOMES = (
    "observed",
    "absent_after_completed_lookup",
    "not_queried",
    "source_error",
    "unknown",
    "conflicting",
)

# The full 4 x 6 product, written out rather than derived, so the expectation
# is an independent statement and not a copy of the implementation.
_EXPECTED_RESOLUTION: dict[tuple[str, str], str] = {
    ("observed", "observed"): "resolved",
    ("observed", "absent_after_completed_lookup"): "resolved",
    ("observed", "not_queried"): "resolved",
    ("observed", "source_error"): "resolved",
    ("observed", "unknown"): "resolved",
    ("observed", "conflicting"): "conflicting",
    ("not_indicated", "observed"): "conflicting",
    ("not_indicated", "absent_after_completed_lookup"): "unresolved",
    ("not_indicated", "not_queried"): "unknown",
    ("not_indicated", "source_error"): "unknown",
    ("not_indicated", "unknown"): "unknown",
    ("not_indicated", "conflicting"): "conflicting",
    ("unknown", "observed"): "resolved",
    ("unknown", "absent_after_completed_lookup"): "unknown",
    ("unknown", "not_queried"): "unknown",
    ("unknown", "source_error"): "unknown",
    ("unknown", "unknown"): "unknown",
    ("unknown", "conflicting"): "conflicting",
    ("conflicting", "observed"): "conflicting",
    ("conflicting", "absent_after_completed_lookup"): "conflicting",
    ("conflicting", "not_queried"): "conflicting",
    ("conflicting", "source_error"): "conflicting",
    ("conflicting", "unknown"): "conflicting",
    ("conflicting", "conflicting"): "conflicting",
}


def test_the_cartesian_matrix_covers_the_whole_public_state_space() -> None:
    assert set(_EXPECTED_RESOLUTION) == {
        (award, link) for award in _AWARD_INPUTS for link in _LINK_OUTCOMES
    }
    assert len(_EXPECTED_RESOLUTION) == 24


@pytest.mark.parametrize("link", _LINK_OUTCOMES)
@pytest.mark.parametrize("award", sorted(_AWARD_INPUTS))
def test_cartesian_award_by_contract_link_sweep(award: str, link: str) -> None:
    """Every legitimately reachable state, checked against every invariant.

    A green suite proves nothing about states no test constructs. Each
    combination here is produced by the resolver from genuine T1A inputs and a
    genuine ``ContractLookupResult`` — nothing is forced past the dataclasses.
    """
    lookup = _lookup(link, _timeout() if link == "source_error" else None)
    signals = _signals(**_AWARD_INPUTS[award])  # type: ignore[arg-type]
    resolution = resolve_process_resolution(signals, contract_lookup=lookup)

    # 1. The two signals and the derived status.
    assert resolution.award_signal == award
    assert resolution.contract_link_signal == link
    assert resolution.resolution_status == _EXPECTED_RESOLUTION[(award, link)]

    # 2. No conflicting state is ever reported as consistent.
    conflicting = "conflicting" in {
        award,
        link,
        resolution.resolution_status,
    }
    if conflicting or resolution.quality.conflicts:
        assert resolution.quality.consistency == "conflicting"
        assert resolution.quality.confidence == "low"
    else:
        assert resolution.quality.consistency == "consistent"
        assert resolution.quality.conflicts == ()

    # 3. The lookup itself is preserved, never summarized away.
    assert resolution.contract_lookup is lookup

    # 4. Evidence from both datasets survives.
    by_field = {e.field: e for e in resolution.evidence}
    for field in ("adjudicado", "id_adjudicacion", "fecha_adjudicacion"):
        assert by_field[field].dataset == PROCESSES
    assert by_field[LINK_FIELD].dataset == CONTRACTS
    assert resolution.evidence == _canonical(resolution.evidence)
    assert len(set(resolution.evidence)) == len(resolution.evidence)

    # 5. A source failure is preserved as such, and is never an absence.
    if link == "source_error":
        assert resolution.quality.source_errors == (lookup.source_error,)
        assert by_field[LINK_FIELD].source_error is lookup.source_error
        assert by_field[LINK_FIELD].role == "not_observed"
        assert LINK_FIELD not in resolution.quality.missing_fields
    else:
        assert resolution.quality.source_errors == ()

    # 6. The lookup's own conflict is preserved with its own evidence.
    if link == "conflicting":
        assert lookup.conflict in resolution.quality.conflicts
        assert set(lookup.conflict.evidences) <= set(resolution.evidence)
        assert {
            e.dataset for e in lookup.conflict.evidences
        } == {CONTRACTS}
    else:
        assert lookup.conflict is None

    # 7. The modelled cross-axis conflicts, and only those.
    expected_codes: set[str] = set()
    if award == "conflicting":
        expected_codes.add("PROCESS_AWARD_FLAG_VS_AWARD_IDENTITY")
    if award == "not_indicated" and link == "observed":
        expected_codes.add("PROCESS_AWARD_ABSENT_VS_CONTRACT_LINK")
    if link == "conflicting":
        expected_codes.add("CONTRACT_LOOKUP_AMBIGUOUS_LINK")
    assert {c.code for c in resolution.quality.conflicts} == expected_codes

    # 8. The award identity is the only source of an AwardId.
    assert resolution.award_id == resolution.award_identity.award_id
    assert (resolution.award_id is not None) == (
        resolution.award_identity.status == "extracted"
    )


@pytest.mark.parametrize("award", sorted(_AWARD_INPUTS))
def test_absent_lookup_matches_an_explicit_not_queried_lookup(award: str) -> None:
    """``contract_lookup=None`` and an explicit ``not_queried`` agree."""
    signals = _signals(**_AWARD_INPUTS[award])  # type: ignore[arg-type]
    implicit = resolve_process_resolution(signals)
    explicit = resolve_process_resolution(
        signals, contract_lookup=_lookup("not_queried")
    )
    assert implicit.contract_link_signal == explicit.contract_link_signal
    assert implicit.resolution_status == explicit.resolution_status
    assert implicit.award_signal == explicit.award_signal
    # ...but the implicit one invents no contract provenance at all.
    assert implicit.contract_lookup is None
    assert not [e for e in implicit.evidence if e.dataset != PROCESSES]


def test_truth_layer_does_not_replace_legacy_reason_codes() -> None:
    resolution = resolve_process_resolution(
        _signals(state="Publicado", adjudicated="Si", adjudication_id="CO1.AWD.1")
    )
    legacy_reasons = {
        "phase_offer_submission",
        "procedure_state_published",
        "procedure_state_post_offer",
        "opening_open",
    }
    assert not set(resolution.quality.reason_codes) & legacy_reasons
    # Truth Layer codes are UPPER_SNAKE_CASE; legacy ones are lowercase.
    assert all(code.isupper() for code in resolution.quality.reason_codes)


def test_internal_process_resolution_is_single_pass_and_replace_stays_public(
    monkeypatch,
) -> None:
    counts = {"stage": 0, "award": 0, "status": 0}
    original_stage = process_mod._stage_from_signals
    original_award = process_mod._award_signal_from
    original_status = process_mod._resolution_status_from

    def stage(*args, **kwargs):
        counts["stage"] += 1
        return original_stage(*args, **kwargs)

    def award(*args, **kwargs):
        counts["award"] += 1
        return original_award(*args, **kwargs)

    def status(*args, **kwargs):
        counts["status"] += 1
        return original_status(*args, **kwargs)

    monkeypatch.setattr(process_mod, "_stage_from_signals", stage)
    monkeypatch.setattr(process_mod, "_award_signal_from", award)
    monkeypatch.setattr(process_mod, "_resolution_status_from", status)

    signals = _signals(state="Publicado")
    lifecycle = resolve_process_lifecycle(signals)
    resolution = resolve_process_resolution(signals)
    assert counts == {"stage": 1, "award": 1, "status": 1}

    assert dataclasses.replace(lifecycle) == lifecycle
    assert dataclasses.replace(resolution) == resolution
    assert counts == {"stage": 2, "award": 2, "status": 2}

    lifecycle_kwargs = {
        field.name: getattr(lifecycle, field.name)
        for field in dataclasses.fields(lifecycle)
    }
    resolution_kwargs = {
        field.name: getattr(resolution, field.name)
        for field in dataclasses.fields(resolution)
    }
    assert type(lifecycle)(**lifecycle_kwargs) == lifecycle
    assert type(resolution)(**resolution_kwargs) == resolution
    assert counts == {"stage": 3, "award": 3, "status": 3}


def test_process_initvars_are_ephemeral_and_reject_foreign_provenance() -> None:
    lifecycle = resolve_process_lifecycle(_signals(state="Publicado"))
    resolution = resolve_process_resolution(
        _signals(adjudicated="Si", adjudication_id="CO1.AWD.1")
    )
    lifecycle_names = {field.name for field in dataclasses.fields(lifecycle)}
    resolution_names = {field.name for field in dataclasses.fields(resolution)}
    assert "_resolved_stage" not in lifecycle_names
    assert "_resolved_resolution" not in resolution_names
    assert "_resolved_stage" not in lifecycle.__slots__
    assert "_resolved_resolution" not in resolution.__slots__
    assert "_resolved_stage" not in repr(lifecycle)
    assert "_resolved_resolution" not in repr(resolution)

    proof = process_mod._ResolvedLifecycleStage(
        raw_procedure_state=lifecycle.raw_procedure_state,
        raw_phase=lifecycle.raw_phase,
        stage=lifecycle.stage,
        evidence=lifecycle.evidence,
        quality=lifecycle.quality,
    )
    assert dataclasses.replace(lifecycle, _resolved_stage=proof) == lifecycle
    resolution_proof = process_mod._ResolvedProcessResolution(
        raw_adjudicated=resolution.raw_adjudicated,
        award_identity=resolution.award_identity,
        contract_lookup=resolution.contract_lookup,
        award_signal=resolution.award_signal,
        contract_link_signal=resolution.contract_link_signal,
        resolution_status=resolution.resolution_status,
        evidence=resolution.evidence,
        quality=resolution.quality,
    )
    assert (
        dataclasses.replace(
            resolution, _resolved_resolution=resolution_proof
        )
        == resolution
    )

    foreign = dataclasses.replace(
        proof,
        raw_procedure_state=dataclasses.replace(lifecycle.raw_procedure_state),
    )
    with pytest.raises(ValueError, match="different raw signals"):
        dataclasses.replace(lifecycle, _resolved_stage=foreign)
    with pytest.raises(ValueError, match="different evidence or quality"):
        dataclasses.replace(
            lifecycle,
            _resolved_stage=dataclasses.replace(
                proof, quality=dataclasses.replace(lifecycle.quality)
            ),
        )
    with pytest.raises(TypeError, match="internal lifecycle result"):
        dataclasses.replace(lifecycle, _resolved_stage=object())
    with pytest.raises(TypeError, match="internal process result"):
        dataclasses.replace(resolution, _resolved_resolution=object())
    with pytest.raises(ValueError, match="different award_identity"):
        dataclasses.replace(
            resolution,
            _resolved_resolution=dataclasses.replace(
                resolution_proof,
                award_identity=dataclasses.replace(resolution.award_identity),
            ),
        )
    with pytest.raises(ValueError, match="different evidence or quality"):
        dataclasses.replace(
            resolution,
            _resolved_resolution=dataclasses.replace(
                resolution_proof,
                evidence=tuple(list(resolution.evidence)),
            ),
        )
