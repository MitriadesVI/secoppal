"""T1B-1 — Evidence / conflicts / quality tests.

Behavioural unit tests for ``app/core/truth_layer/evidence``.

Goals (binding spec):

* dataclass invariants exercised by real construction and real mutation
  attempts (frozen, slots, hashable, deterministic equality);
* ``SentinelAssessment`` is a mandatory input for an observed scalar —
  Evidence never classifies a sentinel on its own;
* raw preservation: ``normalized_value`` never substitutes ``raw_value``
  and may not diverge from the sentinel comparison form;
* T1A ``missing`` and ``not_observed`` keep their T1A meaning and stay
  mutually distinguishable;
* a source error (timeout / transport / invalid payload) is a third thing:
  it never becomes ``missing`` and never becomes an absence;
* conflicts require at least two distinct pieces of evidence, preserve all
  of them, pick no winner and carry no invented score;
* quality is categorical, deterministic and hashable, preserves conflicts
  and source errors, and asserts nothing about lifecycle;
* purity: no I/O, clock, environment, filesystem, parser, formatter,
  Radar, SECOP client or QueryFrame;
* negative scope: no lifecycle / resolution / contract / temporal /
  visibility / opportunity / lot types are defined here.
"""
from __future__ import annotations

import ast
import dataclasses
import inspect
from copy import deepcopy
from itertools import permutations

import pytest

from app.core.truth_layer import (
    AssessmentConflict,
    Evidence,
    FieldObservation,
    FieldSentinelPolicy,
    ResolutionQuality,
    SentinelAssessment,
    SentinelRule,
    SourceError,
    assess_sentinel,
    build_resolution_quality,
    get_field_sentinel_policy,
    validate_evidence_set,
)
import app.core.truth_layer as truth_layer_pkg
import app.core.truth_layer.evidence as evidence_mod


# ---------------------------------------------------------------------------
# Helpers / fixtures.
# ---------------------------------------------------------------------------


PROCESSES = "p6dx-8zbt"
AWARD_FIELD = "id_adjudicacion"
SUMMARY_FIELD = "estado_resumen"


def _observed(value: object) -> FieldObservation:
    return FieldObservation("observed", value)  # type: ignore[arg-type]


def _cataloged_evidence(
    raw: str,
    *,
    field: str = AWARD_FIELD,
    code: str = "AWARD_ID_OBSERVED",
    role: str = "supports",
    carry_normalized: bool = True,
) -> Evidence:
    """Build production-cataloged evidence through the real sentinel layer."""
    observation = _observed(raw)
    policy = get_field_sentinel_policy(PROCESSES, field)
    assessment = assess_sentinel(observation, policy)
    return Evidence(
        code=code,
        dataset=PROCESSES,
        field=field,
        raw_observation=observation,
        sentinel_assessment=assessment,
        normalized_value=(
            assessment.normalized_comparison_value if carry_normalized else None
        ),
        role=role,  # type: ignore[arg-type]
    )


def _missing_evidence(
    *, code: str = "AWARD_ID_MISSING", field: str = AWARD_FIELD
) -> Evidence:
    return Evidence(
        code=code,
        dataset=PROCESSES,
        field=field,
        raw_observation=FieldObservation("missing", None),
        sentinel_assessment=None,
        normalized_value=None,
        role="missing",
    )


def _not_observed_evidence(
    *,
    code: str = "AWARD_ID_NOT_OBSERVED",
    field: str = AWARD_FIELD,
    source_error: SourceError | None = None,
) -> Evidence:
    return Evidence(
        code=code,
        dataset=PROCESSES,
        field=field,
        raw_observation=FieldObservation("not_observed", None),
        sentinel_assessment=None,
        normalized_value=None,
        role="not_observed",
        source_error=source_error,
    )


def _timeout_error(code: str = "CONTRACT_LOOKUP_TIMEOUT") -> SourceError:
    return SourceError(code=code, dataset=PROCESSES, kind="timeout")


def _canonical(*evidences: Evidence) -> tuple[Evidence, ...]:
    return tuple(sorted(evidences, key=evidence_mod._evidence_sort_key))


def _two_way_conflict(
    *, code: str = "AWARD_VS_CONTRACT", severity: str = "material"
) -> AssessmentConflict:
    return AssessmentConflict(
        code=code,
        evidences=_canonical(
            _cataloged_evidence("CO1.AWD.123"),
            _cataloged_evidence(
                "No Adjudicado", code="AWARD_ID_ABSENT", role="contradicts"
            ),
        ),
        consequence="AWARD_IDENTITY_UNDECIDED",
        severity=severity,  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# 1. Evidence — structure.
# ---------------------------------------------------------------------------


def test_evidence_is_frozen() -> None:
    evidence = _cataloged_evidence("CO1.AWD.123")
    assert dataclasses.is_dataclass(evidence)
    assert evidence.__dataclass_params__.frozen  # type: ignore[attr-defined]
    with pytest.raises(dataclasses.FrozenInstanceError):
        evidence.code = "OTHER"  # type: ignore[misc]


def test_evidence_uses_slots() -> None:
    evidence = _cataloged_evidence("CO1.AWD.123")
    assert "__dict__" not in dir(evidence)
    with pytest.raises(AttributeError):
        object.__setattr__(evidence, "smuggled_attribute", 1)


def test_evidence_is_hashable_and_deterministic() -> None:
    a = _cataloged_evidence("CO1.AWD.123")
    b = _cataloged_evidence("CO1.AWD.123")
    assert a == b
    assert hash(a) == hash(b)
    assert len({a, b}) == 1


def test_source_error_and_conflict_and_quality_are_frozen_slotted_hashable() -> None:
    error = _timeout_error()
    conflict = _two_way_conflict()
    quality = ResolutionQuality(
        completeness="partial",
        consistency="conflicting",
        confidence="low",
        conflicts=(conflict,),
    )
    for instance in (error, conflict, quality):
        assert dataclasses.is_dataclass(instance)
        assert instance.__dataclass_params__.frozen  # type: ignore[attr-defined]
        assert "__dict__" not in dir(instance)
        assert isinstance(hash(instance), int)
    with pytest.raises(dataclasses.FrozenInstanceError):
        error.kind = "transport_error"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        conflict.severity = "critical"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        quality.confidence = "high"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 2. Evidence — raw and sentinel preservation.
# ---------------------------------------------------------------------------


def test_evidence_preserves_raw_observation_object() -> None:
    observation = _observed("  No Adjudicado  ")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    assessment = assess_sentinel(observation, policy)
    evidence = Evidence(
        code="AWARD_ID_ABSENT",
        dataset=PROCESSES,
        field=AWARD_FIELD,
        raw_observation=observation,
        sentinel_assessment=assessment,
        normalized_value=assessment.normalized_comparison_value,
        role="supports",
    )
    assert evidence.raw_observation is observation
    assert evidence.raw_observation.raw_value == "  No Adjudicado  "
    # The normalized comparison form differs and does NOT replace the raw.
    assert evidence.normalized_value == "no adjudicado"
    assert evidence.normalized_value != evidence.raw_observation.raw_value


def test_evidence_preserves_sentinel_assessment() -> None:
    evidence = _cataloged_evidence("No Adjudicado", code="AWARD_ID_ABSENT")
    assert evidence.sentinel_assessment is not None
    assert evidence.sentinel_assessment.status == "sentinel_absence"
    assert evidence.sentinel_assessment.sentinel_code == "PROCESS_AWARD_ID_NO_AWARD"


def test_evidence_construction_does_not_mutate_inputs() -> None:
    observation = _observed("CO1.AWD.123")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    assessment = assess_sentinel(observation, policy)
    before_observation = deepcopy(observation)
    before_assessment = deepcopy(assessment)
    Evidence(
        code="AWARD_ID_OBSERVED",
        dataset=PROCESSES,
        field=AWARD_FIELD,
        raw_observation=observation,
        sentinel_assessment=assessment,
        normalized_value=assessment.normalized_comparison_value,
        role="supports",
    )
    assert observation == before_observation
    assert assessment == before_assessment


def test_evidence_may_omit_normalized_value() -> None:
    evidence = _cataloged_evidence("CO1.AWD.123", carry_normalized=False)
    assert evidence.normalized_value is None
    assert evidence.raw_observation.raw_value == "CO1.AWD.123"


def test_evidence_cannot_invent_its_own_normalization() -> None:
    observation = _observed("CO1.AWD.123")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    assessment = assess_sentinel(observation, policy)
    with pytest.raises(ValueError):
        Evidence(
            code="AWARD_ID_OBSERVED",
            dataset=PROCESSES,
            field=AWARD_FIELD,
            raw_observation=observation,
            sentinel_assessment=assessment,
            normalized_value="CO1AWD123",  # not the sentinel comparison form
            role="supports",
        )


# ---------------------------------------------------------------------------
# 3. Evidence — validation.
# ---------------------------------------------------------------------------


def test_evidence_rejects_empty_code() -> None:
    observation = _observed("CO1.AWD.123")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    assessment = assess_sentinel(observation, policy)
    with pytest.raises(ValueError):
        Evidence(
            code="",
            dataset=PROCESSES,
            field=AWARD_FIELD,
            raw_observation=observation,
            sentinel_assessment=assessment,
            normalized_value=None,
            role="supports",
        )


def test_evidence_rejects_product_prose_as_code() -> None:
    # Codes must stay machine-readable; product-visible text is forbidden.
    with pytest.raises(ValueError):
        _cataloged_evidence("CO1.AWD.123", code="No se adjudicó el proceso")


def test_evidence_rejects_dataset_mismatch_with_sentinel() -> None:
    observation = _observed("CO1.AWD.123")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    assessment = assess_sentinel(observation, policy)
    with pytest.raises(ValueError):
        Evidence(
            code="AWARD_ID_OBSERVED",
            dataset="jbjy-vk9h",  # contracts dataset, not the assessed one
            field=AWARD_FIELD,
            raw_observation=observation,
            sentinel_assessment=assessment,
            normalized_value=None,
            role="supports",
        )


def test_evidence_rejects_field_mismatch_with_sentinel() -> None:
    observation = _observed("CO1.AWD.123")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    assessment = assess_sentinel(observation, policy)
    with pytest.raises(ValueError):
        Evidence(
            code="AWARD_ID_OBSERVED",
            dataset=PROCESSES,
            field=SUMMARY_FIELD,
            raw_observation=observation,
            sentinel_assessment=assessment,
            normalized_value=None,
            role="supports",
        )


def test_evidence_rejects_observation_mismatch_with_sentinel() -> None:
    assessed = _observed("CO1.AWD.123")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    assessment = assess_sentinel(assessed, policy)
    with pytest.raises(ValueError):
        Evidence(
            code="AWARD_ID_OBSERVED",
            dataset=PROCESSES,
            field=AWARD_FIELD,
            raw_observation=_observed("CO1.AWD.999"),  # a different observation
            sentinel_assessment=assessment,
            normalized_value=None,
            role="supports",
        )


def test_observed_evidence_without_a_sentinel_is_valid() -> None:
    """Most fields have no authorized policy; they still produce evidence.

    ``get_field_sentinel_policy`` returns ``None`` for them, and a ``None``
    policy is explicitly NOT a synonym of "meaningful". Requiring an
    assessment here would make the whole layer unusable for every
    uncataloged field.
    """
    assert get_field_sentinel_policy(PROCESSES, "estado_del_procedimiento") is None
    evidence = Evidence(
        code="PROCESS_STATE_OBSERVED",
        dataset=PROCESSES,
        field="estado_del_procedimiento",
        raw_observation=_observed("Evaluación"),
        sentinel_assessment=None,
        normalized_value=None,
        role="supports",
    )
    assert evidence.sentinel_assessment is None
    assert evidence.raw_observation.raw_value == "Evaluación"
    assert evidence.raw_observation.presence == "observed"


@pytest.mark.parametrize(
    "dataset,field,raw",
    [
        ("p6dx-8zbt", "estado_del_procedimiento", "Evaluación"),
        ("jbjy-vk9h", "estado_contrato", "En ejecución"),
        ("jbjy-vk9h", "fecha_de_firma", "2025-01-01"),
    ],
)
def test_uncataloged_process_and_contract_fields_produce_evidence(
    dataset: str, field: str, raw: str
) -> None:
    assert get_field_sentinel_policy(dataset, field) is None
    evidence = Evidence(
        code="FIELD_OBSERVED",
        dataset=dataset,
        field=field,
        raw_observation=_observed(raw),
        sentinel_assessment=None,
        normalized_value=None,
        role="supports",
    )
    assert evidence.raw_observation.raw_value == raw
    assert evidence.sentinel_assessment is None
    assert evidence.normalized_value is None


def test_observed_evidence_without_sentinel_cannot_carry_normalized_value() -> None:
    # No assessment means no authorized normalization: Evidence may not
    # normalize on its own, and "no sentinel" never means "meaningful".
    with pytest.raises(ValueError):
        Evidence(
            code="PROCESS_STATE_OBSERVED",
            dataset=PROCESSES,
            field="estado_del_procedimiento",
            raw_observation=_observed("Evaluación"),
            sentinel_assessment=None,
            normalized_value="evaluación",
            role="supports",
        )


def test_evidence_requires_the_very_observation_the_sentinel_assessed() -> None:
    """Provenance is identity, not structural equality.

    Two equal ``FieldObservation`` values may come from different rows; an
    equal copy is a different piece of provenance and must not pass.
    """
    assessed = _observed("CO1.AWD.123")
    twin = _observed("CO1.AWD.123")
    assert assessed == twin and assessed is not twin
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    assessment = assess_sentinel(assessed, policy)

    # Same object: valid.
    accepted = Evidence(
        code="AWARD_ID_OBSERVED",
        dataset=PROCESSES,
        field=AWARD_FIELD,
        raw_observation=assessed,
        sentinel_assessment=assessment,
        normalized_value=None,
        role="supports",
    )
    assert accepted.raw_observation is assessed

    # Equal copy: rejected.
    with pytest.raises(ValueError):
        Evidence(
            code="AWARD_ID_OBSERVED",
            dataset=PROCESSES,
            field=AWARD_FIELD,
            raw_observation=twin,
            sentinel_assessment=assessment,
            normalized_value=None,
            role="supports",
        )


def test_non_sentinel_assessment_object_is_rejected() -> None:
    with pytest.raises(TypeError):
        Evidence(
            code="AWARD_ID_OBSERVED",
            dataset=PROCESSES,
            field=AWARD_FIELD,
            raw_observation=_observed("CO1.AWD.123"),
            sentinel_assessment={"status": "meaningful"},  # type: ignore[arg-type]
            normalized_value=None,
            role="supports",
        )


def test_missing_and_not_observed_are_valid_without_a_sentinel() -> None:
    assert _missing_evidence().sentinel_assessment is None
    assert _not_observed_evidence().sentinel_assessment is None


def test_missing_and_not_observed_cannot_carry_a_sentinel() -> None:
    observation = _observed("CO1.AWD.123")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    assessment = assess_sentinel(observation, policy)
    for presence, role in (("missing", "missing"), ("not_observed", "not_observed")):
        with pytest.raises(ValueError):
            Evidence(
                code="AWARD_ID_ABSENT_FIELD",
                dataset=PROCESSES,
                field=AWARD_FIELD,
                raw_observation=FieldObservation(presence, None),  # type: ignore[arg-type]
                sentinel_assessment=assessment,
                normalized_value=None,
                role=role,  # type: ignore[arg-type]
            )


def test_evidence_rejects_role_presence_mismatch() -> None:
    # "supports" claims an observed scalar; the observation has none.
    with pytest.raises(ValueError):
        Evidence(
            code="AWARD_ID_OBSERVED",
            dataset=PROCESSES,
            field=AWARD_FIELD,
            raw_observation=FieldObservation("missing", None),
            sentinel_assessment=None,
            normalized_value=None,
            role="supports",
        )
    # "missing" claims no scalar; the observation has one.
    observation = _observed("CO1.AWD.123")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    with pytest.raises(ValueError):
        Evidence(
            code="AWARD_ID_MISSING",
            dataset=PROCESSES,
            field=AWARD_FIELD,
            raw_observation=observation,
            sentinel_assessment=assess_sentinel(observation, policy),
            normalized_value=None,
            role="missing",
        )


def test_absent_evidence_cannot_carry_a_sentinel_or_normalized_value() -> None:
    with pytest.raises(ValueError):
        Evidence(
            code="AWARD_ID_MISSING",
            dataset=PROCESSES,
            field=AWARD_FIELD,
            raw_observation=FieldObservation("missing", None),
            sentinel_assessment=None,
            normalized_value="anything",
            role="missing",
        )


def test_evidence_rejects_invalid_role() -> None:
    with pytest.raises(ValueError):
        _cataloged_evidence("CO1.AWD.123", role="proves")


# ---------------------------------------------------------------------------
# 3b. normalized_value is compared type-strictly against the sentinel form.
# ---------------------------------------------------------------------------


def _exact_scalar_evidence(raw: object, normalized: object) -> Evidence:
    """Evidence over an `exact`-mode policy, where normalized == raw verbatim."""
    policy = FieldSentinelPolicy(
        dataset_id="synthetic_dataset",
        field_name="scalar_field",
        comparison_mode="exact",
        unknown_value_disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="UNRELATED_TOKEN", status="sentinel_absence", aliases=("zzz",)
            ),
        ),
    )
    observation = _observed(raw)
    assessment = assess_sentinel(observation, policy)
    return Evidence(
        code="SCALAR_OBSERVED",
        dataset="synthetic_dataset",
        field="scalar_field",
        raw_observation=observation,
        sentinel_assessment=assessment,
        normalized_value=normalized,  # type: ignore[arg-type]
        role="supports",
    )


@pytest.mark.parametrize(
    "raw,normalized",
    [
        (1, True),  # True == 1 in Python, but bool is not int here
        (True, 1),
        (0, 0.0),  # 0 == 0.0 in Python, but int is not float here
        (0.0, 0),
        (0, "0"),  # never a string/number coercion
    ],
)
def test_normalized_value_comparison_is_type_strict(
    raw: object, normalized: object
) -> None:
    with pytest.raises(ValueError):
        _exact_scalar_evidence(raw, normalized)


@pytest.mark.parametrize("value", [1, True, 0, 0.0, "0", 2.5, "texto"])
def test_normalized_value_accepts_the_exact_sentinel_form(value: object) -> None:
    evidence = _exact_scalar_evidence(value, value)
    assert evidence.sentinel_assessment is not None
    assert type(evidence.normalized_value) is type(
        evidence.sentinel_assessment.normalized_comparison_value
    )
    assert evidence.raw_observation.raw_value == value


# ---------------------------------------------------------------------------
# 4. missing vs not_observed vs source error — three distinct things.
# ---------------------------------------------------------------------------


def test_missing_and_not_observed_stay_distinguishable() -> None:
    missing = _missing_evidence(code="AWARD_ID_ABSENT_FIELD")
    not_observed = _not_observed_evidence(code="AWARD_ID_ABSENT_FIELD")
    assert missing != not_observed
    assert missing.role == "missing"
    assert not_observed.role == "not_observed"
    assert missing.raw_observation.presence == "missing"
    assert not_observed.raw_observation.presence == "not_observed"


def test_source_error_never_becomes_missing() -> None:
    with pytest.raises(ValueError):
        Evidence(
            code="AWARD_ID_ABSENT_FIELD",
            dataset=PROCESSES,
            field=AWARD_FIELD,
            raw_observation=FieldObservation("missing", None),
            sentinel_assessment=None,
            normalized_value=None,
            role="missing",
            source_error=_timeout_error(),
        )


def test_source_error_never_qualifies_an_observed_value() -> None:
    observation = _observed("CO1.AWD.123")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    with pytest.raises(ValueError):
        Evidence(
            code="AWARD_ID_OBSERVED",
            dataset=PROCESSES,
            field=AWARD_FIELD,
            raw_observation=observation,
            sentinel_assessment=assess_sentinel(observation, policy),
            normalized_value=None,
            role="supports",
            source_error=_timeout_error(),
        )


def test_timeout_is_not_observed_plus_source_error_not_an_absence() -> None:
    timed_out = _not_observed_evidence(source_error=_timeout_error())
    not_queried = _not_observed_evidence()
    # A timeout is a source failure; "not queried" is plain T1A not_observed.
    assert timed_out.source_error is not None
    assert timed_out.source_error.kind == "timeout"
    assert not_queried.source_error is None
    assert timed_out != not_queried
    # Neither one is the structural absence of the field.
    assert timed_out.role != "missing"
    assert not_queried.role != "missing"


def test_source_error_kinds_stay_distinct() -> None:
    kinds = {
        SourceError(code="E", dataset=PROCESSES, kind=kind).kind
        for kind in ("timeout", "transport_error", "invalid_payload")
    }
    assert kinds == {"timeout", "transport_error", "invalid_payload"}
    with pytest.raises(ValueError):
        SourceError(code="E", dataset=PROCESSES, kind="not_queried")  # type: ignore[arg-type]


def test_source_error_rejects_empty_code_and_dataset() -> None:
    with pytest.raises(ValueError):
        SourceError(code="", dataset=PROCESSES, kind="timeout")
    with pytest.raises(ValueError):
        SourceError(code="E", dataset="", kind="timeout")


def test_source_error_dataset_must_match_its_evidence() -> None:
    with pytest.raises(ValueError):
        _not_observed_evidence(
            source_error=SourceError(
                code="LOOKUP_TIMEOUT", dataset="jbjy-vk9h", kind="timeout"
            )
        )


# ---------------------------------------------------------------------------
# 5. validate_evidence_set.
# ---------------------------------------------------------------------------


def test_validate_evidence_set_returns_input_unchanged() -> None:
    evidences = _canonical(
        _cataloged_evidence("CO1.AWD.123"),
        _cataloged_evidence("CO1.AWD.999", code="AWARD_ID_OBSERVED_ALT"),
    )
    assert validate_evidence_set(evidences) is evidences


def test_validate_evidence_set_rejects_duplicates_and_bad_order() -> None:
    first, second = _canonical(
        _cataloged_evidence("CO1.AWD.123"),
        _cataloged_evidence("CO1.AWD.999", code="AWARD_ID_OBSERVED_ALT"),
    )
    with pytest.raises(ValueError):
        validate_evidence_set((first, first))
    with pytest.raises(ValueError):
        validate_evidence_set((second, first))
    with pytest.raises(TypeError):
        validate_evidence_set([first, second])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        validate_evidence_set((first, "not evidence"))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 6. AssessmentConflict.
# ---------------------------------------------------------------------------


def test_conflict_requires_at_least_two_pieces_of_evidence() -> None:
    single = _cataloged_evidence("CO1.AWD.123")
    with pytest.raises(ValueError):
        AssessmentConflict(
            code="AWARD_VS_CONTRACT",
            evidences=(single,),
            consequence="AWARD_IDENTITY_UNDECIDED",
            severity="material",
        )
    with pytest.raises(ValueError):
        AssessmentConflict(
            code="AWARD_VS_CONTRACT",
            evidences=(),
            consequence="AWARD_IDENTITY_UNDECIDED",
            severity="material",
        )


def test_conflict_rejects_the_same_evidence_twice() -> None:
    duplicated = _cataloged_evidence("CO1.AWD.123")
    with pytest.raises(ValueError):
        AssessmentConflict(
            code="AWARD_VS_CONTRACT",
            evidences=(duplicated, duplicated),
            consequence="AWARD_IDENTITY_UNDECIDED",
            severity="material",
        )


def test_conflict_preserves_every_piece_of_evidence() -> None:
    conflict = _two_way_conflict()
    raws = {e.raw_observation.raw_value for e in conflict.evidences}
    assert raws == {"CO1.AWD.123", "No Adjudicado"}
    assert len(conflict.evidences) == 2
    assert conflict.raw_values == tuple(
        e.raw_observation.raw_value for e in conflict.evidences
    )
    assert conflict.fields == (AWARD_FIELD, AWARD_FIELD)


def test_conflict_picks_no_winner() -> None:
    conflict = _two_way_conflict()
    field_names = {f.name for f in dataclasses.fields(conflict)}
    assert not field_names & {
        "winner",
        "chosen",
        "resolved_value",
        "resolution",
        "preferred_evidence",
    }
    # Both roles survive; neither is dropped nor rewritten.
    assert {e.role for e in conflict.evidences} == {"supports", "contradicts"}


def test_conflict_is_deterministic() -> None:
    a = _two_way_conflict()
    b = _two_way_conflict()
    assert a == b
    assert hash(a) == hash(b)
    assert a.evidences == b.evidences


def test_conflict_requires_canonical_evidence_order() -> None:
    ordered = _canonical(
        _cataloged_evidence("CO1.AWD.123"),
        _cataloged_evidence(
            "No Adjudicado", code="AWARD_ID_ABSENT", role="contradicts"
        ),
    )
    with pytest.raises(ValueError):
        AssessmentConflict(
            code="AWARD_VS_CONTRACT",
            evidences=tuple(reversed(ordered)),
            consequence="AWARD_IDENTITY_UNDECIDED",
            severity="material",
        )


def test_conflict_rejects_empty_or_prose_codes() -> None:
    ordered = _canonical(
        _cataloged_evidence("CO1.AWD.123"),
        _cataloged_evidence(
            "No Adjudicado", code="AWARD_ID_ABSENT", role="contradicts"
        ),
    )
    with pytest.raises(ValueError):
        AssessmentConflict(
            code="",
            evidences=ordered,
            consequence="AWARD_IDENTITY_UNDECIDED",
            severity="material",
        )
    # `consequence` is a stable code, never product-visible prose.
    with pytest.raises(ValueError):
        AssessmentConflict(
            code="AWARD_VS_CONTRACT",
            evidences=ordered,
            consequence="El proceso quedó sin adjudicación clara",
            severity="material",
        )


def test_conflict_rejects_invalid_severity() -> None:
    with pytest.raises(ValueError):
        _two_way_conflict(severity="blocking")


def test_conflict_severities_are_categorical_not_numeric() -> None:
    for severity in ("critical", "material", "informational"):
        assert _two_way_conflict(severity=severity).severity == severity
    with pytest.raises(ValueError):
        _two_way_conflict(severity=0.9)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 7. ResolutionQuality.
# ---------------------------------------------------------------------------


def test_quality_accepts_valid_axes() -> None:
    quality = ResolutionQuality(
        completeness="complete",
        consistency="consistent",
        confidence="high",
    )
    assert quality.completeness == "complete"
    assert quality.consistency == "consistent"
    assert quality.confidence == "high"
    assert quality.conflicts == ()
    assert quality.source_errors == ()


@pytest.mark.parametrize(
    "axis,bad_value",
    [
        ("completeness", "mostly"),
        ("consistency", "fine"),
        ("confidence", "medium-high"),
    ],
)
def test_quality_rejects_invalid_axis_values(axis: str, bad_value: str) -> None:
    kwargs = {
        "completeness": "complete",
        "consistency": "consistent",
        "confidence": "high",
    }
    kwargs[axis] = bad_value
    with pytest.raises(ValueError):
        ResolutionQuality(**kwargs)  # type: ignore[arg-type]


def test_quality_rejects_invented_percentages() -> None:
    # No 0-1 score is authorized anywhere on the quality contract.
    with pytest.raises(ValueError):
        ResolutionQuality(
            completeness="complete",
            consistency="consistent",
            confidence=0.87,  # type: ignore[arg-type]
        )
    assert not {f.name for f in dataclasses.fields(ResolutionQuality)} & {
        "score",
        "confidence_score",
        "percentage",
        "ratio",
    }


def test_quality_rejects_duplicate_and_unordered_reason_codes() -> None:
    with pytest.raises(ValueError):
        ResolutionQuality(
            completeness="partial",
            consistency="unknown",
            confidence="low",
            reason_codes=("A_CODE", "A_CODE"),
        )
    with pytest.raises(ValueError):
        ResolutionQuality(
            completeness="partial",
            consistency="unknown",
            confidence="low",
            reason_codes=("B_CODE", "A_CODE"),
        )


def test_quality_rejects_prose_reason_codes() -> None:
    with pytest.raises(ValueError):
        ResolutionQuality(
            completeness="partial",
            consistency="unknown",
            confidence="low",
            reason_codes=("falta la fecha de firma",),
        )


def test_quality_missing_fields_are_deterministic() -> None:
    quality = ResolutionQuality(
        completeness="partial",
        consistency="unknown",
        confidence="low",
        missing_fields=("estado_resumen", "id_adjudicacion"),
    )
    assert quality.missing_fields == ("estado_resumen", "id_adjudicacion")
    with pytest.raises(ValueError):
        ResolutionQuality(
            completeness="partial",
            consistency="unknown",
            confidence="low",
            missing_fields=("id_adjudicacion", "estado_resumen"),
        )
    with pytest.raises(ValueError):
        ResolutionQuality(
            completeness="partial",
            consistency="unknown",
            confidence="low",
            missing_fields=("id_adjudicacion", "id_adjudicacion"),
        )


def test_quality_preserves_source_errors_with_their_kind() -> None:
    errors = (
        SourceError(code="LOOKUP_TIMEOUT", dataset=PROCESSES, kind="timeout"),
        SourceError(
            code="BAD_BODY", dataset=PROCESSES, kind="invalid_payload"
        ),
    )
    quality = build_resolution_quality(
        completeness="insufficient",
        consistency="unknown",
        confidence="low",
        source_errors=errors,
    )
    assert set(quality.source_errors) == set(errors)
    assert {e.kind for e in quality.source_errors} == {
        "timeout",
        "invalid_payload",
    }
    # A source error is not a missing field.
    assert quality.missing_fields == ()


def test_quality_preserves_conflicts_and_forbids_silent_resolution() -> None:
    conflict = _two_way_conflict()
    quality = ResolutionQuality(
        completeness="partial",
        consistency="conflicting",
        confidence="low",
        conflicts=(conflict,),
    )
    assert quality.conflicts == (conflict,)
    assert quality.conflicts[0].evidences == conflict.evidences
    with pytest.raises(ValueError):
        ResolutionQuality(
            completeness="complete",
            consistency="consistent",  # cannot claim consistency with conflicts
            confidence="high",
            conflicts=(conflict,),
        )


def test_quality_rejects_duplicate_conflicts() -> None:
    conflict = _two_way_conflict()
    with pytest.raises(ValueError):
        ResolutionQuality(
            completeness="partial",
            consistency="conflicting",
            confidence="low",
            conflicts=(conflict, conflict),
        )


def test_critical_conflict_can_coexist_with_a_partial_conclusion() -> None:
    critical = _two_way_conflict(severity="critical")
    quality = ResolutionQuality(
        completeness="partial",
        consistency="conflicting",
        confidence="low",
        conflicts=(critical,),
    )
    assert quality.conflicts[0].severity == "critical"
    assert quality.completeness == "partial"


def test_quality_capabilities_do_not_become_per_row_reason_codes() -> None:
    quality = build_resolution_quality(
        completeness="partial",
        consistency="unknown",
        confidence="medium",
        capabilities=(("process_state_id_mapping", "unavailable"),),
    )
    assert quality.capabilities == (
        ("process_state_id_mapping", "unavailable"),
    )
    assert quality.reason_codes == ()


def test_quality_rejects_repeated_or_unordered_capability_keys() -> None:
    with pytest.raises(ValueError):
        ResolutionQuality(
            completeness="partial",
            consistency="unknown",
            confidence="low",
            capabilities=(("a_key", "x"), ("a_key", "y")),
        )
    with pytest.raises(ValueError):
        ResolutionQuality(
            completeness="partial",
            consistency="unknown",
            confidence="low",
            capabilities=(("b_key", "x"), ("a_key", "y")),
        )
    with pytest.raises(TypeError):
        ResolutionQuality(
            completeness="partial",
            consistency="unknown",
            confidence="low",
            capabilities=(("a_key",),),  # type: ignore[arg-type]
        )


def test_quality_is_hashable_and_deterministic() -> None:
    def build() -> ResolutionQuality:
        return build_resolution_quality(
            completeness="partial",
            consistency="conflicting",
            confidence="low",
            reason_codes=("B_CODE", "A_CODE", "B_CODE"),
            missing_fields=("id_adjudicacion", "estado_resumen"),
            source_errors=(_timeout_error(), _timeout_error()),
            conflicts=(_two_way_conflict(),),
            capabilities=(("process_state_id_mapping", "unavailable"),),
        )

    first, second = build(), build()
    assert first == second
    assert hash(first) == hash(second)
    assert first.reason_codes == ("A_CODE", "B_CODE")
    assert first.missing_fields == ("estado_resumen", "id_adjudicacion")
    assert len(first.source_errors) == 1


def test_build_resolution_quality_does_not_derive_the_axes() -> None:
    # The builder normalizes ordering only; it never infers quality itself.
    quality = build_resolution_quality(
        completeness="complete",
        consistency="unknown",
        confidence="high",
    )
    assert (quality.completeness, quality.consistency, quality.confidence) == (
        "complete",
        "unknown",
        "high",
    )


def test_build_resolution_quality_does_not_merge_conflicting_capabilities() -> None:
    with pytest.raises(ValueError):
        build_resolution_quality(
            completeness="partial",
            consistency="unknown",
            confidence="low",
            capabilities=(("k", "unavailable"), ("k", "available")),
        )


# ---------------------------------------------------------------------------
# 8. Composition with the T1B-0A sentinel layer (production catalog).
# ---------------------------------------------------------------------------


def test_no_adjudicado_becomes_semantic_absence_evidence_keeping_raw() -> None:
    evidence = _cataloged_evidence("No Adjudicado", code="AWARD_ID_ABSENT")
    assert evidence.sentinel_assessment is not None
    assert evidence.sentinel_assessment.status == "sentinel_absence"
    # Semantic absence, NOT the structural T1A absence.
    assert evidence.role == "supports"
    assert evidence.raw_observation.presence == "observed"
    assert evidence.raw_observation.raw_value == "No Adjudicado"


def test_no_definido_stays_unknown_and_is_not_turned_into_absence() -> None:
    evidence = _cataloged_evidence(
        "No Definido", field=SUMMARY_FIELD, code="SUMMARY_STATE_UNDEFINED"
    )
    assert evidence.sentinel_assessment is not None
    assert evidence.sentinel_assessment.status == "sentinel_unknown"
    assert evidence.sentinel_assessment.status != "sentinel_absence"
    assert evidence.raw_observation.raw_value == "No Definido"
    assert evidence.role != "missing"


def test_genuine_award_id_keeps_its_raw_identity() -> None:
    evidence = _cataloged_evidence("CO1.AWD.123")
    assert evidence.sentinel_assessment is not None
    assert evidence.sentinel_assessment.status == "meaningful"
    assert evidence.raw_observation.raw_value == "CO1.AWD.123"
    # The comparison form is casefolded; the identity is not.
    assert evidence.normalized_value == "co1.awd.123"
    assert evidence.raw_observation.raw_value != evidence.normalized_value


def test_semantic_absence_and_structural_absence_are_different_evidence() -> None:
    semantic = _cataloged_evidence("No Adjudicado", code="AWARD_ID_ABSENT")
    structural = _missing_evidence(code="AWARD_ID_ABSENT")
    assert semantic != structural
    assert semantic.sentinel_assessment is not None
    assert structural.sentinel_assessment is None


# ---------------------------------------------------------------------------
# 9. Exports.
# ---------------------------------------------------------------------------


# The package ``__all__`` must stay open: T1B-2 .. T1B-7 will each add API.
# What is frozen is the module's own closed surface plus the guarantee that
# nothing previously published is dropped or rebound.

_T1B_1_EXPORTS = {
    "EvidenceRole",
    "SourceErrorKind",
    "ConflictSeverity",
    "ResolutionCompleteness",
    "ResolutionConsistency",
    "ResolutionConfidence",
    "SourceError",
    "Evidence",
    "AssessmentConflict",
    "ResolutionQuality",
    "validate_evidence_set",
    "build_resolution_quality",
}


def test_evidence_module_keeps_exactly_its_twelve_public_exports() -> None:
    assert set(evidence_mod.__all__) == _T1B_1_EXPORTS
    assert len(evidence_mod.__all__) == 12
    assert len(set(evidence_mod.__all__)) == 12


def test_t1b_1_api_is_published_by_the_package() -> None:
    published = set(truth_layer_pkg.__all__)
    assert _T1B_1_EXPORTS <= published
    for name in _T1B_1_EXPORTS:
        assert getattr(truth_layer_pkg, name) is getattr(evidence_mod, name)


def test_earlier_block_exports_preserved() -> None:
    preserved = {
        "ContractLifecycleSignals",
        "FieldObservation",
        "ObservationPresence",
        "ProcessContractReference",
        "ProcessLifecycleSignals",
        "PublicationVisibilitySignals",
        "RawScalar",
        "TemporalQueryContext",
        "SentinelStatus",
        "UnknownValueDisposition",
        "ComparisonMode",
        "SentinelRule",
        "FieldSentinelPolicy",
        "SentinelAssessment",
        "assess_sentinel",
        "get_field_sentinel_policy",
    }
    assert preserved.issubset(set(truth_layer_pkg.__all__))
    assert len(preserved) == 16


def test_package_all_has_no_duplicates() -> None:
    names = list(truth_layer_pkg.__all__)
    assert len(names) == len(set(names))


def test_private_helpers_are_not_exported() -> None:
    for private in (
        "_evidence_sort_key",
        "_conflict_sort_key",
        "_source_error_sort_key",
        "_require_stable_code",
        "_raw_marker",
    ):
        assert private not in truth_layer_pkg.__all__
        assert private not in evidence_mod.__all__


# ---------------------------------------------------------------------------
# 10. Purity and negative scope.
# ---------------------------------------------------------------------------


def test_module_imports_pure() -> None:
    tree = ast.parse(inspect.getsource(evidence_mod))
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
        "parser",
        "formatter",
        "radar",
        "soql_builder",
        "orchestrator",
        "query_router",
        "entity_resolver",
        "secop",
        "socrata",
    }
    assert not (imported & forbidden), f"forbidden imports: {imported & forbidden}"
    assert imported <= {"app", "dataclasses", "typing", "__future__"}


def test_module_has_no_clock_uuid_or_environment_access() -> None:
    source = inspect.getsource(evidence_mod)
    for forbidden in (
        "datetime.now",
        "time.time",
        "time.monotonic",
        "uuid4",
        "os.environ",
        "open(",
        "random.",
    ):
        assert forbidden not in source, f"forbidden runtime access: {forbidden}"


def test_module_defines_no_downstream_domain_types() -> None:
    tree = ast.parse(inspect.getsource(evidence_mod))
    defined = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
    }
    forbidden = {
        "ProcessLifecycleAssessment",
        "ProcessResolutionAssessment",
        "ContractPerformanceAssessment",
        "ContractSignatureAssessment",
        "ContractChangeAssessment",
        "ContractLiquidationAssessment",
        "ContractLifecycleAssessment",
        "ChangeAxisAssessment",
        "TemporalAssessment",
        "PublicationVisibilityAssessment",
        "OpportunityRoleAssessment",
        "AwardIdentityExtraction",
        "LotStructureEvidence",
        "LotIdentityEvidence",
        "ContractLotLinkageEvidence",
    }
    assert not (defined & forbidden), f"out-of-scope types defined: {defined & forbidden}"


def test_module_carries_no_domain_vocabulary_in_code() -> None:
    """`evidence.py` must hold no domain rule and no domain literal.

    Docstrings may cite the binding contract; executable code may not name a
    lifecycle stage, a resolution signal or a SECOP state literal.
    """
    tree = ast.parse(inspect.getsource(evidence_mod))
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef)):
            # Drop the docstring node so prose is not scanned as code.
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                node.body = body[1:]
    code_strings = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    forbidden_vocabulary = {
        "bid_ready",
        "offer_phase_observed",
        "selected_or_awarded",
        "absent_after_completed_lookup",
        "Prorrogado",
        "En ejecución",
        "No Adjudicado",
        "No Definido",
        "Cerrado",
        "id_adjudicacion",
        "estado_resumen",
        "numero_de_lotes",
        "p6dx-8zbt",
    }
    leaked = {s for s in code_strings if s in forbidden_vocabulary}
    assert not leaked, f"domain vocabulary leaked into evidence.py: {leaked}"


def test_no_lot_types_or_policies_present() -> None:
    source = inspect.getsource(evidence_mod)
    for forbidden in ("Lot", "lote", "numero_de_lotes"):
        assert forbidden not in source, f"lot scope leaked: {forbidden}"


def test_explicit_evidence_key_preserves_legacy_order_for_distinct_evidence() -> None:
    observed = _observed("No Adjudicado")
    policy = get_field_sentinel_policy(PROCESSES, AWARD_FIELD)
    sentinel = assess_sentinel(observed, policy)
    shared = {
        "code": "AWARD_ID_OBSERVED",
        "dataset": PROCESSES,
        "field": AWARD_FIELD,
        "raw_observation": observed,
        "role": "supports",
    }
    samples = (
        Evidence(
            **shared,
            sentinel_assessment=None,
            normalized_value=None,
        ),
        Evidence(
            **shared,
            sentinel_assessment=sentinel,
            normalized_value=None,
        ),
        Evidence(
            **shared,
            sentinel_assessment=sentinel,
            normalized_value=sentinel.normalized_comparison_value,
        ),
        _missing_evidence(),
        _not_observed_evidence(),
        _not_observed_evidence(source_error=_timeout_error()),
    )

    def legacy_key(evidence: Evidence) -> tuple:
        return (
            evidence.dataset,
            evidence.field,
            evidence.code,
            evidence.role,
            *evidence_mod._raw_marker(evidence.raw_observation.raw_value),
            repr(evidence),
        )

    assert sorted(samples, key=evidence_mod._evidence_sort_key) == sorted(
        samples, key=legacy_key
    )

    for candidate in permutations(samples):
        legacy_valid = (
            len(set(candidate)) == len(candidate)
            and list(map(legacy_key, candidate))
            == sorted(map(legacy_key, candidate))
        )
        try:
            validate_evidence_set(candidate)
        except ValueError:
            current_valid = False
        else:
            current_valid = True
        assert current_valid is legacy_valid

    duplicated = tuple(sorted(samples, key=legacy_key)) + (samples[-1],)
    with pytest.raises(ValueError):
        validate_evidence_set(duplicated)


@pytest.mark.parametrize(
    ("left", "right"),
    ((True, 1), (1, 1.0), (False, 0), (0, 0.0), (-0.0, 0)),
)
def test_equal_evidence_has_equal_sort_key(
    left: bool | int | float,
    right: bool | int | float,
) -> None:
    first = Evidence(
        code="VALUE_OBSERVED",
        dataset="dataset",
        field="field",
        raw_observation=_observed(left),
        sentinel_assessment=None,
        normalized_value=None,
        role="supports",
    )
    second = Evidence(
        code="VALUE_OBSERVED",
        dataset="dataset",
        field="field",
        raw_observation=_observed(right),
        sentinel_assessment=None,
        normalized_value=None,
        role="supports",
    )

    assert first == second
    assert evidence_mod._evidence_sort_key(first) == evidence_mod._evidence_sort_key(
        second
    )


def test_explicit_sort_keys_cover_every_dataclass_field() -> None:
    cases = (
        (Evidence, evidence_mod._evidence_sort_key),
        (FieldObservation, evidence_mod._observation_sort_key),
        (SentinelAssessment, evidence_mod._sentinel_sort_key),
        (SourceError, evidence_mod._optional_source_error_sort_key),
        (AssessmentConflict, evidence_mod._conflict_sort_key),
    )

    for model, key_function in cases:
        source = inspect.getsource(key_function)
        missing = {
            field.name
            for field in dataclasses.fields(model)
            if f".{field.name}" not in source
        }
        assert not missing, f"{key_function.__name__} omits {model.__name__}: {missing}"


def test_linear_evidence_validation_rejects_every_invalid_shape() -> None:
    equal_bool = Evidence(
        code="VALUE_OBSERVED",
        dataset="dataset",
        field="field",
        raw_observation=_observed(True),
        sentinel_assessment=None,
        normalized_value=None,
        role="supports",
    )
    equal_int = Evidence(
        code="VALUE_OBSERVED",
        dataset="dataset",
        field="field",
        raw_observation=_observed(1),
        sentinel_assessment=None,
        normalized_value=None,
        role="supports",
    )
    later = Evidence(
        code="Z_VALUE_OBSERVED",
        dataset="dataset",
        field="field",
        raw_observation=_observed("later"),
        sentinel_assessment=None,
        normalized_value=None,
        role="supports",
    )
    ordered = tuple(
        sorted((equal_bool, later), key=evidence_mod._evidence_sort_key)
    )

    with pytest.raises(ValueError, match="duplicated"):
        validate_evidence_set((equal_bool, equal_int))
    with pytest.raises(ValueError):
        validate_evidence_set((equal_bool, later, equal_int))
    with pytest.raises(ValueError, match="canonical order"):
        validate_evidence_set(tuple(reversed(ordered)))
    with pytest.raises(TypeError, match="entries must be Evidence"):
        validate_evidence_set((equal_bool, object()))  # type: ignore[arg-type]


def test_quality_builder_preserves_all_inputs_while_canonicalizing() -> None:
    evidence_pair = _canonical(
        _cataloged_evidence("CO1.AWD.123"),
        _cataloged_evidence("CO1.AWD.456"),
    )
    conflict = AssessmentConflict(
        code="AWARD_CONFLICT",
        evidences=evidence_pair,
        consequence="REVIEW_REQUIRED",
        severity="material",
    )
    timeout = _timeout_error()

    quality = build_resolution_quality(
        completeness="partial",
        consistency="conflicting",
        confidence="low",
        reason_codes=("Z_REASON", "A_REASON", "Z_REASON"),
        missing_fields=("z_field", "a_field", "z_field"),
        source_errors=(timeout, timeout),
        conflicts=(conflict, conflict),
        capabilities=(("z_capability", "unavailable"), ("a_capability", "available")),
    )

    assert quality.reason_codes == ("A_REASON", "Z_REASON")
    assert quality.missing_fields == ("a_field", "z_field")
    assert quality.source_errors == (timeout,)
    assert quality.conflicts == (conflict,)
    assert quality.capabilities == (
        ("a_capability", "available"),
        ("z_capability", "unavailable"),
    )
