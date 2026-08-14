"""T1B-0A — Field Sentinel Semantics tests.

Pure unit tests for ``app/core/truth_layer/sentinels``.

Goals (binding spec):

* dataclass invariants (frozen, slots, hashable, deterministic equality);
* catalog immutability and exact lookup;
* raw preservation verbatim — ``normalized_comparison_value`` may differ
  but ``raw_observation.raw_value`` is never modified;
* open-identity fields pass unclassified values through as ``meaningful``;
* sentinel ``sentinel_unknown`` only for cataloged "No Definido" aliases;
* ``sentinel_absence`` only for cataloged "No Adjudicado" aliases;
* closed-vocabulary policies classify unknown values as ``unrecognized``;
* ``not_applicable`` requires an explicit, cataloged sentinel token;
* "0" has no global meaning — two synthetic policies can produce
  ``meaningful`` and ``sentinel_absence`` for the same raw value;
* exact comparator distinguishes bool/int and ``0`` from ``0.0``;
* ``string_casefold`` does not strip spaces, ``string_casefold_strip``
  does allow external whitespace;
* string comparators reject non-string raws and non-string aliases at
  construction;
* normalized alias collisions and duplicate codes are rejected;
* empty codes and empty aliases tuples are rejected;
* ``missing`` and ``not_observed`` are structurally rejected — they must
  not collapse into a semantic sentinel status;
* purity: no I/O, no clock, no environment, no filesystem, no parser,
  formatter, Radar, SECOP client, or QueryFrame imports;
* negative scope: ``Evidence``, ``ResolutionQuality``, lifecycle assessments
  and lot evidence types are NOT present in the module.
"""
from __future__ import annotations

import ast
import dataclasses
import inspect
from copy import deepcopy

import pytest

from app.core.truth_layer import (
    ComparisonMode,
    FieldObservation,
    FieldSentinelPolicy,
    SentinelAssessment,
    SentinelRule,
    SentinelStatus,
    UnknownValueDisposition,
    assess_sentinel,
    get_field_sentinel_policy,
)
import app.core.truth_layer.sentinels as sentinels_mod
import app.core.truth_layer as truth_layer_pkg


# ---------------------------------------------------------------------------
# Helpers / fixtures.
# ---------------------------------------------------------------------------


PROCESSES = "p6dx-8zbt"


def _obs(value: object) -> FieldObservation:
    return FieldObservation("observed", value)  # type: ignore[arg-type]


def _missing() -> FieldObservation:
    return FieldObservation("missing", None)


def _not_observed() -> FieldObservation:
    return FieldObservation("not_observed", None)


def award_policy() -> FieldSentinelPolicy:
    return get_field_sentinel_policy(PROCESSES, "id_adjudicacion")


def summary_policy() -> FieldSentinelPolicy:
    return get_field_sentinel_policy(PROCESSES, "estado_resumen")


# ---------------------------------------------------------------------------
# 1. Structure: dataclass invariants.
# ---------------------------------------------------------------------------


def test_sentinel_rule_is_frozen_with_slots() -> None:
    rule = SentinelRule(
        code="X",
        status="sentinel_absence",
        aliases=("a",),
    )
    assert dataclasses.is_dataclass(rule)
    assert rule.__dataclass_params__.frozen  # type: ignore[attr-defined]
    assert "__dict__" not in dir(rule)
    # slots prevents new attributes
    with pytest.raises(dataclasses.FrozenInstanceError):
        rule.code = "Y"  # type: ignore[misc]


def test_field_sentinel_policy_is_frozen_with_slots() -> None:
    policy = award_policy()
    assert dataclasses.is_dataclass(policy)
    assert policy.__dataclass_params__.frozen  # type: ignore[attr-defined]
    assert "__dict__" not in dir(policy)
    with pytest.raises(dataclasses.FrozenInstanceError):
        policy.field_name = "other"  # type: ignore[misc]


def test_sentinel_assessment_is_frozen_with_slots() -> None:
    assessment = assess_sentinel(
        _obs("No Adjudicado"), award_policy()
    )
    assert dataclasses.is_dataclass(assessment)
    assert assessment.__dataclass_params__.frozen  # type: ignore[attr-defined]
    assert "__dict__" not in dir(assessment)
    with pytest.raises(dataclasses.FrozenInstanceError):
        assessment.status = "meaningful"  # type: ignore[misc]


def test_sentinel_rule_is_hashable() -> None:
    rule = SentinelRule(
        code="X",
        status="sentinel_absence",
        aliases=("a",),
    )
    # Hashing requires frozen + hashable fields.
    assert hash(rule) == hash(
        SentinelRule(code="X", status="sentinel_absence", aliases=("a",))
    )


def test_field_sentinel_policy_is_hashable() -> None:
    policy = award_policy()
    assert hash(policy) == hash(award_policy())


def test_sentinel_assessment_is_hashable() -> None:
    obs = _obs("No Adjudicado")
    a1 = assess_sentinel(obs, award_policy())
    a2 = assess_sentinel(obs, award_policy())
    assert hash(a1) == hash(a2)
    # Same observation+policy outcome -> structural equality.
    assert a1 == a2


def test_deterministic_equality_for_rules() -> None:
    r1 = SentinelRule(code="A", status="sentinel_unknown", aliases=("x",))
    r2 = SentinelRule(code="A", status="sentinel_unknown", aliases=("x",))
    assert r1 == r2
    assert hash(r1) == hash(r2)


def test_deterministic_equality_for_policies() -> None:
    p1 = award_policy()
    p2 = award_policy()
    assert p1 == p2


def test_deterministic_equality_for_assessments() -> None:
    obs = _obs("No Adjudicado")
    a1 = assess_sentinel(obs, award_policy())
    a2 = assess_sentinel(deepcopy(obs), award_policy())
    assert a1 == a2


# ---------------------------------------------------------------------------
# 2. Catalog immutability + exact lookup.
# ---------------------------------------------------------------------------


def test_catalog_is_immutable_mapping() -> None:
    catalog = sentinels_mod._PRODUCTION_POLICIES
    with pytest.raises(TypeError):
        catalog[("p6dx-8zbt", "nope")] = award_policy()  # type: ignore[index]
    with pytest.raises(TypeError):
        del catalog[("p6dx-8zbt", "id_adjudicacion")]  # type: ignore[attr-defined]


def test_catalog_contains_only_two_authorized_policies() -> None:
    keys = set(sentinels_mod._PRODUCTION_POLICIES.keys())
    assert keys == {
        ("p6dx-8zbt", "id_adjudicacion"),
        ("p6dx-8zbt", "estado_resumen"),
    }
    assert "numero_de_lotes" not in {field for _, field in keys}


def test_lookup_exactly_returns_authoritative_policy() -> None:
    p = get_field_sentinel_policy(PROCESSES, "id_adjudicacion")
    assert p is not None
    assert p.dataset_id == PROCESSES
    assert p.field_name == "id_adjudicacion"
    assert p.comparison_mode == "string_casefold_strip"
    assert p.unknown_value_disposition == "pass_through_as_meaningful"
    assert len(p.rules) == 1
    rule = p.rules[0]
    assert rule.code == "PROCESS_AWARD_ID_NO_AWARD"
    assert rule.status == "sentinel_absence"
    assert rule.aliases == ("No Adjudicado",)


def test_lookup_miss_returns_none() -> None:
    assert get_field_sentinel_policy("unknown-dataset", "anything") is None
    # Known dataset, unknown field
    assert get_field_sentinel_policy(PROCESSES, "id_estado_del_procedimiento") is None
    # Known field, wrong dataset
    assert get_field_sentinel_policy("other", "id_adjudicacion") is None


def test_lookup_rejects_non_string_keys() -> None:
    with pytest.raises(TypeError):
        get_field_sentinel_policy(123, "id_adjudicacion")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        get_field_sentinel_policy(PROCESSES, None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 3. Exports: preserved surface, forward-compatible.
#
# The package ``__all__`` is EXPECTED to grow: every later Truth Layer block
# (T1B-1 .. T1B-7) adds public API to it. Freezing it to an exact set here
# would make this test a gate that blocks every following block by design.
# What must be frozen instead is that nothing already published is dropped,
# renamed or rebound — and that ``sentinels.py`` itself, which IS closed,
# keeps exactly its eight public exports.
# ---------------------------------------------------------------------------


_T1A_EXPORTS = {
    "ContractLifecycleSignals",
    "FieldObservation",
    "ObservationPresence",
    "ProcessContractReference",
    "ProcessLifecycleSignals",
    "PublicationVisibilitySignals",
    "RawScalar",
    "TemporalQueryContext",
}

_T1B_0A_EXPORTS = {
    "SentinelStatus",
    "UnknownValueDisposition",
    "ComparisonMode",
    "SentinelRule",
    "FieldSentinelPolicy",
    "SentinelAssessment",
    "assess_sentinel",
    "get_field_sentinel_policy",
}


def test_previous_exports_are_all_still_published() -> None:
    published = set(truth_layer_pkg.__all__)
    preserved = _T1A_EXPORTS | _T1B_0A_EXPORTS
    assert len(preserved) == 16
    missing = preserved - published
    assert not missing, f"previously published exports disappeared: {missing}"


def test_package_all_has_no_duplicates() -> None:
    names = list(truth_layer_pkg.__all__)
    duplicates = {name for name in names if names.count(name) > 1}
    assert not duplicates, f"duplicated names in __all__: {duplicates}"


def test_sentinels_module_keeps_exactly_eight_public_exports() -> None:
    # sentinels.py is a closed block: unlike the package, its surface must
    # not grow.
    assert set(sentinels_mod.__all__) == _T1B_0A_EXPORTS
    assert len(sentinels_mod.__all__) == 8
    assert len(set(sentinels_mod.__all__)) == 8


def test_no_previous_export_was_rebound_to_a_different_object() -> None:
    import app.core.truth_layer.models as models_mod

    for name in _T1A_EXPORTS:
        assert getattr(truth_layer_pkg, name) is getattr(models_mod, name), (
            f"{name} no longer resolves to the T1A object"
        )
    for name in _T1B_0A_EXPORTS:
        assert getattr(truth_layer_pkg, name) is getattr(sentinels_mod, name), (
            f"{name} no longer resolves to the T1B-0A object"
        )


def test_production_catalog_is_not_public_api() -> None:
    assert "_PRODUCTION_POLICIES" not in sentinels_mod.__all__
    assert "_PRODUCTION_POLICIES" not in truth_layer_pkg.__all__
    assert not hasattr(truth_layer_pkg, "_PRODUCTION_POLICIES")


def test_sentinel_private_helpers_are_not_public_api() -> None:
    for helper in (
        "_normalize_string",
        "_normalize_for_compare",
        "_match_alias",
        "_alias_compatible_with_mode",
        "_collision_marker",
    ):
        assert hasattr(sentinels_mod, helper), f"{helper} vanished from sentinels"
        assert helper not in sentinels_mod.__all__
        assert helper not in truth_layer_pkg.__all__
        assert not hasattr(truth_layer_pkg, helper)


def test_eight_t1a_exports_preserved() -> None:
    t1a = {
        "ContractLifecycleSignals",
        "FieldObservation",
        "ObservationPresence",
        "ProcessContractReference",
        "ProcessLifecycleSignals",
        "PublicationVisibilitySignals",
        "RawScalar",
        "TemporalQueryContext",
    }
    assert t1a.issubset(set(truth_layer_pkg.__all__))


# ---------------------------------------------------------------------------
# 4. Raw preservation — never replaces raw_value.
# ---------------------------------------------------------------------------


def test_no_adjudicado_preserved_verbatim() -> None:
    obs = _obs("No Adjudicado")
    assessment = assess_sentinel(obs, award_policy())
    assert assessment.raw_observation is obs
    assert assessment.raw_observation.raw_value == "No Adjudicado"


def test_no_adjudicado_with_external_spaces_preserved_verbatim() -> None:
    obs = _obs(" no adjudicado ")
    assessment = assess_sentinel(obs, award_policy())
    assert assessment.raw_observation.raw_value == " no adjudicado "
    assert assessment.status == "sentinel_absence"
    assert assessment.sentinel_code == "PROCESS_AWARD_ID_NO_AWARD"
    assert assessment.normalized_comparison_value == "no adjudicado"


def test_no_definido_preserved_verbatim() -> None:
    obs = _obs("No Definido")
    assessment = assess_sentinel(obs, summary_policy())
    assert assessment.raw_observation.raw_value == "No Definido"
    assert assessment.status == "sentinel_unknown"
    assert assessment.sentinel_code == "PROCESS_SUMMARY_UNDEFINED"
    assert assessment.normalized_comparison_value == "no definido"


def test_normalized_comparison_value_can_differ_from_raw() -> None:
    obs = _obs(" no adjudicado ")
    assessment = assess_sentinel(obs, award_policy())
    assert assessment.normalized_comparison_value != obs.raw_value
    # Raw remains untouched.
    assert obs.raw_value == " no adjudicado "


def test_policy_and_observation_are_not_mutated_by_assess() -> None:
    policy = award_policy()
    obs = _obs("No Adjudicado")
    before_policy = deepcopy(policy)
    before_obs = deepcopy(obs)
    assess_sentinel(obs, policy)
    assert policy == before_policy
    assert obs == before_obs


# ---------------------------------------------------------------------------
# 5. Open identity — id_adjudicacion meaningful passthrough.
# ---------------------------------------------------------------------------


def test_id_adjudicacion_genuine_award_id_is_meaningful() -> None:
    obs = _obs("CO1.AWD.123")
    assessment = assess_sentinel(obs, award_policy())
    assert assessment.status == "meaningful"
    assert assessment.sentinel_code is None
    assert assessment.raw_observation.raw_value == "CO1.AWD.123"
    assert assessment.normalized_comparison_value == "co1.awd.123"


def test_unknown_id_value_is_meaningful_not_unrecognized() -> None:
    # id_adjudicacion is open identity (pass_through_as_meaningful)
    obs = _obs("NEW-FORMAT-CASE-9")
    assessment = assess_sentinel(obs, award_policy())
    assert assessment.status == "meaningful"
    assert assessment.sentinel_code is None


# ---------------------------------------------------------------------------
# 6. FieldSentinelPolicy validation — code/alias constraints.
# ---------------------------------------------------------------------------


def _synthetic_string_policy(
    *,
    disposition: UnknownValueDisposition,
    rules: tuple[SentinelRule, ...],
    mode: ComparisonMode = "string_casefold_strip",
    field: str = "synthetic_field",
    dataset: str = "synthetic_dataset",
) -> FieldSentinelPolicy:
    return FieldSentinelPolicy(
        dataset_id=dataset,
        field_name=field,
        comparison_mode=mode,
        unknown_value_disposition=disposition,
        rules=rules,
    )


def test_empty_rule_code_rejected() -> None:
    with pytest.raises(ValueError):
        SentinelRule(code="", status="sentinel_absence", aliases=("a",))


def test_empty_aliases_tuple_rejected() -> None:
    with pytest.raises(ValueError):
        SentinelRule(
            code="X", status="sentinel_absence", aliases=()
        )


def test_none_alias_rejected() -> None:
    with pytest.raises(ValueError):
        SentinelRule(
            code="X",
            status="sentinel_absence",
            aliases=(None,),  # type: ignore[arg-type]
        )


def test_meaningful_status_in_rule_rejected() -> None:
    # Sentinel rules must not define "meaningful".
    with pytest.raises(ValueError):
        SentinelRule(
            code="M",
            status="meaningful",  # type: ignore[arg-type]
            aliases=("x",),
        )


def test_empty_dataset_id_rejected() -> None:
    with pytest.raises(ValueError):
        FieldSentinelPolicy(
            dataset_id="",
            field_name="f",
            comparison_mode="string_casefold_strip",
            unknown_value_disposition="pass_through_as_meaningful",
            rules=(SentinelRule(code="A", status="sentinel_unknown", aliases=("x",)),),
        )


def test_empty_field_name_rejected() -> None:
    with pytest.raises(ValueError):
        FieldSentinelPolicy(
            dataset_id="d",
            field_name="",
            comparison_mode="string_casefold_strip",
            unknown_value_disposition="pass_through_as_meaningful",
            rules=(SentinelRule(code="A", status="sentinel_unknown", aliases=("x",)),),
        )


def test_empty_rules_tuple_rejected() -> None:
    with pytest.raises(ValueError):
        FieldSentinelPolicy(
            dataset_id="d",
            field_name="f",
            comparison_mode="string_casefold_strip",
            unknown_value_disposition="pass_through_as_meaningful",
            rules=(),
        )


# ---------------------------------------------------------------------------
# 7. unrecognized synthetic policy.
# ---------------------------------------------------------------------------


def test_synthetic_unrecognized_policy_classifies_new_value() -> None:
    policy = _synthetic_string_policy(
        disposition="classify_as_unrecognized",
        rules=(
            SentinelRule(
                code="SENT_A",
                status="sentinel_unknown",
                aliases=("No Definido",),
            ),
        ),
    )
    obs = _obs("BrandNewLiteral")
    assessment = assess_sentinel(obs, policy)
    assert assessment.status == "unrecognized"
    assert assessment.sentinel_code is None
    assert assessment.raw_observation.raw_value == "BrandNewLiteral"
    # Synthetic policy did NOT enter the production catalog.
    assert get_field_sentinel_policy(
        "synthetic_dataset", "synthetic_field"
    ) is None


def test_synthetic_unrecognized_still_matches_cataloged_sentinel() -> None:
    policy = _synthetic_string_policy(
        disposition="classify_as_unrecognized",
        rules=(
            SentinelRule(
                code="SENT_A",
                status="sentinel_unknown",
                aliases=("No Definido",),
            ),
        ),
    )
    obs = _obs("No Definido")
    assessment = assess_sentinel(obs, policy)
    assert assessment.status == "sentinel_unknown"
    assert assessment.sentinel_code == "SENT_A"


# ---------------------------------------------------------------------------
# 8. not_applicable synthetic policy.
# ---------------------------------------------------------------------------


def test_synthetic_not_applicable_policy_matches_token() -> None:
    policy = _synthetic_string_policy(
        disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="NA_TOKEN",
                status="not_applicable",
                aliases=("No Aplica",),
            ),
        ),
    )
    obs = _obs("No Aplica")
    assessment = assess_sentinel(obs, policy)
    assert assessment.status == "not_applicable"
    assert assessment.sentinel_code == "NA_TOKEN"
    assert assessment.raw_observation.raw_value == "No Aplica"


def test_synthetic_not_applicable_policy_passes_through_uncataloged() -> None:
    policy = _synthetic_string_policy(
        disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="NA_TOKEN",
                status="not_applicable",
                aliases=("No Aplica",),
            ),
        ),
    )
    obs = _obs("En preparación")
    assessment = assess_sentinel(obs, policy)
    assert assessment.status == "meaningful"
    assert assessment.sentinel_code is None
    assert assessment.raw_observation.raw_value == "En preparación"


# ---------------------------------------------------------------------------
# 9. Contextual "0" — no global sentinel for "0".
# ---------------------------------------------------------------------------


def _zero_meaningful_policy() -> FieldSentinelPolicy:
    # exact-mode policy with a sentinel rule for a different token; "0"
    # remains uncataloged, so pass_through_as_meaningful classifies it
    # as meaningful.
    return FieldSentinelPolicy(
        dataset_id="synthetic_a",
        field_name="zero_field",
        comparison_mode="exact",
        unknown_value_disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="OTHER_TOKEN",
                status="sentinel_unknown",
                aliases=("+no-signal+",),
            ),
        ),
    )


def _zero_absence_policy() -> FieldSentinelPolicy:
    # exact-mode policy where int 0 explicitly denotes absence.
    return FieldSentinelPolicy(
        dataset_id="synthetic_b",
        field_name="zero_field",
        comparison_mode="exact",
        unknown_value_disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="ZERO_AS_ABSENCE",
                status="sentinel_absence",
                aliases=(0,),  # int 0
            ),
        ),
    )


def test_zero_under_field_a_is_meaningful() -> None:
    obs = _obs(0)
    assessment = assess_sentinel(obs, _zero_meaningful_policy())
    assert assessment.status == "meaningful"
    assert assessment.sentinel_code is None


def test_zero_under_field_b_is_sentinel_absence() -> None:
    obs = _obs(0)
    assessment = assess_sentinel(obs, _zero_absence_policy())
    assert assessment.status == "sentinel_absence"
    assert assessment.sentinel_code == "ZERO_AS_ABSENCE"


def test_zero_string_not_treated_as_int_zero() -> None:
    # exact mode: "0" (str) does not match int 0 alias.
    obs = _obs("0")
    assessment = assess_sentinel(obs, _zero_absence_policy())
    assert assessment.status == "meaningful"


def test_numero_de_lotes_not_in_production_catalog() -> None:
    # Reinforced: even with knowledge of the field name, production catalog
    # must NOT carry it.
    assert get_field_sentinel_policy(
        PROCESSES, "numero_de_lotes"
    ) is None


# ---------------------------------------------------------------------------
# 10. Comparators.
# ---------------------------------------------------------------------------


def test_exact_distinguishes_bool_and_int() -> None:
    policy = FieldSentinelPolicy(
        dataset_id="d",
        field_name="bool_field",
        comparison_mode="exact",
        unknown_value_disposition="pass_through_as_meaningful",
        rules=(
            # rule using bool alias True
            SentinelRule(
                code="BOOL_TRUE_SENTINEL",
                status="sentinel_absence",
                aliases=(True,),
            ),
        ),
    )
    # True matches
    a_true = assess_sentinel(_obs(True), policy)
    assert a_true.status == "sentinel_absence"
    # int 1 does NOT match bool True (type strict)
    a_one = assess_sentinel(_obs(1), policy)
    assert a_one.status == "meaningful"


def test_exact_distinguishes_int_and_float_zero() -> None:
    policy = FieldSentinelPolicy(
        dataset_id="d",
        field_name="int_field",
        comparison_mode="exact",
        unknown_value_disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="INT_ZERO",
                status="sentinel_absence",
                aliases=(0,),  # int
            ),
        ),
    )
    assert assess_sentinel(_obs(0), policy).status == "sentinel_absence"
    assert assess_sentinel(_obs(0.0), policy).status == "meaningful"


def test_exact_preserves_string_spaces() -> None:
    # "a " must not equal "a" under exact comparator.
    policy = FieldSentinelPolicy(
        dataset_id="d",
        field_name="text_field",
        comparison_mode="exact",
        unknown_value_disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="EXACT_A",
                status="sentinel_absence",
                aliases=("a",),
            ),
        ),
    )
    assert assess_sentinel(_obs("a"), policy).status == "sentinel_absence"
    assert assess_sentinel(_obs("a "), policy).status == "meaningful"
    assert assess_sentinel(_obs(" a"), policy).status == "meaningful"


def test_string_casefold_does_not_strip_spaces() -> None:
    policy = FieldSentinelPolicy(
        dataset_id="d",
        field_name="text_field",
        comparison_mode="string_casefold",
        unknown_value_disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="CF_A",
                status="sentinel_absence",
                aliases=("a",),
            ),
        ),
    )
    assert assess_sentinel(_obs("A"), policy).status == "sentinel_absence"
    # Pure alias "a" (without spaces) does not match raw "a " (trailing
    # space) because string_casefold does not strip.
    assert (
        assess_sentinel(_obs("a "), policy).status == "meaningful"
    )


def test_string_casefold_strip_allows_external_spaces() -> None:
    policy = FieldSentinelPolicy(
        dataset_id="d",
        field_name="text_field",
        comparison_mode="string_casefold_strip",
        unknown_value_disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="CF_STRIP_A",
                status="sentinel_absence",
                aliases=("A",),  # normalized: "a"
            ),
            SentinelRule(
                code="CF_STRIP_B",
                status="sentinel_unknown",
                aliases=("  b  ",),  # normalized: "b"
            ),
        ),
    )
    # Leading/trailing whitespace on the RAW is also stripped.
    assert (
        assess_sentinel(_obs("  a  "), policy).status == "sentinel_absence"
    )
    assert (
        assess_sentinel(_obs("B"), policy).status == "sentinel_unknown"
    )


def test_string_casefold_strip_alias_with_spaces_normalizes() -> None:
    # alias with whitespace still normalizes under strip mode
    policy = FieldSentinelPolicy(
        dataset_id="d",
        field_name="text_field",
        comparison_mode="string_casefold_strip",
        unknown_value_disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="CF_TOKEN",
                status="sentinel_unknown",
                aliases=("  No Definido  ",),
            ),
        ),
    )
    assessment = assess_sentinel(_obs("no definido"), policy)
    assert assessment.status == "sentinel_unknown"
    assert assessment.sentinel_code == "CF_TOKEN"


def test_string_comparator_rejects_non_string_raw() -> None:
    policy = FieldSentinelPolicy(
        dataset_id="d",
        field_name="text_field",
        comparison_mode="string_casefold",
        unknown_value_disposition="pass_through_as_meaningful",
        rules=(
            SentinelRule(
                code="CF_A",
                status="sentinel_absence",
                aliases=("a",),
            ),
        ),
    )
    with pytest.raises(TypeError):
        assess_sentinel(_obs(1), policy)


def test_string_alias_must_be_string_at_policy_construction() -> None:
    with pytest.raises(TypeError):
        FieldSentinelPolicy(
            dataset_id="d",
            field_name="f",
            comparison_mode="string_casefold_strip",
            unknown_value_disposition="pass_through_as_meaningful",
            rules=(
                SentinelRule(
                    code="CF_A",
                    status="sentinel_absence",
                    aliases=(1,),  # type: ignore[arg-type]
                ),
            ),
        )


def test_exact_alias_supports_string_int_float_bool() -> None:
    for alias, raw in (
        ("x", "x"),
        (1, 1),
        (2.5, 2.5),
        (True, True),
    ):
        policy = FieldSentinelPolicy(
            dataset_id="d",
            field_name="f",
            comparison_mode="exact",
            unknown_value_disposition="pass_through_as_meaningful",
            rules=(SentinelRule(code="C", status="sentinel_absence", aliases=(alias,)),),
        )
        assert assess_sentinel(_obs(raw), policy).status == "sentinel_absence"


def test_normalized_alias_collisions_rejected() -> None:
    with pytest.raises(ValueError):
        FieldSentinelPolicy(
            dataset_id="d",
            field_name="f",
            comparison_mode="string_casefold_strip",
            unknown_value_disposition="pass_through_as_meaningful",
            rules=(
                SentinelRule(
                    code="A",
                    status="sentinel_unknown",
                    aliases=("No Definido",),
                ),
                SentinelRule(
                    code="B",
                    status="sentinel_absence",
                    aliases=("  no definido ",),
                ),
            ),
        )


def test_duplicate_rule_codes_rejected() -> None:
    with pytest.raises(ValueError):
        FieldSentinelPolicy(
            dataset_id="d",
            field_name="f",
            comparison_mode="string_casefold_strip",
            unknown_value_disposition="pass_through_as_meaningful",
            rules=(
                SentinelRule(
                    code="DUP",
                    status="sentinel_unknown",
                    aliases=("a",),
                ),
                SentinelRule(
                    code="DUP",
                    status="sentinel_absence",
                    aliases=("b",),
                ),
            ),
        )


def test_same_rule_aliases_collision_rejected() -> None:
    with pytest.raises(ValueError):
        FieldSentinelPolicy(
            dataset_id="d",
            field_name="f",
            comparison_mode="string_casefold_strip",
            unknown_value_disposition="pass_through_as_meaningful",
            rules=(
                SentinelRule(
                    code="A",
                    status="sentinel_unknown",
                    aliases=("No Definido", "  no definido "),
                ),
            ),
        )


# ---------------------------------------------------------------------------
# 11. Presence gate — missing / not_observed forbidden.
# ---------------------------------------------------------------------------


def test_missing_presence_rejected() -> None:
    with pytest.raises(ValueError):
        assess_sentinel(_missing(), award_policy())


def test_not_observed_presence_rejected() -> None:
    with pytest.raises(ValueError):
        assess_sentinel(_not_observed(), award_policy())


def test_observed_presence_accepted() -> None:
    obs = _obs("CO1.AWD.42")
    assessment = assess_sentinel(obs, award_policy())
    assert assessment.raw_observation.presence == "observed"


def test_missing_and_not_observed_do_not_become_sentinel() -> None:
    # The classifier must not silently emit sentinel_absence nor
    # sentinel_unknown when no scalar was observed.
    for missing_obs in (_missing(), _not_observed()):
        with pytest.raises(ValueError):
            assess_sentinel(missing_obs, award_policy())


def test_assess_rejects_non_field_observation() -> None:
    with pytest.raises(TypeError):
        assess_sentinel("No Adjudicado", award_policy())  # type: ignore[arg-type]


def test_assess_rejects_non_policy() -> None:
    with pytest.raises(TypeError):
        assess_sentinel(_obs("No Adjudicado"), "policy")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 12. Purity — no I/O, clock, env, filesystem, parser/formatter/Radar.
# ---------------------------------------------------------------------------


def test_module_imports_pure() -> None:
    source = inspect.getsource(sentinels_mod)
    tree = ast.parse(source)
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported_modules.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module is not None:
                imported_modules.add(node.module.split(".")[0])
    forbidden_prefixes = (
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
        "glob",
        "shutil",
        "asyncio",
    )
    forbidden_exact = {
        "parser",
        "formatter",
        "radar",
        "soql_builder",
        "feedback",
        "orchestrator",
        "query_router",
        "entity_resolver",
    }
    for mod in imported_modules:
        root = mod.split(".")[0]
        assert not any(
            mod == prefix or mod.startswith(prefix + ".")
            for prefix in forbidden_prefixes
        ), f"forbidden import: {mod}"
        assert mod not in forbidden_exact, f"forbidden import: {mod}"
        # Truth Layer may only import itself + typing/dataclasses/types.
        allowed_roots = {
            "app",
            "dataclasses",
            "typing",
            "types",
            "__future__",
        }
        assert root in allowed_roots, f"unexpected import root: {mod}"


def test_no_time_or_environment_calls() -> None:
    source = inspect.getsource(sentinels_mod)
    tree = ast.parse(source)
    forbidden_calls = {
        "datetime.now",
        "time.time",
        "time.monotonic",
        "os.environ",
        "os.getcwd",
        "open",
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            attr = node.attr
            if isinstance(node.value, ast.Name):
                full = f"{node.value.id}.{attr}"
                assert full not in forbidden_calls, f"forbidden call: {full}"


def test_module_does_not_define_lifecycle_or_evidence_types() -> None:
    """AST-level negative-scope check.

    The spec says "Comprobar ... por inspección o AST". We walk the parsed
    AST and assert that NO class, function, method, attribute assignment,
    or annotation dereferences any forbidden downstream concept. The
    docstring may describe the future pipeline position; the code may not
    import, subclass, name, or declare any of these types.
    """
    source = inspect.getsource(sentinels_mod)
    tree = ast.parse(source)
    forbidden_names = {
        "Evidence",
        "ResolutionQuality",
        "AssessmentConflict",
        "ProcessLifecycleAssessment",
        "ContractLifecycleAssessment",
        "TemporalAssessment",
        "PublicationVisibilityAssessment",
        "OpportunityRoleAssessment",
        "LotStructureEvidence",
        "LotIdentityEvidence",
        "ContractLotLinkageEvidence",
    }
    forbidden_attrs = {"Evidence", "ResolutionQuality", "AssessmentConflict"}
    defined_names: set[str] = set()
    annotated_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            defined_names.add(node.name)
            for base in node.bases:
                if isinstance(base, ast.Name):
                    defined_names.add(base.id)
        elif isinstance(node, ast.FunctionDef):
            defined_names.add(node.name)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                if isinstance(node.annotation, ast.Name):
                    annotated_names.add(node.annotation.id)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in forbidden_attrs:
                    defined_names.add(target.id)
    assert not (defined_names & forbidden_names), (
        f"sentinels.py defines forbidden classes/functions: "
        f"{defined_names & forbidden_names}"
    )
    assert not (annotated_names & forbidden_names), (
        f"sentinels.py references forbidden types in annotations: "
        f"{annotated_names & forbidden_names}"
    )


def test_module_does_not_import_runtime_consumers() -> None:
    source = inspect.getsource(sentinels_mod)
    forbidden_modules = [
        "soql_builder",
        "formatter",
        "radar",
        "opportunity_stage",
        "parser",
        "QueryFrame",
        "orchestrator",
        "query_router",
        "entity_resolver",
        "secop",
        "socrata",
    ]
    for forbidden in forbidden_modules:
        assert (
            f"import {forbidden}" not in source
            and f"from {forbidden}" not in source
        ), f"sentinels.py must not import {forbidden}"


# ---------------------------------------------------------------------------
# 13. SentinelAssessment invariants (post-init).
# ---------------------------------------------------------------------------


def test_assessment_rejects_code_with_meaningful() -> None:
    with pytest.raises(ValueError):
        SentinelAssessment(
            dataset_id="d",
            field_name="f",
            raw_observation=_obs("x"),
            status="meaningful",
            sentinel_code="X",  # type: ignore[arg-type]
            normalized_comparison_value="x",
        )


def test_assessment_rejects_none_code_for_sentinel_absence() -> None:
    with pytest.raises(ValueError):
        SentinelAssessment(
            dataset_id="d",
            field_name="f",
            raw_observation=_obs("x"),
            status="sentinel_absence",
            sentinel_code=None,
            normalized_comparison_value="x",
        )


def test_assessment_rejects_empty_code_for_sentinel_unknown() -> None:
    with pytest.raises(ValueError):
        SentinelAssessment(
            dataset_id="d",
            field_name="f",
            raw_observation=_obs("x"),
            status="sentinel_unknown",
            sentinel_code="",
            normalized_comparison_value="x",
        )


def test_assessment_rejects_non_observation_type() -> None:
    with pytest.raises(TypeError):
        SentinelAssessment(
            dataset_id="d",
            field_name="f",
            raw_observation={"presence": "observed"},  # type: ignore[arg-type]
            status="meaningful",
            sentinel_code=None,
            normalized_comparison_value="x",
        )


# ---------------------------------------------------------------------------
# 14. End-to-end coverage over the production catalog.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["No Adjudicado", " no adjudicado ", "NO ADJUDICADO"])
def test_no_adjudicado_variants_match_absence(raw: str) -> None:
    obs = _obs(raw)
    assessment = assess_sentinel(obs, award_policy())
    assert assessment.status == "sentinel_absence"
    assert assessment.sentinel_code == "PROCESS_AWARD_ID_NO_AWARD"


@pytest.mark.parametrize("raw", ["No Definido", " no definido ", "NO DEFINIDO"])
def test_no_definido_variants_match_unknown(raw: str) -> None:
    obs = _obs(raw)
    assessment = assess_sentinel(obs, summary_policy())
    assert assessment.status == "sentinel_unknown"
    assert assessment.sentinel_code == "PROCESS_SUMMARY_UNDEFINED"


def test_pass_through_does_not_become_unrecognized_for_open_identity() -> None:
    # The cataloged policy uses pass_through_as_meaningful, so a brand-new
    # Award Id-like value MUST never collapse into "unrecognized".
    obs = _obs("CO1.AWD.999")
    assessment = assess_sentinel(obs, award_policy())
    assert assessment.status == "meaningful"
    assert assessment.sentinel_code is None
