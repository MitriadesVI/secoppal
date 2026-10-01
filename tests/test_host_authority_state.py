"""STATE host authority (M1-D-STATE/MOD): the C1.1 kit, the exhaustive catalog property and the A2 probes."""
from __future__ import annotations

from dataclasses import replace
from itertools import product

import pytest
from hypothesis import given, settings, strategies as st

from app.core import estado_families as ef
from app.core.foundation_authority_contract import AuthorityContext, StateValue, decode_value
from app.core.host_authorities import state_catalog as cat
from app.core.host_authorities.datasets import CONTRACTS, PROCESSES, logical_datasets
from app.core.host_authorities.state import FUNNEL_POLICY, StateAuthority
from app.core.interpretation_contract import Polarity, ResolutionDecision
from app.core.semantic_dimensions import SemanticDimension
from app.data.estados import (
    ESTADO_CONTRATO_SYNONYMS, VALID_ESTADOS_APERTURA, VALID_ESTADOS_CONTRATO, VALID_ESTADOS_PROCEDIMIENTO,
)
from tests.foundation_authority_kit import (
    REFERENCE_INSTANT, ConformanceCase, assert_conforms, case, evidence_from_text,
)
from tests.m1_state_mod_gold import conditions, evidence_and_context, outcome

P, C = "p6dx-8zbt", "jbjy-vk9h"
AUTH = StateAuthority()
POS, NEG = Polarity.POSITIVE, Polarity.NEGATIVE
STATE = SemanticDimension.STATE
DATASET_SETS = ((P,), (C,), (P, C))
RESOLVED, CLARIFY = ResolutionDecision.RESOLVED, ResolutionDecision.CLARIFY


def ctx(*datasets: str) -> AuthorityContext:
    return AuthorityContext(REFERENCE_INSTANT, frozenset(datasets or (P, C)))


def run(raw, needle, operator="is", polarity=POS, datasets=(P, C)):
    evidence = evidence_from_text(raw, needle, dimension=STATE, operator=operator, polarity=polarity)
    return AUTH.resolve(evidence, ctx(*datasets))


def reasons(verdict) -> tuple[str | None, str | None]:
    return verdict.reason, verdict.detail


def literals_of(verdict) -> dict:
    return {(name, column): values for name, column, values in verdict.value.literals}


# ---- 1. the C1.1 conformance kit ----------------------------------------------------------------
def _cases() -> list[ConformanceCase]:
    out = []
    specs = [
        ("procesos abiertos de aseo", "abiertos", "family", POS, (P,)),
        ("procesos vigentes de aseo", "vigentes", "family", POS, (P, C)),
        ("procesos publicados de aseo", "publicados", "is", POS, (P,)),
        ("procesos cerrados de aseo", "cerrados", "is", POS, (P,)),
        ("contratos cerrados de aseo", "cerrados", "is", NEG, (C,)),
        ("procesos adjudicados de aseo", "adjudicados", "is", POS, (P,)),
        ("contratos terminados de aseo", "terminados", "is", POS, (C,)),
        ("contratos no terminados de aseo", "no terminados", "is", NEG, (C,)),
        ("aseo en Cali que no esté cerrado", "no esté cerrado", "is", NEG, (P, C)),
        ("contratos en ejecución de aseo", "en ejecución", "is", POS, (C,)),
        ("contratos cedidos de aseo", "cedidos", "is", POS, (C,)),
        ("procesos cancelados de aseo", "cancelados", "is", NEG, (P,)),
        ("procesos de aseo en borrador", "en borrador", "is", POS, (P,)),
        ("procesos en evaluación", "en evaluación", "is", POS, (P,)),
        ("pavimentación terminada en el Huila", "terminada", "is", POS, (P, C)),
        ("procesos publicados hace dos semanas", "publicados", "is", POS, (P,)),     # D6: withdrawn
        ("procesos publicados en 2024 de aseo", "publicados", "is", POS, (P,)),       # D6: withdrawn
        ("procesos publicados o abiertos de aseo", ["publicados", "abiertos"], "family", POS, (P,)),
        ("procesos publicados o abiertos de aseo", "publicados o abiertos", "family", POS, (P,)),
        ("procesos publicados y abiertos de aseo", ["publicados", "abiertos"], "family", POS, (P,)),
        ("contratos terminados anticipadamente", "terminados anticipadamente", "is", POS, (C,)),
        ("contratos liquidados", "liquidados", "is", POS, (C,)),
        ("contratos abiertos de aseo", "abiertos", "family", POS, (C,)),
        ("procesos de aseo que no estén abiertos", "no estén abiertos", "family", NEG, (P,)),
        ("procesos abiertos y cerrados de aseo", ["abiertos", "cerrados"], "family", POS, (P,)),
    ]
    for raw, needle, operator, polarity, datasets in specs:
        out.append(case(raw, needle, ctx=ctx(*datasets), dimension=STATE, operator=operator,
                        polarity=polarity) if isinstance(needle, str) else
                   ConformanceCase(evidence_from_text(raw, needle, dimension=STATE, operator=operator,
                                                      polarity=polarity), ctx(*datasets)))
    return out


def test_authority_conforms_to_the_c1_1_kit():
    report = assert_conforms(AUTH, _cases())
    assert report.cases == len(_cases()) and report.resolved >= 14


def _phrase_cases():
    """Every phrase of the lexicon, every inflection of its last word, every dataset set, both polarities."""
    for phrase, reading in sorted(cat.LEXICON.items()):
        spelled = " ".join(phrase)
        for datasets in DATASET_SETS:
            for polarity in (POS, NEG):
                yield ConformanceCase(
                    evidence_from_text(f"procesos {spelled} de aseo", spelled, dimension=STATE,
                                       operator=reading.operator, polarity=polarity), ctx(*datasets))


def test_the_kit_runs_over_the_whole_lexicon_catalog():
    cases = list(_phrase_cases())
    assert len(cases) == len(cat.LEXICON) * len(DATASET_SETS) * 2
    assert assert_conforms(AUTH, cases).resolved > 0


# ---- 2. the catalog, enumerated -------------------------------------------------------------------
def _every_reading():
    for phrase, reading in sorted(cat.LEXICON.items()):
        for logical, columns in reading.literals:
            yield phrase, reading, logical, columns


def test_every_literal_exists_in_its_dataset_and_sits_in_the_right_column():
    seen = 0
    for phrase, _reading, logical, columns in _every_reading():
        for column, values in columns:
            assert values and len(set(values)) == len(values), (phrase, column)
            for value in values:
                seen += 1
                if logical == CONTRACTS:
                    assert column == "estado_contrato" and value in VALID_ESTADOS_CONTRATO, (phrase, value)
                else:
                    valid = (VALID_ESTADOS_APERTURA if column == "estado_de_apertura_del_proceso"
                             else VALID_ESTADOS_PROCEDIMIENTO)
                    assert column in {"estado_de_apertura_del_proceso", "estado_del_procedimiento"}
                    assert value in valid, (phrase, value)
                    if value not in (VALID_ESTADOS_APERTURA & VALID_ESTADOS_PROCEDIMIENTO):
                        assert column == ef.process_state_field(value), (phrase, value)  # one home only
    assert seen > 30


def test_every_exact_literal_agrees_with_the_lexicons_that_declare_it():
    """The table is read FROM ``estados``; this is the independent check against the other two catalogs."""
    for phrase, reading, logical, columns in _every_reading():
        if len(phrase) != 1 or reading.operator != "is":
            continue
        legacy = ef.resolve_estado(phrase[0], logical)
        if legacy is not None and legacy["kind"] == "exact":
            assert {column: list(values) for column, values in columns} == legacy["filters"], (phrase, logical)
    for lemma, literal in ESTADO_CONTRATO_SYNONYMS.items():
        reading = cat.LEXICON.get((lemma,))
        if reading is not None and reading.for_dataset(CONTRACTS) and reading.operator == "is" \
                and reading.state_class != "in_execution":
            assert reading.for_dataset(CONTRACTS) == (("estado_contrato", (literal,)),), lemma


def test_where_the_authority_resolves_it_never_contradicts_the_legacy_resolver():
    compared = 0
    for phrase, reading, logical, columns in _every_reading():
        legacy = ef.resolve_estado(" ".join(phrase), logical)
        if legacy is None:
            continue  # legacy declines (the A2 GAPs): nothing to contradict
        expected = {column: list(values) for column, values in columns}
        if reading.state_class == "open":  # the funnel also carries the opening column (projection adds it)
            expected.pop("estado_de_apertura_del_proceso", None)
        assert expected == legacy["filters"], (phrase, logical, legacy)
        compared += 1
    assert compared >= 20


def test_each_state_exists_only_in_the_datasets_that_have_it():
    for datasets in DATASET_SETS:
        for phrase, reading in sorted(cat.LEXICON.items()):
            spelled = " ".join(phrase)
            verdict = run(f"procesos {spelled}", spelled, reading.operator, POS, datasets)
            present = {name for name in datasets if reading.for_dataset(logical_datasets(ctx(name))[name])}
            if present:
                assert verdict.decision is RESOLVED, (spelled, datasets)
                assert {name for name, *_ in verdict.value.literals} == present
                assert verdict.value.canonical_class == reading.state_class
            else:
                assert reasons(verdict) == ("UNRESOLVED_HOST_DIMENSION:STATE", "STATE_NOT_IN_DATASET"), spelled


def test_values_are_independent_of_the_dataset_they_are_asked_over_for_the_class():
    """PV2 reads ``canonical_class`` before the view is chosen: it must not depend on ``ctx.datasets``."""
    for phrase, reading in sorted(cat.LEXICON.items()):
        spelled = " ".join(phrase)
        classes = {run(f"procesos {spelled}", spelled, reading.operator, POS, datasets).value.canonical_class
                   for datasets in DATASET_SETS
                   if run(f"procesos {spelled}", spelled, reading.operator, POS, datasets).value}
        assert classes <= {reading.state_class}


def test_inflection_and_case_are_read_by_lemma():
    for surface in ("terminado", "terminada", "terminados", "terminadas", "TERMINADOS", "Terminados"):
        assert run(f"contratos {surface}", surface, "is", POS, (C,)).value.canonical_class == "terminated"
    for surface in ("vigente", "vigentes", "activo", "activas", "disponible", "disponibles"):
        assert run(f"procesos {surface}", surface, "family", POS, (P,)).value.canonical_class == "open"
    for surface in ("borrador", "borradores", "en borrador"):
        assert run(f"procesos {surface}", surface, "is", POS, (P,)).value.canonical_class == "draft"
    assert run("procesos en evaluacion", "en evaluacion", "is", POS, (P,)).decision is RESOLVED  # no tilde


# ---- 3. C4: exact vs family -----------------------------------------------------------------------
def test_the_operator_must_say_what_its_evidence_says():
    for phrase, reading in sorted(cat.LEXICON.items()):
        spelled = " ".join(phrase)
        other = "family" if reading.operator == "is" else "is"
        verdict = run(f"procesos {spelled}", spelled, other, POS, (P, C))
        assert reasons(verdict) == ("OPERATOR_EVIDENCE_MISMATCH:STATE", "STATE_OPERATOR_MISMATCH"), spelled
        right = run(f"procesos {spelled}", spelled, reading.operator, POS, (P, C))
        assert right.decision is RESOLVED, spelled


def test_publicados_is_exact_and_never_widens_to_the_funnel():
    verdict = run("procesos publicados", "publicados", "is", POS, (P,))
    assert literals_of(verdict) == {(P, "estado_del_procedimiento"): ("Publicado",)}
    assert run("procesos publicados", "publicados", "family", POS, (P,)).decision is CLARIFY


def test_the_open_family_is_the_funnel_with_its_opening_column():
    funnel = literals_of(run("procesos abiertos", "abiertos", "family", POS, (P,)))
    assert funnel[(P, "estado_de_apertura_del_proceso")] == ("Abierto",)
    assert funnel[(P, "estado_del_procedimiento")] == tuple(
        ef.ESTADO_FAMILIES["oferta_abierta"]["procesos"]["estado_del_procedimiento"])


def test_vigentes_is_dataset_aware_adr_067_and_liveness():
    value = run("vigentes", "vigentes", "family", POS, (P, C)).value
    by_dataset = {name: {col: vals for n, col, vals in value.literals if n == name} for name in (P, C)}
    assert set(by_dataset[C]) == {"estado_contrato"}
    assert by_dataset[C]["estado_contrato"] == ("En ejecución", "Modificado", "Prorrogado")
    assert "estado_de_apertura_del_proceso" in by_dataset[P]


# ---- 4. S3 (ADR-100): the funnel written as two cites ---------------------------------------------
@pytest.mark.parametrize("raw, needle", [
    ("procesos publicados o abiertos de aseo", ["publicados", "abiertos"]),
    ("procesos abiertos o publicados de aseo", ["abiertos", "publicados"]),
    ("procesos publicados o abiertos de aseo", "publicados o abiertos"),
    ("procesos abiertas o publicadas de aseo", ["abiertas", "publicadas"]),
])
def test_s3_two_cites_under_a_bare_o_is_the_funnel_with_the_adr_100_policy(raw, needle):
    verdict = run(raw, needle, "family", POS, (P,))
    assert verdict.decision is RESOLVED and verdict.value.canonical_class == "open"
    assert verdict.policies == (FUNNEL_POLICY,)
    assert literals_of(verdict) == literals_of(run("procesos abiertos", "abiertos", "family", POS, (P,)))


def test_s3_the_gap_decides_between_the_funnel_and_a_clarification_never_the_value():
    base = evidence_from_text("procesos publicados o abiertos", ["publicados", "abiertos"],
                              dimension=STATE, operator="family")
    assert base.window.gaps == (("o",),)
    funnel = AUTH.resolve(base, ctx(P))
    for gap in ((), ("x",), ("y",), ("o", "x"), ("u",), ("ni",), ("o", "o")):
        other = AUTH.resolve(replace(base, window=replace(base.window, gaps=(gap,))), ctx(P))
        assert other.decision is CLARIFY, gap
        assert reasons(other) == ("RESIDUE_UNREAD:STATE", "STATE_COMPOSITION_UNSUPPORTED"), gap
    assert funnel.decision is RESOLVED


@pytest.mark.parametrize("raw, needles", [
    ("procesos publicados y abiertos de aseo", ["publicados", "abiertos"]),
    ("procesos abiertos y cerrados de aseo", ["abiertos", "cerrados"]),
    ("procesos publicados o cerrados de aseo", ["publicados", "cerrados"]),
    ("procesos cancelados o suspendidos", ["cancelados", "suspendidos"]),
    ("procesos publicados o abiertos o cerrados", ["publicados", "abiertos", "cerrados"]),
    ("procesos publicados o abiertos o publicados", ["publicados", "abiertos", ("publicados", 1)]),
])
def test_any_other_composition_is_asked_never_collapsed_to_one_state(raw, needles):
    verdict = run(raw, needles, "family", POS, (P,))
    assert reasons(verdict) == ("RESIDUE_UNREAD:STATE", "STATE_COMPOSITION_UNSUPPORTED")


def test_s3_with_the_literal_operator_negated_or_over_contracts_is_not_executed():
    assert reasons(run("procesos publicados o abiertos", ["publicados", "abiertos"], "is", POS, (P,)))[0] \
        == "OPERATOR_EVIDENCE_MISMATCH:STATE"
    assert reasons(run("procesos publicados o abiertos", ["publicados", "abiertos"], "family", NEG, (P,))) \
        == ("NEGATION_UNSUPPORTED:STATE", "STATE_COMPOSITION_NOT_NEGATABLE")
    assert reasons(run("procesos publicados o abiertos", ["publicados", "abiertos"], "family", POS, (C,))) \
        == ("UNRESOLVED_HOST_DIMENSION:STATE", "STATE_NOT_IN_DATASET")


def test_a_phrase_cited_again_is_one_state_g1():
    verdict = run("abiertos de aseo y abiertos de obra", ["abiertos", ("abiertos", 1)], "family", POS, (P,))
    assert verdict.decision is RESOLVED and verdict.value.canonical_class == "open"


# ---- 5. NEGATIVE (INV-C1.1-EXCL) -------------------------------------------------------------------
def test_no_terminados_keeps_working_with_the_complement_policy():
    verdict = run("contratos no terminados", "no terminados", "is", NEG, (C,))
    assert verdict.decision is RESOLVED
    assert verdict.value == StateValue.of("terminated", {C: {"estado_contrato": ("terminado",)}}, True)
    assert verdict.policies == ("celebrated-state-complement@1",)
    assert decode_value(verdict.value.encode()) == verdict.value


def test_the_negation_may_sit_inside_the_cite_or_outside_it():
    inside = run("aseo que no esté cerrado", "no esté cerrado", "is", NEG, (P, C)).value
    outside = run("aseo no cerrado", "cerrado", "is", NEG, (P, C)).value
    assert inside == outside and inside.excluded


def test_a_negation_inside_a_positive_cite_is_never_executed_as_the_state():
    for needle in ("no terminados", "no terminado", "que no sean terminados"):
        verdict = run("contratos " + needle, needle, "is", POS, (C,))
        assert reasons(verdict) == ("OPERATOR_EVIDENCE_MISMATCH:STATE", "STATE_NEGATION_IN_POSITIVE_CITE")
    assert reasons(run("contratos no no terminados", "no no terminados", "is", NEG, (C,)))[0] \
        == "RESIDUE_UNREAD:STATE"


def test_negating_a_state_that_filters_two_columns_is_refused_not_guessed():
    for needle, operator in (("no estén abiertos", "family"), ("no vigentes", "family")):
        verdict = run("procesos " + needle, needle, operator, NEG, (P,))
        assert reasons(verdict) == ("NEGATION_UNSUPPORTED:STATE", "STATE_NEGATION_MULTI_COLUMN"), needle


def test_every_single_column_state_can_be_excluded_per_dataset():
    for phrase, reading in sorted(cat.LEXICON.items()):
        spelled = " ".join(phrase)
        verdict = run(f"procesos {spelled}", spelled, reading.operator, NEG, (P, C))
        multi = any(len(columns) > 1 for _logical, columns in reading.literals)
        if multi:
            assert verdict.reason == "NEGATION_UNSUPPORTED:STATE", spelled
        elif verdict.decision is RESOLVED:
            assert verdict.value.excluded and verdict.policies, spelled
            positive = run(f"procesos {spelled}", spelled, reading.operator, POS, (P, C)).value
            assert verdict.value != positive and verdict.value.literals == positive.literals


# ---- 6. residue, unknown and unrepresentable states -------------------------------------------------
@pytest.mark.parametrize("needle", ["terminados anticipadamente", "terminados parcialmente",
                                    "recientemente terminados", "terminados y"])
def test_a_state_word_plus_anything_else_is_residue_a2_state_036(needle):
    verdict = run("contratos " + needle, needle, "is", POS, (C,))
    assert reasons(verdict) == ("RESIDUE_UNREAD:STATE", "STATE_QUOTE_NOT_FULLY_READ")


@pytest.mark.parametrize("needle", ["liquidados", "firmados", "celebrados", "aprobados", "finalizados",
                                    "desiertos", "modificados", "en curso"])
def test_a_state_secop_has_no_literal_for_is_asked_not_approximated(needle):
    verdict = run("contratos " + needle, needle, "is", POS, (P, C))
    assert reasons(verdict) == ("UNRESOLVED_HOST_DIMENSION:STATE", "STATE_NOT_REPRESENTABLE")


def test_an_unknown_phrase_is_unresolved():
    assert reasons(run("contratos de aseo", "de aseo", "is", POS, (C,))) \
        == ("UNRESOLVED_HOST_DIMENSION:STATE", "STATE_PHRASE_NOT_RECOGNIZED")


_WORD = st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=9)


@settings(max_examples=150, deadline=None)
@given(junk=_WORD, before=st.booleans(), index=st.integers(0, len(cat.LEXICON) - 1))
def test_property_a_junk_word_next_to_a_state_phrase_never_resolves(junk, before, index):
    phrase, reading = sorted(cat.LEXICON.items())[index]
    spelled = " ".join(phrase)
    needle = f"{junk} {spelled}" if before else f"{spelled} {junk}"
    if cat.contains_state_phrase(tuple(needle.split())) and cat.lookup(tuple(needle.split())) is not None:
        return  # the junk word completed another phrase of the lexicon: not junk
    scaffold = cat.NEGATION_TOKENS | cat.COPULA_TOKENS
    if before and junk in scaffold:
        return  # «no», «que», «sea»… are the scaffold, legitimately read
    verdict = run(f"procesos {needle}", needle, reading.operator, POS, (P, C))
    assert verdict.decision is not RESOLVED, needle


# ---- 7. D6: the participle that anchors a date is not a state -------------------------------------
@pytest.mark.parametrize("raw, withdrawn", [
    ("procesos publicados hace dos semanas", True),
    ("procesos publicados hoy", True),
    ("procesos publicados en marzo", True),
    ("procesos publicados en 2024", True),
    ("procesos publicados en el 2024", True),
    ("procesos publicados desde enero", True),
    ("procesos publicados el mes pasado", True),
    ("procesos publicados este mes", True),
    ("procesos publicados el 15 de marzo", True),
    ("procesos publicados en Cali", False),
    ("procesos publicados en Cali en 2024", False),
    ("procesos publicados de aseo", False),
    ("procesos publicados", False),
])
def test_d6_publicados_that_anchors_a_date_is_withdrawn_by_the_context(raw, withdrawn):
    verdict = run(raw, "publicados", "is", POS, (P,))
    if withdrawn:
        assert reasons(verdict) == ("UNCITED_QUALIFIER:STATE", "STATE_PARTICIPLE_ANCHORS_DATE"), raw
        stripped = AUTH.resolve(replace(evidence_from_text(raw, "publicados", dimension=STATE, operator="is"),
                                        window=evidence_from_text(raw, "publicados", dimension=STATE,
                                                                  operator="is").window.stripped()), ctx(P))
        assert stripped.decision is RESOLVED  # INV-C0-7: the context only withdraws
    else:
        assert verdict.decision is RESOLVED, raw


def test_d6_does_not_touch_the_states_that_are_real_states_next_to_a_date():
    for raw, needle in (("contratos terminados en 2023", "terminados"), ("procesos cerrados en 2023", "cerrados"),
                        ("procesos cancelados en marzo", "cancelados")):
        assert run(raw, needle, "is", POS, (P, C)).decision is RESOLVED, raw


# ---- 8. datasets of the context ---------------------------------------------------------------------
def test_literals_are_keyed_by_the_context_dataset_ids_inv_c1_1_ds():
    value = run("terminados", "terminados", "is", POS, (P, C)).value
    assert {name for name, *_ in value.literals} == {C}
    named = AuthorityContext(REFERENCE_INSTANT, frozenset({"procesos", "contratos"}))
    verdict = AUTH.resolve(evidence_from_text("vigentes", "vigentes", dimension=STATE, operator="family"), named)
    assert {name for name, *_ in verdict.value.literals} == {"procesos", "contratos"}


def test_an_unknown_dataset_is_never_given_literals():
    foreign = AuthorityContext(REFERENCE_INSTANT, frozenset({"dataset-a"}))
    verdict = AUTH.resolve(evidence_from_text("abiertos", "abiertos", dimension=STATE, operator="family"), foreign)
    assert reasons(verdict) == ("UNRESOLVED_HOST_DIMENSION:STATE", "STATE_DATASET_UNKNOWN")
    mixed = AuthorityContext(REFERENCE_INSTANT, frozenset({"dataset-a", P}))
    value = AUTH.resolve(evidence_from_text("abiertos", "abiertos", dimension=STATE, operator="family"), mixed).value
    assert {name for name, *_ in value.literals} == {P}


def test_declared_scope_a_foreign_dimension_or_operator_never_resolves():
    evidence = evidence_from_text("procesos abiertos", "abiertos", dimension=SemanticDimension.TOPIC,
                                  operator="contains")
    verdict = AUTH.resolve(evidence, ctx(P))
    assert verdict.decision is CLARIFY and verdict.reason.endswith(":TOPIC")
    assert AUTH.resolve(replace(run_evidence("abiertos"), operator="greater_than"), ctx(P)).decision is CLARIFY


def run_evidence(needle):
    return evidence_from_text(f"procesos {needle}", needle, dimension=STATE, operator="family")


def test_the_value_is_a_pure_function_of_the_cited_segments():
    """INV-C0-7: context may only withdraw; the kit proves it per case, this pins the S3 and D6 corners."""
    for left, right in product(((), ("procesos",), ("los", "procesos")), ((), ("de", "aseo"))):
        base = run_evidence("abiertos")
        shaped = replace(base, window=replace(base.window, left=left, right=right))
        assert AUTH.resolve(shaped, ctx(P)) == AUTH.resolve(base, ctx(P))


# ---- 9. Gold slice (synthetic) with the oracle IR -----------------------------------------------------
GOLD_STATE_GAPS = {
    ("P127", "c2"): "CLARIFY:NEGATION_UNSUPPORTED:STATE:STATE_NEGATION_MULTI_COLUMN",
    ("P134", "c1"): "CLARIFY:UNRESOLVED_HOST_DIMENSION:STATE:STATE_NOT_REPRESENTABLE",
    ("X016", "c3"): "CLARIFY:UNRESOLVED_HOST_DIMENSION:STATE:STATE_NOT_REPRESENTABLE",
    ("N018", "c1"): "CLARIFY:OPERATOR_EVIDENCE_MISMATCH:STATE:STATE_OPERATOR_MISMATCH",
    ("N090", "c1"): "CLARIFY:OPERATOR_EVIDENCE_MISMATCH:STATE:STATE_OPERATOR_MISMATCH",
    ("X047", "c1"): "CLARIFY:UNRESOLVED_HOST_DIMENSION:STATE:STATE_NOT_IN_DATASET",
}
_GOLD_CLASS = {"open": "open", "published": "published", "closed": "closed", "awarded": "awarded",
               "terminated": "terminated", "in_execution": "in_execution", "cancelled": "cancelled",
               "other:published_or_open": "open"}


def test_gold_state_slice_matches_the_oracle_except_the_declared_gaps():
    rows = list(conditions("STATE"))
    assert len(rows) == 40
    gaps, resolved = {}, 0
    for gold_id, text, universe, condition in rows:
        evidence, context = evidence_and_context(text, universe, condition, STATE)
        verdict = AUTH.resolve(evidence, context)
        if verdict.decision is RESOLVED:
            resolved += 1
            assert verdict.value.canonical_class == _GOLD_CLASS[condition["value_hint"]], (gold_id, condition)
            assert verdict.value.excluded == (condition["polarity"] == "NEGATIVE"), gold_id
            if condition["value_hint"] == "other:published_or_open":
                assert verdict.policies == (FUNNEL_POLICY,)
        else:
            gaps[(gold_id, condition["id"])] = outcome(verdict)
    assert gaps == GOLD_STATE_GAPS
    assert resolved == len(rows) - len(GOLD_STATE_GAPS)
