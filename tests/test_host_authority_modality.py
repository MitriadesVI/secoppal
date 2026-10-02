"""MODALITY host authority (M1-D-STATE/MOD): the C1.1 kit, the catalog property and the A2 probes."""
from __future__ import annotations

from dataclasses import replace
from itertools import permutations

import pytest
from hypothesis import example, given, settings, strategies as st

from app.core.foundation_authority_contract import AuthorityContext, ModalityValue, decode_value
from app.core.foundation_projection import MODALITY_FAMILIES
from app.core.host_authorities import modality as mod
from app.core.host_authorities.modality import ModalityAuthority
from app.core.interpretation_contract import Polarity, ResolutionDecision
from app.core.modality_lexicon import load_lexicon
from app.core.modality_models import load_catalog
from app.core.modality_resolution_models import MatchStrength
from app.core.modality_resolver import resolve as resolve_modality
from app.core.semantic_dimensions import SemanticDimension
from tests.foundation_authority_kit import (
    REFERENCE_INSTANT, ConformanceCase, assert_conforms, case, evidence_from_text,
)
from tests.m1_state_mod_gold import conditions, evidence_and_context, outcome

P, C = "p6dx-8zbt", "jbjy-vk9h"
AUTH = ModalityAuthority()
POS, NEG = Polarity.POSITIVE, Polarity.NEGATIVE
MODALITY = SemanticDimension.MODALITY
RESOLVED, CLARIFY, BLOCK = ResolutionDecision.RESOLVED, ResolutionDecision.CLARIFY, ResolutionDecision.BLOCK
CATALOG, LEXICON = load_catalog(), load_lexicon(catalog=load_catalog())


def ctx(*datasets: str) -> AuthorityContext:
    return AuthorityContext(REFERENCE_INSTANT, frozenset(datasets or (P, C)))


def run(raw, needle, operator="family", polarity=POS, datasets=(P, C)):
    evidence = evidence_from_text(raw, needle, dimension=MODALITY, operator=operator, polarity=polarity)
    return AUTH.resolve(evidence, ctx(*datasets))


def reasons(verdict):
    return verdict.reason, verdict.detail


# ---- 1. the C1.1 conformance kit --------------------------------------------------------------------
def _cases() -> list[ConformanceCase]:
    specs = [
        ("procesos por licitación pública", "licitación pública", "family", POS),
        ("licitaciones públicas de vigilancia", "licitaciones públicas", "family", POS),
        ("contratos por contratación directa", "contratación directa", "family", POS),
        ("procesos de vigilancia que no sean de contratación directa", "contratación directa", "family", NEG),
        ("procesos de vigilancia que no sean de contratación directa", "no sean de contratación directa",
         "family", NEG),
        ("procesos de vigilancia sin mínima cuantía", "mínima cuantía", "family", NEG),
        ("procesos por mínima cuantía", "mínima cuantía", "family", POS),
        ("procesos por contratación directa con ofertas", "contratación directa con ofertas", "is", POS),
        ("licitaciones de obra", "licitaciones", "family", POS),                  # O6: filters nothing
        ("procesos que no sean licitaciones", "licitaciones", "family", NEG),     # not negatable
        ("procesos por no por contratación directa", "no por contratación directa", "family", POS),
        ("procesos por directa o licitación", "directa o licitación", "family", POS),
        ("procesos por licitación pública de obra", "licitación pública de obra", "family", POS),
        ("procesos por selección abreviada", "selección abreviada", "family", POS),
        ("procesos por subasta", "subasta", "family", POS),
        ("procesos por convenio", "convenio", "family", POS),
        ("contratación directa", "contratación directa", "is", POS),
    ]
    return [case(raw, needle, ctx=ctx(), dimension=MODALITY, operator=operator, polarity=polarity)
            for raw, needle, operator, polarity in specs]


def test_authority_conforms_to_the_c1_1_kit():
    report = assert_conforms(AUTH, _cases())
    assert report.cases == len(_cases()) and report.resolved >= 8


# ---- 2. the catalog of modalities, enumerated from the catalog ------------------------------------------
def _phrases():
    """``(rule, phrase)`` for every phrase of every rule that targets a family or a procedure."""
    for rule in LEXICON.rules:
        if rule.target_kind in {"retrieval_family", "procedure"}:
            for phrase in rule.phrases:
                yield rule, phrase


def _expected(rule, datasets):
    """The procedures the CATALOG says the rule reaches in these datasets, searchable ones only."""
    logical = {"procesos" if name == P else "contratos" for name in datasets}
    dataset = next(iter(logical)) if len(logical) == 1 else None
    if rule.target_kind == "procedure":
        ids = [rule.target_id]
    else:
        ids = [p.procedure_id for p in CATALOG.retrieval_family(rule.target_id)
               if p.user_searchable and p.parent_expansion_policy != "never"
               and (p.parent_expansion_policy == "family_default" or rule.explicit_intent)]
    return tuple(sorted(i for i in ids if dataset is None or CATALOG.by_id(i).applies_to(dataset)))


def test_every_phrase_of_the_catalog_is_resolved_or_refused_for_a_closed_reason():
    seen = {"resolved": 0, "o6": 0, "block": 0, "clarify": 0}
    for (rule, phrase), datasets in ((rp, ds) for rp in _phrases() for ds in ((P,), (C,), (P, C))):
        for operator in ("family", "is"):
            verdict = run(f"procesos por {phrase}", phrase, operator, POS, datasets)
            expected = _expected(rule, datasets)
            if verdict.decision is RESOLVED:
                value = verdict.value
                if value.family_policy == mod.O6_POLICY:
                    assert rule.match_strength == "colloquial" and not value.procedure_ids, phrase
                    seen["o6"] += 1
                    continue
                assert rule.match_strength in {"exact_legal", "exact_common"}, phrase
                assert value.procedure_ids == expected, (phrase, datasets, value)
                assert not value.excluded
                for identifier in value.procedure_ids:  # never a platform artifact, never out of its dataset
                    assert CATALOG.by_id(identifier).user_searchable, (phrase, identifier)
                seen["resolved"] += 1
            elif verdict.decision is BLOCK:
                assert verdict.detail == "UNACCREDITED_MODALITY_FAMILY" and len(expected) > 1, phrase
                seen["block"] += 1
            else:
                assert verdict.reason.split(":")[0] in {
                    "AMBIGUOUS_HOST_DIMENSION", "UNRESOLVED_HOST_DIMENSION", "OPERATOR_EVIDENCE_MISMATCH",
                }, (phrase, reasons(verdict))
                seen["clarify"] += 1
    assert all(seen.values()), seen


def test_every_phrase_of_the_catalog_conforms_to_the_kit_in_both_polarities():
    cases = [ConformanceCase(evidence_from_text(f"procesos por {phrase}", phrase, dimension=MODALITY,
                                                operator="family", polarity=polarity), ctx(*datasets))
             for _rule, phrase in _phrases() for polarity in (POS, NEG) for datasets in ((P,), (P, C))]
    assert assert_conforms(AUTH, cases).resolved > 0


def test_every_exact_phrase_negated_is_the_same_procedures_excluded():
    checked = 0
    for rule, phrase in _phrases():
        positive = run(f"procesos por {phrase}", phrase, "family", POS)
        negative = run(f"procesos por {phrase}", phrase, "family", NEG)
        if positive.decision is not RESOLVED or positive.value.family_policy == mod.O6_POLICY:
            continue
        assert negative.decision is RESOLVED and negative.value.excluded, phrase
        assert negative.value.procedure_ids == positive.value.procedure_ids
        assert mod.EXCLUSION_POLICY in negative.policies and negative.policies[:-1] == positive.policies
        checked += 1
    assert checked >= 15


def test_every_phrase_negated_inside_the_cite_obeys_the_polarity_guard():
    """«no <modalidad>», «sin <modalidad>», «excepto <modalidad>»: POSITIVE is refused, NEGATIVE is excluded."""
    checked = 0
    for rule, phrase in _phrases():
        if rule.match_strength not in {"exact_legal", "exact_common"}:
            continue
        for marker in ("no", "sin", "excepto", "que no sean de"):
            quote = f"{marker} {phrase}"
            positive = run(f"procesos {quote}", quote, "family", POS)
            negative = run(f"procesos {quote}", quote, "family", NEG)
            assert positive.decision is not RESOLVED, quote
            if positive.reason == "OPERATOR_EVIDENCE_MISMATCH:MODALITY":
                assert positive.detail == "MODALITY_NEGATION_IN_POSITIVE_CITE"
                if negative.decision is RESOLVED:
                    assert negative.value.excluded, quote
                else:  # the same family the positive reading would refuse as unaccredited
                    assert negative.decision is BLOCK and negative.detail == "UNACCREDITED_MODALITY_FAMILY", quote
                checked += 1
    assert checked >= 40


def test_no_pair_of_modalities_in_one_cite_is_ever_resolved():
    """«A o B» / «A y B»: the old host kept the first match and dropped the other one silently."""
    phrases = sorted({phrase for _rule, phrase in _phrases()})
    pairs = list(permutations(phrases[:14], 2))
    for first, second in pairs:
        for connector in ("o", "y"):
            quote = f"{first} {connector} {second}"
            verdict = run(f"procesos por {quote}", quote, "family", POS)
            if verdict.decision is RESOLVED:  # only when both words name the SAME procedures (idempotent)
                alone = [run(f"procesos por {phrase}", phrase, "family", POS) for phrase in (first, second)]
                assert all(one.decision is RESOLVED and one.value == verdict.value for one in alone), quote


# ---- 3. A2 probes: the polarity guard and the exact reading ------------------------------------------------
@pytest.mark.parametrize("quote", ["no por contratación directa", "sin contratación directa",
                                   "excepto licitación pública", "que no sean de contratación directa"])
def test_a2_modality_031_032_a_cite_with_a_negation_and_polarity_positive_is_a_mismatch(quote):
    verdict = run(f"procesos {quote}", quote, "family", POS)
    assert reasons(verdict) == ("OPERATOR_EVIDENCE_MISMATCH:MODALITY", "MODALITY_NEGATION_IN_POSITIVE_CITE")


def test_a2_modality_033_directa_o_licitacion_is_residue_not_just_the_licitacion():
    verdict = run("procesos por directa o licitación", "directa o licitación", "family", POS)
    assert reasons(verdict) == ("RESIDUE_UNREAD:MODALITY", "MODALITY_QUOTE_NOT_FULLY_READ")
    both = run("procesos por licitación pública o contratación directa", "licitación pública o contratación directa")
    assert both.decision is CLARIFY


@pytest.mark.parametrize("needle, expected", [("licitación pública de obra", "MODALITY_QUOTE_NOT_FULLY_READ"),
                                              ("contratación directa urgente", "MODALITY_QUOTE_NOT_FULLY_READ")])
def test_a_qualifier_inside_the_cite_is_residue(needle, expected):
    assert reasons(run(f"procesos por {needle}", needle)) == ("RESIDUE_UNREAD:MODALITY", expected)


def test_two_different_modalities_cited_for_one_condition_are_asked():
    verdict = run("procesos por licitación pública o contratación directa",
                  ["licitación pública", "contratación directa"])
    assert reasons(verdict) == ("RESIDUE_UNREAD:MODALITY", "MODALITY_COMPOSITION_UNSUPPORTED")


def test_a_modality_cited_again_is_one_modality_g1():
    verdict = run("contratación directa y de nuevo contratación directa",
                  ["contratación directa", ("contratación directa", 1)])
    assert verdict.decision is RESOLVED and verdict.value.family_policy == "modality-direct-family@1"


def test_two_modalities_with_no_connector_are_a_composition_not_the_first_one():
    quote = "licitación pública contratación directa"
    assert reasons(run(f"procesos por {quote}", quote)) == ("RESIDUE_UNREAD:MODALITY",
                                                           "MODALITY_COMPOSITION_UNSUPPORTED")


@pytest.mark.parametrize("quote", ["licitación pública no", "mínima cuantía ni", "licitación pública sin"])
def test_a_negation_the_resolver_did_not_bind_is_not_a_negative_cite(quote):
    """The word is in the cite but excludes nothing: NEGATIVE cannot be honoured, it is not guessed."""
    verdict = run(f"procesos por {quote}", quote, "family", NEG)
    assert reasons(verdict) == ("RESIDUE_UNREAD:MODALITY", "MODALITY_NEGATION_NOT_BOUND")
    assert run(f"procesos por {quote}", quote, "family", POS).reason == "OPERATOR_EVIDENCE_MISMATCH:MODALITY"


@pytest.mark.parametrize("strength", [MatchStrength.CONTEXTUAL, MatchStrength.LEGACY_RAW])
def test_only_exact_strengths_execute_a_weaker_match_is_asked(strength, monkeypatch):
    """The catalog's weaker rules all end as resolver issues today; the gate protects the day one does not."""
    real = resolve_modality("licitación pública", dataset=None)
    weak = replace(real, includes=(replace(real.includes[0], strength=strength),))
    monkeypatch.setattr(mod, "resolve_modality", lambda text, dataset=None: weak)
    assert reasons(run("procesos por licitación pública", "licitación pública")) \
        == ("UNRESOLVED_HOST_DIMENSION:MODALITY", "MODALITY_STRENGTH_NOT_EXECUTABLE")


# ---- 4. modality-o6@1: «licitación(es)» colloquial never filters ----------------------------------------------
@pytest.mark.parametrize("word", ["licitación", "licitaciones", "proceso licitatorio", "procesos licitatorios",
                                  "Licitaciones", "LICITACIÓN"])
def test_o6_a_colloquial_licitacion_resolves_to_a_value_that_filters_nothing(word):
    verdict = run(f"quiero {word} de vigilancia", word, "family", POS)
    assert verdict.decision is RESOLVED
    assert verdict.value == ModalityValue((), "modality-o6@1", False)
    assert verdict.policies == ("modality-o6@1",)
    assert verdict.receipt.rules == ("lic_colloquial",)
    assert decode_value(verdict.value.encode()) == verdict.value


def test_o6_the_legal_name_is_a_real_filter_the_colloquial_word_is_not():
    legal = run("licitaciones públicas de vigilancia", "licitaciones públicas").value
    assert legal.procedure_ids == ("licitacion_publica", "licitacion_publica_obra")
    assert legal.family_policy == "modality-lp-family@1"
    assert run("licitaciones de vigilancia", "licitaciones").value.procedure_ids == ()


def test_o6_holds_in_every_dataset_context_and_for_both_operators():
    for datasets in ((P,), (C,), (P, C)):
        for operator in ("family", "is"):
            assert run("licitaciones", "licitaciones", operator, POS, datasets).value.family_policy \
                == "modality-o6@1"


def test_o6_a_negated_colloquial_word_is_not_ignored_and_not_guessed():
    verdict = run("procesos que no sean licitaciones", "licitaciones", "family", NEG)
    assert reasons(verdict) == ("NEGATION_UNSUPPORTED:MODALITY", "MODALITY_COLLOQUIAL_NOT_NEGATABLE")
    inside = run("procesos que no sean licitaciones", "no sean licitaciones", "family", POS)
    assert inside.decision is not RESOLVED


# ---- 5. families, literals and accreditation -------------------------------------------------------------------
def test_accredited_families_are_pinned_to_the_projection_table():
    projection = {policy.rsplit("@", 1)[0]: ids for policy, ids in
                  ((policy, ids) for _family, (policy, ids) in mod.ACCREDITED_FAMILIES.items())}
    assert projection == MODALITY_FAMILIES


def test_a_family_with_several_procedures_executes_only_when_accredited():
    assert run("procesos por selección abreviada", "selección abreviada").decision is BLOCK
    assert reasons(run("procesos por selección abreviada", "selección abreviada")) \
        == ("UNRESOLVED_HOST_DIMENSION:MODALITY", "UNACCREDITED_MODALITY_FAMILY")
    assert run("procesos por concurso de méritos", "concurso de méritos").decision is BLOCK


def test_a_single_procedure_is_exact_for_both_operators():
    for operator in ("family", "is"):
        value = run("procesos por mínima cuantía", "mínima cuantía", operator).value
        assert value == ModalityValue(("minima_cuantia",), None, False)


def test_is_over_a_cite_that_names_a_family_is_a_mismatch_not_a_family_filter():
    verdict = run("procesos por contratación directa", "contratación directa", "is")
    assert reasons(verdict) == ("OPERATOR_EVIDENCE_MISMATCH:MODALITY", "MODALITY_LITERAL_NEEDS_ONE_PROCEDURE")
    exact = run("procesos por contratación directa con ofertas", "contratación directa con ofertas", "is")
    assert exact.value.procedure_ids == ("contratacion_directa_con_ofertas",)


def test_ambiguity_and_unknown_are_asked():
    assert reasons(run("subasta", "subasta")) == ("AMBIGUOUS_HOST_DIMENSION:MODALITY",
                                                  "BARE_SUBASTA_MULTIPLE_LEGAL_MEANINGS")
    assert reasons(run("convenio", "convenio")) == ("UNRESOLVED_HOST_DIMENSION:MODALITY", "MODALITY_NO_MATCH")


def test_a_modality_that_does_not_exist_in_the_context_dataset_is_asked():
    rfi = "solicitud de información a proveedores"
    only_contracts = run(f"procesos {rfi}", rfi, "family", POS, (C,))
    assert only_contracts.decision is CLARIFY
    assert only_contracts.reason == "UNRESOLVED_HOST_DIMENSION:MODALITY"


def test_the_resolver_is_wrapped_not_modified():
    """The wrapper reads the same resolution the resolver gives for the same text and dataset."""
    for phrase in ("licitación pública", "contratación directa", "mínima cuantía"):
        direct = resolve_modality(phrase, dataset="procesos")
        wrapped = run(f"x {phrase}", phrase, "family", POS, (P,))
        assert wrapped.receipt.rules == (direct.includes[0].rule_id,)
        assert set(wrapped.value.procedure_ids) == set(direct.includes[0].procedure_ids)


# ---- 6. scope and context -------------------------------------------------------------------------------------
def test_declared_scope_a_foreign_dimension_or_operator_never_resolves():
    topic = evidence_from_text("procesos por licitación pública", "licitación pública",
                               dimension=SemanticDimension.TOPIC, operator="contains")
    assert AUTH.resolve(topic, ctx()).decision is CLARIFY
    base = evidence_from_text("procesos por licitación pública", "licitación pública",
                              dimension=MODALITY, operator="family")
    assert AUTH.resolve(replace(base, operator="greater_than"), ctx()).decision is CLARIFY


def test_the_context_around_the_cite_never_changes_the_value():
    base = evidence_from_text("procesos que no sean de contratación directa", "contratación directa",
                              dimension=MODALITY, operator="family", polarity=POS)
    for left, right in (((), ()), (("no", "sean", "de"), ()), (("sin",), ("ni", "otras"))):
        shaped = replace(base, window=replace(base.window, left=left, right=right))
        assert AUTH.resolve(shaped, ctx()) == AUTH.resolve(base, ctx())


_WORD = st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=3, max_size=9)


# derandomize + no database: the run never depends on ``.hypothesis`` nor on the seed of the day.  The
# scaffold words (``mod.SCAFFOLD_TOKENS``) that used to slip through are pinned as examples, and
# ``test_a_scaffold_word_after_the_modality_is_residue`` enumerates them against every phrase.
@settings(max_examples=120, deadline=None, derandomize=True, database=None)
@given(junk=_WORD, index=st.integers(0, 9))
@example(junk="sea", index=0)
@example(junk="son", index=1)
@example(junk="las", index=2)
@example(junk="del", index=3)
@example(junk="que", index=4)
@example(junk="por", index=5)
@example(junk="los", index=6)
@example(junk="una", index=7)
def test_property_an_unread_word_in_the_cite_never_resolves(junk, index):
    phrases = sorted({phrase for rule, phrase in _phrases() if rule.match_strength != "colloquial"})
    phrase = phrases[index % len(phrases)]
    quote = f"{phrase} {junk}"
    verdict = run(f"procesos por {quote}", quote, "family", POS)
    if verdict.decision is RESOLVED:  # only when the junk word is itself part of a catalog phrase
        assert any(quote.lower() == p.lower() or junk in p.lower() for p in phrases), quote


@pytest.mark.parametrize("raw, needle", [
    ("procesos por acuerdo marco sea", "acuerdo marco sea"),
    ("procesos por contratación directa son", "contratación directa son"),
    ("procesos por licitación pública las", "licitación pública las"),
])
def test_the_hypothesis_reproducers_are_residue(raw, needle):
    assert reasons(run(raw, needle)) == ("RESIDUE_UNREAD:MODALITY", "MODALITY_QUOTE_NOT_FULLY_READ")


def test_a_scaffold_word_after_the_modality_is_residue():
    """The scaffold precedes the modality; after it, a scaffold word is as unread as any other word."""
    phrases = sorted({phrase for rule, phrase in _phrases() if rule.match_strength != "colloquial"})
    assert phrases and mod.SCAFFOLD_TOKENS
    for phrase in phrases:
        for word in sorted(mod.SCAFFOLD_TOKENS):
            for polarity in (POS, NEG):
                quote = f"{phrase} {word}"
                verdict = run(f"procesos por {quote}", quote, "family", polarity)
                assert verdict.decision is not RESOLVED, (quote, polarity)
        # ... while the same scaffold BEFORE the phrase is still read
        assert reasons(run(f"procesos por {phrase}", f"por {phrase}", "family", POS)) \
            == reasons(run(f"procesos por {phrase}", phrase, "family", POS)), phrase


# ---- 7. Gold slice (synthetic) with the oracle IR ---------------------------------------------------------------
GOLD_IDS = {
    "licitación pública": ("licitacion_publica", "licitacion_publica_obra"),
    "contratación directa": ("contratacion_directa", "contratacion_directa_con_ofertas"),
    "mínima cuantía": ("minima_cuantia",),
}
GOLD_MODALITY_GAPS = {
    ("P082", "c2"): "BLOCK:UNRESOLVED_HOST_DIMENSION:MODALITY:UNACCREDITED_MODALITY_FAMILY",
    ("X081", "c1"): "BLOCK:UNRESOLVED_HOST_DIMENSION:MODALITY:UNACCREDITED_MODALITY_FAMILY",
}


def test_gold_modality_slice_matches_the_oracle_except_the_declared_gaps():
    rows = list(conditions("MODALITY"))
    assert len(rows) == 29
    gaps, polarities = {}, set()
    for gold_id, text, universe, condition in rows:
        evidence, context = evidence_and_context(text, universe, condition, MODALITY)
        verdict = AUTH.resolve(evidence, context)
        if verdict.decision is RESOLVED:
            hint = condition["value_hint"].lower().replace("contratacion", "contratación")
            assert verdict.value.procedure_ids == GOLD_IDS[hint.replace("licitacion", "licitación")], gold_id
            assert verdict.value.excluded == (condition["polarity"] == "NEGATIVE"), gold_id
            polarities.add(condition["polarity"])
        else:
            gaps[(gold_id, condition["id"])] = outcome(verdict)
    assert gaps == GOLD_MODALITY_GAPS
    assert polarities == {"POSITIVE", "NEGATIVE"}
