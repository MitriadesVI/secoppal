"""Structural and adversarial contract for Supplier Request Authority V1."""
from __future__ import annotations

import inspect
import unicodedata

import pytest
from hypothesis import given, settings, strategies as st

from app.core.entity_profile.aspects.suppliers import SUPPLIER_LIMIT
from app.core.entity_profile.supplier_ranking_request import MAX_SUPPLIER_LIMIT
from app.core.capability_binding.supplier_request_authority import assess_supplier_request
from app.core.capability_binding.supplier_request_lexing import TokenKind, tokenize
from app.core.capability_binding.supplier_request_types import (
    Authorization,
    CardinalityRelation,
    CardinalityResidueReason,
    Polarity,
    SupplierNoun,
)


def _evidence(text: str):
    return assess_supplier_request(text)


@pytest.mark.parametrize("text", [
    "proveedores",
    "contratistas de la alcaldía de Galapa",
    "los 5 proveedores",
    "top 5 proveedores",
    "exactamente cinco contratistas",
    "los cinco principales proveedores",
    "top proveedores",
    "proveedores con más contratos en 2025",
])
def test_incident_and_historical_positive_surfaces_are_executable(text):
    evidence = _evidence(text)
    assert evidence.authorization is Authorization.EXECUTABLE
    assert evidence.polarity is Polarity.POSITIVE
    assert evidence.complete is True


@pytest.mark.parametrize("text,relation,values", [
    ("más de 5 proveedores", CardinalityRelation.LOWER, (5,)),
    ("al menos 5 proveedores", CardinalityRelation.LOWER, (5,)),
    ("como mínimo 5 proveedores", CardinalityRelation.LOWER, (5,)),
    ("mínimo 5 proveedores", CardinalityRelation.LOWER, (5,)),
    ("un mínimo de 5 proveedores", CardinalityRelation.LOWER, (5,)),
    ("5 proveedores o más", CardinalityRelation.LOWER, (5,)),
    ("5 proveedores, por lo menos", CardinalityRelation.LOWER, (5,)),
    ("5 proveedores como mínimo", CardinalityRelation.LOWER, (5,)),
    ("menos de 5 proveedores", CardinalityRelation.UPPER, (5,)),
    ("hasta 5 proveedores", CardinalityRelation.UPPER, (5,)),
    ("a lo sumo 5 proveedores", CardinalityRelation.UPPER, (5,)),
    ("un máximo de 5 proveedores", CardinalityRelation.UPPER, (5,)),
    ("5 proveedores o menos", CardinalityRelation.UPPER, (5,)),
    ("entre 5 y 10 proveedores", CardinalityRelation.RANGE, (5, 10)),
    ("de 5 a 10 proveedores", CardinalityRelation.RANGE, (5, 10)),
    ("entre 5 y 5 proveedores", CardinalityRelation.RANGE, (5, 5)),
])
def test_closed_cardinality_relations_preserve_operator(text, relation, values):
    evidence = _evidence(text)
    assert evidence.cardinality.relation is relation
    assert evidence.cardinality.values == values
    assert evidence.authorization is Authorization.NOT_ACCREDITED
    assert evidence.cardinality.operator_spans
    assert evidence.cardinality.quantity_spans


@pytest.mark.parametrize("text", [
    "26 proveedores", "30 proveedores", "100 proveedores", "top 100 proveedores",
])
def test_exact_beyond_the_served_range_never_authorizes(text):
    evidence = _evidence(text)
    assert evidence.cardinality.relation is CardinalityRelation.EXACT
    assert evidence.cardinality.values != (SUPPLIER_LIMIT,)
    assert evidence.authorization is Authorization.NOT_ACCREDITED


def test_the_served_exact_range_is_executable_and_bounded():
    # V1.1 sirve un exacto dentro de 1..MAX; el default V1 sigue siendo 5, y el
    # borde de arriba no se trunca: fuera del rango se aclara.
    assert SUPPLIER_LIMIT == 5
    assert MAX_SUPPLIER_LIMIT == 25
    for servable in (1, SUPPLIER_LIMIT, MAX_SUPPLIER_LIMIT):
        assert _evidence(f"{servable} proveedores").authorization is Authorization.EXECUTABLE
    for outside in (0, MAX_SUPPLIER_LIMIT + 1, 100):
        assert _evidence(f"{outside} proveedores").authorization is Authorization.NOT_ACCREDITED


@pytest.mark.parametrize("text,polarity", [
    ("sin proveedores", Polarity.NEGATED),
    ("ningún proveedor", Polarity.NEGATED),
    ("no quiero ver a los proveedores", Polarity.NEGATED),
    ("ni proveedores ni contratistas", Polarity.NEGATED),
    ("excepto los proveedores", Polarity.NEGATED),
    ("proveedores no", Polarity.UNKNOWN),
    ("sin no proveedores", Polarity.UNKNOWN),
    ("proveedores sin", Polarity.UNKNOWN),
    ("proveedores y sin contratistas", Polarity.MIXED),
])
def test_polarity_is_composed_over_every_mention(text, polarity):
    evidence = _evidence(text)
    assert evidence.polarity is polarity
    assert evidence.authorization is Authorization.NOT_ACCREDITED
    if text == "sin no proveedores":
        assert evidence.complete is False


def test_each_ni_binds_the_next_mention_without_a_token_window():
    text = "ni por favor bajo toda circunstancia proveedores ni tras mucho texto contratistas"
    evidence = _evidence(text)
    assert [mention.polarity for mention in evidence.mentions] == [
        Polarity.NEGATED, Polarity.NEGATED,
    ]
    assert evidence.complete is True


@pytest.mark.parametrize("punctuation", [" ", ", ", ": ", " ( "])
def test_soft_punctuation_does_not_split_operator_attachment(punctuation):
    evidence = _evidence(f"al menos{punctuation}5 proveedores")
    assert evidence.cardinality.relation is CardinalityRelation.LOWER
    assert evidence.cardinality.values == (5,)


def test_hard_break_prevents_cross_sentence_attachment():
    evidence = _evidence("sin contratos. proveedores")
    assert evidence.polarity is Polarity.POSITIVE
    assert evidence.authorization is Authorization.EXECUTABLE


@pytest.mark.parametrize("text", [
    "once proveedores", "exactamente proveedores", "proveedores o más",
    "5 proveedores y 5 contratistas", "top 5 proveedores y los 5 contratistas",
    "proveedores 5", "entre proveedores",
])
def test_unlinked_or_multiple_cardinality_claims_fail_closed(text):
    evidence = _evidence(text)
    assert evidence.complete is False
    assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN
    assert evidence.authorization is Authorization.NOT_ACCREDITED


@pytest.mark.parametrize("text", [
    "proveedores de la alcaldía en 2025",
    "proveedores con contratos de 500 millones",
    "proveedores para 2026",
    "proveedores sobre contratos de 100 millones",
])
def test_year_and_money_decoys_are_not_attached(text):
    evidence = _evidence(text)
    assert evidence.cardinality.relation is CardinalityRelation.NONE
    assert evidence.authorization is Authorization.EXECUTABLE
    assert evidence.cardinality.quantity_spans == ()


def test_repeated_mentions_keep_distinct_original_offsets():
    text = "proveedores, proveedores y contratistas"
    evidence = _evidence(text)
    assert [text[slice(*mention.span.original)] for mention in evidence.mentions] == [
        "proveedores", "proveedores", "contratistas",
    ]
    assert len({mention.span.original for mention in evidence.mentions}) == 3


@pytest.mark.parametrize("form", ["NFC", "NFD"])
def test_unicode_forms_preserve_semantics_and_original_offsets(form):
    text = unicodedata.normalize(form, "ningún proveedor, como mínimo 5 proveedores")
    evidence = _evidence(text)
    assert evidence.polarity is Polarity.MIXED
    assert evidence.cardinality.relation is CardinalityRelation.LOWER
    for mention in evidence.mentions:
        assert unicodedata.normalize("NFC", text[slice(*mention.span.original)]) in {
            "proveedor", "proveedores",
        }
    for span in evidence.cardinality.operator_spans:
        assert text[slice(*span.original)]


def test_consumed_spans_are_ordered_unique_and_non_overlapping():
    evidence = _evidence("entre 5 y 10 proveedores y sin contratistas")
    normalized = [span.normalized for span in evidence.consumed_spans]
    assert normalized == sorted(set(normalized))
    assert all(left[1] <= right[0] for left, right in zip(normalized, normalized[1:]))


def test_invalid_inputs_publish_no_spans_and_fail_closed():
    for value in (None, 5, "", "   "):
        evidence = assess_supplier_request(value)  # type: ignore[arg-type]
        assert evidence.mentions == ()
        assert evidence.consumed_spans == ()
        assert evidence.complete is False
        assert evidence.authorization is Authorization.NOT_ACCREDITED


def test_public_authority_accepts_only_original_user_query():
    signature = inspect.signature(assess_supplier_request)
    assert tuple(signature.parameters) == ("user_query",)
    assert "quote" not in inspect.getsource(assess_supplier_request)
    assert "params" not in inspect.getsource(assess_supplier_request)
    assert "state" not in inspect.getsource(assess_supplier_request)


def test_authority_is_deterministic_and_reentrant():
    text = "5 proveedores, por lo menos"
    first = _evidence(text)
    assert first == _evidence(text)
    assert first == _evidence(text)
    assert not hasattr(assess_supplier_request, "cache_info")


@settings(max_examples=60, deadline=None)
@given(
    relation=st.sampled_from(["lower", "upper", "range", "other"]),
    value=st.integers(min_value=1, max_value=30),
    noun=st.sampled_from(["proveedor", "proveedores", "contratista", "contratistas"]),
)
def test_non_none_or_served_exact_structure_never_authorizes(relation, value, noun):
    surfaces = {
        "lower": f"más de {value} {noun}",
        "upper": f"hasta {value} {noun}",
        "range": f"entre {value} y {value} {noun}",
        "other": f"{value} {noun}",
    }
    evidence = _evidence(surfaces[relation])
    if relation == "other" and 1 <= value <= MAX_SUPPLIER_LIMIT:
        assert evidence.authorization is Authorization.EXECUTABLE
    else:
        assert evidence.authorization is Authorization.NOT_ACCREDITED


@settings(max_examples=40, deadline=None)
@given(
    gap=st.lists(st.sampled_from(["por", "favor", "ahora", ","]), max_size=8),
    noun=st.sampled_from(["proveedores", "contratistas"]),
)
def test_applicable_negation_is_monotonic_across_distance(gap, noun):
    positive = _evidence(" ".join([*gap, noun]))
    negated = _evidence(" ".join(["sin", *gap, noun]))
    assert positive.polarity is Polarity.POSITIVE
    assert negated.polarity is Polarity.NEGATED
    assert negated.authorization is Authorization.NOT_ACCREDITED


@settings(max_examples=30, deadline=None)
@given(
    left=st.sampled_from(["proveedores", "5 proveedores", "más de 5 proveedores"]),
    right=st.sampled_from(["contratistas", "5 contratistas", "hasta 5 contratistas"]),
)
def test_independent_claim_permutation_never_resolves_conflict_to_exact(left, right):
    first = _evidence(f"{left} y {right}")
    second = _evidence(f"{right} y {left}")
    if first.cardinality.relation is CardinalityRelation.UNKNOWN:
        assert second.cardinality.relation is CardinalityRelation.UNKNOWN
        assert first.authorization is second.authorization is Authorization.NOT_ACCREDITED


def test_noun_model_is_nominal_not_a_generic_span_parser():
    evidence = _evidence("proveedores y contratistas")
    assert [mention.noun for mention in evidence.mentions] == [
        SupplierNoun.PROVEEDOR, SupplierNoun.CONTRATISTA,
    ]


_COMPARATOR_ORACLE = (">=", "<=", "!=", "==", ">", "<", "=", "+", "-", "≥", "≤", "≠", "≈", "~")


@pytest.mark.parametrize("operator", _COMPARATOR_ORACLE)
@pytest.mark.parametrize("surface", ["{op}5 proveedores", "5{op}proveedores", "5 proveedores{op}"])
def test_every_symbolic_comparator_is_absorbing_unknown(operator, surface):
    evidence = _evidence(surface.format(op=operator))
    assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN
    assert evidence.cardinality.residue_reason is CardinalityResidueReason.UNKNOWN_OPERATOR
    assert evidence.cardinality.residue_spans
    assert evidence.authorization is Authorization.NOT_ACCREDITED


def test_comparator_longest_match_and_punctuation_are_distinct_lexer_types():
    tokens = tokenize(">= 5, proveedores")
    assert [(token.text, token.kind) for token in tokens] == [
        (">=", TokenKind.COMPARATOR_SYMBOL),
        ("5", TokenKind.BOUNDED_INTEGER),
        (",", TokenKind.SOFT_PUNCT),
        ("proveedores", TokenKind.SUPPLIER_NOUN),
    ]


@settings(max_examples=40, deadline=None)
@given(
    stem=st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=2, max_size=10),
    value=st.integers(min_value=2, max_value=9999),
)
def test_generic_word_que_quantity_never_acquires_exact_authority(stem, value):
    evidence = _evidence(f"z{stem} que {value} proveedores")
    assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN
    assert evidence.cardinality.residue_reason is CardinalityResidueReason.AMBIGUOUS_ATTACHMENT
    assert evidence.authorization is Authorization.NOT_ACCREDITED


@settings(max_examples=35, deadline=None)
@given(gap=st.text(alphabet="bcdfghjklpqrstvwxyz", min_size=2, max_size=12))
def test_nonnominal_word_between_quantity_and_mention_blocks_attachment(gap):
    evidence = _evidence(f"5 z{gap} proveedores")
    assert evidence.cardinality.relation is CardinalityRelation.NONE
    assert evidence.cardinality.quantity_spans == ()


@pytest.mark.parametrize("before,after", [
    ("en 2025 quiero ver 5 proveedores", "5 proveedores para el ano 2025"),
    ("por 500 millones quiero 5 proveedores", "5 proveedores con contratos de 500 millones"),
    ("en secop ii quiero 5 proveedores", "5 proveedores en secop ii"),
])
def test_external_dimension_capsules_do_not_change_supplier_cardinality(before, after):
    left = _evidence(before).cardinality
    right = _evidence(after).cardinality
    assert (left.relation, left.values) == (CardinalityRelation.EXACT, (5,))
    assert (right.relation, right.values) == (left.relation, left.values)


@settings(max_examples=20, deadline=None)
@given(
    digits=st.sampled_from([3, 4, 5, 4301]).flatmap(
        lambda size: st.text(alphabet="0123456789", min_size=size, max_size=size)
    ),
)
def test_arbitrary_digit_boundaries_never_raise_or_silently_authorize(digits):
    evidence = _evidence(f"{digits} proveedores")
    assert evidence.authorization is Authorization.NOT_ACCREDITED
    if len(digits) <= 4:
        assert evidence.cardinality.relation is CardinalityRelation.EXACT
    else:
        assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN
        assert evidence.cardinality.residue_reason is CardinalityResidueReason.OVERSIZE_INTEGER


@pytest.mark.parametrize("placeholder", ["X", "n", "#", "*", "?"])
def test_quantity_placeholders_are_not_integers(placeholder):
    evidence = _evidence(f"{placeholder} proveedores")
    assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN
    assert evidence.cardinality.residue_reason is CardinalityResidueReason.UNKNOWN_QUANTITY
    assert evidence.cardinality.values == ()
    assert evidence.authorization is Authorization.NOT_ACCREDITED


def test_unknown_residue_cannot_be_rehabilitated_by_later_exact_claim():
    evidence = _evidence(">= 5 proveedores y 5 contratistas")
    assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN
    assert evidence.complete is False
    assert evidence.authorization is Authorization.NOT_ACCREDITED


@pytest.mark.parametrize("text", [
    "mas proveedores",
    "exactamente proveedores",
    "proveedores o",
    "proveedores por lo",
])
def test_dangling_operator_is_unknown_residue(text):
    evidence = _evidence(text)
    assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN
    assert evidence.cardinality.residue_reason is CardinalityResidueReason.UNKNOWN_OPERATOR
    assert evidence.cardinality.residue_spans


def test_multiple_cardinality_claims_retain_ambiguous_residue_spans():
    evidence = _evidence("5 proveedores y 5 contratistas")
    assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN
    assert evidence.cardinality.residue_reason is CardinalityResidueReason.AMBIGUOUS_ATTACHMENT
    assert evidence.cardinality.residue_spans


def test_no_number_attaches_by_last_prior_position():
    decoy_only = _evidence("en 2025 quiero ver proveedores")
    text = "en 2025 quiero ver 5 proveedores"
    local_exact = _evidence(text)
    assert decoy_only.cardinality.relation is CardinalityRelation.NONE
    assert decoy_only.cardinality.quantity_spans == ()
    assert local_exact.cardinality.values == (5,)
    quantity = local_exact.cardinality.quantity_spans[0]
    assert text[slice(*quantity.original)] == "5"
