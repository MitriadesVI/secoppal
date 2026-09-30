"""Lexical contract controls; local truth is not SoQL source certification."""

import unicodedata

import pytest
from hypothesis import given, strategies as st

from app.core.foundation_text_predicates import (
    fold_text, folded_contains_clause, unicode_tokens, whole_token_truth,
)


@pytest.mark.parametrize("value", ["OBRA", "Obras", "ÓBRA", "obra-hidráulica", "[obras]",
                                 "obra\u2003pública", "obra\U0001f6a7pública", "óbra\u0301"])
def test_work_tokens_with_folded_unicode_punctuation(value):
    assert whole_token_truth((value, None), ("OBRA", "OBRAS")) is True


@pytest.mark.parametrize("value", ["COBRANZA", "SOBRAS", "MANIOBRAS", "obra2", "2obra",
                                 "obra²", "obrañ", "obra中", "obraα", "obra٢"])
def test_substrings_and_adjacent_unicode_letters_numbers_are_not_whole_work_tokens(value):
    assert whole_token_truth((value, "otra cosa"), ("OBRA", "OBRAS")) is False


@pytest.mark.parametrize(("values", "expected"), [
    ((None, None), None), ((None, "cobranza"), None), (("obra", None), True),
    ((None, "obras"), True), (("sobras", "maniobras"), False), (("", ""), False),
])
def test_work_predicate_three_valued_or(values, expected):
    assert whole_token_truth(values, ("OBRA", "OBRAS")) is expected


@given(st.text())
def test_folding_and_tokens_are_idempotent_and_do_not_discard_unicode_alphanumerics(value):
    folded = fold_text(value)
    assert fold_text(folded) == folded
    tokens = unicode_tokens(value)
    assert unicode_tokens(" ".join(tokens)) == tokens
    assert "".join(tokens) == "".join(
        c for c in folded if unicodedata.category(c)[0] in {"L", "N"}
    )


@given(st.characters(whitelist_categories=("Lu", "Ll", "Lo", "Nd", "Nl", "No")))
def test_unicode_letter_or_number_is_not_a_work_token_boundary(character):
    assert whole_token_truth(("OBRA" + character, ""), ("OBRA",)) is False


def test_folded_contains_compiles_exact_literal_and_both_columns():
    clause = folded_contains_clause(("nombre_del_procedimiento", "descripci_n_del_procedimiento"),
                                    "obra hidráulica")
    assert clause == (
        "(unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%obra hidráulica%')) "
        "OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%obra hidráulica%')))"
    )
    assert "coalesce" not in clause and "regex" not in clause
    assert "''" in folded_contains_clause(("objeto_del_contrato",), "l'obra")


@pytest.mark.parametrize("term", ["", " ", "obra%", "obra_", None, 1])
def test_unaccredited_like_wildcards_and_malformed_literals_fail_closed(term):
    with pytest.raises(ValueError):
        folded_contains_clause(("objeto_del_contrato",), term)


@pytest.mark.parametrize("fields", [(), ("x OR 1=1",), (None,), ["x"]])
def test_sql_columns_are_host_identifiers(fields):
    with pytest.raises(ValueError, match="INVALID_TEXT_COLUMNS"):
        folded_contains_clause(fields, "aseo")


def test_null_never_validates_a_malformed_physical_value():
    with pytest.raises(ValueError, match="INVALID_TEXT_VALUE"):
        whole_token_truth((None, 23), ("OBRA",))
