"""Tests del consumo exacto de texto por offsets (M3B-1)."""
from __future__ import annotations

import pytest

from app.core.query_text_consumption import mask_consumed_spans


def test_single_span():
    text = "subasta inversa"
    assert mask_consumed_spans(text, [(0, 7)]) == "        inversa"


def test_multiple_spans():
    text = "licitación pública o subasta"
    # spans: "licitación pública" (0..18) y "subasta" (21..28)
    masked = mask_consumed_spans(text, [(0, 18), (21, 28)])
    assert masked[19] == "o"  # el conector permanece
    assert masked == " " * 18 + " o " + " " * 7


def test_repeated_text_only_first_occurrence_consumed():
    text = "subasta de arte y subasta inversa"
    masked = mask_consumed_spans(text, [(0, 7)])
    # La primera "subasta" se enmascara; la segunda (índice 18) permanece intacta.
    assert masked[0:7] == "       "
    assert masked[18:25] == "subasta"


def test_spans_unordered_are_accepted():
    text = "abcdefghij"
    masked = mask_consumed_spans(text, [(7, 9), (1, 3)])
    assert masked == "a  defg  j"


def test_adjacent_spans_are_merged():
    text = "abcdef"
    # (0,3) y (3,6) adyacentes → todo enmascarado
    assert mask_consumed_spans(text, [(0, 3), (3, 6)]) == "      "


def test_overlapping_spans_are_merged():
    text = "abcdef"
    assert mask_consumed_spans(text, [(0, 4), (2, 6)]) == "      "


def test_unicode_offsets_operate_on_original_string():
    text = "contratación directa"
    # Enmascara solo "contratación" (12 code points); "ó" está en índice 10.
    masked = mask_consumed_spans(text, [(0, 12)])
    assert masked == " " * 12 + " directa"
    assert len(masked) == len(text)
    # El acento fuera de span no se altera y no hay renormalización.
    masked2 = mask_consumed_spans(text, [(13, 20)])
    assert masked2 == "contratación " + " " * 7


def test_punctuation_outside_spans_preserved():
    text = "¿licitación?, sí"
    masked = mask_consumed_spans(text, [(1, 11)])
    assert masked[0] == "¿"
    assert masked[11] == "?"
    assert masked[12] == ","


def test_empty_spans_returns_text_unchanged():
    text = "sin cambios"
    assert mask_consumed_spans(text, []) == text


@pytest.mark.parametrize("bad_span", [
    (-1, 3),          # start negativo
    (0, 100),         # end > len
    (5, 5),           # end == start
    (6, 4),           # end < start
])
def test_out_of_range_spans_rejected(bad_span):
    with pytest.raises(ValueError):
        mask_consumed_spans("0123456789", [bad_span])


def test_bool_index_rejected():
    with pytest.raises(ValueError):
        mask_consumed_spans("0123456789", [(True, 3)])
    with pytest.raises(ValueError):
        mask_consumed_spans("0123456789", [(0, True)])


@pytest.mark.parametrize("bad_shape", [
    "subasta",        # string en vez de par
    (1,),             # longitud 1
    (1, 2, 3),        # longitud 3
    (1.0, 2.0),       # no-int
])
def test_invalid_shape_rejected(bad_shape):
    with pytest.raises(ValueError):
        mask_consumed_spans("0123456789", [bad_shape])


def test_non_string_text_rejected():
    with pytest.raises(ValueError):
        mask_consumed_spans(12345, [(0, 1)])  # type: ignore[arg-type]


@pytest.mark.parametrize("text,spans", [
    ("hola mundo", [(0, 4)]),
    ("contratación directa", [(0, 12), (13, 20)]),
    ("licitación pública o subasta", [(21, 28), (0, 18)]),
    ("abcdef", [(0, 3), (3, 6)]),
])
def test_length_is_preserved(text, spans):
    assert len(mask_consumed_spans(text, spans)) == len(text)


def test_chars_outside_spans_are_identical():
    text = "licitación pública o subasta"
    spans = [(0, 18), (21, 28)]
    masked = mask_consumed_spans(text, spans)
    covered = set()
    for start, end in spans:
        covered.update(range(start, end))
    for i, ch in enumerate(text):
        if i not in covered:
            assert masked[i] == ch
        else:
            assert masked[i] == " "
