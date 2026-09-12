"""Tests del consumo exacto de texto por offsets (M3B-1)."""
from __future__ import annotations

from enum import IntEnum

import pytest

from app.core.query_text_consumption import mask_consumed_spans, replace_text_span
from app.core.text_lineage import LineageOperation, Proof, TextLineageDraft


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


class _HostileStr(str):
    def __getitem__(self, key):
        raise AssertionError("hostile __getitem__ ran")

    def __eq__(self, other):
        raise AssertionError("hostile __eq__ ran")

    def __ne__(self, other):
        raise AssertionError("hostile __ne__ ran")

    def __str__(self):
        raise AssertionError("hostile __str__ ran")

    def __hash__(self):
        raise AssertionError("hostile __hash__ ran")

    def encode(self, *args, **kwargs):
        raise AssertionError("hostile encode ran")


class _HostileTuple(tuple):
    def __len__(self):
        raise AssertionError("hostile tuple __len__ ran")


class _HostileList(list):
    def __iter__(self):
        raise AssertionError("hostile list __iter__ ran")


class _IntSubclass(int):
    pass


class _IndexEnum(IntEnum):
    ZERO = 0
    TWO = 2


@pytest.mark.parametrize("boundary", ["text", "replacement"])
def test_replace_text_span_rejects_hostile_text_before_hooks(
    boundary: str,
) -> None:
    draft = TextLineageDraft("ab", request_id="hostile-text")
    text = _HostileStr("ab") if boundary == "text" else "ab"
    replacement = _HostileStr("cd") if boundary == "replacement" else "cd"

    with pytest.raises(ValueError):
        replace_text_span(
            text,
            (0, 2),
            replacement,
            lineage=draft,
            stage="boundary",
            operation="CORRECT",
        )

    assert draft.current_text == "ab"
    assert draft._events == []


@pytest.mark.parametrize("kind", ["tuple", "list"])
def test_replace_text_span_rejects_span_subclasses_before_hooks(kind: str) -> None:
    span = _HostileTuple((0, 2)) if kind == "tuple" else _HostileList([0, 2])
    with pytest.raises(ValueError, match="exact tuple or list"):
        replace_text_span("ab", span, "cd")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "span",
    [(_IntSubclass(0), 2), (0, _IntSubclass(2)), (_IndexEnum.ZERO, 2), (0, _IndexEnum.TWO)],
)
def test_replace_text_span_rejects_non_exact_integer_indices(span: object) -> None:
    with pytest.raises(ValueError, match="int exacto"):
        replace_text_span("ab", span, "cd")  # type: ignore[arg-type]


def test_replace_text_span_rejects_lineage_subclass_and_duck_before_access() -> None:
    class DraftSubclass(TextLineageDraft):
        pass

    class DraftDuck:
        @property
        def current_text(self):
            raise AssertionError("duck current_text was accessed")

    for lineage in (DraftSubclass("ab", request_id="subclass"), DraftDuck()):
        with pytest.raises(ValueError, match="exact TextLineageDraft"):
            replace_text_span(
                "ab",
                (0, 2),
                "cd",
                lineage=lineage,  # type: ignore[arg-type]
                stage="boundary",
                operation="CORRECT",
            )


def test_replace_text_span_off_ignores_observational_metadata() -> None:
    hostile_metadata = _HostileStr("CORRECT")

    assert replace_text_span(
        "ab",
        [0, 2],
        "cd",
        stage=hostile_metadata,
        operation=hostile_metadata,
        proof=hostile_metadata,  # type: ignore[arg-type]
        placeholder=hostile_metadata,
    ) == "cd"


def test_replace_text_span_exact_tuple_and_list_have_identical_lineage() -> None:
    views = []
    for span in ((0, 2), [0, 2]):
        draft = TextLineageDraft("ab", request_id="native-span")
        output = replace_text_span(
            "ab",
            span,  # type: ignore[arg-type]
            "cd",
            lineage=draft,
            stage="boundary",
            operation=LineageOperation.CORRECT,
            proof=Proof.EXACT,
        )
        views.append(draft.seal(output))

    assert views[0] == views[1]
    views[0].verify_integrity()
