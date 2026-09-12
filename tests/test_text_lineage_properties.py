from __future__ import annotations

from hypothesis import given, strategies as st

from app.core.query_text_consumption import compact_whitespace, mask_consumed_spans
from app.core.query_router import QueryRouter
from app.core.text_lineage import DescendantKind, SegmentKind, TextLineageDraft


WORDS = st.sampled_from(("parques", "viales", "mantenimiento", "aseo"))


def _credentials(view):
    return {
        "request_id": view.request_id,
        "raw_sha256": view.raw_sha256,
        "seam_sha256": view.current_sha256,
    }


@given(word=WORDS, separator=st.text(alphabet=" \t", min_size=1, max_size=8))
def test_disjoint_repeated_occurrences_never_share_identity(word: str, separator: str) -> None:
    raw = f"{word}{separator}{word}"
    second_start = len(word) + len(separator)
    draft = TextLineageDraft(raw, request_id="property-repeat")
    masked = mask_consumed_spans(
        raw,
        ((0, len(word)),),
        lineage=draft,
        stage="consume_first",
    )
    seam = compact_whitespace(masked, lineage=draft)
    view = draft.seal(seam)

    first = view.descendants_of_raw_span(0, len(word), **_credentials(view))
    second = view.descendants_of_raw_span(
        second_start,
        second_start + len(word),
        **_credentials(view),
    )
    assert first.kind is DescendantKind.NO_SURVIVING_DESCENDANT
    assert second.kind is DescendantKind.EXACT_DESCENDANT
    assert seam[slice(*second.seam_intervals[0])] == word


@given(
    left=WORDS,
    right=WORDS,
    separator=st.text(alphabet=" \t\n", min_size=1, max_size=10),
)
def test_whitespace_compaction_does_not_change_neighbor_identity(
    left: str,
    right: str,
    separator: str,
) -> None:
    raw = f"{left}{separator}{right}"
    right_start = len(left) + len(separator)
    draft = TextLineageDraft(raw, request_id="property-whitespace")
    seam = compact_whitespace(raw, lineage=draft)
    view = draft.seal(seam)

    left_answer = view.descendants_of_raw_span(0, len(left), **_credentials(view))
    right_answer = view.descendants_of_raw_span(
        right_start,
        right_start + len(right),
        **_credentials(view),
    )
    assert left_answer.kind is DescendantKind.EXACT_DESCENDANT
    assert right_answer.kind is DescendantKind.EXACT_DESCENDANT


@given(topic=WORDS, year=st.integers(min_value=2020, max_value=2029))
def test_lineage_on_and_off_have_identical_params(topic: str, year: int) -> None:
    raw = f"contratos de {topic} en {year}"
    router = QueryRouter()
    off = router.parse(raw)
    on = router.parse(raw, text_lineage_request_id="property-equivalence")
    assert off.params == on.params


@given(word=WORDS, consume=st.booleans())
def test_every_noninserted_descendant_has_a_real_raw_origin(
    word: str,
    consume: bool,
) -> None:
    draft = TextLineageDraft(word, request_id="property-origin")
    current = word
    if consume:
        current = mask_consumed_spans(
            current,
            ((0, len(word)),),
            lineage=draft,
            stage="consume",
        )
    view = draft.seal(current)

    assert all(
        not segment.original_intervals
        if segment.kind is SegmentKind.INSERTED
        else bool(segment.original_intervals)
        for segment in view.segments
    )
    assert all(tombstone.original_intervals for tombstone in view.tombstones)
