from __future__ import annotations

from hypothesis import given, strategies as st

from app.core.query_text_consumption import (
    compact_whitespace,
    mask_consumed_spans,
    replace_text_span,
)
from app.core.query_router import QueryRouter
from app.core.text_lineage import (
    DescendantKind,
    OriginKind,
    PartitionReceipt,
    Proof,
    SegmentKind,
    TextLineageDraft,
)


WORDS = st.sampled_from(("parques", "viales", "mantenimiento", "aseo"))
KNOWN_ENVELOPES = (
    "KEEP",
    "MASK",
    "DELETE",
    "INSERT",
    "REPLACE",
    "CORRECT",
    "MERGE",
    "NORMALIZE",
)


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


@given(word=st.text(alphabet="abcdef", min_size=1, max_size=20))
def test_same_surface_delete_insert_never_reuses_old_identity(word: str) -> None:
    draft = TextLineageDraft(word, request_id="property-delete-insert")
    length = len(word)
    receipts = (
        PartitionReceipt((0, length), (0, 0), word, "", "DELETE", Proof.EXACT),
        PartitionReceipt((length, length), (0, length), "", word, "INSERT", Proof.EXACT),
    )
    draft.apply_partition(
        word,
        receipts,
        stage="property-delete-insert",
        operation="REPLACE",
    )
    view = draft.seal(word)

    assert view.descendants_of_raw_span(0, length, **_credentials(view)).kind is (
        DescendantKind.NO_SURVIVING_DESCENDANT
    )
    assert view.origin_of_seam_span(0, length, **_credentials(view)).kind is (
        OriginKind.INSERTED
    )
    assert len(view.events) == 1


@given(
    word=st.text(alphabet="abcdef", min_size=1, max_size=20),
    operation=st.sampled_from(("CORRECT", "MERGE", "NORMALIZE")),
)
def test_same_surface_causal_operations_remain_transformed(
    word: str,
    operation: str,
) -> None:
    draft = TextLineageDraft(word, request_id="property-same-transform")
    length = len(word)
    receipts = (
        PartitionReceipt(
            (0, length),
            (0, length),
            word,
            word,
            operation,
            Proof.WHOLE_SEGMENT_ONLY,
        ),
    )
    draft.apply_partition(
        word,
        receipts,
        stage="property-same-transform",
        operation=operation,
    )
    view = draft.seal(word)

    assert view.descendants_of_raw_span(0, length, **_credentials(view)).kind is (
        DescendantKind.TRANSFORMED_DESCENDANT
    )
    assert len(view.events) == 1


@given(
    word=st.text(alphabet="abcdef", min_size=1, max_size=20),
    envelope=st.sampled_from(tuple(value for value in KNOWN_ENVELOPES if value != "MASK")),
    proof=st.sampled_from(tuple(Proof)),
)
def test_mask_receipt_never_crosses_an_incompatible_envelope(
    word: str,
    envelope: str,
    proof: Proof,
) -> None:
    draft = TextLineageDraft(word, request_id="property-envelope")
    length = len(word)
    receipt = PartitionReceipt(
        (0, length),
        (0, length),
        word,
        word,
        "MASK",
        proof,
    )
    before = draft.seal(word)

    try:
        draft.apply_partition(
            word,
            (receipt,),
            stage="property-envelope",
            operation=envelope,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("incompatible MASK envelope accepted")

    assert draft.seal(word) == before


@given(
    word=st.text(alphabet="abc", min_size=1, max_size=20),
    envelope=st.sampled_from(KNOWN_ENVELOPES),
)
def test_repeated_all_keep_partition_accepts_every_known_envelope_without_event(
    word: str,
    envelope: str,
) -> None:
    draft = TextLineageDraft(word, request_id="property-all-keep")
    receipts = tuple(
        PartitionReceipt(
            (index, index + 1),
            (index, index + 1),
            char,
            char,
            "KEEP",
            Proof.EXACT,
        )
        for index, char in enumerate(word)
    )

    draft.apply_partition(
        word,
        receipts,
        stage="property-all-keep",
        operation=envelope,
    )
    view = draft.seal(word)

    assert view.events == ()
    assert view.descendants_of_raw_span(0, len(word), **_credentials(view)).kind is (
        DescendantKind.EXACT_DESCENDANT
    )


@given(
    operation=st.sampled_from(("MASK", "REPLACE", "CORRECT", "MERGE", "NORMALIZE")),
    path=st.sampled_from(("direct", "wrapper", "partition")),
    cardinality=st.integers(min_value=1, max_value=5),
)
def test_operation_path_and_multievent_cardinality_share_one_reducer(
    operation: str,
    path: str,
    cardinality: int,
) -> None:
    draft = TextLineageDraft("ab", request_id="property-reducer")
    current = "ab"
    for _ in range(cardinality):
        output = (
            "cd" if current == "ab" else "ab"
        ) if operation == "REPLACE" else current
        if path == "direct":
            current = draft.replace_span(
                0,
                len(current),
                output,
                stage="property-reducer",
                operation=operation,
                proof=Proof.EXACT,
            )
        elif path == "wrapper":
            current = replace_text_span(
                current,
                (0, len(current)),
                output,
                lineage=draft,
                stage="property-reducer",
                operation=operation,
                proof=Proof.EXACT,
            )
        else:
            current = draft.apply_partition(
                output,
                (
                    PartitionReceipt(
                        (0, len(current)),
                        (0, len(output)),
                        current,
                        output,
                        operation,
                        Proof.EXACT,
                    ),
                ),
                stage="property-reducer",
                operation=operation,
            )

    view = draft.seal(current)
    assert len(view.events) == cardinality
    assert [event.sequence for event in view.events] == list(range(1, cardinality + 1))
    view.verify_integrity()
