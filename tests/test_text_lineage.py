from __future__ import annotations

from dataclasses import FrozenInstanceError
import hashlib

import pytest

from app.core.query_text_consumption import compact_whitespace, mask_consumed_spans
from app.core.text_lineage import (
    DescendantKind,
    LineageMismatchError,
    OriginKind,
    PartitionReceipt,
    Proof,
    TextLineageDraft,
    normalize_query_with_lineage,
    sha256_text,
)
from app.utils.spell_correction import correct_query_with_receipts


def _credentials(view):
    return {
        "request_id": view.request_id,
        "raw_sha256": view.raw_sha256,
        "seam_sha256": view.current_sha256,
    }


def test_normalization_maps_accented_raw_unit_without_changing_output() -> None:
    draft = TextLineageDraft("Árbol", request_id="accent")
    normalized = normalize_query_with_lineage("Árbol", draft)
    view = draft.seal(normalized)

    assert normalized == "arbol"
    answer = view.descendants_of_raw_span(0, 5, **_credentials(view))
    assert answer.kind is DescendantKind.TRANSFORMED_DESCENDANT
    assert answer.seam_intervals == ((0, 5),)
    inverse = view.origin_of_seam_span(0, 5, **_credentials(view))
    assert inverse.kind is OriginKind.TRANSFORMED_ORIGIN
    assert inverse.raw_intervals == ((0, 5),)


def test_spell_correction_receipts_bind_parqes_to_its_actual_raw_token() -> None:
    draft = TextLineageDraft("parqes y parques", request_id="spell")
    normalized = normalize_query_with_lineage(draft.raw_text, draft)
    result = correct_query_with_receipts(
        normalized,
        receipt_sink=lambda output, receipts: draft.apply_partition(
            output, receipts, stage="spell_correction", operation="CORRECT"
        ),
    )
    view = draft.seal(result.text)

    assert result.text == "parques y parques"
    corrected = view.descendants_of_raw_span(0, 6, **_credentials(view))
    untouched = view.descendants_of_raw_span(9, 16, **_credentials(view))
    assert corrected.kind is DescendantKind.TRANSFORMED_DESCENDANT
    assert untouched.kind is DescendantKind.EXACT_DESCENDANT
    assert corrected.seam_intervals != untouched.seam_intervals
    assert any(receipt.operation == "CORRECT" for receipt in result.receipts)


def test_partial_consumption_of_whole_token_correction_is_unproven() -> None:
    draft = TextLineageDraft("parqes", request_id="partial-spell")
    result = correct_query_with_receipts(
        draft.raw_text,
        receipt_sink=lambda output, receipts: draft.apply_partition(
            output, receipts, stage="spell_correction", operation="CORRECT"
        ),
    )
    seam = mask_consumed_spans(
        result.text,
        ((0, 3),),
        lineage=draft,
        stage="partial_consumer",
    )
    view = draft.seal(seam)

    answer = view.descendants_of_raw_span(0, 6, **_credentials(view))
    assert answer.kind is DescendantKind.UNPROVEN


def test_consumed_occurrence_cannot_resurrect_as_identical_survivor() -> None:
    raw = "con vial y con vial"
    draft = TextLineageDraft(raw, request_id="repeat")
    masked = mask_consumed_spans(
        raw,
        ((0, 8),),
        lineage=draft,
        stage="negative_occurrence",
    )
    seam = compact_whitespace(masked, lineage=draft)
    view = draft.seal(seam)

    first = view.descendants_of_raw_span(0, 8, **_credentials(view))
    second = view.descendants_of_raw_span(11, 19, **_credentials(view))
    assert seam == "y con vial"
    assert first.kind is DescendantKind.NO_SURVIVING_DESCENDANT
    assert second.kind is DescendantKind.EXACT_DESCENDANT
    assert seam[slice(*second.seam_intervals[0])] == "con vial"


def test_whitespace_merge_preserves_neighbor_identity() -> None:
    raw = "parques     viales"
    draft = TextLineageDraft(raw, request_id="spaces")
    seam = compact_whitespace(raw, lineage=draft)
    view = draft.seal(seam)

    left = view.descendants_of_raw_span(0, 7, **_credentials(view))
    whitespace = view.descendants_of_raw_span(7, 12, **_credentials(view))
    right = view.descendants_of_raw_span(12, 18, **_credentials(view))
    assert seam == "parques viales"
    assert left.kind is DescendantKind.EXACT_DESCENDANT
    assert whitespace.kind is DescendantKind.TRANSFORMED_DESCENDANT
    assert whitespace.seam_intervals == ((7, 8),)
    assert right.kind is DescendantKind.EXACT_DESCENDANT
    assert left.seam_intervals == ((0, 7),)
    assert right.seam_intervals == ((8, 14),)


@pytest.mark.parametrize(
    ("credential", "value"),
    [
        ("request_id", "stale-request"),
        ("raw_sha256", "0" * 64),
        ("seam_sha256", "f" * 64),
    ],
)
def test_request_raw_and_seam_identity_mismatches_fail_closed(
    credential: str,
    value: str,
) -> None:
    draft = TextLineageDraft("parques", request_id="fresh")
    view = draft.seal("parques")
    credentials = _credentials(view)
    credentials[credential] = value

    with pytest.raises(LineageMismatchError):
        view.descendants_of_raw_span(0, 7, **credentials)


def test_events_are_immutable_and_hash_chained() -> None:
    draft = TextLineageDraft("  PARQES  ", request_id="chain")
    normalized = normalize_query_with_lineage(draft.raw_text, draft)
    result = correct_query_with_receipts(
        normalized,
        receipt_sink=lambda output, receipts: draft.apply_partition(
            output, receipts, stage="spell_correction", operation="CORRECT"
        ),
    )
    view = draft.seal(result.text)

    assert all(
        event.previous_event_sha256 == (
            "0" * 64 if index == 0 else view.events[index - 1].event_sha256
        )
        for index, event in enumerate(view.events)
    )
    assert view.event_chain_head == view.events[-1].event_sha256
    with pytest.raises(FrozenInstanceError):
        view.events[0].stage = "tampered"  # type: ignore[misc]


def test_surrogate_safe_hash_preserves_ordinary_sha256_contract() -> None:
    ordinary = "contratos de parques"

    assert sha256_text(ordinary) == hashlib.sha256(ordinary.encode("utf-8")).hexdigest()
    assert sha256_text("\ud800parques") == sha256_text("\ud800parques")
    assert sha256_text("parques\udfff") != sha256_text("\ud800parques")


@pytest.mark.parametrize("span", [(False, True), (0.0, 1.0)])
@pytest.mark.parametrize("api", ["descendants_of_raw_span", "origin_of_seam_span"])
def test_lineage_query_apis_reject_non_integer_spans(
    span: tuple[object, object],
    api: str,
) -> None:
    view = TextLineageDraft("ab", request_id="typed-bounds").seal("ab")

    with pytest.raises(ValueError, match="indices must be integers"):
        getattr(view, api)(*span, **_credentials(view))


@pytest.mark.parametrize(
    "receipts",
    [
        (
            PartitionReceipt((0, 2), (0, 2), "ab", "ab", "KEEP", Proof.EXACT),
            PartitionReceipt((2, 0), (2, 2), "", "", "KEEP", Proof.EXACT),
            PartitionReceipt((0, 2), (2, 4), "ab", "AB", "CORRECT", Proof.WHOLE_SEGMENT_ONLY),
        ),
        (PartitionReceipt((False, 1), (0, 1), "a", "a", "KEEP", Proof.EXACT),),
        (PartitionReceipt((0.0, 1.0), (0, 1), "a", "a", "KEEP", Proof.EXACT),),
        (PartitionReceipt((0, 3), (0, 2), "ab", "ab", "KEEP", Proof.EXACT),),
        (PartitionReceipt((0, 1), (False, 1), "a", "a", "KEEP", Proof.EXACT),),
        (PartitionReceipt((0, 1), (1, 0), "a", "", "DELETE", Proof.EXACT),),
        (PartitionReceipt((0, 2), (0, 3), "ab", "ab", "KEEP", Proof.EXACT),),
        (
            PartitionReceipt((0, 1), (0, 1), "a", "a", "KEEP", Proof.EXACT),
            PartitionReceipt((0, 2), (1, 3), "ab", "bB", "CORRECT", Proof.WHOLE_SEGMENT_ONLY),
        ),
    ],
)
def test_partition_rejects_malformed_or_duplicate_input_ranges(
    receipts: tuple[PartitionReceipt, ...],
) -> None:
    draft = TextLineageDraft("ab", request_id="partition-bounds")

    with pytest.raises((ValueError, LineageMismatchError)):
        draft.apply_partition(
            "abAB" if len(receipts) == 3 else "ab",
            receipts,
            stage="invalid-partition",
            operation="CORRECT",
        )
