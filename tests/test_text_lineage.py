from __future__ import annotations

from dataclasses import asdict, FrozenInstanceError
import hashlib
import json

import pytest

from app.core.query_text_consumption import (
    compact_whitespace,
    mask_consumed_spans,
    replace_text_span,
)
from app.core.text_lineage import (
    DescendantKind,
    LINEAGE_VERSION,
    LineageMismatchError,
    OriginKind,
    PartitionReceipt,
    Proof,
    SegmentKind,
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
    # V1.1 receipts the same-surface whitespace MERGE as a real transform.
    assert second.kind is DescendantKind.TRANSFORMED_DESCENDANT
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
    previous = "0" * 64
    for event in view.events:
        payload = asdict(event)
        event_hash = payload.pop("event_sha256")
        assert payload["previous_event_sha256"] == previous
        assert event_hash == hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        previous = event_hash
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
    assert draft.current_text == "ab"
    assert draft.seal("ab").events == ()


def test_same_surface_mask_retires_origin_and_emits_equal_hash_event() -> None:
    draft = TextLineageDraft("a b", request_id="same-mask")
    masked = mask_consumed_spans(
        draft.current_text,
        ((1, 2),),
        lineage=draft,
        stage="space-mask",
    )
    view = draft.seal(masked)

    assert view.lineage_version == LINEAGE_VERSION == "TEXT_LINEAGE_V1_1"
    assert view.descendants_of_raw_span(1, 2, **_credentials(view)).kind is (
        DescendantKind.NO_SURVIVING_DESCENDANT
    )
    inverse = view.origin_of_seam_span(1, 2, **_credentials(view))
    assert inverse.kind is OriginKind.INSERTED
    assert len(view.events) == 1
    assert view.events[0].input_sha256 == view.events[0].output_sha256
    assert view.segments[1].kind is SegmentKind.INSERTED
    assert view.segments[1].placeholder is True


def test_delete_then_insert_same_surface_creates_new_identity_in_one_event() -> None:
    draft = TextLineageDraft("ab", request_id="delete-insert")
    receipts = (
        PartitionReceipt((0, 2), (0, 0), "ab", "", "DELETE", Proof.EXACT),
        PartitionReceipt((2, 2), (0, 2), "", "ab", "INSERT", Proof.EXACT),
    )
    draft.apply_partition(
        "ab",
        receipts,
        stage="delete-insert",
        operation="REPLACE",
    )
    view = draft.seal("ab")

    assert view.descendants_of_raw_span(0, 2, **_credentials(view)).kind is (
        DescendantKind.NO_SURVIVING_DESCENDANT
    )
    assert view.origin_of_seam_span(0, 2, **_credentials(view)).kind is (
        OriginKind.INSERTED
    )
    assert len(view.events) == 1
    assert view.events[0].input_sha256 == view.events[0].output_sha256


def test_all_keep_partition_preserves_origin_without_event() -> None:
    draft = TextLineageDraft("ab", request_id="all-keep")
    receipts = (
        PartitionReceipt((0, 1), (0, 1), "a", "a", "KEEP", Proof.EXACT),
        PartitionReceipt((1, 2), (1, 2), "b", "b", "KEEP", Proof.EXACT),
    )

    draft.apply_partition("ab", receipts, stage="keep", operation="KEEP")
    view = draft.seal("ab")
    assert view.events == ()
    assert view.descendants_of_raw_span(0, 2, **_credentials(view)).kind is (
        DescendantKind.EXACT_DESCENDANT
    )


@pytest.mark.parametrize("operation", ["CORRECT", "MERGE", "NORMALIZE"])
def test_same_surface_causal_transform_derives_origin_and_emits_event(
    operation: str,
) -> None:
    draft = TextLineageDraft("ab", request_id=f"same-{operation}")
    receipts = (
        PartitionReceipt(
            (0, 2),
            (0, 2),
            "ab",
            "ab",
            operation,
            Proof.WHOLE_SEGMENT_ONLY,
        ),
    )

    draft.apply_partition("ab", receipts, stage="same-surface", operation=operation)
    view = draft.seal("ab")
    assert view.descendants_of_raw_span(0, 2, **_credentials(view)).kind is (
        DescendantKind.TRANSFORMED_DESCENDANT
    )
    assert len(view.events) == 1
    assert view.events[0].input_sha256 == view.events[0].output_sha256


def test_same_surface_replace_is_rejected_atomically() -> None:
    draft = TextLineageDraft("ab", request_id="same-replace")
    receipts = (
        PartitionReceipt(
            (0, 2),
            (0, 2),
            "ab",
            "ab",
            "REPLACE",
            Proof.EXACT,
        ),
    )

    with pytest.raises(ValueError, match="distinct input and output"):
        draft.apply_partition(
            "ab",
            receipts,
            stage="same-replace",
            operation="REPLACE",
        )
    assert draft.current_text == "ab"
    view = draft.seal("ab")
    assert view.events == ()
    assert view.tombstones == ()


def test_distinct_replace_retires_input_and_inserts_output() -> None:
    draft = TextLineageDraft("ab", request_id="distinct-replace")
    receipts = (
        PartitionReceipt(
            (0, 2),
            (0, 2),
            "ab",
            "cd",
            "REPLACE",
            Proof.EXACT,
        ),
    )
    draft.apply_partition(
        "cd",
        receipts,
        stage="distinct-replace",
        operation="REPLACE",
    )
    view = draft.seal("cd")

    assert view.descendants_of_raw_span(0, 2, **_credentials(view)).kind is (
        DescendantKind.NO_SURVIVING_DESCENDANT
    )
    assert view.origin_of_seam_span(0, 2, **_credentials(view)).kind is (
        OriginKind.INSERTED
    )
    assert len(view.events) == 1


@pytest.mark.parametrize("path", ["direct", "wrapper"])
def test_distinct_replace_span_retires_input_and_inserts_output(path: str) -> None:
    draft = TextLineageDraft("ab", request_id=f"replace-{path}")
    if path == "direct":
        output = draft.replace_span(
            0,
            2,
            "cd",
            stage="replace",
            operation="REPLACE",
            proof=Proof.EXACT,
        )
    else:
        output = replace_text_span(
            "ab",
            (0, 2),
            "cd",
            lineage=draft,
            stage="replace",
            operation="REPLACE",
            proof=Proof.EXACT,
        )
    view = draft.seal(output)

    assert view.descendants_of_raw_span(0, 2, **_credentials(view)).kind is (
        DescendantKind.NO_SURVIVING_DESCENDANT
    )
    assert view.origin_of_seam_span(0, 2, **_credentials(view)).kind is (
        OriginKind.INSERTED
    )
    assert len(view.tombstones) == 1
    assert view.segments[0].original_intervals == ()
    assert view.segments[0].kind is SegmentKind.INSERTED


@pytest.mark.parametrize("envelope", ["KEEP", "DELETE", "MYSTERY", None])
def test_incompatible_partition_envelope_is_rejected_atomically(
    envelope: object,
) -> None:
    draft = TextLineageDraft("ab", request_id="bad-envelope")
    receipt = PartitionReceipt(
        (0, 2),
        (0, 2),
        "ab",
        "ab",
        "MASK",
        Proof.EXACT,
    )
    before = draft.seal("ab")

    with pytest.raises(ValueError, match="partition envelope|incompatible"):
        draft.apply_partition(
            "ab",
            (receipt,),
            stage="bad-envelope",
            operation=envelope,  # type: ignore[arg-type]
        )

    assert draft.seal("ab") == before


@pytest.mark.parametrize(
    "envelope",
    ["KEEP", "MASK", "DELETE", "INSERT", "REPLACE", "CORRECT", "MERGE", "NORMALIZE"],
)
def test_all_keep_validates_known_envelope_and_emits_no_event(envelope: str) -> None:
    draft = TextLineageDraft("ab", request_id=f"all-keep-{envelope}")
    receipt = PartitionReceipt((0, 2), (0, 2), "ab", "ab", "KEEP", Proof.EXACT)

    draft.apply_partition("ab", (receipt,), stage="all-keep", operation=envelope)

    assert draft.seal("ab").events == ()


@pytest.mark.parametrize("envelope", ["MYSTERY", None])
def test_all_keep_rejects_unknown_envelope_before_fast_path(envelope: object) -> None:
    draft = TextLineageDraft("ab", request_id="all-keep-unknown")
    receipt = PartitionReceipt((0, 2), (0, 2), "ab", "ab", "KEEP", Proof.EXACT)
    before = draft.seal("ab")

    with pytest.raises(ValueError, match="unsupported partition envelope"):
        draft.apply_partition(
            "ab",
            (receipt,),
            stage="all-keep",
            operation=envelope,  # type: ignore[arg-type]
        )

    assert draft.seal("ab") == before


def _single_partition_event(*, receipt_operation: str, proof: Proof):
    draft = TextLineageDraft("ab", request_id="manifest")
    receipt = PartitionReceipt(
        (0, 2),
        (0, 2),
        "ab",
        "cd",
        receipt_operation,
        proof,
    )
    draft.apply_partition("cd", (receipt,), stage="manifest", operation="CORRECT")
    return draft.seal("cd").events[0]


def test_event_hash_commits_receipt_operation_and_proof() -> None:
    corrected = _single_partition_event(
        receipt_operation="CORRECT",
        proof=Proof.EXACT,
    )
    merged = _single_partition_event(
        receipt_operation="MERGE",
        proof=Proof.EXACT,
    )
    weaker = _single_partition_event(
        receipt_operation="CORRECT",
        proof=Proof.WHOLE_SEGMENT_ONLY,
    )

    assert corrected.input_sha256 == merged.input_sha256 == weaker.input_sha256
    assert corrected.output_sha256 == merged.output_sha256 == weaker.output_sha256
    assert corrected.receipt_manifest_sha256 != merged.receipt_manifest_sha256
    assert corrected.receipt_manifest_sha256 != weaker.receipt_manifest_sha256
    assert corrected.event_sha256 != merged.event_sha256
    assert corrected.event_sha256 != weaker.event_sha256


def test_replace_span_synthetic_manifest_matches_equivalent_partition() -> None:
    direct = TextLineageDraft("ab", request_id="direct-manifest")
    direct.replace_span(
        0,
        2,
        "cd",
        stage="replace",
        operation="REPLACE",
        proof=Proof.EXACT,
    )
    partition = TextLineageDraft("ab", request_id="partition-manifest")
    partition.apply_partition(
        "cd",
        (
            PartitionReceipt(
                (0, 2),
                (0, 2),
                "ab",
                "cd",
                "REPLACE",
                Proof.EXACT,
            ),
        ),
        stage="replace",
        operation="REPLACE",
    )

    assert direct.seal("cd").events[0].receipt_manifest_sha256 == (
        partition.seal("cd").events[0].receipt_manifest_sha256
    )


@pytest.mark.parametrize(
    ("output", "receipts", "envelope"),
    [
        (
            "b",
            (
                PartitionReceipt((0, 1), (0, 0), "a", "", "DELETE", Proof.EXACT),
                PartitionReceipt((1, 2), (0, 1), "b", "b", "KEEP", Proof.EXACT),
            ),
            "REPLACE",
        ),
        (
            "xy",
            (
                PartitionReceipt((0, 1), (0, 1), "a", "x", "REPLACE", Proof.EXACT),
                PartitionReceipt((1, 2), (1, 1), "b", "", "DELETE", Proof.EXACT),
                PartitionReceipt((2, 2), (1, 2), "", "y", "INSERT", Proof.EXACT),
            ),
            "REPLACE",
        ),
        (
            "ab",
            (
                PartitionReceipt((0, 2), (0, 2), "ab", "ab", "NORMALIZE", Proof.EXACT),
            ),
            "CORRECT",
        ),
    ],
)
def test_out_of_table_partition_combinations_are_rejected_atomically(
    output: str,
    receipts: tuple[PartitionReceipt, ...],
    envelope: str,
) -> None:
    draft = TextLineageDraft("ab", request_id="out-of-table")
    before = draft.seal("ab")

    with pytest.raises(ValueError, match="incompatible"):
        draft.apply_partition(
            output,
            receipts,
            stage="out-of-table",
            operation=envelope,
        )

    assert draft.seal("ab") == before
