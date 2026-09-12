from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import hashlib
import json
import pickle

import pytest

from app.core.query_text_consumption import replace_text_span
from app.core.text_lineage import (
    DescendantKind,
    LINEAGE_EVENT_SCHEMA,
    LINEAGE_VERSION,
    LineageMismatchError,
    LineageOperation,
    OriginKind,
    PartitionReceipt,
    Proof,
    SegmentKind,
    TextLineageDraft,
    TextLineageView,
    normalize_query_with_lineage,
    sha256_text,
)
from app.core.text_lineage_reducer import (
    ReducerState,
    canonicalize_partition,
    initial_state,
    reduce_partition,
    sealed_segments,
    state_sha256,
)


def _credentials(view):
    return {
        "request_id": view.request_id,
        "raw_sha256": view.raw_sha256,
        "seam_sha256": view.current_sha256,
    }


def _snapshot(draft: TextLineageDraft) -> bytes:
    return pickle.dumps(
        (
            draft.current_text,
            draft._pieces,
            draft._tombstones,
            draft._events,
        )
    )


def _run_path(
    path: str,
    *,
    operation: str,
    output: str,
    placeholder: object = None,
):
    draft = TextLineageDraft("ab", request_id="path-parity")
    kwargs = {} if placeholder is None else {"placeholder": placeholder}
    if path == "direct":
        result = draft.replace_span(
            0,
            2,
            output,
            stage="parity",
            operation=operation,
            proof=Proof.EXACT,
            **kwargs,
        )
    elif path == "wrapper":
        result = replace_text_span(
            "ab",
            (0, 2),
            output,
            lineage=draft,
            stage="parity",
            operation=operation,
            proof=Proof.EXACT,
            **kwargs,
        )
    else:
        result = draft.apply_partition(
            output,
            (
                PartitionReceipt(
                    (0, 2),
                    (0, len(output)),
                    "ab",
                    output,
                    operation,
                    Proof.EXACT,
                    **kwargs,
                ),
            ),
            stage="parity",
            operation=operation,
        )
    return draft.seal(result)


@pytest.mark.parametrize(
    ("operation", "output"),
    [
        ("KEEP", "ab"),
        ("MASK", "ab"),
        ("DELETE", ""),
        ("REPLACE", "cd"),
        ("CORRECT", "cd"),
        ("MERGE", "cd"),
        ("NORMALIZE", "cd"),
    ],
)
def test_direct_wrapper_and_partition_have_identical_reducer_state(
    operation: str,
    output: str,
) -> None:
    direct = _run_path("direct", operation=operation, output=output)
    wrapper = _run_path("wrapper", operation=operation, output=output)
    partition = _run_path("partition", operation=operation, output=output)

    assert direct == wrapper == partition
    assert direct.lineage_version == LINEAGE_VERSION == "TEXT_LINEAGE_V1_2"
    direct.verify_integrity()


@pytest.mark.parametrize("path", ["direct", "wrapper", "partition"])
def test_z9_insert_with_input_is_rejected_atomically_on_every_path(path: str) -> None:
    draft = TextLineageDraft("ab", request_id=f"z9-{path}")
    before = _snapshot(draft)

    with pytest.raises(ValueError, match="INSERT receipt requires no input"):
        if path == "direct":
            draft.replace_span(
                0,
                2,
                "ab",
                stage="z9",
                operation="INSERT",
                proof=Proof.EXACT,
            )
        elif path == "wrapper":
            replace_text_span(
                "ab",
                (0, 2),
                "ab",
                lineage=draft,
                stage="z9",
                operation="INSERT",
                proof=Proof.EXACT,
            )
        else:
            draft.apply_partition(
                "ab",
                (
                    PartitionReceipt(
                        (0, 2),
                        (0, 2),
                        "ab",
                        "ab",
                        "INSERT",
                        Proof.EXACT,
                    ),
                ),
                stage="z9",
                operation="INSERT",
            )

    assert _snapshot(draft) == before


@pytest.mark.parametrize(
    ("operation", "output", "message"),
    [
        ("DELETE", "x", "DELETE receipt requires input and no output"),
        ("MASK", "", "MASK receipt requires input and placeholder output"),
        ("REPLACE", "", "REPLACE receipt requires distinct input and output"),
    ],
)
@pytest.mark.parametrize("path", ["direct", "wrapper", "partition"])
def test_z9_other_invalid_shapes_have_path_parity(
    operation: str,
    output: str,
    message: str,
    path: str,
) -> None:
    draft = TextLineageDraft("ab", request_id=f"z9-shape-{path}")
    before = _snapshot(draft)

    with pytest.raises(ValueError, match=message):
        if path == "direct":
            draft.replace_span(0, 2, output, stage="z9", operation=operation)
        elif path == "wrapper":
            replace_text_span(
                "ab",
                (0, 2),
                output,
                lineage=draft,
                stage="z9",
                operation=operation,
            )
        else:
            draft.apply_partition(
                output,
                (
                    PartitionReceipt(
                        (0, 2),
                        (0, len(output)),
                        "ab",
                        output,
                        operation,
                        Proof.EXACT,
                    ),
                ),
                stage="z9",
                operation=operation,
            )

    assert _snapshot(draft) == before


@pytest.mark.parametrize("operation", ["CORRECT", "MERGE", "NORMALIZE"])
@pytest.mark.parametrize("path", ["direct", "wrapper", "partition"])
def test_z10_empty_derived_output_is_rejected_atomically(
    operation: str,
    path: str,
) -> None:
    draft = TextLineageDraft("ab", request_id=f"z10-{path}")
    before = _snapshot(draft)

    with pytest.raises(ValueError, match=f"{operation} receipt requires input and output"):
        if path == "direct":
            draft.replace_span(0, 2, "", stage="z10", operation=operation)
        elif path == "wrapper":
            replace_text_span(
                "ab",
                (0, 2),
                "",
                lineage=draft,
                stage="z10",
                operation=operation,
            )
        else:
            draft.apply_partition(
                "",
                (
                    PartitionReceipt(
                        (0, 2),
                        (0, 0),
                        "ab",
                        "",
                        operation,
                        Proof.EXACT,
                    ),
                ),
                stage="z10",
                operation=operation,
            )

    assert _snapshot(draft) == before


def test_normalize_combining_mark_loss_is_a_delete_receipt() -> None:
    draft = TextLineageDraft("a\u0301", request_id="combining-delete")
    output = normalize_query_with_lineage(draft.raw_text, draft)
    view = draft.seal(output)

    assert output == "a"
    assert [receipt.operation for receipt in view.events[0].receipt_manifest] == [
        LineageOperation.KEEP,
        LineageOperation.DELETE,
    ]
    assert view.descendants_of_raw_span(1, 2, **_credentials(view)).kind is (
        DescendantKind.NO_SURVIVING_DESCENDANT
    )
    view.verify_integrity()


@pytest.mark.parametrize("placeholder", [None, 0, 1, [], {}, "false"])
def test_z11_non_bool_placeholder_is_rejected_without_alias(
    placeholder: object,
) -> None:
    draft = TextLineageDraft("ab", request_id="z11")
    before = _snapshot(draft)

    with pytest.raises(ValueError, match="placeholder must be an exact bool"):
        draft.replace_span(
            0,
            2,
            "cd",
            stage="z11",
            operation="CORRECT",
            proof=Proof.EXACT,
            placeholder=placeholder,
        )

    if isinstance(placeholder, list):
        placeholder.append("mutated")
    assert _snapshot(draft) == before


@pytest.mark.parametrize(
    ("operation", "placeholder"),
    [("MASK", False), ("CORRECT", True), ("DELETE", True), ("INSERT", True)],
)
def test_explicit_placeholder_must_match_canonical_operation(
    operation: str,
    placeholder: bool,
) -> None:
    draft = TextLineageDraft("ab", request_id="placeholder-contradiction")
    output = "ab" if operation == "MASK" else "" if operation == "DELETE" else "cd"
    before = _snapshot(draft)

    with pytest.raises(ValueError, match="contradicts"):
        if operation == "INSERT":
            draft.apply_partition(
                "cdab",
                (
                    PartitionReceipt(
                        (0, 0),
                        (0, 2),
                        "",
                        "cd",
                        operation,
                        Proof.EXACT,
                        placeholder=placeholder,
                    ),
                    PartitionReceipt((0, 2), (2, 4), "ab", "ab", "KEEP", Proof.EXACT),
                ),
                stage="placeholder",
                operation=operation,
            )
        else:
            draft.replace_span(
                0,
                2,
                output,
                stage="placeholder",
                operation=operation,
                placeholder=placeholder,
            )

    assert _snapshot(draft) == before


def test_receipts_are_copied_before_caller_mutates_span_aliases() -> None:
    input_span = [0, 2]
    output_span = [0, 2]
    receipt = PartitionReceipt(
        input_span,  # type: ignore[arg-type]
        output_span,  # type: ignore[arg-type]
        "ab",
        "cd",
        "CORRECT",
        Proof.EXACT,
    )
    draft = TextLineageDraft("ab", request_id="detached")
    draft.apply_partition("cd", (receipt,), stage="detached", operation="CORRECT")
    view = draft.seal("cd")

    input_span[:] = [1, 1]
    output_span[:] = [1, 1]

    assert view.events[0].receipt_manifest[0].input_span == (0, 2)
    assert view.events[0].receipt_manifest[0].output_span == (0, 2)
    view.verify_integrity()
    with pytest.raises(FrozenInstanceError):
        view.events[0].receipt_manifest[0].placeholder = True  # type: ignore[misc]


def test_zero_width_keep_padding_is_removed_from_manifest() -> None:
    draft = TextLineageDraft("ab", request_id="neutral-padding")
    draft.apply_partition(
        "cd",
        (
            PartitionReceipt((0, 0), (0, 0), "", "", "KEEP", Proof.EXACT),
            PartitionReceipt((0, 2), (0, 2), "ab", "cd", "CORRECT", Proof.EXACT),
            PartitionReceipt((2, 2), (2, 2), "", "", "KEEP", Proof.EXACT),
        ),
        stage="neutral-padding",
        operation="CORRECT",
    )
    view = draft.seal("cd")

    assert len(view.events[0].receipt_manifest) == 1
    assert view.events[0].receipt_manifest[0].operation is LineageOperation.CORRECT


def test_repeated_zero_width_insertions_keep_distinct_inserted_identity() -> None:
    draft = TextLineageDraft("ab", request_id="repeated-inserts")
    draft.apply_partition(
        "xayb",
        (
            PartitionReceipt((0, 0), (0, 1), "", "x", "INSERT", Proof.EXACT),
            PartitionReceipt((0, 1), (1, 2), "a", "a", "KEEP", Proof.EXACT),
            PartitionReceipt((1, 1), (2, 3), "", "y", "INSERT", Proof.EXACT),
            PartitionReceipt((1, 2), (3, 4), "b", "b", "KEEP", Proof.EXACT),
        ),
        stage="repeated-inserts",
        operation="INSERT",
    )
    view = draft.seal("xayb")

    assert view.origin_of_seam_span(0, 1, **_credentials(view)).kind is OriginKind.INSERTED
    assert view.origin_of_seam_span(2, 3, **_credentials(view)).kind is OriginKind.INSERTED
    inserted_ids = [
        segment.segment_id
        for segment in view.segments
        if segment.kind is SegmentKind.INSERTED
    ]
    assert len(inserted_ids) == len(set(inserted_ids)) == 2


def test_mixed_original_and_originless_transform_fails_closed() -> None:
    draft = TextLineageDraft("ab", request_id="mixed-origin")
    draft.apply_partition(
        "xab",
        (
            PartitionReceipt((0, 0), (0, 1), "", "x", "INSERT", Proof.EXACT),
            PartitionReceipt((0, 2), (1, 3), "ab", "ab", "KEEP", Proof.EXACT),
        ),
        stage="insert",
        operation="INSERT",
    )
    draft.replace_span(
        0,
        3,
        "z",
        stage="merge-mixed",
        operation="MERGE",
        proof=Proof.EXACT,
    )
    view = draft.seal("z")

    assert view.segments[0].proof is Proof.UNPROVEN
    assert view.descendants_of_raw_span(0, 2, **_credentials(view)).kind is (
        DescendantKind.UNPROVEN
    )
    assert view.origin_of_seam_span(0, 1, **_credentials(view)).kind is OriginKind.UNPROVEN


def test_compact_exact_original_segment_projects_partial_spans_exactly() -> None:
    view = TextLineageDraft("abcd", request_id="compact-raw").seal("abcd")

    descendant = view.descendants_of_raw_span(1, 3, **_credentials(view))
    origin = view.origin_of_seam_span(1, 3, **_credentials(view))

    assert descendant.kind is DescendantKind.EXACT_DESCENDANT
    assert descendant.seam_intervals == ((1, 3),)
    assert origin.kind is OriginKind.EXACT_ORIGIN
    assert origin.raw_intervals == ((1, 3),)


@pytest.mark.parametrize("envelope", ["keep", "mAsK", "MYSTERY", None, True, False])
def test_unknown_or_non_exact_envelope_rejects_before_keep_fast_path(
    envelope: object,
) -> None:
    draft = TextLineageDraft("ab", request_id="exact-envelope")
    before = _snapshot(draft)
    receipt = PartitionReceipt((0, 2), (0, 2), "ab", "ab", "KEEP", Proof.EXACT)

    with pytest.raises(ValueError, match="unsupported partition envelope"):
        draft.apply_partition(
            "ab",
            (receipt,),
            stage="exact-envelope",
            operation=envelope,  # type: ignore[arg-type]
        )

    assert _snapshot(draft) == before


@pytest.mark.parametrize("receipt_operation", ["mask", "unknown", None, True, False])
def test_unknown_or_non_exact_receipt_operation_is_rejected_atomically(
    receipt_operation: object,
) -> None:
    draft = TextLineageDraft("ab", request_id="exact-receipt-operation")
    before = _snapshot(draft)
    receipt = PartitionReceipt(
        (0, 2),
        (0, 2),
        "ab",
        "ab",
        receipt_operation,  # type: ignore[arg-type]
        Proof.EXACT,
    )

    with pytest.raises(ValueError, match="unsupported partition operation"):
        draft.apply_partition(
            "ab",
            (receipt,),
            stage="exact-receipt-operation",
            operation="MASK",
        )

    assert _snapshot(draft) == before


def test_late_invalid_receipt_rolls_back_nonempty_history_byte_for_byte() -> None:
    draft = TextLineageDraft("ab", request_id="history-rollback")
    draft.replace_span(0, 1, "A", stage="first", operation="CORRECT")
    before = _snapshot(draft)

    with pytest.raises(ValueError, match="placeholder"):
        draft.apply_partition(
            "AB",
            (
                PartitionReceipt((0, 1), (0, 1), "A", "A", "KEEP", Proof.EXACT),
                PartitionReceipt(
                    (1, 2),
                    (1, 2),
                    "b",
                    "B",
                    "CORRECT",
                    Proof.EXACT,
                    placeholder=[],
                ),
            ),
            stage="invalid-second",
            operation="CORRECT",
        )

    assert _snapshot(draft) == before
    draft.seal("Ab").verify_integrity()


def _manifest_digest(event) -> str:
    payload = [
        {
            "input_span": receipt.input_span,
            "output_span": receipt.output_span,
            "input_sha256": sha256_text(receipt.input_text),
            "output_sha256": sha256_text(receipt.output_text),
            "operation": receipt.operation.value,
            "proof": receipt.proof.value,
            "placeholder": receipt.placeholder,
        }
        for receipt in event.receipt_manifest
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _independent_event_digest(event) -> str:
    def text_digest(text: str) -> str:
        return hashlib.sha256(
            text.encode("utf-8", errors="surrogatepass")
        ).hexdigest()

    manifest = tuple(
        {
            "input_span": receipt.input_span,
            "output_span": receipt.output_span,
            "input_sha256": text_digest(receipt.input_text),
            "output_sha256": text_digest(receipt.output_text),
            "operation": receipt.operation.value,
            "proof": receipt.proof.value,
            "placeholder": receipt.placeholder,
        }
        for receipt in event.receipt_manifest
    )
    payload = (
        event.schema,
        event.sequence,
        event.stage,
        event.operation.value,
        event.input_sha256,
        event.output_sha256,
        event.affected_input_intervals,
        event.affected_output_intervals,
        manifest,
        event.receipt_manifest_sha256,
        event.post_state_sha256,
        event.previous_event_sha256,
    )
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _segment_payload(segment) -> tuple[object, ...]:
    return (
        segment.segment_id,
        sha256_text(segment.text),
        None,
        segment.original_intervals,
        segment.kind.value,
        segment.proof.value,
        segment.transformation_id,
        segment.placeholder,
    )


def test_independent_hash_recomputation_covers_manifest_event_and_state() -> None:
    view = _run_path("direct", operation="MASK", output="  ")
    event = view.events[0]

    assert event.schema == LINEAGE_EVENT_SCHEMA
    assert event.receipt_manifest_sha256 == _manifest_digest(event)
    state_payload = (
        view.request_id,
        view.raw_sha256,
        view.current_sha256,
        tuple(_segment_payload(segment) for segment in view.segments),
        tuple(_segment_payload(segment) for segment in view.tombstones),
    )
    independent_state = hashlib.sha256(
        json.dumps(state_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert independent_state == event.post_state_sha256 == view.state_sha256
    id_fragment = f":{event.sequence}:{event.receipt_manifest_sha256[:16]}:"
    assert all(
        id_fragment in segment.segment_id
        for segment in (*view.segments, *view.tombstones)
    )

    manifest_payload = tuple(
        {
            "input_span": receipt.input_span,
            "output_span": receipt.output_span,
            "input_sha256": sha256_text(receipt.input_text),
            "output_sha256": sha256_text(receipt.output_text),
            "operation": receipt.operation.value,
            "proof": receipt.proof.value,
            "placeholder": receipt.placeholder,
        }
        for receipt in event.receipt_manifest
    )
    event_payload = (
        event.schema,
        event.sequence,
        event.stage,
        event.operation.value,
        event.input_sha256,
        event.output_sha256,
        event.affected_input_intervals,
        event.affected_output_intervals,
        manifest_payload,
        event.receipt_manifest_sha256,
        event.post_state_sha256,
        event.previous_event_sha256,
    )
    independent_event = hashlib.sha256(
        json.dumps(event_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert independent_event == event.event_sha256 == view.event_chain_head


def _integrity_view():
    return _run_path("direct", operation="MASK", output="  ")


def test_integrity_replay_rejects_corruption_of_every_event_field() -> None:
    view = _integrity_view()
    event = view.events[0]
    changed_manifest = (
        replace(event.receipt_manifest[0], proof=Proof.UNPROVEN),
    )
    mutations = {
        "schema": "wrong",
        "sequence": 2,
        "stage": "tampered",
        "operation": LineageOperation.CORRECT,
        "input_sha256": "0" * 64,
        "output_sha256": "1" * 64,
        "affected_input_intervals": (),
        "affected_output_intervals": (),
        "receipt_manifest": changed_manifest,
        "receipt_manifest_sha256": "2" * 64,
        "post_state_sha256": "3" * 64,
        "previous_event_sha256": "4" * 64,
        "event_sha256": "5" * 64,
    }

    for field, value in mutations.items():
        corrupted = replace(view, events=(replace(event, **{field: value}),))
        with pytest.raises(LineageMismatchError):
            corrupted.verify_integrity()


def test_integrity_replay_rejects_corruption_of_every_view_state_field() -> None:
    view = _integrity_view()
    first_segment = view.segments[0]
    mutations = {
        "request_id": "tampered-request",
        "raw_text": "xb",
        "raw_sha256": "0" * 64,
        "current_text": "xx",
        "current_sha256": "1" * 64,
        "lineage_version": "TEXT_LINEAGE_V0",
        "segments": (replace(first_segment, segment_id="tampered"),),
        "tombstones": (),
        "events": (),
        "event_chain_head": "2" * 64,
        "state_sha256": "3" * 64,
    }

    for field, value in mutations.items():
        with pytest.raises(LineageMismatchError):
            replace(view, **{field: value}).verify_integrity()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("segment_id", "tampered"),
        ("text", "x"),
        ("current_span", (1, 2)),
        ("original_intervals", ((0, 1),)),
        ("kind", SegmentKind.ORIGINAL),
        ("proof", Proof.UNPROVEN),
        ("transformation_id", "tampered"),
        ("placeholder", False),
    ],
)
def test_integrity_rejects_corruption_of_each_live_segment_field(
    field: str,
    value: object,
) -> None:
    view = _integrity_view()
    segment = view.segments[0]

    with pytest.raises(LineageMismatchError):
        replace(
            view,
            segments=(replace(segment, **{field: value}),),
        ).verify_integrity()


def test_seal_refuses_corrupted_draft_state() -> None:
    draft = TextLineageDraft("ab", request_id="corrupt-draft")
    draft._pieces[0] = replace(draft._pieces[0], original_intervals=((1, 2),))

    with pytest.raises(LineageMismatchError):
        draft.seal("ab")


def test_fifty_drafts_keep_request_state_isolated() -> None:
    views = []
    for index in range(50):
        draft = TextLineageDraft("ab", request_id=f"request-{index}")
        draft.replace_span(0, 1, str(index), stage="isolated", operation="CORRECT")
        views.append(draft.seal(f"{index}b"))

    assert len({view.request_id for view in views}) == 50
    assert len({view.state_sha256 for view in views}) == 50
    assert all(view.current_text == f"{index}b" for index, view in enumerate(views))
    for view in views:
        view.verify_integrity()


def test_surrogate_safe_reducer_and_replay() -> None:
    draft = TextLineageDraft("\ud800ab", request_id="surrogate")
    output = draft.replace_span(
        1,
        3,
        "cd",
        stage="surrogate",
        operation="REPLACE",
        proof=Proof.EXACT,
    )
    view = draft.seal(output)

    assert output == "\ud800cd"
    view.verify_integrity()


class _ChangedStr(str):
    def __str__(self) -> str:
        return "ZZ"


@pytest.mark.parametrize(
    "field",
    [
        "request_id",
        "raw_text",
        "raw_sha256",
        "current_text",
        "current_sha256",
        "lineage_version",
        "event_chain_head",
        "state_sha256",
    ],
)
def test_view_rejects_equal_str_subclasses_for_every_string_field(field: str) -> None:
    view = _integrity_view()
    modified = replace(view, **{field: _ChangedStr(getattr(view, field))})

    with pytest.raises(LineageMismatchError, match="exact string"):
        modified.verify_integrity()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema", _ChangedStr(LINEAGE_EVENT_SCHEMA)),
        ("sequence", True),
        ("stage", _ChangedStr("parity")),
        ("operation", "MASK"),
        ("input_sha256", _ChangedStr("0" * 64)),
        ("output_sha256", _ChangedStr("0" * 64)),
        ("affected_input_intervals", ((False, 2),)),
        ("affected_output_intervals", ((0, True),)),
        ("receipt_manifest", []),
        ("receipt_manifest_sha256", _ChangedStr("0" * 64)),
        ("post_state_sha256", _ChangedStr("0" * 64)),
        ("previous_event_sha256", _ChangedStr("0" * 64)),
        ("event_sha256", _ChangedStr("0" * 64)),
    ],
)
def test_event_rejects_nominally_equal_or_bool_typed_fields(
    field: str,
    value: object,
) -> None:
    view = _integrity_view()
    event = replace(view.events[0], **{field: value})

    with pytest.raises(LineageMismatchError):
        replace(view, events=(event,)).verify_integrity()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("input_span", (False, 2)),
        ("output_span", (0, True)),
        ("input_text", _ChangedStr("ab")),
        ("output_text", _ChangedStr("  ")),
        ("operation", "MASK"),
        ("proof", "exact"),
        ("placeholder", 1),
    ],
)
def test_manifest_rejects_nominally_equal_typed_fields(
    field: str,
    value: object,
) -> None:
    view = _integrity_view()
    event = view.events[0]
    receipt = replace(event.receipt_manifest[0], **{field: value})
    modified_event = replace(event, receipt_manifest=(receipt,))

    with pytest.raises(LineageMismatchError):
        replace(view, events=(modified_event,)).verify_integrity()


@pytest.mark.parametrize(
    ("collection", "field", "value"),
    [
        ("segments", "kind", "INSERTED"),
        ("segments", "proof", "exact"),
        ("segments", "placeholder", 0),
        ("segments", "current_span", (False, 2)),
        ("segments", "original_intervals", ((False, 1),)),
        ("tombstones", "kind", "CONSUMED"),
        ("tombstones", "proof", "exact"),
        ("tombstones", "placeholder", 1),
        ("tombstones", "current_span", (0, 1)),
        ("tombstones", "original_intervals", ((0, True),)),
    ],
)
def test_state_segments_reject_equal_string_enums_and_bool_ints(
    collection: str,
    field: str,
    value: object,
) -> None:
    view = _integrity_view()
    values = getattr(view, collection)
    modified = replace(view, **{collection: (replace(values[0], **{field: value}),)})

    with pytest.raises(LineageMismatchError):
        modified.verify_integrity()


@pytest.mark.parametrize(
    "boundary",
    ["output", "input_text", "output_text", "stage", "envelope", "operation"],
)
def test_str_subclass_partition_boundaries_reject_with_history_rollback(
    boundary: str,
) -> None:
    draft = TextLineageDraft("ab", request_id="subclass-boundary")
    draft.replace_span(0, 2, "ef", stage="prior", operation="CORRECT")
    before = _snapshot(draft)
    output: object = "cd"
    input_text: object = "ef"
    output_text: object = "cd"
    stage: object = "next"
    envelope: object = "CORRECT"
    operation: object = "CORRECT"
    if boundary == "output":
        output = _ChangedStr("cd")
    elif boundary == "input_text":
        input_text = _ChangedStr("ef")
    elif boundary == "output_text":
        output_text = _ChangedStr("cd")
    elif boundary == "stage":
        stage = _ChangedStr("next")
    elif boundary == "envelope":
        envelope = _ChangedStr("CORRECT")
    else:
        operation = _ChangedStr("CORRECT")
    receipt = PartitionReceipt(
        (0, 2),
        (0, 2),
        input_text,  # type: ignore[arg-type]
        output_text,  # type: ignore[arg-type]
        operation,  # type: ignore[arg-type]
        Proof.EXACT,
    )

    with pytest.raises(ValueError):
        draft.apply_partition(
            output,  # type: ignore[arg-type]
            (receipt,),
            stage=stage,  # type: ignore[arg-type]
            operation=envelope,  # type: ignore[arg-type]
        )

    assert _snapshot(draft) == before


@pytest.mark.parametrize("path", ["direct", "wrapper"])
def test_str_subclass_replacement_rejects_with_path_parity(path: str) -> None:
    draft = TextLineageDraft("ab", request_id=f"subclass-{path}")
    before = _snapshot(draft)

    with pytest.raises(ValueError, match="replacement must be str"):
        if path == "direct":
            draft.replace_span(
                0,
                2,
                _ChangedStr("cd"),
                stage="subclass",
                operation="CORRECT",
            )
        else:
            replace_text_span(
                "ab",
                (0, 2),
                _ChangedStr("cd"),
                lineage=draft,
                stage="subclass",
                operation="CORRECT",
            )

    assert _snapshot(draft) == before


def test_request_and_credential_string_subclasses_fail_closed() -> None:
    with pytest.raises(ValueError, match="raw_text must be str"):
        TextLineageDraft(_ChangedStr("ab"), request_id="raw")
    with pytest.raises(ValueError, match="request_id must be non-empty"):
        TextLineageDraft("ab", request_id=_ChangedStr("request"))

    view = TextLineageDraft("ab", request_id="request").seal("ab")
    credentials = _credentials(view)
    credentials["request_id"] = _ChangedStr("request")
    with pytest.raises(LineageMismatchError, match="exact strings"):
        view.descendants_of_raw_span(0, 2, **credentials)


class _ExplosiveStr(str):
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


class _ExplosiveTuple(tuple):
    def __len__(self):
        raise AssertionError("hostile tuple __len__ ran")


class _ExplosiveList(list):
    def __iter__(self):
        raise AssertionError("hostile list __iter__ ran")


def _correct_receipt(
    *,
    input_text: object = "ab",
    output_text: object = "cd",
    input_span: object = (0, 2),
) -> PartitionReceipt:
    return PartitionReceipt(
        input_span,  # type: ignore[arg-type]
        (0, 2),
        input_text,  # type: ignore[arg-type]
        output_text,  # type: ignore[arg-type]
        "CORRECT",
        Proof.EXACT,
    )


def test_canonicalize_rejects_hostile_input_text_before_hooks() -> None:
    with pytest.raises(ValueError, match="input must be an exact str"):
        canonicalize_partition(
            _ExplosiveStr("ab"),
            "cd",
            (_correct_receipt(),),
            operation="CORRECT",
        )


@pytest.mark.parametrize("kind", ["tuple", "list"])
def test_canonicalize_rejects_span_subclasses_before_hooks(kind: str) -> None:
    span = _ExplosiveTuple((0, 2)) if kind == "tuple" else _ExplosiveList([0, 2])

    with pytest.raises(ValueError, match="pair of integers"):
        canonicalize_partition(
            "ab",
            "cd",
            (_correct_receipt(input_span=span),),
            operation="CORRECT",
        )


def test_canonicalize_validates_late_receipt_before_matching_any_text() -> None:
    first = PartitionReceipt((1, 2), (0, 1), "b", "c", "CORRECT", Proof.EXACT)
    late = _correct_receipt(output_text=_ExplosiveStr("cd"))

    with pytest.raises(ValueError, match="receipt text must be an exact str"):
        canonicalize_partition("ab", "cd", (first, late), operation="CORRECT")


def test_canonicalize_exact_tuple_and_list_receipts_are_identical() -> None:
    receipt = _correct_receipt()

    from_tuple = canonicalize_partition("ab", "cd", (receipt,), operation="CORRECT")
    from_list = canonicalize_partition("ab", "cd", [receipt], operation="CORRECT")

    assert from_tuple == from_list


def test_canonicalize_rejects_receipt_collection_subclass_before_iteration() -> None:
    receipts = _ExplosiveList([_correct_receipt()])

    with pytest.raises(ValueError, match="exact tuple or list"):
        canonicalize_partition("ab", "cd", receipts, operation="CORRECT")


def test_reduce_partition_rejects_state_subclass_without_access() -> None:
    class StateSubclass(ReducerState):
        pass

    state = initial_state("ab", request_id="state-subclass")
    hostile = StateSubclass(
        state.request_id,
        state.raw_sha256,
        state.current_text,
        state.pieces,
        state.tombstones,
        state.events,
    )

    with pytest.raises(ValueError, match="exact ReducerState"):
        reduce_partition(
            hostile,
            "cd",
            (_correct_receipt(),),
            stage="boundary",
            operation="CORRECT",
        )


@pytest.mark.parametrize("field", ["pieces", "tombstones", "events"])
def test_reduce_partition_rejects_internal_collection_subclasses_before_hooks(
    field: str,
) -> None:
    state = initial_state("ab", request_id="hostile-collection")
    state = replace(state, **{field: _ExplosiveTuple(getattr(state, field))})

    with pytest.raises(LineageMismatchError, match="exact tuple"):
        reduce_partition(
            state,
            "cd",
            (_correct_receipt(),),
            stage="boundary",
            operation="CORRECT",
        )


@pytest.mark.parametrize("boundary", ["output", "stage", "envelope"])
def test_reduce_partition_rejects_hostile_call_strings_before_hooks(
    boundary: str,
) -> None:
    state = initial_state("ab", request_id="hostile-call")
    output: object = "cd"
    stage: object = "boundary"
    envelope: object = "CORRECT"
    if boundary == "output":
        output = _ExplosiveStr("cd")
    elif boundary == "stage":
        stage = _ExplosiveStr("boundary")
    else:
        envelope = _ExplosiveStr("CORRECT")

    with pytest.raises(ValueError):
        reduce_partition(
            state,
            output,  # type: ignore[arg-type]
            (_correct_receipt(),),
            stage=stage,  # type: ignore[arg-type]
            operation=envelope,
        )


@pytest.mark.parametrize("boundary", ["current_text", "piece_text", "event_stage"])
def test_reduce_partition_rejects_nested_hostile_strings_before_hooks(
    boundary: str,
) -> None:
    state = initial_state("ab", request_id="nested-hostile")
    if boundary == "current_text":
        state = replace(state, current_text=_ExplosiveStr("ab"))
    elif boundary == "piece_text":
        state = replace(
            state,
            pieces=(replace(state.pieces[0], text=_ExplosiveStr("ab")),),
        )
    else:
        state = reduce_partition(
            state,
            "cd",
            (_correct_receipt(),),
            stage="first",
            operation="CORRECT",
        )
        state = replace(
            state,
            events=(replace(state.events[0], stage=_ExplosiveStr("first")),),
        )

    with pytest.raises(LineageMismatchError, match="exact string"):
        reduce_partition(
            state,
            "cd",
            (_correct_receipt(),),
            stage="boundary",
            operation="CORRECT",
        )


def test_late_hostile_receipt_preserves_history_byte_for_byte() -> None:
    draft = TextLineageDraft("ab", request_id="hostile-history")
    draft.replace_span(0, 2, "ef", stage="prior", operation="CORRECT")
    before = _snapshot(draft)

    with pytest.raises(ValueError, match="receipt text must be an exact str"):
        draft.apply_partition(
            "cd",
            (
                PartitionReceipt((0, 1), (0, 1), "e", "c", "CORRECT", Proof.EXACT),
                PartitionReceipt(
                    (1, 2),
                    (1, 2),
                    "f",
                    _ExplosiveStr("d"),
                    "CORRECT",
                    Proof.EXACT,
                ),
            ),
            stage="hostile-late",
            operation="CORRECT",
        )

    assert _snapshot(draft) == before
    draft.seal("ef").verify_integrity()


def _advance_state(
    state: ReducerState,
    output_text: str,
    *,
    stage: str,
) -> ReducerState:
    input_text = state.current_text
    receipt = PartitionReceipt(
        (0, len(input_text)),
        (0, len(output_text)),
        input_text,
        output_text,
        "CORRECT",
        Proof.EXACT,
    )
    return reduce_partition(
        state,
        output_text,
        (receipt,),
        stage=stage,
        operation="CORRECT",
    )


def _view_from_state(state: ReducerState, *, raw_text: str) -> TextLineageView:
    return TextLineageView(
        request_id=state.request_id,
        raw_text=raw_text,
        raw_sha256=state.raw_sha256,
        current_text=state.current_text,
        current_sha256=sha256_text(state.current_text),
        lineage_version=LINEAGE_VERSION,
        segments=sealed_segments(state.pieces),
        tombstones=state.tombstones,
        events=state.events,
        event_chain_head=state.events[-1].event_sha256,
        state_sha256=state_sha256(
            state.request_id,
            state.raw_sha256,
            state.current_text,
            state.pieces,
            state.tombstones,
        ),
    )


def _assert_forged_state_is_rejected_atomically(state: ReducerState) -> None:
    before = pickle.dumps(state)

    with pytest.raises(LineageMismatchError, match="canonical replay"):
        _advance_state(state, "gh", stage="third")

    assert pickle.dumps(state) == before


def test_rehashed_affected_intervals_history_cannot_be_extended() -> None:
    original = _advance_state(
        initial_state("ab", request_id="forged-intervals"),
        "cd",
        stage="first",
    )
    changed = replace(
        original.events[0],
        affected_input_intervals=((0, 1),),
    )
    changed = replace(changed, event_sha256=_independent_event_digest(changed))
    attacked = replace(original, events=(changed,))

    _assert_forged_state_is_rejected_atomically(attacked)
    extended = _advance_state(original, "ef", stage="second")
    assert len(extended.events) == 2
    _view_from_state(extended, raw_text="ab").verify_integrity()


def test_rehashed_historical_post_state_cannot_be_extended() -> None:
    first = _advance_state(
        initial_state("ab", request_id="forged-post-state"),
        "cd",
        stage="first",
    )
    original = _advance_state(first, "ef", stage="second")
    changed_first = replace(original.events[0], post_state_sha256="f" * 64)
    changed_first = replace(
        changed_first,
        event_sha256=_independent_event_digest(changed_first),
    )
    changed_second = replace(
        original.events[1],
        previous_event_sha256=changed_first.event_sha256,
    )
    changed_second = replace(
        changed_second,
        event_sha256=_independent_event_digest(changed_second),
    )
    attacked = replace(original, events=(changed_first, changed_second))

    _assert_forged_state_is_rejected_atomically(attacked)


def test_rehashed_attacked_view_is_rejected_by_shared_replay() -> None:
    state = _advance_state(
        initial_state("ab", request_id="forged-view"),
        "cd",
        stage="first",
    )
    original = _view_from_state(state, raw_text="ab")
    changed = replace(
        original.events[0],
        affected_input_intervals=((0, 1),),
    )
    changed = replace(changed, event_sha256=_independent_event_digest(changed))
    attacked = replace(
        original,
        events=(changed,),
        event_chain_head=changed.event_sha256,
    )

    original.verify_integrity()
    with pytest.raises(LineageMismatchError, match="canonical replay"):
        attacked.verify_integrity()
