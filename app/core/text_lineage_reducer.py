"""Pure validator, reducer, and replay verifier for TEXT_LINEAGE_V1_2."""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, replace
from typing import Iterable, TYPE_CHECKING

from app.core.text_lineage import (
    CanonicalReceipt,
    LINEAGE_EVENT_SCHEMA,
    LINEAGE_VERSION,
    LineageEvent,
    LineageMismatchError,
    LineageOperation,
    LineageSegment,
    PartitionReceipt,
    PLACEHOLDER_UNSET,
    Proof,
    SegmentKind,
    sha256_text,
)

if TYPE_CHECKING:
    from app.core.text_lineage import TextLineageView


_ENVELOPE_CAUSES = {
    LineageOperation.KEEP: frozenset(),
    LineageOperation.MASK: frozenset({LineageOperation.MASK}),
    LineageOperation.DELETE: frozenset({LineageOperation.DELETE}),
    LineageOperation.INSERT: frozenset({LineageOperation.INSERT}),
    LineageOperation.CORRECT: frozenset({
        LineageOperation.CORRECT,
        LineageOperation.MERGE,
        LineageOperation.DELETE,
    }),
    LineageOperation.MERGE: frozenset({
        LineageOperation.MERGE,
        LineageOperation.DELETE,
    }),
    LineageOperation.NORMALIZE: frozenset({
        LineageOperation.NORMALIZE,
        LineageOperation.DELETE,
    }),
}


@dataclass(frozen=True, slots=True)
class ReducerState:
    request_id: str
    raw_sha256: str
    current_text: str
    pieces: tuple[LineageSegment, ...]
    tombstones: tuple[LineageSegment, ...]
    events: tuple[LineageEvent, ...]


def initial_state(raw_text: str, *, request_id: str) -> ReducerState:
    return ReducerState(
        request_id=request_id,
        raw_sha256=sha256_text(raw_text),
        current_text=raw_text,
        pieces=(
            LineageSegment(
                segment_id="raw:0",
                text=raw_text,
                current_span=None,
                original_intervals=((0, len(raw_text)),),
                kind=SegmentKind.ORIGINAL,
                proof=Proof.EXACT,
                transformation_id="raw",
            ),
        )
        if raw_text
        else (),
        tombstones=(),
        events=(),
    )


def _operation(value: object, label: str) -> LineageOperation:
    if type(value) is LineageOperation:
        return value
    if type(value) is not str or value not in LineageOperation.__members__:
        raise ValueError(f"unsupported {label}")
    return LineageOperation[value]


def _span(value: object, length: int, label: str) -> tuple[int, int]:
    if type(value) not in (tuple, list):
        raise ValueError(f"{label} span must be a pair of integers")
    if len(value) != 2:
        raise ValueError(f"{label} span must have exactly two indices")
    start, end = value
    if any(type(index) is not int for index in (start, end)):
        raise ValueError(f"{label} span indices must be integers")
    if start < 0 or end < start or end > length:
        raise ValueError(f"{label} span is outside its transformation text")
    return start, end


def _canonical_placeholder(receipt: object, operation: LineageOperation) -> bool:
    expected = operation is LineageOperation.MASK
    supplied = receipt.placeholder
    if supplied is PLACEHOLDER_UNSET:
        return expected
    if type(supplied) is not bool:
        raise ValueError("receipt placeholder must be an exact bool when supplied")
    if supplied is not expected:
        raise ValueError("receipt placeholder contradicts operation semantics")
    return supplied


def _canonical_receipt(
    receipt: object,
    *,
    input_length: int,
    output_length: int,
) -> CanonicalReceipt:
    if type(receipt) not in (PartitionReceipt, CanonicalReceipt):
        raise ValueError("partition entries must be exact receipt values")
    if type(receipt.input_text) is not str or type(receipt.output_text) is not str:
        raise ValueError("partition receipt text must be an exact str")
    input_span = _span(receipt.input_span, input_length, "partition input")
    output_span = _span(receipt.output_span, output_length, "partition output")
    receipt_operation = _operation(receipt.operation, "partition operation")
    if type(receipt.proof) is not Proof:
        raise ValueError("partition proof must be an exact Proof value")
    return CanonicalReceipt(
        input_span=tuple(input_span),
        output_span=tuple(output_span),
        input_text=receipt.input_text,
        output_text=receipt.output_text,
        operation=receipt_operation,
        proof=receipt.proof,
        placeholder=_canonical_placeholder(receipt, receipt_operation),
    )


def _validate_shape(receipt: CanonicalReceipt) -> bool:
    input_length = receipt.input_span[1] - receipt.input_span[0]
    output_length = receipt.output_span[1] - receipt.output_span[0]
    operation = receipt.operation
    if operation is LineageOperation.KEEP:
        if input_length == output_length == 0:
            if receipt.input_text or receipt.output_text:
                raise ValueError("zero-width KEEP padding must have empty text")
            return False
        if input_length == 0 or output_length == 0 or receipt.input_text != receipt.output_text:
            raise ValueError("KEEP receipt requires equal non-empty input and output")
    elif operation is LineageOperation.MASK:
        if input_length == 0 or output_length == 0:
            raise ValueError("MASK receipt requires input and placeholder output")
    elif operation is LineageOperation.DELETE:
        if input_length == 0 or output_length != 0:
            raise ValueError("DELETE receipt requires input and no output")
    elif operation is LineageOperation.INSERT:
        if input_length != 0 or output_length == 0:
            raise ValueError("INSERT receipt requires no input and inserted output")
    elif operation is LineageOperation.REPLACE:
        if (
            input_length == 0
            or output_length == 0
            or receipt.input_text == receipt.output_text
        ):
            raise ValueError(
                "REPLACE receipt requires distinct input and output with non-empty spans"
            )
    elif input_length == 0 or output_length == 0:
        raise ValueError(f"{operation.value} receipt requires input and output")
    return True


def _validate_envelope(
    envelope: LineageOperation,
    receipts: tuple[CanonicalReceipt, ...],
) -> None:
    causes = {receipt.operation for receipt in receipts if receipt.operation is not LineageOperation.KEEP}
    if not causes:
        return
    if envelope is LineageOperation.REPLACE:
        valid = causes == {LineageOperation.REPLACE} or causes == {
            LineageOperation.DELETE,
            LineageOperation.INSERT,
        }
    else:
        valid = causes <= _ENVELOPE_CAUSES[envelope]
    if not valid:
        values = sorted(operation.value for operation in causes)
        raise ValueError(
            f"partition operations {values!r} are incompatible with "
            f"{envelope.value!r} envelope"
        )


def canonicalize_partition(
    input_text: str,
    output_text: str,
    receipts: Iterable[PartitionReceipt | CanonicalReceipt],
    *,
    operation: object,
) -> tuple[LineageOperation, tuple[CanonicalReceipt, ...]]:
    """Validate the full package and detach it from every caller-owned value."""
    if type(input_text) is not str:
        raise ValueError("partition input must be an exact str")
    if type(output_text) is not str:
        raise ValueError("partition output must be an exact str")
    envelope = _operation(operation, "partition envelope")
    if type(receipts) not in (tuple, list):
        raise ValueError("partition receipts must be an exact tuple or list")
    raw_receipts = tuple(receipts)
    detached_receipts = tuple(
        _canonical_receipt(
            receipt,
            input_length=len(input_text),
            output_length=len(output_text),
        )
        for receipt in raw_receipts
    )
    canonical: list[CanonicalReceipt] = []
    input_cursor = output_cursor = 0
    for receipt in detached_receipts:
        input_span = receipt.input_span
        output_span = receipt.output_span
        if input_span[0] != input_cursor or output_span[0] != output_cursor:
            raise LineageMismatchError("partition receipts are not contiguous")
        if input_text[slice(*input_span)] != receipt.input_text:
            raise LineageMismatchError("partition input receipt does not match parser text")
        if output_text[slice(*output_span)] != receipt.output_text:
            raise LineageMismatchError("partition output receipt does not match parser text")
        if _validate_shape(receipt):
            canonical.append(receipt)
        input_cursor = input_span[1]
        output_cursor = output_span[1]
    if input_cursor != len(input_text) or output_cursor != len(output_text):
        raise LineageMismatchError("partition receipts do not cover the transformation")
    result = tuple(canonical)
    _validate_envelope(envelope, result)
    return envelope, result


def _split_piece(
    piece: LineageSegment,
    offset: int,
) -> tuple[LineageSegment, LineageSegment]:
    left_text, right_text = piece.text[:offset], piece.text[offset:]
    if (
        len(piece.original_intervals) == 1
        and piece.proof is Proof.EXACT
        and len(piece.text) == piece.original_intervals[0][1] - piece.original_intervals[0][0]
    ):
        origin_start, origin_end = piece.original_intervals[0]
        left_origins = ((origin_start, origin_start + offset),)
        right_origins = ((origin_start + offset, origin_end),)
        proof = Proof.EXACT
    else:
        left_origins = right_origins = piece.original_intervals
        proof = Proof.UNPROVEN
    return (
        replace(piece, text=left_text, original_intervals=left_origins, proof=proof),
        replace(piece, text=right_text, original_intervals=right_origins, proof=proof),
    )


def _cut(
    pieces: tuple[LineageSegment, ...],
    start: int,
    end: int,
) -> tuple[LineageSegment, ...]:
    inside: list[LineageSegment] = []
    cursor = 0
    for piece in pieces:
        piece_start, piece_end = cursor, cursor + len(piece.text)
        cursor = piece_end
        if piece_end <= start or piece_start >= end:
            continue
        current = piece
        if piece_start < start:
            _, current = _split_piece(current, start - piece_start)
            piece_start = start
        if piece_end > end:
            current, _ = _split_piece(current, end - piece_start)
        inside.append(current)
    return tuple(inside)


def _merged(intervals: Iterable[tuple[int, int]]) -> tuple[tuple[int, int], ...]:
    result: list[tuple[int, int]] = []
    for start, end in sorted(set(intervals)):
        if result and start <= result[-1][1]:
            result[-1] = (result[-1][0], max(result[-1][1], end))
        else:
            result.append((start, end))
    return tuple(result)


def _proof(base: Proof, pieces: tuple[LineageSegment, ...]) -> Proof:
    has_origin = any(piece.original_intervals for piece in pieces)
    has_originless = any(not piece.original_intervals for piece in pieces)
    proofs = {base, *(piece.proof for piece in pieces)}
    if has_origin and has_originless or Proof.UNPROVEN in proofs:
        return Proof.UNPROVEN
    if Proof.WHOLE_SEGMENT_ONLY in proofs:
        return Proof.WHOLE_SEGMENT_ONLY
    return Proof.EXACT


def _receipt_payload(receipt: CanonicalReceipt) -> dict[str, object]:
    return {
        "input_span": receipt.input_span,
        "output_span": receipt.output_span,
        "input_sha256": sha256_text(receipt.input_text),
        "output_sha256": sha256_text(receipt.output_text),
        "operation": receipt.operation.value,
        "proof": receipt.proof.value,
        "placeholder": receipt.placeholder,
    }


def receipt_manifest_sha256(receipts: tuple[CanonicalReceipt, ...]) -> str:
    return hashlib.sha256(
        json.dumps(
            [_receipt_payload(receipt) for receipt in receipts],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _segment_payload(segment: LineageSegment) -> tuple[object, ...]:
    return (
        segment.segment_id,
        sha256_text(segment.text),
        segment.current_span,
        segment.original_intervals,
        segment.kind.value,
        segment.proof.value,
        segment.transformation_id,
        segment.placeholder,
    )


def state_sha256(
    request_id: str,
    raw_sha256: str,
    current_text: str,
    pieces: tuple[LineageSegment, ...],
    tombstones: tuple[LineageSegment, ...],
) -> str:
    payload = (
        request_id,
        raw_sha256,
        sha256_text(current_text),
        tuple(_segment_payload(piece) for piece in pieces),
        tuple(_segment_payload(piece) for piece in tombstones),
    )
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _event_hash(event: LineageEvent) -> str:
    payload = (
        event.schema,
        event.sequence,
        event.stage,
        event.operation.value,
        event.input_sha256,
        event.output_sha256,
        event.affected_input_intervals,
        event.affected_output_intervals,
        tuple(_receipt_payload(receipt) for receipt in event.receipt_manifest),
        event.receipt_manifest_sha256,
        event.post_state_sha256,
        event.previous_event_sha256,
    )
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _apply_canonical_partition(
    state: ReducerState,
    output_text: str,
    manifest: tuple[CanonicalReceipt, ...],
    *,
    stage: str,
    envelope: LineageOperation,
) -> ReducerState:
    """Apply one canonical transition to an already accredited state."""
    if all(receipt.operation is LineageOperation.KEEP for receipt in manifest):
        return state

    manifest_sha256 = receipt_manifest_sha256(manifest)
    sequence = len(state.events) + 1
    transformation_id = f"{sequence}:{manifest_sha256[:16]}"
    ordinal = 0

    def next_id(kind: str) -> str:
        nonlocal ordinal
        ordinal += 1
        return f"{kind}:{sequence}:{manifest_sha256[:16]}:{ordinal}"

    new_pieces: list[LineageSegment] = []
    tombstones = list(state.tombstones)
    for receipt in manifest:
        pieces = (
            _cut(state.pieces, *receipt.input_span)
            if receipt.input_span[0] < receipt.input_span[1]
            else ()
        )
        origins = _merged(
            origin for piece in pieces for origin in piece.original_intervals
        )
        proof = _proof(receipt.proof, pieces)
        if receipt.operation is LineageOperation.KEEP:
            new_pieces.extend(pieces)
            continue
        consumes = receipt.operation in {
            LineageOperation.MASK,
            LineageOperation.DELETE,
            LineageOperation.REPLACE,
        }
        if consumes and origins:
            tombstones.append(LineageSegment(
                segment_id=next_id("tombstone"),
                text=receipt.input_text,
                current_span=None,
                original_intervals=origins,
                kind=SegmentKind.CONSUMED,
                proof=proof,
                transformation_id=transformation_id,
                placeholder=receipt.placeholder,
            ))
        if not receipt.output_text:
            continue
        inserted = receipt.operation in {
            LineageOperation.MASK,
            LineageOperation.INSERT,
            LineageOperation.REPLACE,
        }
        new_pieces.append(LineageSegment(
            segment_id=next_id("segment"),
            text=receipt.output_text,
            current_span=None,
            original_intervals=() if inserted else origins,
            kind=(
                SegmentKind.INSERTED
                if inserted or not origins
                else SegmentKind.DERIVED_FROM_ORIGINAL
            ),
            proof=proof,
            transformation_id=transformation_id,
            placeholder=receipt.placeholder,
        ))
    id_counts = Counter(piece.segment_id for piece in new_pieces)
    pieces_tuple = tuple(
        replace(piece, segment_id=next_id("segment"))
        if id_counts[piece.segment_id] > 1
        else piece
        for piece in new_pieces
    )
    tombstones_tuple = tuple(tombstones)
    post_state_sha256 = state_sha256(
        state.request_id,
        state.raw_sha256,
        output_text,
        pieces_tuple,
        tombstones_tuple,
    )
    previous = state.events[-1].event_sha256 if state.events else "0" * 64
    event = LineageEvent(
        schema=LINEAGE_EVENT_SCHEMA,
        sequence=sequence,
        stage=stage,
        operation=envelope,
        input_sha256=sha256_text(state.current_text),
        output_sha256=sha256_text(output_text),
        affected_input_intervals=_merged(
            receipt.input_span for receipt in manifest if receipt.input_text
        ),
        affected_output_intervals=_merged(
            receipt.output_span for receipt in manifest if receipt.output_text
        ),
        receipt_manifest=manifest,
        receipt_manifest_sha256=manifest_sha256,
        post_state_sha256=post_state_sha256,
        previous_event_sha256=previous,
        event_sha256="",
    )
    event = replace(event, event_sha256=_event_hash(event))
    return ReducerState(
        request_id=state.request_id,
        raw_sha256=state.raw_sha256,
        current_text=output_text,
        pieces=pieces_tuple,
        tombstones=tombstones_tuple,
        events=(*state.events, event),
    )


def _strict_reducer_segment(
    segment: object,
    *,
    label: str,
    tombstone: bool,
) -> LineageSegment:
    if type(segment) is not LineageSegment:
        raise LineageMismatchError(f"{label} must be a LineageSegment")
    _strict_string(segment.segment_id, f"{label}.segment_id", nonempty=True)
    _strict_string(segment.text, f"{label}.text", nonempty=True)
    _strict_string(
        segment.transformation_id,
        f"{label}.transformation_id",
        nonempty=True,
    )
    if segment.current_span is not None:
        raise LineageMismatchError(f"{label}.current_span must be absent in reducer state")
    if type(segment.kind) is not SegmentKind or type(segment.proof) is not Proof:
        raise LineageMismatchError(f"{label} enum fields have invalid nominal types")
    if type(segment.placeholder) is not bool:
        raise LineageMismatchError(f"{label}.placeholder must be an exact bool")
    if tombstone and segment.kind is not SegmentKind.CONSUMED:
        raise LineageMismatchError(f"{label} is not a canonical tombstone")
    if not tombstone and segment.kind is SegmentKind.CONSUMED:
        raise LineageMismatchError(f"{label} exposes a consumed live segment")
    _strict_intervals(
        segment.original_intervals,
        f"{label}.original_intervals",
        allow_empty=False,
    )
    return segment


def _validate_reducer_state(state: object) -> ReducerState:
    if type(state) is not ReducerState:
        raise ValueError("state must be an exact ReducerState")
    _strict_string(state.request_id, "state.request_id", nonempty=True)
    _strict_digest(state.raw_sha256, "state.raw_sha256")
    _strict_string(state.current_text, "state.current_text")
    if type(state.pieces) is not tuple:
        raise LineageMismatchError("state.pieces must be an exact tuple")
    if type(state.tombstones) is not tuple:
        raise LineageMismatchError("state.tombstones must be an exact tuple")
    if type(state.events) is not tuple:
        raise LineageMismatchError("state.events must be an exact tuple")

    pieces = tuple(
        _strict_reducer_segment(piece, label=f"state.pieces[{index}]", tombstone=False)
        for index, piece in enumerate(state.pieces)
    )
    tombstones = tuple(
        _strict_reducer_segment(
            tombstone,
            label=f"state.tombstones[{index}]",
            tombstone=True,
        )
        for index, tombstone in enumerate(state.tombstones)
    )
    if "".join(piece.text for piece in pieces) != state.current_text:
        raise LineageMismatchError("state pieces do not cover current text")
    segment_ids = tuple(segment.segment_id for segment in (*pieces, *tombstones))
    if len(segment_ids) != len(set(segment_ids)):
        raise LineageMismatchError("state segment identifiers must be unique")

    for index, event in enumerate(state.events):
        _strict_event(event, index)

    if not state.events:
        if state != initial_state(state.current_text, request_id=state.request_id):
            raise LineageMismatchError("eventless reducer state is not canonical")
        return state

    first_event = state.events[0]
    raw_text = "".join(
        receipt.input_text for receipt in first_event.receipt_manifest
    )
    if sha256_text(raw_text) != state.raw_sha256:
        raise LineageMismatchError("first event does not cover authenticated raw text")
    replayed = initial_state(raw_text, request_id=state.request_id)
    for index, expected in enumerate(state.events):
        output_text = "".join(
            receipt.output_text for receipt in expected.receipt_manifest
        )
        envelope, manifest = canonicalize_partition(
            replayed.current_text,
            output_text,
            expected.receipt_manifest,
            operation=expected.operation,
        )
        advanced = _apply_canonical_partition(
            replayed,
            output_text,
            manifest,
            stage=expected.stage,
            envelope=envelope,
        )
        if len(advanced.events) != len(replayed.events) + 1:
            raise LineageMismatchError(f"events[{index}] is not a state transition")
        if advanced.events[-1] != expected:
            raise LineageMismatchError(f"events[{index}] diverges from canonical replay")
        replayed = advanced
    if replayed != state:
        raise LineageMismatchError("replayed history diverges from reducer state")
    return state


def reduce_partition(
    state: ReducerState,
    output_text: str,
    receipts: Iterable[PartitionReceipt | CanonicalReceipt],
    *,
    stage: str,
    operation: object,
) -> ReducerState:
    """Return the next state; the supplied state is never mutated."""
    if type(state) is not ReducerState:
        raise ValueError("state must be an exact ReducerState")
    if type(output_text) is not str:
        raise ValueError("partition output must be an exact str")
    if type(stage) is not str or not stage:
        raise ValueError("lineage stage must be a non-empty string")
    envelope = _operation(operation, "partition envelope")
    state = _validate_reducer_state(state)
    envelope, manifest = canonicalize_partition(
        state.current_text,
        output_text,
        receipts,
        operation=envelope,
    )
    return _apply_canonical_partition(
        state,
        output_text,
        manifest,
        stage=stage,
        envelope=envelope,
    )


def sealed_segments(pieces: tuple[LineageSegment, ...]) -> tuple[LineageSegment, ...]:
    cursor = 0
    result: list[LineageSegment] = []
    for piece in pieces:
        span = (cursor, cursor + len(piece.text))
        result.append(replace(piece, current_span=span))
        cursor = span[1]
    return tuple(result)


def _strict_string(value: object, label: str, *, nonempty: bool = False) -> None:
    if type(value) is not str or nonempty and not value:
        raise LineageMismatchError(f"{label} must be an exact string")


def _strict_digest(value: object, label: str) -> None:
    _strict_string(value, label)
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise LineageMismatchError(f"{label} is not a canonical SHA-256 digest")


def _strict_interval(
    value: object,
    label: str,
    *,
    allow_empty: bool,
    upper_bound: int | None = None,
) -> tuple[int, int]:
    if type(value) is not tuple or len(value) != 2:
        raise LineageMismatchError(f"{label} must be an immutable interval")
    start, end = value
    if type(start) is not int or type(end) is not int:
        raise LineageMismatchError(f"{label} bounds must be exact integers")
    if start < 0 or end < start or not allow_empty and start == end:
        raise LineageMismatchError(f"{label} bounds are invalid")
    if upper_bound is not None and end > upper_bound:
        raise LineageMismatchError(f"{label} exceeds its authenticated text")
    return start, end


def _strict_intervals(
    value: object,
    label: str,
    *,
    allow_empty: bool,
    upper_bound: int | None = None,
) -> None:
    if type(value) is not tuple:
        raise LineageMismatchError(f"{label} must be an immutable interval tuple")
    for index, interval in enumerate(value):
        _strict_interval(
            interval,
            f"{label}[{index}]",
            allow_empty=allow_empty,
            upper_bound=upper_bound,
        )


def _strict_segment(
    segment: object,
    *,
    label: str,
    raw_length: int,
    current_length: int,
    tombstone: bool,
) -> LineageSegment:
    if type(segment) is not LineageSegment:
        raise LineageMismatchError(f"{label} must be a LineageSegment")
    _strict_string(segment.segment_id, f"{label}.segment_id", nonempty=True)
    _strict_string(segment.text, f"{label}.text", nonempty=True)
    _strict_string(
        segment.transformation_id,
        f"{label}.transformation_id",
        nonempty=True,
    )
    if type(segment.kind) is not SegmentKind or type(segment.proof) is not Proof:
        raise LineageMismatchError(f"{label} enum fields have invalid nominal types")
    if type(segment.placeholder) is not bool:
        raise LineageMismatchError(f"{label}.placeholder must be an exact bool")
    _strict_intervals(
        segment.original_intervals,
        f"{label}.original_intervals",
        allow_empty=False,
        upper_bound=raw_length,
    )
    if tombstone:
        if segment.current_span is not None or segment.kind is not SegmentKind.CONSUMED:
            raise LineageMismatchError(f"{label} is not a canonical tombstone")
    else:
        _strict_interval(
            segment.current_span,
            f"{label}.current_span",
            allow_empty=False,
            upper_bound=current_length,
        )
        if segment.kind is SegmentKind.CONSUMED:
            raise LineageMismatchError(f"{label} exposes a consumed live segment")
    return segment


def _strict_receipt(receipt: object, label: str) -> CanonicalReceipt:
    if type(receipt) is not CanonicalReceipt:
        raise LineageMismatchError(f"{label} must be a CanonicalReceipt")
    _strict_interval(receipt.input_span, f"{label}.input_span", allow_empty=True)
    _strict_interval(receipt.output_span, f"{label}.output_span", allow_empty=True)
    _strict_string(receipt.input_text, f"{label}.input_text")
    _strict_string(receipt.output_text, f"{label}.output_text")
    if type(receipt.operation) is not LineageOperation or type(receipt.proof) is not Proof:
        raise LineageMismatchError(f"{label} enum fields have invalid nominal types")
    if type(receipt.placeholder) is not bool:
        raise LineageMismatchError(f"{label}.placeholder must be an exact bool")
    return receipt


def _strict_event(event: object, index: int) -> LineageEvent:
    label = f"events[{index}]"
    if type(event) is not LineageEvent:
        raise LineageMismatchError(f"{label} must be a LineageEvent")
    _strict_string(event.schema, f"{label}.schema", nonempty=True)
    _strict_string(event.stage, f"{label}.stage", nonempty=True)
    if type(event.sequence) is not int or event.sequence != index + 1:
        raise LineageMismatchError(f"{label}.sequence must be its exact integer ordinal")
    if type(event.operation) is not LineageOperation:
        raise LineageMismatchError(f"{label}.operation has an invalid nominal type")
    for field in (
        "input_sha256",
        "output_sha256",
        "receipt_manifest_sha256",
        "post_state_sha256",
        "previous_event_sha256",
        "event_sha256",
    ):
        _strict_digest(getattr(event, field), f"{label}.{field}")
    _strict_intervals(
        event.affected_input_intervals,
        f"{label}.affected_input_intervals",
        allow_empty=False,
    )
    _strict_intervals(
        event.affected_output_intervals,
        f"{label}.affected_output_intervals",
        allow_empty=False,
    )
    if type(event.receipt_manifest) is not tuple or not event.receipt_manifest:
        raise LineageMismatchError(f"{label}.receipt_manifest must be a non-empty tuple")
    for receipt_index, receipt in enumerate(event.receipt_manifest):
        _strict_receipt(receipt, f"{label}.receipt_manifest[{receipt_index}]")
    return event


def _strict_view(view: TextLineageView) -> tuple[LineageSegment, ...]:
    for field in (
        "request_id",
        "raw_text",
        "raw_sha256",
        "current_text",
        "current_sha256",
        "lineage_version",
        "event_chain_head",
        "state_sha256",
    ):
        _strict_string(getattr(view, field), field, nonempty=field == "request_id")
    for field in (
        "raw_sha256",
        "current_sha256",
        "event_chain_head",
        "state_sha256",
    ):
        _strict_digest(getattr(view, field), field)
    if type(view.segments) is not tuple or type(view.tombstones) is not tuple:
        raise LineageMismatchError("sealed segments and tombstones must be tuples")
    if type(view.events) is not tuple:
        raise LineageMismatchError("sealed events must be a tuple")
    pieces: list[LineageSegment] = []
    cursor = 0
    for index, segment in enumerate(view.segments):
        typed = _strict_segment(
            segment,
            label=f"segments[{index}]",
            raw_length=len(view.raw_text),
            current_length=len(view.current_text),
            tombstone=False,
        )
        if typed.current_span[0] != cursor:
            raise LineageMismatchError("sealed segment spans are not contiguous")
        cursor = typed.current_span[1]
        pieces.append(replace(typed, current_span=None))
    if (
        cursor != len(view.current_text)
        or "".join(segment.text for segment in view.segments) != view.current_text
    ):
        raise LineageMismatchError("sealed segments do not cover current text")
    for index, tombstone in enumerate(view.tombstones):
        _strict_segment(
            tombstone,
            label=f"tombstones[{index}]",
            raw_length=len(view.raw_text),
            current_length=len(view.current_text),
            tombstone=True,
        )
    for index, event in enumerate(view.events):
        _strict_event(event, index)
    return tuple(pieces)


def verify_view_integrity(view: TextLineageView) -> None:
    """Replay raw history through this reducer and compare every sealed field."""
    presented_pieces = _strict_view(view)
    if view.lineage_version != LINEAGE_VERSION:
        raise LineageMismatchError("unsupported lineage version")
    if view.raw_sha256 != sha256_text(view.raw_text):
        raise LineageMismatchError("raw text hash is invalid")
    if view.current_sha256 != sha256_text(view.current_text):
        raise LineageMismatchError("sealed text hash is invalid")
    if view.state_sha256 != state_sha256(
        view.request_id,
        view.raw_sha256,
        view.current_text,
        presented_pieces,
        view.tombstones,
    ):
        raise LineageMismatchError("presented state digest is invalid")
    try:
        state = _validate_reducer_state(ReducerState(
            request_id=view.request_id,
            raw_sha256=view.raw_sha256,
            current_text=view.current_text,
            pieces=presented_pieces,
            tombstones=view.tombstones,
            events=view.events,
        ))
    except (AttributeError, TypeError, ValueError) as exc:
        if isinstance(exc, LineageMismatchError):
            raise
        raise LineageMismatchError("event history cannot be replayed") from exc
    if state.current_text != view.current_text:
        raise LineageMismatchError("replayed text diverged from sealed text")
    if state.events != view.events:
        raise LineageMismatchError("replayed events diverged from sealed history")
    if sealed_segments(state.pieces) != view.segments:
        raise LineageMismatchError("replayed segments diverged from sealed state")
    if state.tombstones != view.tombstones:
        raise LineageMismatchError("replayed tombstones diverged from sealed state")
    expected_head = state.events[-1].event_sha256 if state.events else "0" * 64
    if view.event_chain_head != expected_head:
        raise LineageMismatchError("event chain head is invalid")
    if view.state_sha256 != state_sha256(
        state.request_id,
        state.raw_sha256,
        state.current_text,
        state.pieces,
        state.tombstones,
    ):
        raise LineageMismatchError("sealed state digest is invalid")
