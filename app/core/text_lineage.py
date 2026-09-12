"""Request-scoped causal lineage from raw query text to the CONTROL seam.

Lineage is updated by the same mutation primitives that produce parser text.
It never searches a later surface to infer where an earlier occurrence went.
"""
from __future__ import annotations

import hashlib
import json
import unicodedata
from dataclasses import dataclass, replace
from enum import Enum
from typing import Iterable


LINEAGE_VERSION = "TEXT_LINEAGE_V1_1"

_PARTITION_OPERATIONS = frozenset({
    "KEEP",
    "MASK",
    "DELETE",
    "INSERT",
    "REPLACE",
    "CORRECT",
    "MERGE",
    "NORMALIZE",
})

_PARTITION_ENVELOPE_OPERATIONS = {
    "KEEP": frozenset(),
    "MASK": frozenset({"MASK"}),
    "DELETE": frozenset({"DELETE"}),
    "INSERT": frozenset({"INSERT"}),
    "CORRECT": frozenset({"CORRECT", "MERGE", "DELETE"}),
    "MERGE": frozenset({"MERGE", "DELETE"}),
    "NORMALIZE": frozenset({"NORMALIZE"}),
}


class SegmentKind(str, Enum):
    ORIGINAL = "ORIGINAL"
    DERIVED_FROM_ORIGINAL = "DERIVED_FROM_ORIGINAL"
    INSERTED = "INSERTED"
    CONSUMED = "CONSUMED"


class Proof(str, Enum):
    EXACT = "exact"
    WHOLE_SEGMENT_ONLY = "whole-segment-only"
    UNPROVEN = "unproven"


class DescendantKind(str, Enum):
    EXACT_DESCENDANT = "EXACT_DESCENDANT"
    TRANSFORMED_DESCENDANT = "TRANSFORMED_DESCENDANT"
    MULTIPLE_DESCENDANTS = "MULTIPLE_DESCENDANTS"
    NO_SURVIVING_DESCENDANT = "NO_SURVIVING_DESCENDANT"
    UNPROVEN = "UNPROVEN"


class OriginKind(str, Enum):
    EXACT_ORIGIN = "EXACT_ORIGIN"
    TRANSFORMED_ORIGIN = "TRANSFORMED_ORIGIN"
    MULTIPLE_ORIGINS = "MULTIPLE_ORIGINS"
    INSERTED = "INSERTED"
    UNPROVEN = "UNPROVEN"


class LineageMismatchError(ValueError):
    """The caller tried to use a trace for a different request or text."""


@dataclass(frozen=True, slots=True)
class PartitionReceipt:
    """One causal input/output piece in an atomic text transformation."""

    input_span: tuple[int, int]
    output_span: tuple[int, int]
    input_text: str
    output_text: str
    operation: str
    proof: Proof


@dataclass(frozen=True, slots=True)
class LineageSegment:
    segment_id: str
    text: str
    current_span: tuple[int, int] | None
    original_intervals: tuple[tuple[int, int], ...]
    kind: SegmentKind
    proof: Proof
    transformation_id: str
    placeholder: bool = False


@dataclass(frozen=True, slots=True)
class LineageEvent:
    sequence: int
    stage: str
    operation: str
    input_sha256: str
    output_sha256: str
    affected_input_intervals: tuple[tuple[int, int], ...]
    affected_output_intervals: tuple[tuple[int, int], ...]
    receipt_manifest_sha256: str
    previous_event_sha256: str
    event_sha256: str


@dataclass(frozen=True, slots=True)
class DescendantAnswer:
    kind: DescendantKind
    seam_intervals: tuple[tuple[int, int], ...] = ()


@dataclass(frozen=True, slots=True)
class OriginAnswer:
    kind: OriginKind
    raw_intervals: tuple[tuple[int, int], ...] = ()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="surrogatepass")).hexdigest()


def _validate_query_span(start: object, end: object, length: int, label: str) -> None:
    if any(isinstance(value, bool) or not isinstance(value, int) for value in (start, end)):
        raise ValueError(f"{label} span indices must be integers")
    if start < 0 or end <= start or end > length:
        raise ValueError(f"{label} span is outside the authenticated text")


def _validate_partition_span(span: object, length: int, label: str) -> tuple[int, int]:
    if isinstance(span, (str, bytes)) or not isinstance(span, (tuple, list)):
        raise ValueError(f"{label} span must be a pair of integers")
    if len(span) != 2:
        raise ValueError(f"{label} span must have exactly two indices")
    start, end = span
    if any(isinstance(value, bool) or not isinstance(value, int) for value in (start, end)):
        raise ValueError(f"{label} span indices must be integers")
    if start < 0 or end < start or end > length:
        raise ValueError(f"{label} span is outside its transformation text")
    return start, end


def _merged(intervals: Iterable[tuple[int, int]]) -> tuple[tuple[int, int], ...]:
    result: list[tuple[int, int]] = []
    for start, end in sorted(set(intervals)):
        if result and start <= result[-1][1]:
            result[-1] = (result[-1][0], max(result[-1][1], end))
        else:
            result.append((start, end))
    return tuple(result)


def _covers(intervals: Iterable[tuple[int, int]], target: tuple[int, int]) -> bool:
    return _merged(intervals) == (target,)


def _weakest_proof(base: Proof, pieces: Iterable[LineageSegment]) -> Proof:
    proofs = {base, *(piece.proof for piece in pieces)}
    if Proof.UNPROVEN in proofs:
        return Proof.UNPROVEN
    if Proof.WHOLE_SEGMENT_ONLY in proofs:
        return Proof.WHOLE_SEGMENT_ONLY
    return Proof.EXACT


def _receipt_manifest_sha256(
    receipts: Iterable[tuple[PartitionReceipt, bool]],
) -> str:
    manifest = [
        {
            "input_span": receipt.input_span,
            "output_span": receipt.output_span,
            "input_sha256": sha256_text(receipt.input_text),
            "output_sha256": sha256_text(receipt.output_text),
            "operation": receipt.operation,
            "proof": receipt.proof.value,
            "placeholder": placeholder,
        }
        for receipt, placeholder in receipts
    ]
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _validate_partition_envelope(
    operation: object,
    receipts: tuple[PartitionReceipt, ...],
) -> None:
    if operation not in _PARTITION_OPERATIONS:
        raise ValueError(f"unsupported partition envelope: {operation!r}")
    causal_operations = {
        receipt.operation for receipt in receipts if receipt.operation != "KEEP"
    }
    if not causal_operations:
        return
    if operation == "REPLACE":
        valid = causal_operations == {"REPLACE"} or causal_operations == {
            "DELETE",
            "INSERT",
        }
    else:
        valid = causal_operations <= _PARTITION_ENVELOPE_OPERATIONS[operation]
    if not valid:
        raise ValueError(
            f"partition operations {sorted(causal_operations)!r} "
            f"are incompatible with {operation!r} envelope"
        )


@dataclass(frozen=True, slots=True)
class TextLineageView:
    request_id: str
    raw_text: str
    raw_sha256: str
    current_text: str
    current_sha256: str
    lineage_version: str
    segments: tuple[LineageSegment, ...]
    tombstones: tuple[LineageSegment, ...]
    events: tuple[LineageEvent, ...]
    event_chain_head: str

    def _validate(
        self,
        *,
        request_id: str,
        raw_sha256: str,
        seam_sha256: str,
    ) -> None:
        if request_id != self.request_id:
            raise LineageMismatchError("request_id does not match this trace")
        if raw_sha256 != self.raw_sha256:
            raise LineageMismatchError("raw hash does not match this trace")
        if seam_sha256 != self.current_sha256:
            raise LineageMismatchError("CONTROL seam hash does not match this trace")

    def descendants_of_raw_span(
        self,
        start: int,
        end: int,
        *,
        request_id: str,
        raw_sha256: str,
        seam_sha256: str,
    ) -> DescendantAnswer:
        self._validate(
            request_id=request_id,
            raw_sha256=raw_sha256,
            seam_sha256=seam_sha256,
        )
        _validate_query_span(start, end, len(self.raw_text), "raw")
        target = (start, end)
        live = [
            segment
            for segment in self.segments
            if any(a < end and start < b for a, b in segment.original_intervals)
        ]
        dead = [
            segment
            for segment in self.tombstones
            if any(a < end and start < b for a, b in segment.original_intervals)
        ]
        if any(
            segment.proof is Proof.UNPROVEN
            or any(a < start or b > end for a, b in segment.original_intervals)
            for segment in live
        ):
            return DescendantAnswer(DescendantKind.UNPROVEN)
        live_origins = [origin for segment in live for origin in segment.original_intervals]
        dead_origins = [origin for segment in dead for origin in segment.original_intervals]
        if dead:
            dead_coverage = (
                (max(start, a), min(end, b))
                for a, b in dead_origins
                if a < end and start < b
            )
            if (
                live
                or any(segment.proof is Proof.UNPROVEN for segment in dead)
                or not _covers(dead_coverage, target)
            ):
                return DescendantAnswer(DescendantKind.UNPROVEN)
            return DescendantAnswer(DescendantKind.NO_SURVIVING_DESCENDANT)
        if not live or not _covers(live_origins, target):
            return DescendantAnswer(DescendantKind.UNPROVEN)
        intervals = _merged(
            segment.current_span
            for segment in live
            if segment.current_span is not None and not segment.placeholder
        )
        if not intervals:
            return DescendantAnswer(DescendantKind.NO_SURVIVING_DESCENDANT)
        if len(intervals) > 1:
            return DescendantAnswer(DescendantKind.MULTIPLE_DESCENDANTS, intervals)
        exact = all(
            segment.kind is SegmentKind.ORIGINAL and segment.proof is Proof.EXACT
            for segment in live
        )
        return DescendantAnswer(
            DescendantKind.EXACT_DESCENDANT if exact else DescendantKind.TRANSFORMED_DESCENDANT,
            intervals,
        )

    def origin_of_seam_span(
        self,
        start: int,
        end: int,
        *,
        request_id: str,
        raw_sha256: str,
        seam_sha256: str,
    ) -> OriginAnswer:
        self._validate(
            request_id=request_id,
            raw_sha256=raw_sha256,
            seam_sha256=seam_sha256,
        )
        _validate_query_span(start, end, len(self.current_text), "seam")
        selected = [
            segment
            for segment in self.segments
            if segment.current_span is not None
            and segment.current_span[0] < end
            and start < segment.current_span[1]
        ]
        if not selected:
            return OriginAnswer(OriginKind.UNPROVEN)
        if any(
            segment.proof is Proof.UNPROVEN
            or segment.current_span is None
            or segment.current_span[0] < start
            or segment.current_span[1] > end
            for segment in selected
        ):
            return OriginAnswer(OriginKind.UNPROVEN)
        if all(not segment.original_intervals for segment in selected):
            return OriginAnswer(OriginKind.INSERTED)
        if any(not segment.original_intervals for segment in selected):
            return OriginAnswer(OriginKind.UNPROVEN)
        origins = _merged(
            origin for segment in selected for origin in segment.original_intervals
        )
        if len(origins) > 1:
            return OriginAnswer(OriginKind.MULTIPLE_ORIGINS, origins)
        exact = all(
            segment.kind is SegmentKind.ORIGINAL and segment.proof is Proof.EXACT
            for segment in selected
        )
        return OriginAnswer(
            OriginKind.EXACT_ORIGIN if exact else OriginKind.TRANSFORMED_ORIGIN,
            origins,
        )


class TextLineageDraft:
    """Mutable request-local builder; ``seal`` returns the immutable view."""

    def __init__(self, raw_text: str, *, request_id: str):
        if not isinstance(raw_text, str):
            raise ValueError("raw_text must be str")
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("request_id must be non-empty")
        self.request_id = request_id
        self.raw_text = raw_text
        self.raw_sha256 = sha256_text(raw_text)
        self._pieces = [
            LineageSegment(
                segment_id=f"raw:{index}",
                text=char,
                current_span=None,
                original_intervals=((index, index + 1),),
                kind=SegmentKind.ORIGINAL,
                proof=Proof.EXACT,
                transformation_id="raw",
            )
            for index, char in enumerate(raw_text)
        ]
        self._tombstones: list[LineageSegment] = []
        self._events: list[LineageEvent] = []

    @property
    def current_text(self) -> str:
        return "".join(piece.text for piece in self._pieces)

    def _split_piece(self, piece: LineageSegment, offset: int) -> tuple[LineageSegment, LineageSegment]:
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

    def _cut(self, start: int, end: int) -> tuple[list[LineageSegment], list[LineageSegment], list[LineageSegment]]:
        before: list[LineageSegment] = []
        inside: list[LineageSegment] = []
        after: list[LineageSegment] = []
        cursor = 0
        for piece in self._pieces:
            piece_start, piece_end = cursor, cursor + len(piece.text)
            cursor = piece_end
            if piece_end <= start:
                before.append(piece)
                continue
            if piece_start >= end:
                after.append(piece)
                continue
            current = piece
            if piece_start < start:
                left, current = self._split_piece(current, start - piece_start)
                before.append(left)
                piece_start = start
            if piece_end > end:
                current, right = self._split_piece(current, end - piece_start)
                after.append(right)
            inside.append(current)
        return before, inside, after

    def _append_event(
        self,
        *,
        stage: str,
        operation: str,
        input_text: str,
        output_text: str,
        input_intervals: Iterable[tuple[int, int]],
        output_intervals: Iterable[tuple[int, int]],
        receipt_manifest_sha256: str,
    ) -> str:
        sequence = len(self._events) + 1
        previous = self._events[-1].event_sha256 if self._events else "0" * 64
        payload = {
            "sequence": sequence,
            "stage": stage,
            "operation": operation,
            "input_sha256": sha256_text(input_text),
            "output_sha256": sha256_text(output_text),
            "affected_input_intervals": _merged(input_intervals),
            "affected_output_intervals": _merged(output_intervals),
            "receipt_manifest_sha256": receipt_manifest_sha256,
            "previous_event_sha256": previous,
        }
        event_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        self._events.append(LineageEvent(**payload, event_sha256=event_hash))
        return f"{stage}:{sequence}:{event_hash[:12]}"

    def replace_span(
        self,
        start: int,
        end: int,
        replacement: str,
        *,
        stage: str,
        operation: str,
        proof: Proof = Proof.WHOLE_SEGMENT_ONLY,
        placeholder: bool = False,
    ) -> str:
        input_text = self.current_text
        if start < 0 or end <= start or end > len(input_text):
            raise ValueError(f"invalid mutation span {(start, end)!r}")
        output_text = f"{input_text[:start]}{replacement}{input_text[end:]}"
        if operation not in _PARTITION_OPERATIONS:
            raise ValueError(f"unsupported mutation operation: {operation!r}")
        if not isinstance(proof, Proof):
            raise ValueError("mutation proof must be a Proof value")
        if operation == "KEEP":
            if output_text != input_text:
                raise ValueError("KEEP cannot change text")
            return input_text
        if operation == "REPLACE" and input_text[start:end] == replacement:
            raise ValueError("same-surface REPLACE cannot preserve old identity")
        before, inside, after = self._cut(start, end)
        effective_proof = _weakest_proof(proof, inside)
        origins = _merged(
            origin for piece in inside for origin in piece.original_intervals
        )
        effective_placeholder = operation == "MASK" or placeholder
        receipt = PartitionReceipt(
            input_span=(start, end),
            output_span=(start, start + len(replacement)),
            input_text=input_text[start:end],
            output_text=replacement,
            operation=operation,
            proof=proof,
        )
        transformation_id = self._append_event(
            stage=stage,
            operation=operation,
            input_text=input_text,
            output_text=output_text,
            input_intervals=((start, end),),
            output_intervals=((start, start + len(replacement)),) if replacement else (),
            receipt_manifest_sha256=_receipt_manifest_sha256(
                ((receipt, effective_placeholder),)
            ),
        )
        if operation in {"MASK", "DELETE", "REPLACE"} and origins:
            self._tombstones.append(LineageSegment(
                segment_id=f"consumed:{transformation_id}",
                text=input_text[start:end],
                current_span=None,
                original_intervals=origins,
                kind=SegmentKind.CONSUMED,
                proof=effective_proof,
                transformation_id=transformation_id,
                placeholder=effective_placeholder,
            ))
        emitted: list[LineageSegment] = []
        if replacement:
            inserted = operation == "REPLACE" or effective_placeholder
            emitted.append(LineageSegment(
                segment_id=f"derived:{transformation_id}",
                text=replacement,
                current_span=None,
                original_intervals=() if inserted else origins,
                kind=(
                    SegmentKind.INSERTED
                    if inserted or not origins
                    else SegmentKind.DERIVED_FROM_ORIGINAL
                ),
                proof=effective_proof,
                transformation_id=transformation_id,
                placeholder=effective_placeholder,
            ))
        self._pieces = [*before, *emitted, *after]
        return output_text

    def apply_partition(
        self,
        output_text: str,
        receipts: Iterable[PartitionReceipt],
        *,
        stage: str,
        operation: str,
    ) -> str:
        input_text = self.current_text
        if not isinstance(output_text, str):
            raise ValueError("partition output must be str")
        receipts = tuple(receipts)
        input_cursor = output_cursor = 0
        new_pieces: list[LineageSegment] = []
        pending: list[tuple[PartitionReceipt, list[LineageSegment]]] = []
        for receipt in receipts:
            if not isinstance(receipt, PartitionReceipt):
                raise ValueError("partition entries must be PartitionReceipt values")
            input_span = _validate_partition_span(
                receipt.input_span,
                len(input_text),
                "partition input",
            )
            output_span = _validate_partition_span(
                receipt.output_span,
                len(output_text),
                "partition output",
            )
            if input_span[0] != input_cursor or output_span[0] != output_cursor:
                raise LineageMismatchError("partition receipts are not contiguous")
            if input_text[slice(*input_span)] != receipt.input_text:
                raise LineageMismatchError("partition input receipt does not match parser text")
            if output_text[slice(*output_span)] != receipt.output_text:
                raise LineageMismatchError("partition output receipt does not match parser text")
            if receipt.operation not in _PARTITION_OPERATIONS:
                raise ValueError(f"unsupported partition operation: {receipt.operation!r}")
            if not isinstance(receipt.proof, Proof):
                raise ValueError("partition proof must be a Proof value")
            if not isinstance(receipt.input_text, str) or not isinstance(
                receipt.output_text,
                str,
            ):
                raise ValueError("partition receipt text must be str")
            input_length = input_span[1] - input_span[0]
            output_length = output_span[1] - output_span[0]
            if receipt.operation == "KEEP" and receipt.input_text != receipt.output_text:
                raise ValueError("KEEP receipt cannot change text")
            if receipt.operation == "MASK" and (
                input_length == 0 or output_length == 0
            ):
                raise ValueError("MASK receipt requires input and placeholder output")
            if receipt.operation == "DELETE" and (
                input_length == 0 or output_length != 0
            ):
                raise ValueError("DELETE receipt requires input and no output")
            if receipt.operation == "INSERT" and (
                input_length != 0 or output_length == 0
            ):
                raise ValueError("INSERT receipt requires no input and inserted output")
            if receipt.operation == "REPLACE" and (
                input_length == 0
                or output_length == 0
                or receipt.input_text == receipt.output_text
            ):
                raise ValueError("REPLACE receipt requires distinct input and output")
            if receipt.operation in {"CORRECT", "MERGE", "NORMALIZE"} and (
                input_length == 0 and output_length == 0
            ):
                raise ValueError(f"{receipt.operation} receipt cannot be empty")
            _, pieces, _ = (
                self._cut(*input_span)
                if input_span[1] > input_span[0]
                else ([], [], [])
            )
            pending.append((receipt, pieces))
            input_cursor = input_span[1]
            output_cursor = output_span[1]
        if input_cursor != len(input_text) or output_cursor != len(output_text):
            raise LineageMismatchError("partition receipts do not cover the transformation")
        _validate_partition_envelope(operation, receipts)
        if all(receipt.operation == "KEEP" for receipt in receipts):
            return input_text
        transformation_id = self._append_event(
            stage=stage,
            operation=operation,
            input_text=input_text,
            output_text=output_text,
            input_intervals=(receipt.input_span for receipt in receipts),
            output_intervals=(receipt.output_span for receipt in receipts if receipt.output_text),
            receipt_manifest_sha256=_receipt_manifest_sha256(
                (receipt, receipt.operation == "MASK") for receipt in receipts
            ),
        )
        for receipt, pieces in pending:
            origins = _merged(origin for piece in pieces for origin in piece.original_intervals)
            effective_proof = _weakest_proof(receipt.proof, pieces)
            if receipt.operation == "KEEP":
                new_pieces.extend(pieces)
                continue
            consumes_origin = receipt.operation in {"MASK", "DELETE", "REPLACE"}
            if consumes_origin and origins:
                self._tombstones.append(LineageSegment(
                    segment_id=f"consumed:{transformation_id}:{receipt.input_span[0]}",
                    text=receipt.input_text,
                    current_span=None,
                    original_intervals=origins,
                    kind=SegmentKind.CONSUMED,
                    proof=effective_proof,
                    transformation_id=transformation_id,
                    placeholder=receipt.operation == "MASK",
                ))
            if not receipt.output_text:
                if origins and not consumes_origin:
                    self._tombstones.append(LineageSegment(
                        segment_id=f"consumed:{transformation_id}:{receipt.input_span[0]}",
                        text=receipt.input_text,
                        current_span=None,
                        original_intervals=origins,
                        kind=SegmentKind.CONSUMED,
                        proof=effective_proof,
                        transformation_id=transformation_id,
                    ))
                continue
            inserted = receipt.operation in {"MASK", "INSERT", "REPLACE"}
            new_pieces.append(LineageSegment(
                segment_id=f"derived:{transformation_id}:{receipt.output_span[0]}",
                text=receipt.output_text,
                current_span=None,
                original_intervals=() if inserted else origins,
                kind=(
                    SegmentKind.INSERTED
                    if inserted or not origins
                    else SegmentKind.DERIVED_FROM_ORIGINAL
                ),
                proof=effective_proof,
                transformation_id=transformation_id,
                placeholder=receipt.operation == "MASK",
            ))
        self._pieces = new_pieces
        return output_text

    def apply_normalization(self, normalized_text: str) -> str:
        raw_units = [
            "".join(
                char
                for char in unicodedata.normalize("NFKD", raw_char)
                if not unicodedata.combining(char)
            ).lower()
            for raw_char in self.raw_text
        ]
        unstripped = "".join(raw_units)
        if unstripped.strip() != normalized_text:
            return self.replace_span(
                0,
                len(self.current_text),
                normalized_text,
                stage="normalize_text",
                operation="NORMALIZE",
                proof=Proof.UNPROVEN,
            )
        receipts: list[PartitionReceipt] = []
        output_cursor = 0
        for index, output_unit in enumerate(raw_units):
            output_span = (output_cursor, output_cursor + len(output_unit))
            receipts.append(PartitionReceipt(
                input_span=(index, index + 1),
                output_span=output_span,
                input_text=self.raw_text[index:index + 1],
                output_text=output_unit,
                operation="KEEP" if output_unit == self.raw_text[index:index + 1] else "NORMALIZE",
                proof=Proof.EXACT if len(output_unit) == 1 else Proof.WHOLE_SEGMENT_ONLY,
            ))
            output_cursor = output_span[1]
        self.apply_partition(
            unstripped,
            receipts,
            stage="normalize_text",
            operation="NORMALIZE",
        )
        trailing = len(self.current_text.rstrip())
        if trailing < len(self.current_text):
            self.replace_span(
                trailing,
                len(self.current_text),
                "",
                stage="normalize_text",
                operation="DELETE",
                proof=Proof.EXACT,
            )
        leading = len(self.current_text) - len(self.current_text.lstrip())
        if leading:
            self.replace_span(
                0,
                leading,
                "",
                stage="normalize_text",
                operation="DELETE",
                proof=Proof.EXACT,
            )
        if self.current_text != normalized_text:
            raise LineageMismatchError("normalization output diverged from normalize_text")
        return normalized_text

    def seal(self, seam_text: str) -> TextLineageView:
        if seam_text != self.current_text:
            raise LineageMismatchError("CONTROL seam text does not match lineage current text")
        cursor = 0
        segments: list[LineageSegment] = []
        for piece in self._pieces:
            current_span = (cursor, cursor + len(piece.text))
            segments.append(replace(piece, current_span=current_span))
            cursor = current_span[1]
        chain_head = self._events[-1].event_sha256 if self._events else "0" * 64
        return TextLineageView(
            request_id=self.request_id,
            raw_text=self.raw_text,
            raw_sha256=self.raw_sha256,
            current_text=seam_text,
            current_sha256=sha256_text(seam_text),
            lineage_version=LINEAGE_VERSION,
            segments=tuple(segments),
            tombstones=tuple(self._tombstones),
            events=tuple(self._events),
            event_chain_head=chain_head,
        )


def normalize_query_with_lineage(text: str, lineage: TextLineageDraft) -> str:
    """Run the canonical normalizer once and record its causal transformation."""
    from app.utils.money import normalize_text

    normalized = normalize_text(text)
    return lineage.apply_normalization(normalized)
