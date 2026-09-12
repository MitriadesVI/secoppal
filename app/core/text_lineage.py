"""Request-scoped causal lineage from raw query text to the CONTROL seam.

Lineage is updated by the same mutation primitives that produce parser text.
It never searches a later surface to infer where an earlier occurrence went.
"""
from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass
from enum import Enum
from typing import Iterable


LINEAGE_VERSION = "TEXT_LINEAGE_V1_2"
LINEAGE_EVENT_SCHEMA = "TEXT_LINEAGE_EVENT_V1_2"


class LineageOperation(str, Enum):
    KEEP = "KEEP"
    MASK = "MASK"
    DELETE = "DELETE"
    INSERT = "INSERT"
    REPLACE = "REPLACE"
    CORRECT = "CORRECT"
    MERGE = "MERGE"
    NORMALIZE = "NORMALIZE"


class _PlaceholderUnset:
    __slots__ = ()


PLACEHOLDER_UNSET = _PlaceholderUnset()


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
    placeholder: object = PLACEHOLDER_UNSET


@dataclass(frozen=True, slots=True)
class CanonicalReceipt:
    """Validated immutable receipt retained in an event manifest."""

    input_span: tuple[int, int]
    output_span: tuple[int, int]
    input_text: str
    output_text: str
    operation: LineageOperation
    proof: Proof
    placeholder: bool


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
    schema: str
    sequence: int
    stage: str
    operation: LineageOperation
    input_sha256: str
    output_sha256: str
    affected_input_intervals: tuple[tuple[int, int], ...]
    affected_output_intervals: tuple[tuple[int, int], ...]
    receipt_manifest: tuple[CanonicalReceipt, ...]
    receipt_manifest_sha256: str
    post_state_sha256: str
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


def _exact_linear_projection(
    segment: LineageSegment,
    start: int,
    end: int,
    *,
    from_raw: bool,
) -> tuple[tuple[int, int], tuple[int, int]] | None:
    if (
        segment.kind is not SegmentKind.ORIGINAL
        or segment.proof is not Proof.EXACT
        or segment.current_span is None
        or len(segment.original_intervals) != 1
    ):
        return None
    raw_start, raw_end = segment.original_intervals[0]
    seam_start, seam_end = segment.current_span
    if raw_end - raw_start != seam_end - seam_start:
        return None
    if from_raw:
        clipped_start, clipped_end = max(start, raw_start), min(end, raw_end)
        if clipped_start >= clipped_end:
            return None
        seam_clip = (
            seam_start + clipped_start - raw_start,
            seam_start + clipped_end - raw_start,
        )
        return (clipped_start, clipped_end), seam_clip
    clipped_start, clipped_end = max(start, seam_start), min(end, seam_end)
    if clipped_start >= clipped_end:
        return None
    raw_clip = (
        raw_start + clipped_start - seam_start,
        raw_start + clipped_end - seam_start,
    )
    return raw_clip, (clipped_start, clipped_end)


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
    state_sha256: str

    def verify_integrity(self) -> None:
        """Replay the immutable history and reject any inconsistent field."""
        from app.core.text_lineage_reducer import verify_view_integrity

        verify_view_integrity(self)

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
        live_origins: list[tuple[int, int]] = []
        live_seams: list[tuple[int, int]] = []
        for segment in live:
            projection = _exact_linear_projection(
                segment,
                start,
                end,
                from_raw=True,
            )
            extends_target = any(
                a < start or b > end for a, b in segment.original_intervals
            )
            if segment.proof is Proof.UNPROVEN or extends_target and projection is None:
                return DescendantAnswer(DescendantKind.UNPROVEN)
            if projection is not None:
                raw_interval, seam_interval = projection
                live_origins.append(raw_interval)
                if not segment.placeholder:
                    live_seams.append(seam_interval)
            else:
                live_origins.extend(segment.original_intervals)
                if segment.current_span is not None and not segment.placeholder:
                    live_seams.append(segment.current_span)
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
        intervals = _merged(live_seams)
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
        selected_origins: list[tuple[int, int]] = []
        for segment in selected:
            projection = _exact_linear_projection(
                segment,
                start,
                end,
                from_raw=False,
            )
            extends_target = (
                segment.current_span is None
                or segment.current_span[0] < start
                or segment.current_span[1] > end
            )
            if segment.proof is Proof.UNPROVEN or extends_target and projection is None:
                return OriginAnswer(OriginKind.UNPROVEN)
            if projection is not None:
                selected_origins.append(projection[0])
            else:
                selected_origins.extend(segment.original_intervals)
        if all(not segment.original_intervals for segment in selected):
            return OriginAnswer(OriginKind.INSERTED)
        if any(not segment.original_intervals for segment in selected):
            return OriginAnswer(OriginKind.UNPROVEN)
        origins = _merged(selected_origins)
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
        from app.core.text_lineage_reducer import initial_state

        state = initial_state(raw_text, request_id=request_id)
        self.request_id = request_id
        self.raw_text = raw_text
        self.raw_sha256 = sha256_text(raw_text)
        self._pieces = list(state.pieces)
        self._tombstones = list(state.tombstones)
        self._events = list(state.events)

    @property
    def current_text(self) -> str:
        return "".join(piece.text for piece in self._pieces)

    def replace_span(
        self,
        start: int,
        end: int,
        replacement: str,
        *,
        stage: str,
        operation: str,
        proof: Proof = Proof.WHOLE_SEGMENT_ONLY,
        placeholder: object = PLACEHOLDER_UNSET,
    ) -> str:
        input_text = self.current_text
        if any(isinstance(value, bool) or not isinstance(value, int) for value in (start, end)):
            raise ValueError("mutation span indices must be integers")
        if start < 0 or end <= start or end > len(input_text):
            raise ValueError(f"invalid mutation span {(start, end)!r}")
        if not isinstance(replacement, str):
            raise ValueError("replacement must be str")
        output_text = f"{input_text[:start]}{replacement}{input_text[end:]}"
        receipts = (
            PartitionReceipt(
                input_span=(0, start),
                output_span=(0, start),
                input_text=input_text[:start],
                output_text=input_text[:start],
                operation="KEEP",
                proof=Proof.EXACT,
            ),
            PartitionReceipt(
                input_span=(start, end),
                output_span=(start, start + len(replacement)),
                input_text=input_text[start:end],
                output_text=replacement,
                operation=operation,
                proof=proof,
                placeholder=placeholder,
            ),
            PartitionReceipt(
                input_span=(end, len(input_text)),
                output_span=(start + len(replacement), len(output_text)),
                input_text=input_text[end:],
                output_text=input_text[end:],
                operation="KEEP",
                proof=Proof.EXACT,
            ),
        )
        return self.apply_partition(
            output_text,
            receipts,
            stage=stage,
            operation=operation,
        )

    def apply_partition(
        self,
        output_text: str,
        receipts: Iterable[PartitionReceipt],
        *,
        stage: str,
        operation: str,
    ) -> str:
        from app.core.text_lineage_reducer import ReducerState, reduce_partition

        state = reduce_partition(
            ReducerState(
                request_id=self.request_id,
                raw_sha256=self.raw_sha256,
                current_text=self.current_text,
                pieces=tuple(self._pieces),
                tombstones=tuple(self._tombstones),
                events=tuple(self._events),
            ),
            output_text,
            receipts,
            stage=stage,
            operation=operation,
        )
        self._pieces, self._tombstones, self._events = (
            list(state.pieces),
            list(state.tombstones),
            list(state.events),
        )
        return state.current_text

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
            raw_unit = self.raw_text[index:index + 1]
            receipt_operation = (
                "KEEP"
                if output_unit == raw_unit
                else "DELETE"
                if not output_unit
                else "NORMALIZE"
            )
            receipts.append(PartitionReceipt(
                input_span=(index, index + 1),
                output_span=output_span,
                input_text=raw_unit,
                output_text=output_unit,
                operation=receipt_operation,
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
        from app.core.text_lineage_reducer import sealed_segments, state_sha256

        chain_head = self._events[-1].event_sha256 if self._events else "0" * 64
        view = TextLineageView(
            request_id=self.request_id,
            raw_text=self.raw_text,
            raw_sha256=self.raw_sha256,
            current_text=seam_text,
            current_sha256=sha256_text(seam_text),
            lineage_version=LINEAGE_VERSION,
            segments=sealed_segments(tuple(self._pieces)),
            tombstones=tuple(self._tombstones),
            events=tuple(self._events),
            event_chain_head=chain_head,
            state_sha256=state_sha256(
                self.request_id,
                self.raw_sha256,
                seam_text,
                tuple(self._pieces),
                tuple(self._tombstones),
            ),
        )
        view.verify_integrity()
        return view


def normalize_query_with_lineage(text: str, lineage: TextLineageDraft) -> str:
    """Run the canonical normalizer once and record its causal transformation."""
    from app.utils.money import normalize_text

    normalized = normalize_text(text)
    return lineage.apply_normalization(normalized)
