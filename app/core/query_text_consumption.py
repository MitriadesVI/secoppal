"""Consumo exacto de texto por offsets (M3B-1).

``mask_consumed_spans`` enmascara con espacios ÚNICAMENTE los caracteres dentro
de los spans dados, operando sobre offsets del texto original recibido (sin
renormalizar Unicode). Preserva exactamente ``len(text)`` y jamás usa
``str.replace`` — así una segunda aparición textual del mismo fragmento que NO
esté cubierta por un span permanece intacta.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import re
from typing import TYPE_CHECKING

from app.core.text_lineage import LineageOperation, PLACEHOLDER_UNSET, Proof

if TYPE_CHECKING:
    from app.core.text_lineage import TextLineageDraft

Span = tuple[int, int]


def _validate_span(span: object, text_len: int) -> Span:
    """Valida forma y límites de un span; devuelve ``(start, end)`` normalizado."""
    if type(span) not in (tuple, list):
        raise ValueError("span con forma inválida")
    if len(span) != 2:
        raise ValueError("span debe tener exactamente 2 elementos")
    start, end = span
    for label, value in (("start", start), ("end", end)):
        if type(value) is not int:
            raise ValueError(f"{label} debe ser un int exacto")
    if start < 0:
        raise ValueError("start negativo")
    if end > text_len:
        raise ValueError("end fuera de rango")
    if end <= start:
        raise ValueError("end debe ser > start")
    return (start, end)


def _merge_spans(spans: list[Span]) -> list[Span]:
    """Ordena y fusiona spans solapados o adyacentes."""
    merged: list[Span] = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1]:  # solapado o adyacente
            prev_start, prev_end = merged[-1]
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))
    return merged


def replace_text_span(
    text: str,
    span: Span,
    replacement: str,
    *,
    lineage: TextLineageDraft | None = None,
    stage: str = "text_replacement",
    operation: str = "REPLACE",
    proof: Proof = Proof.WHOLE_SEGMENT_ONLY,
    placeholder: object = PLACEHOLDER_UNSET,
) -> str:
    """Apply one exact current-text mutation and its causal receipt."""
    if type(text) is not str:
        raise ValueError("text must be an exact str")
    if type(replacement) is not str:
        raise ValueError("replacement must be str")
    if type(span) not in (tuple, list):
        raise ValueError("span must be an exact tuple or list")
    if lineage is not None:
        from app.core.text_lineage import TextLineageDraft

        if type(lineage) is not TextLineageDraft:
            raise ValueError("lineage must be an exact TextLineageDraft")
        if type(stage) is not str:
            raise ValueError("stage must be an exact str")
        if type(operation) not in (str, LineageOperation):
            raise ValueError("operation must be an exact str or LineageOperation")
        if type(proof) is not Proof:
            raise ValueError("proof must be an exact Proof")
        if placeholder is not PLACEHOLDER_UNSET and type(placeholder) is not bool:
            raise ValueError("placeholder must be unset or an exact bool")
    start, end = _validate_span(span, len(text))
    output = f"{text[:start]}{replacement}{text[end:]}"
    if lineage is not None:
        if lineage.current_text != text:
            raise ValueError("lineage current text does not match mutation input")
        recorded = lineage.replace_span(
            start,
            end,
            replacement,
            stage=stage,
            operation=operation,
            proof=proof,
            placeholder=placeholder,
        )
        if recorded != output:
            raise ValueError("lineage mutation diverged from parser mutation")
    return output


def mask_consumed_spans(
    text: str,
    spans: Iterable[object],
    *,
    lineage: TextLineageDraft | None = None,
    stage: str = "mask_consumed_spans",
) -> str:
    """Reemplaza por espacios los caracteres dentro de ``spans`` sobre ``text``.

    Los offsets son sobre el ``text`` original recibido. El resultado conserva la
    misma longitud. Spans desordenados, adyacentes o solapados son válidos y se
    fusionan; formas o límites inválidos lanzan ``ValueError``.
    """
    if not isinstance(text, str):
        raise ValueError(f"text debe ser str (recibido {type(text).__name__})")

    text_len = len(text)
    validated = [_validate_span(span, text_len) for span in spans]
    if not validated:
        return text

    masked = text
    for start, end in reversed(_merge_spans(validated)):
        masked = replace_text_span(
            masked,
            (start, end),
            " " * (end - start),
            lineage=lineage,
            stage=stage,
            operation="MASK",
            proof=Proof.EXACT,
            placeholder=True,
        )
    return masked


def replace_first_literal(
    text: str,
    old: str,
    replacement: str,
    *,
    lineage: TextLineageDraft | None = None,
    stage: str,
    operation: str = "REPLACE",
    proof: Proof = Proof.WHOLE_SEGMENT_ONLY,
    placeholder: object = PLACEHOLDER_UNSET,
) -> str:
    """Preserve ``str.replace(..., 1)`` and receipt its chosen occurrence."""
    start = text.find(old)
    if start < 0 or not old:
        return text
    return replace_text_span(
        text,
        (start, start + len(old)),
        replacement,
        lineage=lineage,
        stage=stage,
        operation=operation,
        proof=proof,
        placeholder=placeholder,
    )


def replace_regex(
    text: str,
    pattern: str | re.Pattern[str],
    replacement: str,
    *,
    count: int = 0,
    flags: int = 0,
    lineage: TextLineageDraft | None = None,
    stage: str,
    operation: str = "REPLACE",
    proof: Proof = Proof.WHOLE_SEGMENT_ONLY,
) -> str:
    """Apply static regex replacement from the matches driving the output."""
    compiled = re.compile(pattern, flags) if isinstance(pattern, str) else pattern
    matches = list(compiled.finditer(text))
    if count:
        matches = matches[:count]
    output = text
    for match in reversed(matches):
        output = replace_text_span(
            output,
            match.span(),
            replacement,
            lineage=lineage,
            stage=stage,
            operation=operation,
            proof=proof,
        )
    return output


def compact_whitespace(
    text: str,
    *,
    lineage: TextLineageDraft | None = None,
    stage: str = "whitespace_compaction",
) -> str:
    r"""Implement ``re.sub(r"\s+", " ", text).strip()`` with receipts."""
    compacted = replace_regex(
        text,
        re.compile(r"\s+"),
        " ",
        lineage=lineage,
        stage=stage,
        operation="MERGE",
    )
    if compacted.endswith(" "):
        compacted = replace_text_span(
            compacted,
            (len(compacted) - 1, len(compacted)),
            "",
            lineage=lineage,
            stage=stage,
            operation="DELETE",
            proof=Proof.EXACT,
        )
    if compacted.startswith(" "):
        compacted = replace_text_span(
            compacted,
            (0, 1),
            "",
            lineage=lineage,
            stage=stage,
            operation="DELETE",
            proof=Proof.EXACT,
        )
    return compacted


@dataclass(frozen=True, slots=True)
class ShieldedText:
    text: str
    replacements: dict[str, str]
    original_intervals: tuple[tuple[tuple[int, int], ...], ...]


def shield_spans_with_origins(text: str, spans: Iterable[object]) -> ShieldedText:
    """Shield spans while retaining the exact input identity of every marker."""
    validated = [_validate_span(span, len(text)) for span in spans]
    replacements: dict[str, str] = {}
    units = [(char, ((index, index + 1),)) for index, char in enumerate(text)]
    for index, (start, end) in enumerate(reversed(_merge_spans(validated))):
        marker = chr(0xE000 + index)
        while marker in text or marker in replacements:
            marker += chr(0xE000)
        replacements[marker] = text[start:end]
        marker_units = [(char, ((start, end),)) for char in marker]
        units = [*units[:start], *marker_units, *units[end:]]
    return ShieldedText(
        text="".join(char for char, _ in units),
        replacements=replacements,
        original_intervals=tuple(origins for _, origins in units),
    )


def shield_spans(text: str, spans: Iterable[object]) -> tuple[str, dict[str, str]]:
    """Replace spans with opaque markers for a consumer that changes offsets.

    Unlike whitespace masking, a marker prevents a command from bridging two
    sides of a protected name. The caller restores each marker after parsing.
    """
    shielded = shield_spans_with_origins(text, spans)
    return shielded.text, shielded.replacements
