"""Consumo exacto de texto por offsets (M3B-1).

``mask_consumed_spans`` enmascara con espacios ÚNICAMENTE los caracteres dentro
de los spans dados, operando sobre offsets del texto original recibido (sin
renormalizar Unicode). Preserva exactamente ``len(text)`` y jamás usa
``str.replace`` — así una segunda aparición textual del mismo fragmento que NO
esté cubierta por un span permanece intacta.
"""
from __future__ import annotations

from collections.abc import Iterable

Span = tuple[int, int]


def _validate_span(span: object, text_len: int) -> Span:
    """Valida forma y límites de un span; devuelve ``(start, end)`` normalizado."""
    if isinstance(span, (str, bytes)) or not isinstance(span, (tuple, list)):
        raise ValueError(f"span con forma inválida: {span!r}")
    if len(span) != 2:
        raise ValueError(f"span debe tener exactamente 2 elementos: {span!r}")
    start, end = span
    for label, value in (("start", start), ("end", end)):
        if isinstance(value, bool):
            raise ValueError(f"{label} bool no es un índice válido: {span!r}")
        if not isinstance(value, int):
            raise ValueError(f"{label} debe ser int: {span!r}")
    if start < 0:
        raise ValueError(f"start negativo: {span!r}")
    if end > text_len:
        raise ValueError(f"end fuera de rango (> len(text)={text_len}): {span!r}")
    if end <= start:
        raise ValueError(f"end debe ser > start: {span!r}")
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


def mask_consumed_spans(text: str, spans: Iterable[object]) -> str:
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

    chars = list(text)
    for start, end in _merge_spans(validated):
        for index in range(start, end):
            chars[index] = " "
    return "".join(chars)
