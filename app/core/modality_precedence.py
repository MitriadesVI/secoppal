"""Precedencia de producto para la vista de texto exclusiva de M2."""
from __future__ import annotations

from dataclasses import dataclass
import re

from app.core.modality_followup import (
    _CLOSED as _FOLLOWUP_CLOSED,
    _at_logical_start,
    _normalize as _normalize_followup_text,
)
from app.core.query_text_consumption import mask_consumed_spans

_PUBLIC_TENDER_OPEN_RE = re.compile(
    r"\b(?:licitacion publica|licitaciones publicas)"
    r"\s+(?P<state>abiert(?:a|o)s?)\b"
)
_GENERIC_OPEN_TENDER_RE = re.compile(
    r"\b(?:licitacion|licitaciones)\s+abiert(?:a|o)s?\b"
)

_RULE_PUBLIC_TENDER_STATE = "public_tender_open_state"
_RULE_GENERIC_OPEN_TENDER = "generic_open_tender"
_RULE_CLEAR_THEN_REPLACE = "clear_then_replace_prefix"

# Reutiliza el descriptor contractual del clasificador: no hay una segunda
# expresión regular que pueda divergir de ``clear_then_replace``.
_CLEAR_THEN_REPLACE_RE = next(
    pattern
    for pattern, _, construction_id in _FOLLOWUP_CLOSED
    if construction_id == "clear_then_replace"
)


@dataclass(frozen=True)
class ModalityPrecedenceResult:
    """Vista para M2 y metadata efímera de los spans ocultados."""

    modality_text: str
    hidden_spans: tuple[tuple[int, int], ...]
    rule_ids: tuple[str, ...]


def _mask_preserving_linebreaks(
    text: str, spans: tuple[tuple[int, int], ...]
) -> str:
    """Enmascara spans sin convertir CR/LF en tokens ni alterar sus offsets."""
    masked = list(mask_consumed_spans(text, spans))
    for index, char in enumerate(text):
        if char in "\r\n":
            masked[index] = char
    return "".join(masked)


def prepare_modality_text(text: str) -> ModalityPrecedenceResult:
    """Oculta solo spans donde intent/estado tienen precedencia sobre M2."""
    if not isinstance(text, str):
        raise ValueError(
            f"text debe ser str (recibido {type(text).__name__})"
        )

    hidden: list[tuple[int, int, str]] = []
    normalized = _normalize_followup_text(text)
    clear_then_replace = _CLEAR_THEN_REPLACE_RE.search(normalized)
    if (
        clear_then_replace
        and _at_logical_start(normalized, clear_then_replace.start())
    ):
        hidden.append(
            (*clear_then_replace.span(), _RULE_CLEAR_THEN_REPLACE)
        )

    for match in _PUBLIC_TENDER_OPEN_RE.finditer(text):
        start, end = match.span("state")
        hidden.append((start, end, _RULE_PUBLIC_TENDER_STATE))

    for match in _GENERIC_OPEN_TENDER_RE.finditer(text):
        hidden.append(
            (*match.span(), _RULE_GENERIC_OPEN_TENDER)
        )

    hidden.sort(key=lambda item: (item[0], item[1], item[2]))
    for previous, current in zip(hidden, hidden[1:]):
        if current[0] < previous[1]:
            raise RuntimeError(
                "las reglas de precedencia produjeron spans solapados"
            )

    spans = tuple((start, end) for start, end, _ in hidden)
    return ModalityPrecedenceResult(
        modality_text=_mask_preserving_linebreaks(text, spans),
        hidden_spans=spans,
        rule_ids=tuple(rule_id for _, _, rule_id in hidden),
    )
