"""Precedencia de producto para la vista de texto exclusiva de M2."""
from __future__ import annotations

from dataclasses import dataclass
import re

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


@dataclass(frozen=True)
class ModalityPrecedenceResult:
    """Vista para M2 y metadata efímera de los spans ocultados."""

    modality_text: str
    hidden_spans: tuple[tuple[int, int], ...]
    rule_ids: tuple[str, ...]


def prepare_modality_text(text: str) -> ModalityPrecedenceResult:
    """Oculta solo spans donde intent/estado tienen precedencia sobre M2."""
    if not isinstance(text, str):
        raise ValueError(
            f"text debe ser str (recibido {type(text).__name__})"
        )

    hidden: list[tuple[int, int, str]] = []
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
        modality_text=mask_consumed_spans(text, spans),
        hidden_spans=spans,
        rule_ids=tuple(rule_id for _, _, rule_id in hidden),
    )
