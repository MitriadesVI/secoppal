"""Integración única del resolver M2 con el parser determinístico."""
from __future__ import annotations

from dataclasses import dataclass

from app.core.modality_frame_adapter import (
    ModalityInterpretation,
    adapt_modality_resolution,
)
from app.core.modality_precedence import prepare_modality_text
from app.core.modality_resolver import resolve
from app.core.query_text_consumption import mask_consumed_spans

_DUAL_SCOPE = ("procesos", "contratos")


@dataclass(frozen=True)
class ParserModalityResult:
    """Resultado efímero del turno y su única proyección ejecutable."""

    interpretation: ModalityInterpretation
    executable_params: dict[str, list[str]]
    masked_text: str
    precedence_rule_ids: tuple[str, ...] = ()
    precedence_hidden_spans: tuple[tuple[int, int], ...] = ()


def resolve_parser_modality(
    text: str,
    *,
    dataset: str,
    dataset_scope: list[str] | tuple[str, ...] | None = None,
) -> ParserModalityResult:
    """Resuelve M2 exactamente una vez sobre ``text`` y enmascara sus spans."""
    resolver_dataset = (
        None if tuple(dataset_scope or ()) == _DUAL_SCOPE else dataset
    )
    precedence = prepare_modality_text(text)
    interpretation = adapt_modality_resolution(
        resolve(precedence.modality_text, dataset=resolver_dataset)
    )
    return ParserModalityResult(
        interpretation=interpretation,
        executable_params=interpretation.executable_filter.to_params(),
        masked_text=mask_consumed_spans(text, interpretation.consumed_spans),
        precedence_rule_ids=precedence.rule_ids,
        precedence_hidden_spans=precedence.hidden_spans,
    )
