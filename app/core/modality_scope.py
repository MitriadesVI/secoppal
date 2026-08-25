"""Scopes de polaridad y coordinación para menciones de modalidad M2.

La exclusión se propaga por listas coordinadas con conjunciones o comas débiles,
y se reinicia por cláusulas contrastivas/fuertes o por una nueva construcción
lingüística no coordinada. Las anti-negaciones se evalúan antes de ``no``/``sin``.
"""
from __future__ import annotations

from app.core.modality_composition import Mention
from app.core.negation_markers import (
    ANTI_NEGATION_PATTERNS,
    EXTERNAL_NEGATIVE_OPERATORS,
)
from app.core.modality_resolution_models import MatchPolarity
from app.core.modality_text import Clause, contains_phrase, is_separator_only

_COORDINATORS = ("y", "o", "ni")


def _contains_any(text: str, phrases: tuple[str, ...]) -> bool:
    return any(contains_phrase(text, phrase) for phrase in phrases)


def _mask_spans(text: str, spans: tuple[tuple[int, int], ...]) -> str:
    """Neutraliza contenido técnico sin alterar offsets ni separadores externos."""
    if not spans:
        return text
    chars = list(text)
    for start, end in spans:
        if start < 0 or end < start or end > len(chars):
            raise ValueError(f"span normalizado inválido: {(start, end)!r}")
        chars[start:end] = " " * (end - start)
    return "".join(chars)


def assign_polarities(
    norm: str,
    mentions: list[Mention],
    clauses: tuple[Clause, ...],
    qualifier_spans: tuple[tuple[int, int], ...] = (),
) -> tuple[MatchPolarity, ...]:
    """Asigna polaridad sin interpretar qualifiers técnicos como negación."""
    operator_text = _mask_spans(norm, qualifier_spans)
    result: dict[Mention, MatchPolarity] = {}
    for clause in clauses:
        local = sorted(
            [mention for mention in mentions if mention.clause_index == clause.index],
            key=lambda mention: (mention.norm_start, mention.norm_end, mention.target_id),
        )
        cursor = clause.norm_start
        active_negative = False
        previous: Mention | None = None

        for mention in local:
            gap = norm[cursor:mention.norm_start]
            operator_gap = operator_text[cursor:mention.norm_start]
            anti_negation = _contains_any(operator_gap, ANTI_NEGATION_PATTERNS)
            negative_operator = _contains_any(operator_gap, EXTERNAL_NEGATIVE_OPERATORS)
            coordinated = _contains_any(gap, _COORDINATORS)

            if anti_negation:
                active_negative = False
            elif negative_operator:
                active_negative = True
            elif previous is not None and active_negative:
                # Una coma desaparece al normalizar y deja un separador vacío.
                # Eso mantiene el scope de listas A, B y C. Cualquier texto
                # léxico no coordinado inicia una construcción nueva y resetea.
                if not coordinated and not is_separator_only(gap):
                    active_negative = False

            result[mention] = (
                MatchPolarity.EXCLUDE if active_negative else MatchPolarity.INCLUDE
            )
            previous = mention
            cursor = mention.norm_end

    return tuple(result[mention] for mention in mentions)
