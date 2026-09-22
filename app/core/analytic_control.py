"""Analytic control language — what to do with the results, never what to find.

Sibling of :mod:`app.core.control_language` (ADR-066).  That module separates
ORDER BY commands from content; this one does the same for the two analytic
instructions the host can accredit today: **count** the results and **group**
them by a closed dimension (modality, reported state), plus the negated forms
that frame them ("no los agrupes por modalidad", "sin listar cada contrato").

Before this module ``Desglosa por modalidad los contratos de Galapa`` shipped
``objeto = [desglosa, modalidad, contratos]``; ``No los agrupes por modalidad;
cuéntame los contratos`` shipped ``objeto_exclude = [agrupes, modalidad]``; and
``por estado vigente reportado`` projected an active-state filter.  Each word
was an instruction about the result set and became a predicate on SECOP.

Contract
--------
``locate_analytic_controls`` is pure and span-based: it receives the normalized
turn and returns every occurrence it can accredit, with kind, dimension and
polarity, in offsets over that same text.  It never removes anything; the
parser consumes the spans (so they cannot become ``objeto``, state, ordering or
a negation target) and the effective operation is derived elsewhere
(:mod:`app.core.analytic_operation`).

INV-ANALYTIC-CONTROL-NOT-CONTENT
    An accredited span never reaches a content extractor.

INV-ANALYTIC-CONTROL-IS-CONTEXTUAL
    No global stopword list.  A dimension word is control only inside a
    criterion ("por modalidad", "en cada estado") of a turn that also carries
    a count or group command; a command word is control only when it governs
    such a criterion.  ``distribución de agua``, ``cuentas bancarias``,
    ``estado de la malla vial`` and ``objeto que mencione modalidades`` keep
    every token.

INV-ANALYTIC-CONTROL-YIELDS-TO-OWNED-SPANS
    A span already owned by another authority — a ranking criterion
    ("mayor número de contratos" is the SUPPLIERS metric, not a count), a
    proper name, a municipal name, a resolved modality alias — is never
    claimed.  Ownership is by span, not by capability name.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping


class AnalyticControlKind(str, Enum):
    COUNT = "COUNT"
    GROUP = "GROUP"
    # The two kinds below exist only in negated form and only because frozen
    # acceptance cases carry them: "sin listar cada contrato" (cc06) and
    # "no me des solo el total" (cm05).  Unrecognized, both became OBJECT
    # negations over the unit word.
    LIST = "LIST"
    TOTAL_ONLY = "TOTAL_ONLY"


class AnalyticDimension(str, Enum):
    MODALITY = "MODALITY"
    REPORTED_STATE = "REPORTED_STATE"


class ControlPolarity(str, Enum):
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"


@dataclass(frozen=True, slots=True)
class AnalyticControlSpan:
    """One accredited analytic instruction, in offsets over the parsed text."""

    kind: AnalyticControlKind
    dimension: AnalyticDimension | None
    polarity: ControlPolarity
    start: int
    end: int
    evidence_text: str

    def to_state(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "dimension": self.dimension.value if self.dimension else None,
            "polarity": self.polarity.value,
            "start": self.start,
            "end": self.end,
            "evidence_text": self.evidence_text,
        }

    @classmethod
    def from_state(cls, data: Mapping[str, object]) -> "AnalyticControlSpan":
        dimension = data.get("dimension")
        return cls(
            kind=AnalyticControlKind(str(data["kind"])),
            dimension=AnalyticDimension(str(dimension)) if dimension else None,
            polarity=ControlPolarity(str(data["polarity"])),
            start=int(data["start"]),  # type: ignore[arg-type]
            end=int(data["end"]),  # type: ignore[arg-type]
            evidence_text=str(data["evidence_text"]),
        )


def controls_from_state(items: Iterable[object] | None) -> tuple[AnalyticControlSpan, ...]:
    """Rebuild controls from Burr state (dicts) or pass typed spans through."""
    out: list[AnalyticControlSpan] = []
    for item in items or ():
        if isinstance(item, AnalyticControlSpan):
            out.append(item)
        elif isinstance(item, Mapping):
            out.append(AnalyticControlSpan.from_state(item))
    return tuple(out)


_F = re.IGNORECASE | re.VERBOSE

# A dimension may be followed by a genitive complement only when it opens a
# noun phrase of the turn ("por modalidad de la Alcaldía"); a bare "de" + noun
# makes it content ("por estado de emergencia").
_NOT_A_NOMINAL_COMPLEMENT = r"(?!\s+(?:de|del)\s+(?!(?:la|las|los|el)\b))"
_DIMENSIONS: dict[AnalyticDimension, str] = {
    AnalyticDimension.MODALITY: (
        r"(?:(?:su|la|las)\s+)?modalidad(?:es)?(?P<complement>\s+de\s+contratacion)?"
        + _NOT_A_NOMINAL_COMPLEMENT
    ),
    AnalyticDimension.REPORTED_STATE: (
        r"""(?:(?:el|su|los)\s+)?estados?
            (?:\s+(?:actualmente\s+)?(?:reportad[oa]s?|vigentes?|actual(?:es|mente)?|
                   observad[oa]s?(?:\s+al\s+consultar)?))*"""
        + _NOT_A_NOMINAL_COMPLEMENT
    ),
}
_UNIT = r"(?:contratos?|procesos?|contrataciones?)"
_GROUP_VERB = r"""(?:desglos(?:a|e|ar|alos|ad[oa]s?)|
    distribu(?:ye|ya|ir|id[oa]s?)|
    agrup(?:a|e|es|ar|alos|ad[oa]s?))"""
_GROUP_NOUN = r"(?:distribucion|desglose|reparto|agrupacion)"
_COUNT_VERB = r"(?:cuent(?:a|e|ame|anos|alos|elos))"

# "por / según / en cada / por cada / para cada" + DIM, optionally led by the
# copula of a distributive clause ("figuran hoy en cada estado").
_CRITERIA = {
    dim: re.compile(
        rf"""(?:\b(?:hay|figuran|estan|quedan)\s+(?:(?:hoy|actualmente)\s+)?)?
            \b(?:(?:por|en|para)\s+cada|por|segun)\s+{rx}\b""",
        _F,
    )
    for dim, rx in _DIMENSIONS.items()
}
# A second dimension coordinated onto a claimed criterion: "por modalidad y
# (por) estado reportado".  Read only right after a criterion was claimed, so a
# bare "y estado" elsewhere stays content.
_COORDINATED = {
    dim: re.compile(rf"\s*(?:,\s*)?(?P<coordinated>\by\s+(?:(?:por|segun)\s+)?{rx}\b)", _F)
    for dim, rx in _DIMENSIONS.items()
}
_NEGATED_CRITERIA = {
    dim: re.compile(rf"\bno\s+(?:por|segun)\s+{rx}\b", _F)
    for dim, rx in _DIMENSIONS.items()
}
_NEGATED_GROUP = {
    dim: re.compile(
        rf"""\b(?:no|sin)\s+(?:(?:me|nos|los|las|lo)\s+)*{_GROUP_VERB}\s+
            (?:(?:los|las)\s+)?(?:{_UNIT}\s+)?(?:por|segun)\s+{rx}\b""",
        _F,
    )
    for dim, rx in _DIMENSIONS.items()
}
_NEGATED_LIST = re.compile(
    rf"""\b(?:sin|no)\s+(?:(?:me|nos)\s+)?
        (?:listar|enumerar|mostrar|listes|enumeres|muestres)\s+
        (?:(?:cada|los|las|el\s+detalle\s+de)\s+)?{_UNIT}\b""",
    _F,
)
_NEGATED_TOTAL_ONLY = re.compile(
    r"\bno\s+(?:me\s+|nos\s+)?(?:des|de|muestres)\s+(?:solo|solamente|unicamente)\s+el\s+total\b",
    _F,
)
_GROUP_COMMAND = re.compile(rf"\b{_GROUP_VERB}\b", _F)
# A group noun is a command only when what follows is the unit or a criterion:
# "la distribución de contratos", never "distribución de agua".
_GROUP_NOUN_COMMAND = re.compile(
    rf"\b(?:(?:la|el)\s+)?{_GROUP_NOUN}(?=\s+(?:de\s+(?:los\s+)?{_UNIT}\b|por\b|segun\b))",
    _F,
)
# "valor total de los contratos" asks for a sum; only the bare quantity nouns
# introduce a count.
_COUNT_CUES = (
    re.compile(r"\bcuant(?:os|as)\b", _F),
    re.compile(
        rf"\b{_COUNT_VERB}(?=\s+(?:(?:los|las|me|nos)\s+)*(?:{_UNIT}\b|por\b|en\s+cada\b))",
        _F,
    ),
    re.compile(
        rf"""(?<!valor\s)(?<!monto\s)(?<!suma\s)
            \b(?:el\s+)?(?:numero|total|cantidad)\s+de(?=\s+(?:los\s+)?{_UNIT}\b)""",
        _F,
    ),
)


def _overlaps(span: tuple[int, int], others: Iterable[tuple[int, int]]) -> bool:
    return any(span[0] < end and start < span[1] for start, end in others)


def locate_analytic_controls(
    text: str,
    *,
    protected_spans: Iterable[tuple[int, int]] = (),
    modality_spans: Iterable[tuple[int, int]] = (),
    candidate_spans: Iterable[tuple[int, int]] = (),
) -> tuple[AnalyticControlSpan, ...]:
    """Every analytic control occurrence in ``text``, ordered by offset.

    ``text`` is the normalized turn.  ``protected_spans`` are owned by other
    authorities and are never claimed; ``modality_spans`` are resolved modality
    aliases — a criterion that runs into one names a modality FILTER ("por
    modalidad de contratación directa"), not a breakdown.  ``candidate_spans``
    are alias hits nobody has accredited yet (municipal names): they block a
    claim like protected spans, except when they lie wholly inside the
    dimension's own lexical name — "Contratación" is a municipality, but in
    "según su modalidad de contratación" it is the name of the SECOP field.
    """
    modality = tuple(modality_spans)
    candidates = tuple(candidate_spans)
    taken: list[tuple[int, int]] = [*protected_spans, *modality]
    found: list[AnalyticControlSpan] = []

    def claim(
        match: re.Match[str],
        kind: AnalyticControlKind,
        dimension: AnalyticDimension | None,
        polarity: ControlPolarity,
    ) -> bool:
        span = (
            match.span("coordinated")
            if match.re.groupindex.get("coordinated")
            else match.span()
        )
        if span[0] == span[1] or _overlaps(span, taken):
            return False
        complement = (
            match.span("complement")
            if match.re.groupindex.get("complement") and match.start("complement") >= 0
            else None
        )
        for candidate in candidates:
            if not _overlaps(span, (candidate,)):
                continue
            if complement is None or not (
                complement[0] <= candidate[0] and candidate[1] <= complement[1]
            ):
                return False
        found.append(AnalyticControlSpan(
            kind, dimension, polarity, span[0], span[1], text[span[0]:span[1]],
        ))
        taken.append(span)
        return True

    negative = ControlPolarity.NEGATIVE
    positive = ControlPolarity.POSITIVE
    group = AnalyticControlKind.GROUP
    for dimension in AnalyticDimension:
        for match in _NEGATED_GROUP[dimension].finditer(text):
            claim(match, group, dimension, negative)
        for match in _NEGATED_CRITERIA[dimension].finditer(text):
            claim(match, group, dimension, negative)
    for match in _NEGATED_LIST.finditer(text):
        claim(match, AnalyticControlKind.LIST, None, negative)
    for match in _NEGATED_TOTAL_ONLY.finditer(text):
        claim(match, AnalyticControlKind.TOTAL_ONLY, None, negative)

    counted = False
    for pattern in _COUNT_CUES:
        for match in pattern.finditer(text):
            counted = claim(match, AnalyticControlKind.COUNT, None, positive) or counted
    commands = [
        match
        for pattern in (_GROUP_COMMAND, _GROUP_NOUN_COMMAND)
        for match in pattern.finditer(text)
        if not _overlaps(match.span(), taken)
    ]
    if not (counted or commands):
        return tuple(sorted(found, key=lambda item: item.start))

    criteria: list[tuple[re.Match[str], AnalyticDimension]] = []
    for dimension, pattern in _CRITERIA.items():
        for match in pattern.finditer(text):
            if any(0 <= start - match.end() <= 1 for start, _ in modality):
                continue
            criteria.append((match, dimension))
    claimed_criterion = False
    for match, dimension in criteria:
        if not claim(match, group, dimension, positive):
            continue
        claimed_criterion = True
        for other, pattern in _COORDINATED.items():
            if other is dimension:
                continue
            coordinated = pattern.match(text, match.end())
            if coordinated is not None:
                claim(coordinated, group, other, positive)
    # A command word is control only when it governs an accredited criterion;
    # alone, "distribuye" may still be what the user is talking about.
    if claimed_criterion:
        for match in commands:
            claim(match, group, None, positive)
    return tuple(sorted(found, key=lambda item: item.start))


_COUNT_IMPERATIVE = re.compile(rf"{_COUNT_VERB}", _F)


def is_count_imperative(control: AnalyticControlSpan) -> bool:
    """A COUNT carried by an imperative ("cuenta", "cuéntame"), not by a question."""
    return (
        control.kind is AnalyticControlKind.COUNT
        and _COUNT_IMPERATIVE.fullmatch(control.evidence_text) is not None
    )


def control_spans(controls: Iterable[AnalyticControlSpan]) -> tuple[tuple[int, int], ...]:
    return tuple((control.start, control.end) for control in controls)


__all__ = [
    "AnalyticControlKind",
    "AnalyticControlSpan",
    "AnalyticDimension",
    "ControlPolarity",
    "control_spans",
    "controls_from_state",
    "is_count_imperative",
    "locate_analytic_controls",
]
