"""Whether the PROCESS states a request names can be honoured exactly.

INV-PROCESS-STATE-NO-SILENT-LOSS (PROCESS_STATE_REQUEST_INTEGRITY_V1.1).

The state authority (``estado_families.detect_estado_family``) answers ONE
question — the first state in the text — by design: "multiple matches are not
allowed". That is the right contract for a producer and the wrong one for a
guard. With two states the first won, the legacy secondary extractor consumed
the second without applying it, and with an opportunity seed the literal lost
to the automatic funnel at the SoQL seam. Nothing downstream could notice.

This module does not read text. The mentions come from
``estado_families.read_process_state_mentions``, which enumerates with the
producers' own lexicon and reading (literal or family), over the text the
parser already masked (modality, negation, entities, …) so a span owned by
another authority —"sobre cerrado"— is never read as a state. V1 enumerated
only literals: "cancelados y adjudicados" looked like one state because
"adjudicados" is family vocabulary, and the verdict depended on word order.

Rule (PROCESS_STATE_SINGLE_REQUEST_V1) — cardinality, not grammar:
  * two or more distinct explicit readings clarify, whatever joins them ("y",
    "o", a comma, parentheses) and in any order. Composing states is outside
    this contract: "abiertos o (abiertos y aprobados)" is not an intersection
    the parser may invent (ZEUS, R3.1);
  * one explicit reading is the request: a literal is applied exactly, a
    family projects as a family;
  * a funnel seeded by another authority ("oportunidades", bidder intent) is
    not a second reading. It only checks compatibility: a literal it contains
    narrows it, anything else clarifies.

An opportunity head ("convocatorias", like "oportunidades") is read by the
opportunity authority, not counted here; the phrase "en convocatoria" is a
different occurrence and stays a state reading.

The decision is the ONLY writer of what those mentions mean (R3): filters,
``estado``/``estado_field``, ``estado_family`` and the opportunity intent the
funnel implies. A single-answer producer may run first, but nothing it picked
by position survives: "abiertos y aprobados" and "aprobados y abiertos" name
the same states, so they get the same verdict.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.core.estado_families import ESTADO_FAMILIES, ProcessStateMention

CLARIFICATION_KEY = "_process_state_clarification"

MULTI_STATE_QUESTION = (
    "Pediste más de un estado de proceso ({states}) y todavía no puedo "
    "combinarlos en una sola búsqueda. ¿Cuál estado quieres consultar?"
)
FAMILY_CONFLICT_QUESTION = (
    "Pediste procesos en estado {state}, pero la búsqueda también pide "
    "{family}, que no incluye ese estado. ¿Quieres procesos en estado {state} "
    "(histórico) o {family}?"
)
# Families whose vocabulary describes availability rather than naming a state.
# Only these absorb a literal they contain.
FUNNEL_FAMILIES = frozenset({"oferta_abierta"})
# Bare opportunity heads the state lexicon also reads. As a source noun they
# are the opportunity authority's, never an explicit state of the turn.
OPPORTUNITY_HEAD_SURFACES = frozenset({"convocatoria", "convocatorias"})
_FAMILY_LABELS = {
    "oferta_abierta": "oportunidades abiertas",
    "en_evaluacion": "procesos en evaluación o adjudicados",
    "no_procede": "procesos que no procedieron",
    "contrato_activo": "procesos adjudicados en ejecución",
}


@dataclass(frozen=True, slots=True)
class ProcessStateDecision:
    """``clarification`` blocks the turn; otherwise ``literal`` is exact.

    ``funnel`` says the open funnel is part of the executable request (named or
    seeded), which is what makes it an opportunity search; ``family`` is the
    family the request belongs to. Both are properties of the SET of readings,
    never of the first one.
    """

    clarification: str | None = None
    literal: str | None = None
    field: str | None = None
    family: str | None = None
    funnel: bool = False


def _family_members(family: str) -> set[str]:
    config = ESTADO_FAMILIES.get(family, {}).get("procesos", {})
    return {value for values in config.values() for value in values}


def _label(mention: ProcessStateMention) -> str:
    if mention.literal:
        return mention.literal
    family = str(mention.family)
    return _FAMILY_LABELS.get(family, family.replace("_", " "))


def decide_process_states(
    mentions: tuple[ProcessStateMention, ...],
    seeded_families: set[str],
) -> ProcessStateDecision:
    """Decide once, over every explicit mention of the turn.

    ``seeded_families`` never adds to the cardinality: a seed is another
    authority's frame, checked for compatibility with the one reading named.
    """
    distinct = tuple({mention.reading: mention for mention in mentions}.values())
    if len(distinct) > 1:
        return ProcessStateDecision(clarification=MULTI_STATE_QUESTION.format(
            states=", ".join(_label(mention) for mention in distinct),
        ))
    if not distinct:
        return ProcessStateDecision()
    (mention,) = distinct
    seeded_funnel = min((f for f in seeded_families if f in FUNNEL_FAMILIES), default=None)
    if seeded_funnel is not None:
        label = _FAMILY_LABELS.get(seeded_funnel, seeded_funnel.replace("_", " "))
        if mention.literal and mention.literal not in _family_members(seeded_funnel):
            return ProcessStateDecision(clarification=FAMILY_CONFLICT_QUESTION.format(
                state=mention.literal, family=label,
            ))
        if not mention.literal and mention.family != seeded_funnel:
            return ProcessStateDecision(clarification=MULTI_STATE_QUESTION.format(
                states=f"{label}, {_label(mention)}",
            ))
    # A literal of the funnel ("publicados") frames it too; one outside it
    # ("aprobados" alone) invents no opportunity search.
    own_funnel = mention.family if mention.family in FUNNEL_FAMILIES else None
    funnel = seeded_funnel or own_funnel
    family = funnel or mention.family
    if mention.literal:
        return ProcessStateDecision(literal=mention.literal, field=mention.field,
                                    family=family, funnel=funnel is not None)
    return ProcessStateDecision(family=family, funnel=funnel is not None)
