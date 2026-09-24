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

Rule, deliberately conservative — no union is invented:
  * the open funnel (``oferta_abierta``, declared by "abiertos",
    "convocatorias", … or by an opportunity search) describes availability, so
    a literal inside it narrows it and a literal outside it clarifies;
  * any other set of two or more distinct readings clarifies;
  * one reading left is the request; a literal is applied exactly.

The decision is the ONLY writer of what those mentions mean (R3): filters,
``estado``/``estado_field``, ``estado_family`` and the opportunity intent the
funnel implies. A single-answer producer may run first, but nothing it picked
by position survives: "abiertos y aprobados" and "aprobados y abiertos" name
the same states, so they are the same request.
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
    """Decide once, over every mention, whether the named states are representable."""
    distinct = tuple({mention.reading: mention for mention in mentions}.values())
    literals = [mention for mention in distinct if mention.literal]
    funnels = {
        *(family for family in seeded_families if family in FUNNEL_FAMILIES),
        *(str(m.family) for m in distinct if not m.literal and m.family in FUNNEL_FAMILIES),
    }
    requests = [m for m in distinct if m.literal or m.family not in FUNNEL_FAMILIES]
    if len(requests) > 1:
        return ProcessStateDecision(clarification=MULTI_STATE_QUESTION.format(
            states=", ".join(_label(mention) for mention in requests),
        ))
    for family in sorted(funnels):
        for mention in literals:
            if mention.literal not in _family_members(family):
                label = _FAMILY_LABELS.get(family, family.replace("_", " "))
                return ProcessStateDecision(clarification=FAMILY_CONFLICT_QUESTION.format(
                    state=mention.literal, family=label,
                ))
        for mention in requests:
            if not mention.literal:
                return ProcessStateDecision(clarification=MULTI_STATE_QUESTION.format(
                    states=f"{_FAMILY_LABELS.get(family, family)}, {_label(mention)}",
                ))
    # Executable: at most one non-funnel reading is left, and a literal with a
    # funnel lies inside it. The funnel frames the request; a literal narrows it.
    # A literal that belongs to the funnel ("publicados") frames it too.
    funnel = min(funnels) if funnels else next(
        (str(m.family) for m in requests if m.family in FUNNEL_FAMILIES), None,
    )
    family = funnel or (requests[0].family if requests else None)
    if literals:
        return ProcessStateDecision(literal=literals[0].literal, field=literals[0].field,
                                    family=family, funnel=funnel is not None)
    return ProcessStateDecision(family=family, funnel=funnel is not None)
