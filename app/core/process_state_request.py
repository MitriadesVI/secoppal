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
    narrows it, anything else clarifies;
  * one exception to cardinality (PROCESS_STATE_INTEGRITY_CLOSE_V1): exactly
    two mentions joined by a bare "o"/"u", one naming the open funnel and the
    other a literal of that same funnel ("en convocatoria o publicados"), are
    one request for the funnel. It is not a set union — the funnel carries the
    current-opportunity policy (O1B) that a lone "publicados" does not — but it
    keeps the effective query a real turn has had since May 2026. Any other
    connector, a third mention, a literal of another family or a family that
    is not the funnel still clarifies.

An opportunity head ("convocatorias", like "oportunidades") is read by the
opportunity authority, not counted here; the phrase "en convocatoria" is a
different occurrence and stays a state reading.

The decision is the ONLY writer of what those mentions mean (R3): filters,
``estado``/``estado_field``, ``estado_family`` and the opportunity intent the
funnel implies. A single-answer producer may run first, but nothing it picked
by position survives: "abiertos y aprobados" and "aprobados y abiertos" name
the same states, so they get the same verdict.

Same verdict is not enough; it must be the same query (R1, ZEUS NO_GO on
4402f1e1). The producer answers with the first state it reads, a multiword
form before any single word, so "publicados o abiertos" left
``estado_del_procedimiento = ['Publicado']`` behind a funnel decision, and the
family projection respects any filter it finds. ``apply_process_state_decision``
therefore replaces every state key before writing the decision: a literal
exactly, a family through the one projection authority.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.core.estado_families import ESTADO_FAMILIES, ProcessStateMention
from app.core.estado_projection import (
    STATE_FILTER_FIELDS, legacy_estado_scalar, project_estado_family,
)

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


# Every key a producer may have derived from a process-state word.
STATE_KEYS = (*STATE_FILTER_FIELDS, "estado", "estado_field", "estado_family")


def _family_members(family: str) -> set[str]:
    config = ESTADO_FAMILIES.get(family, {}).get("procesos", {})
    return {value for values in config.values() for value in values}


def _label(mention: ProcessStateMention) -> str:
    if mention.literal:
        return mention.literal
    family = str(mention.family)
    return _FAMILY_LABELS.get(family, family.replace("_", " "))


def _funnel_absorbing_its_literal(
    mentions: tuple[ProcessStateMention, ...], text: str | None,
) -> ProcessStateMention | None:
    """The funnel mention of "<funnel> o <its literal>", in either order."""
    if text is None or len(mentions) != 2:
        return None
    first, second = sorted(mentions, key=lambda mention: mention.span)
    if text[first.span[1]:second.span[0]].strip().lower() not in {"o", "u"}:
        return None
    funnel = next((m for m in mentions if not m.literal and m.family in FUNNEL_FAMILIES), None)
    literal = next((m for m in mentions if m.literal), None)
    # The literal's own family is the funnel, so it is one of its members.
    if funnel is None or literal is None or literal.family != funnel.family:
        return None
    return funnel


def decide_process_states(
    mentions: tuple[ProcessStateMention, ...],
    seeded_families: set[str],
    text: str | None = None,
) -> ProcessStateDecision:
    """Decide once, over every explicit mention of the turn.

    ``seeded_families`` never adds to the cardinality: a seed is another
    authority's frame, checked for compatibility with the one reading named.
    ``text`` is the text the mentions were read from; without it no connector
    is read and cardinality alone decides.
    """
    distinct = tuple({mention.reading: mention for mention in mentions}.values())
    if len(distinct) > 1 and (funnel := _funnel_absorbing_its_literal(mentions, text)):
        distinct = (funnel,)
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


def apply_process_state_decision(params: dict, decision: ProcessStateDecision) -> None:
    """Write ``decision`` over whatever a single-answer producer left.

    Only the state keys change: entity, dates, topic and every other condition
    stay as they are. A decision that decides nothing leaves ``params`` alone.
    The opportunity intent is not written here: it depends on what other
    authorities read before the state words (QueryRouter).
    """
    if not (decision.clarification or decision.literal or decision.family):
        return
    for key in STATE_KEYS:
        params.pop(key, None)
    if decision.clarification:
        # The clarified frame persists as context: it must not carry the one
        # state a single-answer producer happened to pick.
        params[CLARIFICATION_KEY] = decision.clarification
        return
    if decision.literal:
        # A named literal is the exact filter, also inside the funnel it
        # narrows ("abiertos publicados").
        params[decision.field] = [decision.literal]
        params["estado"] = decision.literal
        params["estado_field"] = decision.field
    if decision.family:
        params["estado_family"] = decision.family
    if not decision.literal:
        project_estado_family(params)
        scalar = legacy_estado_scalar(params)
        if scalar is not None:
            params["estado"], params["estado_field"] = scalar
