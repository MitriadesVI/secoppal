"""STATE authority (M1-D-STATE/MOD, contract C1.1): ``resolve(evidence, ctx) -> AuthorityVerdict``.

The value comes ONLY from the cited segments.  Reading rules, all fail-closed (CLARIFY asks, BLOCK is
reserved for an unavailable authority, which this one never is):

* the cite must be ENTIRELY one state phrase (after the «no / que no sean / no esté» scaffold).  A state
  word plus anything else («terminados anticipadamente», A2 STATE-036) is ``RESIDUE_UNREAD:STATE``: the
  host never executes the word it recognised and drops the one it did not;
* exact vs family (M1.0 C4): ``family`` is the open family (abierto, vigente, disponible, activo,
  convocado) and ``is`` is every exact state.  The operator that disagrees with its own evidence is
  ``OPERATOR_EVIDENCE_MISMATCH:STATE``, never a silently different value;
* one value per dataset (INV-C1.1-DS): ``procesos`` and ``contratos`` have different columns and
  literals, so the value lists literals only for the datasets of ``ctx`` that the state exists in.  A
  state that exists in none of them is ``UNRESOLVED_HOST_DIMENSION:STATE`` (detail
  ``STATE_NOT_REPRESENTABLE`` / ``STATE_NOT_IN_DATASET``): «liquidados» has no SECOP literal and is
  asked, not approximated; «publicados» over contratos is not a contract state;
* S3 (ADR-100): an open word («abiertos», «en convocatoria»…) and another open word or ``publicados``
  under a bare «o» (two cites with the gap ``("o",)``, or one cite spelling it) is the open funnel, with the policy ``adr-100@1`` (HostPolicy ADR-100 v1).  Any
  other gap, a third cite or another pair is ``RESIDUE_UNREAD:STATE`` /
  ``STATE_COMPOSITION_UNSUPPORTED``.  The gap only decides between that value and a clarification
  (INV-C0-7b); the composition cannot be negated (``NEGATION_UNSUPPORTED:STATE``);
* NEGATIVE (INV-C1.1-EXCL): the value carries ``excluded``: NOT IN (the literals of the one column of each
  dataset), so «no terminados» keeps working.  A state that filters TWO columns (the open funnel) is
  ``NEGATION_UNSUPPORTED:STATE`` / ``STATE_NEGATION_MULTI_COLUMN``: the contract does not define
  ``excluded`` over a conjunction and guessing would execute the wrong complement (the legacy host also
  refuses it).  A negation INSIDE a cite whose polarity is POSITIVE would execute the opposite of the
  request: ``OPERATOR_EVIDENCE_MISMATCH:STATE``;
* D6 (M1.0 v1.4): the participle «publicados» that ANCHORS A DATE («publicados hace dos semanas», «en
  marzo») is not a state: the context withdraws it (``UNCITED_QUALIFIER:STATE``).  Only the context
  to the right of the cite is read, and it can only withdraw (INV-C0-7).  Bounded guarantee: months,
  years and a short closed list of periods, not an open grammar of dates.
"""

from __future__ import annotations

import re

from app.core.foundation_authority_contract import (
    AuthorityContext, AuthorityReceipt, AuthorityVerdict, DimensionEvidence, EvidenceWindow,
    StateValue,
)
from app.core.foundation_evidence_window import window_tokens
from app.core.host_authorities import state_catalog as catalog
from app.core.host_authorities.datasets import CONTRACTS, PROCESSES, logical_datasets
from app.core.interpretation_contract import Polarity, ResolutionDecision
from app.core.semantic_dimensions import SemanticDimension

AUTHORITY_ID = "state-authority"
VERSION = "1"
FUNNEL_POLICY = "adr-100@1"  # = HostPolicy("ADR-100", "1"); a verdict policy id is lowercase
EXCLUSION_POLICIES = {CONTRACTS: "celebrated-state-complement@1", PROCESSES: "process-state-exclusion@1"}

_MONTHS = frozenset({"enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre",
                     "setiembre", "octubre", "noviembre", "diciembre"})
_PERIODS = frozenset({"mes", "meses", "ano", "anos", "semana", "semanas", "dia", "dias", "trimestre",
                      "semestre", "pasado", "pasada"})
_PREPOSITIONS = frozenset({"en", "el", "del", "desde", "hasta", "durante", "entre", "de", "a"})
_DETERMINERS = frozenset({"el", "la", "los", "las", "un", "una"})
_YEAR = re.compile(r"(?:19|20)\d{2}\Z")
_DAY = re.compile(r"\d{1,2}\Z")
_TEMPORAL_OPENERS = frozenset({"este", "esta", "ultimo", "ultima", "ultimos", "ultimas"})


def _reject(reason: str, detail: str | None = None, decision=ResolutionDecision.CLARIFY) -> AuthorityVerdict:
    return AuthorityVerdict(decision, f"{AUTHORITY_ID}@{VERSION}", reason=reason, detail=detail)


def _anchors_a_date(right: tuple[str, ...]) -> bool:
    """Whether the tokens right after the cite open a date or a period («hace dos…», «en marzo», «el 2024»)."""
    if not right:
        return False
    head, rest = right[0], right[1:]
    if head in {"hace", "hoy", "ayer"}:
        return True
    if head in _TEMPORAL_OPENERS:
        return bool(rest) and rest[0] in _PERIODS
    if head not in _PREPOSITIONS:
        return False
    if rest and rest[0] in _DETERMINERS:
        rest = rest[1:]
    return bool(rest) and (rest[0] in _MONTHS or rest[0] in _PERIODS or bool(_YEAR.fullmatch(rest[0]))
                           or bool(_DAY.fullmatch(rest[0])))


def _is_funnel_pair(first: tuple[str, ...], second: tuple[str, ...]) -> bool:
    """ADR-100: one open word and another open word or the funnel's own literal «publicados», any order."""
    one, other = catalog.lookup(first), catalog.lookup(second)
    return (one is catalog.OPEN and (other is catalog.OPEN or other is catalog.PUBLISHED)) or (
        one is catalog.PUBLISHED and other is catalog.OPEN)


def _composition(window: EvidenceWindow) -> str:
    """``"none"`` (one phrase), ``"funnel"`` (S3, ADR-100) or ``"unsupported"`` (any other composition).

    The shapes of S3: two cites under the gap ``("o",)``, or one cite that spells it.  A phrase cited
    again (G1) is one phrase.  The gap is structural evidence: it only decides between the funnel and a
    clarification, never the value (INV-C0-7b).
    """
    if window.quote is not None:
        if len(window.segments) > 1:
            return "none"
        tokens = window_tokens(window.segments[0])
        if tokens.count("o") == 1 and 0 < tokens.index("o") < len(tokens) - 1:
            at = tokens.index("o")
            if _is_funnel_pair(tokens[:at], tokens[at + 1:]):
                return "funnel"
        return "none"  # a different connector in one cite is residue of the single-phrase reading
    if (len(window.segments) == 2 and window.gaps == (("o",),)
            and _is_funnel_pair(*(window_tokens(segment) for segment in window.segments))):
        return "funnel"
    return "unsupported"


class StateAuthority:
    authority_id = AUTHORITY_ID
    version = VERSION
    dimensions = frozenset({SemanticDimension.STATE})
    operators = frozenset({"is", "family"})

    def resolve(self, evidence: DimensionEvidence, ctx: AuthorityContext) -> AuthorityVerdict:
        name = evidence.dimension.value
        if evidence.dimension not in self.dimensions:
            return _reject(f"UNRESOLVED_HOST_DIMENSION:{name}", "DIMENSION_NOT_DECLARED")
        if evidence.operator not in self.operators:
            return _reject("OPERATOR_EVIDENCE_MISMATCH:STATE", "STATE_OPERATOR_UNKNOWN")
        window = evidence.window
        datasets = logical_datasets(ctx)
        if not datasets:
            return _reject("UNRESOLVED_HOST_DIMENSION:STATE", "STATE_DATASET_UNKNOWN")

        composition = _composition(window)
        if composition == "unsupported":
            return _reject("RESIDUE_UNREAD:STATE", "STATE_COMPOSITION_UNSUPPORTED")
        if composition == "funnel":
            return self._funnel(evidence, datasets)

        tokens = window_tokens(window.quote)
        phrase, negations = catalog.strip_scaffold(tokens)
        if negations > 1:
            return _reject("RESIDUE_UNREAD:STATE", "STATE_QUOTE_NOT_FULLY_READ")
        reading = catalog.lookup(phrase) if phrase else None
        if reading is None:
            detail = ("STATE_QUOTE_NOT_FULLY_READ" if catalog.contains_state_phrase(phrase)
                      else "STATE_PHRASE_NOT_RECOGNIZED")
            return _reject("RESIDUE_UNREAD:STATE" if detail == "STATE_QUOTE_NOT_FULLY_READ"
                           else "UNRESOLVED_HOST_DIMENSION:STATE", detail)
        if reading == "unrepresentable":
            return _reject("UNRESOLVED_HOST_DIMENSION:STATE", "STATE_NOT_REPRESENTABLE")
        if reading.operator != evidence.operator:
            return _reject("OPERATOR_EVIDENCE_MISMATCH:STATE", "STATE_OPERATOR_MISMATCH")
        if negations and evidence.polarity is Polarity.POSITIVE:
            # «no terminados» proposed POSITIVE would execute the opposite of the request
            return _reject("OPERATOR_EVIDENCE_MISMATCH:STATE", "STATE_NEGATION_IN_POSITIVE_CITE")
        if reading.names_event and _anchors_a_date(window.right):
            return _reject("UNCITED_QUALIFIER:STATE", "STATE_PARTICIPLE_ANCHORS_DATE")
        return self._value(reading, evidence, datasets, policies=())

    def _funnel(self, evidence: DimensionEvidence, datasets: dict[str, str]) -> AuthorityVerdict:
        if evidence.operator != "family":
            return _reject("OPERATOR_EVIDENCE_MISMATCH:STATE", "STATE_OPERATOR_MISMATCH")
        if evidence.polarity is Polarity.NEGATIVE:  # NOT(published OR open) is not NOT(the funnel)
            return _reject("NEGATION_UNSUPPORTED:STATE", "STATE_COMPOSITION_NOT_NEGATABLE")
        return self._value(catalog.OPEN, evidence, datasets, policies=(FUNNEL_POLICY,))

    def _value(self, reading: catalog.StateReading, evidence: DimensionEvidence,
               datasets: dict[str, str], policies: tuple[str, ...]) -> AuthorityVerdict:
        literals = {name: dict(columns) for name, logical in datasets.items()
                    if (columns := reading.for_dataset(logical)) is not None}
        if not literals:
            return _reject("UNRESOLVED_HOST_DIMENSION:STATE", "STATE_NOT_IN_DATASET")
        excluded = evidence.polarity is Polarity.NEGATIVE
        if excluded and any(len(columns) > 1 for columns in literals.values()):
            # NOT(a AND b) is not «each column excluded»: the contract does not say which one ``excluded``
            # means over two columns, and guessing would execute the wrong complement (the open funnel).
            return _reject("NEGATION_UNSUPPORTED:STATE", "STATE_NEGATION_MULTI_COLUMN")
        if excluded:
            policies = (*policies, *sorted({EXCLUSION_POLICIES[datasets[name]] for name in literals}))
        value = StateValue.of(reading.state_class, literals, excluded)
        receipt = AuthorityReceipt(rules=(f"state-class:{reading.state_class}",), candidates=len(literals))
        return AuthorityVerdict(ResolutionDecision.RESOLVED, f"{AUTHORITY_ID}@{VERSION}", value=value,
                                policies=policies, receipt=receipt)
