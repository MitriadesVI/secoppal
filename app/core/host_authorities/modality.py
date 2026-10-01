"""MODALITY authority (M1-D-STATE/MOD, contract C1.1): wraps ``modality_resolver.resolve`` untouched.

The resolver reads language and the catalog is the legal truth (A2: healthy).  What the host used to
lack, and this authority adds on top of it, is everything that decides whether the RESOLVER'S answer is
the answer to THIS condition:

* ``modality-o6@1`` (DA-2bis, M1.0 O6): a colloquial «licitación(es)» is the user's word for the process
  universe, never a modality filter.  The verdict is RESOLVED with a value that FILTERS NOTHING
  (``ModalityValue((), "modality-o6@1", False)``) and the policy as its receipt; the core projects no
  modality condition.  The legal name («licitación pública») is a real filter.  A NEGATED colloquial
  word cannot be honoured as a filter and is not ignored: ``NEGATION_UNSUPPORTED:MODALITY``;
* polarity guard: a cite that CONTAINS a negation («no por contratación directa») while the condition is
  POSITIVE would execute the opposite of the request (A2 MODALITY-031/032): the resolver reads the
  negation as an exclusion and the old host mixed ``includes + excludes``.  It is
  ``OPERATOR_EVIDENCE_MISMATCH:MODALITY``.  A NEGATIVE condition is honoured as ``excluded`` (NOT IN the
  procedures); the negation may be inside the cite or outside it (the condition's own polarity);
* exact reading: the cite must be only the modality (plus the negation scaffold).  «directa o licitación»
  (A2 MODALITY-033: only «licitación» survived), «licitación pública de obra» or two modalities in one cite
  leave residue and are ``RESIDUE_UNREAD:MODALITY``;
* strength: only ``exact_legal`` / ``exact_common`` execute; ``contextual`` / ``legacy_raw`` clarify;
* family vs literal: a family with several procedures is executed only when it is ACCREDITED
  (``modality-lp-family@1``, ``modality-direct-family@1``: the two the projection has always accredited);
  any other multi-procedure family is BLOCK ``UNACCREDITED_MODALITY_FAMILY`` as today; a single
  procedure is exact.  ``is`` over a cite that names a family is ``OPERATOR_EVIDENCE_MISMATCH`` (the
  legacy ``LITERAL_MODALITY_REQUIRES_ONE_PROCEDURE``).

The dataset only narrows the resolver when the context has exactly one of the two known datasets.
"""

from __future__ import annotations

import re

from app.core.foundation_authority_contract import (
    AuthorityContext, AuthorityReceipt, AuthorityVerdict, DimensionEvidence, ModalityValue,
)
from app.core.foundation_evidence_window import window_tokens
from app.core.host_authorities.datasets import logical_datasets
from app.core.interpretation_contract import Polarity, ResolutionDecision
from app.core.modality_resolution_models import IssueType, MatchStrength
from app.core.modality_resolver import resolve as resolve_modality
from app.core.semantic_dimensions import SemanticDimension

AUTHORITY_ID = "modality-authority"
VERSION = "1"
O6_POLICY = "modality-o6@1"
EXCLUSION_POLICY = "modality-exclusion@1"
# retrieval_family -> (policy, every procedure of the family).  Pinned to the projection's own table.
ACCREDITED_FAMILIES = {
    "licitacion_publica": ("modality-lp-family@1", frozenset({"licitacion_publica", "licitacion_publica_obra"})),
    "contratacion_directa": ("modality-direct-family@1",
                             frozenset({"contratacion_directa", "contratacion_directa_con_ofertas"})),
}
NEGATION_TOKENS = frozenset({"no", "ni", "sin", "excepto", "salvo"})
SCAFFOLD_TOKENS = NEGATION_TOKENS | frozenset({"por", "de", "del", "que", "sean", "sea", "es", "son",
                                               "los", "las", "el", "la", "un", "una"})
_EXECUTABLE = frozenset({MatchStrength.EXACT_LEGAL, MatchStrength.EXACT_COMMON})
_DETAIL = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_AMBIGUOUS = frozenset({IssueType.AMBIGUITY, IssueType.CONFLICT})


def _reject(reason: str, detail: str | None = None, decision=ResolutionDecision.CLARIFY) -> AuthorityVerdict:
    return AuthorityVerdict(decision, f"{AUTHORITY_ID}@{VERSION}", reason=reason, detail=detail)


def _rule_id(rule_id: str) -> str:
    """The resolver names composed alternatives ``alternatives:a+b+c``: a receipt rule is ``[A-Za-z0-9_.:@-]``."""
    return re.sub(r"[^A-Za-z0-9_.:@-]", "_", rule_id)[:64]


def _issue_detail(code: str) -> str:
    folded = code.upper()
    return folded if _DETAIL.fullmatch(folded) else "MODALITY_RESOLVER_ISSUE"


class ModalityAuthority:
    authority_id = AUTHORITY_ID
    version = VERSION
    dimensions = frozenset({SemanticDimension.MODALITY})
    operators = frozenset({"is", "family"})

    def resolve(self, evidence: DimensionEvidence, ctx: AuthorityContext) -> AuthorityVerdict:
        name = evidence.dimension.value
        if evidence.dimension not in self.dimensions:
            return _reject(f"UNRESOLVED_HOST_DIMENSION:{name}", "DIMENSION_NOT_DECLARED")
        if evidence.operator not in self.operators:
            return _reject("OPERATOR_EVIDENCE_MISMATCH:MODALITY", "MODALITY_OPERATOR_UNKNOWN")
        quote = evidence.window.quote
        if quote is None:  # two different phrases cited for one condition: neither may win
            return _reject("RESIDUE_UNREAD:MODALITY", "MODALITY_COMPOSITION_UNSUPPORTED")

        logical = set(logical_datasets(ctx).values())
        resolution = resolve_modality(quote, dataset=next(iter(logical)) if len(logical) == 1 else None)
        if resolution.issues:
            ambiguous = any(issue.issue_type in _AMBIGUOUS for issue in resolution.issues)
            return _reject("AMBIGUOUS_HOST_DIMENSION:MODALITY" if ambiguous
                           else "UNRESOLVED_HOST_DIMENSION:MODALITY",
                           _issue_detail(resolution.issues[0].code))
        matches = (*resolution.includes, *resolution.excludes)
        if not matches:
            return _reject("UNRESOLVED_HOST_DIMENSION:MODALITY", "MODALITY_NO_MATCH")
        if len(matches) > 1:
            return _reject("RESIDUE_UNREAD:MODALITY", "MODALITY_COMPOSITION_UNSUPPORTED")
        match = matches[0]

        residue = window_tokens(resolution.residual_text)
        if any(token not in SCAFFOLD_TOKENS for token in residue):
            return _reject("RESIDUE_UNREAD:MODALITY", "MODALITY_QUOTE_NOT_FULLY_READ")
        negated = bool(resolution.excludes) or any(token in NEGATION_TOKENS for token in residue)
        if negated and evidence.polarity is Polarity.POSITIVE:
            return _reject("OPERATOR_EVIDENCE_MISMATCH:MODALITY", "MODALITY_NEGATION_IN_POSITIVE_CITE")
        if negated and not (resolution.excludes and not resolution.includes):
            return _reject("RESIDUE_UNREAD:MODALITY", "MODALITY_NEGATION_NOT_BOUND")
        excluded = evidence.polarity is Polarity.NEGATIVE

        if match.strength is MatchStrength.COLLOQUIAL or match.assumption_code:
            if excluded:
                return _reject("NEGATION_UNSUPPORTED:MODALITY", "MODALITY_COLLOQUIAL_NOT_NEGATABLE")
            value = ModalityValue((), O6_POLICY, False)  # O6: the word names the universe, it filters nothing
            return self._resolved(value, match, (O6_POLICY,))
        if match.strength not in _EXECUTABLE:
            return _reject("UNRESOLVED_HOST_DIMENSION:MODALITY", "MODALITY_STRENGTH_NOT_EXECUTABLE")

        ids = tuple(sorted(match.procedure_ids))
        policies: tuple[str, ...] = ()
        family_policy = None
        if len(ids) > 1:
            if evidence.operator == "is":
                return _reject("OPERATOR_EVIDENCE_MISMATCH:MODALITY", "MODALITY_LITERAL_NEEDS_ONE_PROCEDURE")
            accredited = ACCREDITED_FAMILIES.get(match.retrieval_family)
            if accredited is None or frozenset(ids) != accredited[1]:
                return _reject("UNRESOLVED_HOST_DIMENSION:MODALITY", "UNACCREDITED_MODALITY_FAMILY",
                               ResolutionDecision.BLOCK)
            family_policy = accredited[0]
            policies = (family_policy,)
        if excluded:
            policies = (*policies, EXCLUSION_POLICY)
        return self._resolved(ModalityValue(ids, family_policy, excluded), match, policies)

    def _resolved(self, value: ModalityValue, match, policies: tuple[str, ...]) -> AuthorityVerdict:
        receipt = AuthorityReceipt(rules=(_rule_id(match.rule_id),), candidates=len(match.procedure_ids))
        return AuthorityVerdict(ResolutionDecision.RESOLVED, f"{AUTHORITY_ID}@{VERSION}", value=value,
                                policies=policies, receipt=receipt)
