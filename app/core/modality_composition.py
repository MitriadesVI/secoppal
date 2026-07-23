"""Extracción y composición local de menciones de modalidad M2.

El motor adjunta qualifiers y refinamientos por estructura antes de comprobar
compatibilidad. Nunca cruza otra mención primaria y produce issues locales en
vez de abortar la resolución completa.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from app.core.modality_alternatives import (
    analyze_alternatives,
    attachment_gap,
    crosses_intervening_primary,
    has_explicit_coordinator,
    has_external_negative_operator,
    span_hull,
    structural_attachment_cost,
)
from app.core.modality_lexicon import LexiconRule, ModalityLexicon
from app.core.modality_models import ModalityCatalog
from app.core.modality_resolution_models import IssueType, ResolutionIssue
from app.core.modality_text import (
    Clause,
    clause_for_span,
    contains_phrase,
    context_present_locally,
    find_occurrences,
)

_REFINEMENT_MAX_COST = 7
_COORDINATORS = ("y", "o", "ni")
_REFINEMENT_BLOCKERS = (
    "excepto", "salvo", "no", "sin", "diferente de", "diferentes de",
    "distinto de", "distintos de", "excluyendo",
)
_CODE_QUALIFIER_CONFLICT = "MODALITY_QUALIFIER_FAMILY_CONFLICT"
_CODE_QUALIFIER_VALUE_CONFLICT = "MODALITY_QUALIFIER_VALUE_CONFLICT"

_SEMANTIC_RANK = {
    "procedure": 0,
    "qualifier": 1,
    "qualifier_set": 1,
    "retrieval_family": 2,
    "ambiguity": 3,
}
_STRENGTH_RANK = {
    "exact_legal": 0,
    "legacy_raw": 1,
    "exact_common": 2,
    "contextual": 3,
    "colloquial": 4,
}


@dataclass(frozen=True)
class Candidate:
    """Ocurrencia de una regla en coordenadas normalizadas y originales."""

    norm_start: int
    norm_end: int
    orig_start: int
    orig_end: int
    source_text: str
    rule: LexiconRule
    phrase: str
    clause_index: int

    @property
    def length(self) -> int:
        return self.norm_end - self.norm_start


@dataclass(frozen=True)
class Mention:
    """Mención primaria interna, todavía sin expansión de catálogo/dataset."""

    norm_start: int
    norm_end: int
    orig_start: int
    orig_end: int
    source_text: str
    normalized_alias: str
    clause_index: int
    target_kind: str
    target_id: str
    rule_id: str
    match_strength: str
    confidence: str
    explicit_intent: bool
    assumption_code: str | None
    procedure_ids: tuple[str, ...] | None = None


class CompositionEngine:
    """Extrae candidatos, refina menciones y compone qualifiers contra M1."""

    def __init__(self, catalog: ModalityCatalog, lexicon: ModalityLexicon) -> None:
        self._catalog = catalog
        self._lexicon = lexicon
        self._family_by_procedure = {
            procedure.procedure_id: procedure.retrieval_family
            for procedure in catalog.procedures
        }
        self._canonical_rule_by_procedure = self._build_canonical_rule_index()

    def _build_canonical_rule_index(self) -> dict[str, LexiconRule]:
        index: dict[str, LexiconRule] = {}
        for rule in self._lexicon.rules:
            if rule.target_kind != "procedure" or rule.target_id is None:
                continue
            current = index.get(rule.target_id)
            if current is None or self._rule_sort_key(rule) < self._rule_sort_key(current):
                index[rule.target_id] = rule
        return index

    @staticmethod
    def _rule_sort_key(rule: LexiconRule) -> tuple[int, str]:
        return (_STRENGTH_RANK[rule.match_strength], rule.rule_id)

    def collect_candidates(
        self,
        text: str,
        norm: str,
        positions: tuple[int, ...],
        clauses: tuple[Clause, ...],
    ) -> list[Candidate]:
        candidates: list[Candidate] = []
        for phrase, rule in self._lexicon.phrase_index.items():
            for norm_start, norm_end in find_occurrences(norm, phrase):
                orig_start = positions[norm_start]
                orig_end = positions[norm_end - 1] + 1
                clause = clause_for_span(clauses, orig_start, orig_end)
                if clause is None:
                    continue
                if rule.forbidden_context and context_present_locally(
                    norm, clause, norm_start, norm_end, rule.forbidden_context,
                ):
                    continue
                if rule.required_context and not context_present_locally(
                    norm, clause, norm_start, norm_end, rule.required_context,
                ):
                    continue
                candidates.append(Candidate(
                    norm_start=norm_start,
                    norm_end=norm_end,
                    orig_start=orig_start,
                    orig_end=orig_end,
                    source_text=text[orig_start:orig_end],
                    rule=rule,
                    phrase=phrase,
                    clause_index=clause.index,
                ))
        return candidates

    @staticmethod
    def select_non_overlapping(candidates: list[Candidate]) -> list[Candidate]:
        ordered = sorted(candidates, key=lambda candidate: (
            _SEMANTIC_RANK[candidate.rule.target_kind],
            _STRENGTH_RANK[candidate.rule.match_strength],
            -candidate.length,
            candidate.norm_start,
            candidate.rule.rule_id,
            candidate.phrase,
        ))
        accepted: list[Candidate] = []
        for candidate in ordered:
            if any(
                candidate.norm_start < other.norm_end
                and other.norm_start < candidate.norm_end
                for other in accepted
            ):
                continue
            accepted.append(candidate)
        return sorted(accepted, key=lambda candidate: (
            candidate.norm_start, candidate.norm_end, candidate.rule.rule_id,
        ))

    def analyze_alternatives(
        self,
        text: str,
        norm: str,
        candidates: list[Candidate],
    ) -> tuple[list[Mention], tuple[ResolutionIssue, ...], tuple[tuple[int, int], ...]]:
        analysis = analyze_alternatives(
            text=text,
            norm=norm,
            candidates=candidates,
            family_by_procedure=self._family_by_procedure,
            qualifier_targets=self._lexicon.qualifier_targets,
            qualifier_set_targets=self._lexicon.qualifier_set_targets,
        )
        mentions = [
            Mention(
                norm_start=item.norm_start,
                norm_end=item.norm_end,
                orig_start=item.orig_start,
                orig_end=item.orig_end,
                source_text=text[item.orig_start:item.orig_end],
                normalized_alias=norm[item.norm_start:item.norm_end],
                clause_index=item.primary.clause_index,
                target_kind="retrieval_family",
                target_id=item.family,
                rule_id=(
                    f"alternatives:{item.primary.rule.rule_id}+"
                    + "+".join(item.qualifier_rule_ids)
                ),
                match_strength=item.primary.rule.match_strength,
                confidence=item.primary.rule.confidence,
                explicit_intent=item.primary.rule.explicit_intent,
                assumption_code=item.primary.rule.assumption_code,
                procedure_ids=item.procedure_ids,
            )
            for item in analysis.selections
        ]
        issues = tuple(
            ResolutionIssue(
                issue_type=IssueType.CONFLICT,
                source_text=text[item.orig_start:item.orig_end],
                original_start=item.orig_start,
                original_end=item.orig_end,
                target_ids=item.target_ids,
                code=item.code,
                explanation=item.explanation,
            )
            for item in analysis.conflicts
        )
        return mentions, issues, analysis.consumed_norm_spans

    def compose(
        self,
        text: str,
        norm: str,
        primary_candidates: list[Candidate],
        qualifier_candidates: list[Candidate],
    ) -> tuple[list[Mention], tuple[ResolutionIssue, ...]]:
        mentions = [self._mention_from_candidate(candidate) for candidate in primary_candidates]
        mentions = self._collapse_local_specific_refinements(text, norm, mentions)
        return self._attach_qualifiers(text, norm, mentions, qualifier_candidates)

    @staticmethod
    def _mention_from_candidate(candidate: Candidate) -> Mention:
        assert candidate.rule.target_id is not None
        return Mention(
            norm_start=candidate.norm_start,
            norm_end=candidate.norm_end,
            orig_start=candidate.orig_start,
            orig_end=candidate.orig_end,
            source_text=candidate.source_text,
            normalized_alias=candidate.phrase,
            clause_index=candidate.clause_index,
            target_kind=candidate.rule.target_kind,
            target_id=candidate.rule.target_id,
            rule_id=candidate.rule.rule_id,
            match_strength=candidate.rule.match_strength,
            confidence=candidate.rule.confidence,
            explicit_intent=candidate.rule.explicit_intent,
            assumption_code=candidate.rule.assumption_code,
            procedure_ids=None,
        )

    def family_of(self, mention: Mention) -> str:
        if mention.target_kind == "retrieval_family":
            return mention.target_id
        return self._family_by_procedure[mention.target_id]

    @staticmethod
    def _gap_has_any(norm_gap: str, phrases: tuple[str, ...]) -> bool:
        return any(contains_phrase(norm_gap, phrase) for phrase in phrases)

    def _collapse_local_specific_refinements(
        self, text: str, norm: str, mentions: list[Mention],
    ) -> list[Mention]:
        """Colapsa familia + específico local sin cruzar otra mención primaria."""
        active = sorted(mentions, key=lambda mention: (
            mention.norm_start, mention.norm_end, mention.target_id,
        ))
        removed: set[int] = set()
        replacements: dict[int, Mention] = {}

        for specific_index, specific in enumerate(active):
            if specific.target_kind != "procedure":
                continue
            family = self.family_of(specific)
            candidates: list[tuple[int, int, int, Mention]] = []
            for family_index, general in enumerate(active):
                if family_index == specific_index or general.target_kind != "retrieval_family":
                    continue
                if general.clause_index != specific.clause_index:
                    continue
                if general.target_id != family or general.norm_end > specific.norm_start:
                    continue
                if crosses_intervening_primary(general, specific, active):
                    continue
                norm_gap = norm[general.norm_end:specific.norm_start]
                if has_explicit_coordinator(norm_gap) or self._gap_has_any(
                    norm_gap, _REFINEMENT_BLOCKERS
                ):
                    continue
                gap = (
                    general.norm_end, specific.norm_start,
                    general.orig_end, specific.orig_start,
                )
                structural_cost = structural_attachment_cost(text, norm, gap)
                if structural_cost > _REFINEMENT_MAX_COST:
                    continue
                candidates.append((
                    structural_cost,
                    specific.norm_start - general.norm_end,
                    -general.norm_end,
                    general,
                ))
            if not candidates:
                continue
            _, _, _, general = min(candidates, key=lambda item: item[:3])
            general_index = active.index(general)
            removed.add(general_index)
            replacements[specific_index] = replace(
                specific,
                norm_start=general.norm_start,
                orig_start=general.orig_start,
                source_text=text[general.orig_start:specific.orig_end],
                normalized_alias=norm[general.norm_start:specific.norm_end],
                rule_id=f"compose:{general.rule_id}+{specific.rule_id}",
            )

        result = [
            replacements.get(index, mention)
            for index, mention in enumerate(active)
            if index not in removed
        ]
        return sorted(result, key=lambda mention: (
            mention.norm_start, mention.norm_end, mention.target_id,
        ))

    def _attach_qualifiers(
        self,
        text: str,
        norm: str,
        mentions: list[Mention],
        qualifiers: list[Candidate],
    ) -> tuple[list[Mention], tuple[ResolutionIssue, ...]]:
        active = list(mentions)
        issues: list[ResolutionIssue] = []
        for qualifier in sorted(qualifiers, key=lambda candidate: (
            candidate.norm_start, candidate.norm_end, candidate.rule.rule_id,
        )):
            attachment = self._nearest_structural_primary(text, norm, active, qualifier)
            if attachment is None:
                continue
            index, primary = attachment
            family = self.family_of(primary)
            norm_start, norm_end, start, end = span_hull(primary, (qualifier,))

            if family not in qualifier.rule.allowed_retrieval_families:
                qualifier_targets = tuple(sorted({
                    target
                    for allowed_family in qualifier.rule.allowed_retrieval_families
                    if (target := self._lexicon.composition_target(
                        allowed_family,
                        qualifier.rule.qualifier_type or "",
                        qualifier.rule.qualifier_value or "",
                    )) is not None
                }))
                issues.append(ResolutionIssue(
                    issue_type=IssueType.CONFLICT,
                    source_text=text[start:end],
                    original_start=start,
                    original_end=end,
                    target_ids=tuple(sorted({primary.target_id, *qualifier_targets})),
                    code=_CODE_QUALIFIER_CONFLICT,
                    explanation=(
                        f"Qualifier {qualifier.rule.qualifier_type}="
                        f"{qualifier.rule.qualifier_value} incompatible con "
                        f"retrieval_family={family}."
                    ),
                ))
                del active[index]
                continue

            target_id = self._lexicon.composition_target(
                family,
                qualifier.rule.qualifier_type or "",
                qualifier.rule.qualifier_value or "",
            )
            if target_id is None:
                issues.append(ResolutionIssue(
                    issue_type=IssueType.CONFLICT,
                    source_text=text[start:end],
                    original_start=start,
                    original_end=end,
                    target_ids=(primary.target_id,),
                    code=_CODE_QUALIFIER_CONFLICT,
                    explanation="La composición no produjo un procedimiento canónico.",
                ))
                del active[index]
                continue

            if primary.target_kind == "procedure":
                if target_id == primary.target_id:
                    # Repetir el mismo efecto semántico es idempotente. Se
                    # amplía el hull consumido, pero no se crea otro match ni
                    # se altera el procedimiento ya fijado.
                    active[index] = replace(
                        primary,
                        norm_start=norm_start,
                        norm_end=norm_end,
                        orig_start=start,
                        orig_end=end,
                        source_text=text[start:end],
                        normalized_alias=norm[norm_start:norm_end],
                        rule_id=f"compose:{primary.rule_id}+{qualifier.rule.rule_id}",
                    )
                    continue

                # Un procedimiento específico ya expresa una opción concreta.
                # Otro qualifier que compone a un procedimiento distinto de la
                # misma familia es una contradicción local, no una autorización
                # para que el último qualifier gane silenciosamente.
                issues.append(ResolutionIssue(
                    issue_type=IssueType.CONFLICT,
                    source_text=text[start:end],
                    original_start=start,
                    original_end=end,
                    target_ids=tuple(sorted({primary.target_id, target_id})),
                    code=_CODE_QUALIFIER_VALUE_CONFLICT,
                    explanation=(
                        "Qualifiers sucesivos expresan valores incompatibles "
                        f"para retrieval_family={family}: "
                        f"{primary.target_id} vs {target_id}."
                    ),
                ))
                del active[index]
                continue

            canonical_rule = self._canonical_rule_by_procedure.get(target_id)
            active[index] = Mention(
                norm_start=norm_start,
                norm_end=norm_end,
                orig_start=start,
                orig_end=end,
                source_text=text[start:end],
                normalized_alias=norm[norm_start:norm_end],
                clause_index=primary.clause_index,
                target_kind="procedure",
                target_id=target_id,
                rule_id=f"compose:{primary.rule_id}+{qualifier.rule.rule_id}",
                match_strength=(
                    canonical_rule.match_strength if canonical_rule else primary.match_strength
                ),
                confidence=(canonical_rule.confidence if canonical_rule else primary.confidence),
                explicit_intent=primary.explicit_intent,
                assumption_code=(
                    canonical_rule.assumption_code if canonical_rule else primary.assumption_code
                ),
                procedure_ids=None,
            )

        return (
            sorted(active, key=lambda mention: (
                mention.norm_start, mention.norm_end, mention.target_id,
            )),
            tuple(issues),
        )

    def _nearest_structural_primary(
        self,
        text: str,
        norm: str,
        mentions: list[Mention],
        qualifier: Candidate,
    ) -> tuple[int, Mention] | None:
        rule = qualifier.rule
        candidates: list[tuple[int, int, int, Mention]] = []
        for index, mention in enumerate(mentions):
            if mention.clause_index != qualifier.clause_index:
                continue
            gap = attachment_gap(mention, qualifier)
            if gap is None:
                continue
            gap_start, gap_end, _, _ = gap
            if crosses_intervening_primary(mention, qualifier, mentions):
                continue
            norm_gap = norm[gap_start:gap_end]
            if has_external_negative_operator(norm_gap):
                continue
            if self._gap_has_any(norm_gap, _COORDINATORS):
                continue
            structural_cost = structural_attachment_cost(text, norm, gap)
            if structural_cost > (rule.max_attachment_tokens or 0):
                continue
            char_distance = max(0, gap_end - gap_start)
            candidates.append((structural_cost, char_distance, -mention.norm_end, mention))
        if not candidates:
            return None
        _, _, _, mention = min(candidates, key=lambda item: item[:3])
        return mentions.index(mention), mention
