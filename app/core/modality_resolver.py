"""Orquestador puro del resolver lingüístico de modalidades M2-R1.4.

Fases: detectar → componer → asignar polaridad → resolver catálogo/dataset →
ensamblar. La mecánica textual, la composición y los scopes viven en módulos
separados; este archivo conserva la fachada pública y el contrato de salida.
"""
from __future__ import annotations

from app.core.modality_composition import Candidate, CompositionEngine, Mention
from app.core.modality_lexicon import ModalityLexicon, load_lexicon
from app.core.modality_models import ModalityCatalog, load_catalog
from app.core.modality_resolution_models import (
    Confidence,
    IssueType,
    MatchPolarity,
    MatchStrength,
    ModalityMatch,
    ModalityResolution,
    ResolutionIssue,
    ResolutionStatus,
    TargetKind,
)
from app.core.modality_scope import assign_polarities
from app.core.modality_text import (
    consumed_spans,
    normalize_with_map,
    residual_text,
    segment_clauses,
    validate_non_overlapping_spans,
)

_VALID_DATASETS = frozenset({"procesos", "contratos"})
_DIAG_PENDING_REVIEW = "LEGAL_CLASSIFICATION_PENDING_REVIEW"
_DIAG_NON_CONTRACTUAL = "NON_CONTRACTUAL_PLANNING_RECORD"
_DIAG_PLATFORM_TEST = "PLATFORM_TEST_NOT_USER_SEARCHABLE"
_CODE_DATASET_INAPPLICABLE = "MODALITY_DATASET_INAPPLICABLE"

_ISSUE_STATUS_PRECEDENCE = (
    IssueType.CONFLICT,
    IssueType.AMBIGUITY,
    IssueType.NOT_SEARCHABLE,
    IssueType.DATASET_INAPPLICABLE,
)


class ModalityResolutionError(ValueError):
    """Entrada inválida para el contrato público del resolver."""


class ModalityResolver:
    """Compone catálogo M1 y lexicón M2 sin efectos externos."""

    def __init__(self, catalog: ModalityCatalog, lexicon: ModalityLexicon) -> None:
        self._catalog = catalog
        self._lexicon = lexicon
        self._composition = CompositionEngine(catalog, lexicon)

    def resolve(self, text: str, dataset: str | None = None) -> ModalityResolution:
        self._validate_dataset(dataset)
        norm, positions = normalize_with_map(text)
        if not norm:
            return ModalityResolution(
                status=ResolutionStatus.NO_MATCH,
                normalized_text=norm,
                residual_text=" ".join(text.split()),
            )

        clauses = segment_clauses(text, norm, positions)
        detected_candidates = self._composition.collect_candidates(
            text, norm, positions, clauses,
        )
        qualifier_spans = tuple(
            (candidate.norm_start, candidate.norm_end)
            for candidate in detected_candidates
            if candidate.rule.target_kind in {"qualifier", "qualifier_set"}
        )
        alternative_mentions, alternative_issues, alternative_spans = (
            self._composition.analyze_alternatives(text, norm, detected_candidates)
        )
        remaining_candidates = [
            candidate for candidate in detected_candidates
            if not any(
                candidate.norm_start < span_end and span_start < candidate.norm_end
                for span_start, span_end in alternative_spans
            )
        ]
        candidates = self._composition.select_non_overlapping(remaining_candidates)
        primaries, qualifiers, ambiguities = self._partition_candidates(candidates)
        mentions, conflict_issues = self._composition.compose(
            text, norm, primaries, qualifiers,
        )
        mentions.extend(alternative_mentions)
        mentions.sort(key=lambda mention: (
            mention.norm_start, mention.norm_end, mention.target_id,
        ))
        conflict_issues = tuple([*conflict_issues, *alternative_issues])
        polarities = assign_polarities(
            norm, mentions, clauses, qualifier_spans=qualifier_spans,
        )

        includes: list[ModalityMatch] = []
        excludes: list[ModalityMatch] = []
        issues: list[ResolutionIssue] = list(conflict_issues)
        diagnostics: set[str] = {issue.code for issue in conflict_issues}

        for mention, polarity in zip(mentions, polarities):
            match, issue, local_diagnostics = self._resolve_mention(
                mention, polarity, dataset,
            )
            diagnostics.update(local_diagnostics)
            if issue is not None:
                issues.append(issue)
                diagnostics.add(issue.code)
            elif match is not None:
                (includes if match.polarity is MatchPolarity.INCLUDE else excludes).append(match)

        issues.extend(self._ambiguity_issues(ambiguities))
        includes.sort(key=self._match_sort_key)
        excludes.sort(key=self._match_sort_key)
        issues.sort(key=self._issue_sort_key)
        self._assert_non_overlapping(includes, excludes, issues)

        status = self._status(includes, excludes, issues)
        clarification_codes = {
            issue.code for issue in issues
            if issue.issue_type in {IssueType.AMBIGUITY, IssueType.CONFLICT}
        }
        spans = consumed_spans(includes, excludes, issues)
        return ModalityResolution(
            status=status,
            normalized_text=norm,
            includes=tuple(includes),
            excludes=tuple(excludes),
            issues=tuple(issues),
            consumed_spans=spans,
            residual_text=residual_text(text, spans),
            needs_clarification=bool(clarification_codes),
            clarification_code=(
                next(iter(clarification_codes)) if len(clarification_codes) == 1 else None
            ),
            assumption_codes=tuple(sorted({
                match.assumption_code for match in includes if match.assumption_code
            })),
            diagnostics=tuple(sorted(diagnostics)),
        )

    @staticmethod
    def _validate_dataset(dataset: str | None) -> None:
        if dataset is None:
            return
        if type(dataset) is not str or dataset not in _VALID_DATASETS:  # noqa: E721
            raise ModalityResolutionError(
                "dataset debe ser exactamente None, 'procesos' o 'contratos' "
                f"(recibido {dataset!r})"
            )

    @staticmethod
    def _partition_candidates(
        candidates: list[Candidate],
    ) -> tuple[list[Candidate], list[Candidate], list[Candidate]]:
        return (
            [candidate for candidate in candidates
             if candidate.rule.target_kind in {"retrieval_family", "procedure"}],
            [candidate for candidate in candidates if candidate.rule.target_kind == "qualifier"],
            [candidate for candidate in candidates if candidate.rule.target_kind == "ambiguity"],
        )

    def _resolve_mention(
        self,
        mention: Mention,
        polarity: MatchPolarity,
        dataset: str | None,
    ) -> tuple[ModalityMatch | None, ResolutionIssue | None, tuple[str, ...]]:
        if mention.target_kind == "procedure":
            return self._resolve_procedure(mention, polarity, dataset)
        return self._resolve_family(mention, polarity, dataset)

    def _resolve_procedure(
        self,
        mention: Mention,
        polarity: MatchPolarity,
        dataset: str | None,
    ) -> tuple[ModalityMatch | None, ResolutionIssue | None, tuple[str, ...]]:
        procedure = self._catalog.by_id(mention.target_id)
        assert procedure is not None
        if procedure.record_kind == "platform_test" or not procedure.user_searchable:
            issue = ResolutionIssue(
                issue_type=IssueType.NOT_SEARCHABLE,
                source_text=mention.source_text,
                original_start=mention.orig_start,
                original_end=mention.orig_end,
                target_ids=(procedure.procedure_id,),
                code=_DIAG_PLATFORM_TEST,
                explanation="El registro es un artefacto de plataforma no buscable.",
            )
            return None, issue, (_DIAG_PLATFORM_TEST,)
        if dataset is not None and not procedure.applies_to(dataset):
            issue = ResolutionIssue(
                issue_type=IssueType.DATASET_INAPPLICABLE,
                source_text=mention.source_text,
                original_start=mention.orig_start,
                original_end=mention.orig_end,
                target_ids=(procedure.procedure_id,),
                code=_CODE_DATASET_INAPPLICABLE,
                explanation=f"El procedimiento no está disponible en dataset={dataset}.",
            )
            return None, issue, ()
        match = self._make_match(
            mention=mention,
            polarity=polarity,
            target_kind=TargetKind.PROCEDURE,
            target_id=procedure.procedure_id,
            retrieval_family=procedure.retrieval_family,
            procedure_ids=(procedure.procedure_id,),
            legal_selection_family=procedure.legal_selection_family,
            procedure_variant=procedure.procedure_variant,
            submission_mode=procedure.submission_mode,
            record_kind=procedure.record_kind,
            expansion_policy=procedure.parent_expansion_policy,
        )
        return match, None, self._diagnostics_for_ids((procedure.procedure_id,))

    def _resolve_family(
        self,
        mention: Mention,
        polarity: MatchPolarity,
        dataset: str | None,
    ) -> tuple[ModalityMatch | None, ResolutionIssue | None, tuple[str, ...]]:
        if mention.procedure_ids is not None:
            procedure_ids = tuple(sorted(set(mention.procedure_ids)))
        else:
            expandable = [
                child for child in self._catalog.retrieval_family(mention.target_id)
                if child.user_searchable
                and child.parent_expansion_policy != "never"
                and (
                    child.parent_expansion_policy == "family_default"
                    or mention.explicit_intent
                )
            ]
            procedure_ids = tuple(sorted(child.procedure_id for child in expandable))
        if dataset is not None:
            procedure_ids = tuple(
                procedure_id for procedure_id in procedure_ids
                if self._catalog.by_id(procedure_id).applies_to(dataset)
            )
        if not procedure_ids:
            issue = ResolutionIssue(
                issue_type=IssueType.DATASET_INAPPLICABLE,
                source_text=mention.source_text,
                original_start=mention.orig_start,
                original_end=mention.orig_end,
                target_ids=(
                    mention.procedure_ids if mention.procedure_ids is not None
                    else (mention.target_id,)
                ),
                code=_CODE_DATASET_INAPPLICABLE,
                explanation=f"La familia no tiene procedimientos aplicables a dataset={dataset}.",
            )
            return None, issue, ()

        selected = [self._catalog.by_id(procedure_id) for procedure_id in procedure_ids]
        match = self._make_match(
            mention=mention,
            polarity=polarity,
            target_kind=TargetKind.RETRIEVAL_FAMILY,
            target_id=mention.target_id,
            retrieval_family=mention.target_id,
            procedure_ids=procedure_ids,
            legal_selection_family=_uniform(child.legal_selection_family for child in selected),
            procedure_variant=None,
            submission_mode=None,
            record_kind=_uniform(child.record_kind for child in selected),
            expansion_policy=(
                "explicit_only"
                if all(child.parent_expansion_policy == "explicit_only" for child in selected)
                else "family_default"
            ),
        )
        return match, None, self._diagnostics_for_ids(procedure_ids)

    @staticmethod
    def _make_match(**kwargs) -> ModalityMatch:
        mention: Mention = kwargs["mention"]
        polarity: MatchPolarity = kwargs["polarity"]
        return ModalityMatch(
            target_kind=kwargs["target_kind"],
            target_id=kwargs["target_id"],
            retrieval_family=kwargs["retrieval_family"],
            procedure_ids=kwargs["procedure_ids"],
            legal_selection_family=kwargs["legal_selection_family"],
            procedure_variant=kwargs["procedure_variant"],
            submission_mode=kwargs["submission_mode"],
            record_kind=kwargs["record_kind"],
            polarity=polarity,
            source_text=mention.source_text,
            normalized_alias=mention.normalized_alias,
            original_start=mention.orig_start,
            original_end=mention.orig_end,
            rule_id=mention.rule_id,
            strength=MatchStrength(mention.match_strength),
            confidence=Confidence(mention.confidence),
            assumption_code=(
                mention.assumption_code if polarity is MatchPolarity.INCLUDE else None
            ),
            expansion_policy=kwargs["expansion_policy"],
        )

    def _diagnostics_for_ids(self, procedure_ids: tuple[str, ...]) -> tuple[str, ...]:
        diagnostics: set[str] = set()
        for procedure_id in procedure_ids:
            procedure = self._catalog.by_id(procedure_id)
            assert procedure is not None
            if procedure.legal_classification_status == "pending_review":
                diagnostics.add(_DIAG_PENDING_REVIEW)
            if procedure.record_kind == "planning_rfi":
                diagnostics.add(_DIAG_NON_CONTRACTUAL)
        return tuple(sorted(diagnostics))

    @staticmethod
    def _ambiguity_issues(candidates: list[Candidate]) -> tuple[ResolutionIssue, ...]:
        return tuple(
            ResolutionIssue(
                issue_type=IssueType.AMBIGUITY,
                source_text=candidate.source_text,
                original_start=candidate.orig_start,
                original_end=candidate.orig_end,
                target_ids=candidate.rule.candidate_target_ids,
                code=candidate.rule.ambiguity_code or "",
                explanation=candidate.rule.notes,
            )
            for candidate in sorted(candidates, key=lambda item: (
                item.orig_start, item.orig_end, item.rule.rule_id,
            ))
        )

    @staticmethod
    def _status(
        includes: list[ModalityMatch],
        excludes: list[ModalityMatch],
        issues: list[ResolutionIssue],
    ) -> ResolutionStatus:
        has_matches = bool(includes or excludes)
        if has_matches and issues:
            return ResolutionStatus.PARTIAL
        if has_matches:
            return (
                ResolutionStatus.RESOLVED_WITH_ASSUMPTION
                if any(match.assumption_code for match in includes)
                else ResolutionStatus.RESOLVED
            )
        if not issues:
            return ResolutionStatus.NO_MATCH
        issue_types = {issue.issue_type for issue in issues}
        issue_type = (
            next(iter(issue_types)) if len(issue_types) == 1
            else next(candidate for candidate in _ISSUE_STATUS_PRECEDENCE
                      if candidate in issue_types)
        )
        return {
            IssueType.AMBIGUITY: ResolutionStatus.AMBIGUOUS,
            IssueType.CONFLICT: ResolutionStatus.CONFLICT,
            IssueType.NOT_SEARCHABLE: ResolutionStatus.NOT_SEARCHABLE,
            IssueType.DATASET_INAPPLICABLE: ResolutionStatus.DATASET_INAPPLICABLE,
        }[issue_type]

    @staticmethod
    def _match_sort_key(match: ModalityMatch) -> tuple[int, int, str]:
        return (match.original_start, match.original_end, match.target_id)

    @staticmethod
    def _issue_sort_key(issue: ResolutionIssue) -> tuple[int, int, str, str]:
        return (issue.original_start, issue.original_end, issue.issue_type.value, issue.code)

    @staticmethod
    def _assert_non_overlapping(
        includes: list[ModalityMatch],
        excludes: list[ModalityMatch],
        issues: list[ResolutionIssue],
    ) -> None:
        try:
            validate_non_overlapping_spans(includes, excludes, issues)
        except ValueError as exc:
            raise RuntimeError("el resolver produjo spans finales solapados") from exc


def _uniform(values) -> str | None:
    distinct = set(values)
    return next(iter(distinct)) if len(distinct) == 1 else None


_DEFAULT_RESOLVER: ModalityResolver | None = None


def _default_resolver() -> ModalityResolver:
    global _DEFAULT_RESOLVER
    if _DEFAULT_RESOLVER is None:
        catalog = load_catalog()
        _DEFAULT_RESOLVER = ModalityResolver(catalog, load_lexicon(catalog=catalog))
    return _DEFAULT_RESOLVER


def resolve(text: str, dataset: str | None = None) -> ModalityResolution:
    """Fachada del resolver default."""
    return _default_resolver().resolve(text, dataset)
