"""Tests del adapter puro M2 → ModalityInterpretation (M3B-1)."""
from __future__ import annotations

import ast
import pathlib

import pytest

from app.core.modality_filter import ModalityFilter
from app.core.modality_frame_adapter import (
    ModalityAdaptationError,
    ModalityInterpretation,
    adapt_modality_resolution,
)
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

_APP_CORE = pathlib.Path(__file__).resolve().parents[1] / "app" / "core"


def _match(
    procedure_ids: tuple[str, ...],
    *,
    polarity: MatchPolarity = MatchPolarity.INCLUDE,
    assumption_code: str | None = None,
    start: int = 0,
    end: int = 5,
    target_id: str = "t",
) -> ModalityMatch:
    return ModalityMatch(
        target_kind=TargetKind.RETRIEVAL_FAMILY,
        target_id=target_id,
        retrieval_family="fam",
        procedure_ids=procedure_ids,
        legal_selection_family=None,
        procedure_variant=None,
        submission_mode=None,
        record_kind=None,
        polarity=polarity,
        source_text="src",
        normalized_alias="alias",
        original_start=start,
        original_end=end,
        rule_id="rule",
        strength=MatchStrength.EXACT_LEGAL,
        confidence=Confidence.HIGH,
        assumption_code=assumption_code,
        expansion_policy="family_default",
    )


def _issue(
    issue_type: IssueType = IssueType.AMBIGUITY,
    *,
    code: str = "SOME_CODE",
    target_ids: tuple[str, ...] = ("a", "b"),
) -> ResolutionIssue:
    return ResolutionIssue(
        issue_type=issue_type,
        source_text="subasta",
        original_start=0,
        original_end=7,
        target_ids=target_ids,
        code=code,
        explanation="explicación",
    )


def _resolution(status: ResolutionStatus, **kwargs) -> ModalityResolution:
    return ModalityResolution(status=status, normalized_text="norm", **kwargs)


# ── Matriz de status ────────────────────────────────────────────────────────

def test_no_match_leaves_both_filters_empty():
    interp = adapt_modality_resolution(_resolution(ResolutionStatus.NO_MATCH))
    assert interp.executable_filter.is_empty()
    assert interp.candidate_filter.is_empty()


def test_resolved_family_expands_into_executable():
    res = _resolution(
        ResolutionStatus.RESOLVED,
        includes=(_match(("seleccion_abreviada_menor_cuantia",
                          "seleccion_abreviada_subasta_inversa")),),
    )
    interp = adapt_modality_resolution(res)
    assert interp.executable_filter == ModalityFilter(
        include_procedure_ids=(
            "seleccion_abreviada_menor_cuantia",
            "seleccion_abreviada_subasta_inversa",
        )
    )
    assert interp.candidate_filter.is_empty()


def test_specific_procedure_into_executable():
    res = _resolution(
        ResolutionStatus.RESOLVED,
        includes=(_match(("seleccion_abreviada_subasta_inversa",)),),
    )
    interp = adapt_modality_resolution(res)
    assert interp.executable_filter.include_procedure_ids == (
        "seleccion_abreviada_subasta_inversa",
    )


def test_coordinated_alternatives_union_into_executable():
    res = _resolution(
        ResolutionStatus.RESOLVED,
        includes=(
            _match(("contratacion_directa",), target_id="a"),
            _match(("contratacion_directa_con_ofertas",), target_id="b"),
        ),
    )
    interp = adapt_modality_resolution(res)
    assert interp.executable_filter.include_procedure_ids == (
        "contratacion_directa",
        "contratacion_directa_con_ofertas",
    )


def test_exclude_only_resolved():
    res = _resolution(
        ResolutionStatus.RESOLVED,
        excludes=(_match(("contratacion_directa",), polarity=MatchPolarity.EXCLUDE),),
    )
    interp = adapt_modality_resolution(res)
    assert interp.executable_filter.include_procedure_ids == ()
    assert interp.executable_filter.exclude_procedure_ids == ("contratacion_directa",)
    assert interp.candidate_filter.is_empty()


def test_resolved_with_assumption_carries_codes():
    res = _resolution(
        ResolutionStatus.RESOLVED_WITH_ASSUMPTION,
        includes=(_match(("minima_cuantia",), assumption_code="ASSUME_X"),),
        assumption_codes=("ASSUME_X",),
    )
    interp = adapt_modality_resolution(res)
    assert interp.executable_filter.include_procedure_ids == ("minima_cuantia",)
    assert interp.assumption_codes == ("ASSUME_X",)


def test_partial_keeps_sane_matches_in_candidate_only():
    res = _resolution(
        ResolutionStatus.PARTIAL,
        includes=(_match(("licitacion_publica", "licitacion_publica_obra")),),
        issues=(_issue(code="BARE_SUBASTA_MULTIPLE_LEGAL_MEANINGS"),),
    )
    interp = adapt_modality_resolution(res)
    assert interp.executable_filter.is_empty()
    assert interp.candidate_filter.include_procedure_ids == (
        "licitacion_publica",
        "licitacion_publica_obra",
    )
    assert interp.issues[0].code == "BARE_SUBASTA_MULTIPLE_LEGAL_MEANINGS"


@pytest.mark.parametrize("status,issue_type", [
    (ResolutionStatus.AMBIGUOUS, IssueType.AMBIGUITY),
    (ResolutionStatus.CONFLICT, IssueType.CONFLICT),
    (ResolutionStatus.NOT_SEARCHABLE, IssueType.NOT_SEARCHABLE),
    (ResolutionStatus.DATASET_INAPPLICABLE, IssueType.DATASET_INAPPLICABLE),
])
def test_diagnostic_only_statuses_have_no_filter(status, issue_type):
    res = _resolution(status, issues=(_issue(issue_type),))
    interp = adapt_modality_resolution(res)
    assert interp.executable_filter.is_empty()
    assert interp.candidate_filter.is_empty()
    assert interp.resolution_status is status
    assert len(interp.issues) == 1


def test_ambiguous_never_becomes_filter_even_with_stray_match():
    # Contrato: candidatos ambiguos NUNCA se convierten en filtro.
    res = _resolution(
        ResolutionStatus.AMBIGUOUS,
        includes=(_match(("subasta_x",)),),
        issues=(_issue(),),
    )
    interp = adapt_modality_resolution(res)
    assert interp.executable_filter.is_empty()
    assert interp.candidate_filter.is_empty()


def test_multiple_issues_all_preserved():
    res = _resolution(
        ResolutionStatus.PARTIAL,
        includes=(_match(("licitacion_publica",)),),
        issues=(
            _issue(IssueType.AMBIGUITY, code="C1"),
            _issue(IssueType.CONFLICT, code="C2"),
        ),
    )
    interp = adapt_modality_resolution(res)
    assert {i.code for i in interp.issues} == {"C1", "C2"}


def test_multiple_clarification_codes_flagged_without_scalar_code():
    # Convención M2: >1 código ⇒ needs_clarification True, clarification_code None.
    res = _resolution(
        ResolutionStatus.AMBIGUOUS,
        issues=(_issue(code="C1"), _issue(code="C2")),
        needs_clarification=True,
        clarification_code=None,
    )
    interp = adapt_modality_resolution(res)
    assert interp.needs_clarification is True
    assert interp.clarification_code is None


def test_output_independent_of_match_order():
    matches = (
        _match(("b_id",), target_id="b"),
        _match(("a_id",), target_id="a"),
    )
    res1 = _resolution(ResolutionStatus.RESOLVED, includes=matches)
    res2 = _resolution(ResolutionStatus.RESOLVED, includes=tuple(reversed(matches)))
    assert (
        adapt_modality_resolution(res1).executable_filter
        == adapt_modality_resolution(res2).executable_filter
    )


def test_overlap_between_include_and_exclude_raises_adapter_error():
    res = _resolution(
        ResolutionStatus.RESOLVED,
        includes=(_match(("shared",)),),
        excludes=(_match(("shared",), polarity=MatchPolarity.EXCLUDE),),
    )
    with pytest.raises(ModalityAdaptationError):
        adapt_modality_resolution(res)


def test_consumed_spans_and_residual_passthrough():
    res = _resolution(
        ResolutionStatus.RESOLVED,
        includes=(_match(("minima_cuantia",)),),
        consumed_spans=((0, 5), (10, 14)),
        residual_text="resto",
        diagnostics=("DIAG_A",),
    )
    interp = adapt_modality_resolution(res)
    assert interp.consumed_spans == ((0, 5), (10, 14))
    assert interp.residual_text == "resto"
    assert interp.diagnostics == ("DIAG_A",)


def test_wrong_input_type_raises():
    with pytest.raises(ModalityAdaptationError):
        adapt_modality_resolution({"status": "resolved"})  # type: ignore[arg-type]


def test_interpretation_is_frozen():
    interp = adapt_modality_resolution(_resolution(ResolutionStatus.NO_MATCH))
    assert isinstance(interp, ModalityInterpretation)
    with pytest.raises(Exception):
        interp.residual_text = "x"  # type: ignore[misc]


# ── Frontera de imports (no acoplar a router/frame/builder/radar/warehouse) ──

_FORBIDDEN_IMPORT_PREFIXES = (
    "app.core.query_router",
    "app.core.query_frame",
    "app.core.soql_builder",
    "app.core.followup",
    "app.core.orchestrator",
    "app.core.radar",
    "app.core.llm_handler",
    "warehouse",
)

_NEW_MODULES = (
    "modality_filter.py",
    "modality_frame_adapter.py",
    "query_text_consumption.py",
)


def _imported_modules(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


@pytest.mark.parametrize("module_name", _NEW_MODULES)
def test_new_modules_do_not_import_forbidden_layers(module_name):
    imports = _imported_modules(_APP_CORE / module_name)
    offenders = [
        name for name in imports
        if any(name.startswith(prefix) for prefix in _FORBIDDEN_IMPORT_PREFIXES)
    ]
    assert not offenders, f"{module_name} importa capas prohibidas: {offenders}"
