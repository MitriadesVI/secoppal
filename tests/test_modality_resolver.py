"""Corpus adversarial y propiedades del resolver modular M2-R1.5."""
from __future__ import annotations

import copy
import random
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import pytest

from app.core.modality_lexicon import (
    build_lexicon,
    load_lexicon,
    normalize_phrase,
    strict_yaml_load,
)
from app.core.modality_models import load_catalog
from app.core.modality_resolution_models import (
    IssueType,
    ModalityResolution,
    ResolutionStatus,
)
from app.core.modality_resolver import (
    ModalityResolutionError,
    ModalityResolver,
    resolve,
)

_CORPUS_PATH = Path(__file__).parent / "fixtures" / "modality_resolver_corpus_v1.yaml"


def _load_corpus_document() -> dict:
    with _CORPUS_PATH.open("r", encoding="utf-8") as handle:
        data = strict_yaml_load(handle)
    assert isinstance(data, dict)
    return data


CORPUS_DOCUMENT = _load_corpus_document()
CORPUS = CORPUS_DOCUMENT["cases"]
CASE_IDS = [case["case_id"] for case in CORPUS]


@pytest.fixture(scope="module")
def resolver() -> ModalityResolver:
    catalog = load_catalog()
    return ModalityResolver(catalog, load_lexicon(catalog=catalog))


def _issue_view(resolution) -> list[dict]:
    return [
        {
            "issue_type": issue.issue_type.value,
            "code": issue.code,
            "target_ids": list(issue.target_ids),
            "source_text": issue.source_text,
        }
        for issue in resolution.issues
    ]


@pytest.mark.parametrize("case", CORPUS, ids=CASE_IDS)
def test_corpus_case(case: dict, resolver: ModalityResolver) -> None:
    result = resolver.resolve(case["query"], case["dataset"])

    assert result.status.value == case["expected_status"], case["case_id"]
    assert sorted(match.target_id for match in result.includes) == sorted(
        case["expected_include_targets"]
    ), case["case_id"]
    assert sorted(match.target_id for match in result.excludes) == sorted(
        case["expected_exclude_targets"]
    ), case["case_id"]
    assert list(result.assumption_codes) == sorted(case["expected_assumption_codes"])
    assert _issue_view(result) == case["expected_issues"], case["case_id"]

    if case["expected_procedure_ids"] is not None:
        got = sorted({pid for match in result.includes for pid in match.procedure_ids})
        assert got == sorted(case["expected_procedure_ids"]), case["case_id"]

    if case["expected_ambiguity_code"] is not None:
        assert case["expected_ambiguity_code"] in {
            ambiguity.ambiguity_code for ambiguity in result.ambiguities
        }

    if case["expected_consumed_texts"] is not None:
        got = [case["query"][start:end] for start, end in result.consumed_spans]
        assert got == case["expected_consumed_texts"], case["case_id"]

    if case["expected_residual_text"] is not None:
        assert result.residual_text == case["expected_residual_text"], case["case_id"]


# ── Corpus, esquema y métricas honestas ─────────────────────────────────────

_REQUIRED_CASE_KEYS = {
    "case_id", "query", "dataset", "expected_status",
    "expected_include_targets", "expected_exclude_targets",
    "expected_procedure_ids", "expected_assumption_codes",
    "expected_ambiguity_code", "expected_consumed_texts",
    "expected_residual_text", "expected_issues", "notes",
}


def test_corpus_schema_and_unique_case_ids() -> None:
    assert len(CASE_IDS) == len(set(CASE_IDS))
    for case in CORPUS:
        assert _REQUIRED_CASE_KEYS <= set(case), case["case_id"]
        assert case["dataset"] in {None, "procesos", "contratos"}
        assert isinstance(case["expected_issues"], list)


def test_corpus_distribution_gates() -> None:
    counts = Counter(case["case_id"][0] for case in CORPUS)
    minima = {"A": 30, "B": 30, "C": 30, "D": 20, "E": 20, "F": 15, "G": 15, "H": 20}
    for category, minimum in minima.items():
        assert counts[category] >= minimum
    for category in "IJKL":
        assert counts[category] > 0


def test_corpus_metrics_are_exact_and_honest() -> None:
    metrics = CORPUS_DOCUMENT["metrics"]
    calculated = {
        "corpus_rows": len(CORPUS),
        "unique_queries": len({case["query"] for case in CORPUS}),
        "unique_normalized_queries": len({normalize_phrase(case["query"]) for case in CORPUS}),
        "new_unique_critical_cases": len({
            normalize_phrase(case["query"])
            for case in CORPUS
            if case["case_id"][0] in {"I", "J", "K"}
        }),
    }
    assert metrics == calculated
    assert metrics["corpus_rows"] >= 210
    assert metrics["new_unique_critical_cases"] >= 30
    assert metrics["unique_normalized_queries"] <= metrics["unique_queries"] <= metrics["corpus_rows"]


def test_duplicate_outcomes_require_reason() -> None:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for case in CORPUS:
        issue_signature = tuple(
            (
                issue["issue_type"], issue["code"], tuple(issue["target_ids"]),
                issue["source_text"],
            )
            for issue in case["expected_issues"]
        )
        outcome = (
            case["expected_status"],
            tuple(case["expected_include_targets"]),
            tuple(case["expected_exclude_targets"]),
            issue_signature,
        )
        groups[(normalize_phrase(case["query"]), case["dataset"], outcome)].append(case)
    for duplicates in groups.values():
        if len(duplicates) > 1:
            assert all(case.get("duplicate_reason") for case in duplicates)


def test_corpus_duplicate_yaml_key_rejected(tmp_path: Path) -> None:
    path = tmp_path / "bad_corpus.yaml"
    path.write_text(
        "version: 1\nmetrics: {}\ncases:\n  - case_id: X01\n    case_id: X02\n",
        encoding="utf-8",
    )
    with pytest.raises(Exception, match="clave YAML duplicada"):
        with path.open("r", encoding="utf-8") as handle:
            strict_yaml_load(handle)


# ── Composición y attachment ────────────────────────────────────────────────

@pytest.mark.parametrize(
    "direct,composed",
    [
        ("contratación directa con ofertas", "contratación directa que permita presentar ofertas"),
        ("régimen especial con ofertas", "régimen especial en el que se reciban ofertas"),
        ("concurso de méritos con precalificación", "concurso de méritos que tenga precalificación"),
        ("selección abreviada por subasta inversa", "selección abreviada mediante una subasta inversa"),
    ],
)
def test_fast_path_and_composition_same_semantic_fingerprint(
    direct: str, composed: str, resolver: ModalityResolver,
) -> None:
    a = resolver.resolve(direct)
    b = resolver.resolve(composed)
    assert _semantic_fingerprint(a) == _semantic_fingerprint(b)


def test_structural_attachment_precedes_compatibility(resolver: ModalityResolver) -> None:
    result = resolver.resolve("concurso de méritos y subasta inversa con precalificación")
    assert result.status is ResolutionStatus.PARTIAL
    assert [match.target_id for match in result.includes] == ["concurso_de_meritos"]
    assert len(result.issues) == 1
    issue = result.issues[0]
    assert issue.issue_type is IssueType.CONFLICT
    assert issue.source_text == "subasta inversa con precalificación"
    assert "seleccion_abreviada_subasta_inversa" in issue.target_ids
    assert "concurso_meritos_precalificacion" in issue.target_ids


def test_contaminated_primary_not_retained_as_match(resolver: ModalityResolver) -> None:
    result = resolver.resolve("subasta inversa con precalificación")
    assert result.status is ResolutionStatus.CONFLICT
    assert not result.includes and not result.excludes
    assert [issue.issue_type for issue in result.issues] == [IssueType.CONFLICT]


@pytest.mark.parametrize(
    "query,target_ids",
    [
        (
            "contratación directa con ofertas sin ofertas",
            ("contratacion_directa", "contratacion_directa_con_ofertas"),
        ),
        (
            "contratación directa sin ofertas con ofertas",
            ("contratacion_directa", "contratacion_directa_con_ofertas"),
        ),
        (
            "régimen especial con ofertas sin ofertas",
            ("regimen_especial", "regimen_especial_con_ofertas"),
        ),
        (
            "régimen especial sin ofertas donde se reciban ofertas",
            ("regimen_especial", "regimen_especial_con_ofertas"),
        ),
        (
            "concurso de méritos abierto con precalificación",
            ("concurso_meritos_abierto", "concurso_meritos_precalificacion"),
        ),
    ],
)
def test_contradictory_qualifiers_create_local_conflict(
    query: str,
    target_ids: tuple[str, str],
    resolver: ModalityResolver,
) -> None:
    result = resolver.resolve(query)
    assert result.status is ResolutionStatus.CONFLICT
    assert not result.includes and not result.excludes
    assert len(result.issues) == 1
    issue = result.issues[0]
    assert issue.issue_type is IssueType.CONFLICT
    assert issue.code == "MODALITY_QUALIFIER_VALUE_CONFLICT"
    assert issue.target_ids == tuple(sorted(target_ids))
    assert issue.source_text == query
    assert result.consumed_spans == ((0, len(query)),)


@pytest.mark.parametrize(
    "query,target",
    [
        (
            "concurso de méritos con precalificación que tenga precalificación",
            "concurso_meritos_precalificacion",
        ),
        (
            "contratación directa con ofertas donde se reciban ofertas",
            "contratacion_directa_con_ofertas",
        ),
        (
            "contratación directa en la que se reciban ofertas donde se reciban ofertas",
            "contratacion_directa_con_ofertas",
        ),
    ],
)
def test_repeated_qualifiers_are_idempotent(
    query: str, target: str, resolver: ModalityResolver,
) -> None:
    result = resolver.resolve(query)
    assert result.status is ResolutionStatus.RESOLVED
    assert [match.target_id for match in result.includes] == [target]
    assert not result.excludes and not result.issues
    assert result.includes[0].source_text == query
    assert result.consumed_spans == ((0, len(query)),)


def test_composed_procedure_then_contradictory_qualifier_conflicts(
    resolver: ModalityResolver,
) -> None:
    query = "contratación directa que permita recibir ofertas sin que haya ofertas"
    result = resolver.resolve(query)
    assert result.status is ResolutionStatus.CONFLICT
    assert not result.includes and not result.excludes
    assert len(result.issues) == 1
    issue = result.issues[0]
    assert issue.code == "MODALITY_QUALIFIER_VALUE_CONFLICT"
    assert issue.target_ids == (
        "contratacion_directa",
        "contratacion_directa_con_ofertas",
    )
    assert issue.source_text == query


def test_contradictory_qualifier_order_has_same_semantic_conflict(
    resolver: ModalityResolver,
) -> None:
    forward = resolver.resolve("contratación directa con ofertas sin ofertas")
    reverse = resolver.resolve("contratación directa sin ofertas con ofertas")
    assert forward.status is reverse.status is ResolutionStatus.CONFLICT
    assert forward.issues[0].code == reverse.issues[0].code
    assert forward.issues[0].target_ids == reverse.issues[0].target_ids


def test_qualifier_conflict_is_partial_when_another_match_is_valid(
    resolver: ModalityResolver,
) -> None:
    query = "mínima cuantía y contratación directa con ofertas sin ofertas"
    result = resolver.resolve(query)
    assert result.status is ResolutionStatus.PARTIAL
    assert [match.target_id for match in result.includes] == ["minima_cuantia"]
    assert not result.excludes
    assert len(result.issues) == 1
    assert result.issues[0].code == "MODALITY_QUALIFIER_VALUE_CONFLICT"
    assert result.issues[0].source_text == "contratación directa con ofertas sin ofertas"


@pytest.mark.parametrize(
    "query,issue_code",
    [
        (
            "mínima cuantía y contratación directa con ofertas sin ofertas "
            "y licitación pública",
            "MODALITY_QUALIFIER_VALUE_CONFLICT",
        ),
        (
            "mínima cuantía y contratación directa sin ofertas con ofertas "
            "y licitación pública",
            "MODALITY_QUALIFIER_VALUE_CONFLICT",
        ),
        (
            "mínima cuantía y régimen especial con ofertas sin ofertas "
            "y licitación pública",
            "MODALITY_QUALIFIER_VALUE_CONFLICT",
        ),
        (
            "mínima cuantía, contratación directa con ofertas sin ofertas, "
            "licitación pública",
            "MODALITY_QUALIFIER_VALUE_CONFLICT",
        ),
    ],
)
def test_technical_qualifier_conflict_does_not_leak_polarity(
    query: str, issue_code: str, resolver: ModalityResolver,
) -> None:
    result = resolver.resolve(query)
    assert result.status is ResolutionStatus.PARTIAL
    assert [match.target_id for match in result.includes] == [
        "minima_cuantia", "licitacion_publica",
    ]
    assert not result.excludes
    assert [issue.code for issue in result.issues] == [issue_code]


def test_external_negation_remains_visible_after_technical_qualifier(
    resolver: ModalityResolver,
) -> None:
    result = resolver.resolve(
        "contratación directa sin ofertas y no licitación pública"
    )
    assert result.status is ResolutionStatus.RESOLVED
    assert [match.target_id for match in result.includes] == ["contratacion_directa"]
    assert [match.target_id for match in result.excludes] == ["licitacion_publica"]
    assert not result.issues


def test_incompatible_technical_qualifier_does_not_negate_following_match(
    resolver: ModalityResolver,
) -> None:
    result = resolver.resolve("mínima cuantía sin ofertas y licitación pública")
    assert result.status is ResolutionStatus.PARTIAL
    assert [match.target_id for match in result.includes] == ["licitacion_publica"]
    assert not result.excludes
    assert [issue.code for issue in result.issues] == [
        "MODALITY_QUALIFIER_FAMILY_CONFLICT"
    ]


@pytest.mark.parametrize(
    "query",
    [
        "concurso de méritos sin precalificación",
        "concurso de méritos sin ninguna precalificación",
        "concurso de méritos sin que haya precalificación",
        "concursos de méritos que no tengan precalificación",
    ],
)
def test_explicit_negative_prequalification_composes_open_procedure(
    query: str, resolver: ModalityResolver,
) -> None:
    result = resolver.resolve(query)
    assert result.status is ResolutionStatus.RESOLVED
    assert [match.target_id for match in result.includes] == ["concurso_meritos_abierto"]
    assert not result.excludes and not result.issues
    assert result.consumed_spans == ((0, len(query)),)


def test_positive_prequalification_still_composes_positive_target(
    resolver: ModalityResolver,
) -> None:
    result = resolver.resolve("concurso de méritos con precalificación")
    assert result.status is ResolutionStatus.RESOLVED
    assert [match.target_id for match in result.includes] == [
        "concurso_meritos_precalificacion"
    ]


@pytest.mark.parametrize(
    "query",
    [
        "concurso de méritos con precalificación sin precalificación",
        "concurso de méritos sin precalificación con precalificación",
    ],
)
def test_positive_and_negative_prequalification_conflict(
    query: str, resolver: ModalityResolver,
) -> None:
    result = resolver.resolve(query)
    assert result.status is ResolutionStatus.CONFLICT
    assert not result.includes and not result.excludes
    assert len(result.issues) == 1
    issue = result.issues[0]
    assert issue.code == "MODALITY_QUALIFIER_VALUE_CONFLICT"
    assert issue.target_ids == (
        "concurso_meritos_abierto",
        "concurso_meritos_precalificacion",
    )
    assert issue.source_text == query


def test_repeated_negative_prequalification_is_idempotent(
    resolver: ModalityResolver,
) -> None:
    query = "concurso de méritos sin precalificación que no tenga precalificación"
    result = resolver.resolve(query)
    assert result.status is ResolutionStatus.RESOLVED
    assert [match.target_id for match in result.includes] == ["concurso_meritos_abierto"]
    assert not result.excludes and not result.issues
    assert result.includes[0].source_text == query


def test_external_negation_blocks_positive_prequalification_attachment(
    resolver: ModalityResolver,
) -> None:
    result = resolver.resolve("concurso de méritos no con precalificación")
    assert result.status is ResolutionStatus.RESOLVED
    assert [match.target_id for match in result.includes] == ["concurso_de_meritos"]
    assert "concurso_meritos_precalificacion" not in {
        match.target_id for match in result.includes
    }
    assert result.residual_text == "no con precalificación"


def test_anti_negation_does_not_block_positive_prequalification(
    resolver: ModalityResolver,
) -> None:
    result = resolver.resolve("concurso de méritos no solo con precalificación")
    assert result.status is ResolutionStatus.RESOLVED
    assert [match.target_id for match in result.includes] == [
        "concurso_meritos_precalificacion"
    ]
    assert not result.issues


# ── Contexto y polaridad locales ────────────────────────────────────────────

@pytest.mark.parametrize(
    "query,includes,excludes,status",
    [
        (
            "no solo contratación directa sino también mínima cuantía",
            ["contratacion_directa", "minima_cuantia"],
            [],
            "resolved",
        ),
        ("sin descartar la contratación directa", ["contratacion_directa"], [], "resolved"),
        ("excepto contratación directa y mínima cuantía", [], ["contratacion_directa", "minima_cuantia"], "resolved"),
        (
            "licitación pública, no selección abreviada ni mínima cuantía",
            ["licitacion_publica"],
            ["seleccion_abreviada", "minima_cuantia"],
            "resolved",
        ),
    ],
)
def test_negation_and_anti_negation_scopes(
    query: str, includes: list[str], excludes: list[str], status: str,
    resolver: ModalityResolver,
) -> None:
    result = resolver.resolve(query)
    assert result.status.value == status
    assert [match.target_id for match in result.includes] == includes
    assert [match.target_id for match in result.excludes] == excludes


def test_context_is_evaluated_per_occurrence(resolver: ModalityResolver) -> None:
    result = resolver.resolve("subasta de alimentos y exposición privada de arte")
    assert result.status is ResolutionStatus.AMBIGUOUS
    assert result.issues[0].source_text == "subasta"

    result = resolver.resolve("subasta privada de arte y licitación pública")
    assert result.status is ResolutionStatus.RESOLVED
    assert [match.target_id for match in result.includes] == ["licitacion_publica"]
    assert not result.issues


def test_contrastive_boundary_resets_polarity(resolver: ModalityResolver) -> None:
    result = resolver.resolve(
        "no busco subasta privada de arte, sino subasta de bienes del Estado"
    )
    assert [match.target_id for match in result.includes] == ["enajenacion_subasta"]
    assert not result.excludes and not result.issues




@pytest.mark.parametrize(
    "query,includes,excludes",
    [
        (
            "excepto contratación directa, mínima cuantía y licitación pública",
            [],
            ["contratacion_directa", "minima_cuantia", "licitacion_publica"],
        ),
        (
            "no contratación directa, mínima cuantía ni licitación pública",
            [],
            ["contratacion_directa", "minima_cuantia", "licitacion_publica"],
        ),
        (
            "licitación pública, excepto contratación directa, mínima cuantía y concurso de méritos",
            ["licitacion_publica"],
            ["contratacion_directa", "minima_cuantia", "concurso_de_meritos"],
        ),
        (
            "salvo contratación directa, régimen especial o mínima cuantía",
            [],
            ["contratacion_directa", "regimen_especial", "minima_cuantia"],
        ),
    ],
)
def test_negative_scope_propagates_through_comma_lists(
    query: str,
    includes: list[str],
    excludes: list[str],
    resolver: ModalityResolver,
) -> None:
    result = resolver.resolve(query)
    assert [match.target_id for match in result.includes] == includes
    assert [match.target_id for match in result.excludes] == excludes


@pytest.mark.parametrize(
    "query,target",
    [
        (
            "contratación directa, en la que se reciban ofertas",
            "contratacion_directa_con_ofertas",
        ),
        (
            "régimen especial, en el que se reciban ofertas",
            "regimen_especial_con_ofertas",
        ),
        (
            "concurso de méritos, que tenga precalificación",
            "concurso_meritos_precalificacion",
        ),
        (
            "selección abreviada, mediante una subasta inversa",
            "seleccion_abreviada_subasta_inversa",
        ),
    ],
)
def test_comma_penalizes_but_does_not_block_local_composition(
    query: str, target: str, resolver: ModalityResolver,
) -> None:
    result = resolver.resolve(query)
    assert [match.target_id for match in result.includes] == [target]
    assert result.includes[0].source_text == query
    assert result.consumed_spans == ((0, len(query)),)


def test_refinement_never_crosses_an_intervening_primary(
    resolver: ModalityResolver,
) -> None:
    query = "selección abreviada concurso de méritos subasta inversa"
    result = resolver.resolve(query)
    assert [match.target_id for match in result.includes] == [
        "seleccion_abreviada",
        "concurso_de_meritos",
        "seleccion_abreviada_subasta_inversa",
    ]
    assert [match.source_text for match in result.includes] == [
        "selección abreviada",
        "concurso de méritos",
        "subasta inversa",
    ]

# ── Issues canónicos y estados ──────────────────────────────────────────────


def test_issues_are_the_only_stored_ambiguity_source(resolver: ModalityResolver) -> None:
    assert "ambiguities" not in ModalityResolution.__dataclass_fields__
    result = resolver.resolve("subasta")
    assert result.issues[0].issue_type is IssueType.AMBIGUITY
    assert result.ambiguities[0].ambiguity_code == result.issues[0].code
    with pytest.raises(Exception):
        result.issues += ()  # frozen dataclass


@pytest.mark.parametrize(
    "query,dataset,status",
    [
        ("licitación pública o subasta", None, "partial"),
        ("mínima cuantía y subasta de prueba", None, "partial"),
        ("RFI o contratación directa", "contratos", "partial"),
        ("subasta de prueba y RFI", "contratos", "not_searchable"),
    ],
)
def test_issue_status_matrix(
    query: str, dataset: str | None, status: str, resolver: ModalityResolver,
) -> None:
    assert resolver.resolve(query, dataset).status.value == status


def test_multiple_clarification_codes_yield_null_code(resolver: ModalityResolver) -> None:
    result = resolver.resolve("subasta o subasta inversa con precalificación")
    assert result.status is ResolutionStatus.CONFLICT
    assert result.needs_clarification is True
    assert result.clarification_code is None
    assert {issue.issue_type for issue in result.issues} == {
        IssueType.AMBIGUITY, IssueType.CONFLICT,
    }


# ── Dataset fail-closed ─────────────────────────────────────────────────────

@pytest.mark.parametrize("dataset", ["", "Procesos", "secop", 1, False])
def test_invalid_dataset_raises(dataset, resolver: ModalityResolver) -> None:
    with pytest.raises(ModalityResolutionError, match="dataset"):
        resolver.resolve("licitación pública", dataset)


def test_invalid_dataset_rejected_before_empty_text(resolver: ModalityResolver) -> None:
    with pytest.raises(ModalityResolutionError):
        resolver.resolve("", "otro")


# ── Precedencia total y orden determinístico ────────────────────────────────


def test_specific_procedure_beats_longer_colloquial_overlap() -> None:
    catalog = load_catalog()
    data = {
        "version": 1,
        "rules": [
            {
                "rule_id": "long_colloquial",
                "phrases": ["licitación pública especial"],
                "target_kind": "retrieval_family",
                "target_id": "licitacion_publica",
                "match_strength": "colloquial",
                "confidence": "medium",
                "explicit_intent": False,
                "assumption_code": "ADVERSARIAL_LONG_ALIAS",
                "notes": "",
            },
            {
                "rule_id": "short_specific",
                "phrases": ["pública especial"],
                "target_kind": "procedure",
                "target_id": "licitacion_publica_obra",
                "match_strength": "exact_legal",
                "confidence": "high",
                "explicit_intent": False,
                "assumption_code": None,
                "notes": "",
            },
        ],
    }
    resolver = ModalityResolver(catalog, build_lexicon(data, catalog))
    result = resolver.resolve("licitación pública especial")
    assert [match.target_id for match in result.includes] == ["licitacion_publica_obra"]


def test_lexicon_order_independence() -> None:
    catalog = load_catalog()
    with Path("app/data/modality_lexicon_v1.yaml").open("r", encoding="utf-8") as handle:
        data = strict_yaml_load(handle)
    shuffled = copy.deepcopy(data)
    random.Random(20260722).shuffle(shuffled["rules"])
    default = ModalityResolver(catalog, load_lexicon(catalog=catalog))
    reordered = ModalityResolver(catalog, build_lexicon(shuffled, catalog))
    for case in CORPUS:
        assert default.resolve(case["query"], case["dataset"]) == reordered.resolve(
            case["query"], case["dataset"]
        ), case["case_id"]


# ── Propiedades de normalización, spans y determinismo ──────────────────────

_INVARIANCE_QUERIES = [
    "licitación pública",
    "selección abreviada",
    "mínima cuantía",
    "concurso de méritos con precalificación",
    "subasta inversa",
    "contratación directa con ofertas",
    "enajenación de bienes",
]


@pytest.mark.parametrize("query", _INVARIANCE_QUERIES)
def test_accent_case_and_punctuation_invariance(
    query: str, resolver: ModalityResolver,
) -> None:
    stripped = "".join(
        char for char in unicodedata.normalize("NFD", query)
        if not unicodedata.combining(char)
    )
    variants = [query, stripped, query.upper(), query.title(), "  ¿¿" + query.replace(" ", "   ") + "??"]
    fingerprints = {_semantic_fingerprint(resolver.resolve(variant)) for variant in variants}
    assert len(fingerprints) == 1


@pytest.mark.parametrize("case", CORPUS, ids=CASE_IDS)
def test_deterministic_repeatability(case: dict, resolver: ModalityResolver) -> None:
    assert resolver.resolve(case["query"], case["dataset"]) == resolver.resolve(
        case["query"], case["dataset"]
    )


@pytest.mark.parametrize("case", CORPUS, ids=CASE_IDS)
def test_span_roundtrip_for_matches_and_issues(case: dict, resolver: ModalityResolver) -> None:
    query = case["query"]
    result = resolver.resolve(query, case["dataset"])
    for item in [*result.includes, *result.excludes, *result.issues]:
        assert query[item.original_start:item.original_end] == item.source_text


@pytest.mark.parametrize("case", CORPUS, ids=CASE_IDS)
def test_residual_preserves_all_unconsumed_alphanumerics(
    case: dict, resolver: ModalityResolver,
) -> None:
    query = case["query"]
    result = resolver.resolve(query, case["dataset"])
    covered = [False] * len(query)
    for start, end in result.consumed_spans:
        for index in range(start, end):
            covered[index] = True
    expected = [char for index, char in enumerate(query) if not covered[index] and char.isalnum()]
    got = [char for char in result.residual_text if char.isalnum()]
    assert got == expected


@pytest.mark.parametrize("case", CORPUS, ids=CASE_IDS)
def test_all_output_procedure_ids_exist(case: dict, resolver: ModalityResolver) -> None:
    catalog = load_catalog()
    result = resolver.resolve(case["query"], case["dataset"])
    for match in [*result.includes, *result.excludes]:
        for procedure_id in match.procedure_ids:
            assert catalog.by_id(procedure_id) is not None
    for issue in result.issues:
        for target_id in issue.target_ids:
            assert catalog.by_id(target_id) is not None or target_id in {
                "licitacion_publica", "seleccion_abreviada", "concurso_de_meritos",
                "contratacion_directa", "regimen_especial",
            }




@pytest.mark.parametrize("case", CORPUS, ids=CASE_IDS)
def test_final_output_spans_never_overlap(
    case: dict, resolver: ModalityResolver,
) -> None:
    result = resolver.resolve(case["query"], case["dataset"])
    spans = sorted(
        (item.original_start, item.original_end)
        for item in [*result.includes, *result.excludes, *result.issues]
    )
    assert all(current[0] >= previous[1] for previous, current in zip(spans, spans[1:])), (
        case["case_id"], spans,
    )


def test_modular_import_graph_has_no_reverse_edges() -> None:
    root = Path(__file__).parent.parent / "app" / "core"
    sources = {
        name: (root / name).read_text(encoding="utf-8")
        for name in (
            "modality_text.py",
            "modality_composition.py",
            "modality_scope.py",
            "modality_lexicon.py",
        )
    }
    assert "modality_resolver" not in sources["modality_text.py"]
    assert "modality_resolver" not in sources["modality_composition.py"]
    assert "modality_resolver" not in sources["modality_scope.py"]
    assert "modality_resolver" not in sources["modality_lexicon.py"]
    assert "modality_scope" not in sources["modality_composition.py"]
    assert "modality_composition" not in sources["modality_text.py"]


def test_module_facade_matches_instance(resolver: ModalityResolver) -> None:
    assert resolve("licitación pública") == resolver.resolve("licitación pública")


def _semantic_fingerprint(result) -> tuple:
    return (
        result.status.value,
        tuple(
            (
                match.target_id, match.procedure_ids, match.polarity.value,
                match.strength.value, match.confidence.value,
                match.legal_selection_family, match.procedure_variant,
                match.submission_mode, match.record_kind, match.expansion_policy,
            )
            for match in result.includes
        ),
        tuple((match.target_id, match.polarity.value) for match in result.excludes),
        tuple(
            (issue.issue_type.value, issue.code, issue.target_ids)
            for issue in result.issues
        ),
        result.assumption_codes,
        result.diagnostics,
    )
