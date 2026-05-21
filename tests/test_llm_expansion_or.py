from app.core.llm_handler import is_synonym_expansion
from app.core.soql_builder import SoQLBuilder


def test_detects_two_phrase_synonym_expansion() -> None:
    assert is_synonym_expansion(["residuos sólidos", "manejo de residuos"])


def test_simple_terms_are_not_synonym_expansion() -> None:
    assert not is_synonym_expansion(["aseo", "jardineria"])


def test_objeto_or_builds_single_or_group() -> None:
    soql = SoQLBuilder().build(
        SoQLBuilder.PROCESOS_DATASET,
        {"objeto_or": ["residuos sólidos", "manejo de residuos"]},
    )

    assert "residuos sólidos" in soql
    assert "manejo de residuos" in soql
    assert " OR " in soql
    assert " AND " not in soql.split(" WHERE ", 1)[1].split(" ORDER BY ", 1)[0]
