from __future__ import annotations

import ast
from pathlib import Path

import pytest

from app.core.modality_precedence import prepare_modality_text


@pytest.mark.parametrize(
    "text",
    [
        "licitacion abierta",
        "licitaciones abiertas",
        "licitacion abierto",
        "licitaciones abiertos",
    ],
)
def test_generic_open_tender_is_hidden_only_from_modality_view(text):
    result = prepare_modality_text(text)

    assert result.modality_text == " " * len(text)
    assert result.hidden_spans == ((0, len(text)),)
    assert result.rule_ids == ("generic_open_tender",)


def test_generic_open_tender_preserves_topic_and_offsets():
    text = "licitaciones abiertas de alimentacion"
    result = prepare_modality_text(text)

    assert len(result.modality_text) == len(text)
    assert result.modality_text.endswith(" de alimentacion")
    assert result.hidden_spans == ((0, 21),)
    assert text[result.hidden_spans[0][1]:] == " de alimentacion"


@pytest.mark.parametrize(
    ("text", "state"),
    [
        ("licitacion publica abierta", "abierta"),
        ("licitacion publica abierto", "abierto"),
        ("licitaciones publicas abiertas", "abiertas"),
        ("licitaciones publicas abiertos", "abiertos"),
    ],
)
def test_public_tender_hides_only_open_state(text, state):
    result = prepare_modality_text(text)
    start = text.index(state)
    end = start + len(state)

    assert result.modality_text[:start] == text[:start]
    assert result.modality_text[start:end] == " " * len(state)
    assert result.hidden_spans == ((start, end),)
    assert result.rule_ids == ("public_tender_open_state",)


@pytest.mark.parametrize(
    "text",
    [
        "concurso de meritos abierto",
        "procesos abiertos de alimentacion",
        "contratacion publica abierta",
    ],
)
def test_unrelated_open_phrases_are_not_hidden(text):
    result = prepare_modality_text(text)

    assert result.modality_text == text
    assert result.hidden_spans == ()
    assert result.rule_ids == ()


def test_multiple_rules_are_deterministic_and_non_overlapping():
    text = (
        "licitaciones abiertas o licitacion publica abierta"
    )
    result = prepare_modality_text(text)

    assert result.rule_ids == (
        "generic_open_tender",
        "public_tender_open_state",
    )
    assert len(result.modality_text) == len(text)
    for start, end in result.hidden_spans:
        assert result.modality_text[start:end] == " " * (end - start)


def test_invalid_text_fails_loudly():
    with pytest.raises(ValueError, match="text debe ser str"):
        prepare_modality_text(123)  # type: ignore[arg-type]


def test_module_has_no_router_resolver_dataset_or_http_dependency():
    path = (
        Path(__file__).resolve().parents[1]
        / "app/core/modality_precedence.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    forbidden = {
        "app.core.query_router",
        "app.core.modality_resolver",
        "app.core.modality_models",
        "app.core.soql_builder",
    }
    assert not (imported & forbidden)
    assert not any("http" in name or "socrata" in name for name in imported)
