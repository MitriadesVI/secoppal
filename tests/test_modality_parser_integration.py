from __future__ import annotations

import ast
from pathlib import Path

import pytest

from app.core import modality_parser_integration as integration
from app.core.modality_resolution_models import ResolutionStatus


def test_resolver_is_invoked_once_with_simple_dataset(monkeypatch):
    calls: list[tuple[str, str | None]] = []
    real_resolve = integration.resolve

    def spy(text: str, dataset: str | None = None):
        calls.append((text, dataset))
        return real_resolve(text, dataset)

    monkeypatch.setattr(integration, "resolve", spy)
    result = integration.resolve_parser_modality(
        "contratacion directa de alimentacion",
        dataset="procesos",
    )

    assert calls == [
        ("contratacion directa de alimentacion", "procesos")
    ]
    assert result.executable_params == {
        "modalidad_include_procedure_ids": [
            "contratacion_directa",
            "contratacion_directa_con_ofertas",
        ]
    }


def test_dual_scope_resolves_once_without_dataset_filter(monkeypatch):
    calls: list[str | None] = []
    real_resolve = integration.resolve

    def spy(text: str, dataset: str | None = None):
        calls.append(dataset)
        return real_resolve(text, dataset)

    monkeypatch.setattr(integration, "resolve", spy)
    result = integration.resolve_parser_modality(
        "solicitud de informacion a proveedores",
        dataset="procesos",
        dataset_scope=["procesos", "contratos"],
    )

    assert calls == [None]
    assert result.interpretation.resolution_status is ResolutionStatus.RESOLVED
    assert result.executable_params == {
        "modalidad_include_procedure_ids": [
            "solicitud_informacion_proveedores"
        ]
    }


def test_masking_uses_offsets_on_the_exact_input_text():
    text = "subastas inversas de dotacion"
    result = integration.resolve_parser_modality(
        text, dataset="procesos"
    )

    assert len(result.masked_text) == len(text)
    assert result.masked_text.endswith(" de dotacion")
    for start, end in result.interpretation.consumed_spans:
        assert result.masked_text[start:end] == " " * (end - start)
        assert text[start:end]


def test_generic_open_tender_is_hidden_only_from_m2(monkeypatch):
    calls: list[str] = []
    real_resolve = integration.resolve
    text = "licitaciones abiertas de alimentacion"

    def spy(value: str, dataset: str | None = None):
        calls.append(value)
        return real_resolve(value, dataset)

    monkeypatch.setattr(integration, "resolve", spy)
    result = integration.resolve_parser_modality(
        text,
        dataset="procesos",
    )

    assert calls == [" " * 21 + " de alimentacion"]
    assert result.interpretation.resolution_status is ResolutionStatus.NO_MATCH
    assert result.masked_text == text
    assert result.precedence_rule_ids == ("generic_open_tender",)
    assert result.precedence_hidden_spans == ((0, 21),)


def test_public_tender_keeps_open_state_in_main_text(monkeypatch):
    calls: list[str] = []
    real_resolve = integration.resolve
    text = "licitacion publica abierta de alimentacion"

    def spy(value: str, dataset: str | None = None):
        calls.append(value)
        return real_resolve(value, dataset)

    monkeypatch.setattr(integration, "resolve", spy)
    result = integration.resolve_parser_modality(
        text,
        dataset="procesos",
    )

    assert calls == ["licitacion publica         de alimentacion"]
    assert result.interpretation.resolution_status is ResolutionStatus.RESOLVED
    assert result.executable_params[
        "modalidad_include_procedure_ids"
    ] == ["licitacion_publica", "licitacion_publica_obra"]
    assert result.masked_text == " " * 18 + " abierta de alimentacion"
    assert result.precedence_rule_ids == ("public_tender_open_state",)
    assert result.precedence_hidden_spans == ((19, 26),)
    assert len(result.masked_text) == len(text)


def test_non_executable_interpretation_projects_no_params():
    result = integration.resolve_parser_modality(
        "subasta de alimentacion", dataset="procesos"
    )

    assert result.interpretation.resolution_status is ResolutionStatus.AMBIGUOUS
    assert result.executable_params == {}
    assert "subasta" not in result.masked_text


def test_module_has_no_parser_builder_or_http_dependency():
    path = (
        Path(__file__).resolve().parents[1]
        / "app/core/modality_parser_integration.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert "app.core.query_router" not in imported
    assert "app.core.soql_builder" not in imported
    assert not any("http" in name or "socrata" in name for name in imported)


def test_invalid_dataset_is_not_silently_normalized():
    with pytest.raises(ValueError):
        integration.resolve_parser_modality(
            "licitacion publica", dataset="Procesos"
        )
