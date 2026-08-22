"""Contractual type/nature resolver stays separate from modality authority."""
from __future__ import annotations

import pytest

from app.core.contract_type_resolver import (
    COMPOSITE,
    STRUCTURED_EXACT,
    STRUCTURED_FAMILY,
    resolve_contract_types,
)
from app.core.semantic_dimensions import SemanticDimension


@pytest.mark.parametrize(
    ("text", "canonical_id", "classification"),
    [
        ("convenios de adulto mayor", "convenio", COMPOSITE),
        ("convenio interadministrativo", "convenio_interadministrativo", COMPOSITE),
        ("convenio de asociacion", "convenio_asociacion", COMPOSITE),
        ("convenio solidario", "convenio_solidario", STRUCTURED_FAMILY),
        ("contratos interadministrativos", "contrato_interadministrativo", COMPOSITE),
        ("contratos de prestacion de servicios", "prestacion_servicios", STRUCTURED_EXACT),
        ("contratos de obra", "obra", STRUCTURED_EXACT),
        ("contratos de suministro", "suministro", STRUCTURED_EXACT),
        ("contratos de arrendamiento", "arrendamiento", STRUCTURED_FAMILY),
        ("contratos de arrendamiento de inmuebles", "arrendamiento_inmuebles", STRUCTURED_EXACT),
        ("contratos de compraventa", "compraventa", STRUCTURED_EXACT),
    ],
)
def test_resolver_classification_and_consumption(
    text: str, canonical_id: str, classification: str,
) -> None:
    result = resolve_contract_types(text)

    assert result.include_ids == (canonical_id,)
    assert result.classifications == (classification,)
    assert result.consumed_spans
    assert result.dimension is SemanticDimension.CONTRACT_TYPE_OR_NATURE
    assert "regimen_especial" not in result.include_ids


def test_specific_convenio_span_wins_over_broad_family() -> None:
    result = resolve_contract_types("convenio interadministrativo de adulto mayor")

    assert result.include_ids == ("convenio_interadministrativo",)
    assert result.consumed_spans == ("convenio interadministrativo",)
    assert result.residual_text == "de adulto mayor"


def test_bare_thematic_obra_is_not_consumed_without_contract_type_syntax() -> None:
    result = resolve_contract_types("mantenimiento de obra publica")

    assert result.include_ids == ()
    assert result.residual_text == "mantenimiento de obra publica"
