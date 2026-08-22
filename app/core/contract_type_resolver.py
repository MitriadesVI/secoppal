"""Small deterministic resolver for contractual type/nature language.

The canonical IDs are independent from procedure/modality IDs.  In particular,
``convenio`` is never promoted to ``regimen_especial``.  Source projection lives
in ``contract_type_projection`` because the two SECOP datasets use different
justification field names and some natural classes need composite evidence.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from app.core.semantic_dimensions import SemanticDimension


STRUCTURED_EXACT = "STRUCTURED_EXACT"
STRUCTURED_FAMILY = "STRUCTURED_FAMILY"
COMPOSITE = "COMPOSITE"

CONTRACT_TYPE_CLASSIFICATION: dict[str, str] = {
    "convenio": COMPOSITE,
    "convenio_interadministrativo": COMPOSITE,
    "convenio_asociacion": COMPOSITE,
    "convenio_solidario": STRUCTURED_FAMILY,
    "contrato_interadministrativo": COMPOSITE,
    "prestacion_servicios": STRUCTURED_EXACT,
    "obra": STRUCTURED_EXACT,
    "suministro": STRUCTURED_EXACT,
    "arrendamiento": STRUCTURED_FAMILY,
    "arrendamiento_inmuebles": STRUCTURED_EXACT,
    "arrendamiento_muebles": STRUCTURED_EXACT,
    "compraventa": STRUCTURED_EXACT,
}


@dataclass(frozen=True, slots=True)
class ContractTypeInterpretation:
    include_ids: tuple[str, ...] = ()
    consumed_spans: tuple[str, ...] = ()
    residual_text: str = ""
    dimension: SemanticDimension = SemanticDimension.CONTRACT_TYPE_OR_NATURE

    @property
    def classifications(self) -> tuple[str, ...]:
        return tuple(CONTRACT_TYPE_CLASSIFICATION[item] for item in self.include_ids)


@dataclass(frozen=True, slots=True)
class _Pattern:
    canonical_id: str
    regex: re.Pattern[str]


def _compile(expression: str) -> re.Pattern[str]:
    return re.compile(expression, re.IGNORECASE)


# Longest/specific constructions own their text before broad ``convenio``.
_PATTERNS: tuple[_Pattern, ...] = (
    _Pattern(
        "convenio_interadministrativo",
        _compile(r"\bconvenios?\s+interadministrativos?\b"),
    ),
    _Pattern(
        "convenio_asociacion",
        _compile(r"\bconvenios?\s+(?:de\s+)?asociaci[oó]n\b"),
    ),
    _Pattern(
        "convenio_solidario",
        _compile(r"\bconvenios?\s+solidarios?\b"),
    ),
    _Pattern(
        "contrato_interadministrativo",
        _compile(r"\bcontratos?\s+interadministrativos?\b"),
    ),
    _Pattern(
        "prestacion_servicios",
        _compile(
            r"\b(?:contratos?\s+(?:de\s+)?)?"
            r"prestaci[oó]n\s+de\s+servicios\b"
        ),
    ),
    _Pattern(
        "arrendamiento_inmuebles",
        _compile(r"\bcontratos?\s+de\s+arrendamiento\s+de\s+inmuebles\b"),
    ),
    _Pattern(
        "arrendamiento_muebles",
        _compile(r"\bcontratos?\s+de\s+arrendamiento\s+de\s+muebles\b"),
    ),
    _Pattern(
        "arrendamiento",
        _compile(r"\bcontratos?\s+de\s+arrendamientos?\b"),
    ),
    _Pattern(
        "compraventa",
        _compile(r"\bcontratos?\s+de\s+compraventa\b"),
    ),
    _Pattern(
        "suministro",
        _compile(r"\bcontratos?\s+de\s+suministros?\b"),
    ),
    _Pattern(
        "obra",
        _compile(r"\bcontratos?\s+de\s+obra\b"),
    ),
    _Pattern("convenio", _compile(r"\bconvenios?\b")),
)


def resolve_contract_types(text: str) -> ContractTypeInterpretation:
    """Resolve non-overlapping contractual-class spans and scrub them.

    The caller supplies normalized text.  Matching remains accent-tolerant for
    direct unit use and preserves the exact consumed substring for diagnostics.
    """
    residual = str(text or "")
    include: list[str] = []
    consumed: list[str] = []
    for spec in _PATTERNS:
        while match := spec.regex.search(residual):
            include.append(spec.canonical_id)
            consumed.append(match.group(0))
            residual = f"{residual[:match.start()]} {residual[match.end():]}"
    residual = re.sub(r"\s+", " ", residual).strip()
    return ContractTypeInterpretation(
        include_ids=tuple(dict.fromkeys(include)),
        consumed_spans=tuple(consumed),
        residual_text=residual,
    )
