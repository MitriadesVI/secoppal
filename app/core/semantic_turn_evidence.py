"""One raw-bound semantic receipt for a deterministic QueryRouter parse.

The parser constructs this value once. Burr transports its JSON-native state;
consumers reject an absent, partial, malformed, or foreign receipt as a unit.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Mapping

from app.core.analytic_control import AnalyticControlSpan
from app.core.material_residual import MaterialEvidence
from app.utils.money import normalize_text


_FINGERPRINT = re.compile(r"[0-9a-f]{64}\Z")
_FIELDS = frozenset({
    "raw_fingerprint", "analytic_controls", "material_residual", "material_evidence",
})


def raw_fingerprint(raw: str) -> str:
    """Bind to the exact raw string, including lone surrogate code points."""
    return hashlib.sha256(raw.encode("utf-8", errors="surrogatepass")).hexdigest()


@dataclass(frozen=True, slots=True)
class SemanticTurnEvidence:
    raw_fingerprint: str
    analytic_controls: tuple[AnalyticControlSpan, ...]
    material_residual: tuple[str, ...]
    material_evidence: MaterialEvidence

    @classmethod
    def from_parse(
        cls,
        raw: str,
        controls: tuple[AnalyticControlSpan, ...],
        residual: tuple[str, ...],
        material: MaterialEvidence,
    ) -> "SemanticTurnEvidence":
        """Called by QueryRouter only, after all three projections are final."""
        result = cls(raw_fingerprint(raw), controls, residual, material)
        if not result.valid_for(raw):
            raise ValueError("SEMANTIC_EVIDENCE_INVALID")
        return result

    def valid(self) -> bool:
        return (
            isinstance(self.raw_fingerprint, str)
            and _FINGERPRINT.fullmatch(self.raw_fingerprint) is not None
            and isinstance(self.analytic_controls, tuple)
            and all(isinstance(item, AnalyticControlSpan) for item in self.analytic_controls)
            and isinstance(self.material_residual, tuple)
            and all(isinstance(item, str) for item in self.material_residual)
            and isinstance(self.material_evidence, MaterialEvidence)
            and MaterialEvidence.from_state(self.material_evidence) is not None
            and (not self.material_evidence.occurrences or bool(self.material_residual))
        )

    def valid_for(self, raw: str) -> bool:
        if not self.valid() or self.raw_fingerprint != raw_fingerprint(raw):
            return False
        normalized = normalize_text(raw)
        return all(
            0 <= item.start < item.end <= len(normalized)
            and normalized[item.start:item.end] == item.evidence_text
            for item in self.analytic_controls
        )

    def to_state(self) -> dict[str, object]:
        return {
            "raw_fingerprint": self.raw_fingerprint,
            "analytic_controls": [item.to_state() for item in self.analytic_controls],
            "material_residual": list(self.material_residual),
            "material_evidence": self.material_evidence.to_state(),
        }

    @classmethod
    def from_state(cls, value: object) -> "SemanticTurnEvidence | None":
        if isinstance(value, cls):
            return value if value.valid() else None
        if not isinstance(value, Mapping) or frozenset(value) != _FIELDS:
            return None
        controls = value["analytic_controls"]
        residual = value["material_residual"]
        material = value["material_evidence"]
        if (
            not isinstance(controls, list)
            or not isinstance(residual, list)
            or not all(isinstance(item, str) for item in residual)
            or not isinstance(material, dict)
            or frozenset(material) != {"snapshot", "occurrences", "ownership"}
        ):
            return None
        try:
            if not all(isinstance(item, dict) for item in controls):
                return None
            parsed_controls = tuple(AnalyticControlSpan.from_state(item) for item in controls)
            parsed_material = MaterialEvidence.from_state(material)
            if parsed_material is None:
                return None
            result = cls(
                value["raw_fingerprint"], parsed_controls, tuple(residual), parsed_material,
            )
        except (KeyError, TypeError, ValueError):
            return None
        return result if result.valid() else None


__all__ = ["SemanticTurnEvidence", "raw_fingerprint"]
