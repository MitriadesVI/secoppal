from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path

from rapidfuzz import fuzz, process

from app.data.departamentos import DEPARTAMENTOS, DEPARTAMENTOS_OFICIALES
from app.utils.money import normalize_text

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ResolutionResult:
    value: str | None
    method: str
    confidence: str
    like_value: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


class EntityResolver:
    """Resolves natural language references to SECOP-compatible values."""

    FUZZY_CUTOFF_DEPARTAMENTO = 82
    FUZZY_CUTOFF_ENTIDAD = 82  # Lowered from 88 — SECOP names are very different from colloquial

    def __init__(self, alias_path: str | Path):
        self.alias_path = Path(alias_path)
        self.alias_index: dict[str, str] = {}
        self.choice_to_official: dict[str, str] = {}
        # Index: departamento → list of official entity names in that department
        self.entities_by_department: dict[str, list[str]] = {}
        self._load_aliases()

    def resolve_departamento(self, user_input: str) -> ResolutionResult:
        normalized = normalize_text(user_input)

        # Tier 1: Exact alias match
        if normalized in DEPARTAMENTOS:
            return ResolutionResult(
                value=DEPARTAMENTOS[normalized],
                method="exact",
                confidence="high",
            )

        # Tier 2: Fuzzy against alias keys
        match = process.extractOne(
            normalized,
            list(DEPARTAMENTOS.keys()),
            scorer=fuzz.WRatio,
            score_cutoff=self.FUZZY_CUTOFF_DEPARTAMENTO,
        )
        if match:
            alias, score, _ = match
            return ResolutionResult(
                value=DEPARTAMENTOS[alias],
                method=f"fuzzy({int(score)})",
                confidence="medium",
            )

        # Tier 3: Fuzzy against official values (e.g., "CUNDINAMARCA")
        match = process.extractOne(
            normalized,
            DEPARTAMENTOS_OFICIALES,
            scorer=fuzz.WRatio,
            score_cutoff=self.FUZZY_CUTOFF_DEPARTAMENTO,
        )
        if match:
            official, score, _ = match
            return ResolutionResult(
                value=official,
                method=f"fuzzy_official({int(score)})",
                confidence="medium",
            )

        logger.warning("Could not resolve departamento: '%s'", user_input)
        return ResolutionResult(value=None, method="unresolved", confidence="none")

    def resolve_entidad(self, user_input: str, departamento: str | None = None) -> ResolutionResult:
        normalized = normalize_text(user_input)

        # Tier 1: Exact alias match
        if normalized in self.alias_index:
            return ResolutionResult(
                value=self.alias_index[normalized],
                method="exact",
                confidence="high",
            )

        # Tier 2: Fuzzy matching
        # If departamento is known, try department-scoped entities first for better precision
        if departamento and departamento in self.entities_by_department:
            dept_entities = self.entities_by_department[departamento]
            dept_choices = {normalize_text(e): e for e in dept_entities}
            match = process.extractOne(
                normalized,
                list(dept_choices.keys()),
                scorer=fuzz.WRatio,
                score_cutoff=self.FUZZY_CUTOFF_ENTIDAD,
            )
            if match:
                alias, score, _ = match
                return ResolutionResult(
                    value=dept_choices[alias],
                    method=f"fuzzy_dept({int(score)})",
                    confidence="medium",
                    metadata={"departamento_filter": departamento},
                )

        # Fuzzy against all aliases
        choices = list(self.choice_to_official.keys())
        match = process.extractOne(
            normalized,
            choices,
            scorer=fuzz.WRatio,
            score_cutoff=self.FUZZY_CUTOFF_ENTIDAD,
        )
        if match:
            alias, score, _ = match
            return ResolutionResult(
                value=self.choice_to_official[alias],
                method=f"fuzzy({int(score)})",
                confidence="medium",
            )

        # Tier 3: LIKE fallback
        logger.info("Entity '%s' fell through to LIKE fallback", user_input)
        return ResolutionResult(
            value=None,
            like_value=normalized.upper(),
            method="like",
            confidence="low",
        )

    def _load_aliases(self) -> None:
        if not self.alias_path.exists():
            logger.warning(
                "Alias database not found at %s — entity resolution will use LIKE fallback for all queries. "
                "Run scripts/generate_aliases.py to create it.",
                self.alias_path,
            )
            return

        with self.alias_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)

        loaded_count = 0
        for record in data:
            official = record["official_name"]
            official_key = normalize_text(official)
            self.choice_to_official[official_key] = official

            # Index by department if available
            dept = record.get("departamento")
            if dept:
                self.entities_by_department.setdefault(dept, []).append(official)

            for alias in record.get("aliases", []):
                alias_key = normalize_text(alias)
                self.alias_index[alias_key] = official
                self.choice_to_official[alias_key] = official

            loaded_count += 1

        logger.info("Loaded %d entities with aliases from %s", loaded_count, self.alias_path)
