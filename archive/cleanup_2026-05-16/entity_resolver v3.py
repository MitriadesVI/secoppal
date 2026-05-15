from __future__ import annotations

import json
import logging
import re
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


@dataclass(slots=True)
class ScanResult:
    """Result of scanning text for a known entity/department."""
    official_name: str  # SECOP official name
    matched_alias: str  # the alias that was found in text
    remaining_text: str  # text with the matched alias removed
    method: str  # "scan_exact"


class EntityResolver:
    """
    Resolves natural language references to SECOP-compatible values.

    Two modes:
    1. scan_text(): finds known entities BY SUBSTRING in user text (gazetteer-first)
    2. resolve_entidad(): resolves an already-extracted entity string (fuzzy fallback)
    """

    FUZZY_CUTOFF_DEPARTAMENTO = 82
    FUZZY_CUTOFF_ENTIDAD = 82

    def __init__(self, alias_path: str | Path):
        self.alias_path = Path(alias_path)

        # For resolve (fuzzy matching)
        self.alias_index: dict[str, str] = {}          # normalized_alias → official_name
        self.choice_to_official: dict[str, str] = {}    # all choices for fuzzy
        self.entities_by_department: dict[str, list[str]] = {}

        # For scan (substring detection in text)
        # Index: first_word → [(normalized_alias, official_name), ...] sorted by alias length DESC
        self._scan_index: dict[str, list[tuple[str, str]]] = {}

        # Department scan index: same structure
        self._dept_scan_index: dict[str, list[tuple[str, str]]] = {}

        self._load_aliases()
        self._build_dept_scan_index()

    # Words that qualify a department name as an entity reference
    # "gobernación DE santander" → entity, "en santander" → department
    _ENTITY_QUALIFIERS = {
        "gobernacion", "gobernación", "alcaldia", "alcaldía",
        "departamento", "municipio", "distrito",
    }

    # =====================================================================
    # SCAN: Find entities/departments as substrings in text
    # =====================================================================

    def scan_entity(self, normalized_text: str) -> ScanResult | None:
        """
        Scan text for known entity aliases. Returns the LONGEST match.

        Post-validation: if the match is ALSO a known department name
        (e.g., "antioquia", "atlantico"), reject it UNLESS the text has
        an entity qualifier before it ("gobernacion de", "alcaldia de").
        This prevents department names from being stolen by the entity scanner.
        """
        result = self._scan_against_index(
            normalized_text, self._scan_index, reject_fn=self._is_unqualified_dept
        )
        return result

    def _is_unqualified_dept(self, alias: str, full_text: str) -> bool:
        """
        Returns True if this alias should be REJECTED from entity scan.
        Rejects department names that don't have entity qualifiers.

        Logic:
        - "antioquia" → bare dept name, no qualifier → REJECT
        - "gobernacion de santander" → alias itself contains qualifier → KEEP
        - "alcaldia de barranquilla" → alias contains qualifier → KEEP  
        - "en atlantico" → "en" is not qualifier → REJECT
        """
        alias_stripped = alias.strip()

        # Check if alias is/contains a department name
        is_dept = False
        for dept_key in DEPARTAMENTOS:
            if dept_key == alias_stripped or alias_stripped.endswith(dept_key):
                is_dept = True
                break

        if not is_dept:
            return False  # Not a dept name at all → don't reject

        # Check if the alias ITSELF starts with an entity qualifier
        first_word = alias_stripped.split()[0] if alias_stripped else ""
        if first_word in self._ENTITY_QUALIFIERS:
            return False  # "gobernacion de santander" → keep as entity

        # Check if there's a qualifier BEFORE the alias in the text
        match_pos = full_text.find(alias_stripped)
        if match_pos > 0:
            prefix = full_text[:match_pos].rstrip()
            words = prefix.split()
            if words:
                last = words[-1]
                if last in self._ENTITY_QUALIFIERS:
                    return False  # "gobernacion de [santander]" → keep
                if last == "de" and len(words) >= 2 and words[-2] in self._ENTITY_QUALIFIERS:
                    return False  # "alcaldia de [medellin]" → keep

        # Bare department name without qualifier → reject
        return True

    def scan_departamento(self, normalized_text: str) -> ScanResult | None:
        """
        Scan text for known department aliases. Returns the LONGEST match.
        Should be called AFTER scan_entity on the remaining text.
        """
        return self._scan_against_index(normalized_text, self._dept_scan_index)

    def _scan_against_index(
        self,
        text: str,
        index: dict[str, list[tuple[str, str]]],
        reject_fn: callable | None = None,
    ) -> ScanResult | None:
        """
        Generic scan: find longest known alias as substring in text.
        Uses first-word index for speed (~20 comparisons instead of 10K).

        If reject_fn is provided, it's called with (alias, full_text) and
        if it returns True, that match is skipped and scanning continues
        to find the next-best match.
        """
        # Collect ALL matches, then pick the longest non-rejected one
        all_matches: list[tuple[str, str]] = []  # (alias, official)

        words_in_text = set(text.split())

        for word in words_in_text:
            candidates = index.get(word, [])
            for alias, official in candidates:
                if re.search(rf'\b{re.escape(alias)}\b', text):
                    all_matches.append((alias, official))

        # Sort by alias length descending — longest first
        all_matches.sort(key=lambda x: len(x[0]), reverse=True)

        # Pick the first non-rejected match
        for alias, official in all_matches:
            if reject_fn and reject_fn(alias, text):
                continue

            remaining = re.sub(rf'\b{re.escape(alias)}\b', ' ', text, count=1)
            remaining = re.sub(r'\s+', ' ', remaining).strip()

            return ScanResult(
                official_name=official,
                matched_alias=alias,
                remaining_text=remaining,
                method="scan_exact",
            )

        return None

    # =====================================================================
    # RESOLVE: Fuzzy match an already-extracted string
    # =====================================================================

    def resolve_departamento(self, user_input: str) -> ResolutionResult:
        normalized = normalize_text(user_input)

        if normalized in DEPARTAMENTOS:
            return ResolutionResult(value=DEPARTAMENTOS[normalized], method="exact", confidence="high")

        match = process.extractOne(
            normalized, list(DEPARTAMENTOS.keys()),
            scorer=fuzz.WRatio, score_cutoff=self.FUZZY_CUTOFF_DEPARTAMENTO,
        )
        if match:
            return ResolutionResult(
                value=DEPARTAMENTOS[match[0]], method=f"fuzzy({int(match[1])})", confidence="medium",
            )

        match = process.extractOne(
            normalized, DEPARTAMENTOS_OFICIALES,
            scorer=fuzz.WRatio, score_cutoff=self.FUZZY_CUTOFF_DEPARTAMENTO,
        )
        if match:
            return ResolutionResult(
                value=match[0], method=f"fuzzy_official({int(match[1])})", confidence="medium",
            )

        logger.warning("Could not resolve departamento: '%s'", user_input)
        return ResolutionResult(value=None, method="unresolved", confidence="none")

    def resolve_entidad(self, user_input: str, departamento: str | None = None) -> ResolutionResult:
        normalized = normalize_text(user_input)

        # Tier 1: Exact alias match
        if normalized in self.alias_index:
            return ResolutionResult(value=self.alias_index[normalized], method="exact", confidence="high")

        # Tier 2: Department-scoped fuzzy
        if departamento and departamento in self.entities_by_department:
            dept_entities = self.entities_by_department[departamento]
            dept_choices = {normalize_text(e): e for e in dept_entities}
            match = process.extractOne(
                normalized, list(dept_choices.keys()),
                scorer=fuzz.WRatio, score_cutoff=self.FUZZY_CUTOFF_ENTIDAD,
            )
            if match:
                return ResolutionResult(
                    value=dept_choices[match[0]], method=f"fuzzy_dept({int(match[1])})", confidence="medium",
                    metadata={"departamento_filter": departamento},
                )

        # Tier 3: Global fuzzy
        match = process.extractOne(
            normalized, list(self.choice_to_official.keys()),
            scorer=fuzz.WRatio, score_cutoff=self.FUZZY_CUTOFF_ENTIDAD,
        )
        if match:
            return ResolutionResult(
                value=self.choice_to_official[match[0]], method=f"fuzzy({int(match[1])})", confidence="medium",
            )

        # Tier 4: LIKE fallback
        logger.info("Entity '%s' fell through to LIKE fallback", user_input)
        return ResolutionResult(value=None, like_value=normalized.upper(), method="like", confidence="low")

    # =====================================================================
    # INDEX BUILDING
    # =====================================================================

    def _load_aliases(self) -> None:
        if not self.alias_path.exists():
            logger.warning(
                "Alias database not found at %s — entity resolution will use LIKE fallback. "
                "Run: python scripts/build_gazetteer.py full --top 500",
                self.alias_path,
            )
            return

        with self.alias_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)

        scan_entries: list[tuple[str, str]] = []  # (normalized_alias, official_name)

        for record in data:
            official = record["official_name"]
            official_key = normalize_text(official)
            self.choice_to_official[official_key] = official

            dept = record.get("departamento")
            if dept:
                self.entities_by_department.setdefault(dept, []).append(official)

            # Collect all aliases for scan index
            all_aliases = [official_key]
            for alias in record.get("aliases", []):
                alias_key = normalize_text(alias)
                self.alias_index[alias_key] = official
                self.choice_to_official[alias_key] = official
                all_aliases.append(alias_key)

            # Add unique aliases to scan entries
            seen = set()
            for a in all_aliases:
                if a and a not in seen and len(a) >= 3:
                    scan_entries.append((a, official))
                    seen.add(a)

        # Build first-word index, sorted by alias length DESC within each group
        self._scan_index = self._build_scan_index(scan_entries)

        logger.info(
            "Loaded %d entities, %d scan aliases from %s",
            len(data), len(scan_entries), self.alias_path,
        )

    def _build_dept_scan_index(self) -> None:
        """Build scan index for departments from DEPARTAMENTOS dict."""
        entries: list[tuple[str, str]] = []
        for alias, official in DEPARTAMENTOS.items():
            if len(alias) >= 3:
                entries.append((alias, official))
        self._dept_scan_index = self._build_scan_index(entries)

    @staticmethod
    def _build_scan_index(entries: list[tuple[str, str]]) -> dict[str, list[tuple[str, str]]]:
        """
        Build a first-word index from (alias, official) pairs.
        Within each group, aliases are sorted by length DESC so longest matches first.
        """
        index: dict[str, list[tuple[str, str]]] = {}
        for alias, official in entries:
            first_word = alias.split()[0] if alias else ""
            if not first_word:
                continue
            index.setdefault(first_word, []).append((alias, official))

        # Sort each group by alias length descending
        for key in index:
            index[key].sort(key=lambda x: len(x[0]), reverse=True)

        return index
