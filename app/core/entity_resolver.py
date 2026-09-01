from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

from rapidfuzz import fuzz, process

from app.core.generic_buyer import (
    GENERIC_BUYER_TOKENS,
    incomplete_buyer_hint,
    is_generic_buyer_token,
)
from app.data.colombia_geography import REGION_ALIASES
from app.data.departamentos import DEPARTAMENTOS, DEPARTAMENTOS_OFICIALES
from app.utils.money import normalize_text

logger = logging.getLogger(__name__)


# ── Ciudad gazetteer — canonical names for Colombian municipalities ──────
# Built from _COMMON_CITIES already known to query_router.  Used by
# geo_mention_resolution() to detect city tokens in object text.
GAZETTEER_CIUDADES: dict[str, str] = {
    "barranquilla": "Barranquilla",
    "soledad": "Soledad",
    "cartagena": "Cartagena",
    "bogota": "Bogotá",
    "bogotá": "Bogotá",
    "medellin": "Medellín",
    "medellín": "Medellín",
    "cali": "Cali",
    "santa marta": "Santa Marta",
    "sincelejo": "Sincelejo",
    "monteria": "Montería",
    "montería": "Montería",
    "valledupar": "Valledupar",
    "bucaramanga": "Bucaramanga",
    "cucuta": "Cúcuta",
    "cúcuta": "Cúcuta",
    "ibague": "Ibagué",
    "ibagué": "Ibagué",
    "pereira": "Pereira",
    "manizales": "Manizales",
    "armenia": "Armenia",
    "neiva": "Neiva",
    "popayan": "Popayán",
    "popayán": "Popayán",
    "pasto": "Pasto",
    "tunja": "Tunja",
    "villavicencio": "Villavicencio",
    "yopal": "Yopal",
    "quibdo": "Quibdó",
    "quibdó": "Quibdó",
    "leticia": "Leticia",
    "riohacha": "Riohacha",
    "san andres": "San Andrés",
    "san andrés": "San Andrés",
    "mocoa": "Mocoa",
    "inirida": "Inírida",
    "inírida": "Inírida",
    "mitú": "Mitú",
    "mitu": "Mitú",
    "puerto carreño": "Puerto Carreño",
    "arauca": "Arauca",
    "chiriguana": "Chiriguaná",
    "chiriguaná": "Chiriguaná",
    # ── Additional cities from backlog / CORPUS_BACKLOG.md ──
    "ipiales": "Ipiales",
    "olaya": "Olaya",
    "yarumal": "Yarumal",
    "palermo": "Palermo",
    "doncello": "Doncello",
    "paicol": "Paicol",
}


@dataclass
class GeoMention:
    """Result of looking up a token in the geography gazetteer."""
    kind: str  # "city", "department", "entity", "unknown"
    canonical: str | None = None
    confidence: str = "low"  # "high", "medium", "low"


def geo_mention_resolution(token: str) -> GeoMention:
    """Pure lookup: gazetteer-only, no fuzzy beyond exact lowercase match.

    Checks GAZETTEER_CIUDADES first (cities), then DEPARTAMENTOS
    (departments). If neither matches, returns kind="unknown".
    """
    if not isinstance(token, str):
        return GeoMention(kind="unknown")
    key = normalize_text(token.strip())
    if not key:
        return GeoMention(kind="unknown")
    if key in GAZETTEER_CIUDADES:
        return GeoMention(kind="city", canonical=GAZETTEER_CIUDADES[key], confidence="high")
    if key in DEPARTAMENTOS:
        return GeoMention(kind="department", canonical=DEPARTAMENTOS[key], confidence="high")
    return GeoMention(kind="unknown")


def department_mention_resolution(token: str) -> GeoMention:
    """Resolve an explicitly typed department token by exact authority lookup.

    Unlike ``geo_mention_resolution()``, this lookup has no city precedence:
    the caller already obtained the geographic type from the user's grammar.
    """
    if not isinstance(token, str):
        return GeoMention(kind="unknown")
    key = normalize_text(token.strip())
    if not key or key not in DEPARTAMENTOS:
        return GeoMention(kind="unknown")
    return GeoMention(
        kind="department",
        canonical=DEPARTAMENTOS[key],
        confidence="high",
    )


@dataclass(slots=True)
class ResolutionResult:
    value: str | None
    method: str
    confidence: str
    like_value: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    ecosystem_entities: list[str] = field(default_factory=list)
    clarification_needed: bool = False
    clarification_hint: str | None = None

    @property
    def central_entity(self) -> str | None:
        return self.value

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

        # Region scan index: alias → región canónica
        self._region_scan_index: dict[str, list[tuple[str, str]]] = {}

        self._load_aliases()
        self._build_dept_scan_index()
        self._build_region_scan_index()

    # Words that qualify a department name as an entity reference
    # "gobernación DE santander" → entity, "en santander" → department
    _ENTITY_QUALIFIERS = {
        "gobernacion", "gobernación", "alcaldia", "alcaldía",
        "departamento", "municipio", "distrito",
    }

    # Bare generic entity-type tokens that should NEVER resolve to a
    # specific entity on their own.  If the alias is ONLY one of these
    # words (not a multi-word phrase like "alcaldía de manizales"), the
    # scan must skip it and force clarification.
    #
    # Fixes ENTITY-MISRESOLUTION-CATASTROPHIC: "alcaldía" alone was
    # resolving to MUNICIPIO DE MANIZALES because the gazetteer had it
    # as a standalone alias (data-side bug) and _is_unqualified_dept
    # only protected department names, not generic entity-type tokens
    # (code-side bug).
    # Autoridad compartida con QueryRouter: ver app/core/generic_buyer.py.
    _GENERIC_ENTITY_TOKENS: frozenset[str] = GENERIC_BUYER_TOKENS

    # =====================================================================
    # SCAN: Find entities/departments as substrings in text
    # =====================================================================

    def scan_entity(self, normalized_text: str) -> ScanResult | None:
        """
        Scan text for known entity aliases. Returns the LONGEST match.

        Post-validation: two reject functions run in series:
        1. _is_unqualified_dept — rejects bare dept names (e.g., "antioquia")
        2. _is_unqualified_generic — rejects bare generic tokens (e.g., "alcaldía")
        Both must pass for the match to be accepted.
        """
        def _combined_reject(alias: str, full_text: str) -> bool:
            return (
                self._is_unqualified_dept(alias, full_text)
                or self._is_unqualified_generic(alias, full_text)
            )

        result = self._scan_against_index(
            normalized_text, self._scan_index, reject_fn=_combined_reject
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

    def _is_unqualified_generic(self, alias: str, full_text: str) -> bool:
        """
        Returns True if the alias is a bare generic entity-type token
        that should NEVER resolve to a specific entity on its own.

        Examples:
        - "alcaldía" → bare generic → REJECT (was resolving to Manizales)
        - "la alcaldía" → article + generic → REJECT
        - "gobernación" → bare generic → REJECT
        - "el municipio" → article + generic → REJECT
        - "alcaldía de manizales" → qualified → KEEP
        - "gobernación de bolívar" → qualified → KEEP

        This is the complement of _is_unqualified_dept which protects
        department names.  Together they prevent ENTITY-MISRESOLUTION.
        """
        alias_lower = alias.strip().lower()

        # Exact match against generic tokens
        if alias_lower in self._GENERIC_ENTITY_TOKENS:
            return True

        # Article/preposition + generic token: "la alcaldía", "el municipio"
        _ARTICLES = {"el", "la", "los", "las", "de", "del"}
        words = alias_lower.split()
        content_words = [w for w in words if w not in _ARTICLES]
        if len(content_words) == 1 and content_words[0] in self._GENERIC_ENTITY_TOKENS:
            return True

        return False

    def scan_departamento(self, normalized_text: str) -> ScanResult | None:
        """
        Scan text for known department aliases. Returns the LONGEST match.
        Should be called AFTER scan_entity on the remaining text.
        """
        return self._scan_against_index(normalized_text, self._dept_scan_index)

    def scan_region(self, normalized_text: str) -> ScanResult | None:
        """
        Scan text for known region aliases (Caribe, Andina, Pacífica, ...).
        Returns the LONGEST match; ``official_name`` carries the canonical
        region. Should be called AFTER scan_departamento on the remaining text
        so a specific department always wins over a broader region.
        """
        return self._scan_against_index(normalized_text, self._region_scan_index)

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

        # INV-INCOMPLETE-BUYER-CANNOT-BECOME-TOPIC. ``scan_entity`` ya rechaza
        # el genérico desnudo, pero esta puerta quedaba abierta: los tiers de
        # fuzzy global le encuentran vecino a cualquier cosa y "alcaldia" salía
        # resuelto —con confianza alta— a una entidad que nadie pidió.
        if is_generic_buyer_token(normalized):
            return ResolutionResult(
                value=None,
                method="generic_buyer_unqualified",
                confidence="none",
                metadata={"buyer_mention": normalized},
                clarification_needed=True,
                clarification_hint=incomplete_buyer_hint(normalized),
            )

        strict_rewrite = self._strict_rewrite_target(normalized)

        # Tier 1: Exact alias match
        if normalized in self.alias_index:
            official = self.alias_index[normalized]
            if strict_rewrite and not self._official_matches_strict_rewrite(official, strict_rewrite):
                # Skip exact match that fails strict rewrite, continue to fuzzy
                pass
            else:
                return ResolutionResult(value=official, method="exact", confidence="high")

        # Tier 2: Department-scoped fuzzy
        if departamento and departamento in self.entities_by_department:
            dept_entities = self.entities_by_department[departamento]
            dept_choices = {normalize_text(e): e for e in dept_entities}
            match = process.extractOne(
                normalized, list(dept_choices.keys()),
                scorer=fuzz.WRatio, score_cutoff=self.FUZZY_CUTOFF_ENTIDAD,
            )
            if match:
                official = dept_choices[match[0]]
                if strict_rewrite and not self._official_matches_strict_rewrite(official, strict_rewrite):
                    return self._unresolved_rewrite(user_input, strict_rewrite)
                return ResolutionResult(
                    value=official, method=f"fuzzy_dept({int(match[1])})", confidence="medium",
                    metadata={"departamento_filter": departamento},
                )

        # Tier 3: Global fuzzy — try top-3 candidates to avoid alias-collision traps
        matches = process.extract(
            normalized, list(self.choice_to_official.keys()),
            scorer=fuzz.WRatio, limit=3,
        )
        for match, score, _ in matches:
            if score < self.FUZZY_CUTOFF_ENTIDAD:
                break
            official = self.choice_to_official[match]
            if strict_rewrite and not self._official_matches_strict_rewrite(official, strict_rewrite):
                continue
            return ResolutionResult(
                value=official, method=f"fuzzy({int(score)})", confidence="medium",
            )

        # Tier 4: LIKE fallback
        if strict_rewrite:
            return self._unresolved_rewrite(user_input, strict_rewrite)
        logger.info("Entity '%s' fell through to LIKE fallback", user_input)
        return ResolutionResult(value=None, like_value=normalized.upper(), method="like", confidence="low")

    def _unresolved_rewrite(self, user_input: str, strict_rewrite: tuple[str, str]) -> ResolutionResult:
        _kind, target = strict_rewrite
        return ResolutionResult(
            value=None,
            method="rewrite_unresolved",
            confidence="low",
            metadata={"target": target},
            clarification_needed=True,
            clarification_hint=(
                f"No encontré '{user_input}' en el catálogo de entidades. "
                "¿Cuál es el nombre oficial de la entidad?"
            ),
        )

    @staticmethod
    def _strict_rewrite_target(normalized: str) -> tuple[str, str] | None:
        municipality_match = re.match(
            r"^(?:municipio|alcaldia(?:\s+municipal)?)\s+de\s+(.+)$",
            normalized,
        )
        if municipality_match:
            return ("municipality", municipality_match.group(1).strip())

        # ``de(?:l)?`` por la misma razón que en ``entity_types.ENTITY_REWRITES``:
        # esta guardia es lo que impide que el fuzzy global elija cualquier
        # vecino léxico. Sin ella, "gobernacion del X" quedaba a merced del
        # fuzzy —así "gobernación de San Andrés" terminaba en DEPARTAMENTO DE
        # SANTANDER (ENTITY-CORE-COVERAGE-R0, WB-03)—. Cuando no hay candidato
        # que satisfaga la reescritura, el resolver pide precisión en vez de
        # adivinar (Tier 4 → _unresolved_rewrite).
        department_match = re.match(
            r"^(?:departamento|gobernacion)\s+de(?:l)?\s+(.+)$", normalized,
        )
        if department_match:
            return ("department", department_match.group(1).strip())

        return None

    @staticmethod
    def _official_matches_strict_rewrite(official: str, strict_rewrite: tuple[str, str]) -> bool:
        kind, target = strict_rewrite
        official_norm = normalize_text(official)
        if target not in official_norm:
            return False
        if kind == "municipality":
            return any(marker in official_norm for marker in ("municipio", "alcaldia", "distrito"))
        if kind == "department":
            return any(marker in official_norm for marker in ("departamento", "gobernacion"))
        return False

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

    def _build_region_scan_index(self) -> None:
        """Build scan index for regions from REGION_ALIASES (alias → región)."""
        entries = [
            (alias, region)
            for alias, region in REGION_ALIASES.items()
            if len(alias) >= 3
        ]
        self._region_scan_index = self._build_scan_index(entries)

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
