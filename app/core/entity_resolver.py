from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

from rapidfuzz import fuzz, process

# ``entity_types`` no importa nada de ``app.core``: la arista resolver→types
# no cierra ciclo. Allí vive la autoridad de tokenización de nombres, junto a
# la gramática de captura que la produce.
from app.core.entity_types import name_tokens
from app.core.generic_buyer import (
    GENERIC_BUYER_TOKENS,
    department_root_buyer_target,
    incomplete_buyer_hint,
    is_unqualified_buyer_name,
)
from app.core.fuzzy_identity_guard import (
    FILLER_TOKENS,
    candidate_keeps_requested_identity,
)
from app.core.geography_alias import is_geography_alias
from app.core.scan_alias_policy import is_scan_eligible_alias, record_alias_policy
from app.core.territory_phrase_index import TerritoryPhraseIndex
from app.core.proper_name_spans import ExactNameIndex
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


# Frases que nombran territorio, no institución.  Las usa la puerta del fuzzy
# para saber qué pidió el usuario; la autoridad sigue siendo el catálogo
# territorial, no una lista propia.  El índice es por FRASE porque el catálogo
# lo es: "norte de santander" no es "santander" (R2E.1A).
TERRITORY_PHRASES: TerritoryPhraseIndex = TerritoryPhraseIndex.from_alias_maps(
    DEPARTAMENTOS, GAZETTEER_CIUDADES, connectors=FILLER_TOKENS
)


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


def is_department_root_name(official: str) -> bool:
    """True si ``official`` ES un comprador raíz departamental.

    "DEPARTAMENTO DE SANTANDER" lo es; "GOBERNACION DEL VALLE DEL CAUCA - DADI"
    no, porque lo que sigue al conector no es un departamento del catálogo.
    """
    target = department_root_buyer_target(normalize_text(official))
    return bool(target and DEPARTAMENTOS.get(target))


def _contains_token_sequence(haystack: str, needle: str) -> bool:
    """True si ``needle`` aparece en ``haystack`` como secuencia de palabras."""
    hay = name_tokens(haystack)
    want = name_tokens(needle)
    if not want or len(want) > len(hay):
        return False
    return any(
        hay[i:i + len(want)] == want
        for i in range(len(hay) - len(want) + 1)
    )


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

        # Alias ownership and SECOP physical names are different evidence.
        # ``alias_index`` remains for backward-compatible non-root resolution;
        # strict departmental roots use the non-lossy ownership structures.
        self.owners_by_alias: dict[str, set[str]] = {}
        self.aliases_by_official: dict[str, set[str]] = {}
        self.source_names_to_official: dict[str, set[str]] = {}
        self.resolution_blocked_aliases: set[str] = set()
        self._root_evidence_by_department: dict[str, dict[str, tuple[str, ...]]] = {}

        # For scan (substring detection in text)
        # Index: first_word → [(normalized_alias, official_name), ...] sorted by alias length DESC
        self._scan_index: dict[str, list[tuple[str, str]]] = {}

        # Department scan index: same structure
        self._dept_scan_index: dict[str, list[tuple[str, str]]] = {}

        # Region scan index: alias → región canónica
        self._region_scan_index: dict[str, list[tuple[str, str]]] = {}

        self._load_aliases()
        self.proper_names = ExactNameIndex([
            *self.owners_by_alias, *self.source_names_to_official,
            *GAZETTEER_CIUDADES, *DEPARTAMENTOS,
        ])
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

    def scan_entity(self, normalized_text: str, *, declared_group=None) -> ScanResult | None:
        """
        Scan text for known entity aliases. Returns the LONGEST match.

        Post-validation: two reject functions run in series:
        1. _is_unqualified_dept — rejects bare dept names (e.g., "antioquia")
        2. _is_unqualified_generic — rejects bare generic tokens (e.g., "alcaldía")
        Both must pass for the match to be accepted.
        """
        def _combined_reject(alias: str, full_text: str, official: str) -> bool:
            strict_rewrite = self._strict_rewrite_target(alias)
            # An accredited alias can name a child through its declared parent:
            # "alcaldía de Bogotá integración social". The full scan-eligible
            # alias and membership jointly authorize the child, never the parent
            # phrase alone or another institution sharing its NIT/territory.
            declared_child_alias = bool(
                declared_group and official in declared_group.members
                and any(alias.startswith(term + " ") for term in declared_group.aliases)
            )
            return (
                self._is_unqualified_dept(alias, full_text)
                or self._is_unqualified_generic(alias, full_text)
                or bool(
                    strict_rewrite
                    # Department roots have a stronger, non-lexical authority:
                    # ``_root_evidence_by_department`` can accredit an opaque
                    # physical name such as Tolima's GOBIERNO DEPARTAMENTAL.
                    # Their scan entries were already admitted/rejected from
                    # unique alias ownership while building the index.
                    and strict_rewrite[0] != "department"
                    and not declared_child_alias
                    and not self._official_matches_institutional_family(
                        official, strict_rewrite[0]
                    )
                )
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
        - "corpocaldas" → institución que termina en un departamento → KEEP
        """
        alias_stripped = alias.strip()

        # ``gobierno departamental del Tolima`` es una mención de comprador
        # calificada aunque su primera palabra no esté en _ENTITY_QUALIFIERS.
        # La gramática raíz conserva precedencia sobre el catálogo territorial.
        root_target = department_root_buyer_target(alias_stripped)
        if root_target and root_target in DEPARTAMENTOS:
            return False

        # ¿El alias nombra territorio? La autoridad es la pertenencia al
        # catálogo, no el sufijo textual: ver app/core/geography_alias.py.
        if not is_geography_alias(alias_stripped):
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

        if is_unqualified_buyer_name(alias_lower):
            return True

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

        If reject_fn is provided, it's called with
        (alias, full_text, official) and
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
            if reject_fn and reject_fn(alias, text, official):
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

        # Some phrases were once aliases but the audited evidence shows that
        # they do not identify a unique buyer even when written alone.  They
        # are removed from ownership and declared as negative evidence in the
        # gazetteer so global fuzzy cannot silently recreate the old owner.
        if normalized in self.resolution_blocked_aliases:
            return ResolutionResult(
                value=None,
                method="alias_resolution_blocked",
                confidence="none",
                metadata={"blocked_alias": normalized},
                clarification_needed=True,
                clarification_hint=(
                    f"'{user_input}' no identifica un comprador único. "
                    "¿Cuál es el nombre institucional completo?"
                ),
            )

        # INV-INCOMPLETE-BUYER-CANNOT-BECOME-TOPIC. ``scan_entity`` ya rechaza
        # el genérico desnudo, pero esta puerta quedaba abierta: los tiers de
        # fuzzy global le encuentran vecino a cualquier cosa y "alcaldia" salía
        # resuelto —con confianza alta— a una entidad que nadie pidió.
        if is_unqualified_buyer_name(normalized):
            return ResolutionResult(
                value=None,
                method="generic_buyer_unqualified",
                confidence="none",
                metadata={"buyer_mention": normalized},
                clarification_needed=True,
                clarification_hint=incomplete_buyer_hint(normalized),
            )

        strict_rewrite = self._strict_rewrite_target(normalized)

        # A recognized departmental root never enters lexical fuzzy.  Its
        # identity comes from unique alias ownership for the same canonical
        # territory, not from words that happen to occur in an SECOP physical
        # name.  This simultaneously rejects child hijacks and admits Tolima's
        # opaque but correctly-owned ``GOBIERNO DEPARTAMENTAL`` record.
        if strict_rewrite and strict_rewrite[0] == "department":
            canonical_department = DEPARTAMENTOS.get(strict_rewrite[1])
            if canonical_department:
                return self._resolve_department_root(
                    user_input,
                    normalized,
                    strict_rewrite[1],
                    canonical_department,
                )

        # Tier 1: Exact alias match
        if normalized in self.alias_index:
            official = self.alias_index[normalized]
            # ADR-072: exact alias ownership already accredits the written
            # territory, including qualified forms such as ``Pereira,
            # Risaralda`` whose complete tail need not occur in the physical
            # SECOP name.  It does not, however, make a Personeria an
            # Alcaldia: exact ownership must still preserve the requested
            # institutional family.
            if strict_rewrite and not self._official_matches_institutional_family(
                official, strict_rewrite[0]
            ):
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
                if not self._keeps_requested_identity(normalized, match[0], official):
                    return self._unresolved_identity(user_input, normalized)
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
            if not self._keeps_requested_identity(normalized, match, official):
                continue
            return ResolutionResult(
                value=official, method=f"fuzzy({int(score)})", confidence="medium",
            )

        # Tier 4: LIKE fallback
        if strict_rewrite:
            return self._unresolved_rewrite(user_input, strict_rewrite)
        logger.info("Entity '%s' fell through to LIKE fallback", user_input)
        return ResolutionResult(value=None, like_value=normalized.upper(), method="like", confidence="low")

    def _keeps_requested_identity(
        self, normalized: str, matched_alias: str, official: str
    ) -> bool:
        """Puerta de los tiers inferenciales; el ownership exacto no la cruza."""
        return candidate_keeps_requested_identity(
            normalized, matched_alias, official, TERRITORY_PHRASES
        )

    def _unresolved_identity(self, user_input: str, normalized: str) -> ResolutionResult:
        """Sin candidato que conserve la identidad pedida: filtro literal."""
        logger.info(
            "Entity '%s' has no fuzzy candidate that keeps its requested identity",
            user_input,
        )
        return ResolutionResult(
            value=None, like_value=normalized.upper(), method="like", confidence="low"
        )

    def _resolve_department_root(
        self,
        user_input: str,
        normalized: str,
        target: str,
        canonical_department: str,
    ) -> ResolutionResult:
        """Resolve a strict root request only from unambiguous alias ownership."""
        exact_owners = self.owners_by_alias.get(normalized, set())
        if len(exact_owners) > 1:
            return self._ambiguous_root_alias(
                user_input,
                target,
                canonical_department,
                exact_owners,
            )

        evidence = self._root_evidence_by_department.get(canonical_department, {})
        if len(evidence) > 1:
            return self._ambiguous_root_alias(
                user_input,
                target,
                canonical_department,
                set(evidence),
            )
        if not evidence:
            unresolved = self._unresolved_rewrite(
                user_input, ("department", target)
            )
            unresolved.metadata.update({
                "canonical_department": canonical_department,
                "root_evidence": "missing",
            })
            return unresolved

        official, supporting_aliases = next(iter(evidence.items()))
        if exact_owners and exact_owners != {official}:
            # Defensive consistency guard: a phrase cannot be accepted through
            # evidence owned by a different institution.
            return self._ambiguous_root_alias(
                user_input,
                target,
                canonical_department,
                exact_owners | {official},
            )

        exact = normalized in supporting_aliases and exact_owners == {official}
        supporting_alias = normalized if exact else supporting_aliases[0]
        return ResolutionResult(
            value=official,
            method="exact" if exact else "root_alias_evidence",
            confidence="high",
            metadata={
                "target": target,
                "canonical_department": canonical_department,
                "supporting_alias": supporting_alias,
                "root_evidence": "unique_alias_owner",
            },
        )

    def _ambiguous_root_alias(
        self,
        user_input: str,
        target: str,
        canonical_department: str,
        owners: set[str],
    ) -> ResolutionResult:
        return ResolutionResult(
            value=None,
            method="root_alias_ambiguous",
            confidence="none",
            metadata={
                "target": target,
                "canonical_department": canonical_department,
                "owner_count": str(len(owners)),
                "root_evidence": "ambiguous",
            },
            clarification_needed=True,
            clarification_hint=(
                f"Encontré más de un comprador que reclama '{user_input}'. "
                "¿Cuál es el nombre oficial de la entidad?"
            ),
        )

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
        # A locality token is not institutional identity.  Puente Aranda has
        # both a district local mayoralty/FDL and a military CENAC; accepting
        # either because both contain the locality would reproduce the
        # cross-owner defect closed under ADR-074.  Keep this before the
        # municipality grammar so ``alcaldia local`` is its own family.
        local_executive_match = re.match(
            r"^(?:alcaldia\s+local|fondo\s+de\s+desarrollo\s+local)"
            r"(?:\s+de)?\s+(.+)$",
            normalized,
        )
        if local_executive_match:
            return ("local_executive", local_executive_match.group(1).strip())

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
        department_target = department_root_buyer_target(normalized)
        if department_target:
            return ("department", department_target)

        return None

    @staticmethod
    def _official_matches_strict_rewrite(official: str, strict_rewrite: tuple[str, str]) -> bool:
        kind, target = strict_rewrite
        official_norm = normalize_text(official)
        # COMPOUND-BUYER-CAPTURE (R2A.1). La contención se mide por TOKENS
        # completos, no por substring: "villa" es prefijo de "villavicencio"
        # pero no es una palabra suya, y por esa rendija "Municipio de Villa"
        # —fragmento de "Villa de Leyva"— acreditaba a la alcaldía de
        # Villavicencio. El ruido de registro de SECOP ("ANTIOQUIA//",
        # "ATLANTICO**") se descarta al tokenizar, no al comparar.
        if not _contains_token_sequence(official_norm, target):
            return False
        return EntityResolver._official_matches_institutional_family(
            official, kind
        )

    @staticmethod
    def _official_matches_institutional_family(official: str, kind: str) -> bool:
        """Whether an official name belongs to the requested buyer family.

        This is deliberately separate from territory preservation.  Exact
        aliases own their complete wording under ADR-072; fuzzy candidates
        additionally pass ``_official_matches_strict_rewrite`` so the target
        territory must occur in the inferred owner too.
        """
        official_norm = normalize_text(official)
        tokens = name_tokens(official_norm)
        # SECOP antepone a veces el departamento (CUNDINAMARCA-ALCALDIA
        # MUNICIPIO MOSQUERA). Sólo la autoridad territorial puede acreditar
        # ese prefijo; un nombre de otra institución no es un wrapper.
        for prefix in sorted(
            (name_tokens(name) for name in DEPARTAMENTOS), key=len, reverse=True
        ):
            if tokens[:len(prefix)] == prefix:
                tokens = tokens[len(prefix):]
                break
        is_local_executive = (
            tokens[:2] == ["alcaldia", "local"]
            or tokens[:5] == ["fondo", "de", "desarrollo", "local", "de"]
            or tokens[:4] == ["fondo", "de", "desarrollo", "local"]
        )
        if kind == "municipality":
            # Family compatibility is affirmative: the requested municipal
            # executive must be the official name's institutional head.  A
            # child/control buyer such as ``PERSONERIA MUNICIPIO DE ...`` is
            # not made an alcaldia merely because its tail contains
            # ``municipio`` (Villa del Rosario: same NIT, distinct BuyerUnit).
            # The less-specific human form ``alcaldia de <localidad>`` is an
            # established alias of Bogota's alcaldias locales/FDL, so local
            # executive is a compatible specialization (Teusaquillo et al.).
            if is_local_executive:
                return True
            return bool(tokens and tokens[0] in {"municipio", "alcaldia", "distrito"})
        if kind == "local_executive":
            return is_local_executive
        if kind == "department":
            return bool(
                tokens
                and (
                    tokens[0] in {"departamento", "gobernacion"}
                    or tokens[:2] == ["gobierno", "departamental"]
                )
            )
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

        normalized_records: list[
            tuple[str, str, list[str], frozenset[str]]
        ] = []
        for record in data:
            official = record["official_name"]
            official_key = normalize_text(official)
            scan_disabled, resolution_blocked = record_alias_policy(record)
            self.resolution_blocked_aliases.update(resolution_blocked)
            self.source_names_to_official.setdefault(official_key, set()).add(official)
            if not is_unqualified_buyer_name(official_key):
                self.choice_to_official[official_key] = official

            dept = record.get("departamento")
            if dept:
                self.entities_by_department.setdefault(dept, []).append(official)

            alias_keys: list[str] = []
            for alias in record.get("aliases", []):
                alias_key = normalize_text(alias)
                if not alias_key:
                    continue
                self.alias_index[alias_key] = official
                self.choice_to_official[alias_key] = official
                self.owners_by_alias.setdefault(alias_key, set()).add(official)
                self.aliases_by_official.setdefault(official, set()).add(alias_key)
                alias_keys.append(alias_key)

            # ADR-072 also applies to the automatically indexed canonical
            # surface. Disabling it must not manufacture a human alias.
            unknown_scan_policy = scan_disabled - {*alias_keys, official_key}
            if unknown_scan_policy:
                aliases = ", ".join(sorted(unknown_scan_policy)[:5])
                raise ValueError(
                    "Gazetteer scan policy conflict: disabled aliases are not "
                    f"owned by {official}: {aliases}"
                )

            normalized_records.append(
                (official, official_key, alias_keys, scan_disabled)
            )

        policy_conflicts = self.resolution_blocked_aliases & self.owners_by_alias.keys()
        if policy_conflicts:
            aliases = ", ".join(sorted(policy_conflicts)[:5])
            raise ValueError(
                "Gazetteer alias policy conflict: resolution-blocked aliases "
                f"still have owners: {aliases}"
            )

        # Derive root authority only after every alias owner is known.  A
        # normalized alias with more than one owner supplies no evidence, and
        # multiple independently-owned root aliases for one territory make the
        # territory ambiguous rather than load-order dependent.
        root_evidence: dict[str, dict[str, set[str]]] = {}
        for alias, owners in self.owners_by_alias.items():
            target = department_root_buyer_target(alias)
            canonical_department = DEPARTAMENTOS.get(target or "")
            if not canonical_department or len(owners) != 1:
                continue
            official = next(iter(owners))
            root_evidence.setdefault(canonical_department, {}).setdefault(
                official, set()
            ).add(alias)

        self._root_evidence_by_department = {
            department: {
                official: tuple(sorted(aliases))
                for official, aliases in sorted(owners.items())
            }
            for department, owners in root_evidence.items()
        }

        scan_entries: list[tuple[str, str]] = []  # (normalized_alias, official_name)
        for official, official_key, alias_keys, scan_disabled in normalized_records:
            # SOURCE_PHYSICAL_NAME is not automatically a SCAN_ALIAS.  Physical
            # names remain available to ordinary fuzzy resolution, except when
            # the name itself is structurally unqualified nationwide.
            all_scan_aliases = list(alias_keys)
            if not is_unqualified_buyer_name(official_key):
                all_scan_aliases.append(official_key)

            # Add unique aliases to scan entries
            seen = set()
            for alias in all_scan_aliases:
                if not alias or alias in seen or len(alias) < 3:
                    continue
                seen.add(alias)

                if not is_scan_eligible_alias(alias, scan_disabled):
                    continue

                target = department_root_buyer_target(alias)
                canonical_department = DEPARTAMENTOS.get(target or "")
                if canonical_department:
                    evidence = self._root_evidence_by_department.get(
                        canonical_department, {}
                    )
                    if len(evidence) != 1 or official not in evidence:
                        continue

                scan_entries.append((alias, official))

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
