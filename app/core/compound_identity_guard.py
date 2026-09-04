"""Protect complete municipal identities without accrediting municipal buyers.

DIVIPOLA supplies negative constraints. Buyer ownership still comes exclusively
from the resolver's existing aliases and institutional family evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re

from app.core.entity_resolver import EntityResolver, ResolutionResult
from app.core.entity_types import EntityCandidate
from app.core.municipal_territory import MunicipalTerritory, MunicipalTerritoryRegistry, key
from app.utils.money import normalize_text

_MUNICIPAL_HEAD = re.compile(r'^(?:municipio|alcald[ií]a(?:\s+municipal)?)\s+de\s+(.+)$', re.I)
_QUALIFIER = re.compile(r'^(?:(?:el\s+)?(?:departamento|depto\.?)\s+(?:de\s+)?)?', re.I)


class IdentityStatus(str, Enum):
    NONE = 'NONE'
    UNIQUE_MUNICIPAL_IDENTITY = 'UNIQUE_MUNICIPAL_IDENTITY'
    AMBIGUOUS_MUNICIPAL_IDENTITY = 'AMBIGUOUS_MUNICIPAL_IDENTITY'


@dataclass(frozen=True)
class CompoundIdentityDecision:
    status: IdentityStatus = IdentityStatus.NONE
    protected_span: str = ''
    candidates: tuple[MunicipalTerritory, ...] = ()
    department_mismatch: bool = False

    def unresolved(self) -> ResolutionResult:
        ambiguous = self.status == IdentityStatus.AMBIGUOUS_MUNICIPAL_IDENTITY
        hint = (f'¿En qué departamento está {self.protected_span}? Opciones: '
                + ', '.join(sorted({t.department_value for t in self.candidates}))
                if ambiguous else
                f"No acredité un comprador para '{self.protected_span}'. ¿Cuál es su nombre institucional completo?")
        return ResolutionResult(
            value=None, method='municipal_identity_guard', confidence='none',
            clarification_needed=True, clarification_hint=hint,
            metadata={'municipal_identity_status': self.status.value,
                      'protected_span': self.protected_span,
                      'protected_divipola': ','.join(t.divipola for t in self.candidates),
                      'department_mismatch': str(self.department_mismatch)},
        )

    def district_alias(self, resolver: EntityResolver, registry: MunicipalTerritoryRegistry):
        # A district is another municipal executive spelling. Consult only an
        # existing full exact alias, after the ordinary rewrite ladder fails.
        target = _MUNICIPAL_HEAD.fullmatch(self.protected_span)[1]
        canonical = f'Distrito especial de {target}'
        if normalize_text(canonical) not in resolver.owners_by_alias:
            return None
        match = resolver.resolve_entidad(canonical)
        return (canonical, match) if self.accepts(canonical, match, resolver, registry) else None

    def accepts(self, canonical: str, match: ResolutionResult, resolver: EntityResolver,
                registry: MunicipalTerritoryRegistry) -> bool:
        if (self.status != IdentityStatus.UNIQUE_MUNICIPAL_IDENTITY
                or self.department_mismatch or match.method != 'exact' or not match.value):
            return False
        # Exactness must mean unique ownership, never alias_index's last owner.
        owners = resolver.owners_by_alias.get(normalize_text(canonical), set())
        if not owners:
            owners = resolver.source_names_to_official.get(normalize_text(canonical), set())
        if owners != {match.value}:
            return False
        if not resolver._official_matches_institutional_family(match.value, 'municipality'):
            return False
        # A known contradictory jurisdiction defeats even an exact alias
        # (e.g. a local mayoralty whose name is also a complete municipality).
        departments = {registry.department_aliases.get(key(department))
                       for department, buyers in resolver.entities_by_department.items()
                       if match.value in buyers}
        return not departments or departments == {self.candidates[0].department_value}


def decide_identity(span: str, registry: MunicipalTerritoryRegistry) -> CompoundIdentityDecision:
    match = _MUNICIPAL_HEAD.fullmatch(span)
    if not match:
        return CompoundIdentityDecision()
    name, comma, department = match[1], '', ''
    candidates = registry.lookup(name)
    if not candidates:
        name, comma, department = match[1].partition(',')
        candidates = registry.lookup(name)
    if not candidates:
        return CompoundIdentityDecision()
    filtered = candidates
    if comma:
        department = _QUALIFIER.sub('', department.strip(), count=1)
        filtered = registry.lookup(name, department)
    return CompoundIdentityDecision(
        status=(IdentityStatus.UNIQUE_MUNICIPAL_IDENTITY if len(filtered) == 1
                else IdentityStatus.AMBIGUOUS_MUNICIPAL_IDENTITY),
        protected_span=span, candidates=filtered or candidates,
        department_mismatch=bool(comma and not filtered),
    )


def identity_guards(candidates: list[EntityCandidate], registry: MunicipalTerritoryRegistry
                    ) -> dict[str, CompoundIdentityDecision]:
    """First complete identity in each syntactic ladder is an indivisible floor.

    Non-municipal trailing tokens may be removed until this floor is reached.
    The candidate generator remains purely syntactic.
    """
    guards = {}
    seen = set()
    for candidate in candidates:
        if candidate.rewrite_type in guards or candidate.consumed_span in seen:
            continue
        seen.add(candidate.consumed_span)
        decision = decide_identity(candidate.consumed_span, registry)
        if decision.status != IdentityStatus.NONE:
            guards[candidate.rewrite_type] = decision
    return guards


def compound_identity_clarification(params: dict) -> str | None:
    resolution = params.get('entidad_resolution') or {}
    if ((resolution.get('metadata') or {}).get('municipal_identity_status')
            and resolution.get('clarification_needed')):
        return resolution.get('clarification_hint') or 'Precisa el municipio y su departamento.'
    return None
