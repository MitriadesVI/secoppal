"""Pure geography authority for Foundation SEARCH (department, municipality, region).

``resolve_geography`` decides what a GEOGRAPHY condition's *quote* denotes. It
reads nothing but the quote (plus the typed ``level`` and an optional
``department_hint`` the caller already owns) and composes the existing
catalogs: ``department_mention_resolution``, DIVIPOLA
(``get_municipal_territories``), ``REGION_ALIASES``/``ZONES_BY_NAME`` and the
curated SECOP spellings in ``app.data.geo_secop_variants``. It never guesses: a
homonym, a level conflict or an unknown name is CLARIFY; a negated or country
scope is BLOCK. SoQL projection is not here (core, C1).
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from app.core.entity_resolver import department_mention_resolution
from app.core.geo_regions import RegionMatch, region_lookup
from app.core.geo_window import clip, department_after, frame_level_before, owner_cue
from app.core.municipal_territory import get_municipal_territories, key
from app.data.geo_secop_variants import (
    CITY_KEYS_FILED_AS_DEPARTMENT,
    COUNTRY_KEYS,
    HUILA_COLOMBIA_DIVIPOLA,
    SECOP_CITY_VALUES,
    SUPPLEMENT_TERRITORIES,
    USER_ALIASES,
)

AUTHORITY = "geo-authority@1"
LEVELS = ("DEPARTMENT", "MUNICIPALITY", "REGION", "UNKNOWN")
BOGOTA_DEPARTMENT = "Distrito Capital de Bogotá"
BOGOTA_DIVIPOLA = "11001"

RESOLVED, CLARIFY, BLOCK = "RESOLVED", "CLARIFY", "BLOCK"


@dataclass(frozen=True)
class GeoResolution:
    decision: str                          # RESOLVED | CLARIFY | BLOCK
    level_resolved: str | None             # DEPARTMENT | MUNICIPALITY | REGION
    department: str | None                 # official SECOP department value
    municipality_divipola: str | None
    municipality_name: str | None          # official DANE name, title-cased
    region: str | None                     # key of ZONES_BY_NAME
    departments_in_region: tuple[str, ...]
    reason: str
    authority: str = AUTHORITY
    # Additive (defaults keep the minimal contract stable):
    candidates: tuple[str, ...] = ()       # LEVEL:payload options for CLARIFY
    secop_city_values: tuple[str, ...] = ()  # `ciudad` spellings SECOP stores
    evidence: tuple[str, ...] = ()         # receipt: which catalog entry decided


@dataclass(frozen=True)
class Place:
    divipola: str
    name: str                              # official DANE name, title-cased
    department: str                        # SECOP department value


_FRAME = re.compile(
    r'^(?:(?:el|la)\s+)?(?P<marker>municipio|ciudad|distrito|departamento|depto|region|zona)'
    r'\s+(?:(?:de|del)\s+)?(?:(?:el|la)\s+)?(?P<rest>.+)$')
_MARKER_LEVEL = {'municipio': 'MUNICIPALITY', 'ciudad': 'MUNICIPALITY', 'distrito': 'MUNICIPALITY',
                 'departamento': 'DEPARTMENT', 'depto': 'DEPARTMENT',
                 'region': 'REGION', 'zona': 'REGION'}
_LEAD_PREP = re.compile(r'^(?:(?:en|de|del)\s+)+')
_DETERMINER = re.compile(r'^(?:el|la|los|las)\s+')
_QUALIFIER_SPLITS = (
    re.compile(r'\s*[,;]\s*'),
    re.compile(r'\s*\(\s*(?P<q>[^)]*)\)?\s*$'),
    re.compile(r'\s+[-–—]\s+'),
    re.compile(r'\s+(?:departamento|depto\.?)\s+(?:de|del)\s+', re.I),
    re.compile(r'\s+en\s+', re.I),
)


def _enum(value: object) -> str:
    return str(getattr(value, 'value', value) or '').strip().upper()


def _place(code: str) -> Place | None:
    territory = get_municipal_territories().by_code.get(code)
    if territory:
        return Place(code, territory.municipality.title(), territory.department_value)
    if code in SUPPLEMENT_TERRITORIES:
        name, department = SUPPLEMENT_TERRITORIES[code]
        return Place(code, name.title(), department)
    return None


def municipal_candidates(name: str) -> tuple[Place, ...]:
    """DIVIPOLA officials + curated colloquial/SECOP spellings, deduplicated."""
    k = key(name)
    codes = {t.divipola for t in get_municipal_territories().lookup(k)}
    codes.update(USER_ALIASES.get(k, ()))
    return tuple(p for p in (_place(c) for c in sorted(codes)) if p)


def department_of(name: str) -> str | None:
    """Department value for an explicit department mention (``None`` otherwise)."""
    k = key(name)
    if not k or k in CITY_KEYS_FILED_AS_DEPARTMENT:
        return None
    found = department_mention_resolution(k).canonical
    return found or get_municipal_territories().department_aliases.get(k)


def secop_city_values(divipola: str) -> tuple[str, ...]:
    """``ciudad`` values SECOP stores for a municipality (verified against data)."""
    place = _place(divipola)
    if place is None:
        return ()
    return SECOP_CITY_VALUES.get(divipola) or (place.name,)


def _label(place: Place) -> str:
    return f"MUNICIPALITY:{place.divipola}:{place.name} ({place.department})"


def _out(decision: str, reason: str, *, level: str | None = None, department: str | None = None,
         place: Place | None = None, match: RegionMatch | None = None,
         candidates: tuple[str, ...] = (), evidence: tuple[str, ...] = ()) -> GeoResolution:
    return GeoResolution(
        decision=decision, level_resolved=level, department=department,
        municipality_divipola=place.divipola if place else None,
        municipality_name=place.name if place else None,
        region=match.region if match else None,
        departments_in_region=match.departments if match else (),
        reason=reason, candidates=candidates,
        secop_city_values=secop_city_values(place.divipola) if place else (),
        evidence=evidence)


def _has_hit(k: str) -> bool:
    return bool(k in COUNTRY_KEYS or department_of(k) or municipal_candidates(k) or region_lookup(k))


def _frame(k: str) -> tuple[str, str | None]:
    """Split a typed frame (``municipio de X``) if what follows is a known name.

    A whole-name hit wins (``ciudad bolivar`` is a municipality, not ``ciudad`` + ``bolivar``).
    """
    if _has_hit(k):
        return k, None
    framed = _FRAME.match(k)
    if framed and _has_hit(framed['rest']):
        return framed['rest'], _MARKER_LEVEL[framed['marker']]
    return k, None


def _parse(raw: str) -> tuple[str, str | None, str | None]:
    """(name key, typed marker level, raw qualifier) from the quote alone."""
    text = raw.strip().strip('"\'«»“”').strip()
    base = _LEAD_PREP.sub('', key(text))
    if _has_hit(base):
        return base, None, None
    bare = _DETERMINER.sub('', base)
    if bare != base and _has_hit(bare):
        return bare, None, None
    name, marker = _frame(base)
    if marker:
        return name, marker, None
    trimmed = _LEAD_PREP.sub('', text.lower())
    for splitter in _QUALIFIER_SPLITS:
        found = splitter.search(trimmed)
        if not found:
            continue
        head = _LEAD_PREP.sub('', key(trimmed[:found.start()]))
        name, marker = _frame(head)
        if _has_hit(name):
            return name, marker, found.groupdict().get('q') or trimmed[found.end():]
    return base, None, None


def _qualifier_department(raw: str) -> str | None:
    k = key(raw)
    framed = _FRAME.match(k)
    if framed and _MARKER_LEVEL[framed['marker']] == 'DEPARTMENT':
        k = framed['rest']
    return department_of(k)


def resolve_geography(quote: str, level: str, polarity: str, *,
                      department_hint: str | None = None,
                      left: tuple[str, ...] = (), right: tuple[str, ...] = ()) -> GeoResolution:
    """Decide what a GEOGRAPHY quote denotes.

    ``left``/``right`` are the folded window tokens (<= 4 each) around the quote;
    they can only withdraw a RESOLVED value (``_retire``), never grant one.
    """
    result = _resolve(quote, level, polarity, department_hint)
    if result.decision != RESOLVED:
        return result
    return _retire(result, clip(left, 'left'), clip(right, 'right'), tuple(key(quote).split()))


def _retire(result: GeoResolution, left: tuple[str, ...], right: tuple[str, ...],
            quote_tokens: tuple[str, ...]) -> GeoResolution:
    def withdraw(reason: str, *evidence: str) -> GeoResolution:
        return _out(CLARIFY, reason, evidence=evidence)

    if owner_cue(left, quote_tokens):
        return withdraw('GEO_CONTEXT_OWNER_CUE', 'window:owner')
    typed = frame_level_before(left)
    capital = result.reason == 'GEO_CAPITAL_DISTRICT'      # Bogotá is both levels: either frame is true
    if typed and typed != result.level_resolved and not (capital and typed == 'MUNICIPALITY'):
        return withdraw('GEO_LEVEL_CONFLICT', f'window:{typed}', f'resolved:{result.level_resolved}')
    after = department_after(right, department_of)
    if after and result.level_resolved == 'MUNICIPALITY' and after != result.department:
        return withdraw('GEO_CONTEXT_DEPARTMENT_CONFLICT', f'window:{after}', f'resolved:{result.department}')
    return result


def _resolve(quote: str, level: str, polarity: str, department_hint: str | None) -> GeoResolution:
    lvl, pol = _enum(level), _enum(polarity)
    if pol == 'NEGATIVE':
        return _out(BLOCK, 'GEO_NEGATIVE_UNSUPPORTED')
    if pol != 'POSITIVE':
        return _out(BLOCK, 'GEO_POLARITY_INVALID')
    if lvl not in LEVELS:
        return _out(BLOCK, 'GEO_LEVEL_INVALID')
    if not isinstance(quote, str) or not key(quote):
        return _out(BLOCK, 'GEO_EMPTY_QUOTE')

    name, marker, qualifier = _parse(quote)
    hint = None
    for source in (department_hint, qualifier):
        if source is None or not str(source).strip():
            continue
        if key(str(source)) in COUNTRY_KEYS:
            continue                      # "Cali, Colombia": the country adds nothing
        found = _qualifier_department(str(source))
        if not found:
            return _out(CLARIFY, 'GEO_QUALIFIER_UNRESOLVED', evidence=(f'qualifier:{key(str(source))}',))
        if hint and found != hint:
            return _out(CLARIFY, 'GEO_DEPARTMENT_HINT_CONFLICT', candidates=(f'DEPARTMENT:{hint}', f'DEPARTMENT:{found}'))
        hint = found
    if lvl != 'UNKNOWN' and marker and marker != lvl:
        return _out(CLARIFY, 'GEO_LEVEL_CONFLICT', evidence=(f'typed:{marker}', f'declared:{lvl}'))
    effective = lvl if lvl != 'UNKNOWN' else (marker or 'UNKNOWN')

    if name in COUNTRY_KEYS:
        if effective == 'MUNICIPALITY' and name == 'colombia' and (marker or hint == 'Huila'):
            pass                          # "municipio de Colombia" is the Huila municipality
        elif effective == 'MUNICIPALITY':
            return _out(CLARIFY, 'GEO_COUNTRY_VS_MUNICIPALITY', evidence=(f'country:{name}',))
        else:
            return _out(BLOCK, 'GEO_COUNTRY_IS_NOT_A_FILTER', evidence=(f'country:{name}',))

    dept = department_of(name)
    munis = municipal_candidates(name)
    regions = region_lookup(name)
    if name in COUNTRY_KEYS:              # only reachable as the Huila municipality
        munis = tuple(p for p in munis if p.divipola == HUILA_COLOMBIA_DIVIPOLA)
        dept = None

    if effective == 'DEPARTMENT':
        return _department(name, dept, munis, regions, hint)
    if effective == 'MUNICIPALITY':
        return _municipality(name, dept, munis, regions, hint)
    if effective == 'REGION':
        return _region(name, dept, munis, regions)
    return _unknown(name, dept, munis, regions, hint)


def _options(dept: str | None, munis: tuple[Place, ...], regions: tuple[RegionMatch, ...]) -> tuple[str, ...]:
    return tuple(([f'DEPARTMENT:{dept}'] if dept else []) + [_label(p) for p in munis]
                 + [f'REGION:{m.region}' for m in regions])


def _department(name, dept, munis, regions, hint) -> GeoResolution:
    if not dept:
        if munis or regions:
            return _out(CLARIFY, 'GEO_LEVEL_CONFLICT', candidates=_options(dept, munis, regions))
        return _out(CLARIFY, 'GEO_DEPARTMENT_NOT_FOUND', evidence=(f'name:{name}',))
    if hint and hint != dept:
        return _out(CLARIFY, 'GEO_DEPARTMENT_HINT_CONFLICT', candidates=(f'DEPARTMENT:{dept}', f'DEPARTMENT:{hint}'))
    return _out(RESOLVED, 'GEO_DEPARTMENT_RESOLVED', level='DEPARTMENT', department=dept,
                evidence=(f'department:{name}',))


def _municipality(name, dept, munis, regions, hint) -> GeoResolution:
    if not munis:
        if dept or regions:
            return _out(CLARIFY, 'GEO_LEVEL_CONFLICT', candidates=_options(dept, munis, regions))
        return _out(CLARIFY, 'GEO_MUNICIPALITY_NOT_FOUND', evidence=(f'name:{name}',))
    chosen = tuple(p for p in munis if p.department == hint) if hint else munis
    if hint and not chosen:
        return _out(CLARIFY, 'GEO_DEPARTMENT_MISMATCH', candidates=tuple(_label(p) for p in munis))
    if len(chosen) > 1:
        return _out(CLARIFY, 'GEO_MUNICIPALITY_AMBIGUOUS', candidates=tuple(_label(p) for p in chosen))
    place = chosen[0]
    return _out(RESOLVED, 'GEO_MUNICIPALITY_RESOLVED', level='MUNICIPALITY',
                department=place.department, place=place,
                evidence=(f'divipola:{place.divipola}',) + ((f'hint:{hint}',) if hint else ()))


def _region(name, dept, munis, regions) -> GeoResolution:
    if not regions:
        if dept or munis:
            return _out(CLARIFY, 'GEO_LEVEL_CONFLICT', candidates=_options(dept, munis, regions))
        return _out(CLARIFY, 'GEO_REGION_NOT_FOUND', evidence=(f'name:{name}',))
    if len(regions) > 1:
        return _out(CLARIFY, 'GEO_REGION_AMBIGUOUS', candidates=tuple(f'REGION:{m.region}' for m in regions))
    match = regions[0]
    return _out(RESOLVED, 'GEO_REGION_RESOLVED', level='REGION', match=match,
                evidence=(f'region_alias:{match.alias}', f'departments:{len(match.departments)}'))


def _unknown(name, dept, munis, regions, hint) -> GeoResolution:
    if hint:
        chosen = tuple(p for p in munis if p.department == hint)
        if len(chosen) == 1:
            return _municipality(name, dept, chosen, regions, hint)
        if dept and dept == hint and not chosen:
            return _department(name, dept, munis, regions, hint)
        if munis or dept:
            return _out(CLARIFY, 'GEO_DEPARTMENT_MISMATCH' if not chosen else 'GEO_MUNICIPALITY_AMBIGUOUS',
                        candidates=_options(dept, chosen or munis, regions))
    if (dept == BOGOTA_DEPARTMENT and not regions
            and {p.divipola for p in munis} == {BOGOTA_DIVIPOLA}):
        place = munis[0]                  # one entity: the capital district is both levels
        return _out(RESOLVED, 'GEO_CAPITAL_DISTRICT', level='DEPARTMENT', department=dept, place=place,
                    evidence=('department:bogota', f'divipola:{BOGOTA_DIVIPOLA}'))
    kinds = sum(bool(x) for x in (dept, munis, regions))
    if kinds == 0:
        return _out(CLARIFY, 'GEO_NOT_FOUND', evidence=(f'name:{name}',))
    if kinds > 1:
        return _out(CLARIFY, 'GEO_LEVEL_AMBIGUOUS', candidates=_options(dept, munis, regions))
    if dept:
        return _department(name, dept, munis, regions, hint)
    if regions:
        return _region(name, dept, munis, regions)
    return _municipality(name, dept, munis, regions, hint)


# --------------------------------------------------------------------------- #
# Destination contract (C0 §5.5): GeoValue + host reasons. The core adapter
# (host_authorities, C1.1) calls `to_geo_value` / `host_reason`; nothing here
# imports the core contract.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GeoValue:
    level: str                             # DEPARTMENT | MUNICIPALITY | REGION
    canonical: str                         # SECOP department / DANE municipality name / region key
    divipola: str | None                   # municipality only
    members: tuple[str, ...]               # REGION: its departments; MUNICIPALITY: (containing department,)


HOST_REASON: dict[str, str] = {
    'GEO_NEGATIVE_UNSUPPORTED': 'NEGATION_UNSUPPORTED:GEOGRAPHY',
    'GEO_COUNTRY_IS_NOT_A_FILTER': 'GENERIC_IDENTITY:GEOGRAPHY',
    'GEO_LEVEL_AMBIGUOUS': 'AMBIGUOUS_HOST_DIMENSION:GEOGRAPHY',
    'GEO_MUNICIPALITY_AMBIGUOUS': 'AMBIGUOUS_HOST_DIMENSION:GEOGRAPHY',
    'GEO_REGION_AMBIGUOUS': 'AMBIGUOUS_HOST_DIMENSION:GEOGRAPHY',
    'GEO_COUNTRY_VS_MUNICIPALITY': 'AMBIGUOUS_HOST_DIMENSION:GEOGRAPHY',
    'GEO_DEPARTMENT_HINT_CONFLICT': 'AMBIGUOUS_HOST_DIMENSION:GEOGRAPHY',
    'GEO_CONTEXT_DEPARTMENT_CONFLICT': 'AMBIGUOUS_HOST_DIMENSION:GEOGRAPHY',
    'GEO_CONTEXT_OWNER_CUE': 'LEVEL_MISMATCH:GEOGRAPHY',
    'GEO_LEVEL_CONFLICT': 'LEVEL_MISMATCH:GEOGRAPHY',
    'GEO_DEPARTMENT_MISMATCH': 'UNRESOLVED_HOST_DIMENSION:GEOGRAPHY',
    'GEO_NOT_FOUND': 'UNRESOLVED_HOST_DIMENSION:GEOGRAPHY',
    'GEO_DEPARTMENT_NOT_FOUND': 'UNRESOLVED_HOST_DIMENSION:GEOGRAPHY',
    'GEO_MUNICIPALITY_NOT_FOUND': 'UNRESOLVED_HOST_DIMENSION:GEOGRAPHY',
    'GEO_REGION_NOT_FOUND': 'UNRESOLVED_HOST_DIMENSION:GEOGRAPHY',
    'GEO_QUALIFIER_UNRESOLVED': 'UNRESOLVED_HOST_DIMENSION:GEOGRAPHY',
    'GEO_EMPTY_QUOTE': 'UNRESOLVED_HOST_DIMENSION:GEOGRAPHY',
    'GEO_POLARITY_INVALID': 'UNRESOLVED_HOST_DIMENSION:GEOGRAPHY',
    'GEO_LEVEL_INVALID': 'UNRESOLVED_HOST_DIMENSION:GEOGRAPHY',
}
RESOLVED_REASONS = frozenset({'GEO_DEPARTMENT_RESOLVED', 'GEO_MUNICIPALITY_RESOLVED',
                              'GEO_REGION_RESOLVED', 'GEO_CAPITAL_DISTRICT'})


def host_reason(resolution: GeoResolution) -> str | None:
    """Destination reason for a non-RESOLVED verdict (``None`` when RESOLVED)."""
    return None if resolution.decision == RESOLVED else HOST_REASON[resolution.reason]


def to_geo_value(resolution: GeoResolution) -> GeoValue | None:
    """The executable value of a RESOLVED verdict; ``None`` otherwise."""
    if resolution.decision != RESOLVED:
        return None
    level = resolution.level_resolved
    if level == 'REGION':
        return GeoValue('REGION', resolution.region, None, resolution.departments_in_region)
    if level == 'MUNICIPALITY':
        return GeoValue('MUNICIPALITY', resolution.municipality_name, resolution.municipality_divipola,
                        (resolution.department,))
    return GeoValue('DEPARTMENT', resolution.department, None, ())
