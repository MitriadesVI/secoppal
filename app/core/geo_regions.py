"""Pure region lookup over ``colombia_geography`` (until now data without a function)."""
from __future__ import annotations

from dataclasses import dataclass
import re

from app.core.municipal_territory import key
from app.data.colombia_geography import REGION_ALIASES, ZONES_BY_NAME
from app.data.geo_secop_variants import AMBIGUOUS_REGION_ALIASES


@dataclass(frozen=True)
class RegionMatch:
    region: str                      # key of ZONES_BY_NAME ("Caribe", "Eje Cafetero"...)
    departments: tuple[str, ...]     # official SECOP department values
    alias: str                       # token key that matched (the receipt)


_INDEX: dict[str, str] = {key(alias): region for alias, region in REGION_ALIASES.items()}
_INDEX.update({key(region): region for region in ZONES_BY_NAME})
_FRAME = re.compile(r'^(?:(?:la|el|las|los)\s+)?(?:region|zona)\s+(?:(?:de|del)\s+)?(?:(?:la|el|las|los)\s+)?(?P<rest>.+)$')


def _match(alias_key: str, region: str) -> RegionMatch:
    return RegionMatch(region, tuple(ZONES_BY_NAME[region]), alias_key)


def region_lookup(text: str) -> tuple[RegionMatch, ...]:
    """Regions a mention can denote: 0 (not a region), 1, or several (ambiguous).

    ``Caribe``, ``costa atlántica``, ``región del Caribe`` -> Caribe; ``Eje
    Cafetero`` -> its three departments; ``la costa`` -> Caribe AND Pacífica.
    ``Amazonas`` is a department and is not a region.
    """
    k = key(text) if isinstance(text, str) else ''
    if not k:
        return ()
    framed_match = _FRAME.match(k)
    candidates = (k, framed_match['rest']) if framed_match else (k,)
    for candidate in candidates:
        if candidate in AMBIGUOUS_REGION_ALIASES:
            return tuple(_match(candidate, r) for r in AMBIGUOUS_REGION_ALIASES[candidate])
        if candidate in _INDEX:
            return (_match(candidate, _INDEX[candidate]),)
        framed = f'region {candidate}'
        if framed in _INDEX:
            return (_match(framed, _INDEX[framed]),)
    return ()


def is_region_key(text: str) -> bool:
    return bool(region_lookup(text))
