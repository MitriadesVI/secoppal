"""A place or a buyer right after «o mayores» ends the bound (one responsibility).

«de 300 millones o mayores Galapa» is a bound on the amount followed by the NAME of the place the
person is asking about; «de 300 millones o mayores plazos» is not (``mayores`` modifies ``plazos``).
Since R2 only punctuation, a connector or a report verb/participle (``money_suffix_event``) closed the
bound, so the bare name made the amount ask. That was the single VALUE regression of wave 1 against the
base, which read the bound and the buyer.

A continuation that STARTS with a name the existing catalogs recognise counts as the boundary:

* a municipality of the DIVIPOLA gazetteer (``municipal_territory``) with its curated aliases
  (``geo_authority.municipal_candidates``), a department or a region (``geo_authority`` /
  ``geo_regions``);
* a buyer mention in the grammar of ``entity_types.ENTITY_REWRITES`` («alcaldía de X», «gobernación
  del X», «hospital …», «universidad …»).

No word list lives here: the catalogs are asked, never copied. A noun that is not a place
(«plazo», «duración», «cuantía») is not recognised, so «y mayores plazos» and «o mayor duración» keep
asking. Fail-closed: when no catalog recognises the continuation, the amount asks as before.

Pure: no I/O beyond the lazily loaded read-only catalogs.
"""
from __future__ import annotations

import re

_WORD = re.compile(r"[a-zA-Z0-9áéíóúüñÁÉÍÓÚÜÑ]+")
_MAX_WORDS_CAP = 6


def _gazetteer_names(words: list[str]) -> bool:
    """A leading run of ``words`` that is a municipality, department or region of the catalogs."""
    from app.core.geo_authority import department_of, municipal_candidates
    from app.core.geo_regions import region_lookup
    from app.core.municipal_territory import get_municipal_territories

    widest = min(len(words), get_municipal_territories().max_words, _MAX_WORDS_CAP)
    for width in range(widest, 0, -1):
        name = " ".join(words[:width])
        if municipal_candidates(name) or department_of(name) or region_lookup(name):
            return True
    return False


_ARTICLE = re.compile(r"(?:el|la|los|las)\s+", re.IGNORECASE)


def _buyer_grammar(continuation: str) -> bool:
    """The continuation opens a buyer mention the entity grammar already knows."""
    from app.core.entity_types import ENTITY_REWRITES

    article = _ARTICLE.match(continuation)
    bare = continuation[article.end():] if article else continuation
    return any(pattern.match(bare) for pattern, _ in ENTITY_REWRITES)


def names_a_place(continuation: str) -> bool:
    """True when ``continuation`` (the text right after the suffix) opens a place or a buyer."""
    stripped = continuation.lstrip()
    if not stripped:
        return False
    return _buyer_grammar(stripped) or _gazetteer_names(_WORD.findall(stripped))
