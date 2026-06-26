"""Precedencia entre phrase_catalog y morphological_variants (KNOWN_BIGRAMS).

Regla de propiedad (una sola capa decide cada frase):
  - phrase_catalog       = qué palabras forman la unidad + forma canónica literal
  - morphological_variants = variación superficial/gramatical (equipo↔equipos)
  - polysemic_topics     = qué sentido tiene según el contexto

PRECEDENCIA (post-Commit 5): `detect_phrases` (catálogo) corre ANTES que
`_extract_object_terms`/KNOWN_BIGRAMS en el parser y CONSUME el span; además el
WHERE ya consume `objeto_phrases`. Por eso el catálogo es el dueño: aunque una
frase también estuviera en KNOWN_BIGRAMS, el catálogo la detecta primero y
KNOWN_BIGRAMS no la doble-procesa. El solapamiento ya NO es regresión (antes del
Commit 5 sí lo era: la frase salía de objeto a objeto_phrases sin que el WHERE la
usara). Por tanto NO exigimos ∅; exigimos OWNERSHIP del catálogo.
"""
from __future__ import annotations

import pytest

from app.core.phrase_catalog import PHRASE_CATALOG, detect_phrases
from app.core.retrieval_text import normalize_retrieval_text


@pytest.mark.parametrize("spec", PHRASE_CATALOG, ids=lambda s: s.canonical)
def test_catalog_owns_and_consumes_its_phrases(spec):
    """Cada frase del catálogo es detectada y su span consumido (precedencia)."""
    for surface in (spec.canonical, *spec.variants):
        canonicals, remaining = detect_phrases(surface)
        assert spec.canonical in canonicals, f"no detectada: {surface}"
        # span consumido: las anclas ya no quedan en el texto restante
        rem = normalize_retrieval_text(remaining)
        for anchor in spec.ordered_anchors:
            assert anchor not in rem.split(), f"ancla '{anchor}' no consumida de '{surface}'"
