"""Frontera entre phrase_catalog y morphological_variants (KNOWN_BIGRAMS).

Una sola capa debe ser dueña de cada frase. Hasta que el WHERE consuma
`objeto_phrases` (Commit 5), una frase que esté TAMBIÉN en KNOWN_BIGRAMS saldría
de `objeto` (la raíz que el WHERE usa hoy) hacia `objeto_phrases` (ignorado por el
builder) → dejaría de filtrar = regresión silenciosa.

Regla de propiedad:
  - phrase_catalog       = qué palabras forman la unidad + forma canónica literal
  - morphological_variants = variación superficial/gramatical (equipo↔equipos)
  - polysemic_topics     = qué sentido tiene según el contexto
"""
from __future__ import annotations

from app.core.phrase_catalog import PHRASE_CATALOG
from app.data.morphological_variants import KNOWN_BIGRAMS


def test_catalog_phrases_not_owned_by_known_bigrams():
    offending = []
    for spec in PHRASE_CATALOG:
        for surface in (spec.canonical, *spec.variants):
            if tuple(surface.split()) in KNOWN_BIGRAMS:
                offending.append(surface)
    assert offending == [], (
        f"Frases en AMBOS phrase_catalog y KNOWN_BIGRAMS: {offending}. "
        "Hasta Commit 5 esto causa regresión (objeto→objeto_phrases sin WHERE). "
        "Declarar un dueño único o sacarla de morphological_variants."
    )
