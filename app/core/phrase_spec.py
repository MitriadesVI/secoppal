"""Contrato de datos para frases contractuales (`objeto_phrases`).

`PhraseSpec` representa una colocación de dominio que debe sobrevivir intacta
desde la comprensión de la consulta hasta el retrieval y el ranking — p.ej.
"manual de contratación", que el extractor actual parte en tokens sueltos y
pierde ("contratación" desaparece).

Es DELIBERADAMENTE un contrato de datos inmutable y genérico, no un "dios
semántico": NO contiene generación de SoQL, lectura de datasets, ranking, regex
compiladas, detección desde la consulta, ni lógica específica de ningún concepto.
Esas responsabilidades viven en otros módulos (Commits 3-6).

Punto de inserción futuro (Commit 3, NO tocar aún): el extractor
`QueryRouter._extract_object_terms` (app/core/query_router.py:1125) ya tiene un
mecanismo de frases vía `KNOWN_BIGRAMS` (línea ~1144) que corre ANTES del scrub de
stopwords (línea ~1170). Allí se registrará el `canonical`/`variants` de cada
PhraseSpec para capturar la colocación como unidad antes de que "de" se descarte y
la frase se pierda. Este commit solo define la representación.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PhraseSpec:
    """Una frase contractual y sus variantes.

    Inmutable (frozen) ⇒ hashable y serializable. Los campos de colección son
    tuplas, no listas, para preservar inmutabilidad e igualdad por valor.

    Attributes:
        canonical: forma canónica normalizada, p.ej. "manual de contratacion".
        ordered_anchors: anclas que deben aparecer en orden, p.ej.
            ("manual", "contratacion").
        variants: otras superficies aceptadas, p.ej. ("manuales de contratacion",).
        max_gap_tokens: tokens intermedios permitidos entre anclas consecutivas
            (cubre "manual interno de contratacion").
    """

    canonical: str
    ordered_anchors: tuple[str, ...]
    variants: tuple[str, ...] = field(default_factory=tuple)
    max_gap_tokens: int = 2
