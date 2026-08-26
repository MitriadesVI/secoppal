"""Autoridad única sobre compradores institucionales genéricos.

INV-INCOMPLETE-BUYER-CANNOT-BECOME-TOPIC (HCA-ENTITY-005).

"contratos de la alcaldía" no nombra un comprador: nombra una CLASE de
comprador. Colombia tiene más de mil alcaldías. Las dos únicas respuestas
honestas son preguntar cuál, o no responder; escoger una es inventar.

El sistema ya defendía la mitad del problema —``EntityResolver.scan_entity``
rechaza el alias desnudo, así que no elegía una entidad arbitraria— pero el
token sobrante caía en la extracción de objeto y terminaba como
``LIKE '%alcaldia%'``. Ese LIKE busca la palabra "alcaldía" en el OBJETO del
contrato, que es una pregunta distinta, y de paso satisface el guard de alcance:
un comprador incompleto se colaba como tema y desbloqueaba la ejecución.

Este módulo reúne el vocabulario que estaba repartido entre
``EntityResolver._GENERIC_ENTITY_TOKENS`` y un literal suelto de ``QueryRouter``,
y añade lo único que faltaba: decidir si una mención está CALIFICADA.

Un genérico calificado nombra una entidad concreta y debe resolverse
normalmente:

    "alcaldía de Medellín"              → comprador válido
    "alcaldía local de Teusaquillo"     → comprador válido
    "gobernación del Atlántico"         → comprador válido

Uno sin calificar no:

    "la alcaldía"                       → ¿cuál?
    "el ministerio"                     → ¿cuál?

Puro, determinista, sin estado y sin I/O.
"""
from __future__ import annotations

import re
import unicodedata

__all__ = [
    "GENERIC_BUYER_TOKENS",
    "build_incomplete_buyer_resolution",
    "detect_incomplete_buyer",
    "incomplete_buyer_hint",
    "is_generic_buyer_token",
]

# Sustantivos que nombran un TIPO de entidad pública, nunca una entidad.
GENERIC_BUYER_TOKENS: frozenset[str] = frozenset({
    "alcaldia",
    "gobernacion",
    "secretaria",
    "municipio",
    "departamento",
    "distrito",
    "instituto",
    "ministerio",
    "agencia",
    "fondo",
    "corporacion",
    "empresa",
    "entidad",
})

_ARTICLES: frozenset[str] = frozenset({"el", "la", "los", "las", "de", "del"})

# Palabras que, siguiendo al genérico, lo CALIFICAN: abren el nombre propio
# ("alcaldía DE Medellín") o son el adjetivo institucional que precede al nombre
# ("alcaldía LOCAL de Teusaquillo"). Cualquier otra cosa —fin de frase, "en
# 2025", "para el aseo"— deja el sustantivo desnudo.
_NAME_QUALIFIER = (
    r"(?:de|del|local|locales|distrital|municipal|departamental|nacional|"
    r"general|mayor|regional|seccional|territorial)"
)

_TOKEN_ALTERNATION = "|".join(sorted(GENERIC_BUYER_TOKENS, key=len, reverse=True))

# La mención tiene que venir introducida por una preposición: es lo que la marca
# como referencia a un comprador y no como el tema del contrato. "contratos de
# dotación para la alcaldía" habla de una alcaldía; "manual de alcaldías" no.
_INCOMPLETE_BUYER_RE = re.compile(
    rf"\b(?:de|del|para|en|por)\s+(?:la\s+|el\s+|los\s+|las\s+)?"
    rf"({_TOKEN_ALTERNATION})\b(?!\s+{_NAME_QUALIFIER}\b)",
    re.IGNORECASE,
)


def _normalize(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    return "".join(char for char in nfkd if not unicodedata.combining(char))


def is_generic_buyer_token(text: str) -> bool:
    """True si ``text`` es un genérico desnudo, con o sin artículo."""
    words = [w for w in _normalize(text).strip().split() if w not in _ARTICLES]
    return len(words) == 1 and words[0] in GENERIC_BUYER_TOKENS


def detect_incomplete_buyer(text: str) -> str | None:
    """Devuelve el genérico sin calificar mencionado en ``text``, si lo hay."""
    match = _INCOMPLETE_BUYER_RE.search(_normalize(text))
    return match.group(1) if match else None


def incomplete_buyer_hint(token: str) -> str:
    """Pregunta que devuelve la decisión a quien puede tomarla."""
    return (
        f"¿A cuál {token} te refieres? Indica la ciudad o el departamento; "
        "no voy a escoger una entidad al azar."
    )


def build_incomplete_buyer_resolution(token: str) -> dict:
    """``entidad_resolution`` que representa la ambigüedad sin resolverla."""
    return {
        "value": None,
        "method": "generic_buyer_unqualified",
        "kind": "buyer_entity",
        "confidence": "none",
        "like_value": None,
        "metadata": {"buyer_mention": token},
        "clarification_needed": True,
        "clarification_hint": incomplete_buyer_hint(token),
    }
