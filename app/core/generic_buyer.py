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
    "department_root_buyer_target",
    "is_territory_fragment_of_buyer",
    "detect_incomplete_buyer",
    "incomplete_buyer_hint",
    "is_generic_buyer_token",
    "is_unqualified_buyer_name",
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

# Una mención departamental raíz tiene dos piezas de autoridad: una cabeza
# institucional y un territorio explícito.  El helper sólo reconoce la
# gramática; EntityResolver valida después que el territorio pertenezca a la
# autoridad geográfica y que el gazetteer tenga ownership raíz inequívoco.
_DEPARTMENT_ROOT_BUYER_RE = re.compile(
    r"^(?:(?:el|la)\s+)?"
    r"(?:gobernacion|departamento|gobierno\s+departamental)"
    r"\s+de(?:l)?\s+(?P<target>.+)$",
)

# Un nombre físico de SECOP puede ser real y, a la vez, no ser una frase de
# búsqueda nacional segura.  "GOBIERNO DEPARTAMENTAL" no incluye el territorio
# que lo convierte en la Gobernación del Tolima; como scan alias desnudo puede
# secuestrar cualquier otro departamento.
_UNQUALIFIED_STRUCTURAL_BUYER_RE = re.compile(
    r"^(?:gobierno|gobernacion)\s+departamental$",
)


def _normalize(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    return "".join(char for char in nfkd if not unicodedata.combining(char))


def is_generic_buyer_token(text: str) -> bool:
    """True si ``text`` es un genérico desnudo, con o sin artículo."""
    words = [w for w in _normalize(text).strip().split() if w not in _ARTICLES]
    return len(words) == 1 and words[0] in GENERIC_BUYER_TOKENS


def is_unqualified_buyer_name(text: str) -> bool:
    """True si el nombre sólo expresa una clase institucional sin identidad."""
    normalized = _normalize(text).strip()
    return (
        is_generic_buyer_token(normalized)
        or bool(_UNQUALIFIED_STRUCTURAL_BUYER_RE.fullmatch(normalized))
    )


def department_root_buyer_target(text: str) -> str | None:
    """Extrae X de una mención raíz ``gobernación/departamento de X``.

    También reconoce ``gobierno departamental de/del X``.  No decide si X es
    un departamento ni quién lo representa: esas autoridades viven en el
    catálogo territorial y en el ownership del gazetteer, respectivamente.
    """
    match = _DEPARTMENT_ROOT_BUYER_RE.fullmatch(_normalize(text).strip())
    if not match:
        return None
    target = match.group("target").strip()
    return target or None


def is_territory_fragment_of_buyer(scanned: str, buyer_department: str) -> bool:
    """True si ``scanned`` es un trozo del territorio que el comprador ya nombró.

    La gramática de comprador corta la captura en " de/del ": de
    "gobernación del Valle del Cauca" sólo consume "gobernación del Valle" y
    deja "del Cauca" en el texto. El scan territorial lo lee después como
    *Cauca* — un departamento que el usuario nunca nombró — y el WHERE termina
    filtrando por el territorio equivocado. Lo mismo con "Norte de Santander",
    cuya cola se lee como *Santander*.

    No es un filtro: es la cola de una frase ya consumida. Se exige que el
    departamento escaneado sea parte estricta del territorio del comprador,
    para no descartar un territorio genuinamente distinto ("la gobernación del
    Cauca en Antioquia"), que sí es una intersección legítima.
    """
    if not scanned or not buyer_department:
        return False
    scanned_key = _normalize(scanned).strip()
    buyer_key = _normalize(buyer_department).strip()
    if not scanned_key or scanned_key == buyer_key:
        return False
    buyer_tokens = buyer_key.split()
    scanned_tokens = scanned_key.split()
    if len(scanned_tokens) >= len(buyer_tokens):
        return False
    return any(
        buyer_tokens[i:i + len(scanned_tokens)] == scanned_tokens
        for i in range(len(buyer_tokens) - len(scanned_tokens) + 1)
    )


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
