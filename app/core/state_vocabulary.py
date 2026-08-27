"""Reconocimiento léxico del estado — una sola autoridad, ninguna semántica.

Tres cosas distintas se habían mezclado en el mismo sitio:

``RECONOCIMIENTO LÉXICO``
    ¿Este trozo de texto habla de un estado? Es una pregunta sobre la forma
    superficial y no depende del dataset ni del ciclo de vida.

``SEMÁNTICA DE ESTADO``
    ¿A qué familia pertenece? Vive en :mod:`app.core.estado_families`.

``PROYECCIÓN A LA FUENTE``
    ¿Qué campo y qué literales de SECOP? Vive en ``ESTADO_FAMILIES`` y en
    :mod:`app.data.estados`.

Este módulo posee **solo la primera**. Existe porque el reconocimiento estaba
repartido en tres catálogos que divergían: ``ESTADO_SYNONYMS`` (apertura +
contrato, sin ``estado_del_procedimiento``), ``ESTADO_PROCEDIMIENTO_SYNONYMS``
y ``NATURAL_TO_FAMILY``. El binder de negación leía solo el primero, así que
"que no estén **publicados**" no se reconocía como estado y la negación caía a
``OBJECT`` — mientras que "que no estén **abiertos**", que sí estaba en esa
lista, se reconocía a medias y liberaba el token al extractor positivo.

INV-STATE-RECOGNITION-HAS-ONE-AUTHORITY
    Toda capa que necesite saber si un span nombra un estado deriva de aquí.
    El conjunto se **construye** desde los catálogos semánticos: no se escribe
    a mano, así que no puede quedarse atrás cuando uno de ellos crece.

Puro, determinista, sin estado, sin I/O.
"""
from __future__ import annotations

import re

from app.core.estado_families import (
    DATASET_AWARE_FAMILY,
    LEXICAL_EXACT_STATE,
    NATURAL_TO_FAMILY,
)
from app.data.estados import (
    ESTADO_APERTURA_SYNONYMS,
    ESTADO_CONTRATO_SYNONYMS,
    ESTADO_PROCEDIMIENTO_SYNONYMS,
)

__all__ = [
    "STATE_ALTERNATION",
    "STATE_SPAN_RE",
    "STATE_SURFACE_FORMS",
    "find_state_spans",
    "names_state",
]

STATE_SURFACE_FORMS: frozenset[str] = frozenset({
    *NATURAL_TO_FAMILY,
    *LEXICAL_EXACT_STATE,
    *DATASET_AWARE_FAMILY,
    *ESTADO_APERTURA_SYNONYMS,
    *ESTADO_PROCEDIMIENTO_SYNONYMS,
    *ESTADO_CONTRATO_SYNONYMS,
})

# El orden importa: la forma más larga primero, para que "en ejecucion" gane
# sobre "ejecucion" y el span reconocido sea el que el usuario escribió.
STATE_ALTERNATION: str = "|".join(
    sorted((re.escape(form) for form in STATE_SURFACE_FORMS), key=len, reverse=True)
)

STATE_SPAN_RE = re.compile(rf"\b(?:{STATE_ALTERNATION})\b", re.IGNORECASE)


def find_state_spans(text: str) -> tuple[tuple[int, int], ...]:
    """Offsets de cada forma de estado presente en ``text``."""
    return tuple(match.span() for match in STATE_SPAN_RE.finditer(text or ""))


def names_state(token: str) -> bool:
    """True si ``token`` es, íntegro, una forma superficial de estado."""
    return bool(STATE_SPAN_RE.fullmatch((token or "").strip()))
