"""Compatibilidad entre el tipo de AP1 y el tipo que publica la fuente.

Por qué existe
--------------
``IdentityKey`` es ``(document_type, base_number)``: AP1 declaró que dos
tipos documentales distintos son dos actores. Si la ejecución filtra solo
por el número, esa segunda dimensión no llega a donde importa y la llave
queda decorativa. Verificado en la fuente: el documento ``13848603``
devuelve 192 filas de una persona natural y 1 de una unión temporal, y el
mismo SoQL las trae todas.

Excluir lo incompatible, no exigir lo idéntico
----------------------------------------------
La tentación es filtrar ``tipodocproveedor = 'NIT'`` para una identidad
NIT. Los datos lo desaconsejan: de los 4.230 contratos del NIT
860.002.400, **cuatro** están publicados con tipo ``"Otro"`` —uniones
temporales que la entidad clasificó así—, y en el universo completo hay
10.073 filas con documento utilizable y tipo indeterminado. Un filtro de
igualdad las borraría, cambiando falsas atribuciones por omisiones
silenciosas.

Así que se excluye únicamente lo que **contradice** la identidad: una
cédula de ciudadanía declarada no puede ser el NIT de una empresa, y
viceversa. ``"No Definido"`` y ``"Otro"`` se conservan siempre — son
indeterminados, no contradictorios, y tratar la indeterminación como
evidencia es exactamente lo que AP1 prohíbe.

Lo que este filtro NO resuelve
------------------------------
La fuente también clasifica mal. De las 192 filas con tipo ``"Cédula de
Ciudadanía"`` bajo ``13848603``, una es ``"UNION TEMPORAL DE RECARGAS
2022"``. Ningún filtro por tipo la separa, porque el error ya está en el
dato publicado. Por eso el filtro **no sustituye** la declaración de
nombres observados ni la advertencia de tipos mezclados: son dos defensas
para dos fallas distintas.

Puro: sin red, sin reloj, sin configuración.
"""
from __future__ import annotations

import unicodedata

__all__ = [
    "ORGANIZATION_SOURCE_TYPES",
    "PERSONAL_SOURCE_TYPES",
    "UNDETERMINED_SOURCE_TYPES",
    "incompatible_source_types",
    "is_personal_source_type",
]

# Valores REALES de ``tipodocproveedor`` en jbjy-vk9h, verificados el
# 2026-08-18 (ver tests/fixtures/actor_sources/). No son un vocabulario
# inventado: son las diez cadenas que la fuente publica.
PERSONAL_SOURCE_TYPES: tuple[str, ...] = (
    "Cédula de Ciudadanía",
    "Cédula de Extranjería",
    "Tarjeta de Identidad",
    "Pasaporte",
    "Registro Civil",
    "Permiso por Protección Temporal",
    "Permiso especial de permanencia",
)

ORGANIZATION_SOURCE_TYPES: tuple[str, ...] = ("NIT",)

# Ni afirman ni niegan nada sobre la clase de actor. Nunca se excluyen.
UNDETERMINED_SOURCE_TYPES: tuple[str, ...] = ("No Definido", "Otro")

# Los tipos de AP1, con las escrituras que la fuente usa para lo mismo.
_PERSONAL_IDENTITY_TYPES = frozenset({"CC", "CE", "TI"})


def is_personal_source_type(value: str | None) -> bool:
    """¿Ese valor designa el documento de una persona natural?

    Acepta tanto el vocabulario de AP1 (``"CC"``) como el de la fuente
    (``"Cédula de Ciudadanía"``), porque el perfil ve ambos: uno viene de
    lo que el usuario escribió, el otro de lo que la fuente publicó.
    """
    if not value:
        return False
    candidate = value.strip()
    if candidate in _PERSONAL_IDENTITY_TYPES:
        return True
    folded = _fold(candidate)
    return any(_fold(known) == folded for known in PERSONAL_SOURCE_TYPES)


def _fold(value: str) -> str:
    """Sin tildes y en minúscula.

    La misma cadena llega acentuada desde la fuente ("Cédula de Ciudadanía")
    y sin acentos desde el texto ya normalizado del parser. Compararlas
    literalmente haría que media identidad personal no se reconociera como
    tal y su documento se mostrara completo en pantalla.
    """
    decomposed = unicodedata.normalize("NFD", value.casefold())
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def incompatible_source_types(document_type: str | None) -> tuple[str, ...]:
    """Valores de ``tipodocproveedor`` que CONTRADICEN esa identidad.

    Devuelve una tupla vacía cuando el tipo es desconocido: sin evidencia
    de qué clase de actor es, nada puede declararse incompatible. Ese caso
    se resuelve observando qué tipos vuelven y declarando la ambigüedad,
    no recortando el universo a ciegas.
    """
    if not document_type:
        return ()
    candidate = document_type.strip().upper()
    if candidate == "NIT":
        return PERSONAL_SOURCE_TYPES
    if candidate in _PERSONAL_IDENTITY_TYPES:
        return ORGANIZATION_SOURCE_TYPES
    return ()
