"""Distingue un topónimo de una institución cuyo nombre termina en topónimo.

`_is_unqualified_dept` decidía esto con ``alias.endswith(clave)``, sin frontera
de palabra. *corpocaldas* "termina en" Caldas, y con él quedaban descartados
dos mil aliases institucionales legítimos — *Acueducto de Bogotá*, *ICCU de
Cundinamarca*, *Canal Teleantioquia* — porque su última sílaba o su último
token coincidía con un departamento (deuda D6, R2D §7).

La autoridad correcta no es el sufijo sino la **pertenencia**: un alias es
geografía cuando él mismo, o lo que queda tras retirar wrappers *iniciales*,
es una clave del catálogo territorial. Sólo el inicio se retira: ahí está la
diferencia entre "el Cauca" —territorio— y "Universidad del Cauca", donde el
mismo token final pertenece al nombre de la institución.

La igualdad estricta no basta y quedó refutada con evidencia: "el Cauca", "el
Valle del Cauca", "el Distrito Capital de Bogotá" y "el Arauca" no son claves
del catálogo, y sin el despojo inicial resolvían a la Universidad del Cauca, a
la Universidad Pedagógica Nacional y a compradores departamentales.
"""
from __future__ import annotations

from app.data.departamentos import DEPARTAMENTOS

# Se retiran únicamente en cabeza del alias. Ningún token interno o final se
# toca: "de bogota" al final es parte del nombre, no un envoltorio.
LEADING_WRAPPERS: frozenset[str] = frozenset(
    {"el", "la", "los", "las", "de", "del", "en", "al"}
)


def geography_forms(alias: str) -> list[str]:
    """El alias y cada forma que resulta de retirar wrappers iniciales.

    "en el meta" → ["en el meta", "el meta", "meta"]. La forma original se
    conserva siempre: hay claves del catálogo que ya incluyen su envoltorio
    ("del atlantico", "departamento del atlantico").
    """
    forms = [alias]
    words = alias.split()
    while words and words[0] in LEADING_WRAPPERS:
        words = words[1:]
        forms.append(" ".join(words))
    return [form for form in forms if form]


def is_geography_alias(alias: str) -> bool:
    """True si el alias nombra territorio, no una institución.

    Sin substring y sin sufijo textual: pertenencia exacta al catálogo, sobre
    el alias o sobre su forma despojada de envoltorios iniciales.
    """
    return any(form in DEPARTAMENTOS for form in geography_forms(alias.strip()))
