"""Impide que el fuzzy entregue un dueño que perdió lo que el usuario pidió.

``resolve_entidad`` tiene dos autoridades distintas.  El alias exacto **acredita**
propiedad: el gazetteer afirma que esa frase nombra a ese comprador.  Los tiers
de fuzzy sólo **infieren** parecido léxico, y ``WRatio`` puntúa 90 la contención
parcial: un alias corto dentro de una frase larga, o una frase corta dentro de un
alias largo, marcan igual.

La segunda dirección es legítima —"Banrep" es una abreviatura de su nombre—.
La primera no: cuando el candidato deja fuera la cabeza institucional o el
territorio que el usuario escribió, el resultado ya no es la entidad pedida.
Así "Presidencia de la República" terminaba en el Banco por el fragmento
`la república`, y "Contraloría Departamental de Bolívar" en la Contraloría
General por el fragmento `contraloría` —o en una gobernación por el fragmento
`bolívar`, según qué vecino ganara—.

INV-NO-SILENT-SEMANTIC-FALLBACK (ADR-069): el defecto no es el hueco del
gazetteer, es rellenarlo en silencio.  Sin candidato que conserve la identidad
pedida el resolver cae al filtro literal con las palabras del usuario, que no
atribuye contratos a nadie.

El módulo no conoce instituciones ni topónimos: recibe el catálogo territorial
de quien lo llama y opera sobre tokens.
"""

from __future__ import annotations

from collections.abc import Container

from app.core.entity_types import name_tokens

# Conectores sin contenido identitario.  Misma frontera que usan
# ``geography_alias.LEADING_WRAPPERS`` y ``generic_buyer._ARTICLES``.
FILLER_TOKENS: frozenset[str] = frozenset(
    {"el", "la", "los", "las", "de", "del", "en", "al", "y", "e"}
)


def requested_identity_tokens(query: str) -> list[str]:
    """Tokens con contenido de la frase pedida, en orden."""
    return [token for token in name_tokens(query) if token not in FILLER_TOKENS]


def candidate_keeps_requested_identity(
    query: str,
    matched_alias: str,
    official_name: str,
    territory_tokens: Container[str],
) -> bool:
    """Si el candidato conserva la cabeza y el territorio que el usuario pidió.

    ``matched_alias`` y ``official_name`` cuentan juntos como evidencia: una
    ficha puede llevar la cabeza en el alias y el territorio en el nombre
    físico, y ninguna de las dos formas es más autoridad que la otra.
    """
    requested = requested_identity_tokens(query)
    if not requested:
        return True

    evidence = set(name_tokens(matched_alias)) | set(name_tokens(official_name))

    # La cabeza es lo primero que el usuario escribió: "contraloría", "hospital",
    # "presidencia".  Un candidato que no la lleva nombra otra cosa.
    if requested[0] not in evidence:
        return False

    # Un territorio explícito es discriminante, no decorado: la contraloría de
    # Bolívar y la de Caldas son compradores distintos con NIT distinto.
    return all(
        token in evidence for token in requested if token in territory_tokens
    )
