"""Qué territorio nombra una petición, como frase y no como tokens sueltos.

El catálogo territorial de este proyecto está indexado por FRASE —`norte de
santander`, `san andres y providencia`, `puerto carreño`, `distrito capital de
bogota`—, pero un consumidor que sólo mire tokens sueltos no ve ninguna de
ellas: `norte`, `san`, `puerto` y `capital` no son claves por separado. Peor
que no verlas: `norte de santander` queda reducido a `santander`, que es otro
departamento.

Volcar todos los tokens de todas las claves en un conjunto plano tampoco sirve.
Convierte `departamento`, `distrito`, `san`, `santa`, `norte` y `capital` en
palabras territoriales globales, y esas son vocabulario institucional corriente
—99 de las 591 fichas del gazetteer las llevan en su nombre—.

Este índice conserva la frase. Reconoce el tramo territorial más largo que la
petición contiene y enumera las formas que nombran ese mismo territorio
canónico, para que `del valle` y `valle del cauca`, o `bogotá distrito capital`
y `distrito capital de bogotá`, cuenten como la misma identidad.

No define geografía: recibe los catálogos que ya son autoridad
(`DEPARTAMENTOS`, `GAZETTEER_CIUDADES`) y el conjunto de conectores de quien lo
construye.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from app.core.entity_types import name_tokens

Span = tuple[str, ...]


def contains_span(sequence: Sequence[str], span: Span) -> bool:
    """True si ``span`` aparece en ``sequence`` como tramo contiguo."""
    width = len(span)
    if not width or width > len(sequence):
        return False
    return any(
        tuple(sequence[i:i + width]) == span
        for i in range(len(sequence) - width + 1)
    )


class TerritoryPhraseIndex:
    """Frases territoriales y sus equivalentes, sobre tokens sin conectores."""

    def __init__(
        self,
        canonicals_by_span: Mapping[Span, frozenset[str]],
        spans_by_canonical: Mapping[str, frozenset[Span]],
    ) -> None:
        self._canonicals_by_span = dict(canonicals_by_span)
        self._spans_by_canonical = dict(spans_by_canonical)
        self._max_span = max((len(span) for span in self._canonicals_by_span), default=0)

    @classmethod
    def from_alias_maps(
        cls,
        *alias_maps: Mapping[str, str],
        connectors: Iterable[str] = (),
    ) -> TerritoryPhraseIndex:
        """Construye el índice desde catálogos ``alias → territorio canónico``."""
        skip = frozenset(connectors)
        entries = [
            (name_tokens(alias), canonical)
            for alias_map in alias_maps
            for alias, canonical in alias_map.items()
        ]
        # Claves que ya nombran el territorio sin ayuda de conectores.  Sirven
        # de aval: retirar el artículo inicial no puede INVENTAR una palabra
        # territorial que el catálogo no reconozca sola.  Sin este aval, "la
        # capital" fabricaba el topónimo `capital`, que es vocabulario
        # institucional —CANAL CAPITAL, SENA REGIONAL DISTRITO CAPITAL—.
        bare = {
            tuple(tokens) for tokens, _ in entries
            if tokens and not any(t in skip for t in tokens)
        }
        canonicals: dict[Span, set[str]] = {}
        for tokens, canonical in entries:
            span = tuple(t for t in tokens if t not in skip)
            if not span:
                continue
            if len(span) == 1 and span not in bare:
                continue
            canonicals.setdefault(span, set()).add(canonical)
        spans: dict[str, set[Span]] = {}
        for span, names in canonicals.items():
            for canonical in names:
                spans.setdefault(canonical, set()).add(span)
        return cls(
            {span: frozenset(names) for span, names in canonicals.items()},
            {canonical: frozenset(found) for canonical, found in spans.items()},
        )

    def spans_in(self, tokens: Sequence[str]) -> list[Span]:
        """Tramos territoriales de ``tokens``: el más largo gana, sin solapar.

        "norte de santander" se reconoce entero; el `santander` que contiene no
        se reporta aparte, porque el usuario no pidió ese departamento.
        """
        found: list[Span] = []
        index = 0
        while index < len(tokens):
            for width in range(min(self._max_span, len(tokens) - index), 0, -1):
                span = tuple(tokens[index:index + width])
                if span in self._canonicals_by_span:
                    found.append(span)
                    index += width
                    break
            else:
                index += 1
        return found

    def equivalent_forms(self, span: Span) -> frozenset[Span]:
        """Formas que nombran el mismo territorio canónico y comparten nombre.

        Nombrar el mismo territorio no basta para acreditar identidad. El
        catálogo incluye atajos conversacionales que sirven para filtrar y no
        para nombrar: `cucuta` resuelve a Norte de Santander y `la capital` al
        Distrito Capital. Tomarlos como evidencia dejaría que un comprador de
        Cúcuta acreditara una petición por el departamento, o que cualquier
        nombre con la palabra "capital" acreditara una por Bogotá.

        El filtro es compartir al menos un token con el NOMBRE del territorio
        canónico: `bogotá` y `distrito capital` valen los dos por el Distrito
        Capital de Bogotá, y `valle` por Valle del Cauca, pero `cucuta` deja de
        valer por Norte de Santander y `cundi` por Cundinamarca. El propio
        ``span`` siempre cuenta.

        Medirlo contra el canónico y no contra la frase pedida es deliberado:
        "contraloría del distrito capital" y "contraloría de Bogotá" nombran al
        mismo comprador aunque sus dos formas no compartan una sola palabra.
        """
        shared: set[Span] = {span}
        for canonical in self._canonicals_by_span.get(span, ()):
            canonical_tokens = set(name_tokens(canonical))
            for form in self._spans_by_canonical.get(canonical, frozenset()):
                if canonical_tokens & set(form):
                    shared.add(form)
        return frozenset(shared)
