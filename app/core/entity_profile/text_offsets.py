"""Un mapa inequívoco del texto normalizado al texto que la persona escribió.

Las autoridades de este sistema trabajan sobre ``normalize_text`` —NFKD, sin
marcas diacríticas, en minúsculas y con los extremos recortados—. Los spans que
producen son offsets de ESE texto, no del original.

Casi siempre coinciden, y por eso el defecto es difícil de ver: en una cadena
sin acentos ni ligaduras ambos índices son el mismo número. Basta un acento ya
descompuesto, un espacio inicial o una ligadura tipográfica para que dejen de
serlo, y entonces un span "sobre el texto original" señala otra palabra.

El diseño prohíbe reconstruir la posición con ``text.find(term)``: la primera
coincidencia no es la ocurrencia, y con dos ocurrencias iguales elige la que no
es. Aquí se construye el mapa mientras se normaliza, carácter por carácter.

Si la normalización carácter a carácter no reprodujera exactamente la de la
autoridad —``lower()`` no es siempre distributivo, p. ej. la sigma final
griega—, el mapa se declara NO fiel. Un consumidor con ``faithful=False`` no
publica spans: deja la ocurrencia sin resolver en vez de señalar mal.
"""
from __future__ import annotations

from dataclasses import dataclass
import unicodedata

from app.utils.money import normalize_text


@dataclass(frozen=True, slots=True)
class NormalizedText:
    """El texto normalizado, su original y el mapa entre los dos."""

    original: str
    text: str
    # offsets[i] es el índice en `original` del carácter que produjo text[i];
    # tiene un elemento final para poder cerrar un span en el borde derecho.
    offsets: tuple[int, ...]
    faithful: bool

    def to_original(self, span: tuple[int, int]) -> tuple[int, int] | None:
        """El mismo tramo, expresado sobre el texto original."""
        start, end = span
        if not self.faithful:
            return None
        if not 0 <= start < end <= len(self.text):
            return None
        return (self.offsets[start], self.offsets[end])


def normalize_with_offsets(original: str) -> NormalizedText:
    """Reproduce ``normalize_text`` conservando la procedencia de cada carácter."""
    chars: list[str] = []
    sources: list[int] = []
    for index, char in enumerate(original):
        for decomposed in unicodedata.normalize("NFKD", char):
            if unicodedata.combining(decomposed):
                continue
            for lowered in decomposed.lower():
                chars.append(lowered)
                sources.append(index)

    start, end = 0, len(chars)
    while start < end and chars[start].isspace():
        start += 1
    while end > start and chars[end - 1].isspace():
        end -= 1
    chars = chars[start:end]
    sources = sources[start:end]

    text = "".join(chars)
    # El borde derecho de un span apunta al carácter siguiente al último; sin
    # este cierre no se podría expresar un tramo que termina en el final.
    offsets = tuple(sources) + (
        sources[-1] + 1 if sources else len(original),
    )
    return NormalizedText(
        original=original,
        text=text,
        offsets=offsets,
        faithful=text == normalize_text(original),
    )
