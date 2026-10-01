"""What may follow «o/y mayor(es)…» of an amount without modifying a noun (one responsibility).

«300 millones o mayores firmados en 2025» is a bound on the amount followed by a REPORT
about the contracts («firmados…»). «300 millones y mayor plazo» is not: «mayor» modifies
«plazo». The first continuation is a participle or a finite verb; the second one is a noun.

The class is closed and morphological, not a list of examples:

* regular participles ``-ado/-ada/-ados/-adas/-ido/-ida/-idos/-idas`` (firmados, adjudicadas,
  celebrados, vendidos…), minus ``NOUNS_LIKE_PARTICIPLES`` (estado, mercado, grado…: nouns
  that only look like participles and DO take an adjective);
* irregular participles (suscritos, hechos, abiertos…);
* third-person plural preterite ``-aron/-ieron`` (firmaron, suscribieron);
* ``AUXILIARIES`` (se, son, están, sean, hayan…).

Anything else after the suffix (a noun, an article, an adjective such as «vigentes») still
leaves the amount without an operator: it is asked, as in R2.
"""
from __future__ import annotations

import re

NOUNS_LIKE_PARTICIPLES = (
    "estado", "estados", "mercado", "mercados", "grado", "grados", "resultado", "resultados",
    "ganado", "senado", "soldado", "soldados", "abogado", "abogados", "sentido", "partido",
    "partidos", "contenido", "contenidos", "lado", "lados", "cuidado", "condado", "candidato",
    "pasado", "pasados", "dado", "dados", "ahorrado", "ordenado", "salario", "colegiado",
)
IRREGULAR_PARTICIPLES = tuple(
    f"{stem}{ending}"
    for stem in ("suscrit", "hech", "abiert", "escrit", "puest", "vuelt", "dich", "vist",
                 "resuelt", "cubiert", "previst")
    for ending in ("o", "a", "os", "as")
)
# Both spellings of the accented forms (the comparator reader folds accents, the polarity
# scanner does not). «esta» / «este» are left out on purpose: they are demonstratives
# («o mayor este año»).
AUXILIARIES = (
    "se", "son", "sean", "estan", "están", "esten", "estén", "ha", "han", "haya", "hayan",
    "hay", "tiene", "tienen", "tengan", "fue", "fueron",
)

_LETTERS = "a-záéíóúüñ"
_WORD_END = r"(?![a-záéíóúüñ0-9])"


def _words(words) -> str:
    return "|".join(sorted(map(re.escape, words), key=lambda w: (-len(w), w)))


# A word of the class, case-insensitive, accents tolerated (the reader folds, the polarity does not).
EVENT_WORD = (
    rf"(?!(?:{_words(NOUNS_LIKE_PARTICIPLES)}){_WORD_END})"
    rf"(?:[{_LETTERS}]+(?:ad|id)[oa]s?|{_words(IRREGULAR_PARTICIPLES)}|"
    rf"[{_LETTERS}]+(?:aron|ieron)|{_words(AUXILIARIES)}){_WORD_END}"
)
# One space, or a dash / opening bracket / slash around it: «o mayores - firmados», «(firmados)».
_SEPARATOR = r"(?:\s+|\s*[\-–—(\[/]+\s*)"
LOOKAHEAD = rf"{_SEPARATOR}{EVENT_WORD}"

_EVENT_RE = re.compile(rf"^{EVENT_WORD}$", re.IGNORECASE)


def is_event_word(word: str) -> bool:
    return bool(_EVENT_RE.match(word))
