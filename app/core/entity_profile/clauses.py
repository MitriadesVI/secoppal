"""Partir la petición en cláusulas coordinadas, con offsets.

La revisión independiente mostró que tres hallazgos distintos —F1, F2 y F3—
eran el mismo defecto: buscar una palabra en TODA la frase y atribuirle un
significado que en realidad pertenece a una cláusula concreta.

    "contratos terminados en agosto y actualmente activos"

`_CURRENT_MARKER.search(texto)` encontraba `actualmente` en la segunda cláusula
y con eso desactivaba la lectura de `terminados`, que vive en la primera. El
resultado era ejecutar cuatro consultas por fecha de firma sin decir nunca que
se había pedido terminación.

    "contratos de gestion de procesos industriales"

`procesos` aparece dentro del TEMA y elegía una fuente.

Este módulo hace una sola cosa: dice dónde empieza y termina cada cláusula. No
interpreta. Quienes interpretan —``temporal_event`` y ``source_request``—
consumen ESTA partición, de modo que no existan dos escáneres reclamando la
misma ocurrencia.

Se coordina por ``y``/``e``/``,``/``;`` y deliberadamente **no** por ``o``: la
disyunción es lenguaje temático ("mantenimiento o vías") y partir por ella
rompería un tema en dos cláusulas falsas.
"""
from __future__ import annotations

from dataclasses import dataclass
import re


_COORDINATION = re.compile(r"\s+(?:y|e)\s+|\s*[,;]\s*")


@dataclass(frozen=True, slots=True)
class Clause:
    """Un tramo de la petición, con sus offsets sobre el texto normalizado."""

    text: str
    start: int
    end: int

    def owns(self, span: tuple[int, int]) -> bool:
        """¿Esta ocurrencia pertenece a esta cláusula?"""
        return self.start <= span[0] and span[1] <= self.end


def split_clauses(text: str) -> tuple[Clause, ...]:
    """Cláusulas no vacías en orden, cubriendo el texto sin solaparse."""
    clauses: list[Clause] = []
    cursor = 0
    for match in _COORDINATION.finditer(text):
        clauses.append(Clause(text[cursor:match.start()], cursor, match.start()))
        cursor = match.end()
    clauses.append(Clause(text[cursor:], cursor, len(text)))
    return tuple(clause for clause in clauses if clause.text.strip())


def clause_of(clauses: tuple[Clause, ...], span: tuple[int, int]) -> Clause | None:
    """La cláusula dueña de una ocurrencia, si alguna la contiene entera."""
    for clause in clauses:
        if clause.owns(span):
            return clause
    return None
