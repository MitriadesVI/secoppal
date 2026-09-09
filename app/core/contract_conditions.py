"""Qué condiciones contractuales pide una consulta, con propiedad de ocurrencias.

Este módulo hace una sola cosa: enumerar las condiciones contractuales que el
usuario **pidió** —los estados excluidos y los admitidos o celebrados— y decir
si esa composición puede representarse con el adaptador vigente, que transporta
un único ``estado_contrato`` dentro de un solo universo. No elige universo, no
construye SQL y no interpreta tema, comprador ni periodo.

Existe porque la detección de composiciones vivía dispersa en tres reglas
locales de ``contract_universe_intent``: el estado que el patrón capturaba, la
cola inmediatamente posterior al match, y el marcador de celebración que
siguiera a una palabra de fuente reconocida. Ninguna de las tres mira el
**prefijo** de la petición, y ``split_clauses`` parte por ``y``, así que en

    registros celebrados y cancelados de la Alcaldía de Cartagena

``celebrados`` quedaba en una cláusula que nadie leía: se ejecutaba sólo
``Cancelado``. Invertir los dos estados cambiaba la respuesta, que es
exactamente lo que el contrato prohíbe.

La lección de ``clauses.py`` sigue en pie y este módulo no la contradice: no se
busca una palabra en TODA la frase para atribuirle significado. Cada condición
se acredita dentro de **su** cláusula y sólo si lo que la precede allí es
lenguaje funcional —artículos, el sustantivo de la petición, otra condición ya
acreditada—. Una palabra dentro de un objeto contractual o de un nombre propio
queda descartada por esa misma regla, no por una excepción por frase:

    contratos de elaboración de borradores de documentos …   -> 0 condiciones
    qué ha contratado FUNDACION BORRADORES                   -> 0 condiciones
    registros cancelados de obras terminadas …               -> 1 condición

Enumerar es todo lo que hace. Quien decide qué hacer con dos condiciones
incompatibles es ``contract_universe_intent``.
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from app.core.entity_profile.clauses import split_clauses
from app.core.query_text_consumption import mask_consumed_spans
from app.core.source_occurrence import _is_request_framing

# Los cuatro estados que el universo celebrado excluye, en la misma superficie
# léxica que ya usa el adaptador de peticiones explícitas de registros.
EXCLUDED_CONDITION = (
    r"cancelad[oa]s?|(?:en\s+)?borrador(?:es)?|enviad[oa]s?\s+(?:al\s+)?proveedor"
    r"|en\s+aprobacion"
)
# Los siete estados posteriores, Aprobado, y los adjetivos con que un usuario
# nombra la celebración. `en aprobacion` se distingue de `aprobados` por el
# límite de palabra: son cadenas distintas, no una variante de la otra.
ADMITTED_CONDITION = (
    r"celebrad[oa]s?|firmad[oa]s?|suscrit[oa]s?|en\s+ejecucion|cerrad[oa]s?"
    r"|modificad[oa]s?|terminad[oa]s?|cedid[oa]s?|suspendid[oa]s?"
    r"|prorrogad[oa]s?|aprobad[oa]s?"
)
_CONDITION = re.compile(rf"\b(?P<excluded>{EXCLUDED_CONDITION})\b"
                        rf"|\b(?P<admitted>{ADMITTED_CONDITION})\b")

# `celebrados`, `firmados` y `suscritos` no nombran un estado de Socrata: nombran
# el universo entero. Se distinguen de los estados concretos porque el universo
# ya viaja en el predicado, mientras que un estado necesita el filtro.
CELEBRATED_UNIVERSE = '*universo celebrado*'
_DENOTES = (
    (re.compile(r"^(?:celebrad|firmad|suscrit)"), CELEBRATED_UNIVERSE),
    (re.compile(r"^cancelad"), 'Cancelado'),
    (re.compile(r"^(?:en\s+)?borrador"), 'Borrador'),
    (re.compile(r"^enviad"), 'enviado Proveedor'),
    (re.compile(r"^en\s+aprobacion"), 'En aprobación'),
    (re.compile(r"^en\s+ejecucion"), 'En ejecución'),
    (re.compile(r"^cerrad"), 'Cerrado'),
    (re.compile(r"^modificad"), 'Modificado'),
    (re.compile(r"^terminad"), 'terminado'),
    (re.compile(r"^cedid"), 'cedido'),
    (re.compile(r"^suspendid"), 'Suspendido'),
    (re.compile(r"^prorrogad"), 'Prorrogado'),
    (re.compile(r"^aprobad"), 'Aprobado'),
)


def denotes(surface: str) -> str:
    """Qué nombra una superficie: un literal de estado, o el universo celebrado."""
    for pattern, value in _DENOTES:
        if pattern.match(surface):
            return value
    raise ValueError(f'superficie sin denotación declarada: {surface!r}')
# El sustantivo con que se nombra la petición no es tema: precede a la condición
# sin convertirla en objeto contractual.
_REQUEST_NOUN = re.compile(r"\b(?:registros?|contratos?)\b")
# La sintaxis de rol Actor tiene precedencia: lo que sigue es una identidad.
_ACTOR_LEAD = re.compile(r"\b(?:ha contratado|proveedor|contratista)\s*$")


@dataclass(frozen=True, slots=True)
class ContractCondition:
    """Una condición acreditada, con su span sobre el texto normalizado."""

    span: tuple[int, int]
    surface: str
    admitted: bool


@dataclass(frozen=True, slots=True)
class ConditionComposition:
    """Las condiciones pedidas y si el adaptador vigente puede transportarlas."""

    conditions: tuple[ContractCondition, ...] = ()
    lead: tuple[int, int] | None = None

    @property
    def excluded(self) -> tuple[ContractCondition, ...]:
        return tuple(c for c in self.conditions if not c.admitted)

    @property
    def admitted(self) -> tuple[ContractCondition, ...]:
        return tuple(c for c in self.conditions if c.admitted)

    @property
    def denoted(self) -> frozenset[str]:
        """Lo que la petición nombra, ya sin variantes de superficie."""
        return frozenset(denotes(condition.surface) for condition in self.conditions)

    @property
    def representable(self) -> bool:
        """Un solo ``estado_contrato`` dentro de un solo universo.

        Dos estados distintos —excluidos o admitidos, da igual— no caben en el
        filtro singular que producen los adaptadores vigentes: ``UniverseRequest``
        transporta un ``state`` escalar y ``detect_estado_family`` declara que
        «multiple matches are not allowed». El transporte SQL sí sabe emitir
        ``estado_contrato IN (...)``, pero ningún productor construye esa unión a
        partir de dos condiciones sueltas, y fabricarla sería una gramática nueva.
        Mientras no exista esa autoridad, la composición se aclara.

        El universo celebrado no cuenta como estado: ya viaja en el predicado, de
        modo que ``celebrados y suspendidos`` conserva ambas condiciones. Lo que
        no puede convivir con él es un estado excluido: eso cruza la frontera.
        """
        states = {value for value in self.denoted if value != CELEBRATED_UNIVERSE}
        if len(states) > 1:
            return False
        excluded = states & {denotes(c.surface) for c in self.excluded}
        return not (excluded and CELEBRATED_UNIVERSE in self.denoted)

    @property
    def materializable(self) -> bool:
        """Composing cannot outrun the current producer's contractual filter.

        INV-CONDITION-REPRESENTABLE-IMPLIES-MATERIALIZABLE: membership in the
        celebrated catalog is not evidence of a filter. Ask the same resolver
        the ordinary producer uses, requiring an effective contractual filter
        containing the denoted state. A pre-existing family is not a union
        invented from two conditions. Explicit excluded states remain owned by the
        record adapter. Single-condition lexical gaps are a separate lane.
        """
        from app.core.estado_families import resolve_estado

        if len(self.conditions) < 2:
            return True
        for condition in self.admitted:
            value = denotes(condition.surface)
            if value == CELEBRATED_UNIVERSE:
                continue
            resolution = resolve_estado(condition.surface, 'contratos')
            if not resolution or value not in resolution['filters'].get('estado_contrato', ()):
                return False
        return True

    def control_is_contiguous(self, text: str) -> bool:
        """A successful scalar control cannot swallow intervening object text."""
        if self.span is None:
            return False
        start, end = self.span
        control = text[start:end]
        spans = [(c.span[0] - start, c.span[1] - start) for c in self.conditions]
        spans.extend(m.span() for m in _REQUEST_NOUN.finditer(control))
        # General request framing also admits month/current-state words. Those
        # retain their own pipeline spans; only bare joining syntax is erasable.
        remainder = mask_consumed_spans(control, spans)
        return re.fullmatch(r'(?:\s|[,;:]|\b(?:y|e|o|el|la|los|las)\b)*', remainder) is not None

    @property
    def span(self) -> tuple[int, int] | None:
        """El tramo que cubre la composición completa, para consumir el control.

        Incluye el sustantivo que la introduce —``registros``, ``contratos``—
        cuando lo precede: si no, esa palabra sobrevive como tema residual de
        una petición que ni siquiera se ejecutó.
        """
        if not self.conditions:
            return None
        start = min(c.span[0] for c in self.conditions)
        if self.lead is not None:
            start = min(start, self.lead[0])
        return (start, max(c.span[1] for c in self.conditions))


def read_conditions(
    text: str,
    *,
    occurrences: tuple = (),
    protected: tuple[tuple[int, int], ...] = (),
) -> ConditionComposition:
    """Condiciones contractuales acreditadas en ``text`` (ya normalizado).

    ``occurrences`` son las palabras de fuente que ``source_occurrence`` ya
    acreditó; ``protected`` son los spans de nombres propios que no pueden
    reinterpretarse. Ambas se consumen del prefijo, no se usan como excepciones.
    """
    accepted: list[ContractCondition] = []
    lead: tuple[int, int] | None = None
    for clause in split_clauses(text):
        consumed: list[tuple[int, int]] = []
        for match in _CONDITION.finditer(clause.text):
            start = clause.start + match.start()
            end = clause.start + match.end()
            if any(a < end and start < b for a, b in protected):
                continue
            prefix = clause.text[:match.start()]
            if _ACTOR_LEAD.search(prefix):
                continue
            if not _is_request_framing(_consume(prefix, clause, occurrences, consumed)):
                continue
            if not accepted:
                nouns = list(_REQUEST_NOUN.finditer(prefix))
                if nouns:
                    lead = (clause.start + nouns[0].start(), clause.start + nouns[0].end())
            accepted.append(ContractCondition(
                span=(start, end), surface=match.group(0),
                admitted=match.group("admitted") is not None,
            ))
            consumed.append(match.span())
    return ConditionComposition(tuple(accepted), lead)


def _consume(prefix: str, clause, occurrences, consumed: list) -> str:
    """Quita del prefijo lo que ya tiene dueño: fuente, sustantivo, condiciones."""
    spans = [span for span in consumed if span[1] <= len(prefix)]
    spans.extend((match.start(), match.end())
                 for match in _REQUEST_NOUN.finditer(prefix))
    spans.extend((occurrence.span[0] - clause.start, occurrence.span[1] - clause.start)
                 for occurrence in occurrences
                 if clause.start <= occurrence.span[0]
                 and occurrence.span[1] - clause.start <= len(prefix))
    return mask_consumed_spans(prefix, spans)
