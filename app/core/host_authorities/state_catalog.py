"""Lexicon and per-dataset literal table of the STATE authority (M1-D-STATE/MOD, contract C1.1).

One reading per state phrase: the canonical class (``STATE_CLASSES``), the operator an honest cite of
that phrase carries (C4: ``family`` only for the open family, ``is`` for every exact state) and the
``(column, values)`` pairs it filters on in each dataset (AND between columns, OR within values).

The literals are NOT retyped here: they are read from the lexicons that already own them
(``app.data.estados`` and ``ESTADO_FAMILIES``) by lemma, so the four catalogs of A2 cannot drift
apart silently; ``tests/test_host_authority_state.py`` enumerates this table against them.

A phrase the table does not know is not a state; a phrase it knows but SECOP has no literal for
(``liquidado``, ``firmado``…) is ``KNOWN_UNREPRESENTABLE`` and the authority clarifies it with a closed
reason instead of approximating it («liquidado» ≈ «Cerrado» is the legacy lexicon's guess, not a state).
"""

from __future__ import annotations

from dataclasses import dataclass

from app.core.contract_invariants import INVALID_CONTRACT_STATES
from app.core.estado_families import ESTADO_FAMILIES, process_state_field
from app.core.host_authorities.datasets import CONTRACTS, PROCESSES
from app.data.estados import (
    ESTADO_APERTURA_SYNONYMS, ESTADO_CONTRATO_SYNONYMS, ESTADO_PROCEDIMIENTO_SYNONYMS,
)

Literals = tuple[tuple[str, tuple[str, ...]], ...]  # ((column, values), ...): AND across columns
_PROCESS_LEXICON = {**ESTADO_PROCEDIMIENTO_SYNONYMS, **ESTADO_APERTURA_SYNONYMS}


@dataclass(frozen=True, slots=True)
class StateReading:
    state_class: str
    operator: str  # the operator an honest cite of this phrase carries: "family" | "is"
    literals: tuple[tuple[str, Literals], ...]  # (logical dataset, columns); absent = not representable
    names_event: bool = False  # a participle that names a dated event («publicados hace dos semanas», D6)

    def for_dataset(self, logical: str) -> Literals | None:
        return next((columns for name, columns in self.literals if name == logical), None)


def _process(lemma: str) -> Literals:
    literal = _PROCESS_LEXICON[lemma]
    return ((process_state_field(literal), (literal,)),)


def _contract(lemma: str) -> Literals:
    return (("estado_contrato", (ESTADO_CONTRATO_SYNONYMS[lemma],)),)


_OPEN_FUNNEL: Literals = tuple(sorted((
    ("estado_de_apertura_del_proceso", (_PROCESS_LEXICON["abierto"],)),
    ("estado_del_procedimiento", tuple(ESTADO_FAMILIES["oferta_abierta"][PROCESSES]["estado_del_procedimiento"])),
)))
_ACTIVE_CONTRACTS: Literals = (
    ("estado_contrato", tuple(ESTADO_FAMILIES["contrato_activo"][CONTRACTS]["estado_contrato"])),
)


def _reading(state_class: str, operator: str, *, names_event: bool = False,
             processes: Literals | None = None, contracts: Literals | None = None) -> StateReading:
    pairs = tuple((name, columns) for name, columns in ((PROCESSES, processes), (CONTRACTS, contracts))
                  if columns is not None)
    return StateReading(state_class, operator, pairs, names_event)


OPEN = _reading("open", "family", processes=_OPEN_FUNNEL)
LIVE = _reading("open", "family", processes=_OPEN_FUNNEL, contracts=_ACTIVE_CONTRACTS)  # vigente: both lives
PUBLISHED = _reading("published", "is", names_event=True, processes=_process("publicado"))

# folded, masculine-singular phrases (tokens); the inflection is read by ``lemmas`` below
LEXICON: dict[tuple[str, ...], StateReading] = {
    ("abierto",): OPEN, ("convocado",): OPEN, ("convocatoria",): OPEN, ("en", "convocatoria"): OPEN,
    ("en", "concurso"): OPEN, ("reciben", "ofertas"): OPEN, ("todavia", "reciben", "ofertas"): OPEN,
    ("vigente",): LIVE, ("activo",): LIVE, ("disponible",): LIVE, ("en", "curso"): LIVE,  # C4: open family
    ("publicado",): PUBLISHED,
    ("cerrado",): _reading("closed", "is", processes=_process("cerrado"), contracts=_contract("cerrado")),
    ("adjudicado",): _reading("awarded", "is", processes=_process("adjudicado")),
    ("seleccionado",): _reading("awarded", "is", processes=_process("seleccionado")),
    ("terminado",): _reading("terminated", "is", contracts=_contract("terminado")),
    ("en", "ejecucion"): _reading("in_execution", "is", contracts=_ACTIVE_CONTRACTS),
    ("ejecutando",): _reading("in_execution", "is", contracts=_ACTIVE_CONTRACTS),
    ("cancelado",): _reading("cancelled", "is", processes=_process("cancelado"),
                             contracts=_contract("cancelado")),
    ("borrador",): _reading("draft", "is", processes=_process("borrador"), contracts=_contract("borrador")),
    ("en", "borrador"): _reading("draft", "is", processes=_process("borrador"),
                                 contracts=_contract("borrador")),
    ("en", "evaluacion"): _reading("in_evaluation", "is", processes=_process("evaluacion")),
    ("suspendido",): _reading("suspended", "is", processes=_process("suspendido"),
                              contracts=_contract("suspendido")),
    ("cedido",): _reading("ceded", "is", contracts=_contract("cedido")),
}

# Words that name a state SECOP has no literal for: clarified with a closed reason, never approximated.
# «firmado»/«celebrado» are the contract states the invariants forbid as a SECOP value: they are read from
# their single source of truth (``INVALID_CONTRACT_STATES``), never retyped here. The rest is this
# authority's own lexicon of words with no literal in either dataset.
KNOWN_UNREPRESENTABLE = frozenset({
    *((state,) for state in INVALID_CONTRACT_STATES),
    ("liquidado",), ("aprobado",), ("en", "aprobacion"), ("desierto",),
    ("revocado",), ("modificado",), ("prorrogado",), ("finalizado",), ("enviado", "proveedor"),
})

# M1.0 C3: «ahora», «hoy», «en este momento»… are not TIME; alone with processes they cite STATE family
# open, and after an open word («abiertos hoy») they add nothing.
DEICTICS = frozenset({("hoy",), ("ahora",), ("ahorita",), ("actualmente",), ("en", "este", "momento"),
                      ("en", "la", "actualidad")})

NEGATION_TOKENS = frozenset({"no", "ni"})
COPULA_TOKENS = frozenset({"que", "sea", "sean", "este", "esten", "esta", "estan", "es", "son"})


def lemmas(token: str) -> tuple[str, ...]:
    """The token and its masculine singular: cerradas -> cerrada -> cerrado, vigentes -> vigente."""
    found = [token]
    base = token[:-1] if token.endswith("s") else token
    if base != token:
        found.append(base)
    if base.endswith("a"):
        found.append(base[:-1] + "o")
    if token.endswith("es"):
        found.append(token[:-2])  # borradores -> borrador
    return tuple(dict.fromkeys(found))


def _phrase_forms(phrase: tuple[str, ...]) -> list[tuple[str, ...]]:
    """The lexicon keys a folded phrase may denote (only a one-word phrase inflects)."""
    if len(phrase) == 1:
        return [(form,) for form in lemmas(phrase[0])]
    return [phrase]


def lookup(phrase: tuple[str, ...]) -> StateReading | str | None:
    """The reading of exactly ``phrase``; ``"unrepresentable"`` for a known word without a literal.

    An open word followed by a deictic («abiertos hoy») reads as the open word.
    """
    if phrase in DEICTICS:
        return OPEN
    for key in _phrase_forms(phrase):
        if key in LEXICON:
            return LEXICON[key]
    if any(key in KNOWN_UNREPRESENTABLE for key in _phrase_forms(phrase)):
        return "unrepresentable"
    for deictic in DEICTICS:
        if len(phrase) > len(deictic) and phrase[-len(deictic):] == deictic:
            head = lookup(phrase[:-len(deictic)])
            if head is OPEN or head is LIVE:
                return head
    return None


def contains_state_phrase(tokens: tuple[str, ...]) -> bool:
    """True when some contiguous run of ``tokens`` is a state phrase (so a quote that is not ONLY one has residue)."""
    return any(lookup(tokens[i:j]) is not None
               for i in range(len(tokens)) for j in range(i + 1, min(len(tokens), i + 3) + 1))


def strip_scaffold(tokens: tuple[str, ...]) -> tuple[tuple[str, ...], int]:
    """``(rest, negations)``: the leading «no / que no sean / no esté» scaffold is not part of the state."""
    index = 0
    while index < len(tokens) and (tokens[index] in NEGATION_TOKENS or tokens[index] in COPULA_TOKENS):
        index += 1
    return tokens[index:], sum(token in NEGATION_TOKENS for token in tokens[:index])
