"""Which entities does an ambiguous buyer alias name, and which of them can the user actually type? (M1-H0 R1).

The host refuses an alias that several entities claim (``AMBIGUOUS_HOST_DIMENSION:BUYER``).
Telling the user to «write the full name» is a loop when the full name is the same alias, so the
clarification names the owners.  A name is only offered when the host itself would accept it: the
official name must be claimed by exactly that entity (``alias_owners``) and the resolver must
return it as ``exact``.

R2: a list that leaves out an owner is never presented as the whole choice.  ``BuyerChoices.unavailable``
names the owners the host cannot take by name; the message then says the list is partial and offers no
buttons.  An owner whose name contains every word the user cited («ICBF Bogotá» -> ICBF REGIONAL BOGOTA) is
the entity the user most likely named: it goes first, and when it is unavailable the message says so instead
of steering to a sibling.  A NEGATIVE buyer never gets options: choosing one would invert the exclusion.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.core.buyer_source_names import project_buyer_names
from app.core.foundation_evidence import material_phrase
from app.core.foundation_operations import Subject
from app.core.interpretation_contract import Polarity
from app.core.semantic_dimensions import SemanticDimension
from app.core.soql_builder import SoQLBuilder
from app.utils.money import normalize_text

MAX_SHOWN = 6
_HEADQUARTERS = re.compile(r"\b(?:nacional|central)\b")
_WORD = re.compile(r"\w+")


@dataclass(frozen=True, slots=True)
class BuyerChoices:
    alias: str
    shown: tuple[str, ...]  # offerable owners, named-by-the-user first, at most MAX_SHOWN
    hidden: int  # offerable owners over the cap (a real count, never a guess)
    unavailable: tuple[str, ...] = ()  # owners the host cannot take by name: the list is partial when not empty
    named_unavailable: tuple[str, ...] = ()  # unavailable owners whose name contains every cited word
    negated: bool = False  # the citation is excluded: no option may be offered

    @property
    def complete(self) -> bool:
        return not self.unavailable


def typable(entity_resolver, name: str, datasets: tuple[str, ...] = ()) -> bool:
    """True when typing this official name resolves, in the host, to exactly this entity."""
    if entity_resolver.alias_owners(normalize_text(name)) != frozenset({name}):
        return False
    resolved = entity_resolver.resolve_entidad(name)
    return (resolved.method == "exact" and resolved.value == name
            and all(project_buyer_names(name, dataset) for dataset in datasets))


def _datasets(proposal, condition) -> tuple[str, ...]:
    by_subject = {Subject.PROCESSES: SoQLBuilder.PROCESOS_DATASET, Subject.CONTRACTS: SoQLBuilder.CONTRATOS_DATASET}
    return tuple(sorted({by_subject[r.subject] for r in proposal.requests
                         if r.request_id in condition.request_ids and r.subject in by_subject}))


def _words(text: str) -> frozenset[str]:
    return frozenset(_WORD.findall(normalize_text(text)))


def _headquarters_first(name: str) -> tuple[bool, str]:
    return (_HEADQUARTERS.search(normalize_text(name)) is None, normalize_text(name))


def _named(owners: frozenset[str], phrase: str) -> frozenset[str]:
    """Owners whose name holds every cited word; distinctive only when they are a non-empty minority (<= half).

    «ICBF» is in the name of 11 of its 12 owners: it identifies none of them, so nothing is promoted.
    «IDRD» is in 1 of 2 and «ICBF Bogotá» in 1 of 12: they point at that owner.
    """
    cited = _words(phrase)
    named = frozenset(name for name in owners if cited and cited <= _words(name))
    return named if 0 < 2 * len(named) <= len(owners) else frozenset()


def buyer_choices(proposal, entity_resolver, raw: str | None = None) -> BuyerChoices | None:
    """The owners of the first multi-owner buyer citation of the proposal; None when there is none.

    The cited phrase is read with ``material_phrase`` (the host's own reader of a condition's spans), so the
    alias looked up here is the very one admission refused.  Without the raw text there is nothing to read.
    """
    if proposal is None or entity_resolver is None or raw is None:
        return None
    for condition in proposal.conditions:
        if condition.dimension is not SemanticDimension.BUYER:
            continue
        phrase, unread = material_phrase(condition.evidence, raw, SemanticDimension.BUYER.value)
        if unread is not None:
            continue
        owners = entity_resolver.alias_owners(normalize_text(phrase))
        if len(owners) < 2:
            continue
        if condition.polarity is Polarity.NEGATIVE:
            return BuyerChoices(phrase, (), 0, negated=True)
        datasets = _datasets(proposal, condition)
        named = _named(owners, phrase)
        offerable = sorted((n for n in owners if typable(entity_resolver, n, datasets)),
                           key=lambda n: (n not in named, *_headquarters_first(n)))
        unavailable = tuple(sorted((n for n in owners if n not in offerable),
                                   key=lambda n: (n not in named, *_headquarters_first(n))))
        return BuyerChoices(phrase, tuple(offerable[:MAX_SHOWN]), max(0, len(offerable) - MAX_SHOWN), unavailable,
                            tuple(n for n in unavailable if n in named))
    return None
