"""Which entities does an ambiguous buyer alias name, and which of them can the user actually type? (M1-H0 R1).

The host refuses an alias that several entities claim (``AMBIGUOUS_HOST_DIMENSION:BUYER``).
Telling the user to «write the full name» is a loop when the full name is the same alias, so the
clarification names the owners.  A name is only offered when the host itself would accept it: the
official name must be claimed by exactly that entity (``alias_owners``) and the resolver must
return it as ``exact``.  Anything else is counted in «y N más», never shown as an option.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.core.buyer_source_names import project_buyer_names
from app.core.semantic_dimensions import SemanticDimension
from app.core.soql_builder import SoQLBuilder
from app.utils.money import normalize_text

MAX_SHOWN = 6
_HEADQUARTERS = re.compile(r"\b(?:nacional|central)\b")


@dataclass(frozen=True, slots=True)
class BuyerChoices:
    alias: str
    shown: tuple[str, ...]
    hidden: int  # owners of the alias that are not offered (unverifiable or over the cap)


def typable(entity_resolver, name: str, datasets: tuple[str, ...] = ()) -> bool:
    """True when typing this official name resolves, in the host, to exactly this entity."""
    if entity_resolver.alias_owners(normalize_text(name)) != frozenset({name}):
        return False
    resolved = entity_resolver.resolve_entidad(name)
    return (resolved.method == "exact" and resolved.value == name
            and all(project_buyer_names(name, dataset) for dataset in datasets))


def _datasets(proposal, condition) -> tuple[str, ...]:
    by_subject = {"processes": SoQLBuilder.PROCESOS_DATASET, "contracts": SoQLBuilder.CONTRATOS_DATASET}
    return tuple(sorted({by_subject[r.subject] for r in proposal.requests
                         if r.request_id in condition.request_ids and r.subject in by_subject}))


def _headquarters_first(name: str) -> tuple[bool, str]:
    return (_HEADQUARTERS.search(normalize_text(name)) is None, normalize_text(name))


def buyer_choices(proposal, entity_resolver) -> BuyerChoices | None:
    """The owners of the first multi-owner buyer citation of the proposal; None when there is none."""
    if proposal is None or entity_resolver is None:
        return None
    for condition in proposal.conditions:
        if condition.dimension is not SemanticDimension.BUYER:
            continue
        quotes = tuple(dict.fromkeys(span.quote for span in condition.evidence))
        owners = entity_resolver.alias_owners(normalize_text(" ".join(quotes)))
        if len(owners) < 2:
            continue
        datasets = _datasets(proposal, condition)
        ordered = sorted((name for name in owners if typable(entity_resolver, name, datasets)),
                         key=_headquarters_first)
        shown = tuple(ordered[:MAX_SHOWN])
        return BuyerChoices(" ".join(quotes), shown, len(owners) - len(shown))
    return None
