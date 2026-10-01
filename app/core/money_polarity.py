"""One lexical authority for money relations; no conversion or admission.

Comparators must connect directly to an amount through a closed connector
grammar. A noun breaks that connection. Numeric and unit grammars are imported
from their owners; decisions retain source operands, never calculate COP.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, replace
from enum import Enum

from app.core import money_suffix_place
from app.core.money_comparators import (
    BARE_ADJECTIVE_DOWN_PATTERN,
    BARE_ADJECTIVE_UP_PATTERN,
    DOWN_PATTERN,
    NEGATION_FILLER,
    SUFFIX_DOWN_PATTERN,
    SUFFIX_UP_PATTERN,
    UP_PATTERN,
)
from app.core.money_resolution import CARDINAL_WORD_PATTERN
from app.utils.money import MONETARY_UNIT_PATTERN


class MoneyRelation(str, Enum):
    LOWER_BOUND = "LOWER_BOUND"
    UPPER_BOUND = "UPPER_BOUND"
    RANGE = "RANGE"
    APPROXIMATE = "APPROXIMATE"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class MoneyPolarityDecision:
    relation: MoneyRelation
    source_span: str
    amount_spans: tuple[str, ...]
    start: int
    end: int
    comparator_span: str | None = None
    negated: bool = False
    # Symmetric ranges sort endpoints; compound inequalities retain roles.
    ordered: bool = False


_DIGIT = r"(?:\$\s*|cop\s+)?\d+(?:[.,]\d+)*"
_VERBAL = rf"{CARDINAL_WORD_PATTERN}(?:\s+{CARDINAL_WORD_PATTERN})*"
_AMOUNT = (
    rf"(?:{_DIGIT}|{_VERBAL})(?:\s*{MONETARY_UNIT_PATTERN})?"
    r"(?:\s+y\s+medi[oa]s?)?(?:(?:\s+de)?\s+pesos?)?"
)
_AMOUNT_RE = re.compile(rf"(?<![\w.,]){_AMOUNT}", re.IGNORECASE)
# The comparator vocabulary has ONE owner (money_comparators); only the
# grammar around it (negation, link, position) is local to this scanner.
_UP = UP_PATTERN
_DOWN = DOWN_PATTERN
_LINK = r"(?:\s+o\s+igual(?:es)?)?(?:\s+(?:a|de|que))?(?:\s+(?:los?|las?|el))?\s*"
_PREFIX_RE = re.compile(
    rf"(?<!\w)(?:que\s+)?(?P<neg>no\s+(?:{NEGATION_FILLER})?)?(?:sean?\s+)?(?:por\s+)?"
    rf"(?:(?P<up>{_UP})|(?P<down>{_DOWN})){_LINK}$", re.IGNORECASE,
)
# «por 300 millones» es la aproximación legada autorizada. «más o menos 300 millones»
# aproxima, no acota: el «menos» de «más o menos» NO es el comparador «menos»
# (alineado con ``read_comparators``, R2 P3), y no se inventa una banda: se aclara.
_APPROX_RE = re.compile(
    r"\b(?P<neg>no\s+)?(?:por|(?P<loose>m[aá]s\s+o\s+menos(?:\s+de)?))\s+$", re.IGNORECASE,
)
_SUFFIX_RE = re.compile(
    rf"^\s+(?:(?P<up>{SUFFIX_UP_PATTERN})|(?P<down>{SUFFIX_DOWN_PATTERN}))\b", re.IGNORECASE,
)
# The adjective suffix with nothing checked after it: valid only when a place or a buyer follows it
# («300 millones o mayores Galapa», ``money_suffix_place``).
_BARE_SUFFIX_RE = re.compile(
    rf"^\s+(?:(?P<up>{BARE_ADJECTIVE_UP_PATTERN})|(?P<down>{BARE_ADJECTIVE_DOWN_PATTERN}))\b", re.IGNORECASE,
)
_RANGE_RE = re.compile(
    rf"\b(?P<neg>no\s+)?(?:entre|de|desde)\s+(?P<lo>{_AMOUNT})"
    rf"(?:\s*[-–—]\s*|\s+(?:y|a|hasta)\s+)(?P<hi>{_AMOUNT})", re.IGNORECASE,
)
_HYPHEN_RE = re.compile(
    rf"(?<![\w.,])(?P<lo>{_DIGIT})\s*[-–—]\s*(?P<hi>{_DIGIT})"
    rf"\s+(?P<unit>{MONETARY_UNIT_PATTERN})", re.IGNORECASE,
)
_COORDINATION_RE = re.compile(r"\s*(?:y|e)\s*", re.IGNORECASE)


def _direction(match: re.Match[str]) -> MoneyRelation:
    lower = match.group("up") is not None
    if match.groupdict().get("neg"):
        lower = not lower
    return MoneyRelation.LOWER_BOUND if lower else MoneyRelation.UPPER_BOUND


def has_money_relation_before(text: str) -> bool:
    """Local comparator evidence for identifier arbitration (no NIT decision)."""
    return bool(_PREFIX_RE.search(text) or _APPROX_RE.search(text))


def _single(text: str, match: re.Match[str]) -> MoneyPolarityDecision:
    start, end = match.span()
    # Amount regex can consume trailing space, needed for optional units.
    end = start + len(match.group().rstrip())
    amount = text[start:end]
    prefix = _PREFIX_RE.search(text[:start])
    suffix = _SUFFIX_RE.search(text[end:])
    if suffix is None:
        bare = _BARE_SUFFIX_RE.search(text[end:])
        if bare and money_suffix_place.names_a_place(text[end + bare.end():]):
            suffix = bare
    if suffix and re.match(rf"{_LINK}{_AMOUNT}", text[end + suffix.end():], re.IGNORECASE):
        suffix = None  # "X y menos de Y" belongs to the next operand.
    relation = MoneyRelation.UNKNOWN
    comparator = None
    negated = False
    approx = _APPROX_RE.search(text[:start])
    loose = bool(approx and approx.group("loose"))
    if prefix and not loose:
        relation = _direction(prefix)
        start = prefix.start()
        comparator = prefix.group().strip()
        negated = bool(prefix.group("neg"))
    elif approx:
        relation = (
            MoneyRelation.UNKNOWN if approx.group("neg") or loose else MoneyRelation.APPROXIMATE
        )
        start = approx.start()
        comparator = approx.group().strip()
        negated = bool(approx.group("neg"))
    if suffix:
        suffix_relation = _direction(suffix)
        if prefix and relation != suffix_relation:
            relation = MoneyRelation.UNKNOWN
        elif not negated or prefix:
            relation = suffix_relation
        comparator = " ".join(filter(None, (comparator, suffix.group().strip())))
        end += suffix.end()
    return MoneyPolarityDecision(
        relation, text[start:end], (amount,), start, end, comparator, negated,
    )


def _coordinated(text: str, decisions: list[MoneyPolarityDecision]) -> tuple[MoneyPolarityDecision, ...]:
    result: list[MoneyPolarityDecision] = []
    for current in decisions:
        if result and _COORDINATION_RE.fullmatch(text[result[-1].end:current.start]):
            previous = result.pop()
            roles = {previous.relation, current.relation}
            valid = roles == {MoneyRelation.LOWER_BOUND, MoneyRelation.UPPER_BOUND}
            operands = (*previous.amount_spans, *current.amount_spans)
            if valid and previous.relation is MoneyRelation.UPPER_BOUND:
                operands = tuple(reversed(operands))
            result.append(MoneyPolarityDecision(
                MoneyRelation.RANGE if valid else MoneyRelation.UNKNOWN,
                text[previous.start:current.end], operands, previous.start, current.end,
                " ".join(filter(None, (previous.comparator_span, current.comparator_span))),
                previous.negated or current.negated, ordered=valid,
            ))
        else:
            result.append(current)
    return tuple(result)


def resolve_money_polarity(text: str) -> tuple[MoneyPolarityDecision, ...]:
    """Recognize relations and source spans, including unresolved amounts.

    UNKNOWN still requires monetary admission by the caller. Years, references
    and small counts are not declared money by this lexical scanner.
    """
    decisions: list[MoneyPolarityDecision] = []
    for pattern in (_RANGE_RE, _HYPHEN_RE):
        for match in pattern.finditer(text):
            if pattern is _RANGE_RE and _PREFIX_RE.search(text[:match.start()]):
                continue  # "más de X y Y" is not a symmetric "de X a Y".
            if any(match.start() < d.end and d.start < match.end() for d in decisions):
                continue
            operands = (match.group("lo").strip(), match.group("hi").strip())
            if unit := match.groupdict().get("unit"):
                operands = tuple(f"{operand} {unit}" for operand in operands)
            negated = bool(match.groupdict().get("neg"))
            decisions.append(MoneyPolarityDecision(
                MoneyRelation.UNKNOWN if negated else MoneyRelation.RANGE,
                match.group().rstrip(), operands, match.start(),
                match.start() + len(match.group().rstrip()), negated=negated,
            ))
    for match in _AMOUNT_RE.finditer(text):
        if match.group().strip().lower() == "y":
            continue  # Numeric cardinal connector, not an operand by itself.
        if any(match.start() < d.end and d.start < match.end() for d in decisions):
            continue
        decision = _single(text, match)
        # A relation cannot reach back across a previously claimed range.
        if any(decision.start < d.end and d.start < decision.end for d in decisions):
            decision = replace(decision, relation=MoneyRelation.UNKNOWN,
                               start=match.start(), source_span=match.group().strip())
        decisions.append(decision)
    return _coordinated(text, sorted(decisions, key=lambda d: d.start))
