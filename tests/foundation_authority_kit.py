"""Conformance kit for Foundation dimension authorities (spine v5, C0 §5.5.4; C1.1).

Every lane writes ``tests/test_host_authority_<dim>.py`` and calls::

    assert_conforms(authority, cases)

``cases`` are ``ConformanceCase(evidence, ctx)`` built with ``case(...)`` / ``evidence_from_text``
(which go through ``build_window``, the only window constructor).  The kit raises
``ConformanceFailure`` (an ``AssertionError`` with a stable ``code``) on the first violation.

What it verifies, per case:

* INV-C0-8 purity: every call runs with sockets blocked, ``date.today`` / ``datetime.now`` /
  ``utcnow`` / ``today`` and ``get_settings`` replaced by guards that RECORD the touch (a swallowed
  exception still fails), and two calls on the same input return equal verdicts.
* INV-C0-7 monotonicity: if the verdict with context is RESOLVED(v), the verdict with ANY
  truncation of that context (the stripped window included) is RESOLVED(v).  Context may only
  withdraw authority.  ``monotone_chain=False`` checks only the stripped window (the literal INV).
* INV-C0-7b gap monotonicity: if the verdict is RESOLVED(v), replacing each gap of the window (and
  all of them) by ``()`` and by ``("x",)`` gives RESOLVED(v) or a non-RESOLVED verdict, never another
  value.  A gap may withdraw or condition authority, never change what it resolves.
* INV-C1.1-EXCL: every RESOLVED value that has ``excluded`` has ``excluded == (polarity is NEGATIVE)``.
* INV-C1.1-DS: the datasets of a ``StateValue``'s literals are contained in ``ctx.datasets``.
* INV-C0-9: every resolved value is a closed ``ExecutableValue`` of the dimension's type, round
  trips through ``decode_value`` and fits a ``HostResolution`` (``to_resolution``).
* Verdict shape, re-checked here independently of ``AuthorityVerdict.__post_init__``: RESOLVED if
  and only if a value is present; a reason in ``AUTHORITY_REASONS`` whose ``:<D>`` suffix names
  the evidence dimension (the ``VALUE_*`` aliases only with VALUE); ``authority ==
  "<id>@<version>"``; ``detail`` only on a non-RESOLVED verdict; a GEOGRAPHY value carries the
  evidence level (any concrete level when the evidence level is UNKNOWN).
* Polarity: with the same text flipped to NEGATIVE, either the verdict is RESOLVED with a value
  that differs from the positive one AND a policy that covers it, or it is non-RESOLVED with
  ``NEGATION_UNSUPPORTED:<D>``.  A negation is never ignored.
* Declared scope: an operator or dimension the authority does not declare never RESOLVES.

The kit demands at least one RESOLVED case (``require_resolved``): otherwise INV-C0-7/9 are vacuous.
"""

from __future__ import annotations

import re
import socket
import sys
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import datetime as _datetime_module
from collections.abc import Iterable, Sequence

from app.core.foundation_authority_contract import (
    AUTHORITY_DIMENSION_NAMES, AUTHORITY_REASONS, VALUE_ALIASES, WINDOW_TOKENS, ActorValue,
    AuthorityContext, AuthorityVerdict, BuyerValue, CardinalityValue, ContractTypeValue,
    DimensionEvidence, GeoValue, ModalityValue, MoneyValue, OrderValue, StateValue, TimeValue,
    TopicValue, decode_value, is_executable_value,
)
from app.core.foundation_evidence_window import build_window
from app.core.foundation_geo_level import GeoLevel
from app.core.interpretation_contract import (
    ContractError, Effect, Polarity, RawSpan, ResolutionDecision,
)
from app.core.semantic_dimensions import SemanticDimension

REFERENCE_INSTANT = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
_UNDECLARED_OPERATOR = "__undeclared_operator__"
_VALUE_TYPE_BY_DIMENSION = {
    "TOPIC": TopicValue, "BUYER": BuyerValue, "ACTOR": ActorValue, "GEOGRAPHY": GeoValue,
    "TIME": TimeValue, "VALUE": MoneyValue, "STATE": StateValue, "MODALITY": ModalityValue,
    "CONTRACT_TYPE_OR_NATURE": ContractTypeValue, "ORDER": OrderValue,
    "CARDINALITY": CardinalityValue,
}
assert set(_VALUE_TYPE_BY_DIMENSION) == AUTHORITY_DIMENSION_NAMES
_HAS_EXCLUDED = (TopicValue, BuyerValue, ActorValue, GeoValue, TimeValue, StateValue, ModalityValue,
                 ContractTypeValue)
_DETAIL = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_GAP_FILLERS = ((), ("x",))


class ConformanceFailure(AssertionError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


@dataclass(frozen=True, slots=True)
class ConformanceCase:
    evidence: DimensionEvidence
    ctx: AuthorityContext


@dataclass(frozen=True, slots=True)
class ConformanceReport:
    cases: int
    resolved: int
    calls: int


def default_context(datasets: Iterable[str] = ("dataset-a",)) -> AuthorityContext:
    return AuthorityContext(REFERENCE_INSTANT, frozenset(datasets))


def _span(raw: str, needle: str, occurrence: int = 0) -> RawSpan:
    start = -1
    for _ in range(occurrence + 1):
        start = raw.index(needle, start + 1)
    return RawSpan(start, start + len(needle), needle)


def evidence_from_text(raw: str, needle: str | Sequence[str | tuple[str, int]], *,
                       dimension: SemanticDimension, operator: str,
                       polarity: Polarity = Polarity.POSITIVE, level: GeoLevel | None = None,
                       condition_id: str = "c1", occurrence: int = 0) -> DimensionEvidence:
    """Evidence for ``needle`` in ``raw``, via the one builder.

    ``needle`` is one string (its ``occurrence``-th appearance) or several cites, each a string or
    ``(string, occurrence)``: a condition cited twice, as in «publicados o abiertos».
    """
    cites = [(needle, occurrence)] if isinstance(needle, str) else [
        (item, 0) if isinstance(item, str) else item for item in needle]
    spans = [_span(raw, text, index) for text, index in cites]
    return DimensionEvidence(condition_id, dimension, operator, polarity, level,
                             build_window(raw, spans))


def case(raw: str, needle: str, *, ctx: AuthorityContext | None = None, **kwargs) -> ConformanceCase:
    return ConformanceCase(evidence_from_text(raw, needle, **kwargs), ctx or default_context())


# ---- purity sandbox -------------------------------------------------------------------------
class PurityViolation(Exception):
    pass


def _guards(touched: list[str]):
    real_date, real_datetime = _datetime_module.date, _datetime_module.datetime

    def forbid(name: str):
        def guard(*_args, **_kwargs):
            touched.append(name)
            raise PurityViolation(name)
        return guard

    def meta(real):
        class Meta(type):
            def __instancecheck__(cls, obj):
                return isinstance(obj, real)

            def __subclasscheck__(cls, sub):
                return issubclass(sub, real)

            def __getattr__(cls, name):  # pure constructors/constants of the real class
                return getattr(real, name)
        return Meta

    class GuardedDate(metaclass=meta(real_date)):
        today = staticmethod(forbid("date.today"))

        def __new__(cls, *args, **kwargs):
            return real_date(*args, **kwargs)

    class GuardedDatetime(metaclass=meta(real_datetime)):
        now = staticmethod(forbid("datetime.now"))
        utcnow = staticmethod(forbid("datetime.utcnow"))
        today = staticmethod(forbid("datetime.today"))

        def __new__(cls, *args, **kwargs):
            return real_datetime(*args, **kwargs)

    return real_date, real_datetime, GuardedDate, GuardedDatetime, forbid


_NOT_SWEPT = {"app.core.foundation_authority_contract", "app.core.foundation_authority_values"}


@contextmanager
def pure_environment(authority):
    """Block sockets, guard the clock and ``get_settings``; yield the list of recorded touches."""
    from app import config

    touched: list[str] = []
    real_date, real_datetime, guarded_date, guarded_datetime, forbid = _guards(touched)
    real_settings = config.get_settings
    swaps: list[tuple[object, str, object]] = []

    def swap(owner, name, replacement):
        swaps.append((owner, name, getattr(owner, name)))
        setattr(owner, name, replacement)

    extra = {cls.__module__ for cls in type(authority).__mro__}
    try:
        swap(socket.socket, "connect", forbid("socket.connect"))
        swap(socket.socket, "connect_ex", forbid("socket.connect_ex"))
        for name in ("create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex"):
            swap(socket, name, forbid(f"socket.{name}"))
        swap(_datetime_module, "date", guarded_date)
        swap(_datetime_module, "datetime", guarded_datetime)
        for module_name, module in list(sys.modules.items()):
            if module is None or module_name in _NOT_SWEPT:
                continue
            if not (module_name.startswith("app.") or module_name in extra):
                continue
            for attribute, value in list(vars(module).items()):
                if value is real_date:
                    swap(module, attribute, guarded_date)
                elif value is real_datetime:
                    swap(module, attribute, guarded_datetime)
                elif value is real_settings:
                    swap(module, attribute, forbid("get_settings"))
        swap(config, "get_settings", forbid("get_settings"))
        yield touched
    finally:
        for owner, name, original in reversed(swaps):
            setattr(owner, name, original)


# ---- checks ---------------------------------------------------------------------------------
def _fail(code: str, detail: str = ""):
    raise ConformanceFailure(code, detail)


def _call(authority, evidence: DimensionEvidence, ctx: AuthorityContext) -> AuthorityVerdict:
    with pure_environment(authority) as touched:
        try:
            verdict = authority.resolve(evidence, ctx)
        except PurityViolation:
            verdict = None
        except Exception as error:  # an authority never raises: it clarifies or blocks
            if touched:
                _fail("INV-C0-8", f"impure call: {touched}")
            _fail("AUTHORITY_RAISED", f"{type(error).__name__}: {error}")
    if touched:
        _fail("INV-C0-8", f"impure call: {sorted(set(touched))}")
    return verdict


def _check_declaration(authority) -> None:
    ident, version = getattr(authority, "authority_id", None), getattr(authority, "version", None)
    for value in (ident, version):
        if type(value) is not str or not value or "@" in value:
            _fail("AUTHORITY_DECLARATION", "authority_id / version must be non-empty strings without '@'")
    dimensions, operators = getattr(authority, "dimensions", None), getattr(authority, "operators", None)
    if (type(dimensions) is not frozenset or not dimensions
            or any(type(item) is not SemanticDimension for item in dimensions)):
        _fail("AUTHORITY_DECLARATION", "dimensions must be a non-empty frozenset[SemanticDimension]")
    if (type(operators) is not frozenset or not operators
            or any(type(item) is not str or not item for item in operators)):
        _fail("AUTHORITY_DECLARATION", "operators must be a non-empty frozenset[str]")
    if any(item.value not in AUTHORITY_DIMENSION_NAMES for item in dimensions):
        _fail("AUTHORITY_DECLARATION", "a declared dimension has no authority")
    if not callable(getattr(authority, "resolve", None)):
        _fail("AUTHORITY_DECLARATION", "resolve is missing")


def _check_shape(authority, verdict, evidence: DimensionEvidence, ctx: AuthorityContext) -> None:
    """Independent of AuthorityVerdict.__post_init__: a mutated contract must not blind the kit."""
    if type(verdict) is not AuthorityVerdict:
        _fail("VERDICT_TYPE", type(verdict).__name__)
    if type(verdict.decision) is not ResolutionDecision:
        _fail("VERDICT_DECISION")
    if verdict.authority != f"{authority.authority_id}@{authority.version}":
        _fail("VERDICT_AUTHORITY", verdict.authority)
    resolved = verdict.decision is ResolutionDecision.RESOLVED
    if resolved != (verdict.value is not None):
        _fail("RESOLVED_IFF_VALUE", f"{verdict.decision.value} value={verdict.value!r}")
    if verdict.detail is not None:
        if resolved:
            _fail("RESOLVED_WITH_DETAIL", repr(verdict.detail))
        if type(verdict.detail) is not str or not _DETAIL.fullmatch(verdict.detail):
            _fail("VERDICT_DETAIL", repr(verdict.detail))
    if resolved:
        if not is_executable_value(verdict.value):
            _fail("VALUE_OUTSIDE_UNION", type(verdict.value).__name__)
        if verdict.reason is not None:
            _fail("RESOLVED_WITH_REASON")
        expected = _VALUE_TYPE_BY_DIMENSION[evidence.dimension.value]
        if type(verdict.value) is not expected:
            _fail("VALUE_TYPE_MISMATCH", f"{evidence.dimension.value} -> {type(verdict.value).__name__}")
        if (type(verdict.value) is GeoValue and evidence.level is not GeoLevel.UNKNOWN
                and verdict.value.level is not evidence.level):
            _fail("LEVEL_MISMATCH", "a GEOGRAPHY value must carry the evidence level")
        if (isinstance(verdict.value, _HAS_EXCLUDED)
                and verdict.value.excluded != (evidence.polarity is Polarity.NEGATIVE)):
            _fail("INV-C1.1-EXCL", f"excluded={verdict.value.excluded} with {evidence.polarity.value}")
        if type(verdict.value) is CardinalityValue:
            # INV-C1.2-CAP (P8): the announcement is born of the typed value, never of the text. Computed
            # here from the two figures, not from ``capped``: a mutated property must not blind the kit.
            capped = verdict.value.n_requested > verdict.value.n
            announced = (verdict.effect is Effect.NARROWING, "topn-cap@1" in verdict.policies,
                         "topn.capped" in verdict.receipt.rules)
            if (capped and not all(announced)) or (not capped and any(announced)):
                # capped => NARROWING, the policy AND the rule; not capped => none of the three
                _fail("INV-C1.2-CAP", f"n={verdict.value.n} n_requested={verdict.value.n_requested} "
                                      f"effect={verdict.effect.value} policies={verdict.policies!r} "
                                      f"rules={verdict.receipt.rules!r}")
        if type(verdict.value) is StateValue:
            stray = {name for name, _column, _values in verdict.value.literals} - ctx.datasets
            if stray:
                _fail("INV-C1.1-DS", f"literals for datasets outside the context: {sorted(stray)}")
        return
    if verdict.reason not in AUTHORITY_REASONS:
        _fail("REASON_OUTSIDE_CLOSED_SET", repr(verdict.reason))
    suffix = verdict.reason.partition(":")[2]
    if suffix and suffix != evidence.dimension.value:
        _fail("REASON_DIMENSION_MISMATCH", f"{verdict.reason} for {evidence.dimension.value}")
    if verdict.reason in VALUE_ALIASES and evidence.dimension.value != "VALUE":
        _fail("REASON_DIMENSION_MISMATCH", f"{verdict.reason} for {evidence.dimension.value}")


def _check_round_trip(verdict: AuthorityVerdict) -> None:
    if verdict.value is None:
        return
    items = verdict.value.encode()
    try:
        again = decode_value(items)
        verdict.to_resolution("c1")  # resolved_ids keeps its shape: distinct non-blank strings
    except ContractError as error:
        _fail("INV-C0-9", f"{type(error).__name__}: {error}")
    if again != verdict.value or type(again) is not type(verdict.value) or again.encode() != items:
        _fail("INV-C0-9", "decode(encode(v)) != v")


def _variants(window, chain: bool):
    lengths = (len(window.left), len(window.right))
    if not chain:
        yield 0, 0
        return
    for left in range(lengths[0] + 1):
        for right in range(lengths[1] + 1):
            if (left, right) != lengths:
                yield left, right


def _check_monotone(authority, evidence, ctx, verdict, chain: bool) -> None:
    if verdict.decision is not ResolutionDecision.RESOLVED:
        return
    for left, right in _variants(evidence.window, chain):
        reduced = replace(evidence, window=evidence.window.truncated(left, right))
        other = _call(authority, reduced, ctx)
        _check_shape(authority, other, reduced, ctx)
        if other.decision is not ResolutionDecision.RESOLVED or other.value != verdict.value:
            _fail("INV-C0-7", f"RESOLVED with context, {other.decision.value} with context "
                              f"({left},{right}) of ({len(evidence.window.left)},"
                              f"{len(evidence.window.right)})")


def _gap_variants(window):
    """The window with each gap, then all of them, replaced by ``()`` and by ``("x",)``."""
    gaps = window.gaps
    for filler in _GAP_FILLERS:
        for index in range(len(gaps)):
            yield replace(window, gaps=gaps[:index] + (filler,) + gaps[index + 1:])
        if len(gaps) > 1:
            yield replace(window, gaps=(filler,) * len(gaps))


def _check_gap_monotone(authority, evidence, ctx, verdict) -> None:
    """INV-C0-7b: a gap may withdraw or condition authority, never change the resolved value."""
    if verdict.decision is not ResolutionDecision.RESOLVED:
        return
    for window in _gap_variants(evidence.window):
        if window.gaps == evidence.window.gaps:
            continue
        changed = replace(evidence, window=window)
        other = _call(authority, changed, ctx)
        _check_shape(authority, other, changed, ctx)
        if other.decision is ResolutionDecision.RESOLVED and other.value != verdict.value:
            _fail("INV-C0-7b", f"the value changed when the gaps became {window.gaps!r}")


def _check_polarity(authority, evidence, ctx) -> None:
    positive = replace(evidence, polarity=Polarity.POSITIVE)
    negative = replace(evidence, polarity=Polarity.NEGATIVE)
    pos, neg = _call(authority, positive, ctx), _call(authority, negative, ctx)
    _check_shape(authority, pos, positive, ctx)
    _check_shape(authority, neg, negative, ctx)
    _check_round_trip(neg)
    name = evidence.dimension.value
    if neg.decision is ResolutionDecision.RESOLVED:
        if not neg.policies:
            _fail("NEGATION_IGNORED", "a RESOLVED NEGATIVE needs the policy that covers it")
        if pos.decision is ResolutionDecision.RESOLVED and pos.value == neg.value:
            _fail("NEGATION_IGNORED", "NEGATIVE resolved to the POSITIVE value")
    elif pos.decision is ResolutionDecision.RESOLVED and neg.reason != f"NEGATION_UNSUPPORTED:{name}":
        _fail("NEGATION_UNSUPPORTED_REQUIRED", f"{neg.reason!r} after a RESOLVED positive")


def _check_declared_scope(authority, evidence, ctx) -> None:
    probes = [replace(evidence, operator=_UNDECLARED_OPERATOR)]
    other = next((item for item in SemanticDimension
                  if item.value in AUTHORITY_DIMENSION_NAMES and item not in authority.dimensions), None)
    if other is not None:
        probes.append(replace(evidence, dimension=other,
                              level=GeoLevel.DEPARTMENT if other is SemanticDimension.GEOGRAPHY else None))
    for probe in probes:
        verdict = _call(authority, probe, ctx)
        _check_shape(authority, verdict, probe, ctx)
        if verdict.decision is ResolutionDecision.RESOLVED:
            _fail("UNDECLARED_SCOPE_RESOLVED", f"{probe.dimension.value}/{probe.operator}")


def assert_conforms(authority, cases: Sequence[ConformanceCase], *, monotone_chain: bool = True,
                    require_resolved: bool = True) -> ConformanceReport:
    _check_declaration(authority)
    cases = tuple(cases)
    if not cases or any(type(item) is not ConformanceCase for item in cases):
        _fail("CASES", "assert_conforms needs ConformanceCase items")
    resolved = calls = 0
    for item in cases:
        evidence, ctx = item.evidence, item.ctx
        if evidence.dimension not in authority.dimensions or evidence.operator not in authority.operators:
            _fail("CASE_OUTSIDE_DECLARED_SCOPE", f"{evidence.dimension.value}/{evidence.operator}")
        first = _call(authority, evidence, ctx)
        _check_shape(authority, first, evidence, ctx)
        second = _call(authority, evidence, ctx)
        if first != second:
            _fail("INV-C0-8", "two calls on the same input returned different verdicts")
        _check_round_trip(first)
        _check_monotone(authority, evidence, ctx, first, monotone_chain)
        _check_gap_monotone(authority, evidence, ctx, first)
        _check_polarity(authority, evidence, ctx)
        _check_declared_scope(authority, evidence, ctx)
        resolved += first.decision is ResolutionDecision.RESOLVED
        calls += 1
    if require_resolved and not resolved:
        _fail("NO_RESOLVED_CASE", "INV-C0-7 and INV-C0-9 are vacuous without a RESOLVED case")
    return ConformanceReport(len(cases), resolved, calls)


__all__ = ["REFERENCE_INSTANT", "WINDOW_TOKENS", "ConformanceCase", "ConformanceFailure",
           "ConformanceReport", "PurityViolation", "assert_conforms", "case", "default_context",
           "evidence_from_text", "pure_environment"]
