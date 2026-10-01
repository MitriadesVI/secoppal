"""C1.1 — the conformance kit proves itself: one conforming toy and many it must reject."""

from __future__ import annotations

import datetime as dt
import socket
from dataclasses import replace
from datetime import date, datetime, timezone
from itertools import count

import pytest

from app.config import get_settings
from app.core.foundation_authority_contract import (
    AuthorityReceipt, AuthorityVerdict, GeoValue, ModalityValue, TimeValue, TopicValue,
)
from app.core.foundation_geo_level import GeoLevel
from app.core.interpretation_contract import Effect, Polarity, ResolutionDecision
from app.core.semantic_dimensions import SemanticDimension
from tests.foundation_authority_kit import (
    ConformanceFailure, PurityViolation, assert_conforms, case, default_context, pure_environment,
)

TIME = SemanticDimension.TIME
RESOLVED, CLARIFY, BLOCK = (ResolutionDecision.RESOLVED, ResolutionDecision.CLARIFY,
                            ResolutionDecision.BLOCK)
QUALIFIERS = frozenset({"desde", "hasta", "entre", "antes", "despues", "agosto", "enero", "mayo"})


def raw_verdict(**fields) -> AuthorityVerdict:
    """A verdict that bypasses AuthorityVerdict.__post_init__, as a broken authority could."""
    base = dict(decision=RESOLVED, authority="toy@1", value=None, reason=None,
                effect=Effect.EQUIVALENT, policies=(), receipt=AuthorityReceipt())
    verdict = object.__new__(AuthorityVerdict)
    for name, value in {**base, **fields}.items():
        object.__setattr__(verdict, name, value)
    return verdict


class Toy:
    """Shared plumbing of the toys: the verdict builder and the declared-scope guard."""

    def _v(self, decision, **fields):
        return AuthorityVerdict(decision, f"{self.authority_id}@{self.version}", **fields)

    def _scope(self, evidence):
        name = evidence.dimension.value
        if evidence.dimension not in self.dimensions:
            return self._v(CLARIFY, reason=f"UNRESOLVED_HOST_DIMENSION:{name}")
        if evidence.operator not in self.operators:
            return self._v(CLARIFY, reason=f"OPERATOR_EVIDENCE_MISMATCH:{name}")
        return None


class ToyYear(Toy):
    """CONFORMING: a year is RESOLVED unless the context qualifies it (then it only withdraws)."""

    authority_id, version = "toy", "1"
    dimensions, operators = frozenset({TIME}), frozenset({"during_year"})

    def resolve(self, evidence, ctx):
        quote = evidence.window.quote
        if (refused := self._scope(evidence)) is not None:
            return refused
        if evidence.polarity is Polarity.NEGATIVE:
            return self._v(CLARIFY, reason="NEGATION_UNSUPPORTED:TIME")
        if not (quote.isdigit() and len(quote) == 4):
            return self._v(CLARIFY, reason="UNRESOLVED_HOST_DIMENSION:TIME")
        if QUALIFIERS & {*evidence.window.left, *evidence.window.right}:
            return self._v(CLARIFY, reason="UNCITED_QUALIFIER:TIME")
        year = int(quote)
        return self._v(RESOLVED, value=TimeValue(date(year, 1, 1), date(year + 1, 1, 1), "year"),
                       receipt=AuthorityReceipt(("year.plain",), 1))


class ToyModality(Toy):
    """CONFORMING: honours a negation with `excluded` and the policy that covers it."""

    authority_id, version = "toy-mod", "1"
    dimensions, operators = frozenset({SemanticDimension.MODALITY}), frozenset({"is"})

    def resolve(self, evidence, ctx):
        if (refused := self._scope(evidence)) is not None:
            return refused
        if evidence.window.quote.lower() != "contratacion directa":
            return self._v(CLARIFY, reason="UNRESOLVED_HOST_DIMENSION:MODALITY")
        negative = evidence.polarity is Polarity.NEGATIVE
        return self._v(RESOLVED, value=ModalityValue(("CD",), None, negative),
                       policies=("modality-neg@1",) if negative else ())


class ToyGeo(Toy):
    """CONFORMING: the level of the evidence travels into the value."""

    authority_id, version = "toy-geo", "1"
    dimensions, operators = frozenset({SemanticDimension.GEOGRAPHY}), frozenset({"is"})

    def resolve(self, evidence, ctx):
        if (refused := self._scope(evidence)) is not None:
            return refused
        if evidence.polarity is Polarity.NEGATIVE:
            return self._v(CLARIFY, reason="NEGATION_UNSUPPORTED:GEOGRAPHY")
        if evidence.level is not GeoLevel.MUNICIPALITY or evidence.window.quote.lower() != "cali":
            return self._v(CLARIFY, reason="LEVEL_MISMATCH:GEOGRAPHY")
        return self._v(RESOLVED, value=GeoValue(GeoLevel.MUNICIPALITY, "CALI", "76001"))


YEAR = dict(dimension=TIME, operator="during_year")
YEAR_CASES = [
    case("contratos de 2025 en Cali", "2025", **YEAR),            # plain: RESOLVED, with context
    case("contratos en agosto de 2025", "2025", **YEAR),          # qualified: CLARIFY
    case("busco contratos de ayer", "ayer", **YEAR),              # not a year: CLARIFY
    case("obras 2024", "2024", **YEAR),                            # no right context
]


def test_a_conforming_authority_passes_and_the_report_counts_what_ran():
    report = assert_conforms(ToyYear(), YEAR_CASES)
    assert (report.cases, report.resolved) == (4, 2) and report.calls == 4


def test_conforming_authorities_with_negation_policy_and_level_pass():
    assert_conforms(ToyModality(), [case("contratacion directa en Cali", "contratacion directa",
                                         dimension=SemanticDimension.MODALITY, operator="is")])
    assert_conforms(ToyGeo(), [case("contratos en Cali de obras", "Cali", dimension=SemanticDimension.GEOGRAPHY,
                                    operator="is", level=GeoLevel.MUNICIPALITY)])


# ---- non-conforming authorities: each must be rejected, with the expected code ---------------------
class ContextCreatesResolved(ToyYear):
    """RESOLVED only BECAUSE of the context: INV-C0-7."""

    def resolve(self, evidence, ctx):
        if evidence.window.left or evidence.window.right:
            return self._v(RESOLVED, value=TimeValue(date(2025, 1, 1), None, "year"))
        return self._v(CLARIFY, reason="UNCITED_QUALIFIER:TIME")


class ContextChangesTheValue(ToyYear):
    def resolve(self, evidence, ctx):
        year = 2025 + len(evidence.window.left)
        return self._v(RESOLVED, value=TimeValue(date(year, 1, 1), None, "year"))


class ChainBreaker(ToyYear):
    """Resolves with the full context and with none, but not in between (only the chain sees it)."""

    def resolve(self, evidence, ctx):
        window = evidence.window
        if (refused := self._scope(evidence)) is not None:
            return refused
        if 0 < len(window.left) < 2 and not window.right:
            return self._v(CLARIFY, reason="UNCITED_QUALIFIER:TIME")
        return super().resolve(evidence, ctx)


class ReadsTheClock(ToyYear):
    def resolve(self, evidence, ctx):
        return self._v(RESOLVED, value=TimeValue(date.today(), None, "year"))


class ReadsTheClockViaModule(ToyYear):
    def resolve(self, evidence, ctx):
        dt.datetime.now(timezone.utc)
        return super().resolve(evidence, ctx)


class ReadsTheDateViaModule(ToyYear):
    def resolve(self, evidence, ctx):
        return self._v(RESOLVED, value=TimeValue(dt.date.today(), None, "year"))


class SwallowsTheClock(ToyYear):
    def resolve(self, evidence, ctx):
        try:
            datetime.now()
        except Exception:  # noqa: BLE001 - the kit records the touch even when it is swallowed
            pass
        return super().resolve(evidence, ctx)


class OpensASocket(ToyYear):
    def resolve(self, evidence, ctx):
        try:
            socket.create_connection(("127.0.0.1", 9), timeout=0.01)
        except Exception:  # noqa: BLE001
            pass
        return super().resolve(evidence, ctx)


class ConnectsASocket(ToyYear):
    def resolve(self, evidence, ctx):
        probe = socket.socket()
        try:
            probe.connect(("127.0.0.1", 9))
        except Exception:  # noqa: BLE001
            pass
        finally:
            probe.close()
        return super().resolve(evidence, ctx)


class ReadsSettings(ToyYear):
    def resolve(self, evidence, ctx):
        get_settings()
        return super().resolve(evidence, ctx)


class NonDeterministic(ToyYear):
    ticks = count()

    def resolve(self, evidence, ctx):
        year = 2000 + next(self.ticks)
        return self._v(RESOLVED, value=TimeValue(date(year, 1, 1), None, "year"))


class ValueThatDoesNotRoundTrip(ToyYear):
    """The value is a member of the union but its state cannot be decoded back (INV-C0-9)."""

    def resolve(self, evidence, ctx):
        if (refused := self._scope(evidence)) is not None:
            return refused
        if evidence.polarity is Polarity.NEGATIVE:  # only the positive path is broken
            return self._v(CLARIFY, reason="NEGATION_UNSUPPORTED:TIME")
        broken = object.__new__(TimeValue)
        for name, value in (("start", date(2025, 6, 1)), ("end_exclusive", date(2025, 1, 1)),
                            ("precision", "year")):
            object.__setattr__(broken, name, value)
        return raw_verdict(decision=RESOLVED, value=broken)


class Raises(ToyYear):
    def resolve(self, evidence, ctx):
        raise KeyError("no index")


class ReasonOutsideTheSet(ToyYear):
    def resolve(self, evidence, ctx):
        return raw_verdict(decision=CLARIFY, reason="MY_OWN_REASON")


class ReasonForAnotherDimension(ToyYear):
    def resolve(self, evidence, ctx):
        return raw_verdict(decision=CLARIFY, reason="UNCITED_QUALIFIER:BUYER")


class ResolvedWithoutValue(ToyYear):
    def resolve(self, evidence, ctx):
        return raw_verdict(decision=RESOLVED, value=None)


class ValueWithoutResolved(ToyYear):
    def resolve(self, evidence, ctx):
        return raw_verdict(decision=CLARIFY, value=TimeValue(date(2025, 1, 1), None, "year"),
                           reason="UNCITED_QUALIFIER:TIME")


class ValueOutsideTheUnion(ToyYear):
    def resolve(self, evidence, ctx):
        return raw_verdict(decision=RESOLVED, value=("time@1", "start=2025-01-01"))


class ValueOfTheWrongType(ToyYear):
    def resolve(self, evidence, ctx):
        return self._v(RESOLVED, value=TopicValue("2025", False))


class WrongAuthorityTag(ToyYear):
    def resolve(self, evidence, ctx):
        return AuthorityVerdict(CLARIFY, "other@9", reason="UNCITED_QUALIFIER:TIME")


class IgnoresNegation(ToyYear):
    def resolve(self, evidence, ctx):
        return super().resolve(replace(evidence, polarity=Polarity.POSITIVE), ctx)


class NegatesWithoutPolicy(ToyModality):
    def resolve(self, evidence, ctx):
        verdict = super().resolve(evidence, ctx)
        return replace(verdict, policies=()) if verdict.value is not None else verdict


class NegationWithdrawnForAnotherReason(ToyYear):
    def resolve(self, evidence, ctx):
        if evidence.polarity is Polarity.NEGATIVE:
            return self._v(CLARIFY, reason="UNRESOLVED_HOST_DIMENSION:TIME")
        return super().resolve(evidence, ctx)


class ResolvesAnyOperator(ToyYear):
    """Checks the dimension but not the operator it declared."""

    def resolve(self, evidence, ctx):
        if evidence.dimension not in self.dimensions:
            return self._scope(evidence)
        return super().resolve(replace(evidence, operator="during_year"), ctx)


class ResolvesAnyDimension(ToyYear):
    """Checks the operator but not the dimension it declared."""

    def resolve(self, evidence, ctx):
        if evidence.operator not in self.operators:
            return self._scope(evidence)
        return super().resolve(replace(evidence, dimension=TIME, level=None), ctx)


class WrongGeoLevel(ToyGeo):
    def resolve(self, evidence, ctx):
        return self._v(RESOLVED, value=GeoValue(GeoLevel.DEPARTMENT, "VALLE", "76"))


class BadDeclaration(ToyYear):
    dimensions = frozenset()


GEO_CASE = case("contratos en Cali de obras", "Cali", dimension=SemanticDimension.GEOGRAPHY, operator="is",
                level=GeoLevel.MUNICIPALITY)
MOD_CASE = case("no contratacion directa", "contratacion directa", dimension=SemanticDimension.MODALITY,
                operator="is", polarity=Polarity.NEGATIVE)

REJECTED = [
    (ContextCreatesResolved, YEAR_CASES, "INV-C0-7"),
    (ContextChangesTheValue, YEAR_CASES, "INV-C0-7"),
    (ChainBreaker, YEAR_CASES, "INV-C0-7"),
    (ReadsTheClock, YEAR_CASES, "INV-C0-8"),
    (ReadsTheClockViaModule, YEAR_CASES, "INV-C0-8"),
    (ReadsTheDateViaModule, YEAR_CASES, "INV-C0-8"),
    (ValueThatDoesNotRoundTrip, YEAR_CASES, "INV-C0-9"),
    (SwallowsTheClock, YEAR_CASES, "INV-C0-8"),
    (OpensASocket, YEAR_CASES, "INV-C0-8"),
    (ConnectsASocket, YEAR_CASES, "INV-C0-8"),
    (ReadsSettings, YEAR_CASES, "INV-C0-8"),
    (NonDeterministic, YEAR_CASES, "INV-C0-8"),
    (Raises, YEAR_CASES, "AUTHORITY_RAISED"),
    (ReasonOutsideTheSet, YEAR_CASES, "REASON_OUTSIDE_CLOSED_SET"),
    (ReasonForAnotherDimension, YEAR_CASES, "REASON_DIMENSION_MISMATCH"),
    (ResolvedWithoutValue, YEAR_CASES, "RESOLVED_IFF_VALUE"),
    (ValueWithoutResolved, YEAR_CASES, "RESOLVED_IFF_VALUE"),
    (ValueOutsideTheUnion, YEAR_CASES, "VALUE_OUTSIDE_UNION"),
    (ValueOfTheWrongType, YEAR_CASES, "VALUE_TYPE_MISMATCH"),
    (WrongAuthorityTag, YEAR_CASES, "VERDICT_AUTHORITY"),
    (IgnoresNegation, YEAR_CASES, "NEGATION_IGNORED"),
    (NegatesWithoutPolicy, [MOD_CASE], "NEGATION_IGNORED"),
    (NegationWithdrawnForAnotherReason, YEAR_CASES, "NEGATION_UNSUPPORTED_REQUIRED"),
    (WrongGeoLevel, [GEO_CASE], "LEVEL_MISMATCH"),
    (ResolvesAnyOperator, YEAR_CASES, "UNDECLARED_SCOPE_RESOLVED"),
    (ResolvesAnyDimension, YEAR_CASES, "VALUE_TYPE_MISMATCH"),
    (BadDeclaration, YEAR_CASES, "AUTHORITY_DECLARATION"),
]


@pytest.mark.parametrize("authority_type, cases, code", REJECTED,
                         ids=[item[0].__name__ for item in REJECTED])
def test_the_kit_rejects_every_non_conforming_authority(authority_type, cases, code):
    with pytest.raises(ConformanceFailure) as failure:
        assert_conforms(authority_type(), cases)
    assert failure.value.code == code, failure.value


def test_the_literal_inv_c0_7_check_misses_what_only_the_chain_catches():
    with pytest.raises(ConformanceFailure):
        assert_conforms(ChainBreaker(), YEAR_CASES)
    assert_conforms(ChainBreaker(), YEAR_CASES, monotone_chain=False)  # full -> stripped is monotone


def test_a_kit_run_without_a_resolved_case_is_vacuous_and_refused():
    with pytest.raises(ConformanceFailure) as failure:
        assert_conforms(ToyYear(), YEAR_CASES[1:3])
    assert failure.value.code == "NO_RESOLVED_CASE"
    assert_conforms(ToyYear(), YEAR_CASES[1:3], require_resolved=False)


def test_the_kit_refuses_empty_or_foreign_cases_and_cases_outside_the_declared_scope():
    for bad in ([], [object()]):
        with pytest.raises(ConformanceFailure) as failure:
            assert_conforms(ToyYear(), bad)
        assert failure.value.code == "CASES"
    with pytest.raises(ConformanceFailure) as failure:
        assert_conforms(ToyYear(), [GEO_CASE])
    assert failure.value.code == "CASE_OUTSIDE_DECLARED_SCOPE"


def test_the_contract_itself_refuses_what_the_kit_would_otherwise_catch():
    """Defence in depth: the broken verdicts above cannot even be built through the contract."""
    with pytest.raises(Exception, match="RESOLVED_WITHOUT_VALUE"):
        AuthorityVerdict(RESOLVED, "toy@1")
    with pytest.raises(Exception, match="REASON_OUTSIDE_CLOSED_SET"):
        AuthorityVerdict(CLARIFY, "toy@1", reason="MY_OWN_REASON")
    for authority_type in (ResolvedWithoutValue, ReasonOutsideTheSet):
        verdict = authority_type().resolve(YEAR_CASES[0].evidence, YEAR_CASES[0].ctx)
        assert type(verdict) is AuthorityVerdict  # the bypass produces the invalid object the kit must see


def test_the_sandbox_is_restored_after_every_run_even_a_failing_one():
    real_create, real_connect, real_settings = socket.create_connection, socket.socket.connect, get_settings
    with pytest.raises(ConformanceFailure):
        assert_conforms(ReadsTheClock(), YEAR_CASES)
    assert socket.create_connection is real_create and socket.socket.connect is real_connect
    assert dt.date.today() == date.today() and isinstance(dt.datetime.now(), datetime)
    import app.config
    assert app.config.get_settings is real_settings and get_settings is real_settings
    assert isinstance(date(2025, 1, 1), date) and type(date(2025, 1, 1)) is date


def test_default_context_is_an_injected_timezone_aware_instant():
    ctx = default_context()
    assert ctx.reference_instant.tzinfo is not None and ctx.datasets == frozenset({"dataset-a"})


def test_the_sandbox_guards_every_entry_point_and_records_the_touch_even_when_swallowed():
    entry_points = [
        ("socket.create_connection", lambda: socket.create_connection(("127.0.0.1", 9), timeout=0.01)),
        ("socket.getaddrinfo", lambda: socket.getaddrinfo("localhost", 80)),
        ("socket.gethostbyname", lambda: socket.gethostbyname("localhost")),
        ("socket.gethostbyname_ex", lambda: socket.gethostbyname_ex("localhost")),
        ("socket.connect", lambda: socket.socket().connect(("127.0.0.1", 9))),
        ("socket.connect_ex", lambda: socket.socket().connect_ex(("127.0.0.1", 9))),
        ("date.today", lambda: date.today()),
        ("date.today", lambda: dt.date.today()),
        ("datetime.now", lambda: datetime.now()),
        ("datetime.now", lambda: dt.datetime.now()),
        ("datetime.utcnow", lambda: datetime.utcnow()),
        ("datetime.today", lambda: datetime.today()),
        ("get_settings", lambda: get_settings()),
    ]
    for name, call in entry_points:
        with pure_environment(ToyYear()) as touched:
            with pytest.raises(PurityViolation):
                call()
        assert touched == [name], (name, touched)
    with pure_environment(ToyYear()) as touched:  # pure constructors keep working inside the sandbox
        assert date(2025, 1, 1) + (date(2025, 1, 2) - date(2025, 1, 1)) == date(2025, 1, 2)
        assert datetime(2025, 1, 1, tzinfo=timezone.utc).year == 2025
        assert isinstance(date(2025, 1, 1), date) and date.fromisoformat("2025-01-01").year == 2025
        assert dt.datetime.fromisoformat("2025-01-01T00:00:00+00:00").tzinfo is not None
    assert touched == []
