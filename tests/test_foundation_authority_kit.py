"""C1.1 — the conformance kit proves itself: one conforming toy and many it must reject."""

from __future__ import annotations

import datetime as dt
import socket
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from itertools import count

import pytest

from app.config import get_settings
from app.core.foundation_authority_contract import (
    AUTHORITY_DIMENSION_NAMES, VALUE_ALIASES, AuthorityContext, AuthorityReceipt, AuthorityVerdict,
    Comparator, GeoValue, ModalityValue, MoneyValue, StateValue, TimeValue, TopicValue,
)
from app.core.foundation_geo_level import GeoLevel
from app.core.interpretation_contract import Effect, Polarity, ResolutionDecision
from app.core.semantic_dimensions import SemanticDimension
from tests.foundation_authority_kit import (
    ConformanceFailure, PurityViolation, assert_conforms, case, default_context, evidence_from_text,
    pure_environment,
)

TIME = SemanticDimension.TIME
RESOLVED, CLARIFY, BLOCK = (ResolutionDecision.RESOLVED, ResolutionDecision.CLARIFY,
                            ResolutionDecision.BLOCK)
QUALIFIERS = frozenset({"desde", "hasta", "entre", "antes", "despues", "agosto", "enero", "mayo"})


def raw_verdict(**fields) -> AuthorityVerdict:
    """A verdict that bypasses AuthorityVerdict.__post_init__, as a broken authority could."""
    base = dict(decision=RESOLVED, authority="toy@1", value=None, reason=None,
                effect=Effect.EQUIVALENT, policies=(), receipt=AuthorityReceipt(), detail=None)
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
        if quote is None:  # two different phrases: a single-phrase dimension reads nothing
            return self._v(CLARIFY, reason="RESIDUE_UNREAD:TIME")
        if not (quote.isdigit() and len(quote) == 4):
            return self._v(CLARIFY, reason="UNRESOLVED_HOST_DIMENSION:TIME")
        if QUALIFIERS & {*evidence.window.left, *evidence.window.right}:
            return self._v(CLARIFY, reason="UNCITED_QUALIFIER:TIME")
        year = int(quote)
        return self._v(RESOLVED, value=TimeValue(date(year, 1, 1), date(year + 1, 1, 1), "year", False),
                       receipt=AuthorityReceipt(("year.plain",), 1))


class ToyModality(Toy):
    """CONFORMING: honours a negation with `excluded` and the policy that covers it."""

    authority_id, version = "toy-mod", "1"
    dimensions, operators = frozenset({SemanticDimension.MODALITY}), frozenset({"is"})

    def resolve(self, evidence, ctx):
        if (refused := self._scope(evidence)) is not None:
            return refused
        if (evidence.window.quote or "").lower() != "contratacion directa":
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
        if evidence.level is not GeoLevel.MUNICIPALITY or (evidence.window.quote or "").lower() != "cali":
            return self._v(CLARIFY, reason="LEVEL_MISMATCH:GEOGRAPHY")
        return self._v(RESOLVED, value=CALI)


CALI = GeoValue(GeoLevel.MUNICIPALITY, "CALI", "VALLE DEL CAUCA", "76001", (), False)


class ToyGeoAnyLevel(Toy):
    """CONFORMING: an UNKNOWN evidence level resolves to a concrete one when the quote is unambiguous."""

    authority_id, version = "toy-geo-any", "1"
    dimensions, operators = frozenset({SemanticDimension.GEOGRAPHY}), frozenset({"is"})

    def resolve(self, evidence, ctx):
        if (refused := self._scope(evidence)) is not None:
            return refused
        if evidence.polarity is Polarity.NEGATIVE:
            return self._v(CLARIFY, reason="NEGATION_UNSUPPORTED:GEOGRAPHY")
        if evidence.window.quote != "Cali" or evidence.level not in (GeoLevel.UNKNOWN, GeoLevel.MUNICIPALITY):
            return self._v(CLARIFY, reason="LEVEL_MISMATCH:GEOGRAPHY")
        return self._v(RESOLVED, value=CALI)


class ToyState(Toy):
    """CONFORMING: literals per (dataset, column) for every dataset of the context; the funnel
    «publicados o abiertos» (S3, ADR-100) resolves only with the bare «o» in the gap, and the VALUE
    never depends on the gap (INV-C0-7b)."""

    authority_id, version = "toy-state", "1"
    dimensions, operators = frozenset({SemanticDimension.STATE}), frozenset({"is"})

    def literals(self, ctx):
        return {name: {"estado": ("Abierto",)} for name in ctx.datasets}

    def resolve(self, evidence, ctx):
        if (refused := self._scope(evidence)) is not None:
            return refused
        window, negative = evidence.window, evidence.polarity is Polarity.NEGATIVE
        policies = ("state-complement@1",) if negative else ()
        if window.segments == ("abiertos",):
            pass
        elif window.segments == ("publicados", "abiertos"):
            if window.gaps != (("o",),):
                return self._v(CLARIFY, reason="RESIDUE_UNREAD:STATE", detail="STATE_COMPOSITION_UNSUPPORTED")
            if negative:
                return self._v(CLARIFY, reason="NEGATION_UNSUPPORTED:STATE")
            policies = ("adr-100@1",)
        else:
            return self._v(CLARIFY, reason="UNRESOLVED_HOST_DIMENSION:STATE")
        return self._v(RESOLVED, value=StateValue.of("open", self.literals(ctx), negative), policies=policies)


class ToyMoney(Toy):
    """CONFORMING: a negation is not honoured (the comparator absorbs it) and the aliases stay in VALUE."""

    authority_id, version = "toy-money", "1"
    dimensions, operators = frozenset({SemanticDimension.VALUE}), frozenset({"gt"})

    def resolve(self, evidence, ctx):
        if (refused := self._scope(evidence)) is not None:
            return refused
        if evidence.polarity is Polarity.NEGATIVE:
            return self._v(CLARIFY, reason="NEGATION_UNSUPPORTED:VALUE")
        quote = evidence.window.quote or ""
        if not quote.isdigit():
            return self._v(CLARIFY, reason="VALUE_OPERATOR_UNGROUNDED")
        return self._v(RESOLVED, value=MoneyValue(int(quote), Comparator.GT))


class ToyThisYear(Toy):
    """CONFORMING: «hoy» is the Colombian local date of the injected instant, never a clock read."""

    authority_id, version = "toy-this-year", "1"
    dimensions, operators = frozenset({TIME}), frozenset({"during_year"})

    def resolve(self, evidence, ctx):
        if (refused := self._scope(evidence)) is not None:
            return refused
        if evidence.polarity is Polarity.NEGATIVE:
            return self._v(CLARIFY, reason="NEGATION_UNSUPPORTED:TIME")
        year = ctx.local_date().year
        return self._v(RESOLVED, value=TimeValue(date(year, 1, 1), date(year + 1, 1, 1), "year", False))


YEAR = dict(dimension=TIME, operator="during_year")
YEAR_CASES = [
    case("contratos de 2025 en Cali", "2025", **YEAR),            # plain: RESOLVED, with context
    case("contratos en agosto de 2025", "2025", **YEAR),          # qualified: CLARIFY
    case("busco contratos de ayer", "ayer", **YEAR),              # not a year: CLARIFY
    case("obras 2024", "2024", **YEAR),                            # no right context
    case("contratos de 2025 y 2026", ["2025", "2026"], **YEAR),   # two different phrases: no quote
]


def test_a_conforming_authority_passes_and_the_report_counts_what_ran():
    report = assert_conforms(ToyYear(), YEAR_CASES)
    assert (report.cases, report.resolved) == (5, 2) and report.calls == 5


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
            return self._v(RESOLVED, value=TimeValue(date(2025, 1, 1), None, "year", False))
        return self._v(CLARIFY, reason="UNCITED_QUALIFIER:TIME")


class ContextChangesTheValue(ToyYear):
    def resolve(self, evidence, ctx):
        year = 2025 + len(evidence.window.left)
        return self._v(RESOLVED, value=TimeValue(date(year, 1, 1), None, "year", False))


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
        return self._v(RESOLVED, value=TimeValue(date.today(), None, "year", False))


class ReadsTheClockViaModule(ToyYear):
    def resolve(self, evidence, ctx):
        dt.datetime.now(timezone.utc)
        return super().resolve(evidence, ctx)


class ReadsTheDateViaModule(ToyYear):
    def resolve(self, evidence, ctx):
        return self._v(RESOLVED, value=TimeValue(dt.date.today(), None, "year", False))


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
        return self._v(RESOLVED, value=TimeValue(date(year, 1, 1), None, "year", False))


class ValueThatDoesNotRoundTrip(ToyYear):
    """The value is a member of the union but its state cannot be decoded back (INV-C0-9)."""

    def resolve(self, evidence, ctx):
        if (refused := self._scope(evidence)) is not None:
            return refused
        if evidence.polarity is Polarity.NEGATIVE:  # only the positive path is broken
            return self._v(CLARIFY, reason="NEGATION_UNSUPPORTED:TIME")
        broken = object.__new__(TimeValue)
        for name, value in (("start", date(2025, 6, 1)), ("end_exclusive", date(2025, 1, 1)),
                            ("precision", "year"), ("excluded", False)):
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
        return raw_verdict(decision=CLARIFY, value=TimeValue(date(2025, 1, 1), None, "year", False),
                           reason="UNCITED_QUALIFIER:TIME")


class ValueOutsideTheUnion(ToyYear):
    def resolve(self, evidence, ctx):
        return raw_verdict(decision=RESOLVED, value=("time@1", "start=2025-01-01"))


class ValueOfTheWrongType(ToyYear):
    def resolve(self, evidence, ctx):
        return self._v(RESOLVED, value=TopicValue("2025", False, False))


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
        return self._v(RESOLVED, value=GeoValue(GeoLevel.DEPARTMENT, "VALLE", None, "76", (), False))


class GapChangesTheValue(ToyState):
    """What a gap says picks the value: INV-C0-7b (it may withdraw or condition, never change)."""

    def resolve(self, evidence, ctx):
        if evidence.window.segments == ("publicados", "abiertos"):
            if evidence.window.gaps == (("o",),):
                return self._v(RESOLVED, value=StateValue.of("open", self.literals(ctx)),
                               policies=("adr-100@1",))
            return self._v(RESOLVED, value=StateValue.of("published", self.literals(ctx)))
        return super().resolve(evidence, ctx)


class EmptyGapPicksAnotherValue(ToyState):
    """With the gap emptied the same segments resolve to a different value: INV-C0-7b."""

    def resolve(self, evidence, ctx):
        if evidence.window.segments == ("publicados", "abiertos"):
            klass = "open" if evidence.window.gaps == ((),) else "closed"
            return self._v(RESOLVED, value=StateValue.of(klass, self.literals(ctx)))
        return super().resolve(evidence, ctx)


class AnyWordGapPicksAnotherValue(ToyState):
    """Resolves the funnel with the bare «o» and with an unseen gap word, but to ANOTHER value there."""

    def resolve(self, evidence, ctx):
        if evidence.window.segments == ("publicados", "abiertos") and evidence.window.gaps == (("x",),):
            return self._v(RESOLVED, value=StateValue.of("published", self.literals(ctx)))
        return super().resolve(evidence, ctx)


class IgnoresExcluded(ToyModality):
    """Honours the negation in the verdict (policy) but not in the value: INV-C1.1-EXCL."""

    def resolve(self, evidence, ctx):
        verdict = super().resolve(evidence, ctx)
        if verdict.value is None:
            return verdict
        return replace(verdict, value=ModalityValue(("CD",), None, False))


class ExcludedWithoutNegation(ToyModality):
    def resolve(self, evidence, ctx):
        verdict = super().resolve(evidence, ctx)
        if verdict.value is None:
            return verdict
        return replace(verdict, value=ModalityValue(("CD",), None, True), policies=("modality-neg@1",))


class StateLiteralsOutsideTheContext(ToyState):
    def literals(self, ctx):
        return {"dataset-z": {"estado": ("Abierto",)}}


class AliasOfAnotherDimension(ToyYear):
    """A VALUE_* alias for a TIME verdict: the aliases are VALUE's, not everyone's."""

    def resolve(self, evidence, ctx):
        return self._v(CLARIFY, reason="VALUE_OPERATOR_AMBIGUOUS")


class DetailOnAResolvedVerdict(ToyYear):
    def resolve(self, evidence, ctx):
        verdict = super().resolve(evidence, ctx)
        return raw_verdict(decision=RESOLVED, value=verdict.value, detail="WHATEVER") \
            if verdict.value is not None else verdict


class MalformedDetail(ToyYear):
    def resolve(self, evidence, ctx):
        return raw_verdict(decision=CLARIFY, reason="UNCITED_QUALIFIER:TIME", detail="not a code")


class IgnoresNegationOfAMoneyValue(ToyMoney):
    """Resolves a NEGATIVE to the very value of the POSITIVE and claims a policy covers it."""

    def resolve(self, evidence, ctx):
        verdict = super().resolve(replace(evidence, polarity=Polarity.POSITIVE), ctx)
        if evidence.polarity is Polarity.NEGATIVE and verdict.value is not None:
            return replace(verdict, policies=("money-neg@1",))
        return verdict


class BadDeclaration(ToyYear):
    dimensions = frozenset()


GEO_CASE = case("contratos en Cali de obras", "Cali", dimension=SemanticDimension.GEOGRAPHY, operator="is",
                level=GeoLevel.MUNICIPALITY)
GEO_UNKNOWN_CASE = case("contratos en Cali de obras", "Cali", dimension=SemanticDimension.GEOGRAPHY,
                        operator="is", level=GeoLevel.UNKNOWN)
STATE = dict(dimension=SemanticDimension.STATE, operator="is")
S3_CASE = case("procesos publicados o abiertos en Cali", ["publicados", "abiertos"], **STATE)
STATE_CASES = [
    case("procesos abiertos en Cali", "abiertos", **STATE),
    S3_CASE,
    case("procesos publicados y abiertos", ["publicados", "abiertos"], **STATE),  # another gap: CLARIFY
    case("procesos publicados, abiertos", ["publicados", "abiertos"], **STATE),   # an empty gap
    case("procesos cerrados", "cerrados", **STATE),
    case("procesos no abiertos", "abiertos", polarity=Polarity.NEGATIVE, **STATE),
]
MONEY_CASES = [
    case("contratos de mas de 500000000", "500000000", dimension=SemanticDimension.VALUE, operator="gt"),
    case("contratos de mas de mil", "mil", dimension=SemanticDimension.VALUE, operator="gt"),
]
MOD_CASE = case("no contratacion directa", "contratacion directa", dimension=SemanticDimension.MODALITY,
                operator="is", polarity=Polarity.NEGATIVE)
MOD_POSITIVE_CASE = case("contratacion directa en Cali", "contratacion directa",
                         dimension=SemanticDimension.MODALITY, operator="is")

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
    (IgnoresNegation, YEAR_CASES, "INV-C1.1-EXCL"),  # TimeValue has `excluded`: caught there first
    (NegatesWithoutPolicy, [MOD_CASE], "NEGATION_IGNORED"),
    (NegationWithdrawnForAnotherReason, YEAR_CASES, "NEGATION_UNSUPPORTED_REQUIRED"),
    (WrongGeoLevel, [GEO_CASE], "LEVEL_MISMATCH"),
    (GapChangesTheValue, STATE_CASES, "INV-C0-7b"),
    (EmptyGapPicksAnotherValue, STATE_CASES, "INV-C0-7b"),
    (AnyWordGapPicksAnotherValue, STATE_CASES, "INV-C0-7b"),
    (IgnoresExcluded, [MOD_CASE], "INV-C1.1-EXCL"),
    (ExcludedWithoutNegation, [MOD_POSITIVE_CASE], "INV-C1.1-EXCL"),
    (StateLiteralsOutsideTheContext, STATE_CASES, "INV-C1.1-DS"),
    (AliasOfAnotherDimension, YEAR_CASES, "REASON_DIMENSION_MISMATCH"),
    (DetailOnAResolvedVerdict, YEAR_CASES, "RESOLVED_WITH_DETAIL"),
    (MalformedDetail, YEAR_CASES, "VERDICT_DETAIL"),
    (IgnoresNegationOfAMoneyValue, MONEY_CASES, "NEGATION_IGNORED"),
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


def test_conforming_state_money_geo_and_clock_authorities_pass():
    """S3 (two cites, a gap), literals per dataset, an UNKNOWN level resolved, the local date injected."""
    ctx = default_context(("dataset-a", "dataset-b"))
    assert assert_conforms(ToyState(), STATE_CASES).resolved == 3  # abiertos, the S3 funnel, no abiertos
    assert_conforms(ToyState(), [replace(item, ctx=ctx) for item in STATE_CASES])
    assert assert_conforms(ToyMoney(), MONEY_CASES).resolved == 1
    assert_conforms(ToyGeoAnyLevel(), [GEO_UNKNOWN_CASE, GEO_CASE])
    assert_conforms(ToyThisYear(), [case("contratos de este ano", "este ano", ctx=ctx, **YEAR)])


def test_the_s3_funnel_resolves_only_with_the_bare_o_and_the_gap_is_never_the_value():
    verdict = ToyState().resolve(S3_CASE.evidence, S3_CASE.ctx)
    assert verdict.decision is RESOLVED and verdict.policies == ("adr-100@1",)
    assert verdict.value.canonical_class == "open"
    other = STATE_CASES[2]  # «publicados y abiertos»
    refused = ToyState().resolve(other.evidence, other.ctx)
    assert refused.decision is CLARIFY and refused.reason == "RESIDUE_UNREAD:STATE"
    assert refused.detail == "STATE_COMPOSITION_UNSUPPORTED"
    assert S3_CASE.evidence.window.quote is None and S3_CASE.evidence.window.gaps == (("o",),)


def test_case_builds_a_window_from_several_cites_and_from_a_repeated_phrase():
    repeated = evidence_from_text("obras en Cali y obras en Bogota", [("obras", 0), ("obras", 1)],
                                  dimension=SemanticDimension.TOPIC, operator="about")
    assert repeated.window.segments == ("obras", "obras") and repeated.window.quote == "obras"
    assert repeated.window.gaps == (("en", "cali", "y"),)
    pair = evidence_from_text("procesos publicados o abiertos", ["publicados", "abiertos"], **STATE)
    assert pair.window.segments == ("publicados", "abiertos") and pair.window.gaps == (("o",),)
    single = evidence_from_text("obras y obras", "obras", occurrence=1,
                                dimension=SemanticDimension.TOPIC, operator="about")
    assert single.window.segments == ("obras",) and single.window.left == ("obras", "y")


@pytest.mark.parametrize("dimension", [d for d in SemanticDimension if d.value in AUTHORITY_DIMENSION_NAMES],
                         ids=lambda d: d.value)
def test_the_value_aliases_are_accepted_by_the_kit_only_for_the_value_dimension(dimension):
    from tests import foundation_authority_kit as kit

    class Stub:
        authority_id, version = "stub", "1"

    evidence = evidence_from_text("obras", "obras", dimension=dimension, operator="is",
                                  level=GeoLevel.DEPARTMENT if dimension is SemanticDimension.GEOGRAPHY else None)
    for alias in sorted(VALUE_ALIASES):
        verdict = AuthorityVerdict(CLARIFY, "stub@1", reason=alias)
        if dimension is SemanticDimension.VALUE:
            kit._check_shape(Stub(), verdict, evidence, default_context())
        else:
            with pytest.raises(ConformanceFailure) as failure:
                kit._check_shape(Stub(), verdict, evidence, default_context())
            assert failure.value.code == "REASON_DIMENSION_MISMATCH"


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


def test_default_context_is_an_injected_timezone_aware_instant_with_datasets():
    ctx = default_context()
    assert ctx.reference_instant.tzinfo is not None and ctx.datasets == frozenset({"dataset-a"})
    assert isinstance(ctx, AuthorityContext) and ctx.local_date() == date(2026, 9, 30)
    with pytest.raises(Exception, match="EMPTY_CONTEXT_DATASETS"):
        default_context(())  # an empty dataset set is not a context any more


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
