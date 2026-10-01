"""C1.1 — the frozen authority contract, the evidence window and the registry (no runtime change)."""

from __future__ import annotations

import dataclasses
import re
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from app.core import foundation_messages
from app.core.foundation_authority_contract import (
    AUTHORITY_DIMENSION_NAMES, AUTHORITY_REASONS, MAX_TOP_N, STATE_CLASSES, VALUE_ALIASES,
    VALUE_TYPES, WINDOW_TOKENS, ActorValue, AuthorityContext, AuthorityReceipt, AuthorityVerdict,
    BuyerValue, CardinalityValue, Comparator, ContractTypeValue, DimensionAuthority,
    DimensionEvidence, EvidenceWindow, GeoValue, ModalityValue, MoneyValue, OrderDirection,
    OrderKey, OrderValue, StateValue, TimeValue, TopicValue, decode_value,
)
from app.core.foundation_authority_registry import (
    REGISTRY, AuthorityDeps, AuthorityEntry, Maturity,
)
from app.core.foundation_evidence_window import (
    EvidenceWindowError, build_window, window_tokens,
)
from app.core.foundation_geo_level import GeoLevel
from app.core.interpretation_contract import (
    ContractError, Effect, HostResolution, Polarity, RawSpan, ResolutionDecision,
)
from app.core.semantic_dimensions import SemanticDimension
from app.utils.money import normalize_text

ROOT = Path(__file__).resolve().parents[1]
FAST = settings(max_examples=150, deadline=None,
                suppress_health_check=[HealthCheck.function_scoped_fixture])
NEW_RUNTIME_MODULES = ("foundation_authority_contract", "foundation_authority_values",
                       "foundation_evidence_window", "foundation_authority_registry",
                       "foundation_geo_level", "host_authorities")


# ---- the window: at most K tokens, only the cited text, never the raw turn -------------------
_WORD = st.text(alphabet="abcdefghijklmnopqrstuvwxyzáéíóúñÁÉÑ0123456789", min_size=1, max_size=9)
_SEP = st.sampled_from([" ", "  ", ", ", ". ", " - ", "\n", "\t", " (", ") "])


@st.composite
def turns(draw, minimum=0, maximum=14):
    """(raw, start, end): a cited word sits among arbitrarily many words and separators."""
    before = draw(st.lists(_WORD, min_size=minimum, max_size=maximum))
    after = draw(st.lists(_WORD, min_size=minimum, max_size=maximum))
    quote = draw(st.lists(_WORD, min_size=1, max_size=3))
    left = "".join(word + draw(_SEP) for word in before)
    right = "".join(draw(_SEP) + word for word in after)
    cited = " ".join(quote)
    return left + cited + right, len(left), len(left) + len(cited)


@FAST
@given(turns())
def test_window_never_exceeds_k_tokens_on_either_side(case):
    raw, start, end = case
    window = build_window(raw, [RawSpan(start, end, raw[start:end])], "TIME")
    assert WINDOW_TOKENS == 4
    assert len(window.left) <= 4 and len(window.right) <= 4


@FAST
@given(turns())
def test_window_is_exactly_the_cite_plus_the_k_nearest_tokens(case):
    raw, start, end = case
    window = build_window(raw, [RawSpan(start, end, raw[start:end])], "TIME")
    before = re.findall(r"[a-z0-9]+", normalize_text(raw[:start]))
    after = re.findall(r"[a-z0-9]+", normalize_text(raw[end:]))
    assert window.left == tuple(before[-4:]) and window.right == tuple(after[:4])
    assert window.quote == unicodedata.normalize("NFC", raw[start:end])
    assert window.spans == (RawSpan(start, end, raw[start:end]),)


@FAST
@given(turns(minimum=6), st.integers(0, 3))
def test_window_contains_no_character_outside_the_cite_and_k_tokens(case, which):
    """A canary placed beyond K tokens on either side must never reach any field of the window."""
    raw, start, end = case
    canary = "zzcanaryqq"
    far_left = canary + " " + " ".join(["relleno"] * 5) + " "
    far_right = " " + " ".join(["relleno"] * 5) + " " + canary
    raw = (far_left if which in (0, 2) else "") + raw + (far_right if which in (1, 2) else "")
    offset = len(far_left) if which in (0, 2) else 0
    window = build_window(raw, [RawSpan(start + offset, end + offset, raw[start + offset:end + offset])],
                          "TIME")
    assert canary not in repr(window)
    assert {field.name for field in dataclasses.fields(window)} == {"spans", "quote", "left", "right"}


@FAST
@given(st.text(min_size=1, max_size=60), st.data())
def test_window_tokens_are_a_subset_of_the_folded_neighbour_text(raw, data):
    start = data.draw(st.integers(0, len(raw) - 1))
    end = data.draw(st.integers(start + 1, len(raw)))
    if not raw[start:end].strip():
        return
    window = build_window(raw, [RawSpan(start, end, raw[start:end])], "TOPIC")
    assert window.left == window_tokens(raw[:start])[-4:]
    assert window.right == window_tokens(raw[end:])[:4]
    for token in (*window.left, *window.right):
        assert re.fullmatch(r"[a-z0-9]+", token)
        assert token in normalize_text(raw) or token in normalize_text(raw[:start] + raw[end:])


def test_window_tokenizer_matches_the_m1_h0_folding():
    assert window_tokens("Contratos de AGOSTO de 2026, en Cáli ²") == (
        "contratos", "de", "agosto", "de", "2026", "en", "cali", "2")


def test_window_cuts_at_the_span_boundary_even_inside_a_word():
    raw = "vendo sobras ya"
    start = raw.index("obras")
    window = build_window(raw, [RawSpan(start, start + 5, "obras")], "TOPIC")
    assert window.left == ("vendo", "s") and window.right == ("ya",)


def test_window_accepts_one_phrase_cited_in_contiguous_pieces_and_a_duplicate_citation():
    raw = "contratos de primera infancia en Cali"
    spans = [RawSpan(13, 20, "primera"), RawSpan(21, 29, "infancia")]
    window = build_window(raw, spans, "TOPIC")
    assert window.quote == "primera infancia" and window.left == ("contratos", "de")
    assert window.right == ("en", "cali")
    assert build_window(raw, [spans[0], spans[0]], "TOPIC").quote == "primera"


@pytest.mark.parametrize("raw, spans, reason", [
    ("obras de obras", [RawSpan(0, 5, "obras"), RawSpan(9, 14, "obras")], "DISJOINT_EVIDENCE"),
    ("aseo y vigilancia", [RawSpan(0, 4, "aseo"), RawSpan(7, 17, "vigilancia")],
     "MULTI_EVIDENCE_TOPIC_UNSUPPORTED"),
    ("obras", [], "NO_MATERIAL_EVIDENCE"),
    ("obras", [RawSpan(0, 5, "otras")], "UNBOUND_RAW_SPAN"),
    ("obras", [RawSpan(0, 9, "obras")], "UNBOUND_RAW_SPAN"),
])
def test_window_builder_refuses_what_it_cannot_show_in_full(raw, spans, reason):
    with pytest.raises(EvidenceWindowError) as error:
        build_window(raw, spans, "TOPIC")
    assert error.value.reason == reason and str(error.value) == reason


def test_window_errors_never_echo_the_raw_turn():
    raw = "texto-secreto-unico y otra cosa"
    with pytest.raises(EvidenceWindowError) as error:
        build_window(raw, [RawSpan(0, 5, "otros")], "TOPIC")
    assert "secreto" not in str(error.value) and "secreto" not in repr(error.value.args)


def test_window_builder_refuses_a_non_text_turn():
    with pytest.raises(EvidenceWindowError):
        build_window(None, [RawSpan(0, 1, "a")], "TOPIC")


@pytest.mark.parametrize("left, right, quote", [
    (("a",) * 5, (), "x"), ((), ("a",) * 5, "x"), (("Mayus",), (), "x"), (("con espacio",), (), "x"),
    (("ñ",), (), "x"), (("",), (), "x"), ([], (), "x"), ((), (), " "), ((), (), "é"),
])
def test_window_validates_its_own_shape(left, right, quote):
    with pytest.raises(ContractError):
        EvidenceWindow((RawSpan(0, 1, "x"),), quote, left, right)


def test_window_truncation_only_removes_context():
    raw = "uno dos tres cuatro 2026 cinco seis"
    start = raw.index("2026")
    window = build_window(raw, [RawSpan(start, start + 4, "2026")], "TIME")
    assert window.left == ("uno", "dos", "tres", "cuatro") and window.right == ("cinco", "seis")
    assert window.truncated(1, 1).left == ("cuatro",) and window.truncated(1, 1).right == ("cinco",)
    assert window.stripped().left == () and window.stripped().right == ()
    assert window.stripped().quote == window.quote and window.stripped().spans == window.spans
    with pytest.raises(ContractError):
        window.truncated(len(window.left) + 1, 0)
    with pytest.raises(ContractError):
        window.truncated(-1, 0)


def test_only_the_window_module_and_the_contract_construct_an_evidence_window():
    allowed = {"foundation_authority_contract.py", "foundation_evidence_window.py"}
    offenders = [str(path.relative_to(ROOT)) for path in (ROOT / "app").rglob("*.py")
                 if path.name not in allowed and re.search(r"\bEvidenceWindow\(", path.read_text())]
    assert offenders == []


def test_c1_1_modules_are_not_imported_by_any_runtime_module():
    """C1.1 does not change runtime: nothing under app/ imports the new modules."""
    pattern = re.compile(r"\b(?:" + "|".join(NEW_RUNTIME_MODULES) + r")\b")
    offenders = []
    for path in (ROOT / "app").rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if path.stem in NEW_RUNTIME_MODULES or "/host_authorities/" in rel:
            continue
        if pattern.search(path.read_text()):
            offenders.append(rel)
    assert offenders == []
    assert (ROOT / "app/core/host_authorities/__init__.py").read_text() == ""


# ---- ExecutableValue: closed union, tagged and versioned, with round trip (INV-C0-9) ---------
_TEXT = st.text(min_size=1, max_size=20).filter(lambda s: s.strip())
_UNIQUE = st.lists(_TEXT, min_size=1, max_size=4, unique=True)
_POLICY = st.from_regex(r"[a-z][a-z0-9_-]{0,10}@[0-9][0-9A-Za-z.]{0,4}", fullmatch=True)
_DATE = st.dates(min_value=date(1990, 1, 1), max_value=date(2100, 12, 31))


@st.composite
def geo_values(draw):
    level = draw(st.sampled_from([GeoLevel.DEPARTMENT, GeoLevel.MUNICIPALITY, GeoLevel.REGION]))
    digits = {GeoLevel.DEPARTMENT: 2, GeoLevel.MUNICIPALITY: 5}.get(level)
    code = draw(st.one_of(st.none(), st.from_regex(rf"\d{{{digits}}}", fullmatch=True))) if digits else None
    members = tuple(draw(_UNIQUE)) if level is GeoLevel.REGION else ()
    return GeoValue(level, draw(_TEXT), code, members)


@st.composite
def time_values(draw):
    a, b = draw(_DATE), draw(_DATE)
    start, end = min(a, b), max(a, b) + timedelta(days=1)
    bounds = draw(st.sampled_from(["both", "start", "end"]))
    return TimeValue(start if bounds != "end" else None, end if bounds != "start" else None,
                     draw(st.from_regex(r"[a-z][a-z0-9_]{0,12}", fullmatch=True)))


@st.composite
def state_values(draw):
    datasets = draw(st.dictionaries(_TEXT, st.lists(_TEXT, min_size=1, max_size=3, unique=True).map(tuple),
                                    max_size=3))
    return StateValue.of(draw(st.sampled_from(STATE_CLASSES)), datasets)


@st.composite
def modality_values(draw):
    ids = tuple(draw(st.lists(_TEXT, max_size=3, unique=True)))
    policy = draw(st.one_of(st.none(), _POLICY) if ids else _POLICY)  # at least one of the two
    return ModalityValue(ids, policy, draw(st.booleans()))


VALUE_STRATEGIES = {
    TopicValue: st.builds(TopicValue, _TEXT, st.booleans()),
    BuyerValue: st.builds(BuyerValue, _TEXT, _TEXT.map(lambda s: frozenset({s}))),
    ActorValue: st.builds(ActorValue, st.from_regex(r"[A-Z0-9]{1,20}", fullmatch=True),
                          st.from_regex(r"[A-Z][A-Z0-9_]{0,15}", fullmatch=True)
                          .filter(lambda s: s != "UNKNOWN"), _TEXT),
    GeoValue: geo_values(),
    TimeValue: time_values(),
    MoneyValue: st.builds(MoneyValue, st.integers(0, 10 ** 15), st.sampled_from(list(Comparator))),
    StateValue: state_values(),
    ModalityValue: modality_values(),
    ContractTypeValue: st.builds(lambda ids: ContractTypeValue(tuple(ids)), _UNIQUE),
    OrderValue: st.builds(OrderValue, st.sampled_from(list(OrderKey)), st.sampled_from(list(OrderDirection))),
    CardinalityValue: st.builds(CardinalityValue, st.integers(1, MAX_TOP_N)),
}


def test_the_union_is_closed_and_every_member_has_a_strategy():
    assert set(VALUE_STRATEGIES) == set(VALUE_TYPES) and len(VALUE_TYPES) == 11
    assert len({kind.TAG for kind in VALUE_TYPES}) == 11
    assert all(re.fullmatch(r"[a-z_]+@[0-9]+", kind.TAG) for kind in VALUE_TYPES)


@pytest.mark.parametrize("kind", VALUE_TYPES, ids=lambda kind: kind.__name__)
@FAST
@given(data=st.data())
def test_every_executable_value_round_trips_and_fits_a_host_resolution(kind, data):
    value = data.draw(VALUE_STRATEGIES[kind])
    items = value.encode()
    again = decode_value(items)
    assert again == value and type(again) is kind and again.encode() == items
    assert kind.decode(items) == value
    assert items[0] == kind.TAG and all(item.strip() for item in items) and len(set(items)) == len(items)
    HostResolution("c1", ResolutionDecision.RESOLVED, "x@1", items)  # same shape as resolved_ids today


@pytest.mark.parametrize("kind", VALUE_TYPES, ids=lambda kind: kind.__name__)
@FAST
@given(data=st.data())
def test_decode_is_strict_on_every_single_edit_of_a_valid_encoding(kind, data):
    items = data.draw(VALUE_STRATEGIES[kind]).encode()
    index = data.draw(st.integers(0, len(items) - 1))
    position = data.draw(st.integers(0, len(items[index])))
    char = data.draw(st.sampled_from(list("=%,;:!@ 0aZ") + ["", "é"]))
    edited = items[index][:position] + char + items[index][position + 1:]
    mutated = items[:index] + (edited,) + items[index + 1:]
    try:
        decoded = decode_value(mutated)
    except ContractError:
        return  # rejected: the contract error, never a bare ValueError/KeyError
    assert decoded.encode() == mutated  # accepted only if it is already the canonical spelling


@FAST
@given(st.tuples(st.text(max_size=12), st.text(max_size=12)), st.sampled_from(["", "topic@1", "money@1"]))
def test_decode_never_raises_anything_but_the_contract_error(junk, tag):
    for items in ((tag, *junk), junk, (tag,), ()):
        try:
            decode_value(items)
        except ContractError:
            pass


def test_decode_rejects_unknown_version_tag_extra_missing_reordered_and_noncanonical():
    good = TopicValue("obras", True).encode()
    assert good == ("topic@1", "phrase_folded=obras", "token_mode=1")
    for bad in (("topic@2", *good[1:]), ("topic@0", *good[1:]), ("nope@1", *good[1:]), good[:-1],
                (*good, "extra=1"), (good[0], good[2], good[1]), (good[0], good[1], "token_mode=2"),
                ("topic@1", "phrase_folded=%6Fbras", "token_mode=1"), ("topic@1", "phrase_folded=obras", "token_mode=01"),
                ("topic@1", "phrase_folded=obras", "token_mode"), (), ("",), "topic@1"):
        with pytest.raises(ContractError):
            decode_value(bad)
    with pytest.raises(ContractError):
        MoneyValue.decode(("money@1", "amount_cop=007", "comparator=>"))
    with pytest.raises(ContractError):
        MoneyValue.decode(("money@1", "amount_cop=-5", "comparator=>"))
    with pytest.raises(ContractError):
        TopicValue.decode(("money@1", "amount_cop=5", "comparator=>"))


@pytest.mark.parametrize("build", [
    lambda: TopicValue("  ", False), lambda: TopicValue("x", 1), lambda: TopicValue("\ud800", False),
    lambda: BuyerValue("x", frozenset()), lambda: BuyerValue("x", frozenset({"a", "b"})),
    lambda: BuyerValue("x", ["a"]), lambda: ActorValue("12-3", "NIT", "x"),
    lambda: ActorValue("123", "UNKNOWN", "x"), lambda: ActorValue("123", "nit", "x"),
    lambda: GeoValue(GeoLevel.UNKNOWN, "x"), lambda: GeoValue(GeoLevel.DEPARTMENT, "x", "123"),
    lambda: GeoValue(GeoLevel.MUNICIPALITY, "x", "05"), lambda: GeoValue(GeoLevel.REGION, "x"),
    lambda: GeoValue(GeoLevel.DEPARTMENT, "x", None, ("a",)), lambda: GeoValue(GeoLevel.REGION, "x", "05", ("a",)),
    lambda: TimeValue(None, None, "year"), lambda: TimeValue(date(2025, 1, 1), date(2025, 1, 1), "year"),
    lambda: TimeValue(datetime(2025, 1, 1), None, "year"), lambda: TimeValue(date(2025, 1, 1), None, "Year"),
    lambda: MoneyValue(-1, Comparator.GT), lambda: MoneyValue(True, Comparator.GT), lambda: MoneyValue(1.5, Comparator.GT),
    lambda: MoneyValue(1, ">"), lambda: StateValue("unknown_state"), lambda: StateValue("open", (("b", ("x",)), ("a", ("y",)))),
    lambda: StateValue("open", (("a", ()),)), lambda: StateValue("open", (("a", ("x",)), ("a", ("y",)))),
    lambda: ModalityValue((), None, False), lambda: ModalityValue(("a",), "no version", False),
    lambda: ModalityValue(("a",), None, 1), lambda: ContractTypeValue(()), lambda: ContractTypeValue(("a", "a")),
    lambda: OrderValue("value", OrderDirection.ASC), lambda: CardinalityValue(0),
    lambda: CardinalityValue(MAX_TOP_N + 1), lambda: CardinalityValue(True),
])
def test_values_refuse_to_exist_when_invalid(build):
    with pytest.raises(ContractError):
        build()


def test_state_value_helpers_and_the_top_n_cap_of_p8():
    value = StateValue.of("open", {"b": ("x",), "a": ("y", "z")})
    assert value.by_dataset == (("a", ("y", "z")), ("b", ("x",)))
    assert value.literals("a") == ("y", "z") and value.literals("c") is None
    assert MAX_TOP_N == 100 and len(STATE_CLASSES) == 11


# ---- verdict, receipt, context, evidence -------------------------------------------------------
RESOLVED = ResolutionDecision.RESOLVED


def verdict(**overrides):
    base = dict(decision=RESOLVED, authority="time@2", value=TimeValue(date(2025, 1, 1), None, "year"))
    return AuthorityVerdict(**{**base, **overrides})


def test_a_resolved_verdict_needs_an_accepted_value_and_only_then():
    assert verdict().to_resolution("c1").resolved_ids == verdict().value.encode()
    with pytest.raises(ContractError, match="RESOLVED_WITHOUT_VALUE"):
        verdict(value=None)
    with pytest.raises(ContractError, match="RESOLVED_WITHOUT_VALUE"):
        verdict(value=("time@1",))
    with pytest.raises(ContractError, match="RESOLVED_WITHOUT_VALUE"):
        verdict(value=object())
    with pytest.raises(ContractError, match="RESOLVED_WITH_REASON"):
        verdict(reason="UNCITED_QUALIFIER:TIME")
    for decision in (ResolutionDecision.CLARIFY, ResolutionDecision.BLOCK):
        with pytest.raises(ContractError, match="UNRESOLVED_WITH_VALUE"):
            verdict(decision=decision, reason="UNCITED_QUALIFIER:TIME")


@pytest.mark.parametrize("reason", [None, "", "MY_OWN_REASON", "UNCITED_QUALIFIER", "UNCITED_QUALIFIER:NOPE",
                                    "LEVEL_MISMATCH:TIME", "GENERIC_IDENTITY:TIME", "uncited_qualifier:TIME",
                                    "UNCITED_QUALIFIER:UNSUPPORTED", "UNCITED_QUALIFIER:RETRIEVAL_INTENT"])
def test_a_verdict_reason_outside_the_closed_set_is_refused(reason):
    with pytest.raises(ContractError, match="REASON_OUTSIDE_CLOSED_SET"):
        verdict(decision=ResolutionDecision.CLARIFY, value=None, reason=reason)


def test_the_closed_reason_set_holds_the_documented_codes_and_the_existing_value_aliases():
    for dimension in AUTHORITY_DIMENSION_NAMES:
        for code in ("UNRESOLVED_HOST_DIMENSION", "AMBIGUOUS_HOST_DIMENSION", "UNCITED_QUALIFIER",
                     "OPERATOR_EVIDENCE_MISMATCH", "NEGATION_UNSUPPORTED", "AUTHORITY_UNAVAILABLE",
                     "RESIDUE_UNREAD"):
            assert f"{code}:{dimension}" in AUTHORITY_REASONS
    assert {"LEVEL_MISMATCH:GEOGRAPHY", "GENERIC_IDENTITY:BUYER", "GENERIC_IDENTITY:ACTOR"} <= AUTHORITY_REASONS
    existing = {code for code in foundation_messages.KNOWN_CODES if code.startswith("VALUE_")}
    assert existing == set(VALUE_ALIASES) and existing <= AUTHORITY_REASONS
    assert AUTHORITY_DIMENSION_NAMES >= {dimension.value for dimension in SemanticDimension
                                         if dimension.value not in ("UNSUPPORTED", "CONTROL_LANGUAGE",
                                                                    "RETRIEVAL_INTENT")}


def test_verdict_effect_policies_and_availability_rules():
    verdict(effect=Effect.WIDENING, policies=("topic-k1@1",))
    with pytest.raises(ContractError, match="UNCOVERED_EFFECT"):
        verdict(effect=Effect.WIDENING)
    with pytest.raises(ContractError, match="UNRESOLVED_WITH_EFFECT"):
        verdict(decision=ResolutionDecision.CLARIFY, value=None, reason="UNCITED_QUALIFIER:TIME",
                effect=Effect.NARROWING)
    verdict(decision=ResolutionDecision.BLOCK, value=None, reason="AUTHORITY_UNAVAILABLE:BUYER")
    with pytest.raises(ContractError, match="UNAVAILABLE_MUST_BLOCK"):
        verdict(decision=ResolutionDecision.CLARIFY, value=None, reason="AUTHORITY_UNAVAILABLE:BUYER")
    for bad in ("time", "Time@2", "time@", "@2", "time 2@2"):
        with pytest.raises(ContractError):
            verdict(authority=bad)
    with pytest.raises(ContractError):
        verdict(policies=("nopolicy",))
    with pytest.raises(ContractError):
        verdict(receipt=None)


def test_receipt_carries_identifiers_and_digests_never_free_text():
    AuthorityReceipt(("rule.one:a@1",), 3, (("index", "0" * 64),))
    for bad in (dict(rules=("two words",)), dict(rules=("x" * 65,)), dict(rules=("é",)),
                dict(rules=("a",) * 33), dict(candidates=-1), dict(candidates=True),
                dict(digests=(("index", "XYZ"),)), dict(digests=(("Bad Label", "0" * 64),)),
                dict(digests=(("index", "0" * 64),) * 17), dict(rules=["a"])):
        with pytest.raises(ContractError):
            AuthorityReceipt(**bad)


def test_context_requires_an_injected_timezone_aware_instant():
    AuthorityContext(datetime(2026, 9, 30, tzinfo=timezone.utc), frozenset())
    for instant in (datetime(2026, 9, 30), date(2026, 9, 30), "2026-09-30", None):
        with pytest.raises(ContractError):
            AuthorityContext(instant, frozenset())
    with pytest.raises(ContractError):
        AuthorityContext(datetime(2026, 9, 30, tzinfo=timezone.utc), {"a"})
    with pytest.raises(ContractError):
        AuthorityContext(datetime(2026, 9, 30, tzinfo=timezone.utc), frozenset({""}))


def test_evidence_level_is_required_exactly_for_geography_and_dimensions_without_authority_are_refused():
    window = build_window("en Cali", [RawSpan(3, 7, "Cali")], "GEOGRAPHY")
    DimensionEvidence("c1", SemanticDimension.GEOGRAPHY, "is", Polarity.POSITIVE, GeoLevel.MUNICIPALITY, window)
    with pytest.raises(ContractError):
        DimensionEvidence("c1", SemanticDimension.GEOGRAPHY, "is", Polarity.POSITIVE, None, window)
    with pytest.raises(ContractError):
        DimensionEvidence("c1", SemanticDimension.BUYER, "is", Polarity.POSITIVE, GeoLevel.REGION, window)
    for dimension in (SemanticDimension.UNSUPPORTED, SemanticDimension.RETRIEVAL_INTENT,
                      SemanticDimension.CONTROL_LANGUAGE, "TIME"):
        with pytest.raises(ContractError):
            DimensionEvidence("c1", dimension, "is", Polarity.POSITIVE, None, window)
    with pytest.raises(ContractError):
        DimensionEvidence("c1", SemanticDimension.TIME, "during_year", "POSITIVE", None, window)
    with pytest.raises(ContractError):
        DimensionEvidence("c1", SemanticDimension.TIME, "during_year", Polarity.POSITIVE, None, "Cali")


def test_a_dimension_authority_protocol_is_structural():
    class Toy:
        authority_id, version = "toy", "1"
        dimensions, operators = frozenset({SemanticDimension.TIME}), frozenset({"during_year"})

        def resolve(self, evidence, ctx):  # pragma: no cover - structure only
            raise NotImplementedError

    assert isinstance(Toy(), DimensionAuthority) and not isinstance(object(), DimensionAuthority)


# ---- registry: empty and unplugged ---------------------------------------------------------------
def test_registry_is_empty_and_entries_validate():
    assert REGISTRY == () and [m.value for m in Maturity] == ["DECLARED", "SHADOW", "ACTIVE"]
    AuthorityEntry(SemanticDimension.TIME, lambda deps: None, Maturity.SHADOW)
    for args in (("TIME", lambda deps: None, Maturity.SHADOW), (SemanticDimension.TIME, None, Maturity.SHADOW),
                 (SemanticDimension.TIME, lambda deps: None, "ACTIVE")):
        with pytest.raises(ContractError):
            AuthorityEntry(*args)
    assert {field.name for field in dataclasses.fields(AuthorityDeps)} == {
        "entity_resolver", "actor_name_resolver", "territories", "catalogs"}
