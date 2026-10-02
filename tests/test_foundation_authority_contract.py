"""C1.1 — the frozen authority contract, the evidence window and the registry (no runtime change)."""

from __future__ import annotations

import ast
import dataclasses
import io
import re
import tokenize
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path, PurePosixPath

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from app.core import foundation_messages
from app.core.foundation_authority_contract import (
    AUTHORITY_DIMENSION_NAMES, AUTHORITY_REASONS, MAX_EVIDENCE_SEGMENTS, MAX_GAP_TOKENS, MAX_TOP_N,
    REFERENCE_TZ, STATE_CLASSES, VALUE_ALIASES, VALUE_TYPES, WINDOW_TOKENS, ActorValue,
    AuthorityContext, AuthorityReceipt, AuthorityVerdict, BuyerKind, BuyerValue, CardinalityBasis,
    CardinalityValue, Comparator, ContractTypeValue, DimensionAuthority, DimensionEvidence,
    EvidenceWindow, GeoValue, ModalityValue, MoneyValue, OrderDirection, OrderKey, OrderValue,
    StateValue, TimeValue, TopicValue, decode_value,
)
from app.core.foundation_authority_values import MAX_REQUESTED_TOP_N
from app.core.foundation_authority_registry import (
    REGISTRY, AuthorityDeps, AuthorityEntry, Maturity,
)
from app.core.foundation_authority_contract import _fold
from app.core.foundation_evidence_window import (
    WINDOW_ERROR_CODES, EvidenceWindowError, build_window, window_tokens,
)
from app.core.foundation_geo_level import GeoLevel
from app.core.interpretation_contract import (
    ContractError, Effect, HostResolution, Polarity, RawSpan, ResolutionDecision,
)
from app.core.semantic_dimensions import SemanticDimension
from app.utils.money import normalize_text
from tests.m2a_steps import registered_modules

ROOT = Path(__file__).resolve().parents[1]
FAST = settings(max_examples=150, deadline=None,
                suppress_health_check=[HealthCheck.function_scoped_fixture])
NEW_RUNTIME_MODULES = ("foundation_authority_contract", "foundation_authority_values",
                       "foundation_evidence_window", "foundation_authority_registry",
                       "host_authorities")
# C1.2 integrates the geographic level into the IR v2 (it was created for that: C1.1 §3.2).
# Nothing else may import it, so the authority contract still has no runtime caller.
GEO_LEVEL_IMPORTERS = frozenset({"interpretation_contract", "foundation_lift_v1", "foundation_acceptance_loader",
                                 "foundation_authority_contract", "foundation_authority_values",
                                 "foundation_accepted"})  # the one CLOSED_TYPE_CONSUMERS entry, see below
# M2A-1a (user decisions 2026-10-02): the ONLY exception to «nothing under app/ imports the C1.1 modules». It is a
# consumer of CLOSED TYPES, not of authorities: it may import ``foundation_authority_values`` (and the geographic
# level) solely to validate/decode already-accredited ``ExecutableValue``s and ``GeoLevel``. It may not import the
# registry, the authority contract, the evidence window, ``host_authorities`` or any resolver, nor call ``resolve``.
# C1.x amendment (wave-1 prep, user decision 2026-10-02): the importers of ``foundation_accepted`` under app/ are no
# longer «nobody» but «registered M2a modules» (``tests/m2a_steps``: M2a consumes itself only between modules explicitly
# registered; no other module of app/, scripts/ or streamlit_app does, pinned by tests/test_m2a_boundaries.py). The set
# of closed-type consumers does NOT grow with the registry: the other M2a modules never import
# ``foundation_authority_values`` or ``foundation_geo_level`` directly, they decode through ``foundation_accepted``.
CLOSED_TYPE_CONSUMERS = {"foundation_accepted": frozenset({"foundation_authority_values"})}


def _code_of(data: bytes) -> str:
    """A module's text without its comments: a comment that names a module is not an import."""
    tokens = tokenize.tokenize(io.BytesIO(data).readline)
    return " ".join(token.string for token in tokens if token.type != tokenize.COMMENT)


def _code(path: Path) -> str:
    return _code_of(path.read_bytes())


def _runtime_sources(virtual: dict[str, str] | None = None) -> dict[str, bytes]:
    """Every ``app/**/*.py`` as (posix path from the repo root -> bytes), plus VIRTUAL modules (path -> text) that
    exist only in memory, so a mutant can add a module without creating a file."""
    sources = {path.relative_to(ROOT).as_posix(): path.read_bytes() for path in (ROOT / "app").rglob("*.py")}
    for relative, text in (virtual or {}).items():
        assert relative not in sources, f"{relative} exists on disk: it is not a virtual module"
        sources[relative] = text.encode("utf-8")
    return sources


def _module_name(relative: str) -> str:
    parts = relative[: -len(".py")].split("/")
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


# ---- the window: segments, gaps shown whole, at most K tokens outside, never the raw turn ------
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


def one(raw, start, end):
    return build_window(raw, [RawSpan(start, end, raw[start:end])])


@FAST
@given(turns())
def test_window_never_exceeds_k_tokens_on_either_side(case):
    raw, start, end = case
    window = one(raw, start, end)
    assert WINDOW_TOKENS == 4 and MAX_GAP_TOKENS == 8 and MAX_EVIDENCE_SEGMENTS == 4
    assert len(window.left) <= 4 and len(window.right) <= 4


@FAST
@given(turns())
def test_window_is_exactly_the_cite_plus_the_k_nearest_tokens(case):
    raw, start, end = case
    window = one(raw, start, end)
    before = re.findall(r"[a-z0-9]+", normalize_text(raw[:start]))
    after = re.findall(r"[a-z0-9]+", normalize_text(raw[end:]))
    assert window.left == tuple(before[-4:]) and window.right == tuple(after[:4])
    assert window.segments == (unicodedata.normalize("NFC", raw[start:end]),) and window.gaps == ()
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
    window = one(raw, start + offset, end + offset)
    assert canary not in repr(window)
    assert {field.name for field in dataclasses.fields(window)} == {
        "spans", "segments", "gaps", "left", "right"}


@FAST
@given(st.text(min_size=1, max_size=60), st.data())
def test_window_tokens_are_a_subset_of_the_folded_neighbour_text(raw, data):
    start = data.draw(st.integers(0, len(raw) - 1))
    end = data.draw(st.integers(start + 1, len(raw)))
    if not raw[start:end].strip():
        return
    try:
        window = one(raw, start, end)
    except EvidenceWindowError as error:  # the only refusal a single cite can get
        assert error.reason == "SPAN_INSIDE_WORD"
        return
    assert window.left == window_tokens(raw[:start])[-4:]
    assert window.right == window_tokens(raw[end:])[:4]
    for token in (*window.left, *window.right):
        assert re.fullmatch(r"[a-z0-9]+", token)
        assert token in normalize_text(raw) or token in normalize_text(raw[:start] + raw[end:])


@FAST
@given(st.text(max_size=80))
def test_the_contract_fold_is_the_runtime_normalize_text(text):
    """The contract imports no runtime, so its `quote` fold is pinned to `normalize_text` here."""
    assert _fold(text) == normalize_text(text)


def test_window_tokenizer_matches_the_m1_h0_folding():
    assert window_tokens("Contratos de AGOSTO de 2026, en Cáli ²") == (
        "contratos", "de", "agosto", "de", "2026", "en", "cali", "2")


def test_window_refuses_a_cite_that_starts_or_ends_inside_a_word():
    """No authority can tell «s obras» from «sobras»: refused, never a fragment token."""
    for raw, needle in (("vendo sobras ya", "obras"), ("vendo obrasx ya", "obras"),
                        ("vendo sobrasx ya", "obras"), ("vendo ñobras ya", "obras"),
                        ("vendo 2025x ya", "2025"), ("vendo obras ya", "bras"), ("vendo obras ya", "obra")):
        start = raw.index(needle)
        with pytest.raises(EvidenceWindowError) as error:
            build_window(raw, [RawSpan(start, start + len(needle), needle)])
        assert error.value.reason == "SPAN_INSIDE_WORD", (raw, needle)
    for raw, needle in (("obras ya", "obras"), ("ya obras", "obras"), ("ya, obras.", "obras"),
                        ("ya (obras)", "obras"), ("ya $5.000 mas", "5.000"), ("ya\nobras\tya", "obras")):
        start = raw.index(needle)
        assert build_window(raw, [RawSpan(start, start + len(needle), needle)]).segments == (needle,)
    raw = "vendo sobras ya"  # one bad cite among good ones refuses the whole window
    with pytest.raises(EvidenceWindowError, match="SPAN_INSIDE_WORD"):
        build_window(raw, [RawSpan(0, 5, "vendo"), RawSpan(7, 12, "obras")])


def test_window_accepts_one_phrase_cited_in_contiguous_pieces_and_a_duplicate_citation():
    raw = "contratos de primera infancia en Cali"
    spans = [RawSpan(13, 20, "primera"), RawSpan(21, 29, "infancia")]
    window = build_window(raw, spans)
    assert window.segments == ("primera infancia",) and window.gaps == ()
    assert window.quote == "primera infancia" and window.left == ("contratos", "de")
    assert window.right == ("en", "cali")
    assert build_window(raw, [spans[0], spans[0]]).segments == ("primera",)
    overlapping = build_window(raw, [RawSpan(13, 29, "primera infancia"), RawSpan(21, 29, "infancia")])
    assert overlapping.segments == ("primera infancia",) and overlapping.gaps == ()
    contained = build_window(raw, [RawSpan(13, 32, "primera infancia en"), RawSpan(21, 29, "infancia")])
    assert contained.segments == ("primera infancia en",) and contained.right == ("cali",)


def test_s3_two_cites_of_one_state_condition_build_a_window_with_the_gap_in_view():
    """«publicados o abiertos» (ADR-100): the bare «o» between the cites is the evidence of the funnel."""
    raw = "procesos publicados o abiertos en Cali"
    window = build_window(raw, [RawSpan(9, 19, "publicados"), RawSpan(22, 30, "abiertos")])
    assert window.segments == ("publicados", "abiertos") and window.gaps == (("o",),)
    assert window.left == ("procesos",) and window.right == ("en", "cali")
    assert window.quote is None  # two different phrases: a single-phrase dimension reads nothing
    assert window.stripped().gaps == (("o",),) and window.truncated(0, 1).gaps == (("o",),)
    for other, gap in (("procesos publicados y abiertos", ("y",)), ("procesos publicados, abiertos", ()),
                       ("procesos publicados o sea abiertos", ("o", "sea"))):
        a = other.index("publicados")
        b = other.index("abiertos")
        built = build_window(other, [RawSpan(a, a + 10, "publicados"), RawSpan(b, b + 8, "abiertos")])
        assert built.gaps == (gap,) and built.quote is None


def test_g1_a_phrase_cited_twice_builds_a_window_whose_gap_holds_the_middle_qualifier():
    """G1 accepts «obras ... obras»; the qualifier between the occurrences is in the gap, in view."""
    raw = "obras en Cali y obras en Bogota"
    window = build_window(raw, [RawSpan(0, 5, "obras"), RawSpan(16, 21, "obras")])
    assert window.segments == ("obras", "obras") and window.gaps == (("en", "cali", "y"),)
    assert window.quote == "obras" and window.left == () and window.right == ("en", "bogota")
    again = build_window("Obras de obras", [RawSpan(0, 5, "Obras"), RawSpan(9, 14, "obras")])
    assert again.quote == "Obras" and again.gaps == (("de",),)  # same folded phrase: the first spelling


def test_window_gap_is_shown_whole_up_to_two_k_tokens_and_never_elided():
    nine = " ".join(f"p{n}" for n in range(9))
    eight = " ".join(f"p{n}" for n in range(8))
    ok = build_window(f"aaa {eight} bbb", [RawSpan(0, 3, "aaa"), RawSpan(4 + len(eight) + 1, 8 + len(eight), "bbb")])
    assert ok.gaps == (tuple(f"p{n}" for n in range(8)),) and len(ok.gaps[0]) == 2 * WINDOW_TOKENS
    with pytest.raises(EvidenceWindowError) as error:
        build_window(f"aaa {nine} bbb", [RawSpan(0, 3, "aaa"), RawSpan(4 + len(nine) + 1, 8 + len(nine), "bbb")])
    assert error.value.reason == "EVIDENCE_GAP_TOO_WIDE"


def test_window_segments_are_capped_and_touching_cites_are_one_segment():
    raw = "a1 b2 c3 d4 e5 f6 g7 h8 i9"
    cite = {w: RawSpan(raw.index(w), raw.index(w) + 2, w) for w in raw.split()}
    four = build_window(raw, [cite[w] for w in ("a1", "c3", "e5", "g7")])
    assert len(four.segments) == MAX_EVIDENCE_SEGMENTS == 4
    assert four.gaps == (("b2",), ("d4",), ("f6",)) and four.left == () and four.right == ("h8", "i9")
    with pytest.raises(EvidenceWindowError) as error:
        build_window(raw, [cite[w] for w in ("a1", "c3", "e5", "g7", "i9")])
    assert error.value.reason == "TOO_MANY_EVIDENCE_SEGMENTS"
    joined = build_window(raw, [cite[w] for w in ("a1", "b2", "e5", "f6", "i9")])  # touching: 3 segments
    assert joined.segments == ("a1 b2", "e5 f6", "i9") and joined.gaps == (("c3", "d4"), ("g7", "h8"))


@pytest.mark.parametrize("raw, spans, reason", [
    ("obras", [], "NO_MATERIAL_EVIDENCE"),
    ("obras", ["obras"], "NO_MATERIAL_EVIDENCE"),
    ("obras", [RawSpan(0, 5, "otras")], "UNBOUND_RAW_SPAN"),
    ("obras", [RawSpan(0, 9, "obras")], "UNBOUND_RAW_SPAN"),
    ("sobras", [RawSpan(1, 6, "obras")], "SPAN_INSIDE_WORD"),
    (None, [RawSpan(0, 1, "a")], "INVALID_RAW_TEXT"),
])
def test_window_builder_refuses_with_a_closed_code(raw, spans, reason):
    with pytest.raises(EvidenceWindowError) as error:
        build_window(raw, spans)
    assert error.value.reason == reason and str(error.value) == reason
    assert reason in WINDOW_ERROR_CODES


def test_window_error_codes_are_closed_and_none_comes_from_the_old_material_phrase():
    assert WINDOW_ERROR_CODES == {"INVALID_RAW_TEXT", "NO_MATERIAL_EVIDENCE", "UNBOUND_RAW_SPAN",
                                  "SPAN_INSIDE_WORD", "EVIDENCE_GAP_TOO_WIDE", "TOO_MANY_EVIDENCE_SEGMENTS"}
    with pytest.raises(ValueError):
        EvidenceWindowError("DISJOINT_EVIDENCE")
    source = (ROOT / "app/core/foundation_evidence_window.py").read_text()
    assert "material_phrase" not in source and "DISJOINT_EVIDENCE" not in source


def test_build_window_takes_no_dimension():
    with pytest.raises(TypeError):
        build_window("obras", [RawSpan(0, 5, "obras")], "TOPIC")


def test_window_errors_never_echo_the_raw_turn():
    raw = "texto-secreto-unico y otra cosa"
    with pytest.raises(EvidenceWindowError) as error:
        build_window(raw, [RawSpan(0, 5, "otros")])
    assert "secreto" not in str(error.value) and "secreto" not in repr(error.value.args)


def test_window_builder_refuses_a_non_text_turn():
    with pytest.raises(EvidenceWindowError):
        build_window(None, [RawSpan(0, 1, "a")])


_SPAN = (RawSpan(0, 1, "x"),)
_EIGHT = tuple(f"t{n}" for n in range(8))


@pytest.mark.parametrize("args", [
    (_SPAN, ("x",), (), ("a",) * 5, ()), (_SPAN, ("x",), (), (), ("a",) * 5),
    (_SPAN, ("x",), (), ("Mayus",), ()), (_SPAN, ("x",), (), ("con espacio",), ()),
    (_SPAN, ("x",), (), ("ñ",), ()), (_SPAN, ("x",), (), ("",), ()), (_SPAN, ("x",), (), [], ()),
    (_SPAN, (" ",), (), (), ()), (_SPAN, ("e\u0301",), (), (), ()),  # NFD, not NFC
    (_SPAN, (), (), (), ()),
    (_SPAN, ["x"], (), (), ()), (_SPAN, ("x",) * 5, ((),) * 4, (), ()),
    (_SPAN, ("x", "y"), (), (), ()), (_SPAN, ("x",), ((),), (), ()),
    (_SPAN, ("x", "y"), (("a",) * 9,), (), ()), (_SPAN, ("x", "y"), (("A",),), (), ()),
    (_SPAN, ("x", "y"), ([],), (), ()), (_SPAN, ("x", "y"), [()], (), ()),
    ((), ("x",), (), (), ()), (list(_SPAN), ("x",), (), (), ()),
])
def test_window_validates_its_own_shape(args):
    with pytest.raises(ContractError):
        EvidenceWindow(*args)


def test_window_accepts_the_declared_bounds():
    EvidenceWindow(_SPAN, ("x", "y"), (_EIGHT,), (), ())
    EvidenceWindow(_SPAN, ("x", "y"), ((),), ("a",) * 4, ("b",) * 4)
    EvidenceWindow(_SPAN, ("x",) * 4, (_EIGHT,) * 3, (), ())


def test_window_quote_is_the_one_phrase_every_segment_spells_else_none():
    def window(*segments):
        return EvidenceWindow(_SPAN, segments, ((),) * (len(segments) - 1), (), ())

    assert window("obras").quote == "obras"
    assert window("obras", "obras").quote == "obras"
    assert window("Obras", "OBRAS", "óbras").quote == "Obras"  # folded equal: the first spelling
    assert window("obras", "vias").quote is None
    assert window("obras", "obras", "vias").quote is None
    assert window("obras", "vias", "obras").quote is None


def test_window_truncation_only_removes_context_and_keeps_the_gaps():
    raw = "uno dos tres cuatro 2026 cinco seis"
    start = raw.index("2026")
    window = one(raw, start, start + 4)
    assert window.left == ("uno", "dos", "tres", "cuatro") and window.right == ("cinco", "seis")
    assert window.truncated(1, 1).left == ("cuatro",) and window.truncated(1, 1).right == ("cinco",)
    assert window.stripped().left == () and window.stripped().right == ()
    assert window.stripped().quote == window.quote and window.stripped().spans == window.spans
    with pytest.raises(ContractError):
        window.truncated(len(window.left) + 1, 0)
    with pytest.raises(ContractError):
        window.truncated(-1, 0)
    text = "ya procesos publicados o abiertos en Cali hoy"
    a, b = text.index("publicados"), text.index("abiertos")
    s3 = build_window(text, [RawSpan(a, a + 10, "publicados"), RawSpan(b, b + 8, "abiertos")])
    for cut in (s3.stripped(), s3.truncated(1, 2), s3.truncated(0, 0)):
        assert cut.segments == s3.segments and cut.gaps == s3.gaps == (("o",),) and cut.spans == s3.spans


def test_only_the_window_module_and_the_contract_construct_an_evidence_window():
    allowed = {"foundation_authority_contract.py", "foundation_evidence_window.py"}
    offenders = [str(path.relative_to(ROOT)) for path in (ROOT / "app").rglob("*.py")
                 if path.name not in allowed and re.search(r"\bEvidenceWindow\(", path.read_text())]
    assert offenders == []


def c1_1_import_offenders(sources: dict[str, bytes]) -> list[str]:
    """Runtime modules (path -> source) that name a C1.1 module they may not: only the closed-type consumers may, and
    only the closed types they declare. REGISTRATION as an M2a module grants nothing here."""
    pattern = re.compile(r"\b(?:" + "|".join(NEW_RUNTIME_MODULES) + r")\b")
    offenders = []
    for relative, data in sorted(sources.items()):
        stem = PurePosixPath(relative).stem
        if stem in NEW_RUNTIME_MODULES or "/host_authorities/" in relative:
            continue
        found = set(pattern.findall(_code_of(data)))
        allowed = CLOSED_TYPE_CONSUMERS.get(stem) if relative == f"app/core/{stem}.py" else None
        if found and (allowed is None or not found <= allowed):
            offenders.append(relative)
    return offenders


def closed_type_importer_offenders(sources: dict[str, bytes], registered: frozenset[str]) -> list[str]:
    """Modules that name a closed-type consumer (``foundation_accepted``) without being it nor a REGISTERED M2a
    module (C1.x amendment: the importers under app/ are a subset of the registry)."""
    offenders = []
    for stem in CLOSED_TYPE_CONSUMERS:
        offenders += [relative for relative, data in sorted(sources.items())
                      if _module_name(relative) not in registered and re.search(rf"\b{stem}\b", _code_of(data))]
    return offenders


def geo_level_importers(sources: dict[str, bytes]) -> set[str]:
    return {PurePosixPath(relative).stem for relative, data in sources.items()
            if PurePosixPath(relative).stem != "foundation_geo_level"
            and re.search(r"\bfoundation_geo_level\b", _code_of(data))}


def test_c1_1_modules_are_not_imported_by_any_runtime_module():
    """C1.1 does not change runtime: nothing under app/ imports the new modules, but the closed-type consumers."""
    assert c1_1_import_offenders(_runtime_sources()) == []
    assert (ROOT / "app/core/host_authorities/__init__.py").read_text() == ""


def test_closed_type_consumers_never_resolve_and_only_registered_m2a_modules_import_them():
    """The M2A-1a exception stays a closed-type consumer: no ``resolve``, no authority module; and (C1.x amendment)
    the importers of ``foundation_accepted`` under app/ are a subset of the registered M2a modules."""
    assert set(CLOSED_TYPE_CONSUMERS) == {"foundation_accepted"}
    forbidden = re.compile(r"_authority$|_authorities$|_resolver$|authority_registry|authority_contract"
                           r"|evidence_window|host_authorities|accreditation")
    for stem in CLOSED_TYPE_CONSUMERS:
        tree = ast.parse((ROOT / "app" / "core" / f"{stem}.py").read_text(encoding="utf-8"))
        imported = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
        imported |= {f"{node.module}.{alias.name}" for node in ast.walk(tree)
                     if isinstance(node, ast.ImportFrom) for alias in node.names}
        imported |= {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
        assert not [name for name in imported if any(forbidden.search(part) for part in name.split("."))]
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and getattr(
            node.func, "attr", getattr(node.func, "id", None)) == "resolve"]
        assert calls == []
    assert closed_type_importer_offenders(_runtime_sources(), frozenset(registered_modules())) == []


def test_only_the_ir_v2_and_the_authority_contract_import_the_geographic_level():
    importers = geo_level_importers(_runtime_sources())
    assert importers <= GEO_LEVEL_IMPORTERS and {"interpretation_contract", "foundation_lift_v1"} <= importers


# ---- non-vacuity of the C1.x amendment: virtual modules (no file is created) ------------------------------------------
VIRTUAL_REGISTERED = "app/core/m2a_virtual_decoder.py"
VIRTUAL_REGISTERED_NAME = "app.core.m2a_virtual_decoder"
IMPORTS_ACCEPTED = "from app.core.foundation_accepted import AcceptedTurnRecord\n"
IMPORTS_VALUES = "from app.core.foundation_authority_values import MAX_REQUESTED_TOP_N\n"
IMPORTS_GEO_LEVEL = "from app.core.foundation_geo_level import GeoLevel\n"


def test_c1x_control_the_real_importers_are_the_registered_modules_and_the_registry_is_not_vacuous():
    registered = frozenset(registered_modules())
    assert "app.core.foundation_accepted" in registered
    real = [relative for relative, data in _runtime_sources().items()
            if re.search(r"\bfoundation_accepted\b", _code_of(data)) and relative != "app/core/foundation_accepted.py"]
    assert all(_module_name(relative) in registered for relative in real)


@pytest.mark.parametrize("relative", ["app/core/m2a_virtual_external.py", "app/m2a_virtual_external.py",
                                      "app/core/workflow/m2a_virtual_external.py"])
def test_mutant_an_external_virtual_module_that_mentions_foundation_accepted_is_caught(relative):
    sources = _runtime_sources({relative: IMPORTS_ACCEPTED})
    assert closed_type_importer_offenders(sources, frozenset(registered_modules())) == [relative]


@pytest.mark.parametrize("source", [
    IMPORTS_ACCEPTED, "from app.core import foundation_accepted\n", "import app.core.foundation_accepted as _a\n",
    "def _late():\n    import importlib\n    return importlib.import_module('app.core.foundation_accepted')\n",
])
def test_mutant_every_spelling_of_the_import_is_caught_in_an_unregistered_module(source):
    sources = _runtime_sources({VIRTUAL_REGISTERED: source})
    assert closed_type_importer_offenders(sources, frozenset(registered_modules())) == [VIRTUAL_REGISTERED]


def test_c1x_control_a_registered_virtual_module_may_import_foundation_accepted():
    sources = _runtime_sources({VIRTUAL_REGISTERED: IMPORTS_ACCEPTED})
    registered = frozenset(registered_modules()) | {VIRTUAL_REGISTERED_NAME}
    assert closed_type_importer_offenders(sources, registered) == []
    assert c1_1_import_offenders(sources) == [] and not geo_level_importers(sources) - GEO_LEVEL_IMPORTERS


def test_a_comment_that_names_foundation_accepted_is_not_an_import():
    sources = _runtime_sources({VIRTUAL_REGISTERED: "# decodes through foundation_accepted\nX = 1\n"})
    assert closed_type_importer_offenders(sources, frozenset(registered_modules())) == []


def test_mutant_registering_a_module_does_not_grant_it_the_closed_types():
    """Registered: it may consume M2a (the amendment) but ``foundation_authority_values`` stays an offence."""
    sources = _runtime_sources({VIRTUAL_REGISTERED: IMPORTS_ACCEPTED + IMPORTS_VALUES})
    registered = frozenset(registered_modules()) | {VIRTUAL_REGISTERED_NAME}
    assert closed_type_importer_offenders(sources, registered) == []
    assert c1_1_import_offenders(sources) == [VIRTUAL_REGISTERED]


@pytest.mark.parametrize("relative", [VIRTUAL_REGISTERED, "app/core/m2a_virtual_external.py"])
def test_mutant_a_module_that_imports_foundation_authority_values_is_an_offender(relative):
    assert c1_1_import_offenders(_runtime_sources({relative: IMPORTS_VALUES})) == [relative]


@pytest.mark.parametrize("module", ["foundation_authority_contract", "foundation_evidence_window",
                                    "foundation_authority_registry", "host_authorities"])
def test_mutant_a_registered_module_that_imports_any_other_c1_1_module_is_an_offender(module):
    sources = _runtime_sources({VIRTUAL_REGISTERED: IMPORTS_ACCEPTED + f"from app.core import {module}\n"})
    assert c1_1_import_offenders(sources) == [VIRTUAL_REGISTERED]


def test_c1_1_control_foundation_accepted_itself_may_import_the_closed_types_and_nothing_else_may_be_added():
    real = (ROOT / "app/core/foundation_accepted.py").read_bytes()
    assert c1_1_import_offenders({"app/core/foundation_accepted.py": real}) == []
    widened = real + b"\nfrom app.core import foundation_authority_registry\n"
    assert c1_1_import_offenders({"app/core/foundation_accepted.py": widened}) == ["app/core/foundation_accepted.py"]


def test_mutant_a_registered_module_that_imports_the_geographic_level_directly_is_caught():
    """Decision 3: the other M2a modules decode through ``foundation_accepted``; the level has no new importer."""
    sources = _runtime_sources({VIRTUAL_REGISTERED: IMPORTS_ACCEPTED + IMPORTS_GEO_LEVEL})
    assert geo_level_importers(sources) - GEO_LEVEL_IMPORTERS == {"m2a_virtual_decoder"}


def test_the_closed_type_consumer_set_does_not_grow_with_the_registry():
    assert set(CLOSED_TYPE_CONSUMERS) == {"foundation_accepted"}
    assert GEO_LEVEL_IMPORTERS - {"interpretation_contract", "foundation_lift_v1", "foundation_acceptance_loader",
                                  "foundation_authority_contract", "foundation_authority_values"} == {
        "foundation_accepted"}


def test_the_virtual_source_helper_refuses_a_module_that_exists_on_disk():
    with pytest.raises(AssertionError):
        _runtime_sources({"app/core/foundation_accepted.py": ""})


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
    department = draw(_TEXT) if level is GeoLevel.MUNICIPALITY else None
    return GeoValue(level, draw(_TEXT), department, code, members, draw(st.booleans()))


@st.composite
def time_values(draw):
    a, b = draw(_DATE), draw(_DATE)
    start, end = min(a, b), max(a, b) + timedelta(days=1)
    bounds = draw(st.sampled_from(["both", "start", "end"]))
    return TimeValue(start if bounds != "end" else None, end if bounds != "start" else None,
                     draw(st.from_regex(r"[a-z][a-z0-9_]{0,12}", fullmatch=True)), draw(st.booleans()))


@st.composite
def state_values(draw):
    columns = st.dictionaries(_TEXT, st.lists(_TEXT, min_size=1, max_size=3, unique=True).map(tuple),
                              max_size=3)
    literals = draw(st.dictionaries(_TEXT, columns, max_size=3))
    return StateValue.of(draw(st.sampled_from(STATE_CLASSES)), literals, draw(st.booleans()))


@st.composite
def buyer_values(draw):
    if draw(st.booleans()):
        name = draw(_TEXT)
        return BuyerValue(name, BuyerKind.UNIT, (name,), None, draw(st.booleans()))
    members = tuple(sorted(draw(st.lists(_TEXT, min_size=2, max_size=4, unique=True))))
    name = draw(_TEXT.filter(lambda text: text not in members))
    return BuyerValue(name, BuyerKind.GROUP, members, draw(_POLICY), draw(st.booleans()))


@st.composite
def cardinality_values(draw):
    if draw(st.booleans()):
        return CardinalityValue(1, CardinalityBasis.SINGULAR, 1)
    figure = draw(st.one_of(st.integers(1, 300), st.integers(1, MAX_REQUESTED_TOP_N)))
    return CardinalityValue(min(figure, MAX_TOP_N), CardinalityBasis.FIGURE, figure)


@st.composite
def modality_values(draw):
    ids = tuple(draw(st.lists(_TEXT, max_size=3, unique=True)))
    policy = draw(st.one_of(st.none(), _POLICY) if ids else _POLICY)  # at least one of the two
    return ModalityValue(ids, policy, draw(st.booleans()))


VALUE_STRATEGIES = {
    TopicValue: st.builds(TopicValue, _TEXT, st.booleans(), st.booleans()),
    BuyerValue: buyer_values(),
    ActorValue: st.builds(ActorValue, st.from_regex(r"[A-Z0-9]{1,20}", fullmatch=True),
                          st.from_regex(r"[A-Z][A-Z0-9_]{0,15}", fullmatch=True)
                          .filter(lambda s: s != "UNKNOWN"), _TEXT, st.booleans()),
    GeoValue: geo_values(),
    TimeValue: time_values(),
    MoneyValue: st.builds(MoneyValue, st.integers(0, 10 ** 15), st.sampled_from(list(Comparator))),
    StateValue: state_values(),
    ModalityValue: modality_values(),
    ContractTypeValue: st.builds(lambda ids, excluded: ContractTypeValue(tuple(ids), excluded), _UNIQUE,
                                 st.booleans()),
    OrderValue: st.builds(OrderValue, st.sampled_from(list(OrderKey)), st.sampled_from(list(OrderDirection))),
    CardinalityValue: cardinality_values(),
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


@pytest.mark.parametrize("kind", VALUE_TYPES, ids=lambda kind: kind.__name__)
@FAST
@given(data=st.data())
def test_a_right_shaped_payload_under_another_tag_or_version_is_refused_as_such(kind, data):
    items = data.draw(VALUE_STRATEGIES[kind]).encode()
    name, version = kind.TAG.split("@")
    # Derived from the real TAG: a ``replace("@1", "@2")`` is the identity on a tag that already is ``@2``.
    # ``@0`` (when the version is 1) is invalid as a tag and refused all the same.
    for tag in ("other@1", f"{name}@{int(version) + 1}", f"{name}@{int(version) - 1}"):
        assert tag != kind.TAG
        with pytest.raises(ContractError, match="UNSUPPORTED_VALUE_TAG"):
            kind.decode((tag, *items[1:]))
        with pytest.raises(ContractError, match="UNSUPPORTED_VALUE_TAG"):
            decode_value((tag, *items[1:]))


@FAST
@given(st.tuples(st.text(max_size=12), st.text(max_size=12)), st.sampled_from(["", "topic@1", "money@1"]))
def test_decode_never_raises_anything_but_the_contract_error(junk, tag):
    for items in ((tag, *junk), junk, (tag,), ()):
        try:
            decode_value(items)
        except ContractError:
            pass


def test_decode_rejects_unknown_version_tag_extra_missing_reordered_and_noncanonical():
    good = TopicValue("obras", True, False).encode()
    assert good == ("topic@1", "phrase_folded=obras", "token_mode=1", "excluded=0")
    for bad in (("topic@2", *good[1:]), ("topic@0", *good[1:]), ("nope@1", *good[1:]), good[:-1],
                (*good, "extra=1"), (good[0], good[2], good[1], good[3]), (good[0], good[1], "token_mode=2", good[3]),
                (good[0], good[1], good[2], "excluded=2"), (good[0], good[1], good[2], "excluded=true"),
                ("topic@1", "phrase_folded=%6Fbras", "token_mode=1", "excluded=0"),
                ("topic@1", "phrase_folded=obras", "token_mode=01", "excluded=0"),
                ("topic@1", "phrase_folded=obras", "token_mode", "excluded=0"), (), ("",), "topic@1"):
        with pytest.raises(ContractError):
            decode_value(bad)
    with pytest.raises(ContractError):
        MoneyValue.decode(("money@1", "amount_cop=007", "comparator=>"))
    with pytest.raises(ContractError):
        MoneyValue.decode(("money@1", "amount_cop=-5", "comparator=>"))
    with pytest.raises(ContractError):
        TopicValue.decode(("money@1", "amount_cop=5", "comparator=>"))


_MUNICIPALITY = dict(level=GeoLevel.MUNICIPALITY, canonical="CALI", department="VALLE DEL CAUCA",
                     divipola="76001", members=(), excluded=False)


def geo(**overrides):
    return GeoValue(**{**_MUNICIPALITY, **overrides})


def buyer_group(**overrides):
    base = dict(canonical="SENA", kind=BuyerKind.GROUP, members=("SENA CAUCA", "SENA VALLE"),
                membership="sena@1", excluded=False)
    return BuyerValue(**{**base, **overrides})


@pytest.mark.parametrize("build", [
    lambda: TopicValue("  ", False, False), lambda: TopicValue("x", 1, False), lambda: TopicValue("\ud800", False, False),
    lambda: TopicValue("x", False, 0), lambda: TopicValue("x", False, None),
    lambda: BuyerValue("x", BuyerKind.UNIT, (), None, False),
    lambda: BuyerValue("x", BuyerKind.UNIT, ("a", "b"), None, False),
    lambda: BuyerValue("x", BuyerKind.UNIT, ("a",), None, False),  # a unit is exactly (canonical,)
    lambda: BuyerValue("x", BuyerKind.UNIT, ("x",), "g@1", False),
    lambda: BuyerValue("x", "unit", ("x",), None, False), lambda: BuyerValue("x", BuyerKind.UNIT, ["x"], None, False),
    lambda: BuyerValue("x", BuyerKind.UNIT, ("x",), None, 1),
    lambda: buyer_group(members=("SENA CAUCA",)),  # a one-member group is a unit
    lambda: buyer_group(members=()), lambda: buyer_group(members=("SENA VALLE", "SENA CAUCA")),
    lambda: buyer_group(members=("SENA CAUCA", "SENA CAUCA")), lambda: buyer_group(canonical="SENA CAUCA"),
    lambda: buyer_group(membership=None), lambda: buyer_group(membership="no version"),
    lambda: buyer_group(membership="Sena@1"),
    lambda: ActorValue("12-3", "NIT", "x", False),
    lambda: ActorValue("123", "UNKNOWN", "x", False), lambda: ActorValue("123", "nit", "x", False),
    lambda: ActorValue("123", "NIT", "x", None),
    lambda: GeoValue(GeoLevel.UNKNOWN, "x", None, None, (), False),
    lambda: geo(department=None),  # a municipality needs its department
    lambda: geo(department=" "), lambda: geo(department=5),
    lambda: GeoValue(GeoLevel.DEPARTMENT, "x", "dept", "76", (), False),  # a department has none
    lambda: GeoValue(GeoLevel.REGION, "x", "dept", None, ("a",), False),
    lambda: GeoValue(GeoLevel.DEPARTMENT, "x", None, "123", (), False),
    lambda: geo(divipola="05"), lambda: GeoValue(GeoLevel.REGION, "x", None, None, (), False),
    lambda: GeoValue(GeoLevel.DEPARTMENT, "x", None, None, ("a",), False),
    lambda: GeoValue(GeoLevel.REGION, "x", None, "05", ("a",), False), lambda: geo(excluded="no"),
    lambda: TimeValue(None, None, "year", False), lambda: TimeValue(date(2025, 1, 1), date(2025, 1, 1), "year", False),
    lambda: TimeValue(datetime(2025, 1, 1), None, "year", False), lambda: TimeValue(date(2025, 1, 1), None, "Year", False),
    lambda: TimeValue(date(2025, 1, 1), None, "year", 1),
    lambda: MoneyValue(-1, Comparator.GT), lambda: MoneyValue(True, Comparator.GT), lambda: MoneyValue(1.5, Comparator.GT),
    lambda: MoneyValue(1, ">"), lambda: StateValue("unknown_state", (), False),
    lambda: StateValue("open", (("b", "c", ("x",)), ("a", "c", ("y",))), False),
    lambda: StateValue("open", (("a", "c", ("x",)), ("a", "b", ("y",))), False),  # sorted by (dataset, column)
    lambda: StateValue("open", (("a", "c", ()),), False), lambda: StateValue("open", (("a", "c", ("x", "x")),), False),
    lambda: StateValue("open", (("a", "c", ("x",)), ("a", "c", ("y",))), False),
    lambda: StateValue("open", (("a", ("x",)),), False),  # the column is mandatory: a pair is not a literal
    lambda: StateValue("open", (("a", "", ("x",)),), False), lambda: StateValue("open", (("a", "c", ["x"]),), False),
    lambda: StateValue("open", [("a", "c", ("x",))], False), lambda: StateValue("open", (), 1),
    lambda: StateValue(["open"], (), False),
    lambda: ModalityValue((), None, False), lambda: ModalityValue(("a",), "no version", False),
    lambda: ModalityValue(("a",), None, 1), lambda: ContractTypeValue((), False),
    lambda: ContractTypeValue(("a", "a"), False), lambda: ContractTypeValue(("a",), None),
    lambda: OrderValue("value", OrderDirection.ASC),
])
def test_values_refuse_to_exist_when_invalid(build):
    with pytest.raises(ContractError):
        build()


_F, _S = CardinalityBasis.FIGURE, CardinalityBasis.SINGULAR
_BIG = MAX_REQUESTED_TOP_N


@pytest.mark.parametrize(("build", "code"), [
    # step 1: the executed n is 1..MAX_TOP_N
    (lambda: CardinalityValue(0, _F, 0), "INVALID_CARDINALITY_N"),
    (lambda: CardinalityValue(MAX_TOP_N + 1, _F, MAX_TOP_N + 1), "INVALID_CARDINALITY_N"),
    (lambda: CardinalityValue(True, _F, 1), "INVALID_CARDINALITY_N"),
    # step 2: the basis is a CardinalityBasis
    (lambda: CardinalityValue(5, "figure", 5), "INVALID_CARDINALITY_BASIS"),
    # step 3: the requested figure is an int in 1..MAX_REQUESTED_TOP_N
    (lambda: CardinalityValue(100, _F, _BIG + 1), "INVALID_CARDINALITY_N_REQUESTED"),
    (lambda: CardinalityValue(100, _F, True), "INVALID_CARDINALITY_N_REQUESTED"),
    (lambda: CardinalityValue(100, _F, None), "INVALID_CARDINALITY_N_REQUESTED"),
    (lambda: CardinalityValue(1, _F, 0), "INVALID_CARDINALITY_N_REQUESTED"),
    # step 4: a singular is one, executed and requested
    (lambda: CardinalityValue(2, _S, 2), "INVALID_CARDINALITY_SINGULAR"),
    (lambda: CardinalityValue(MAX_TOP_N, _S, MAX_TOP_N), "INVALID_CARDINALITY_SINGULAR"),
    (lambda: CardinalityValue(1, _S, 2), "INVALID_CARDINALITY_SINGULAR"),  # not CAP: step 4 runs before step 5
    # step 5: n == min(n_requested, MAX_TOP_N)
    (lambda: CardinalityValue(100, _F, 99), "INVALID_CARDINALITY_CAP"),
    (lambda: CardinalityValue(50, _F, 500), "INVALID_CARDINALITY_CAP"),
    (lambda: CardinalityValue(10, _F, 11), "INVALID_CARDINALITY_CAP"),
])
def test_an_invalid_cardinality_fails_with_its_own_code(build, code):
    with pytest.raises(ContractError, match=rf"^{code}$"):  # anchored: ..._N is a prefix of ..._N_REQUESTED
        build()


def test_the_seven_dimension_values_that_project_a_membership_or_interval_carry_excluded():
    """V5a: `excluded` is the LAST field and the last encoded item of every one of them (and MODALITY)."""
    for kind in (TopicValue, BuyerValue, ActorValue, GeoValue, TimeValue, StateValue, ContractTypeValue,
                 ModalityValue):
        names = [field.name for field in dataclasses.fields(kind)]
        assert names[-1] == "excluded", kind
    for kind in (MoneyValue, OrderValue, CardinalityValue):
        assert "excluded" not in {field.name for field in dataclasses.fields(kind)}, kind
    samples = [TopicValue("obras", False, True), buyer_group(excluded=True), ActorValue("123", "NIT", "X", True),
               geo(excluded=True), TimeValue(date(2025, 1, 1), None, "year", True),
               StateValue.of("open", {"d": {"c": ("a",)}}, True), ContractTypeValue(("a",), True),
               ModalityValue(("a",), None, True)]
    for value in samples:
        assert value.encode()[-1] == "excluded=1" and decode_value(value.encode()) == value
        flipped = dataclasses.replace(value, excluded=False)
        assert flipped != value and flipped.encode()[-1] == "excluded=0"


def test_buyer_value_is_a_unit_or_a_declared_versioned_group_never_an_owner_set():
    unit = BuyerValue("SENA CAUCA", BuyerKind.UNIT, ("SENA CAUCA",), None, False)
    group = buyer_group()
    assert unit.encode() == ("buyer@1", "canonical=SENA%20CAUCA", "kind=unit", "members=SENA%20CAUCA",
                             "membership=!", "excluded=0")
    assert group.encode()[2:5] == ("kind=group", "members=SENA%20CAUCA,SENA%20VALLE", "membership=sena%401")
    assert decode_value(group.encode()) == group and decode_value(unit.encode()) == unit
    assert "owners" not in {field.name for field in dataclasses.fields(BuyerValue)}
    assert [kind.value for kind in BuyerKind] == ["unit", "group"]


def test_geo_value_carries_the_department_of_a_municipality_only():
    assert geo().encode()[:4] == ("geo@1", "level=MUNICIPALITY", "canonical=CALI",
                                  "department=VALLE%20DEL%20CAUCA")
    GeoValue(GeoLevel.DEPARTMENT, "VALLE", None, "76", (), False)
    GeoValue(GeoLevel.REGION, "PACIFICO", None, None, ("CAUCA", "VALLE"), False)
    assert decode_value(geo().encode()) == geo()
    with pytest.raises(ContractError, match="INVALID_GEO_DEPARTMENT"):
        geo(department=None)
    with pytest.raises(ContractError, match="INVALID_GEO_DEPARTMENT"):
        GeoValue(GeoLevel.DEPARTMENT, "VALLE", "VALLE", "76", (), False)


def test_state_value_literals_are_dataset_column_values_triples_and_the_classes_cover_the_families():
    value = StateValue.of("open", {"b": {"y": ("2",), "x": ("1",)}, "a": {"c": ("p", "q")}})
    assert value.literals == (("a", "c", ("p", "q")), ("b", "x", ("1",)), ("b", "y", ("2",)))
    assert value.literals_for("b") == (("x", ("1",)), ("y", ("2",)))
    assert value.literals_for("a") == (("c", ("p", "q")),) and value.literals_for("z") is None
    assert value.excluded is False and "by_dataset" not in {f.name for f in dataclasses.fields(StateValue)}
    assert {"suspended", "ceded"} <= set(STATE_CLASSES) and len(STATE_CLASSES) == 13
    for name in STATE_CLASSES:
        assert StateValue.of(name, {"d": {"c": ("v",)}}).canonical_class == name
    assert decode_value(value.encode()) == value
    with pytest.raises(ContractError):
        StateValue.decode(("state@1", "canonical_class=open", "literals=a:c", "excluded=0"))
    with pytest.raises(ContractError):
        StateValue.decode(("state@1", "canonical_class=open", "literals=a:c:x:y", "excluded=0"))


def test_cardinality_value_records_its_basis_and_a_singular_is_one():
    singular = CardinalityValue(1, _S, 1)
    assert singular.encode() == ("cardinality@2", "n=1", "basis=singular", "n_requested=1")
    assert CardinalityValue(5, _F, 5).encode()[-2:] == ("basis=figure", "n_requested=5")
    capped = CardinalityValue(100, _F, 500)
    assert capped.encode() == ("cardinality@2", "n=100", "basis=figure", "n_requested=500")
    assert capped.capped is True and singular.capped is False
    assert CardinalityValue(10, _F, 10).capped is False and CardinalityValue(100, _F, 100).capped is False
    for value in (singular, capped, CardinalityValue(1, _F, 1), CardinalityValue(100, _F, _BIG)):
        assert decode_value(value.encode()) == value and CardinalityValue.decode(value.encode()) == value
    with pytest.raises(ContractError, match=r"^UNSUPPORTED_VALUE_TAG$"):
        decode_value(("cardinality@1", "n=1", "basis=singular"))
    with pytest.raises(ContractError, match=r"^UNSUPPORTED_VALUE_TAG$"):
        CardinalityValue.decode(("cardinality@1", "n=1", "basis=singular", "n_requested=1"))
    with pytest.raises(ContractError, match=r"^INVALID_CARDINALITY_N$"):
        CardinalityValue.decode(("cardinality@2", "n=500", "basis=figure", "n_requested=500"))
    with pytest.raises(ContractError, match=r"^INVALID_CARDINALITY_SINGULAR$"):
        CardinalityValue.decode(("cardinality@2", "n=2", "basis=singular", "n_requested=2"))
    with pytest.raises(ContractError, match=r"^INVALID_VALUE_FIELDS$"):
        CardinalityValue.decode(("cardinality@2", "n=1", "basis=singular"))  # n_requested is mandatory
    with pytest.raises(ContractError, match=r"^INVALID_VALUE_FIELDS$"):
        CardinalityValue.decode(("cardinality@2", "n=100", "basis=figure", "n_requested=\u0665\u0660\u0660"))
    with pytest.raises(ContractError, match=r"^NONCANONICAL_VALUE_ENCODING$"):
        CardinalityValue.decode(("cardinality@2", "n=100", "basis=figure", "n_requested=0500"))
    assert MAX_TOP_N == 100
    assert MAX_REQUESTED_TOP_N == 10 ** 18 - 1


# ---- verdict, receipt, context, evidence -------------------------------------------------------
RESOLVED = ResolutionDecision.RESOLVED


def verdict(**overrides):
    base = dict(decision=RESOLVED, authority="time@2", value=TimeValue(date(2025, 1, 1), None, "year", False))
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
                                    "LEVEL_MISMATCH:TIME", "GENERIC_IDENTITY:TIME", "GENERIC_IDENTITY:TOPIC", "uncited_qualifier:TIME",
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
    assert {"LEVEL_MISMATCH:GEOGRAPHY", "GENERIC_IDENTITY:BUYER", "GENERIC_IDENTITY:ACTOR",
            "GENERIC_IDENTITY:GEOGRAPHY"} <= AUTHORITY_REASONS  # «Colombia» is not a filter (D-GEO)
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


def test_a_verdict_detail_is_a_diagnostic_of_a_non_resolved_verdict_only():
    for decision in (ResolutionDecision.CLARIFY, ResolutionDecision.BLOCK):
        kept = verdict(decision=decision, value=None, reason="UNCITED_QUALIFIER:TIME",
                       detail="TIME_YEAR_NOT_STATED")
        assert kept.detail == "TIME_YEAR_NOT_STATED" and kept.decision is decision
    assert verdict().detail is None
    assert verdict(decision=ResolutionDecision.CLARIFY, value=None, reason="UNCITED_QUALIFIER:TIME").detail is None
    with pytest.raises(ContractError, match="RESOLVED_WITH_DETAIL"):
        verdict(detail="TIME_YEAR_NOT_STATED")
    for bad in ("", "lower", "1ABC", "A B", "A-B", "A" * 65, "É", 5, ["A"]):
        with pytest.raises(ContractError):
            verdict(decision=ResolutionDecision.CLARIFY, value=None, reason="UNCITED_QUALIFIER:TIME", detail=bad)
    verdict(decision=ResolutionDecision.CLARIFY, value=None, reason="UNCITED_QUALIFIER:TIME", detail="A" * 64)
    # the detail rides outside the decision: the host resolution is the same with or without it
    plain = verdict(decision=ResolutionDecision.CLARIFY, value=None, reason="UNCITED_QUALIFIER:TIME")
    fine = dataclasses.replace(plain, detail="TIME_YEAR_NOT_STATED")
    assert fine.to_resolution("c1") == plain.to_resolution("c1")


def test_context_requires_an_injected_timezone_aware_instant_and_at_least_one_dataset():
    AuthorityContext(datetime(2026, 9, 30, tzinfo=timezone.utc), frozenset({"a"}))
    for instant in (datetime(2026, 9, 30), date(2026, 9, 30), "2026-09-30", None):
        with pytest.raises(ContractError):
            AuthorityContext(instant, frozenset({"a"}))
    with pytest.raises(ContractError):
        AuthorityContext(datetime(2026, 9, 30, tzinfo=timezone.utc), {"a"})
    with pytest.raises(ContractError):
        AuthorityContext(datetime(2026, 9, 30, tzinfo=timezone.utc), frozenset({""}))


def test_context_datasets_are_never_empty_because_state_and_modality_literals_are_per_dataset():
    with pytest.raises(ContractError, match="EMPTY_CONTEXT_DATASETS"):
        AuthorityContext(datetime(2026, 9, 30, tzinfo=timezone.utc), frozenset())


def test_local_date_is_the_colombian_date_of_the_instant():
    """V2/INV-C1.1-TZ: «este año» on the night of 31 December is the same for every authority."""
    assert REFERENCE_TZ == timezone(timedelta(hours=-5))

    def local(instant):
        return AuthorityContext(instant, frozenset({"a"})).local_date()

    assert local(datetime(2027, 1, 1, 3, 0, tzinfo=timezone.utc)) == date(2026, 12, 31)  # 22:00 in Bogota
    assert local(datetime(2027, 1, 1, 5, 0, tzinfo=timezone.utc)) == date(2027, 1, 1)  # 00:00 in Bogota
    assert local(datetime(2027, 1, 1, 4, 59, tzinfo=timezone.utc)) == date(2026, 12, 31)
    assert local(datetime(2026, 12, 31, 23, 0, tzinfo=timezone(timedelta(hours=-5)))) == date(2026, 12, 31)
    assert local(datetime(2027, 1, 1, 9, 0, tzinfo=timezone(timedelta(hours=9)))) == date(2026, 12, 31)
    assert local(datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)) == date(2026, 9, 30)
    assert type(local(datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc))) is date


def test_evidence_level_is_required_exactly_for_geography_and_dimensions_without_authority_are_refused():
    window = build_window("en Cali", [RawSpan(3, 7, "Cali")])
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
