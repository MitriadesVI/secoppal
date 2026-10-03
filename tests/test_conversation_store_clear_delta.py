"""M2A-2 legacy-divergence pin: ``app/core/conversation_store.py`` may differ from main ONLY by the declared delta.

Why this exists (user decision 2026-10-02, ``docs/audits/m2a_conversation/M2A_WAVE1_PREP.md`` §7; plan §0.6). The
legacy closure is frozen against main by ``tests/fixtures/legacy_closure_manifest_88653752.json``, and that manifest is
NOT regenerated to legitimize a touched module. ``conversation_store`` is therefore declared in ``ALLOWED_DIVERGENCE``
(``tests/test_m1_wave1_parity_pins.py``), and that declaration alone would accept ANY change to it. This file is what
makes the declaration narrow, with the whole pattern of pin 6 of ``foundation_guard``: main's source is kept as a fixture
(``.py.txt``) and the two ASTs are compared (parsed from BYTES, so an encoding cookie or a BOM is honoured exactly as
``import`` honours it), and the exact sha256 of the candidate file is pinned (``CANDIDATE_SHA256``, the ``LEAF_SHA256``
style): ANY byte outside the declared delta, even one that cannot change the AST, fails here. The AST pin is what makes
regenerating the sha not enough to legitimize a changed body.

The ONLY admitted delta: ``ConversationStore.clear()`` has ONE more element, FIRST in the tuple of paths it unlinks under
``conversation_lock(envelope_path)``: ``self.base_path / f"{_sanitize_chat_id(chat_id)}.accepted.jsonl"``. By literal,
because the legacy closure must never import M2a. Everything else, module level included (imports, docstrings,
constants, every other method), must be main's by AST (comments and whitespace are free for the AST pin, not for the
sha pin: a comment needs a declared reason and a fresh review like any other edit). In particular this pin rejects
the repair of ``DEBT_CLEAR_GENERATIONS_RETENTION.md`` (``.generations``) until the user authorizes it and this pin is
changed on purpose, and it rejects any import of M2a from the legacy closure (also pinned, apart, by
``tests/test_m2a_boundaries.py``).

Each rule is shown not to be vacuous by a mutant it must catch.
"""
from __future__ import annotations

import ast
import copy
import sys
import types
from pathlib import Path

import pytest

from tests import test_m1_wave1_parity_pins as parity
from tests import test_c1_wave3_prep_boundary as c1_boundary
from tests.test_m1_wave1_parity_pins import ALLOWED_DIVERGENCE, frozen_module_problems, legacy_manifest
from tests.test_m1_wave1_parity_pins import _sha_bytes
from tests.test_m2a_boundaries import legacy_reach_problems

ROOT = Path(__file__).resolve().parents[1]
MODULE = "app.core.conversation_store"
RELATIVE = "app/core/conversation_store.py"
MAIN_SOURCE = ROOT / "tests" / "fixtures" / "conversation_store_main_88653752.py.txt"
CLASS = "ConversationStore"
METHOD = "clear"
ADDED_ELEMENT = 'self.base_path / f"{_sanitize_chat_id(chat_id)}.accepted.jsonl"'
# The exact bytes of the candidate ``conversation_store.py`` (main's plus the declared delta), fixed at M2A-2 (commit
# 76881287). Any edit of the file fails here: it needs a declared reason and a fresh ZEUS review. The AST pin below
# compares with main as well, so regenerating this sha does not legitimize a changed body.
CANDIDATE_SHA256 = "db3c96924ada89fed4770e495d9b5478eff38c90f72a0533e51b33e1cce3fbc7"


def _methods(tree: ast.Module) -> list[ast.AST]:
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == CLASS]
    assert len(classes) == 1, f"expected exactly one class {CLASS}"
    return [node for node in classes[0].body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == METHOD]


def _dump_without_clear(tree: ast.Module) -> str:
    tree = copy.deepcopy(tree)
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == CLASS)
    cls.body = [ast.Pass() if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == METHOD
                else node for node in cls.body]
    return ast.dump(tree)


def _expected_clear(main_clear: ast.AST) -> str:
    """Main's ``clear()`` plus the declared element, first in the tuple of the one ``for`` loop."""
    clear = copy.deepcopy(main_clear)
    loops = [node for node in ast.walk(clear) if isinstance(node, ast.For) and isinstance(node.iter, ast.Tuple)]
    assert len(loops) == 1, "main's clear() no longer has the one tuple loop: the fixture or the pin is out of date"
    loops[0].iter.elts.insert(0, ast.parse(ADDED_ELEMENT, mode="eval").body)
    return ast.dump(clear)


def delta_problems(source: bytes | str) -> list[str]:
    """Why ``source`` is not main's ``conversation_store`` plus exactly the declared delta, by AST (empty = it is).
    Bytes are parsed AS BYTES: a ``# coding:`` cookie or a BOM changes what the file means and ``import`` honours it,
    so the pin must too (a decoded text would not see it)."""
    reference = MAIN_SOURCE.read_bytes()
    data = source.encode("utf-8") if isinstance(source, str) else source
    out: list[str] = []
    if _sha_bytes(reference) != legacy_manifest()[RELATIVE]:
        out.append("the fixture is not main's conversation_store.py (its sha is not the manifest's)")
        return out
    try:
        main, mine = ast.parse(reference), ast.parse(data)
    except (SyntaxError, ValueError) as error:
        return [f"the file does not parse as bytes ({type(error).__name__})"]
    main_clear, my_clear = _methods(main), _methods(mine)
    if len(main_clear) != 1 or len(my_clear) != 1:
        return [f"{CLASS}.{METHOD} must be defined exactly once (main: {len(main_clear)}, here: {len(my_clear)})"]
    if _dump_without_clear(main) != _dump_without_clear(mine):
        out.append(f"outside {CLASS}.{METHOD}() the module is not main's (declared delta: none)")
    if ast.dump(my_clear[0]) != _expected_clear(main_clear[0]):
        out.append(f"{CLASS}.{METHOD}() is not main's plus the first path element {ADDED_ELEMENT}")
    return out


def candidate_problems(data: bytes) -> list[str]:
    """The whole pin: the AST delta AND the exact bytes of the candidate."""
    out = delta_problems(data)
    if _sha_bytes(data) != CANDIDATE_SHA256:
        out.append("conversation_store.py is not the pinned candidate (any edit needs a declared reason and a review)")
    return out


def current_bytes() -> bytes:
    return (ROOT / RELATIVE).read_bytes()


def current_source() -> str:
    return current_bytes().decode("utf-8")


def _replace(old: str, new: str, source: str | None = None) -> str:
    source = current_source() if source is None else source
    assert source.count(old) == 1, f"mutation anchor not unique: {old!r}"
    return source.replace(old, new)


ACCEPTED = 'self.base_path / f"{_sanitize_chat_id(chat_id)}.accepted.jsonl"'
HISTORY = 'self.base_path / f"{_sanitize_chat_id(chat_id)}.jsonl"'
PAGINATION = 'self.base_path / f"{_sanitize_chat_id(chat_id)}.pagination.json"'
IMPORT_EPOCH = "        from app.core.workflow.store_epoch import advance_epoch\n"
CLEAR_START = "    def clear(self, chat_id: str) -> None:\n"
CLEAR_END = "    def save_pagination_buffer("


def _clear_block(elements: tuple[str, ...], *, before_lock: str = "", missing_ok: str = "missing_ok=True",
                 advance: str = "advance_epoch(envelope_path)", doc: str | None = None,
                 extra_import: str = "") -> str:
    """A full ``clear()`` text, main's shape with the knobs the mutants turn."""
    doc = doc or '"""Invalidate pre-reset reservations before removing the visible pointer."""'
    loop = ",\n                         ".join(elements)
    return (f"{CLEAR_START}        {doc}\n{IMPORT_EPOCH}{extra_import}\n"
            f"        envelope_path = self._envelope_path_for(chat_id)\n{before_lock}"
            f"        with conversation_lock(envelope_path):\n            {advance}\n"
            f"            for path in ({loop}):\n                path.unlink({missing_ok})\n\n")


def _with_clear(block: str) -> str:
    source = current_source()
    head, rest = source.split(CLEAR_START, 1)
    _, tail = rest.split(CLEAR_END, 1)
    return head + block + CLEAR_END + tail


ENVELOPE = "envelope_path"
DECLARED = (ACCEPTED, ENVELOPE, HISTORY, PAGINATION)


# ---- the real file -----------------------------------------------------------------------------------------------------
def test_the_fixture_is_mains_conversation_store():
    assert _sha_bytes(MAIN_SOURCE.read_bytes()) == legacy_manifest()[RELATIVE]


def test_conversation_store_is_main_plus_exactly_the_declared_clear_delta():
    assert delta_problems(current_bytes()) == []


def test_conversation_store_is_exactly_the_pinned_candidate():
    assert candidate_problems(current_bytes()) == []


def test_the_divergence_is_declared_and_really_differs_from_the_manifest():
    assert MODULE in ALLOWED_DIVERGENCE
    assert _sha_bytes((ROOT / RELATIVE).read_bytes()) != legacy_manifest()[RELATIVE]
    assert frozen_module_problems() == []


def test_the_legacy_closure_still_does_not_reach_m2a():
    assert legacy_reach_problems() == []


def test_without_the_declaration_the_parity_pin_rejects_the_divergence(monkeypatch):
    """The manifest is not regenerated: the ONLY thing that legitimizes the touched module is its declaration."""
    monkeypatch.delitem(ALLOWED_DIVERGENCE, MODULE)
    found = frozen_module_problems()
    assert any(line.startswith(f"{MODULE}: differs from main") for line in found), found


def test_without_the_runtime_literal_exemption_the_data_pin_flags_the_new_literal(monkeypatch):
    """``.accepted.jsonl`` is per-chat runtime state, not versioned data; it needs its declared exemption."""
    monkeypatch.delitem(parity.NON_DATA_LITERALS, (MODULE, ".accepted.jsonl"))
    verdicts = {verdict for module, _, literal, verdict, _ in parity.closure_data_reads()
                if module == MODULE and literal == ".accepted.jsonl"}
    assert verdicts and "exempt" not in verdicts, verdicts


# ---- controls: the pin is not a byte comparison, and it requires the delta, it does not merely allow it ------------------
def test_control_comments_and_blank_lines_are_free_for_the_ast_pin_but_not_for_the_sha_pin():
    source = _replace("        advance_epoch(envelope_path)\n",
                      "        advance_epoch(envelope_path)\n\n            # a comment\n\n")
    assert source != current_source()
    assert delta_problems(source) == []  # the AST is main's plus the delta ...
    assert candidate_problems(source.encode("utf-8"))  # ... but the bytes are not the pinned candidate


def test_control_main_itself_fails_the_pin_because_the_delta_is_required():
    found = delta_problems(MAIN_SOURCE.read_text(encoding="utf-8"))
    assert any(line.startswith(f"{CLASS}.{METHOD}() is not main's plus") for line in found), found


# ---- mutants: each must be caught -------------------------------------------------------------------------------------------
MUTANTS = {
    "clear_does_not_delete_the_store": lambda: _with_clear(_clear_block((ENVELOPE, HISTORY, PAGINATION))),
    "clear_imports_the_store_lazily": lambda: _with_clear(_clear_block(
        DECLARED, extra_import="        from app.core import foundation_accepted_store\n")),
    "module_level_import_of_the_store": lambda: _replace(
        "from app.core.store_lock import conversation_lock\n",
        "from app.core.store_lock import conversation_lock\nfrom app.core import foundation_accepted_store\n"),
    "store_deleted_last_instead_of_first": lambda: _with_clear(_clear_block((ENVELOPE, HISTORY, PAGINATION, ACCEPTED))),
    "store_deleted_between_the_others": lambda: _with_clear(_clear_block((ENVELOPE, ACCEPTED, HISTORY, PAGINATION))),
    "clear_also_repairs_the_generations_debt": lambda: _with_clear(_clear_block(
        (*DECLARED, 'self.base_path / ".generations" / _sanitize_chat_id(chat_id)'))),
    "store_deleted_outside_the_lock": lambda: _with_clear(_clear_block(
        (ENVELOPE, HISTORY, PAGINATION),
        before_lock=f"        ({ACCEPTED}).unlink(missing_ok=True)\n")),
    "unlink_is_no_longer_missing_ok": lambda: _with_clear(_clear_block(DECLARED, missing_ok="")),
    "epoch_advance_removed": lambda: _with_clear(_clear_block(DECLARED, advance="pass")),
    "docstring_of_clear_changed": lambda: _with_clear(_clear_block(DECLARED, doc='"""Reset."""')),
    "another_method_changed_publish_epoch_check": lambda: _replace(
        " or read_epoch(path) != reservation.epoch:", ":"),
    "another_method_changed_get_history_raises": lambda: _replace(
        "                except (json.JSONDecodeError, KeyError):\n                    continue\n",
        "                except (json.JSONDecodeError, KeyError):\n                    raise\n"),
    "module_constant_changed": lambda: _replace('ENVELOPE_SUFFIX = ".envelopes.jsonl"',
                                                 'ENVELOPE_SUFFIX = ".envelopes.jsonl2"'),
    "a_second_clear_shadows_the_first": lambda: _replace(
        CLEAR_END, "    def clear(self, chat_id: str) -> None:\n        return None\n\n" + CLEAR_END),
    "a_new_public_method": lambda: _replace(
        CLEAR_END, "    def clear_accepted(self, chat_id: str) -> None:\n        return None\n\n" + CLEAR_END),
}


def test_the_mutant_factory_reproduces_the_real_clear_when_it_carries_the_declared_delta():
    """Control of the factory itself: with the declared elements it is the real ``clear()`` by AST."""
    assert delta_problems(_with_clear(_clear_block(DECLARED))) == []


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_mutant_is_caught_by_the_delta_pin(name):
    mutated = MUTANTS[name]()
    assert mutated != current_source()
    assert delta_problems(mutated), f"mutant {name} survived the delta pin"
    assert candidate_problems(mutated.encode("utf-8")), f"mutant {name} survived the whole pin"


@pytest.mark.parametrize("name", ["clear_imports_the_store_lazily", "module_level_import_of_the_store"])
def test_mutant_an_import_of_m2a_from_the_legacy_closure_is_caught_by_the_boundary_pin_too(name):
    """Two independent pins: the delta pin above and the closure pin of ``tests/test_m2a_boundaries.py``."""
    found = legacy_reach_problems(overrides={MODULE: MUTANTS[name]()})
    assert any(line.startswith("legacy closure reaches app.core.foundation_accepted_store") for line in found), found


# ---- mutants that live in the BYTES: an encoding cookie changes what the file means, a BOM or CRLF does not --------------
# (name -> (the mutated bytes, whether the AST pin alone must catch it). Those the AST cannot see are caught by the sha.)
BYTE_MUTANTS = {
    "latin1_encoding_cookie": (lambda: b"# -*- coding: latin-1 -*-\n" + current_bytes(), True),
    "unknown_codec_cookie": (lambda: b"# coding: no-such-codec\n" + current_bytes(), True),
    "utf8_bom": (lambda: b"\xef\xbb\xbf" + current_bytes(), False),
    "explicit_utf8_cookie": (lambda: b"# coding: utf-8\n" + current_bytes(), False),
    "crlf_line_endings": (lambda: current_bytes().replace(b"\n", b"\r\n"), False),
    "one_trailing_comment": (lambda: current_bytes() + b"# one byte more\n", False),
}


@pytest.mark.parametrize("name", sorted(BYTE_MUTANTS))
def test_mutant_of_the_bytes_is_caught(name):
    make, by_ast = BYTE_MUTANTS[name]
    data = make()
    assert data != current_bytes()
    assert candidate_problems(data), f"mutant {name} survived the whole pin"
    assert bool(delta_problems(data)) is by_ast, f"mutant {name}: the AST pin alone should{'' if by_ast else ' not'} see it"


def test_the_encoding_cookie_mutant_really_changes_what_the_module_does():
    """Not a hypothetical: a latin-1 cookie makes ``import`` decode ``"á"`` as two characters, and a Spanish
    pagination phrase stops being one. A pin that parsed decoded text would have said the file is unchanged."""
    import app.core.conversation_store as real

    module = types.ModuleType("m2a_cookie_mutant")
    module.__file__ = "<mutant>"
    sys.modules[module.__name__] = module
    try:
        exec(compile(BYTE_MUTANTS["latin1_encoding_cookie"][0](), "<mutant>", "exec"), module.__dict__)
    finally:
        sys.modules.pop(module.__name__, None)
    assert real.is_pagination_phrase("más resultados") is True
    assert module.is_pagination_phrase("más resultados") is False
    assert delta_problems(BYTE_MUTANTS["latin1_encoding_cookie"][0]())  # and the pin sees it


# ---- meta-pin: every declared divergence has a narrowing pin that catches an undeclared change ----------------------------------
def _leaf_narrowing() -> list[str]:
    name = "app.core.foundation_guard"
    source = parity.index_source(name) + "\n# an undeclared change\n"
    return [line for line in frozen_module_problems({name: source}) if line.startswith(f"{name}:")]


# The two new exemptions have real source enforcement, not only a metadata entry.
C1_NARROWING_ENFORCEMENT = {
    "app.core.orchestrator": c1_boundary.boundary_errors,
    "app.core.turn_ledger": c1_boundary.ledger_boundary_errors,
}


def c1_narrowed_legacy_problems(overrides: dict[str, bytes] | None = None) -> list[str]:
    # The canonical legacy gate itself invokes both narrow checks; not only this meta-test.
    return frozen_module_problems({name: data.decode("utf-8") for name, data in (overrides or {}).items()})



NARROWING_PINS = {
    "app.core.orchestrator": lambda: c1_boundary.boundary_errors(c1_boundary.replace(
        (ROOT / "app/core/orchestrator.py").read_bytes(),
        "binding = capability_for(observation, deps.settings)", "binding = None")),
    "app.core.turn_ledger": lambda: c1_boundary.ledger_boundary_errors(c1_boundary.replace(
        (ROOT / "app/core/turn_ledger.py").read_bytes(), "LEDGER_VERSION = 1", "LEDGER_VERSION = 2")),
    # each entry: a function that applies the divergence's own narrowing pin to an UNDECLARED change and returns what
    # it reports (empty would mean the divergence is open: any change to the module would be accepted)
    MODULE: lambda: candidate_problems(current_bytes() + b"# an undeclared change\n"),
    "app.core.foundation_guard": _leaf_narrowing,  # LEAF_SHA256 (+ the AST of its functions, pin 6)
    "app.core.semantic_dimensions": lambda: parity.semantic_dimensions_problems(  # pins 4 and 5
        "# an undeclared change\n" + parity.index_source("app.core.semantic_dimensions")),
}


def narrowing_gaps(allowed: dict, pins: dict) -> list[str]:
    out = [f"{name}: declared in ALLOWED_DIVERGENCE with no narrowing pin (add it to NARROWING_PINS)"
           for name in sorted(set(allowed) - set(pins))]
    out += [f"{name}: a narrowing pin for a divergence that is no longer declared" for name in sorted(set(pins) - set(allowed))]
    out += [f"{name}: its narrowing pin accepts an undeclared change (the divergence is open)"
            for name, pin in sorted(pins.items()) if not pin()]
    return out


def test_every_allowed_divergence_has_a_narrowing_pin_that_catches_an_undeclared_change():
    assert narrowing_gaps(ALLOWED_DIVERGENCE, NARROWING_PINS) == []


def test_mutant_a_divergence_declared_without_its_pin_is_a_gap():
    found = narrowing_gaps({**ALLOWED_DIVERGENCE, "app.core.unapproved_divergence": "a new divergence"}, NARROWING_PINS)
    assert found == ["app.core.unapproved_divergence: declared in ALLOWED_DIVERGENCE with no narrowing pin (add it to NARROWING_PINS)"]


def test_mutant_a_pin_that_accepts_everything_is_a_gap():
    found = narrowing_gaps(ALLOWED_DIVERGENCE, {**NARROWING_PINS, MODULE: lambda: []})
    assert found == [f"{MODULE}: its narrowing pin accepts an undeclared change (the divergence is open)"]


def test_mutant_a_stale_pin_is_a_gap():
    found = narrowing_gaps({k: v for k, v in ALLOWED_DIVERGENCE.items() if k != MODULE}, NARROWING_PINS)
    assert found == [f"{MODULE}: a narrowing pin for a divergence that is no longer declared"]


def test_c1_divergences_are_really_enforced_and_both_postimages_pass():
    assert set(C1_NARROWING_ENFORCEMENT) <= set(ALLOWED_DIVERGENCE)
    assert c1_narrowed_legacy_problems() == []
    assert c1_narrowed_legacy_problems({
        "app.core.orchestrator": c1_boundary.approved_hook_source(),
        "app.core.turn_ledger": c1_boundary.approved_ledger_source(),
    }) == []


@pytest.mark.parametrize("name,old,new", [
    ("app.core.orchestrator", "binding = capability_for(observation, deps.settings)", "binding = None"),
    ("app.core.turn_ledger", "LEDGER_VERSION = 1", "LEDGER_VERSION = 2"),
])
def test_new_whitelist_entries_do_not_exempt_their_whole_modules(name, old, new):
    source = (ROOT / (name.replace(".", "/") + ".py")).read_bytes()
    assert c1_narrowed_legacy_problems({name: c1_boundary.replace(source, old, new)})


@pytest.mark.parametrize("name", tuple(C1_NARROWING_ENFORCEMENT))
def test_removing_either_new_narrow_pin_or_making_it_always_green_is_a_gap(name):
    found = narrowing_gaps(ALLOWED_DIVERGENCE, {k: v for k, v in NARROWING_PINS.items() if k != name})
    assert any(line.startswith(f"{name}: declared") for line in found)
    found = narrowing_gaps(ALLOWED_DIVERGENCE, {**NARROWING_PINS, name: lambda: []})
    assert any(line.startswith(f"{name}: its narrowing pin accepts") for line in found)



@pytest.mark.parametrize("name,source", [
    ("app.core.orchestrator", c1_boundary.approved_hook_source),
    ("app.core.turn_ledger", c1_boundary.approved_ledger_source),
])
def test_canonical_legacy_gate_accepts_each_exact_postimage(name, source):
    assert frozen_module_problems({name: source().decode("utf-8")}) == []


def test_canonical_legacy_gate_rejects_inner_hook_and_ledger_adversaries():
    hook = c1_boundary.replace(c1_boundary.approved_hook_source(),
                               "record_operation_handoff(foundation)", "pass")
    ledger = c1_boundary.replace(c1_boundary.approved_ledger_source(),
                                 "type(self.authority) is str", "isinstance(self.authority, str)")
    assert any("UNAPPROVED_HOOK_POSTIMAGE" in line for line in frozen_module_problems({
        "app.core.orchestrator": hook.decode("utf-8")}))
    assert any("UNAPPROVED_LEDGER_POSTIMAGE" in line for line in frozen_module_problems({
        "app.core.turn_ledger": ledger.decode("utf-8")}))


def test_canonical_legacy_gate_respects_the_encoding_cookie_of_real_bytes():
    source = (ROOT / "app/core/orchestrator.py").read_bytes()
    changed = b"# coding: latin-1\n" + source
    assert any("OUTSIDE_HOOK_CHANGED" in line for line in frozen_module_problems({
        "app.core.orchestrator": changed.decode("utf-8")}))
