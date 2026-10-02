"""M2A-2 legacy-divergence pin: ``app/core/conversation_store.py`` may differ from main ONLY by the declared delta.

Why this exists (user decision 2026-10-02, ``docs/audits/m2a_conversation/M2A_WAVE1_PREP.md`` §7; plan §0.6). The
legacy closure is frozen against main by ``tests/fixtures/legacy_closure_manifest_88653752.json``, and that manifest is
NOT regenerated to legitimize a touched module. ``conversation_store`` is therefore declared in ``ALLOWED_DIVERGENCE``
(``tests/test_m1_wave1_parity_pins.py``), and that declaration alone would accept ANY change to it. This file is what
makes the declaration narrow: main's source is kept as a fixture (``.py.txt``, the pin 6(c) pattern of
``foundation_guard``) and the two ASTs are compared.

The ONLY admitted delta: ``ConversationStore.clear()`` has ONE more element, FIRST in the tuple of paths it unlinks under
``conversation_lock(envelope_path)``: ``self.base_path / f"{_sanitize_chat_id(chat_id)}.accepted.jsonl"``. By literal,
because the legacy closure must never import M2a. Everything else, module level included (imports, docstrings,
constants, every other method), must be main's by AST; comments and whitespace are free. In particular this pin rejects
the repair of ``DEBT_CLEAR_GENERATIONS_RETENTION.md`` (``.generations``) until the user authorizes it and this pin is
changed on purpose, and it rejects any import of M2a from the legacy closure (also pinned, apart, by
``tests/test_m2a_boundaries.py``).

Each rule is shown not to be vacuous by a mutant it must catch.
"""
from __future__ import annotations

import ast
import copy
from pathlib import Path

import pytest

from tests import test_m1_wave1_parity_pins as parity
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


def delta_problems(source: str) -> list[str]:
    """Why ``source`` is not main's ``conversation_store`` plus exactly the declared delta (empty = it is)."""
    reference = MAIN_SOURCE.read_bytes()
    out: list[str] = []
    if _sha_bytes(reference) != legacy_manifest()[RELATIVE]:
        out.append("the fixture is not main's conversation_store.py (its sha is not the manifest's)")
        return out
    main, mine = ast.parse(reference.decode("utf-8")), ast.parse(source)
    main_clear, my_clear = _methods(main), _methods(mine)
    if len(main_clear) != 1 or len(my_clear) != 1:
        return [f"{CLASS}.{METHOD} must be defined exactly once (main: {len(main_clear)}, here: {len(my_clear)})"]
    if _dump_without_clear(main) != _dump_without_clear(mine):
        out.append(f"outside {CLASS}.{METHOD}() the module is not main's (declared delta: none)")
    if ast.dump(my_clear[0]) != _expected_clear(main_clear[0]):
        out.append(f"{CLASS}.{METHOD}() is not main's plus the first path element {ADDED_ELEMENT}")
    return out


def current_source() -> str:
    return (ROOT / RELATIVE).read_text(encoding="utf-8")


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
    assert delta_problems(current_source()) == []


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
def test_control_comments_and_blank_lines_are_free():
    source = _replace("        advance_epoch(envelope_path)\n",
                      "        advance_epoch(envelope_path)\n\n            # a comment\n\n")
    assert source != current_source()
    assert delta_problems(source) == []


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


@pytest.mark.parametrize("name", ["clear_imports_the_store_lazily", "module_level_import_of_the_store"])
def test_mutant_an_import_of_m2a_from_the_legacy_closure_is_caught_by_the_boundary_pin_too(name):
    """Two independent pins: the delta pin above and the closure pin of ``tests/test_m2a_boundaries.py``."""
    found = legacy_reach_problems(overrides={MODULE: MUTANTS[name]()})
    assert any(line.startswith("legacy closure reaches app.core.foundation_accepted_store") for line in found), found
