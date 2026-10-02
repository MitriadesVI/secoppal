"""The legacy closure manifest is frozen: its bytes are pinned (wave-1 prep, user decision 2026-10-02).

``tests/fixtures/legacy_closure_manifest_88653752.json`` is the reference of main 88653752 that
``tests/test_m1_wave1_parity_pins.py`` compares the legacy closure against. A touched legacy module (for M2A-2:
``ConversationStore.clear()``) is NOT legitimised by regenerating or hand-editing the manifest: it is declared as an
explicit ``ALLOWED_DIVERGENCE`` and protected by a pin that only allows the declared delta.

The parity pins accept a manifest edited in a single entry (every other pin recomputes against whatever the file
says), so the rule «the manifest is not regenerated» is enforced here, on the bytes of the file. Changing the manifest
is a reviewed decision: it means editing ``MANIFEST_SHA256`` in this file, with the reason in the commit.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "tests" / "fixtures" / "legacy_closure_manifest_88653752.json"
MANIFEST_SHA256 = "33e81b8eb80c4655c7ccccfd23af489ab5ad7951233d7dd3ffbb8ca2aaf32a09"
TOUCHED_ENTRY = "app/core/conversation_store.py"  # the entry M2A-2 would be tempted to edit


def manifest_problems(data: bytes, pinned: str = MANIFEST_SHA256) -> list[str]:
    actual = hashlib.sha256(data).hexdigest()
    return [] if actual == pinned else [f"the legacy closure manifest changed: sha256 {actual}, pinned {pinned}"]


def test_the_legacy_closure_manifest_is_the_frozen_one():
    assert manifest_problems(MANIFEST.read_bytes()) == []


def _entry_edited(data: bytes) -> bytes:
    """The manifest with ONLY the sha of one legacy module changed (the parity pins accept this silently)."""
    manifest = json.loads(data)
    old = manifest[TOUCHED_ENTRY]
    assert old in data.decode("utf-8")
    return data.replace(old.encode("ascii"), ("0" * 64).encode("ascii"), 1)


def _entry_removed(data: bytes) -> bytes:
    manifest = json.loads(data)
    del manifest[TOUCHED_ENTRY]
    return json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")


def _entry_added(data: bytes) -> bytes:
    manifest = json.loads(data)
    manifest["app/core/foundation_accepted_store.py"] = "0" * 64
    return json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")


@pytest.mark.parametrize("mutate", [
    _entry_edited, _entry_removed, _entry_added,
    lambda data: data + b"\n",  # one trailing byte
    lambda data: data[:-1],  # one byte less
    lambda data: json.dumps(json.loads(data), indent=4).encode("utf-8"),  # the same content, regenerated differently
    lambda data: b"{}",
])
def test_mutant_any_change_to_the_manifest_is_caught(mutate):
    mutated = mutate(MANIFEST.read_bytes())
    assert mutated != MANIFEST.read_bytes()
    assert manifest_problems(mutated)


def test_the_single_entry_edit_is_exactly_what_the_pin_exists_for():
    """The edited manifest is still valid JSON with the same keys: only its bytes betray it."""
    raw = MANIFEST.read_bytes()
    original, edited = json.loads(raw), json.loads(_entry_edited(raw))
    assert set(edited) == set(original)
    assert [key for key, sha in edited.items() if original[key] != sha] == [TOUCHED_ENTRY]
    assert manifest_problems(_entry_edited(raw))


def test_control_the_unchanged_bytes_pass_and_a_deliberate_new_pin_is_an_explicit_edit():
    data = MANIFEST.read_bytes()
    assert manifest_problems(data) == []
    assert manifest_problems(data + b"\n", pinned=hashlib.sha256(data + b"\n").hexdigest()) == []
