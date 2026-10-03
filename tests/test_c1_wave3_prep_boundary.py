"""Closed C1.4 hook slot; this prep authorizes no runtime postimage.

Hashes come from main 5a829d5dbfe836a231e6aa3db1bedcdc1c69a11f. No git, product
imports, provider, persistence or automatic fixture regeneration is used.
"""
from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BASE_SHA = "5a829d5dbfe836a231e6aa3db1bedcdc1c69a11f"
BASE_HOOK_AST = "0c8fa739026c676625c81498b304bae9a554e698db0b72be002c402ebf956983"
OUTSIDE_HOOK_AST = "5ebeb7a8b5399e7d73be2466a27dd27c5917ebc9e9edb16ca8a97b6b79d10738"
LEGACY_MANIFEST_SHA = "33e81b8eb80c4655c7ccccfd23af489ab5ad7951233d7dd3ffbb8ca2aaf32a09"
# PENDING: an exact postimage requires the C1.4 microgate and independent review.
# Empty is deliberate. Never compute an approved digest from the candidate.
APPROVED_C1_4_HOOK_AST: frozenset[str] = frozenset()
# This is a proposed slot, NOT an amendment of the legacy pin's whitelist.
ALLOWED_DIVERGENCE = {"app.core.orchestrator": "_dispatch: exact approved Foundation hook AST only"}


def ast_digest(node: ast.AST) -> str:
    return hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()


def boundary_errors(source: str) -> set[str]:
    """Compare all AST outside one delimited hook and the exact hook postimage."""
    tree = ast.parse(source)
    dispatches = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_dispatch"]
    if len(dispatches) != 1:
        return {"DISPATCH_COUNT"}
    body = dispatches[0].body
    starts = [i for i, n in enumerate(body) if isinstance(n, ast.ImportFrom)
              and n.module == "app.core.foundation_runtime"]
    ends = [i for i, n in enumerate(body) if isinstance(n, ast.With) and any(
        isinstance(item.context_expr, ast.Call) and isinstance(item.context_expr.func, ast.Name)
        and item.context_expr.func.id == "legacy_budget" for item in n.items)]
    if len(starts) != 1 or len(ends) != 1 or starts[0] >= ends[0]:
        return {"HOOK_BOUNDARY"}
    start, end = starts[0], ends[0]
    hook_hash = ast_digest(ast.Module(body=body[start:end], type_ignores=[]))
    errors = set()
    if hook_hash not in {BASE_HOOK_AST, *APPROVED_C1_4_HOOK_AST}:
        errors.add("UNAPPROVED_HOOK_POSTIMAGE")
    body[start:end] = [ast.Pass()]
    if ast_digest(tree) != OUTSIDE_HOOK_AST:
        errors.add("OUTSIDE_HOOK_CHANGED")
    return errors


@pytest.fixture
def source():
    return (ROOT / "app/core/orchestrator.py").read_text(encoding="utf-8")


def test_orchestrator_has_only_the_exact_approved_hook_delta(source):
    assert not boundary_errors(source)


def test_legacy_manifest_is_not_regenerated_to_legitimize_a_delta():
    manifest = ROOT / "tests/fixtures/legacy_closure_manifest_88653752.json"
    assert hashlib.sha256(manifest.read_bytes()).hexdigest() == LEGACY_MANIFEST_SHA


def test_a_whole_dispatch_rewrite_cannot_hide_behind_the_slot(source):
    changed = source.replace('binding = capability_for(observation, deps.settings)',
                             'binding = capability_for(None, deps.settings)')
    assert changed != source
    assert "OUTSIDE_HOOK_CHANGED" in boundary_errors(changed)


def test_a_ctx_mutation_inside_the_hook_is_not_an_approved_postimage(source):
    changed = source.replace('foundation = run_foundation_search(self, ctx, seed_resolved_params,',
                             'ctx.user_query = "forged"\n        foundation = run_foundation_search(self, ctx, seed_resolved_params,')
    assert changed != source
    assert boundary_errors(changed) == {"UNAPPROVED_HOOK_POSTIMAGE"}


def test_a_second_foundation_import_outside_the_hook_is_rejected(source):
    changed = 'from app.core.foundation_operation_host import decide_operation\n' + source
    assert "OUTSIDE_HOOK_CHANGED" in boundary_errors(changed)


def test_removing_or_duplicating_the_hook_anchor_fails_closed(source):
    anchor = 'from app.core.foundation_runtime import run_foundation_search'
    assert boundary_errors(source.replace(anchor, 'pass')) == {"HOOK_BOUNDARY"}
    assert boundary_errors(source.replace(anchor, anchor + '\n        ' + anchor)) == {"HOOK_BOUNDARY"}


def test_legacy_fallback_cannot_be_changed_by_the_hook_exemption(source):
    changed = source.replace('result = handle_normal_flow(observed_deps(deps, observation, binding), ctx)',
                             'result = handle_normal_flow(deps, ctx)')
    assert changed != source
    assert "OUTSIDE_HOOK_CHANGED" in boundary_errors(changed)


def test_comments_and_line_numbers_do_not_reseal_the_semantic_pin(source):
    assert not boundary_errors(source + '\n# synthetic comment\n')
