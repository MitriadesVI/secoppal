"""Exact C1.4 postimage pins; this prep changes no runtime.

Hashes come from main 5a829d5dbfe836a231e6aa3db1bedcdc1c69a11f. No git, product
imports, provider, persistence or automatic fixture regeneration is used.
"""
from __future__ import annotations

import ast
import copy
import sys
import types
from contextlib import contextmanager
import hashlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BASE_SHA = "5a829d5dbfe836a231e6aa3db1bedcdc1c69a11f"
BASE_HOOK_AST = "0c8fa739026c676625c81498b304bae9a554e698db0b72be002c402ebf956983"
OUTSIDE_HOOK_AST = "5ebeb7a8b5399e7d73be2466a27dd27c5917ebc9e9edb16ca8a97b6b79d10738"
LEGACY_MANIFEST_SHA = "33e81b8eb80c4655c7ccccfd23af489ab5ad7951233d7dd3ffbb8ca2aaf32a09"
# METIS GO_SPEC, contract-derived excerpts; never derive expected from a candidate.
APPROVED_C1_4_HOOK_AST = frozenset({"9b4b69fe3c3984aa252f83fbfe8a7f9843623dfa1bf1e4ce50166aa51ac8a2e4"})
BASE_LEDGER_AST = "b70abcca6ecc1c07f9f90212a2b80aab1c645e9bd84a0175f022cfba269e448f"
APPROVED_LEDGER_AST = "80667853385c8970e3bf6143ab66e8085c46384b7e22bc4a9f2479fdb625449c"
APPROVED_LEDGER_METHOD_AST = "8472007529bd89e57e782d9e2414e1ac88f2b8dadc60448bb4984945926f3d8e"
OUTSIDE_LEDGER_METHOD_AST = "5f0a57c8571fb42e8837e3fa7f2cbf5f9e254c6a91f610cab3e8ad884d947eb1"
FIXTURES = ROOT / "tests/fixtures"


def ast_digest(node: ast.AST) -> str:
    return hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()


def boundary_errors(source: bytes | str) -> set[str]:
    """Compare all AST outside one delimited hook and the exact hook postimage."""
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return {"INVALID_SOURCE"}
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
    return (ROOT / "app/core/orchestrator.py").read_bytes()


def test_orchestrator_has_only_the_exact_approved_hook_delta(source):
    assert not boundary_errors(source)


def test_legacy_manifest_is_not_regenerated_to_legitimize_a_delta():
    manifest = ROOT / "tests/fixtures/legacy_closure_manifest_88653752.json"
    assert hashlib.sha256(manifest.read_bytes()).hexdigest() == LEGACY_MANIFEST_SHA


def test_a_whole_dispatch_rewrite_cannot_hide_behind_the_slot(source):
    changed = replace(source, 'binding = capability_for(observation, deps.settings)',
                             'binding = capability_for(None, deps.settings)')
    assert changed != source
    assert "OUTSIDE_HOOK_CHANGED" in boundary_errors(changed)


def test_a_ctx_mutation_inside_the_hook_is_not_an_approved_postimage(source):
    changed = replace(source, 'foundation = run_foundation_search(self, ctx, seed_resolved_params,',
                             'ctx.user_query = "forged"\n        foundation = run_foundation_search(self, ctx, seed_resolved_params,')
    assert changed != source
    assert boundary_errors(changed) == {"UNAPPROVED_HOOK_POSTIMAGE"}


def test_a_second_foundation_import_outside_the_hook_is_rejected(source):
    changed = b'from app.core.foundation_operation_host import decide_operation\n' + source
    assert "OUTSIDE_HOOK_CHANGED" in boundary_errors(changed)


def test_removing_or_duplicating_the_hook_anchor_fails_closed(source):
    tree = ast.parse(source)
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module == 'app.core.foundation_runtime')
    anchor = ast.get_source_segment(source.decode('utf-8'), node)
    assert anchor
    assert boundary_errors(replace(source, anchor, 'pass')) == {"HOOK_BOUNDARY"}
    assert boundary_errors(replace(source, anchor, anchor + '\n        ' + anchor)) == {"HOOK_BOUNDARY"}


def test_legacy_fallback_cannot_be_changed_by_the_hook_exemption(source):
    changed = replace(source, 'result = handle_normal_flow(observed_deps(deps, observation, binding), ctx)',
                             'result = handle_normal_flow(deps, ctx)')
    assert changed != source
    assert "OUTSIDE_HOOK_CHANGED" in boundary_errors(changed)


def test_comments_and_line_numbers_do_not_reseal_the_semantic_pin(source):
    assert not boundary_errors(source + b'\n# synthetic comment\n')


def replace(source: bytes, old: str, new: str, count: int = -1) -> bytes:
    assert old.encode() in source
    changed = source.replace(old.encode(), new.encode(), count)
    assert changed != source
    return changed


def ledger_boundary_errors(source: bytes | str) -> set[str]:
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return {"INVALID_SOURCE"}
    classes = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Stage"]
    if len(classes) != 1:
        return {"STAGE_COUNT"}
    methods = [n for n in classes[0].body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
               and n.name == "to_dict"]
    if len(methods) != 1:
        return {"LEDGER_METHOD_COUNT"}
    errors = set()
    if ast_digest(tree) not in {BASE_LEDGER_AST, APPROVED_LEDGER_AST}:
        errors.add("UNAPPROVED_LEDGER_POSTIMAGE")
    methods[0].body = [ast.Pass()]
    if ast_digest(tree) != OUTSIDE_LEDGER_METHOD_AST:
        errors.add("OUTSIDE_LEDGER_METHOD_CHANGED")
    return errors


def approved_hook_source() -> bytes:
    tree = ast.parse((ROOT / "app/core/orchestrator.py").read_bytes())
    method = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_dispatch")
    start = next(i for i, n in enumerate(method.body) if isinstance(n, ast.ImportFrom)
                 and n.module == "app.core.foundation_runtime")
    end = next(i for i, n in enumerate(method.body) if isinstance(n, ast.With))
    fragment = ast.parse((FIXTURES / "c1_wave3_hook_expected.py.txt").read_bytes())
    assert ast_digest(fragment) in APPROVED_C1_4_HOOK_AST
    method.body[start:end] = fragment.body
    return ast.unparse(tree).encode()


def approved_ledger_source() -> bytes:
    tree = ast.parse((ROOT / "app/core/turn_ledger.py").read_bytes())
    stage = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Stage")
    at = next(i for i, n in enumerate(stage.body) if isinstance(n, ast.FunctionDef) and n.name == "to_dict")
    fragment = ast.parse((FIXTURES / "c1_wave3_ledger_method_expected.py.txt").read_bytes())
    assert len(fragment.body) == 1 and ast_digest(fragment.body[0]) == APPROVED_LEDGER_METHOD_AST
    stage.body[at] = fragment.body[0]
    assert ast_digest(tree) == APPROVED_LEDGER_AST
    return ast.unparse(tree).encode()


def test_both_postimages_and_baselines_are_accepted():
    assert not boundary_errors(approved_hook_source())
    assert not ledger_boundary_errors((ROOT / "app/core/turn_ledger.py").read_bytes())
    assert not ledger_boundary_errors(approved_ledger_source())


def test_python_encoding_cookie_cannot_change_the_effective_ast(source):
    assert any(byte > 127 for byte in source)
    assert "OUTSIDE_HOOK_CHANGED" in boundary_errors(b"# coding: latin-1\n" + source)


@pytest.mark.parametrize("old,new", [
    ('record_operation_handoff(foundation)', 'ctx.user_query = "forged"; record_operation_handoff(foundation)'),
    ('record_operation_handoff(foundation)', 'pass'),
    ('record_operation_handoff_result(foundation, result)', 'pass'),
    ('record_operation_handoff_result(foundation, result)', 'record_operation_handoff_result(foundation, {})'),
    ('handle_normal_flow(observed_deps(deps, observation, binding), ctx)', 'handle_normal_flow(deps, ctx)'),
    ('handle_normal_flow(observed_deps(deps, observation, binding), ctx)', 'handle_normal_flow(deps, foundation)'),
    ('record_operation_handoff_result(foundation, result)', 'handle_normal_flow(deps, ctx); record_operation_handoff_result(foundation, result)'),
])
def test_hook_postimage_rejects_an_undeclared_inner_delta(old, new):
    changed = replace(approved_hook_source(), old, new, 1)
    assert "UNAPPROVED_HOOK_POSTIMAGE" in boundary_errors(changed)


def test_hook_postimage_rejects_changed_order_and_another_import():
    source = approved_hook_source()
    changed = replace(source, 'record_operation_handoff(foundation)', 'record_operation_handoff_result(foundation, result)')
    assert "UNAPPROVED_HOOK_POSTIMAGE" in boundary_errors(changed)
    assert "OUTSIDE_HOOK_CHANGED" in boundary_errors(b'import app.core.foundation_operation_host\n' + source)
    changed = replace(source, 'binding = capability_for(observation, deps.settings)', 'binding = None')
    assert "OUTSIDE_HOOK_CHANGED" in boundary_errors(changed)


@pytest.mark.parametrize("old,new,outside", [
    ('self.authority == \"operation-owners@1\"', 'self.authority.startswith(\"operation-owners@\")', False),
    ('type(self.authority) is str', 'isinstance(self.authority, str)', False),
    ('type(key) is str', 'isinstance(key, str)', False),
    ('type(item) is str', 'isinstance(item, str)', False),
    ('key == \"table_version\"', 'key == \"private\"', False),
    ('StageName(self.name) is StageName.CAPABILITY', 'True', False),
    ('result[\"decision\"]', 'result[\"authority\"]', False),
    ('LEDGER_VERSION = 1', 'LEDGER_VERSION = 2', True),
])
def test_ledger_postimage_rejects_inner_and_outer_changes(old, new, outside):
    source = approved_ledger_source()
    old, new = old.replace('\"', '"'), new.replace('\"', '"')
    # ast.unparse uses single quotes for constants.
    old, new = old.replace('"', "'"), new.replace('"', "'")
    if old == "result['decision']":
        old, new = "'decision': _token(self.decision)", "'decision': self.decision"
    errors = ledger_boundary_errors(replace(source, old, new))
    assert "UNAPPROVED_LEDGER_POSTIMAGE" in errors
    assert ("OUTSIDE_LEDGER_METHOD_CHANGED" in errors) is outside


def test_ledger_generic_filter_and_other_method_are_outside_the_permission():
    source = approved_ledger_source()
    changed = replace(source, "[A-Za-z0-9_.:", "[A-Za-z0-9@_.:")
    assert "OUTSIDE_LEDGER_METHOD_CHANGED" in ledger_boundary_errors(changed)
    changed = replace(source, 'self.stages.append(stage)', 'self.stages.clear()')
    assert "OUTSIDE_LEDGER_METHOD_CHANGED" in ledger_boundary_errors(changed)


@pytest.fixture
def ledger_apis(monkeypatch):
    def load(data, name):
        module = types.ModuleType(name)
        monkeypatch.setitem(sys.modules, name, module)
        exec(compile(data, name, 'exec'), module.__dict__)
        return module
    return (load((ROOT / 'app/core/turn_ledger.py').read_bytes(), 'c1_base_ledger'),
            load(approved_ledger_source(), 'c1_approved_ledger'))


def capability_row(api, **extra):
    args = dict(name=api.StageName.CAPABILITY, authority='operation-owners@1', decision='HANDOFF',
                detail={'table_version': 'operation-owners@1'})
    args.update(extra)
    return api.Stage(**args).to_dict()


def test_approved_public_literal_serializes_and_generic_filters_are_identical(ledger_apis):
    base, approved = ledger_apis
    row = capability_row(approved)
    assert row['authority'] == row['detail']['table_version'] == 'operation-owners@1'
    assert capability_row(base)['authority'] == '<redacted>'
    for name in ('_TOKEN',):
        assert getattr(base, name).pattern == getattr(approved, name).pattern
    for name in ('_token', '_clean'):
        for value in ('operation-owners@1', 'private@1', 'user@example.com', ['private@1']):
            assert getattr(base, name)(value) == getattr(approved, name)(value)


class StringAlias(str):
    pass


@pytest.mark.parametrize('value', [
    'private@1', 'operation-owners@2', 'operation-owners@1:chat-123', 'chat-123@owner',
    'user@example.com', 'operation-owners@1\n', '', {'table_version': 'operation-owners@1'},
    ['operation-owners@1'], StringAlias('operation-owners@1'),
])
def test_ledger_exception_rejects_private_or_non_exact_values(ledger_apis, value):
    base, approved = ledger_apis
    assert capability_row(approved, authority=value) == capability_row(base, authority=value)
    expected = capability_row(base, detail={'table_version': value})
    expected['authority'] = 'operation-owners@1'
    assert capability_row(approved, detail={'table_version': value}) == expected


@pytest.mark.parametrize('detail', [
    {'alias': 'operation-owners@1'}, {'table_version_alias': 'operation-owners@1'},
    {'table_version': {'value': 'operation-owners@1'}}, {'nested': {'table_version': 'operation-owners@1'}},
    {StringAlias('table_version'): 'operation-owners@1'},
])
def test_ledger_exception_is_not_nested_or_aliasable(ledger_apis, detail):
    base, approved = ledger_apis
    expected = capability_row(base, detail=detail)
    expected['authority'] = 'operation-owners@1'
    assert capability_row(approved, detail=detail) == expected


def test_ledger_exception_cannot_leak_into_other_stages_or_fields(ledger_apis):
    base, approved = ledger_apis
    for stage in base.StageName:
        if stage is not base.StageName.CAPABILITY:
            assert capability_row(approved, name=stage.value) == capability_row(base, name=stage)
    for field in ('decision', 'reason_code', 'input_digest', 'output_digest'):
        expected = capability_row(base, **{field: 'operation-owners@1'})
        expected['authority'] = expected['detail']['table_version'] = 'operation-owners@1'
        assert capability_row(approved, **{field: 'operation-owners@1'}) == expected
    assert capability_row(approved, authority='OTHER')['detail']['table_version'] == '<redacted>'


@pytest.mark.parametrize('disposition', ['none', 'dict', 'handoff'])
def test_approved_hook_skeleton_has_one_legacy_call_and_preserves_context(disposition):
    # All proposed helpers stubbed; no Foundation import, provider, persistence or source.
    hook = ast.parse((FIXTURES / 'c1_wave3_hook_expected.py.txt').read_bytes())
    tail = ast.parse('''with legacy_budget(observation is not None):
    result = handle_normal_flow(observed_deps(deps, observation, binding), ctx)
params = result.get("resolved_params") or {}
result["_continuation_owner"] = "ACTOR" if params.get("actor_scope") or params.get("actor_pending") else "TABULAR"
return result
''').body
    function = ast.parse('def seam(self, ctx, seed_resolved_params, deps, observation, binding): pass').body[0]
    function.body = hook.body[1:] + tail
    program = compile(ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[])), '<hook_stub>', 'exec')
    class OperationHandoff:
        __slots__ = ('operation', 'aspect', 'owner', 'job_id', 'table_version')
    handoff = OperationHandoff()
    result = {'resolved_params': {}}
    produced = {'none': None, 'dict': {}, 'handoff': handoff}[disposition]
    ctx, deps = {'user_query': 'synthetic'}, {'token': 'original'}
    before = copy.deepcopy((ctx, deps))
    observation, binding, events = object(), object(), []
    def produce(*args, **kwargs):
        events.append('produce')
        return produced
    def previous(value):
        assert value is handoff
        events.append('before')
    def observed(value, response):
        assert value is handoff and response is result
        events.append('after')
    @contextmanager
    def budget(value):
        assert value is True
        events.append('budget')
        yield
    def adapt(d, o, b):
        assert d is deps and o is observation and b is binding
        return d
    def normal(d, c):
        assert d is deps and c is ctx
        events.append('legacy')
        return result
    scope = dict(OperationHandoff=OperationHandoff, run_foundation_search=produce,
                 record_operation_handoff=previous, record_operation_handoff_result=observed,
                 legacy_budget=budget, observed_deps=adapt, handle_normal_flow=normal)
    exec(program, scope)
    actual = scope['seam'](None, ctx, None, deps, observation, binding)
    assert (ctx, deps) == before
    if disposition == 'handoff':
        assert events == ['produce', 'before', 'budget', 'legacy', 'after'] and actual is result
    elif disposition == 'none':
        assert events == ['produce', 'budget', 'legacy'] and actual is result
    else:
        assert events == ['produce'] and actual is produced
