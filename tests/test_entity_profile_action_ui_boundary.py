import ast
from pathlib import Path


def _function(path: str, name: str):
    tree = ast.parse(Path(path).read_text())
    return next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def test_ui_transports_only_action_id_and_opaque_frame_reference():
    source = ast.unparse(_function("app/entity_profile_ui.py", "_render_actions"))
    assert "action_id" in source and "reference" in source
    for forbidden in (
        "resolved_params", "seed_resolved_params", "apply_refinement",
        "build_entity_profile", "soql", "phrase",
    ):
        assert forbidden not in source.lower()


def test_public_port_does_not_accept_query_params_or_semantic_phrase():
    source = ast.unparse(_function("app/core/orchestrator.py", "select_entity_profile_action"))
    assert "profile_action_selection" in source
    assert "seed_resolved_params" not in source
    assert "resolved_params" not in source
    assert "soql" not in source.lower()


def test_off_history_cannot_materialize_a_profile_action_reference():
    function = _function("streamlit_app.py", "_profile_action_ref")
    namespace = {}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "streamlit_app.py", "exec"), namespace)
    assert namespace["_profile_action_ref"]({
        "entity_profile_view": {"kind": "entity_profile"},
        "result_id": "legacy-turn",
        "chat_id": "off-chat",
    }) is None
