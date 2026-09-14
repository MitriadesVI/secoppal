import ast
import json
from pathlib import Path
from unittest.mock import MagicMock

from app.core.natural_alpha.models import GroundedJob, Observation
from app.core.natural_alpha.presentation import project, render
from tests.test_natural_alpha_contract import IDENTITY, config


def result():
    return {
        **Observation("alpha", "MATCHED", (GroundedJob("ANALYZE", "VALUES", 0, 5),)).metadata(),
        "resolved_params": {"entidad": "HOST BUYER", "objeto": ["mantenimiento"],
                            "valor_min": 200_000_000},
        "route_reason": "heuristic_only",
        "response": "respuesta legacy",
        "soql_query": "SELECT private",
        "query_plan": {"internal": "private"},
    }


def test_ui_separates_proposal_and_host_resolution_without_internal_fields():
    view = project(result())
    assert view["labels"] == ["Analizar valores"]
    assert ["Entidad", "HOST BUYER"] in view["resolved"]
    assert ["Ruta", "Búsqueda"] in view["resolved"]
    assert "private" not in json.dumps(view)
    ui = MagicMock()
    render(ui, view, settings=config(), identity=IDENTITY)
    ui.expander.assert_called_once_with("Alfa entendió", expanded=False)
    ui.caption.assert_called_once_with("SECOPPAL resolvió")
    assert "HOST BUYER" in str(ui.text.call_args_list)


def test_ui_hidden_when_off_or_other_user_and_invalid_not_trusted():
    ui = MagicMock()
    settings = config()
    settings.secoppal_natural_alpha_mode = "off"
    render(ui, project(result()), settings=settings, identity=IDENTITY)
    render(ui, project(result()), settings=config(), identity={"is_anonymous": True})
    ui.expander.assert_not_called()
    for status in ("INVALID", "PROVIDER_ERROR", "SKIPPED"):
        assert project({**result(), "natural_alpha_status": status}) is None
    assert project({}) is None


def test_multi_has_no_claim_of_host_execution():
    view = project({**result(), "natural_alpha_status": "MULTI_JOB_BLOCKED"})
    assert view["resolved"] == []
    assert not view["legacy_ran"]


def test_assistant_history_projection_survives_json_rerun():
    tree = ast.parse(Path("streamlit_app.py").read_text())
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == "_assistant_history_item")
    namespace = {"_clean_resolved": lambda value: value, "_continuation_ref": lambda _: None,
                 "_profile_action_ref": lambda _: None, "actor_candidate_options": lambda _: []}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "streamlit_app.py", "exec"), namespace)
    item = namespace["_assistant_history_item"](result())
    assert json.loads(json.dumps(item))["natural_alpha"] == project(result())
    assert "natural_alpha" not in namespace["_assistant_history_item"]({})
