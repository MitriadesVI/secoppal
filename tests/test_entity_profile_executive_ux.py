from __future__ import annotations

import ast
from contextlib import nullcontext
import json
from pathlib import Path

from hypothesis import given, strategies as st
import pytest

from app.core.actor_candidate_ui import actor_candidate_options
from app.core.entity_profile.presentation import (
    ENTITY_PROFILE_VIEW_KEY,
    _count_slot,
    _money_slot,
)
from app.entity_profile_ui import (
    escape_entity_profile_markdown,
    render_entity_profile_content,
    render_entity_profile_view,
)
from entity_profile_frame_b_harness import Conversation, FailingClient, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient, _workflow


def _overview(*, schema_version: int = 1) -> dict:
    return {
        "kind": "entity_profile",
        "schema_version": schema_version,
        "focus": "overview",
        "header": {"display_name": "ALCALDÍA *MUNICIPAL*", "period": "2026", "territory": None},
        "kpis": [
            {"key": key, "label": label, "status": "available", "display": value}
            for key, label, value in (
                ("process_count", "Procesos publicados", "10"),
                ("contract_count", "Contratos", "4"),
                ("total_contract_value", "Valor contratado", "$1.0 mil millones"),
                ("average_contract_value", "Promedio por contrato", "$250 millones"),
            )
        ],
        "modalities": [],
        "suppliers": {},
        "recent": {},
        "sources": [],
        "universe": None,
        "scope": [],
        "warnings": [],
        "next_steps": "Puedes pedirme sus proveedores.",
    }


class FakeStreamlit:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def __getattr__(self, name):
        def call(value=None, *args, **kwargs):
            self.calls.append((name, value))
            if name == "columns":
                return [self for _ in range(value)]
            if name == "expander":
                return nullcontext()
            return None

        return call


def test_frame_off_transports_json_safe_overview_and_preserves_text():
    workflow, _ = _workflow()
    result = workflow.run_query(
        "analiza la alcaldia de baranoa en 2026", channel="telegram",
    )
    view = result[ENTITY_PROFILE_VIEW_KEY]
    assert result["response"].startswith("ALCALDIA MUNICIPAL DE BARANOA")
    assert "$1.0 mil millones" in result["response"]
    assert [slot["display"] for slot in view["kpis"]] == [
        "10", "4", "$1.0 mil millones", "$250 millones",
    ]
    assert json.loads(json.dumps(view, ensure_ascii=False)) == view


def test_assistant_history_keeps_view_across_json_rerun():
    tree = ast.parse(Path("streamlit_app.py").read_text())
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_assistant_history_item"
    )
    namespace = {
        "actor_candidate_options": actor_candidate_options,
        "_clean_resolved": lambda value: value,
        "_continuation_ref": lambda _value: None,
        "_profile_action_ref": lambda _value: None,
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), "streamlit_app.py", "exec"), namespace)
    view = _overview()
    item = namespace["_assistant_history_item"]({
        "response": "canonical", ENTITY_PROFILE_VIEW_KEY: view,
    })
    assert json.loads(json.dumps(item, ensure_ascii=False))[ENTITY_PROFILE_VIEW_KEY] == view


def test_frame_on_uses_same_view_contract(frame_v12):
    result = Conversation("executive-ux-frame-on").say(
        "analiza la alcaldia de baranoa en 2026",
    ).result
    view = result[ENTITY_PROFILE_VIEW_KEY]
    assert view["kind"] == "entity_profile"
    assert view["schema_version"] == 1
    assert view["focus"] == "overview"
    assert [slot["display"] for slot in view["kpis"]] == [
        "10", "4", "$1.0 mil millones", "$250 millones",
    ]
    assert {source["id"] for source in view["sources"]} == {"p6dx-8zbt", "jbjy-vk9h"}
    assert all(not line.startswith("Fuentes: ") for line in view["scope"])


def test_projector_bounds_recent_title_without_losing_full_text():
    title = ("OBJETO CONTRACTUAL MUY EXTENSO " * 10).strip()

    class LongTitleClient(WorkflowClient):
        def query(self, dataset: str, soql: str) -> list[dict]:
            rows = super().query(dataset, soql)
            key = "nombre_del_procedimiento" if dataset == "p6dx-8zbt" else "objeto_del_contrato"
            return [{**row, key: title} for row in rows]

    workflow, _ = _workflow(LongTitleClient())
    recent = workflow.run_query("analiza la alcaldia de baranoa")[ENTITY_PROFILE_VIEW_KEY]["recent"]
    assert recent["process"]["full_title"] == title
    assert recent["process"]["excerpt"].endswith("…")
    assert len(recent["process"]["excerpt"]) <= 120


def test_source_records_labels_every_contract_kpi_as_records():
    workflow, _ = _workflow()
    result = workflow.run_query(
        "analiza todos los registros de la fuente de la alcaldía de Baranoa",
    )
    view = result[ENTITY_PROFILE_VIEW_KEY]
    assert view["universe"]["noun"] == "registro"
    assert view["kpis"][1]["label"] == "Registros"
    assert view["kpis"][2]["label"] == "Valor de registros"
    assert view["kpis"][3]["label"] == "Promedio por registro"


def test_total_failure_keeps_header_and_never_fabricates_zero(frame_v12):
    result = Conversation(
        "executive-ux-total-failure", client_factory=FailingClient,
    ).say("analiza la alcaldia de baranoa en 2026").result
    view = result[ENTITY_PROFILE_VIEW_KEY]
    assert view["header"]["display_name"] == "ALCALDIA MUNICIPAL DE BARANOA"
    assert all(slot["status"] == "unavailable" for slot in view["kpis"])
    assert all(slot["display"] == "No disponible" for slot in view["kpis"])
    assert result["partial"] is False


def test_non_profile_turn_cannot_carry_a_stale_view():
    workflow, _ = _workflow()
    assert workflow.run_query("analiza la alcaldia de baranoa")[ENTITY_PROFILE_VIEW_KEY]
    result = workflow.run_query("contratos de alimentación en bogotá")
    assert result.get(ENTITY_PROFILE_VIEW_KEY) is None


@pytest.mark.parametrize("view", [None, {}, _overview(schema_version=99), {**_overview(), "focus": "suppliers"}])
def test_unknown_or_focused_view_uses_canonical_text_fallback(view):
    fake = FakeStreamlit()
    assert render_entity_profile_view(fake, view) is False
    assert fake.calls == []


@pytest.mark.parametrize("field,value", [("modalities", [None]), ("suppliers", "bad"), ("recent", 3)])
def test_optional_malformed_sections_do_not_crash_supported_overview(field, value):
    view = _overview()
    view[field] = value
    assert render_entity_profile_view(FakeStreamlit(), view) is True


def test_overview_renders_four_metrics_without_markdown_currency():
    fake = FakeStreamlit()
    assert render_entity_profile_view(fake, _overview()) is True
    metric_values = [value for name, value in fake.calls if name == "metric"]
    assert metric_values == [
        "Procesos publicados", "Contratos", "Valor contratado", "Promedio por contrato",
    ]
    assert not any(name == "markdown" and "$" in str(value) for name, value in fake.calls)


def test_focused_profile_fallback_escapes_currency_but_non_profile_does_not():
    focused = {**_overview(), "focus": "suppliers"}
    profile_ui = FakeStreamlit()
    render_entity_profile_content(
        profile_ui, focused, "Total $12; promedio $3", is_profile=True,
    )
    assert profile_ui.calls == [("markdown", r"Total \$12; promedio \$3")]

    normal_ui = FakeStreamlit()
    render_entity_profile_content(normal_ui, None, "Precio $12", is_profile=False)
    assert normal_ui.calls == [("markdown", "Precio $12")]


def test_long_recent_title_keeps_bounded_excerpt_and_accessible_full_text():
    view = _overview()
    title = "Objeto contractual " * 20
    view["recent"] = {
        "process": {"excerpt": title[:119] + "…", "full_title": title, "date": None, "url": None},
        "contract": None,
        "contract_label": "Contrato reciente",
    }
    fake = FakeStreamlit()
    assert render_entity_profile_view(fake, view) is True
    assert ("text", title) in fake.calls
    assert ("text", view["recent"]["process"]["excerpt"]) in fake.calls


def test_profile_markdown_escape_preserves_currency_sign_as_text():
    text = "Total $12.5 mil millones; promedio $32.7 millones"
    assert escape_entity_profile_markdown(text) == (
        r"Total \$12.5 mil millones; promedio \$32.7 millones"
    )


@given(st.one_of(st.none(), st.booleans(), st.text(), st.floats(allow_nan=True, allow_infinity=True)))
def test_invalid_or_optional_count_never_becomes_zero(value):
    slot = _count_slot("count", "Conteo", value)
    assert slot == {
        "key": "count", "label": "Conteo", "status": "unavailable", "display": "No disponible",
    }


@given(st.one_of(st.none(), st.booleans(), st.text(), st.sampled_from([float("nan"), float("inf"), float("-inf")])))
def test_invalid_or_optional_money_never_becomes_zero(value):
    slot = _money_slot("money", "Valor", value)
    assert slot == {
        "key": "money", "label": "Valor", "status": "unavailable", "display": "No disponible",
    }


def test_accredited_zero_remains_visible_zero():
    assert _count_slot("count", "Conteo", 0)["display"] == "0"
    assert _money_slot("money", "Valor", 0)["display"] == "$0"
