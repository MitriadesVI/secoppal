"""M1 wave 1 R3 (ZEUS2 P1-1 and P2-5): the relative windows of ``time_relative`` ask too.

R2 made a relative window next to another period of the same turn a question, but its rule
only knew the legacy lexicon (``temporal_semantics``: «últimos N meses», «la última
semana»…). ``time_relative`` declares more windows («el último mes/año», «los dos últimos
meses», «N últimos días», «hace dos semanas») and «contratos … en los dos últimos meses de
2025» still resolved as the whole year 2025, silently. The rule now takes its windows from
BOTH lexicons; ``time_relative`` is imported, never copied.

The property is enumerated from the two lexicons (the counts, units, articles and accents of
each pattern) and crossed with the anchor classes. A new relative form in ``time_relative``
without a decision here fails ``test_every_relative_form_has_a_decision``.

Follow-up: a window in a turn that continues a conversation which already has a period asks
(counted from today, or inside the previous period?), through the same question.

P2-5: the year of a norm citation («decreto 1082 de 2015») is not a period.
"""
from __future__ import annotations

import itertools
import re
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.config import Settings, get_settings
from app.core import normative_citation as nc
from app.core import temporal_period_question as tpq
from app.core import temporal_semantics as ts
from app.core import temporal_window_anchor as twa
from app.core import time_relative as tr
from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.models import AdmissionStatus
from app.core.capability_binding.supplier_contract import Outcome, decide
from app.core.entity_profile.frame_turn import TurnKind, run_profile_turn
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.entity_resolver import EntityResolver
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.core.temporal_semantics import resolve_temporal_scope

TODAY = date(2026, 10, 1)
CODE = twa.ERROR_CODE
BUYER = "la Alcaldía de Galapa"


def _resolve(text: str):
    return resolve_temporal_scope(text, today=TODAY)


# ── the lexicon of time_relative, enumerated ─────────────────────────────────────────────

COUNTS = [*ts._COUNT_WORDS, "1", "2", "10", "30"]
UNITS = ["días", "dias", "semanas", "meses", "años", "anos"]
ARTICLES = ["", "los ", "las ", "estos ", "estas "]
ULTIMOS = ["últimos", "ultimos", "últimas", "ultimas"]


def _number_before() -> list[str]:
    """«los dos últimos meses», «estas 2 últimas semanas»… (pattern ``_NUMERO_ANTES``)."""
    out = []
    for article, count, ultimos, unit in itertools.product(ARTICLES, COUNTS, ULTIMOS, UNITS):
        out.append(f"{article}{count} {ultimos} {unit}")
    return out


def _single_last() -> list[str]:
    """«el último mes», «este último año»… (pattern ``_ULTIMO_UNICO``)."""
    return [
        f"{article}{ultimo} {unit}"
        for article, ultimo, unit in itertools.product(
            ["", "el ", "este "], ["último", "ultimo"], ["mes", "año", "ano"],
        )
    ]


def _weeks_ago() -> list[str]:
    return [f"hace {count} {unit}" for count in COUNTS for unit in ("semana", "semanas")]


NEW_WINDOWS = _number_before() + _single_last() + _weeks_ago()
# One representative per form, article and accent: crossed with every anchor of the lexicon.
SAMPLE_NEW = [
    "los dos últimos meses", "las 2 ultimas semanas", "estos 10 últimos días", "3 ultimos meses",
    "el último mes", "este ultimo ano", "último año", "hace dos semanas", "hace 1 semana",
]
CLASS_ANCHORS = [
    "2025", "marzo de 2025", "marzo", "el año pasado", "este año", "primer trimestre de 2025",
    "enero a marzo de 2025", "antes de 2024", "2025-03-01",
]
MONTH_YEAR_JOINS = [" de ", " del ", " ", ", ", "-", "/"]
LEXICON_ANCHORS = (
    ["2019", "2024", "2025", "2026"]
    + [f"{m}{join}2025" for m in ts._MONTH_ALIASES for join in MONTH_YEAR_JOINS]
    + ["este año", "el año pasado", "este mes", "el mes pasado", "hace 2 años", "hace tres meses",
       "primer trimestre de 2025", "segundo semestre de 2024", "enero a marzo de 2025",
       "entre marzo y mayo de 2025", "2023 a 2025", "desde 2023", "hasta 2025", "antes de 2024",
       "después de 2022", "desde marzo de 2025", "2025-03-01", "15 de marzo de 2025"]
)
AFTER = ["", "de ", "del ", "en ", "durante ", "del año ", "de la vigencia ", "y "]
BEFORE_SHAPES = ["{a}, {w}", "{a} {w}", "en {a} en {w}", "{a} y {w}", "contratos de {a} de aseo en {w}"]
CONTEXT = f"contratos de {BUYER} en"


def _shapes(window: str, anchor: str) -> list[str]:
    return [f"{CONTEXT} {window} {c}{anchor}" for c in AFTER] + [
        shape.format(a=anchor, w=window) for shape in BEFORE_SHAPES
    ]


def test_every_relative_form_has_a_decision():
    """A pattern added to ``time_relative`` must be classified as a franja or a window."""
    declared = {pattern for pattern, _ in tr._FORMS}
    patterns = {value for name, value in vars(tr).items() if isinstance(value, re.Pattern)}
    assert patterns == declared, patterns ^ declared
    assert {kind for _, kind in tr._FORMS} == {"franja", "window"}
    assert tr._FRANJA not in tr.WINDOW_PATTERNS
    assert set(tr.WINDOW_PATTERNS) == {tr._ULTIMO_UNICO, tr._NUMERO_ANTES, tr._HACE_SEMANAS}


def test_the_authority_windows_are_the_union_of_both_lexicons_not_a_copy():
    assert ts.window_patterns() == ts._WINDOW_PATTERNS + tr.WINDOW_PATTERNS
    assert set(tr.WINDOW_PATTERNS).isdisjoint(ts._WINDOW_PATTERNS)
    assert all(pattern in ts.window_patterns() for pattern in tr.WINDOW_PATTERNS)


def test_the_enumerated_windows_are_real_window_phrases_of_time_relative():
    for phrase in NEW_WINDOWS:
        assert any(p.search(phrase) for p in tr.WINDOW_PATTERNS), phrase
    assert len(NEW_WINDOWS) > 1000


def _failures(windows, anchors) -> list[str]:
    bad: list[str] = []
    for window, anchor in itertools.product(windows, anchors):
        for text in _shapes(window, anchor):
            found = _resolve(text)
            if found.error_code != CODE or found.fecha_desde or found.fecha_hasta:
                bad.append(f"{text!r} -> {found.error_code} {found.fecha_desde}..{found.fecha_hasta}")
    return bad


def test_every_new_window_with_every_class_of_anchor_asks():
    bad = _failures(NEW_WINDOWS, CLASS_ANCHORS)
    assert not bad, bad[:10]


def test_every_kind_of_new_window_with_every_anchor_of_the_lexicon_asks():
    bad = _failures(SAMPLE_NEW, LEXICON_ANCHORS)
    assert not bad, bad[:10]


@pytest.mark.parametrize("text", [
    "contratos de la Alcaldía de Galapa en los dos últimos meses de 2025",
    "contratos de la Alcaldía de Galapa en el último mes de 2025",
    "contratos de la Alcaldía de Galapa en el último mes del año pasado",
    "contratos de la Alcaldía de Galapa en las dos últimas semanas de marzo de 2025",
    "contratos de la Alcaldía de Galapa en los 10 últimos días de marzo de 2025",
    "perfil de la Alcaldía de Galapa en los dos últimos meses de 2025",
    "contratos de 2025 de la Alcaldía de Galapa en el último mes",
    "contratos de la Alcaldía de Galapa hace dos semanas de marzo de 2025",
])
def test_the_cases_of_the_zeus2_finding_ask(text):
    found = _resolve(text)
    assert found.error_code == CODE and found.should_clarify
    assert (found.fecha_desde, found.fecha_hasta) == (None, None)


def test_a_window_of_time_relative_alone_never_raises_the_anchor_error():
    """No other period, no question: what the lexicon does with a lone window is unchanged."""
    for phrase in SAMPLE_NEW:
        found = _resolve(f"contratos de {BUYER} en {phrase}")
        assert found.error_code != CODE, phrase


def test_the_year_of_a_window_that_time_relative_resolves_is_still_the_clocks():
    for text in ("contratos en los últimos 3 meses", "contratos en la última semana"):
        assert _resolve(text).error_code is None


# ── the consumers ──────────────────────────────────────────────────────────────────────────

NEW_FORMS = [
    "en los dos últimos meses de 2025",
    "en el último mes de 2025",
    "en el último mes del año pasado",
    "en las dos últimas semanas de marzo de 2025",
    "en los 10 últimos días de marzo de 2025",
    "hace dos semanas de marzo de 2025",
]
NEW_IDS = [f"new{i}" for i in range(len(NEW_FORMS))]


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


@pytest.mark.parametrize("form", NEW_FORMS, ids=NEW_IDS)
@pytest.mark.parametrize("template", [
    "contratos de {b} {t}", "procesos de la Gobernación del Atlántico {t}", "proveedores de {b} {t}",
])
def test_search_parse_carries_the_error_and_no_date(router, form, template):
    params = router.parse(template.format(b=BUYER, t=form), today=TODAY).params
    assert params.get("_temporal_error") == CODE
    assert not params.get("fecha_desde") and not params.get("fecha_hasta")


@pytest.mark.parametrize("form", NEW_FORMS, ids=NEW_IDS)
def test_contract_activity_asks(router, form):
    raw = f"¿Cuántos contratos firmó {BUYER} {form}?"
    parsed = router.parse(raw, today=TODAY)
    admission = admit_contract_activity(
        raw, dict(parsed.params), today=TODAY, semantic_evidence=parsed.semantic_evidence.to_state(),
    )
    assert admission.status is AdmissionStatus.CLARIFY, raw
    assert admission.reason.startswith("SIGNATURE_PERIOD"), (raw, admission.reason)


@pytest.mark.parametrize("form", NEW_FORMS, ids=NEW_IDS)
def test_suppliers_a2_asks_and_never_executes(router, form):
    query = f"proveedores de {BUYER} {form}"
    decision = decide(query, dict(router.parse(query, today=TODAY).params), today=TODAY)
    assert decision.outcome is Outcome.CLARIFY, (query, decision.outcome)


@pytest.mark.parametrize("form", NEW_FORMS, ids=NEW_IDS)
def test_the_legacy_entity_profile_turn_asks(router, form):
    query = f"perfil de {BUYER} {form}"
    turn = classify_entity_profile_turn(query, router.parse(query, today=TODAY).params)
    assert turn.detected and turn.clarification and turn.plan is None


@pytest.mark.parametrize("form", NEW_FORMS[:4], ids=NEW_IDS[:4])
def test_the_frame_profile_turn_asks_and_makes_no_source_call(router, form):
    query = f"perfil de {BUYER} {form}"
    client = MagicMock()
    outcome = run_profile_turn(
        user_query=query, params=router.parse(query, today=TODAY).params,
        state=ProfileConversationState(), turn_id="r3", soql_builder=SoQLBuilder(),
        secop_client=client, today=TODAY,
    )
    assert outcome.kind is TurnKind.CLARIFICATION
    assert client.mock_calls == []


@pytest.fixture
def workflow(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "false")
    get_settings.cache_clear()
    flow = SecopalWorkflow(Settings(
        _env_file=None, deepseek_api_key=None, secoppal_natural_alpha_mode="off",
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.db",
        secop_feedback_sqlite_enabled=False,
    ))
    calls: list[str] = []
    row = {
        "id_contrato": "X", "nombre_entidad": "ALCALDIA MUNICIPAL DE GALAPA",
        "objeto_del_contrato": "Aseo", "valor_del_contrato": "1000000", "fecha_de_firma": "2025-11-01",
    }
    monkeypatch.setattr(flow.secop_client, "query", lambda dataset, soql, **kw: (calls.append(soql), [dict(row)])[1])
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **k: (calls.append("count"), 1)[1])
    monkeypatch.setattr(flow.secop_client, "aggregate", lambda *a, **k: (calls.append("aggregate"), [])[1])
    monkeypatch.setattr(flow.narrator, "narrate_with_grounding", lambda **kwargs: None)
    flow.calls = calls
    yield flow
    get_settings.cache_clear()


@pytest.mark.parametrize("query", [
    "contratos de la Alcaldía de Galapa en los dos últimos meses de 2025",
    "contratos de la Alcaldía de Galapa en el último mes de 2025",
    "contratos de la Alcaldía de Galapa en el último mes del año pasado",
    "contratos de la Alcaldía de Galapa en las dos últimas semanas de marzo de 2025",
    "contratos de la Alcaldía de Galapa en los 10 últimos días de marzo de 2025",
    "procesos de la Gobernación del Atlántico en los dos últimos meses de 2025",
    "perfil de la Alcaldía de Galapa en los dos últimos meses de 2025",
    "proveedores de la Alcaldía de Galapa en el último mes de 2025",
    "¿Cuántos contratos firmó la Alcaldía de Galapa en los 3 últimos meses de 2025?",
])
def test_the_workflow_asks_with_zero_source_calls(workflow, query):
    result = workflow.run_query(query, chat_id="r3-ask", today=TODAY)
    assert result["needs_clarification"] is True, query
    assert workflow.calls == [], (query, workflow.calls)
    assert "desde hoy" in result["response"] and "(1)" not in result["response"]
    params = result.get("resolved_params") or {}
    assert not params.get("fecha_desde") and not params.get("fecha_hasta")


@pytest.mark.parametrize("frame", ["true", "false"])
def test_the_profile_workflow_asks_with_or_without_the_frame(tmp_path, monkeypatch, frame):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", frame)
    get_settings.cache_clear()
    try:
        flow = SecopalWorkflow(Settings(
            _env_file=None, deepseek_api_key=None, secoppal_natural_alpha_mode="off",
            secop_feedback_path=tmp_path / "feedback.jsonl",
            secop_feedback_db_path=tmp_path / "feedback.db", secop_feedback_sqlite_enabled=False,
        ))
        calls: list[str] = []
        monkeypatch.setattr(flow.secop_client, "query", lambda *a, **k: (calls.append("q"), [])[1])
        monkeypatch.setattr(flow.secop_client, "count", lambda *a, **k: (calls.append("c"), 0)[1])
        monkeypatch.setattr(flow.secop_client, "aggregate", lambda *a, **k: (calls.append("a"), [])[1])
        result = flow.run_query(
            "perfil de la Alcaldía de Galapa en los dos últimos meses de 2025",
            chat_id=f"r3-fr-{frame}", today=TODAY,
        )
    finally:
        get_settings.cache_clear()
    assert result["needs_clarification"] is True
    assert calls == []


# ── follow-up: a window while the conversation already has a period ────────────────────────

def _first(workflow, text, chat):
    result = workflow.run_query(text, chat_id=chat, today=TODAY)
    assert result["needs_clarification"] is False, text
    return result


@pytest.mark.parametrize("first,followup,previous", [
    ("contratos de la Alcaldía de Galapa en 2025", "ahora solo el último mes", ("2025-01-01", "2025-12-31")),
    ("contratos de la Alcaldía de Galapa en 2025", "y los 3 últimos meses?", ("2025-01-01", "2025-12-31")),
    ("contratos de la Alcaldía de Galapa en 2025", "y en los últimos 3 meses", ("2025-01-01", "2025-12-31")),
    ("contratos de la Alcaldía de Galapa en marzo de 2025", "solo la última semana", ("2025-03-01", "2025-03-31")),
    ("contratos de la Alcaldía de Galapa en 2025", "y las dos últimas semanas", ("2025-01-01", "2025-12-31")),
])
def test_a_window_follow_up_asks_and_keeps_the_confirmed_scope(workflow, first, followup, previous):
    _first(workflow, first, "r3-fu")
    before = len(workflow.calls)
    second = workflow.run_query(followup, chat_id="r3-fu", today=TODAY)
    assert second["needs_clarification"] is True, followup
    assert len(workflow.calls) == before, "the follow-up must not reach the source"
    kept = second["resolved_params"]
    assert (kept.get("fecha_desde"), kept.get("fecha_hasta")) == previous
    assert "desde hoy" in second["response"] and "(1)" not in second["response"]
    assert tpq.MARKER in kept


def test_a_window_follow_up_without_a_previous_period_still_runs(workflow):
    first = workflow.run_query("contratos de la Alcaldía de Galapa", chat_id="r3-nofu", today=TODAY)
    assert first["needs_clarification"] is False
    second = workflow.run_query("y en los últimos 3 meses", chat_id="r3-nofu", today=TODAY)
    assert second["needs_clarification"] is False
    assert second["resolved_params"]["fecha_desde"] == "2026-07-01"


def test_a_search_the_engine_classifies_as_new_runs_with_its_own_window(workflow):
    """A new dataset with its own scope inherits nothing: the window is counted from today."""
    _first(workflow, "contratos de la Alcaldía de Galapa en 2025", "r3-new")
    second = workflow.run_query(
        "procesos de la Gobernación del Atlántico en los últimos 3 meses", chat_id="r3-new", today=TODAY,
    )
    assert second["needs_clarification"] is False
    assert second["resolved_params"]["fecha_desde"] == "2026-07-01"


@pytest.mark.parametrize("followup", [
    "contratos de aseo en Barranquilla en los últimos 3 meses",   # change_scope
    "ahora los de aseo en el último mes",                          # contextual_requery
])
def test_any_follow_up_that_inherits_context_asks_about_its_window_declared_over_asking(workflow, followup):
    """Fail-closed by choice: a follow-up that is not a NEW search and carries a window asks.

    The engine inherits or replaces the previous period for these intents; for the lexicon of
    ``time_relative`` it would silently keep the old one. Asking is cheaper than guessing.
    """
    _first(workflow, "contratos de la Alcaldía de Galapa en 2025", "r3-over")
    before = len(workflow.calls)
    second = workflow.run_query(followup, chat_id="r3-over", today=TODAY)
    assert second["needs_clarification"] is True
    assert len(workflow.calls) == before


def test_an_explicit_period_follow_up_is_not_a_window_and_runs(workflow):
    _first(workflow, "contratos de la Alcaldía de Galapa en 2025", "r3-ex")
    second = workflow.run_query("y de 2024?", chat_id="r3-ex", today=TODAY)
    assert second["needs_clarification"] is False
    assert second["resolved_params"]["fecha_desde"] == "2024-01-01"


# ── the follow-up decision (pure) ─────────────────────────────────────────────────────────

def _decide(text, intent="refine_filter", previous=None, pending=None, parsed=None):
    return tpq.block_for(
        text=text, intent=intent, parsed=parsed or {}, previous=previous or {}, pending=pending,
    )


PERIOD = {"fecha_desde": "2025-01-01", "fecha_hasta": "2025-12-31"}


def test_a_window_with_a_previous_period_asks_for_every_window_of_both_lexicons():
    for phrase in [*SAMPLE_NEW, "los últimos 3 meses", "la última semana", "últimos 10 días"]:
        block = _decide(f"y {phrase}", previous=PERIOD)
        assert block is not None and block.code == tpq.FOLLOWUP_CODE, phrase
        assert "2025-01-01 a 2025-12-31" in block.question


def test_no_window_or_no_previous_period_or_a_new_search_never_asks():
    assert _decide("y de 2024?", previous=PERIOD) is None
    assert _decide("y en los últimos 3 meses", previous={}) is None
    assert _decide("y en los últimos 3 meses", previous=PERIOD, intent="new_search") is None
    assert _decide("y en los últimos 3 meses", previous=PERIOD, intent="reset_context") is None
    assert _decide("y en los últimos 3 meses", previous={"fecha_desde": "2025-01-01"}) is not None


# ── P2-5: a norm citation is not a period ───────────────────────────────────────────────────

CITATIONS = [
    "decreto 1082 de 2015", "Decreto 1082 del 2015", "ley 80 de 1993", "Ley 1150 de 2007",
    "resolución 1234 de 2025", "resolucion 12 de 2024", "acuerdo 003 de 2020", "ordenanza 12 de 2019",
    "circular externa 001 de 2023", "circular 4 de 2022", "decreto ley 019 de 2012",
    "decreto-ley 019 de 2012", "decreto n.º 1082 de 2015", "decreto No. 1082 de 2015",
    "resolución 1.234 de 2025",
]


@pytest.mark.parametrize("citation", CITATIONS)
@pytest.mark.parametrize("window", ["en los últimos 6 meses", "en las últimas 2 semanas", "en la última semana"])
def test_a_norm_citation_next_to_a_window_is_not_an_anchor(citation, window):
    for text in (f"contratos por {citation} {window}", f"{window} contratos por {citation}"):
        found = _resolve(text)
        assert found.error_code is None, text
        assert found.matched and found.fecha_hasta == TODAY.isoformat(), text


def test_a_real_period_next_to_a_citation_still_asks():
    text = "contratos por decreto 1082 de 2015 en los últimos 6 meses de 2025"
    assert _resolve(text).error_code == CODE
    assert _resolve("contratos por la ley 80 de 1993 en marzo de 2025 en los últimos 3 meses").error_code == CODE


@pytest.mark.parametrize("text", [
    "contratos de aseo con plazo hasta 2027 en los últimos 3 meses",
    "contratos con cierre en 2026 en los últimos 3 meses",
    "contratos de aseo en Galapa en los últimos 3 meses y contratos de obra en Galapa en 2025",
])
def test_declared_over_firing_stays_closed(text):
    """Fail-closed by choice (0 cases in the 2.778 corpus texts): these still ask."""
    assert _resolve(text).error_code == CODE


@pytest.mark.parametrize("text", [
    "decreto 1082 de dos mil quince", "el decreto 1082, 2015", "ley de 1993", "resolución 1234 2025",
    "decreto 1082 de 15", "decreto de 1082 de 2015", "decreto de 2015", "artículo 1082 de 2015",
])
def test_the_citation_shape_is_closed(text):
    """Only «<kind> <number> de|del <year>» is a citation; anything looser is left to the authority."""
    assert nc.spans(text) == (), text


def test_the_citation_span_covers_the_whole_citation_and_nothing_else():
    text = "contratos por Decreto 1082 de 2015 en 2025"
    (start, end), = nc.spans(text)
    assert text[start:end] == "Decreto 1082 de 2015"
    assert nc.spans("contratos de 2025") == ()
    # a 5-digit «year» or a hyphenated process number is not a citation
    assert nc.spans("ley 80 de 20255") == ()
    assert nc.spans("resolución 5 de 2025-2026") == ()
