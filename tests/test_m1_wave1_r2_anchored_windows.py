"""M1 wave 1 R2 (ZEUS P1-A): a relative window next to another period is asked, never resolved.

«los últimos 3 meses de 2025» used to resolve as 2026-07-01..2026-10-01 (the window
counted from the clock, with the year silently dropped): a period DISJOINT from the one
asked. The decision (orchestrator): the temporal authority fails closed with the code
``relative_window_explicit_anchor`` and a clarification that names both readings. It
never interprets the anchored window (that would be a new capability) and no consumer
ever runs without the date.

The oracle is written from the Spanish meaning with the civil date fixed at 2026-10-01.
The lexicon is ENUMERATED from ``temporal_semantics`` (its relative patterns, its
months, its counts) instead of listing examples: a new relative pattern without a
decision here fails ``test_every_relative_kind_has_a_decision``.
"""
from __future__ import annotations

import copy
import itertools
import re
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.config import Settings, get_settings
from app.core import temporal_semantics as ts
from app.core import temporal_window_anchor as twa
from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.filters import accredit_filters
from app.core.analytics.contract_activity.models import AdmissionStatus, Operation
from app.core.capability_binding.supplier_contract import Outcome, decide
from app.core.entity_profile.frame_turn import TurnKind, run_profile_turn
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.entity_resolver import EntityResolver
from app.core.orchestrator import SecopalWorkflow
from app.core.period_consistency import TEMPORAL_UNRESOLVED, period_conflict
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.core.temporal_semantics import resolve_temporal_scope

TODAY = date(2026, 10, 1)
CODE = twa.ERROR_CODE
BUYER = "la Alcaldía de Galapa"


def _resolve(text: str):
    return resolve_temporal_scope(text, today=TODAY)


# ── the lexicon, enumerated ──────────────────────────────────────────────────────────

WINDOW_KINDS = {"trailing_months", "trailing_years", "trailing_weeks", "trailing_days", "last_7_days"}
# Calendar periods counted from the clock: no window to count back, never the subject of this rule.
NOT_A_WINDOW = {
    "this_month", "last_month", "months_ago", "this_year", "last_year", "years_ago",
    "this_week", "last_week", "today", "yesterday",
}
COUNTS = [*ts._COUNT_WORDS, "1", "3", "6", "12", "30"]


def _window_phrases(kind: str, counts=COUNTS) -> list[tuple[str, str | None]]:
    """(phrase, the count word or digits it carries)."""
    out: list[tuple[str, str | None]] = []
    for n in counts:
        if kind == "trailing_months":
            out += [(f"{a}{u} {n} meses", n) for a in ("los ", "") for u in ("últimos", "ultimos")]
        elif kind == "trailing_years":
            out += [(f"{a}{u} {n} años", n) for a in ("los ", "") for u in ("últimos", "ultimos")]
        elif kind == "trailing_days":
            out += [(f"{u} {n} {d}", n) for u in ("últimos", "ultimos") for d in ("días", "dias")]
        elif kind == "trailing_weeks":
            out += [(f"{a}{u} {n} semanas", n) for a in ("las ", "") for u in ("últimas", "ultimas")]
    if kind == "last_7_days":
        out += [(f"{a}{u} semana", None) for a in ("la ", "") for u in ("última", "ultima")]
    return out


ALL_WINDOWS = [(kind, phrase) for kind in sorted(WINDOW_KINDS) for phrase, _ in _window_phrases(kind)]
# One representative per kind and per article/accent variant: crossed with EVERY anchor below.
SAMPLE_WINDOWS = [
    (kind, phrase) for kind in sorted(WINDOW_KINDS)
    for phrase, _ in _window_phrases(kind, counts=("tres", "12"))
]

YEARS = ["2019", "2024", "2025", "2026"]
MONTH_YEAR_JOINS = [" de ", " del ", " ", ", ", "-", "/"]
MONTHS = list(ts._MONTH_ALIASES)
MONTH_ANCHORS = (
    MONTHS
    + [f"{m}{join}{year}" for m in MONTHS for join in MONTH_YEAR_JOINS for year in ("2025",)]
    + [f"mes de {m}" for m in MONTHS]
)
DEICTIC_ANCHORS = [
    "este año", "el año pasado", "año pasado", "este mes", "el mes pasado", "mes pasado",
    "esta semana", "la semana pasada", "hace 2 años", "hace tres meses",
]
PERIOD_ANCHORS = [
    "primer trimestre de 2025", "segundo semestre de 2024", "2do trimestre de 2025",
    "último trimestre de 2025", "el primer semestre de 2024",
    "enero a marzo de 2025", "entre marzo y mayo de 2025", "del 1 al 15 de marzo de 2025",
    "2023 a 2025", "entre 2022 y 2024", "desde 2023", "hasta 2025", "antes de 2024",
    "después de 2022", "a partir de 2023", "desde marzo de 2025", "hasta marzo de 2025",
    "2025-03-01", "2025-03-01 a 2025-03-31", "15 de marzo de 2025", "el 15 de marzo",
]
ANCHORS = YEARS + MONTH_ANCHORS + DEICTIC_ANCHORS + PERIOD_ANCHORS
# One anchor per class: crossed with EVERY window (kind x count x article x accent).
CLASS_ANCHORS = [
    "2025", "marzo de 2025", "marzo", "el año pasado", "este año", "primer trimestre de 2025",
    "enero a marzo de 2025", "antes de 2024", "2025-03-01",
]
AFTER_CONNECTORS = ["", "de ", "del ", "en ", "durante ", "del año ", "de la vigencia ", "y "]
BEFORE_SHAPES = [
    "{a}, {w}", "{a} {w}", "en {a} en {w}", "{a} y {w}",
    "contratos de {a} de aseo en {w}",
]
CONTEXT = f"contratos de {BUYER} en"


def _shapes(window: str, anchor: str) -> list[str]:
    after = [f"{CONTEXT} {window} {c}{anchor}" for c in AFTER_CONNECTORS]
    before = [shape.format(a=anchor, w=window) for shape in BEFORE_SHAPES]
    return after + before


def test_every_relative_kind_has_a_decision():
    kinds = {kind for _, kind in ts._RELATIVE_PATTERNS}
    assert kinds == WINDOW_KINDS | NOT_A_WINDOW, kinds ^ (WINDOW_KINDS | NOT_A_WINDOW)
    assert {k for _, k in ts._RELATIVE_PATTERNS if k in ts._WINDOW_KINDS} == WINDOW_KINDS
    assert {p for p, k in ts._RELATIVE_PATTERNS if k in WINDOW_KINDS} == set(ts._WINDOW_PATTERNS)


def test_the_enumerated_windows_are_real_window_phrases():
    by_kind = {kind: pattern for pattern, kind in ts._RELATIVE_PATTERNS}
    for kind, phrase in ALL_WINDOWS:
        assert by_kind[kind].search(phrase), (kind, phrase)


def test_the_enumerated_anchors_resolve_on_their_own():
    """An anchor that the authority does not read is not an anchor: the generator is sound."""
    for anchor in ANCHORS:
        found = _resolve(anchor)
        assert found.matched and not found.error_code, anchor


# ── the property: window x anchor ⇒ ask ──────────────────────────────────────────────

def _failures(windows, anchors) -> list[str]:
    bad: list[str] = []
    for (_, window), anchor in itertools.product(windows, anchors):
        for text in _shapes(window, anchor):
            found = _resolve(text)
            if found.error_code != CODE or found.fecha_desde or found.fecha_hasta:
                bad.append(f"{text!r} -> {found.error_code} {found.fecha_desde}..{found.fecha_hasta}")
    return bad


def test_every_window_with_every_class_of_anchor_asks():
    bad = _failures(ALL_WINDOWS, CLASS_ANCHORS)
    assert not bad, bad[:10]


def test_every_kind_of_window_with_every_anchor_of_the_lexicon_asks():
    bad = _failures(SAMPLE_WINDOWS, ANCHORS)
    assert not bad, bad[:10]


def test_the_anchor_is_found_anywhere_in_the_turn_not_only_next_to_the_window():
    """Adjacency would be a finite vocabulary that extra words walk around."""
    for text in (
        "contratos de 2025 de aseo de la Alcaldía de Galapa en los últimos 3 meses",
        "en los últimos 3 meses los contratos de aseo de la Alcaldía de Galapa de 2025",
        "en marzo de 2025 quiero ver los contratos de la Alcaldía de Galapa de la última semana",
        "contratos de la Alcaldía de Galapa en los últimos 3 meses, año pasado",
    ):
        assert _resolve(text).error_code == CODE, text


def test_the_error_names_both_readings_and_carries_no_dates():
    found = _resolve("contratos de la Alcaldía de Galapa en los últimos 3 meses de 2025")
    assert found.should_clarify and found.matched
    assert (found.fecha_desde, found.fecha_hasta) == (None, None)
    assert found.precision == "invalid"
    assert "los últimos 3 meses" in found.clarification
    assert "de 2025" in found.clarification
    # R3: the question names both periods and asks for the period written out. No numbered
    # options and no «completo»: those words come back as the topic of the next search.
    assert "desde hoy" in found.clarification
    assert "(1)" not in found.clarification and "completo" not in found.clarification
    # The span covers the window and the anchor, so a consumer that masks it consumed both.
    text = "contratos de la Alcaldía de Galapa en los últimos 3 meses de 2025"
    assert text[slice(*found.span)] == found.phrase
    assert found.phrase == "los últimos 3 meses de 2025"


def test_with_two_windows_the_first_one_is_named():
    text = "contratos en la última semana y los últimos 3 meses de 2025"
    found = _resolve(text)
    assert found.error_code == CODE
    assert found.phrase.startswith("la última semana")
    assert found.span[0] == text.index("la última semana")
    assert found.clarification.count("«la última semana»") == 1


def test_the_dormant_time_authority_names_the_error_as_two_temporal_facts_not_a_bad_count():
    from app.core import time_authority as ta

    reason = ta._LEGACY_REASON[CODE]
    assert reason == ta.TIME_MULTIPLE_TEMPORAL_FACTS
    found = _resolve("contratos en los últimos 3 meses de 2025")
    assert ta._legacy_failure(found).detail == reason


def test_the_errors_of_the_authority_that_come_first_keep_their_own_code():
    assert _resolve("los últimos 3 meses de 31 de febrero de 2025").error_code == "invalid_date"
    assert _resolve("los últimos 3 meses de 2025-03-31 a 2025-01-01").error_code == "reverse_range"


# ── what must NOT change: relative windows with no other period, and every other period ──

def _months_back(months: int) -> str:
    year, zero = divmod(TODAY.year * 12 + TODAY.month - 1 - months, 12)
    return date(year, zero + 1, 1).isoformat()


def test_a_window_alone_resolves_exactly_as_before_for_every_count_and_phrase():
    expected_since = {
        "trailing_months": lambda n: _months_back(n),
        "trailing_years": lambda n: _months_back(12 * n),
        "trailing_weeks": lambda n: date.fromordinal(TODAY.toordinal() - 7 * n).isoformat(),
        "trailing_days": lambda n: date.fromordinal(TODAY.toordinal() - n).isoformat(),
        "last_7_days": lambda n: date.fromordinal(TODAY.toordinal() - 7).isoformat(),
    }
    seen = 0
    for kind in sorted(WINDOW_KINDS):
        for phrase, count in _window_phrases(kind):
            n = None if count is None else (int(count) if count.isdigit() else ts._COUNT_WORDS[count])
            found = _resolve(f"contratos de {BUYER} en {phrase}")
            assert found.matched and not found.error_code, phrase
            assert found.fecha_hasta == TODAY.isoformat(), phrase
            assert found.fecha_desde == expected_since[kind](n), phrase
            seen += 1
    assert seen == len(ALL_WINDOWS)


@pytest.mark.parametrize("phrase,code", [
    ("los últimos 121 meses", "unsupported_month_count"),
    ("los últimos 31 años", "unsupported_year_count"),
    ("las últimas 53 semanas", "unsupported_week_count"),
    ("los últimos 366 días", "unsupported_day_count"),
    ("los últimos 0 meses", "unsupported_month_count"),
])
def test_an_out_of_range_window_keeps_its_own_error(phrase, code):
    assert _resolve(f"contratos de {BUYER} en {phrase}").error_code == code


@pytest.mark.parametrize("text,expected", [
    ("en los últimos 3 meses", ("2026-07-01", "2026-10-01")),
    ("los últimos tres meses", ("2026-07-01", "2026-10-01")),
    ("en los últimos 12 meses", ("2025-10-01", "2026-10-01")),
    ("en los últimos 2 años", ("2024-10-01", "2026-10-01")),
    ("la última semana", ("2026-09-24", "2026-10-01")),
    ("en las últimas 2 semanas", ("2026-09-17", "2026-10-01")),
    ("en los últimos 10 días", ("2026-09-21", "2026-10-01")),
    ("en los últimos 3 meses hasta hoy", ("2026-07-01", "2026-10-01")),   # «hoy» is not an anchor
    ("la última semana, hoy", ("2026-09-24", "2026-10-01")),
    ("primer semestre de 2024", ("2024-01-01", "2024-06-30")),
    ("segundo trimestre de 2025", ("2025-04-01", "2025-06-30")),
    ("el último trimestre de 2025", ("2025-10-01", "2025-12-31")),
    ("enero a marzo de 2025", ("2025-01-01", "2025-03-31")),
    ("desde marzo de 2025", ("2025-03-01", None)),
    ("hasta marzo de 2025", (None, "2025-03-31")),
    ("antes de 2024", (None, "2023-12-31")),
    ("desde 2024 hasta hoy", ("2024-01-01", None)),
    ("en 2025", ("2025-01-01", "2025-12-31")),
    ("2025", ("2025-01-01", "2025-12-31")),
    ("marzo de 2025", ("2025-03-01", "2025-03-31")),
    ("en marzo", ("2026-03-01", "2026-03-31")),
    ("entre 2022 y 2024", ("2022-01-01", "2024-12-31")),
    ("este año", ("2026-01-01", "2026-12-31")),
    ("el año pasado", ("2025-01-01", "2025-12-31")),
    ("hace 2 años", ("2024-01-01", "2024-12-31")),
    ("el mes pasado", ("2026-09-01", "2026-09-30")),
    ("hoy", ("2026-10-01", "2026-10-01")),
])
def test_everything_that_is_not_a_window_next_to_another_period_resolves_as_before(text, expected):
    found = _resolve(f"contratos de {BUYER} {text}")
    assert not found.error_code, text
    assert (found.fecha_desde, found.fecha_hasta) == expected, text


def test_a_year_protected_as_part_of_a_name_is_not_an_anchor():
    text = "contratos de la Alcaldía de Galapa en los últimos 3 meses de 2025"
    start = text.index("2025")
    found = resolve_temporal_scope(text, today=TODAY, protected_spans=((start, start + 4),))
    assert not found.error_code
    assert (found.fecha_desde, found.fecha_hasta) == ("2026-07-01", "2026-10-01")


def test_a_year_followed_by_money_is_not_an_anchor():
    found = _resolve("contratos en los últimos 3 meses por más de 2025 millones")
    assert not found.error_code
    assert found.fecha_desde == "2026-07-01"


# ── the seam (pure): find_anchored_window ────────────────────────────────────────────────

WINDOWS = (re.compile(r"ultimos \d+ meses"),)
IGNORED = (re.compile(r"hoy"),)


def _rest_reading(word: str):
    def resolve_rest(text: str):
        index = text.find(word)
        return (True, (index, index + len(word))) if index >= 0 else (False, None)
    return resolve_rest


def test_the_window_is_blanked_before_the_rest_is_read():
    # The «rest» reader would match the window's own words: it must never see them.
    assert twa.find_anchored_window("en ultimos 3 meses", WINDOWS, IGNORED, _rest_reading("ultimos")) is None
    assert twa.find_anchored_window("en ultimos 3 meses", WINDOWS, IGNORED, _rest_reading("meses")) is None


def test_the_ignored_words_are_blanked_too():
    text = "ultimos 3 meses hoy"
    assert twa.find_anchored_window(text, WINDOWS, IGNORED, _rest_reading("hoy")) is None
    assert twa.find_anchored_window(text, WINDOWS, (), _rest_reading("hoy")) is not None


def test_no_window_means_no_conflict_whatever_the_rest_says():
    assert twa.find_anchored_window("en 2025", WINDOWS, IGNORED, _rest_reading("2025")) is None


def test_the_conflict_reports_window_anchor_and_the_span_that_covers_both():
    text = "los ultimos 3 meses de aseo del 2025"
    found = twa.find_anchored_window(text, WINDOWS, IGNORED, _rest_reading("del 2025"))
    assert found == twa.AnchoredWindow("ultimos 3 meses", "del 2025", (4, len(text)), ((4, 19), (28, 36)))
    # R6: ``mentions`` are the two periods alone; ``span`` also covers what lies between them («de aseo»).
    assert [text[a:b] for a, b in found.mentions] == ["ultimos 3 meses", "del 2025"]
    found = twa.find_anchored_window("del 2025 ultimos 3 meses", WINDOWS, IGNORED, _rest_reading("del 2025"))
    assert found.span == (0, 24)
    question = twa.clarification(found)
    # each reading is named where it is offered, and the two are named together up front
    assert question.count("«ultimos 3 meses»") == 1 and question.count("«del 2025»") == 1


# ── the consumers: the error reaches each one as a clarification ────────────────────────

ANCHORED_FORMS = [
    "en los últimos 3 meses de 2025",
    "en la última semana de marzo de 2025",
    "en los últimos 6 meses del 2024",
    "de 2025, los últimos 3 meses",
    "en los últimos 3 meses del año pasado",
    "en la última semana del mes pasado",
    "en los últimos 10 días de marzo",
    "en los últimos 2 años antes de 2025",
    "en los últimos 3 meses del primer semestre de 2025",
]
ANCHORED_IDS = [f"anchored{i}" for i in range(len(ANCHORED_FORMS))]


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _parse(router: QueryRouter, text: str):
    return router.parse(text, today=TODAY)


@pytest.mark.parametrize("form", ANCHORED_FORMS, ids=ANCHORED_IDS)
@pytest.mark.parametrize("template", [
    "contratos de {b} {t}", "procesos de la Gobernación del Atlántico {t}", "proveedores de {b} {t}",
])
def test_search_parse_carries_the_error_and_no_date(router, form, template):
    params = _parse(router, template.format(b=BUYER, t=form)).params
    assert params.get("_temporal_error") == CODE
    assert params.get("_temporal_clarification")
    assert not params.get("fecha_desde") and not params.get("fecha_hasta")


@pytest.mark.parametrize("form", ANCHORED_FORMS, ids=ANCHORED_IDS)
def test_contract_activity_legacy_reader_asks(router, form):
    raw = f"¿Cuántos contratos firmó {BUYER} {form}?"
    parsed = _parse(router, raw)
    admission = admit_contract_activity(
        raw, dict(parsed.params), today=TODAY, semantic_evidence=parsed.semantic_evidence.to_state(),
    )
    assert admission.status is AdmissionStatus.CLARIFY, raw
    assert admission.reason.startswith("SIGNATURE_PERIOD") or admission.reason.startswith("UNSUPPORTED"), raw


@pytest.mark.parametrize("form", ANCHORED_FORMS, ids=ANCHORED_IDS)
@pytest.mark.parametrize("operation", [Operation.TOTAL, Operation.BY_REPORTED_STATE])
def test_contract_activity_authority_filters_ask_even_if_the_parse_dropped_the_error(router, form, operation):
    raw = f"¿Cuántos contratos firmó {BUYER} {form}?"
    params = {k: v for k, v in _parse(router, raw).params.items() if not k.startswith("_temporal")}
    filters = accredit_filters(raw, params, operation, today=TODAY)
    assert filters.reason == "SIGNATURE_PERIOD_UNRESOLVED", raw
    assert filters.date_from is None and filters.date_to is None


@pytest.mark.parametrize("form", ANCHORED_FORMS, ids=ANCHORED_IDS)
@pytest.mark.parametrize("template", ["proveedores de {b} {t}", "5 proveedores de {b} {t}"])
def test_suppliers_a2_asks_and_never_executes(router, form, template):
    query = template.format(b=BUYER, t=form)
    decision = decide(query, dict(_parse(router, query).params), today=TODAY)
    assert decision.outcome is Outcome.CLARIFY, (query, decision.outcome)
    assert decision.admitted_params is None or not (
        decision.admitted_params.get("fecha_desde") or decision.admitted_params.get("fecha_hasta")
    )


@pytest.mark.parametrize("form", ANCHORED_FORMS, ids=ANCHORED_IDS)
def test_suppliers_a2_asks_even_if_the_parse_dropped_the_error(router, form):
    query = f"proveedores de {BUYER} {form}"
    params = {k: v for k, v in _parse(router, query).params.items() if not k.startswith("_temporal")}
    params.pop("fecha_desde", None), params.pop("fecha_hasta", None)
    assert decide(query, params, today=TODAY).outcome is Outcome.CLARIFY, query


@pytest.mark.parametrize("form", ANCHORED_FORMS, ids=ANCHORED_IDS)
def test_period_consistency_reports_the_authority_error(form):
    assert period_conflict(f"contratos de {BUYER} {form}", (None, None), today=TODAY) == TEMPORAL_UNRESOLVED


@pytest.mark.parametrize("form", ANCHORED_FORMS, ids=ANCHORED_IDS)
def test_the_legacy_entity_profile_turn_asks_instead_of_running_without_a_date(router, form):
    query = f"perfil de {BUYER} {form}"
    turn = classify_entity_profile_turn(query, _parse(router, query).params)
    assert turn.detected
    assert turn.clarification
    assert turn.scope is None and turn.plan is None


def test_the_legacy_entity_profile_turn_asks_for_any_error_of_the_authority_not_only_this_one(router):
    query = f"perfil de {BUYER} del 31 de febrero de 2025"
    params = _parse(router, query).params
    assert params.get("_temporal_error") == "invalid_date"
    turn = classify_entity_profile_turn(query, params)
    assert turn.clarification and turn.plan is None


def test_the_legacy_entity_profile_turn_with_a_window_alone_still_runs(router):
    query = f"perfil de {BUYER} en los últimos 3 meses"
    turn = classify_entity_profile_turn(query, _parse(router, query).params)
    assert turn.clarification is None
    assert (turn.scope.time_from, turn.scope.time_to) == ("2026-07-01", "2026-10-01")


@pytest.mark.parametrize("form", ANCHORED_FORMS[:4], ids=ANCHORED_IDS[:4])
def test_the_frame_profile_turn_asks_and_makes_no_source_call(router, form):
    query = f"perfil de {BUYER} {form}"
    client = MagicMock()
    outcome = run_profile_turn(
        user_query=query, params=_parse(router, query).params,
        state=ProfileConversationState(), turn_id="r2", soql_builder=SoQLBuilder(),
        secop_client=client, today=TODAY,
    )
    assert outcome.kind is TurnKind.CLARIFICATION
    assert outcome.response and "2025" in outcome.response or "pasado" in outcome.response
    assert client.mock_calls == []


# ── end to end through the workflow: zero source calls, the question, the previous scope kept ──

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
    "contratos de la Alcaldía de Galapa en los últimos 3 meses de 2025",
    "contratos de la Alcaldía de Galapa en la última semana de marzo de 2025",
    "contratos de aseo en Barranquilla en los últimos 6 meses del 2024",
    "procesos de la Gobernación del Atlántico en los últimos 3 meses de 2025",
    "perfil de la Alcaldía de Galapa en los últimos 3 meses de 2025",
    "proveedores de la Alcaldía de Galapa en los últimos 3 meses de 2025",
    "¿Cuántos contratos firmó la Alcaldía de Galapa en los últimos 3 meses de 2025?",
])
def test_the_workflow_asks_with_zero_source_calls_and_never_drops_the_date(workflow, query):
    result = workflow.run_query(query, chat_id="r2-ask", today=TODAY)
    assert result["needs_clarification"] is True, query
    assert workflow.calls == [], (query, workflow.calls)
    response = result["response"]
    assert "desde hoy" in response and "(1)" not in response, response
    params = result.get("resolved_params") or {}
    assert not params.get("fecha_desde") and not params.get("fecha_hasta")


def test_the_workflow_still_runs_a_window_alone_with_its_dates(workflow):
    result = workflow.run_query(
        "contratos de la Alcaldía de Galapa en los últimos 3 meses", chat_id="r2-ok", today=TODAY,
    )
    assert result["needs_clarification"] is False
    assert (result["resolved_params"]["fecha_desde"], result["resolved_params"]["fecha_hasta"]) == (
        "2026-07-01", "2026-10-01",
    )
    assert len(workflow.calls) >= 1


def test_a_followup_with_an_anchored_window_asks_and_keeps_the_confirmed_scope(workflow):
    first = workflow.run_query("contratos de la Alcaldía de Galapa en 2024", chat_id="r2-fu", today=TODAY)
    assert first["needs_clarification"] is False
    before = len(workflow.calls)
    second = workflow.run_query("y en los últimos 3 meses de 2025", chat_id="r2-fu", today=TODAY)
    assert second["needs_clarification"] is True
    assert len(workflow.calls) == before, "the follow-up must not reach the source"
    kept = second["resolved_params"]
    assert (kept.get("fecha_desde"), kept.get("fecha_hasta")) == ("2024-01-01", "2024-12-31")
    # R3: the question stays open; an explicit period answers it (a window would not).
    third = workflow.run_query("octubre a diciembre de 2025", chat_id="r2-fu", today=TODAY)
    assert third["needs_clarification"] is False
    assert third["resolved_params"]["fecha_desde"] == "2025-10-01"
    assert len(workflow.calls) > before


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
            "perfil de la Alcaldía de Galapa en los últimos 3 meses de 2025", chat_id="r2-fr", today=TODAY,
        )
    finally:
        get_settings.cache_clear()
    assert result["needs_clarification"] is True
    assert calls == []


# ── mutant M16: the A2 period guard reads the turn WITHOUT the buyer's own surface ────────

@pytest.mark.parametrize("dated_name", ["7 de Agosto", "20 de Julio", "2025"])
def test_a_buyer_name_that_carries_a_date_is_not_a_period_of_the_turn(router, dated_name):
    """«Hospital 7 de Agosto», «Colegio 20 de Julio»: the name is not the period asked.

    The router reads that date out of the name before it resolves the buyer (a known
    upstream limit), so the buyer arrives here the way a carried-over scope delivers it:
    its surface is in ``buyer_mention`` and the turn has no period of its own.
    """
    base = f"proveedores de {BUYER}"
    params = copy.deepcopy(_parse(router, base).params)
    params["entidad_resolution"]["metadata"]["buyer_mention"] = f"{BUYER} {dated_name}"
    assert not params.get("fecha_desde") and not params.get("fecha_hasta")
    decision = decide(f"{base} {dated_name}", params, today=TODAY)
    assert decision.outcome is Outcome.EXECUTE, (dated_name, decision.reason)
    assert not (decision.admitted_params.get("fecha_desde") or decision.admitted_params.get("fecha_hasta"))


def test_the_same_date_outside_the_buyer_name_is_still_a_period_the_guard_asks_about(router):
    base = f"proveedores de {BUYER}"
    params = copy.deepcopy(_parse(router, base).params)
    decision = decide(f"{base} 7 de Agosto", params, today=TODAY)
    assert decision.outcome is Outcome.CLARIFY
