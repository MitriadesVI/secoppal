"""M1 wave 1 R3 (ZEUS2 P1-1 and P2-5), grammar half: the relative windows of ``time_relative`` ask too.

Paridad temporal con ``main``: SOLO queda la gramática de la ruta de cita (``time_grammar``,
``time_relative``, ``normative_citation``). Los consumidores legados (parse, CA, SUPPLIERS,
perfiles, workflow, follow-up) pasaron al corpus ``docs/audits/m1_wave1/E_TIME_REGRESSION_CASES.jsonl``.


R2 made a relative window next to another period of the same turn a question, but its rule
only knew the legacy lexicon (``time_grammar``: «últimos N meses», «la última
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

import pytest

from app.core import normative_citation as nc
from app.core import temporal_window_anchor as twa
from app.core import time_grammar as ts
from app.core import time_relative as tr
from app.core.time_grammar import resolve_temporal_scope

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
