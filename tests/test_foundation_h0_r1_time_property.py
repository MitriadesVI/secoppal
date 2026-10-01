"""M1-H0 R1: the year-coverage guard sees the qualifier in BOTH orders, and its lexicon is exactly the list.

ZEUS found that a numeric or ordinal token cut the walk («2025 segundo semestre», «primeros 100 días de
2025»), that plural periods and later bounds were missing, and that numeric dates hid the year.  The lists
below are written out independently of the module and compared by EQUALITY: removing a word from the module
(not only emptying a list) must fail here.  Guarantee under test: a guard of the frontier of the cited span,
not a temporal parser.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from app.core.foundation_time_coverage import (
    LEADING_BOUNDS, LEADING_LEXICON, MONTHS, ORDINALS, PERIODS, TRAILING_BOUNDS, TRAILING_LEXICON, WINDOW,
    _GLUE, _TRAILING_GLUE, uncited_time_qualifier,
)
from app.core.interpretation_contract import AcceptanceStatus, RawSpan
from tests.test_foundation_h0_time_coverage import ASKED, admit, assert_asked, assert_year

I_MONTHS = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "setiembre",
            "octubre", "noviembre", "diciembre", "ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep",
            "sept", "oct", "nov", "dic"]
I_PERIODS = ["semestre", "trimestre", "bimestre", "cuatrimestre", "quincena", "semana", "mes", "mitad", "dia",
             "semestres", "trimestres", "bimestres", "cuatrimestres", "quincenas", "semanas", "meses", "mitades",
             "dias", "inicio", "inicios", "comienzo", "comienzos", "principio", "principios", "mediados",
             "final", "finales", "fines", "fin", "cierre"]
I_LEADING = ["desde", "hasta", "entre", "antes", "despues", "tras", "partir", "previo", "previa", "previos",
             "posterior", "posteriores", "anterior", "anteriores", "pre", "post"]
I_TRAILING = ["adelante", "atras", "posterior", "posteriores", "anterior", "anteriores", "siguientes", "hoy",
              "fecha", "antes", "despues", "ahora", "actualidad", "presente", "aca"]
I_ORDINALS = ["primer", "primero", "primera", "primeros", "primeras", "segundo", "segunda", "segundos",
              "segundas", "tercer", "tercero", "tercera", "terceros", "terceras", "cuarto", "cuarta", "cuartos",
              "cuartas", "ultimo", "ultima", "ultimos", "ultimas", "un", "una", "dos", "tres", "cuatro", "cinco",
              "seis", "siete", "ocho", "nueve", "diez", "once", "doce"]
I_GLUE = ["de", "del", "el", "la", "los", "las", "ano", "anos", "y", "e", "o", "u", "a", "al"]
I_TRAILING_ONLY_GLUE = ["en", "hacia", "para", "hasta"]


# ── the module lexicon IS the list: equality, so removing one word fails ─────────────────────────────

def test_every_lexicon_is_exactly_the_independent_list():
    assert set(MONTHS) == set(I_MONTHS)
    assert set(PERIODS) == set(I_PERIODS)
    assert set(LEADING_BOUNDS) == set(I_LEADING)
    assert set(TRAILING_BOUNDS) == set(I_TRAILING)
    assert set(ORDINALS) == set(I_ORDINALS)
    assert set(_GLUE) == set(I_GLUE) | set(I_ORDINALS)
    assert set(_TRAILING_GLUE) == set(I_GLUE) | set(I_ORDINALS) | set(I_TRAILING_ONLY_GLUE)
    assert set(LEADING_LEXICON) == set(I_MONTHS) | set(I_PERIODS) | set(I_LEADING)
    assert set(TRAILING_LEXICON) == set(I_MONTHS) | set(I_PERIODS) | set(I_TRAILING)


# ── behaviour per word, not per list: each word alone is enough to ask ───────────────────────────────

@pytest.mark.parametrize("word", I_MONTHS + I_PERIODS + I_LEADING)
def test_every_leading_word_alone_before_the_year_is_asked(word):
    for raw in (f"contratos {word} 2025", f"contratos {word} de 2025", f"contratos {word} del año 2025"):
        assert_asked(admit(raw))


@pytest.mark.parametrize("word", I_MONTHS + I_PERIODS + I_TRAILING)
def test_every_trailing_word_alone_after_the_year_is_asked(word):
    for raw in (f"contratos 2025 {word}", f"contratos de 2025 y {word}", f"contratos del 2025, {word}"):
        assert_asked(admit(raw))


@pytest.mark.parametrize("glue", I_GLUE + I_ORDINALS)
def test_every_glue_word_lets_the_walk_reach_the_qualifier_in_both_directions(glue):
    assert_asked(admit(f"contratos desde {glue} 2025"))
    assert_asked(admit(f"contratos 2025 {glue} semestre"))


@pytest.mark.parametrize("glue", I_TRAILING_ONLY_GLUE)
def test_the_trailing_only_glue_reaches_the_qualifier_after_the_year(glue):
    assert_asked(admit(f"contratos 2025 {glue} semestre"))


# ── both orders by property over the lexicon: qualifier before AND after the year ────────────────────

@pytest.mark.parametrize("ordinal", I_ORDINALS)
@pytest.mark.parametrize("period", ["semestre", "trimestres", "meses", "semanas", "dias", "bimestre", "quincena"])
def test_an_ordinal_period_is_seen_before_and_after_the_year(ordinal, period):
    for raw in (f"contratos del {ordinal} {period} de 2025", f"contratos de los {ordinal} {period} del 2025",
                f"contratos de 2025 {ordinal} {period}", f"contratos 2025, {ordinal} {period}",
                f"contratos del 2025 los {ordinal} {period}", f"contratos 2025 - {ordinal} {period}"):
        assert_asked(admit(raw))


@pytest.mark.parametrize("number", ["1", "2", "6", "12", "30", "100", "365"])
@pytest.mark.parametrize("period", ["dias", "semanas", "meses", "trimestres"])
def test_a_counted_period_is_seen_in_both_orders(number, period):
    for raw in (f"contratos de los primeros {number} {period} de 2025", f"contratos de 2025 primeros {number} {period}",
                f"contratos de los últimos {number} {period} de 2025", f"contratos 2025 últimos {number} {period}"):
        assert_asked(admit(raw))


@pytest.mark.parametrize("raw", [
    "contratos de los primeros meses de 2025", "contratos de los últimos meses de 2025",
    "contratos de los primeros 6 meses de 2025", "contratos de los primeros 100 días de 2025",
    "contratos de las primeras semanas de 2025", "contratos de los dos primeros trimestres de 2025",
    "contratos 2025 los dos primeros trimestres", "contratos de 2025 las primeras semanas",
    "contratos de 2025 segundo semestre", "contratos 2025 primer trimestre", "contratos del 2025, segundo semestre",
    "contratos 2025 segundo semestre", "contratos 2025 primer semestre", "contratos 2025 - segundo semestre",
    "contratos de segundo semestre de 2025", "contratos de primer trimestre 2025",
])
def test_the_ordinal_and_plural_cases_of_the_audit_are_asked(raw):
    assert_asked(admit(raw))


@pytest.mark.parametrize("raw", [
    "contratos del 2025 o antes", "contratos del 2025 o después", "contratos del 2025 y antes",
    "contratos del 2025 y después", "contratos del 2025 hasta ahora", "contratos del 2025 a la actualidad",
    "contratos del 2025 al presente", "contratos del 2025 al día de hoy", "contratos del 2025 para acá",
    "contratos del 2025 a la fecha", "contratos del 2025 hasta la fecha", "contratos de 2025 hasta hoy",
    "contratos pre 2025", "contratos post 2025", "contratos pre-2025", "contratos post-2025",
    "contratos previos al 2025", "contratos previa al 2025", "contratos posteriores al 2025",
    "contratos anteriores al 2025",
])
def test_the_bounds_of_the_audit_are_asked_before_and_after(raw):
    assert_asked(admit(raw))


@pytest.mark.parametrize("raw", [
    "contratos de fin de año 2025", "contratos de fin de 2025", "contratos de fin del 2025",
    "contratos del cierre de 2025", "contratos al cierre del 2025", "contratos de finales de 2025",
    "contratos de comienzos de 2025", "contratos de mediados de 2025", "contratos de principios de 2025",
    "contratos de inicios del año 2025", "contratos 2025 a finales", "contratos 2025 a mediados",
    "contratos 2025 a comienzos", "contratos de 2025 al cierre",
])
def test_the_year_edges_are_asked_before_and_after(raw):
    assert_asked(admit(raw))


@pytest.mark.parametrize("raw", [
    "contratos del 15/03/2025", "contratos del 2025-03", "contratos del 2025/03", "contratos del 12/2025",
    "contratos del 03-2025", "contratos del 2025.03", "contratos del 15-03-2025", "contratos del 15.03.2025",
    "contratos del 2025-03-15", "contratos del periodo 2025-1", "contratos del periodo 2025-2",
    "contratos del periodo 2025-I", "contratos del periodo 2025-II", "contratos del periodo 2025-IV",
    "contratos a junio 30 de 2025", "contratos de marzo 15 de 2025", "contratos de marzo 15, 2025",
    "contratos del 30 de junio de 2025", "contratos de 2025 junio 30", "contratos de 2025 30 de junio",
    "contratos del 15 / 03 / 2025", "contratos Q1 2025", "contratos T1 2025", "contratos H1 2025",
    "contratos 2025 Q3", "contratos 2025 T2", "contratos 2025 H2", "contratos 1T 2025", "contratos 2025 2S",
])
def test_numeric_and_compact_dates_that_contain_the_year_are_asked(raw):
    assert_asked(admit(raw))


# ── what must still execute: the repair does not turn numbers into a blanket qualifier ──────────────

@pytest.mark.parametrize("raw", [
    "contratos en 2025", "contratos del 2025", "contratos de 2025", "contratos 2025", "contratos año 2025",
    "contratos de 2025 de 30 millones", "contratos de 2025 de 500 millones", "contratos de 2025 de 5 municipios",
    "contratos de 2025 en 500 municipios", "contratos de 2025 para 300 personas", "contratos de 2025 y 30 contratistas",
    "contratos hasta 500 millones en 2025", "contratos de 2025 hasta 500 millones", "contratos del 2025 de obra",
    "contratos de 2025, 3 de obra", "contratos de 5 obras en 2025", "contratos de más de 300 millones del 2025",
    "contratos de 2025 y de más de 300 millones", "contratos 2025 mayores a 2000 millones",
    "contratos con 12 meses en 2025",  # «en» is not glue before the year: «meses» is out of the walk
    "contratos de 3025 en 2025", "contratos de 12025 pesos del 2025", "contratos de 1899 pesos en 2025",
    "contratos de 2025 por $1.500 millones", "contratos de 2025 por 1.500.000",
])
def test_a_year_with_amounts_counts_or_plain_context_still_executes(raw):
    assert_year(admit(raw, "2025", raw.count("2025")))


def test_a_primero_de_mayo_street_after_en_is_a_plain_year_and_the_doc_says_so():
    # The R0 docstring/candidate claimed this asks; it does not («en» is not glue before the year).
    assert_year(admit("contratos de la Avenida Primero de Mayo en 2025"))
    assert_asked(admit("contratos de la Avenida Primero de Mayo 2025"))  # the real over-ask: without «en»


# ── properties, both orders ──────────────────────────────────────────────────────────────────────────

GLUE_BEFORE = st.lists(st.sampled_from(I_GLUE + I_ORDINALS + ["1", "15", "100"]), max_size=WINDOW - 1)
FILLER = ["contratos", "obra", "aseo", "ICBF", "Soledad", "500", "millones", "con", "que", "firmados"]


@settings(max_examples=400, deadline=None)
@given(st.lists(st.sampled_from(FILLER), max_size=5), st.sampled_from(I_MONTHS + I_PERIODS + I_LEADING), GLUE_BEFORE)
def test_property_a_leading_qualifier_behind_any_glue_is_seen(prefix, qualifier, glue):
    glue = glue[:WINDOW - 1]
    raw = " ".join([*prefix, qualifier, *glue, "2025"])
    assert uncited_time_qualifier(raw, [RawSpan(raw.index("2025"), len(raw), "2025")]) is True


@settings(max_examples=400, deadline=None)
@given(st.sampled_from(I_MONTHS + I_PERIODS + I_TRAILING), GLUE_BEFORE,
       st.lists(st.sampled_from(FILLER), max_size=5), st.sampled_from(["", "en", "para", "hasta", "hacia"]))
def test_property_a_trailing_qualifier_behind_any_glue_is_seen(qualifier, glue, suffix, lead_in):
    walk = [*([lead_in] if lead_in else []), *glue][:WINDOW - 1]
    raw = " ".join(["contratos de 2025", *walk, qualifier, *suffix])
    assert uncited_time_qualifier(raw, [RawSpan(raw.index("2025"), raw.index("2025") + 4, "2025")]) is True


@settings(max_examples=300, deadline=None)
@given(st.lists(st.sampled_from(FILLER), min_size=1, max_size=5),
       st.lists(st.sampled_from(FILLER + ["de", "en", "el"]), min_size=1, max_size=5))
def test_property_without_a_qualifier_on_either_side_the_year_is_plain(before, after):
    raw = " ".join([*before, "de", "2025", *after])
    start = raw.index("2025")
    assert uncited_time_qualifier(raw, [RawSpan(start, start + 4, "2025")]) is False


def test_the_message_still_says_a_whole_year_only():
    from app.core.foundation_messages import user_message

    assert "año completo" in user_message("NEEDS_CLARIFICATION", ASKED).text
    assert AcceptanceStatus.NEEDS_CLARIFICATION.value == "NEEDS_CLARIFICATION"
