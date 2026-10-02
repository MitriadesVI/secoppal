"""M1-H0: a year the raw text qualifies is asked, never executed as the whole calendar year.

The producer cites only the year (prompt rule Y1), so «agosto de 2026» arrives as «2026».
The lexicons below are written out independently of the module: emptying the module's
lexicon must fail here instead of passing over an empty set.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from app.core.entity_resolver import EntityResolver
from app.core.foundation_admission import admit_proposal
from app.core.foundation_messages import user_message
from app.core.foundation_producers import decode_proposal
from app.core.foundation_time_coverage import (
    LEADING_LEXICON, TRAILING_LEXICON, WINDOW, qualifies_year, uncited_time_qualifier,
)
from app.core.interpretation_contract import AcceptanceStatus, RawSpan

RESOLVER = EntityResolver("app/data/aliases_db.json")
ASKED = "UNRESOLVED_HOST_DIMENSION:TIME"

MONTHS = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre",
          "setiembre", "octubre", "noviembre", "diciembre",
          "ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "sept", "oct", "nov", "dic"]
PERIODS = ["semestre", "trimestre", "bimestre", "cuatrimestre", "quincena", "semana", "mes", "mitad",
           "inicio", "inicios", "comienzo", "comienzos", "principio", "principios", "mediados", "final",
           "finales", "fines"]
LEADING = ["desde", "hasta", "entre", "antes de", "antes del", "después de", "después del", "tras",
           "a partir de", "a partir del", "previo a", "previo al", "posterior a", "posterior al",
           "anterior a", "anterior al"]
TRAILING = ["en adelante", "hacia adelante", "para adelante", "hacia atrás", "para atrás", "y posteriores",
            "o posteriores", "y anteriores", "o anteriores", "y siguientes", "a la fecha", "hasta hoy",
            "hasta la fecha", "a hoy"]


def span(raw, text, occurrence=1):
    at = -1
    for _ in range(occurrence):
        at = raw.index(text, at + 1)
    return {"start": at, "end": at + len(text), "quote": text}


def admit(raw, cited="2025", occurrence=1, operator="during_year"):
    payload = {
        "requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": "contracts",
                      "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                      "universe_evidence": span(raw, "contratos"),
                      "condition_expression": {"operator": "ATOM", "condition_id": "c0"}}],
        "conditions": [{"condition_id": "c0", "request_ids": ["r1"], "dimension": "TIME", "operator": operator,
                        "polarity": "POSITIVE", "evidence": [span(raw, cited, occurrence)]}],
    }
    return admit_proposal(decode_proposal(payload, raw, arm="C"), raw, entity_resolver=RESOLVER)


def assert_asked(accepted):
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    assert accepted.reason == ASKED


def assert_year(accepted, year="2025"):
    assert accepted.status is AcceptanceStatus.READY
    assert [r.resolved_ids for r in accepted.resolutions if r.target_id == "c0"] == [(year,)]


# ── the lexicon is the closed list of the mission, not less ───────────────────────────────────────

def test_the_module_lexicon_covers_the_independent_lists():
    from app.utils.money import normalize_text

    leading = {word for marker in LEADING for word in normalize_text(marker).split()}
    assert {normalize_text(w) for w in MONTHS + PERIODS} | {w for w in leading if w not in {"de", "del", "a", "al"}} \
        <= set(LEADING_LEXICON)
    trailing = {word for marker in TRAILING for word in normalize_text(marker).split()}
    assert {normalize_text(w) for w in MONTHS + PERIODS} <= set(TRAILING_LEXICON)
    assert {"adelante", "atras", "posteriores", "anteriores", "siguientes", "fecha", "hoy"} <= set(TRAILING_LEXICON)
    assert trailing >= {"adelante"}


# ── exhaustive: every month, period and bound x every written form ────────────────────────────────

def _forms_before(marker):
    return [f"contratos {marker} 2025", f"contratos {marker} de 2025", f"contratos {marker} del 2025",
            f"contratos {marker} el 2025", f"contratos {marker} el año 2025", f"contratos de {marker} del 2025"]


@pytest.mark.parametrize("word", MONTHS + [w.capitalize() for w in MONTHS] + [w.upper() for w in MONTHS])
def test_every_month_form_before_the_year_is_asked(word):
    for raw in (*_forms_before(word), f"contratos del 15 de {word} de 2025", f"contratos de {word}. 2025"):
        assert_asked(admit(raw))


@pytest.mark.parametrize("word", MONTHS)
def test_a_month_cited_together_with_its_year_is_still_asked(word):
    raw = f"contratos de {word} de 2025"
    assert_asked(admit(raw, cited=f"{word} de 2025"))


@pytest.mark.parametrize("word", PERIODS)
def test_every_partial_period_word_is_asked(word):
    for raw in (f"contratos del primer {word} de 2025", f"contratos durante el último {word} del 2025",
                f"contratos a {word} de 2025", f"contratos {word} 2025"):
        assert_asked(admit(raw))


@pytest.mark.parametrize("marker", LEADING + [m.upper() for m in LEADING])
def test_every_leading_bound_is_asked(marker):
    for raw in _forms_before(marker):
        assert_asked(admit(raw))


@pytest.mark.parametrize("marker", TRAILING)
def test_every_trailing_bound_is_asked(marker):
    assert_asked(admit(f"contratos de 2025 {marker}"))


@pytest.mark.parametrize("raw,cited,occurrence", [
    ("contratos entre enero y junio de 2024", "2024", 1),
    ("contratos de agosto de 2026", "2026", 1),
    ("contratos de septiembre del 2025", "2025", 1),
    ("contratos entre 2023 y 2025", "2025", 1),
    ("contratos entre 2023 y 2025", "2023", 1),
    ("contratos de 2023 y 2025", "2025", 1),
    ("contratos de 2024 o 2025", "2025", 1),
    ("contratos de 2025-2026", "2025", 1),
    ("contratos de 2025/2026", "2026", 1),
    ("contratos de 2025 a 2026", "2025", 1),
    ("contratos de 2025 hasta 2026", "2025", 1),
    ("contratos desde 2023", "2023", 1),
    ("contratos antes de 2024", "2024", 1),
    ("contratos después de 2022", "2022", 1),
    ("contratos a partir de 2023", "2023", 1),
    ("contratos hasta 2024", "2024", 1),
    ("contratos desde el año 2023", "2023", 1),
    ("contratos a finales de 2024", "2024", 1),
])
def test_the_seven_classes_of_the_audit_and_their_neighbours_are_asked(raw, cited, occurrence):
    assert_asked(admit(raw, cited, occurrence))


# ── a year that stands alone still executes (the fix must not over-ask) ──────────────────────────

@pytest.mark.parametrize("raw", [
    "contratos en 2025", "contratos del 2025", "contratos de 2025", "contratos año 2025",
    "contratos del año 2025", "contratos vigencia 2025", "contratos de la vigencia 2025", "contratos 2025",
    "2025 contratos", "contratos firmados en 2025", "contratos de obra del 2025",
    "contratos de más de 500 millones en 2025", "contratos hasta 500 millones en 2025",
    "contratos de 2025 hasta 500 millones", "contratos 2025 mayores a 2000 millones",
    "contratos de 2025 y de más de 300 millones", "contratos de 2025 de aseo",
])
def test_a_year_that_stands_alone_is_executed(raw):
    assert_year(admit(raw))


def test_numbers_that_are_not_years_are_not_qualifiers():
    for raw in ("contratos de 3025 en 2025", "contratos de 12025 pesos del 2025", "contratos de 1899 pesos en 2025"):
        assert_year(admit(raw, "2025", raw.count("2025")))


def test_only_the_cited_year_decides_not_a_qualifier_of_another_clause():
    raw = "contratos de 2025 con fecha de corte mayo"
    # «mayo» is farther than the glue chain from the year: it does not qualify it.
    assert_year(admit(raw))


# ── window boundary: WINDOW tokens including the marker ──────────────────────────────────────────

def _qualifies(raw):
    at = raw.index("2025")
    return qualifies_year(raw, at, at + 4)


def test_the_window_counts_the_marker_and_stops_after_it():
    assert WINDOW == 4
    assert _qualifies("desde la los 2025") is True          # marker is the 3rd token back
    assert _qualifies("desde el la los 2025") is True       # marker is the 4th token back
    assert _qualifies("desde el la los las 2025") is False  # marker is the 5th: out of window


def test_a_non_glue_word_stops_the_walk():
    assert _qualifies("desde Bogotá en 2025") is False


# ── properties ────────────────────────────────────────────────────────────────────────────────────

FILLER = ["contratos", "de", "obra", "aseo", "ICBF", "Soledad", "500", "millones", "para", "con", "en", "que"]
QUALIFIERS = MONTHS + PERIODS + ["desde", "hasta", "entre", "antes", "despues", "tras", "partir", "previo"]


@settings(max_examples=300, deadline=None)
@given(st.lists(st.sampled_from(FILLER), max_size=6), st.sampled_from(QUALIFIERS),
       st.lists(st.sampled_from(["de", "del", "el", "el año", "la"]), max_size=1))  # <= WINDOW-1 glue tokens
def test_a_qualifier_right_before_the_year_is_always_seen_whatever_precedes(prefix, qualifier, glue):
    raw = " ".join([*prefix, qualifier, *glue, "2025"])
    assert uncited_time_qualifier(raw, [RawSpan(raw.index("2025"), len(raw), "2025")]) is True


@settings(max_examples=300, deadline=None)
@given(st.lists(st.sampled_from([w for w in FILLER if w not in {"de"}]), min_size=1, max_size=6),
       st.sampled_from(["", "de", "del", "en", "el año", "vigencia"]))
def test_without_a_qualifier_the_year_is_plain_whatever_surrounds_it(words, glue):
    raw = " ".join([*words, glue, "2025"]).replace("  ", " ")
    assert uncited_time_qualifier(raw, [RawSpan(raw.index("2025"), len(raw), "2025")]) is False


# ── scope: only during_year consults it; the message tells the truth ──────────────────────────────

def test_other_time_operators_remain_unresolved_as_before():
    # IR v2 closes the operator table (C0 §5.1.3): «before» is no longer a TIME operator the contract admits
    # (OPERATOR_OUTSIDE_DIMENSION, see test_foundation_ir_v2); the table's other operators stay unresolved here.
    assert_asked(admit("contratos antes de 2025", operator="between_dates"))


def test_the_user_is_told_it_is_a_whole_year_only_and_no_source_call_is_made(tmp_path, monkeypatch):
    from app.core.foundation_projection import SOURCE_SELECT_FIELDS
    from app.core.soql_builder import SoQLBuilder
    from datetime import datetime
    from tests.product_contract.harness import ProductHarness

    raw = "contratos de agosto de 2026"
    now = datetime.fromisoformat("2026-09-26T12:00:00-05:00")
    payload = {
        "requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": "contracts",
                      "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                      "universe_evidence": span(raw, "contratos"),
                      "condition_expression": {"operator": "ATOM", "condition_id": "c0"}}],
        "conditions": [{"condition_id": "c0", "request_ids": ["r1"], "dimension": "TIME",
                        "operator": "during_year", "polarity": "POSITIVE", "evidence": [span(raw, "2026")]}],
    }
    produced = decode_proposal(payload, raw, arm="C")
    rows = {dataset: [dict.fromkeys({*SoQLBuilder.SPECS[dataset].select_fields, *SOURCE_SELECT_FIELDS[dataset]})]
            for dataset in (SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET)}
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows, reference_instant=now, today=now.date(),
                             foundation_producer=lambda _: produced,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    turn = harness.say(raw)
    assert turn.result["foundation_terminal"] == "NEEDS_CLARIFICATION"
    assert turn.result["foundation_internal_reason"] == ASKED
    assert harness.transport.snapshot() == []
    assert "año completo" in user_message("NEEDS_CLARIFICATION", ASKED).text
