"""Tests for dialogue_policy.classify — all 7 signal kinds."""

from datetime import date

import pytest

from app.core.dialogue_policy import DialogueSignal, classify


# ═══════════════════════════════════════════════════════════════════════════
# R1 — command_incomplete
# ═══════════════════════════════════════════════════════════════════════════

def test_command_incomplete_departamento():
    signals = classify("filtra por departamento", {}, None)
    assert any(
        s.kind == "command_incomplete" and s.payload["slot"] == "departamento"
        for s in signals
    )


def test_command_incomplete_ciudad():
    signals = classify("filtra por ciudad", {}, None)
    assert any(
        s.kind == "command_incomplete" and s.payload["slot"] == "ciudad"
        for s in signals
    )


def test_command_incomplete_valor():
    signals = classify("filtra por valor", {}, None)
    assert any(s.payload["slot"] == "valor" for s in signals)


def test_command_complete_does_not_trigger():
    signals = classify("filtra por departamento huila", {}, None)
    assert not any(s.kind == "command_incomplete" for s in signals)


def test_ordena_por_incomplete():
    signals = classify("ordena por", {}, None)
    assert any(s.kind == "command_incomplete" for s in signals)


# ═══════════════════════════════════════════════════════════════════════════
# R2 — implicit_opportunity
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("query", [
    "algún proceso para mantenimiento",
    "alguna oferta del sena",
    "hay algún proceso de aseo",
    "dónde me puedo presentar como proveedor de carpas",
    "dónde podría ofertar para transporte",
])
def test_implicit_opportunity_triggers(query):
    signals = classify(query, {}, None)
    assert any(s.kind == "implicit_opportunity" for s in signals)


def test_implicit_opportunity_suppressed_by_historical():
    signals = classify("procesos del año pasado", {}, None)
    assert not any(s.kind == "implicit_opportunity" for s in signals)


# ═══════════════════════════════════════════════════════════════════════════
# R3 — discourse_filler + non_query_followup
# ═══════════════════════════════════════════════════════════════════════════

def test_discourse_filler_courtesy():
    signals = classify(
        "hola, estoy interesado en vender carpas", {}, None
    )
    filler = [s for s in signals if s.kind == "discourse_filler"]
    assert filler
    assert "hola" in filler[0].payload["tokens_to_scrub"]


def test_discourse_filler_saludo():
    signals = classify("buenas tardes procesos de aseo", {}, None)
    assert any(s.kind == "discourse_filler" for s in signals)


def test_non_query_followup_after_complaint():
    prev = {"objeto": ["mantenimiento"], "departamento_resolved": "Bolívar"}
    signals = classify(
        "nada de eso sirve, no funcionó",
        {},
        {"prev_params": prev, "turn_index": 2},
    )
    assert any(s.kind == "non_query_followup" for s in signals)


def test_no_non_query_followup_without_prev_params():
    signals = classify("nada de eso sirve", {}, None)
    # Without prev_params, no turn context → no non_query_followup
    assert not any(s.kind == "non_query_followup" for s in signals)


# ═══════════════════════════════════════════════════════════════════════════
# R4 — temporal_month
# ═══════════════════════════════════════════════════════════════════════════

def test_temporal_month_en_mayo():
    signals = classify(
        "aseo en putumayo en mayo", {},
        {"today": date(2026, 5, 21)},
    )
    month_sig = [s for s in signals if s.kind == "temporal_month"]
    assert month_sig
    assert month_sig[0].payload["month"] == 5
    assert month_sig[0].payload["year"] == 2026


def test_temporal_month_este_mes():
    signals = classify(
        "contratos este mes", {},
        {"today": date(2026, 5, 21)},
    )
    sig = [s for s in signals if s.kind == "temporal_month"]
    assert sig
    assert sig[0].payload["month"] == 5
    assert sig[0].payload["year"] == 2026


def test_temporal_month_mes_pasado():
    signals = classify(
        "aseo el mes pasado", {},
        {"today": date(2026, 5, 21)},
    )
    sig = [s for s in signals if s.kind == "temporal_month"]
    assert sig
    assert sig[0].payload["month"] == 4
    assert sig[0].payload["year"] == 2026


def test_temporal_month_hace_meses():
    signals = classify(
        "contratos hace 3 meses", {},
        {"today": date(2026, 5, 21)},
    )
    sig = [s for s in signals if s.kind == "temporal_month"]
    assert sig
    assert sig[0].payload["month"] == 2


def test_no_temporal_for_explicit_year():
    signals = classify("transporte 2025 bolívar", {}, None)
    assert not any(s.kind == "temporal_month" for s in signals)


# ═══════════════════════════════════════════════════════════════════════════
# R5 — number_phrase_scrub
# ═══════════════════════════════════════════════════════════════════════════

def test_number_phrase_scrub_when_value_extracted():
    params = {
        "valor_max": 25_000_000,
        "objeto": ["transporte", "escolar", "supere", "los"],
    }
    signals = classify(
        "transporte escolar no supere los 25 millones", params, None
    )
    scrub = [s for s in signals if s.kind == "number_phrase_scrub"]
    assert scrub
    assert "supere" in scrub[0].payload["tokens_to_scrub"]


def test_number_phrase_no_scrub_when_no_value():
    params = {"objeto": ["transporte", "escolar"]}
    signals = classify("transporte escolar", params, None)
    assert not any(s.kind == "number_phrase_scrub" for s in signals)


# ═══════════════════════════════════════════════════════════════════════════
# R6 — city_mention_as_topic
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("query,token,city", [
    ("diseño ipiales", "ipiales", "Ipiales"),
    ("manejo de residuos sólidos olaya", "olaya", "Olaya"),
    ("silvopastoriles yarumal", "yarumal", "Yarumal"),
])
def test_city_mention_as_topic(query, token, city):
    params = {"objeto": query.lower().split()}
    signals = classify(query, params, None)
    geo = [s for s in signals if s.kind == "city_mention_as_topic"]
    assert geo, f"Expected city signal for {query!r}"
    assert geo[0].payload["city"] == city


def test_city_mention_suppressed_when_ciudad_already_set():
    params = {"objeto": ["diseño", "ipiales"], "ciudad": "Ipiales"}
    signals = classify("diseño ipiales", params, None)
    assert not any(s.kind == "city_mention_as_topic" for s in signals)


def test_city_mention_suppressed_when_departamento_resolved():
    params = {
        "objeto": ["diseño", "ipiales"],
        "departamento_resolved": "Nariño",
    }
    signals = classify("diseño ipiales", params, None)
    assert not any(s.kind == "city_mention_as_topic" for s in signals)


# ═══════════════════════════════════════════════════════════════════════════
# Regression: clean queries must NOT emit blocking signals
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("query", [
    "contratos de mantenimiento en bogotá",
    "procesos abiertos de alimentación en atlántico",
    "consultoría cartagena",
])
def test_clean_queries_emit_no_blocking_signals(query):
    signals = classify(query, {"objeto": query.split()}, None)
    blocking_kinds = {"command_incomplete", "non_query_followup"}
    assert not any(
        s.kind in blocking_kinds for s in signals
    ), f"Blocking signal emitted for clean query: {query!r}"


# ═══════════════════════════════════════════════════════════════════════════
# Edge cases
# ═══════════════════════════════════════════════════════════════════════════

def test_empty_query_returns_empty():
    signals = classify("", {}, None)
    assert signals == []


def test_classify_is_stateless():
    s1 = classify("diseño ipiales", {"objeto": ["diseño", "ipiales"]}, None)
    s2 = classify("diseño ipiales", {"objeto": ["diseño", "ipiales"]}, None)
    assert len(s1) == len(s2)


def test_classify_does_not_mutate_params():
    params = {"objeto": ["diseño", "ipiales"]}
    orig = dict(params, objeto=list(params["objeto"]))
    classify("diseño ipiales", params, None)
    assert params == orig
