"""M1 wave 1 R5 (ZEUS4 P1, blocking): a buyer's own words never become the TOPIC because of a period.

«contratos de aseo con la alcaldía de Barranquilla los últimos dos años» shipped
``AND objeto LIKE '%alcaldia%'`` to SECOP (and «con el municipio de Galapa …» ``%municipio%``/``%galapa%``,
«con la Gobernación del Atlántico durante …» ``%gobernacion%``): the relative window was resolved and masked
BEFORE the actor grammar captured the buyer, so the capture («la alcaldía de barranquilla los ultimos dos
años») could not be found again in the masked residual and the buyer's own words stayed behind as topic.
The base already did this with an explicit year («… en 2025»); a real query in data/feedback.jsonl
(«que ha contratado funsocom con la alcaldia de barranquilla los ultimos dos años?») lost its buyer + NIT route.

The grammar now reads the text WITHOUT the period (``period_free_text.without_period``) and the buyer's span
is found again tolerant to punctuation (``aligned_pattern``).

The property is enumerated, not sampled by hand: buyers x temporal forms x connectors x positions. For each,
the topic carries no word of the buyer and the buyer is the one resolved WITHOUT the period.
"""
from __future__ import annotations

import itertools
import re
import unicodedata
from datetime import date
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.period_free_text import aligned_pattern, without_period
from app.core.query_router import QueryRouter

TODAY = date(2026, 10, 1)

# Buyers of the repo's catalogues: municipal mayoralties (one multi-word), a municipality, governorates
# (with and without article contraction) and frequent national entities that resolve by alias.
BUYERS = [
    ("la alcaldía de Barranquilla", {"barranquilla", "alcaldia"}),
    ("la alcaldía de Galapa", {"galapa", "alcaldia"}),
    ("la alcaldía de Soledad", {"soledad", "alcaldia"}),
    ("la alcaldía de Puerto Colombia", {"puerto", "colombia", "alcaldia"}),
    ("el municipio de Galapa", {"galapa", "municipio"}),
    ("el municipio de Malambo", {"malambo", "municipio"}),
    ("la gobernación del Atlántico", {"atlantico", "gobernacion"}),
    ("la gobernación de Antioquia", {"antioquia", "gobernacion"}),
    ("la gobernación del Valle del Cauca", {"valle", "cauca", "gobernacion"}),
    ("el SENA", {"sena"}),
    ("el ICBF", {"icbf"}),
    ("la Universidad Nacional de Colombia", {"universidad", "nacional", "colombia"}),
]
# Both lexicons (legacy + time_relative), explicit years, month-year, ranges and anchors.
FORMS = [
    "en 2025", "durante 2025", "de enero a marzo de 2025", "en marzo de 2025", "desde enero de 2025",
    "entre 2023 y 2025", "en el primer semestre de 2025", "este año", "el año pasado",
    "los últimos 6 meses", "en los últimos 6 meses", "durante los últimos 2 años", "los últimos dos años",
    "en el último año", "los últimos 30 días", "en las últimas 2 semanas",
]
CONNECTORS = ["con", "de", "para", "en"]
POSITIONS = ["after", "before", "start", "mid", "topic_last"]
TOPICS = ["aseo", "vigilancia", "obra civil", "vías", "alimentación escolar"]


def _fold(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text.lower()) if unicodedata.category(c) != "Mn")


def _tokens(values) -> set[str]:
    out: set[str] = set()
    for value in values if isinstance(values, (list, tuple, set)) else [values]:
        if isinstance(value, (list, tuple, set)):
            out |= _tokens(value)
        elif isinstance(value, str):
            out |= set(re.findall(r"[a-z0-9]+", _fold(value)))
    return out


def _phrase(connector: str, buyer: str) -> str:
    if connector == "de" and buyer.startswith("el "):
        return "del " + buyer[3:]
    return f"{connector} {buyer}"


def _query(position: str, connector: str, buyer: str, period: str, topic: str) -> str:
    who = _phrase(connector, buyer)
    shape = {
        "after": f"contratos de {topic} {who} {period}",
        "before": f"contratos de {topic} {period} {who}",
        "start": f"{period} contratos de {topic} {who}",
        "mid": f"contratos {who} {period} de {topic}",
        "topic_last": f"contratos {who} {period} sobre {topic}",
    }[position]
    return " ".join(shape.split())


@pytest.fixture(scope="module")
def router():
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _parse(router, text):
    return router.parse(text, today=TODAY).params


def _buyer_of(params):
    return (params.get("entidad_resolved"), params.get("departamento_resolved"), params.get("municipio"))


def _cases():
    """Every (form, connector, position) once; the buyer rotates so each buyer meets every form."""
    for index, (form, connector, position) in enumerate(itertools.product(FORMS, CONNECTORS, POSITIONS)):
        buyer, words = BUYERS[index % len(BUYERS)]
        yield form, connector, position, buyer, words, TOPICS[index % len(TOPICS)]


# ── the property ──────────────────────────────────────────────────────────────────────────────────

def test_the_buyer_words_never_become_topic_and_the_buyer_is_the_one_without_the_period(router):
    failures: list[str] = []
    controls: dict[str, tuple] = {}
    for form, connector, position, buyer, words, topic in _cases():
        text = _query(position, connector, buyer, form, topic)
        params = _parse(router, text)
        assert params.get("fecha_desde") or params.get("fecha_hasta") or params.get("_temporal_error"), (
            f"the form is not read as a period (the property would be vacuous): {text!r}"
        )
        topic_words = _tokens([params.get(k) for k in ("objeto", "objeto_or", "objeto_phrases")])
        leaked = (words - _tokens(topic)) & topic_words
        if leaked:
            failures.append(f"LEAK {sorted(leaked)} <- {text!r} -> {params.get('objeto')}")
        control_text = _query(position, connector, buyer, "", topic)
        if control_text not in controls:
            controls[control_text] = _buyer_of(_parse(router, control_text))
        if not params.get("_temporal_error") and _buyer_of(params) != controls[control_text]:
            failures.append(f"BUYER {_buyer_of(params)} != {controls[control_text]} <- {text!r}")
    assert not failures, f"{len(failures)} failures, first 12:\n" + "\n".join(failures[:12])


# ── the reproductions of ZEUS4 ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("text,topic", [
    ("contratos de aseo con la alcaldia de barranquilla los ultimos dos años", ["aseo"]),
    ("contratos de aseo con la alcaldia de barranquilla en 2025", ["aseo"]),        # the base did it too
    ("contratos de aseo con la alcaldia de barranquilla del último año", ["aseo"]),  # the base did it too
    ("que ha contratado funsocom con la alcaldia de barranquilla los ultimos dos años?", ["funsocom"]),
    ("contratos de aseo con el municipio de galapa los ultimos dos años", ["aseo"]),
    ("contratos de aseo con la gobernación del atlántico durante los últimos 2 años", ["aseo"]),
    ("contratos de aseo con la Alcaldía de Soledad los últimos 6 meses", ["aseo"]),
    ("contratos de aseo de la alcaldia de barranquilla los ultimos dos años", ["aseo"]),   # base: 'anos'
])
def test_the_reproductions_carry_only_the_real_topic(router, text, topic):
    params = _parse(router, text)
    assert params.get("objeto") == topic, text
    assert params.get("entidad_resolved") or params.get("departamento_resolved"), "the buyer is resolved"


def test_the_buyer_is_resolved_as_without_the_period(router):
    with_window = _parse(router, "contratos de aseo con la alcaldia de barranquilla los ultimos dos años")
    without = _parse(router, "contratos de aseo con la alcaldia de barranquilla")
    assert with_window["entidad_resolved"] == without["entidad_resolved"]
    assert with_window["entidad"] == without["entidad"], "the captured buyer does not include the period"
    assert (with_window["fecha_desde"], with_window["fecha_hasta"]) == ("2024-10-01", "2026-10-01")


def test_a_period_that_opens_the_sentence_does_not_hide_a_buyer_that_resolves_by_relation(router):
    """«en <ventana> contratos de … con el ICBF»: the leading «en» read as a territory switch."""
    params = _parse(router, "en los últimos 6 meses contratos de alimentación escolar con el ICBF")
    assert params["entidad_resolved"] == "ICBF SEDE NACIONAL" and params["objeto"] == ["alimentacion_escolar"]


def test_punctuation_in_the_buyer_name_does_not_leave_its_words_as_topic(router):
    params = _parse(router, "contratos de vías con Subred Integrada De Servicios De Salud Sur Occidente Ese. en los últimos 6 meses")
    assert params["objeto"] == ["vias"]


# ── the two helpers ───────────────────────────────────────────────────────────────────────────────

def test_without_period_masks_the_span_and_keeps_offsets():
    text = "contratos con la alcaldia de x los ultimos dos anos"
    start = text.index("los ultimos")
    out = without_period(text, (start, len(text)))
    assert len(out) == len(text) and out[:start] == text[:start] and out[start:].strip() == ""
    assert without_period(text, None) is text


def test_without_period_also_masks_the_leading_en_and_nothing_else():
    text = "en los ultimos 6 meses contratos de aseo con el icbf"
    start = text.index("los ultimos")
    out = without_period(text, (start, start + len("los ultimos 6 meses")))
    assert out.strip() == "contratos de aseo con el icbf"
    mid = "contratos de aseo en los ultimos 6 meses"
    mid_start = mid.index("los ultimos")
    assert without_period(mid, (mid_start, len(mid))).startswith("contratos de aseo en "), "only a leading en"
    durante = "durante los ultimos 6 meses contratos"
    assert without_period(durante, (durante.index("los"), durante.index("contratos") - 1)).startswith("durante")


def test_aligned_pattern_tolerates_separators_but_not_other_words():
    pattern = re.compile(aligned_pattern("con subred sur occidente ese en"), re.IGNORECASE)
    assert pattern.search("contratos con subred sur occidente ese. en los ultimos")
    assert pattern.search("contratos con  subred sur occidente, ese en")
    assert not pattern.search("contratos contra subred sur occidente ese en"), "'con' is not a prefix of 'contra'"
    assert not pattern.search("contratos con subred norte occidente ese en")
    assert not pattern.search("contratos con subredsur occidente ese en"), "tokens stay separate words"
    assert not pattern.search("contratos xcon subred sur occidente ese en"), "the span starts at a word boundary"
    assert not pattern.search("contratos con subred sur occidente esen"), "and ends at one"
