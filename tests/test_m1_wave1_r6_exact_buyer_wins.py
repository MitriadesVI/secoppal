"""M1 wave 1 R6 (ZEUS5 NO_GO on R5): the exact buyer wins over the period; a capture is trimmed mention by mention.

R5 masked the period in the INPUT of the buyer capture. Three defects followed:

* P1-1: the span of «relative window + another period» ENVELOPES everything written between them, buyer
  included; masking it erased the buyer («contratos de aseo de 2025 con la Alcaldía de Soledad de los últimos
  3 meses» → «de octubre a diciembre de 2025» ran WITHOUT Soledad, silently).
* P1-2: the same envelope was masked in the residual text (R4), so «… de los últimos 3 meses de la Alcaldía de
  Barranquilla de 2025» lost Barranquilla.
* The declared group ICBF (33 entities) became ICBF SEDE NACIONAL, and «Hospital 7 de Agosto» became another
  hospital: masking the temporal words changed buyers whose own capture resolved exactly.

Precedence (``period_free_text``): the capture is resolved as written; if it resolves it stays, temporal words
included. Only when it does not resolve are the temporal mentions the authority recognised trimmed off it ONE BY
ONE (borders first, never the stretch that joins two of them), resolving again after each cut. The words of an
accredited buyer never become the topic.
"""
from __future__ import annotations

import re
import unicodedata
from datetime import date
from pathlib import Path

import pytest

from app.config import Settings, get_settings
from app.core import period_free_text as pft
from app.core.actor_conversation import ActorConversationInstruction
from app.core.entity_resolver import EntityResolver
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from app.core.temporal_semantics import resolve_temporal_scope

TODAY = date(2026, 10, 1)


@pytest.fixture(scope="module")
def router():
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _parse(router, text):
    return router.parse(text, today=TODAY).params


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


def _buyer(params):
    return (
        params.get("entidad_resolved"), params.get("departamento_resolved"), params.get("municipio"),
        (params.get("buyer_group") or {}).get("id"),
    )


def _topic(params) -> set[str]:
    return _tokens([params.get(key) for key in ("objeto", "objeto_or", "objeto_phrases")])


# ── the helper: precedence and one-by-one trimming (pure) ─────────────────────────────────────────

def _instruction(capture: str) -> ActorConversationInstruction:
    return ActorConversationInstruction(operation="SET_ACTOR", buyer_mention=capture, construction="contracts_of_x_with_y")


def _run(capture: str, mentions: tuple[str, ...], accepts: set[str]):
    calls: list[str] = []

    def resolves(form: str) -> bool:
        calls.append(form)
        return form in accepts

    return pft.exact_buyer_first(_instruction(capture), mentions, resolves), calls


def test_a_capture_that_resolves_as_written_is_never_trimmed():
    """«Hospital 7 de Agosto»: the temporal words are part of the buyer; one resolution, as written."""
    capture = "la ese hospital 7 de agosto"
    result, calls = _run(capture, ("7 de agosto",), {capture, "la ese hospital"})
    assert result.buyer_mention == capture and calls == [capture]


def test_the_exact_capture_wins_even_when_a_trimmed_form_would_resolve_too():
    capture = "la alcaldia de soledad en 2025"
    result, calls = _run(capture, ("en 2025",), {capture, "la alcaldia de soledad"})
    assert result.buyer_mention == capture and calls == [capture]


def test_a_capture_that_does_not_resolve_is_trimmed_and_resolved_again():
    result, calls = _run(
        "la alcaldia de barranquilla los ultimos dos anos", ("los ultimos dos anos",), {"la alcaldia de barranquilla"},
    )
    assert result.buyer_mention == "la alcaldia de barranquilla"
    assert calls == ["la alcaldia de barranquilla los ultimos dos anos", "la alcaldia de barranquilla"]


def test_mentions_are_cut_one_by_one_borders_first_resolving_after_each_cut():
    capture = "la alcaldia de soledad de 2025 de los ultimos 3 meses"
    mentions = ("los ultimos 3 meses", "de 2025")
    # Only the form with BOTH cut resolves: the window (right border) goes first, then «de 2025».
    result, calls = _run(capture, mentions, {"la alcaldia de soledad"})
    assert result.buyer_mention == "la alcaldia de soledad"
    assert calls[0] == capture
    assert calls[1] == "la alcaldia de soledad de 2025 de", "one mention cut: the window, from the right border"
    assert "la alcaldia de soledad de 2025" in calls and calls[-1] == "la alcaldia de soledad"
    # If the form with only the window cut resolves, the anchor is never touched.
    only_window, calls = _run(capture, mentions, {"la alcaldia de soledad de 2025"})
    assert only_window.buyer_mention == "la alcaldia de soledad de 2025"
    assert not any(form == "la alcaldia de soledad" for form in calls)


def test_a_border_mention_is_cut_before_a_longer_interior_one():
    capture = "alfa los ultimos 3 meses beta gamma en 2025"
    forms = list(pft.trimmed_forms(capture, ("los ultimos 3 meses", "en 2025")))
    assert forms[0] == "alfa los ultimos 3 meses beta gamma", "the right border goes first, though shorter"
    assert forms[-1] == "alfa beta gamma"


def test_the_stretch_between_two_mentions_is_never_cut():
    """The authority's span envelopes both mentions and what lies between them; no form drops that stretch."""
    capture = "alfa en 2025 beta gamma los ultimos 3 meses"
    forms = list(pft.trimmed_forms(capture, ("los ultimos 3 meses", "en 2025")))
    assert forms, "mentions are cut"
    assert all("beta gamma" in form and form.startswith("alfa") for form in forms), forms
    assert "alfa beta gamma" in forms, "the interior mention is cut on its own, after the border one"


def test_glue_left_at_the_new_edge_is_dropped_after_the_plain_cut():
    forms = list(pft.trimmed_forms("la gobernacion del atlantico durante los ultimos 2 anos", ("los ultimos 2 anos",)))
    assert forms == ["la gobernacion del atlantico durante", "la gobernacion del atlantico"]


def test_a_capture_with_no_temporal_mention_is_not_even_resolved_twice():
    result, calls = _run("la alcaldia de soledad", ("los ultimos 2 anos",), {"x"})
    assert result.buyer_mention == "la alcaldia de soledad" and calls == []
    result, calls = _run("la alcaldia de soledad", (), {"x"})
    assert calls == []


def test_an_unresolvable_capture_is_returned_untouched():
    capture = "xyz los ultimos 2 anos"
    result, calls = _run(capture, ("los ultimos 2 anos",), set())
    assert result.buyer_mention == capture and calls[0] == capture


def test_mention_texts_and_without_mentions():
    text = "contratos de aseo de 2025 con la alcaldia de soledad de los ultimos 3 meses"
    spans = ((text.index("de 2025"), text.index("de 2025") + len("de 2025")), (text.index("los ultimos"), len(text)))
    mentions = pft.mention_texts(text, spans)
    assert set(mentions) == {"de 2025", "los ultimos 3 meses"} and mentions[0] == "los ultimos 3 meses", "longest first"
    assert pft.without_mentions("con la alcaldia de soledad de los ultimos 3 meses", mentions) == "con la alcaldia de soledad de"
    assert pft.without_mentions("con la ese hospital 7 de agosto", ("7 de agosto",)) == "con la ese hospital"
    assert pft.mention_texts(text, ()) == ()


def test_a_window_next_to_another_period_is_masked_with_its_introducer_and_only_a_whole_word():
    text = "contratos de aseo durante los ultimos 2 anos de la alcaldia de el reten de 2025"
    window = (text.index("los ultimos"), text.index("los ultimos") + len("los ultimos 2 anos"))
    year = (text.index("de 2025"), len(text))
    spans = pft.consumed(text, (window, year))
    assert text[spans[0][0]:spans[0][1]] == "durante los ultimos 2 anos", "the introducer goes with the window"
    assert spans[1] == year, "«en» inside «reten» is not an introducer"
    assert pft.consumed(text, (window,)) == (window,), "a single period keeps its own span"
    joined = "alcaldia de galapa de 2025, los ultimos 3 meses"
    anchor, win = (joined.index("de 2025"), joined.index("de 2025") + 7), (joined.index("los"), len(joined))
    assert pft.consumed(joined, (anchor, win)) == ((anchor[0], len(joined)),), "punctuation alone between them goes too"


def test_a_period_that_opens_the_sentence_is_masked_with_its_introducer_and_nothing_else():
    text = "en el ultimo ano con quien ha contratado la alcaldia de x"
    spans = ((6, 16),)
    masked = pft.without_leading_period(text, spans)
    assert len(masked) == len(text) and masked.strip() == "con quien ha contratado la alcaldia de x"
    mid = "contratos de aseo en 2025 con la alcaldia de x"
    assert pft.without_leading_period(mid, ((mid.index("2025"), mid.index("2025") + 4),)) == mid, "not leading"
    word = "contratos en 2025"
    assert pft.without_leading_period(word, ((12, 17),)) == word, "a real word before the period: not leading"
    assert pft.without_leading_period(text, ()) == text


def _reading():
    calls: list[str] = []

    def interpret(text: str) -> ActorConversationInstruction:
        calls.append(text)
        # The grammar loses the capture while the period opens the sentence, finds it once it is masked.
        return _instruction("la alcaldia de x") if text.lstrip().startswith("con ") else ActorConversationInstruction()

    return interpret, calls


def _read(resolves: bool, claimed: bool, text="en 2025 con la alcaldia de x", spans=((0, 7),)):
    interpret, calls = _reading()
    result = pft.read_buyer(text, spans, ("en 2025",), interpret, lambda _m: resolves, lambda _m: claimed)
    return result, calls


def test_a_lost_capture_is_recovered_from_the_text_without_the_leading_period():
    result, calls = _read(resolves=True, claimed=False)
    assert result.buyer_mention == "la alcaldia de x" and len(calls) == 2


def test_a_declared_group_keeps_its_own_path_and_an_unresolved_capture_is_not_adopted():
    assert _read(resolves=True, claimed=True)[0].buyer_mention is None, "the group path (base) is kept"
    assert _read(resolves=False, claimed=False)[0].buyer_mention is None, "only an exact capture is adopted"


def test_a_sentence_that_does_not_open_with_a_period_is_read_once():
    result, calls = _read(resolves=True, claimed=False, text="contratos en 2025 con la alcaldia de x", spans=((10, 17),))
    assert result.buyer_mention is None and len(calls) == 1


# ── the authority hands in each mention, not the envelope ─────────────────────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("contratos de aseo de 2025 con la alcaldia de soledad de los ultimos 3 meses", ["de 2025", "los ultimos 3 meses"]),
    ("contratos de aseo de los ultimos 3 meses de la alcaldia de barranquilla de 2025", ["los ultimos 3 meses", "de 2025"]),
    ("contratos de aseo de 2025 de la alcaldia de galapa en los ultimos 3 meses", ["de 2025", "en los ultimos 3 meses"]),
    ("contratos de aseo de la vigencia anterior con la alcaldia de soledad en los ultimos 6 meses",
     ["de la vigencia anterior", "los ultimos 6 meses"]),
])
def test_an_anchored_window_reports_its_two_mentions_and_not_what_lies_between(text, expected):
    found = resolve_temporal_scope(text, today=TODAY)
    assert found.error_code == "relative_window_explicit_anchor"
    covered = [text[start:end].strip() for start, end in found.mentions]
    assert len(covered) == 2
    assert all(any(item in got or got in item for got in covered) for item in expected), (covered, expected)
    start, end = found.span
    assert "alcaldia" in text[start:end], "the envelope swallows the buyer: the test is not vacuous"
    assert not any("alcaldia" in got for got in covered), "no mention contains the buyer"
    assert all(found.span[0] <= a and b <= found.span[1] for a, b in found.mentions)


def test_a_single_period_has_one_mention_equal_to_its_span():
    found = resolve_temporal_scope("contratos de aseo con la alcaldia de soledad en 2025", today=TODAY)
    assert found.error_code is None and found.mentions == (found.span,)
    assert resolve_temporal_scope("contratos de aseo", today=TODAY).mentions == ()


@pytest.mark.parametrize("text,entity", [
    ("contratos de aseo con la contraloria general los ultimos dos años", "CGR-NIVEL CENTRAL"),
    ("contratos de aseo con la defensoria en 2025", "DEFENSORÍA DEL PUEBLO"),
])
def test_a_capture_that_resolves_only_without_the_period_is_trimmed(router, text, entity):
    """The whole capture («la contraloria general los ultimos dos anos») is not an exact buyer: the base lost it
    and made «contraloria general anos» the topic. The mention is trimmed off and the buyer resolves."""
    params = _parse(router, text)
    assert params["entidad_resolved"] == entity and params["objeto"] == ["aseo"], params


@pytest.mark.parametrize("text,control", [
    ("en marzo de 2025 con quien ha contratado la alcaldía de Mosquera", "con quien ha contratado la alcaldía de Mosquera"),
    ("el año pasado con quien ha contratado la alcaldía de Palmar De Varela", "con quien ha contratado la alcaldía de Palmar De Varela"),
    ("en el último año con quien ha contratado la alcaldía de Arauca", "con quien ha contratado la alcaldía de Arauca"),
    ("los últimos 30 días con quien ha contratado la alcaldía de Arauca", "con quien ha contratado la alcaldía de Arauca"),
    ("en 2025 contratos de aseo con la alcaldía de Soledad", "contratos de aseo con la alcaldía de Soledad"),
])
def test_a_period_that_opens_the_sentence_does_not_hide_the_buyer(router, text, control):
    params = _parse(router, text)
    assert _buyer(params) == _buyer(_parse(router, control)) and params.get("entidad_resolved"), text
    assert "quien" not in _topic(params), "the question word is not a topic once the buyer is read"


# ── P1-1 / P1-2: the buyer survives a window next to another period (the T0 frame the answer merges into) ─

@pytest.mark.parametrize("text,buyer", [
    ("contratos de aseo de 2025 con la Alcaldía de Soledad de los últimos 3 meses", "MUNICIPIO DE SOLEDAD"),
    ("contratos de aseo con la Alcaldía de Soledad de 2025 de los últimos 3 meses", "MUNICIPIO DE SOLEDAD"),
    ("contratos de aseo de los últimos 3 meses con la Alcaldía de Soledad en 2025", "MUNICIPIO DE SOLEDAD"),
    ("contratos de más de 100 millones de 2025 con la Alcaldía de Soledad de los últimos 3 meses", "MUNICIPIO DE SOLEDAD"),
    ("contratos de aseo de los últimos 3 meses de la Alcaldía de Barranquilla de 2025",
     "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"),
    ("contratos de aseo de 2025 de la alcaldía de Galapa en los últimos 3 meses", "MUNICIPIO DE GALAPA."),
    ("contratos de la Alcaldía de Galapa en los últimos 3 meses de 2025", "MUNICIPIO DE GALAPA."),
    ("contratos de aseo con la Alcaldía de Soledad de 2025 durante los últimos 2 años", "MUNICIPIO DE SOLEDAD"),
    ("¿Cuántos contratos firmó la Alcaldía de Galapa de 2025, los últimos 3 meses?", "MUNICIPIO DE GALAPA."),
])
def test_the_buyer_between_two_mentions_is_kept_in_the_blocked_frame(router, text, buyer):
    params = _parse(router, text)
    assert params.get("_temporal_error") == "relative_window_explicit_anchor"
    assert params.get("entidad_resolved") == buyer, text
    assert not (_tokens(params.get("objeto")) & {"alcaldia", "soledad", "barranquilla", "galapa", "durante"}), params.get("objeto")


@pytest.mark.parametrize("text,buyer_group", [
    ("contratos de vigilancia de 2025 con el ICBF de los últimos 6 meses", None),
    ("contratos de mantenimiento vial de 2024 con el INVIAS de los últimos 6 meses", None),
])
def test_a_frequent_entity_between_two_mentions_keeps_its_buyer(router, text, buyer_group):
    params = _parse(router, text)
    control = _parse(router, re.sub(r" de 20\d\d| de los últimos 6 meses", "", text))
    assert params.get("_temporal_error") and _buyer(params) == _buyer(control) and _buyer(params) != (None,) * 4


# ── the declared groups and the buyers with temporal words are the base's, not R5's ───────────────

@pytest.mark.parametrize("text", [
    "en 2025 con quien ha contratado el ICBF",
    "desde enero de 2024 con quien ha contratado el ICBF",
    "en los últimos dos años con quien ha contratado el ICBF",
    "en los últimos 6 meses contratos de aseo con el ICBF",
    "contratos de la icbf en 2026",
])
def test_the_declared_group_icbf_is_not_narrowed_to_its_headquarters(router, text):
    params = _parse(router, text)
    assert (params.get("buyer_group") or {}).get("id") == "icbf_national", text
    assert not params.get("entidad_resolved")


@pytest.mark.parametrize("text", [
    "contratos de aseo con la ESE Hospital 7 de Agosto",
    "con quien ha contratado el Hospital 7 de Agosto",
    "con quien ha contratado el Hospital 7 de Agosto en 2025",
    "contratos de aseo con el Hospital 7 de Agosto los últimos dos años",
])
def test_a_buyer_whose_name_has_a_date_stays_the_buyer(router, text):
    """R5 masked «7 de agosto» inside the name and moved to other hospitals (SAN CRISTOBAL, MANUEL URIBE)."""
    params = _parse(router, text)
    assert params.get("entidad_resolved") == "E.S.E HOSPITAL 7 DE AGOSTO", text


def test_where_the_construction_does_not_resolve_the_hospital_the_period_changes_nothing(router):
    """Same buyer as the base and as the sentence without the period (a resolution defect that is not temporal).
    Declared debt E-TIME: «7 de agosto» is read as THE period of the sentence (2026-08-07), so with another period
    in front («en 2025 con quien ha contratado el Hospital 7 de Agosto») the buyer is still lost, as in the base."""
    text, control = "contratos de la ESE Hospital 7 de Agosto en 2025", "contratos de la ESE Hospital 7 de Agosto"
    assert _buyer(_parse(router, text)) == _buyer(_parse(router, control))


def test_the_real_funsocom_query_keeps_its_buyer_and_its_period(router):
    params = _parse(router, "que ha contratado funsocom con la alcaldia de barranquilla los ultimos dos años?")
    assert params["entidad_resolved"] == "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"
    assert params["objeto"] == ["funsocom"]
    assert (params["fecha_desde"], params["fecha_hasta"]) == ("2024-10-01", "2026-10-01")


# ── the exhaustive property: buyer x form x position, incl. the buyer BETWEEN two mentions ───────

PLAIN_BUYERS = [
    "la alcaldía de Barranquilla", "la alcaldía de Galapa", "la alcaldía de Soledad", "la alcaldía de Puerto Colombia",
    "el municipio de Malambo", "la gobernación del Atlántico", "la gobernación de Antioquia", "el SENA",
    "la Universidad Nacional de Colombia", "el INVIAS",
]
# Catalogue buyers whose name carries a date or a month (aliases_db: Hospital 7 de Agosto, Julio César
# Peñaloza, Julio Méndez Barreneche). Their resolution is fragile EVEN WITHOUT a period (a trailing «sobre aseo»
# sends the sentence to another hospital), so they are held to the property only where the period follows the
# buyer (``test_a_buyer_with_a_date_in_its_name_keeps_it_when_the_period_follows``).
TEMPORAL_NAME_BUYERS = [
    "el hospital 7 de agosto", "la ese hospital 7 de agosto", "el hospital julio mendez barreneche",
    "el hospital julio cesar peñaloza",
]
FORMS = [
    "en 2025", "durante 2025", "de enero a marzo de 2025", "desde enero de 2025", "entre 2023 y 2025",
    "este año", "los últimos 6 meses", "durante los últimos 2 años", "los últimos dos años", "de octubre a diciembre de 2025",
]
SHAPES = {
    "after": "contratos de {t} {who} {p}", "before": "contratos de {t} {p} {who}", "start": "{p} contratos de {t} {who}",
    "mid": "contratos {who} {p} de {t}",
}
BETWEEN_WINDOWS = ["los últimos 3 meses", "en los últimos 6 meses", "durante los últimos 2 años"]
BETWEEN_ANCHORS = ["de 2025", "en 2025", "de octubre a diciembre de 2025", "de la vigencia anterior"]
BETWEEN = {
    "anchor_who_window": "contratos de {t} {a} {who} {w}",
    "window_who_anchor": "contratos de {t} {w} {who} {a}",
    "who_anchor_window": "contratos de {t} {who} {a} {w}",
    "who_window_anchor": "contratos de {t} {who} {w} {a}",
    "lead_window_anchor": "{w} {a} contratos de {t} {who}",
}
TOPICS = ["aseo", "vigilancia", "obra civil", "alimentación escolar"]


def _who(connector: str, buyer: str) -> str:
    return f"del {buyer[3:]}" if connector == "de" and buyer.startswith("el ") else f"{connector} {buyer}"


def _cases():
    """Single period (every form meets every shape and connector) and the buyer BETWEEN two mentions."""
    index = 0
    for buyer in PLAIN_BUYERS:
        for form in FORMS:
            for shape, template in SHAPES.items():
                connector = ("con", "de", "para")[index % 3]
                topic = TOPICS[index % len(TOPICS)]
                index += 1
                yield buyer, template.format(t=topic, who=_who(connector, buyer), p=form), template.format(
                    t=topic, who=_who(connector, buyer), p="",
                ), topic
        for shape, template in BETWEEN.items():
            for window in BETWEEN_WINDOWS:
                anchor = BETWEEN_ANCHORS[index % len(BETWEEN_ANCHORS)]
                connector = ("con", "de", "para")[index % 3]
                topic = TOPICS[index % len(TOPICS)]
                index += 1
                who = _who(connector, buyer)
                yield buyer, template.format(t=topic, who=who, w=window, a=anchor), (
                    "{t} {who}".format(t=f"contratos de {topic}", who=who)
                ), topic


def _squash(text: str) -> str:
    return " ".join(text.split())


def test_the_buyer_is_never_lost_nor_changed_and_its_words_are_never_topic_whatever_the_period(router):
    failures: list[str] = []
    controls: dict[str, dict] = {}
    for buyer, text, control, topic in _cases():
        text, control = _squash(text), _squash(control)
        params = _parse(router, text)
        assert params.get("fecha_desde") or params.get("fecha_hasta") or params.get("_temporal_error"), (
            f"the form is not read as a period (the property would be vacuous): {text!r}"
        )
        if control not in controls:
            controls[control] = _parse(router, control)
        expected = controls[control]
        if _buyer(params) != _buyer(expected):
            failures.append(f"BUYER {_buyer(params)} != {_buyer(expected)} <- {text!r}")
        words = _tokens(buyer) - _tokens(topic) - {"de", "del", "la", "el", "con", "en"}
        leaked = (words & _topic(params)) - _topic(expected)
        if leaked:
            failures.append(f"TOPIC {sorted(leaked)} <- {text!r} -> {params.get('objeto')}")
    assert not failures, f"{len(failures)} failures, first 12:\n" + "\n".join(failures[:12])


def test_the_property_covers_the_error_turns_the_blocked_frame_is_made_of():
    texts = [_squash(text) for _, text, _, _ in _cases()]
    errors = [text for text in texts if resolve_temporal_scope(_fold(text), today=TODAY).error_code]
    assert len(errors) >= 150, "the buyer between two mentions is in the enumerated set"


# ── end to end: T0 asks, T1 answers the period, the buyer is still in the query that runs ─────────

@pytest.fixture
def workflow(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "false")
    get_settings.cache_clear()
    flow = SecopalWorkflow(Settings(
        _env_file=None, deepseek_api_key=None, secoppal_natural_alpha_mode="off",
        secop_feedback_path=tmp_path / "feedback.jsonl", secop_feedback_db_path=tmp_path / "feedback.db",
        secop_feedback_sqlite_enabled=False,
    ))
    calls: list[str] = []
    row = {"id_contrato": "X", "nombre_entidad": "MUNICIPIO DE SOLEDAD", "objeto_del_contrato": "Aseo",
           "valor_del_contrato": "150000000", "fecha_de_firma": "2025-11-01"}
    monkeypatch.setattr(flow.secop_client, "query", lambda dataset, soql, **kw: (calls.append(soql), [dict(row)])[1])
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **k: (calls.append("count"), 1)[1])
    monkeypatch.setattr(flow.secop_client, "aggregate", lambda *a, **k: (calls.append("aggregate"), [])[1])
    monkeypatch.setattr(flow.narrator, "narrate_with_grounding", lambda **kwargs: None)
    flow.calls = calls
    yield flow
    get_settings.cache_clear()


@pytest.mark.parametrize("first,entity", [
    ("contratos de aseo de 2025 con la Alcaldía de Soledad de los últimos 3 meses", "MUNICIPIO DE SOLEDAD"),
    ("contratos de aseo de los últimos 3 meses con la Alcaldía de Soledad en 2025", "MUNICIPIO DE SOLEDAD"),
    ("contratos de aseo de los últimos 3 meses de la Alcaldía de Barranquilla de 2025",
     "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"),
    ("contratos de aseo de 2025 de la alcaldía de Galapa en los últimos 3 meses", "MUNICIPIO DE GALAPA."),
])
def test_the_answer_to_the_period_question_runs_with_the_buyer_of_the_question(workflow, first, entity):
    asked = workflow.run_query(first, chat_id="r6", today=TODAY)
    assert asked["needs_clarification"] is True and workflow.calls == []
    workflow.run_query("de octubre a diciembre de 2025", chat_id="r6", today=TODAY)
    sql = " ".join(call for call in workflow.calls if isinstance(call, str))
    assert entity in sql, f"the buyer of the question is lost: {sql[:400]}"
    assert "2025-10-01" in sql and "2025-12-31" in sql


def test_a_buyer_with_a_date_in_its_name_keeps_it_when_the_period_follows(router):
    """«Hospital 7 de Agosto», «Julio Méndez Barreneche»: R5 masked the temporal words of the name and changed
    hospital. The enumerated period follows the buyer, at the end of the capture, as the grammar reads it."""
    failures: list[str] = []
    for buyer in TEMPORAL_NAME_BUYERS:
        for form in FORMS:
            for template in ("contratos de aseo con {b} {p}", "con quien ha contratado {b} {p}"):
                text = _squash(template.format(b=buyer, p=form))
                control = _squash(template.format(b=buyer, p=""))
                if _buyer(_parse(router, text)) != _buyer(_parse(router, control)):
                    failures.append(f"{_buyer(_parse(router, text))} != {_buyer(_parse(router, control))} <- {text!r}")
    assert not failures, f"{len(failures)} failures, first 8:\n" + "\n".join(failures[:8])
