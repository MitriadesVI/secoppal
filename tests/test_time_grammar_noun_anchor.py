"""M1 wave 1 R4 (ZEUS3 blocking): a temporal noun or a non-canonical year after a window asks.

Wave 1 resolves a relative window («los últimos 3 meses») from the clock. When the person also
wrote a period the temporal authority cannot place («de la vigencia anterior», «del gobierno
anterior», «de dos mil veinticinco», «de 2.025», «de 25») the window ran from today, DISJOINT from
what was asked, with no warning (the base found no period at all: the history contained the
request). ``temporal_noun_anchor`` declares the finite vocabulary and the rule asks with the same
error and the same question as an explicit anchor.

The properties are enumerated: temporal nouns x adjectives x determiners x windows x position, the
2000-2099 years in words / with a separator / in two digits, and a control list of continuations
taken from the repo's corpora that must keep the window exactly as in R3.
"""
from __future__ import annotations

import itertools
import re
from datetime import date

import pytest

from app.core import non_canonical_year as ncy
from app.core import temporal_noun_anchor as tna
from app.core import time_grammar as ts
from app.core import temporal_window_anchor as twa
from app.core.time_grammar import resolve_temporal_scope

TODAY = date(2026, 10, 1)
CODE = twa.ERROR_CODE
B = "contratos de la Alcaldía de Galapa"
WINDOW_RESULT = ("2026-07-01", "2026-10-01")

# Windows of BOTH lexicons (legacy and time_relative), as they are written.
WINDOWS = [
    "en los últimos 3 meses", "en los últimos 6 meses", "en las últimas 2 semanas",
    "en los últimos 10 días", "en los últimos 2 años", "en la última semana",
    "en los dos últimos meses", "en el último mes", "hace dos semanas",
]

# Independent of the module under test: the nouns the orchestrator named, with their plurals.
STRONG_NOUNS = [
    "vigencia", "vigencias", "periodo", "periodos", "período", "períodos", "año", "años",
    "cuatrienio", "cuatrienios", "semestre", "semestres", "trimestre", "trimestres", "bimestre",
    "bimestres", "cuatrimestre", "cuatrimestres", "quincena", "quincenas", "pandemia", "pandemias",
]
# Nouns that are ALSO part of buyer names in the repo's catalogs: only an explicit qualifier anchors.
ENTITY_OFFICE_NOUNS = [
    "gobierno", "gobiernos", "administración", "administraciones", "gestión", "gestiones",
]
OFFICE_NOUNS = ["mandato", "mandatos", "ejercicio", "ejercicios"]
QUALIFIERS = ["anterior", "pasado", "pasada", "saliente", "entrante", "actual", "previo", "previa",
              "anteriores", "pasados"]
NOT_QUALIFIERS = ["fiscal", "vigente", "siguiente", "próximo", "presente", "corriente", "municipal",
                  "departamental", "nacional", "central", "digital", "público", "local", "regional"]
PERSON_NOUNS = ["alcalde", "alcaldes", "gobernador", "gobernadores", "presidente", "presidentes"]
ADJECTIVES = [
    "anterior", "pasado", "pasada", "actual", "siguiente", "próximo", "próxima", "vigente", "fiscal",
    "anteriores", "pasados", "actuales", "siguientes", "previo", "saliente", "entrante",
]
PLAIN = ["la", "el", "los", "las"]
POSSESSIVE = ["su", "sus", "este", "esta", "ese", "esa", "mi", "nuestro"]


# The authority already refuses these with its own error («unsupported_temporal_expression»):
# «año|mes|semana|día pasado|anterior», «trimestre», «semestre».
_OWN_RULE = re.compile(
    r"\b(?:trimestres?|semestres?|(?:mes|a[nñ]o|semanas?|d[ií]as?)\s+(?:pasad|anterior))", re.IGNORECASE,
)


def must_ask(text: str) -> None:
    """This rule's question, or the authority's own refusal for the same words; never a window."""
    assert blocked(text), text
    assert asks(text) or _OWN_RULE.search(text), text


def _resolve(text: str):
    return resolve_temporal_scope(text, today=TODAY)


def asks(text: str) -> bool:
    found = _resolve(text)
    return found.error_code == CODE and not (found.fecha_desde or found.fecha_hasta)


def blocked(text: str) -> bool:
    """Fail-closed: this rule's question, or the authority's pre-existing «unsupported expression»
    («año anterior», «mes pasado» next to a window); never a period executed from the clock."""
    found = _resolve(text)
    return found.error_code in (CODE, "unsupported_temporal_expression") and not (
        found.fecha_desde or found.fecha_hasta
    )


def keeps_window(text: str) -> bool:
    found = _resolve(text)
    return not found.error_code and (found.fecha_desde, found.fecha_hasta) == WINDOW_RESULT


def _phrases(nouns, determiners, adjectives):
    for noun in nouns:
        for det in determiners:
            for adj in adjectives:
                yield f"de {det} {noun}" + (f" {adj}" if adj else "")


def _positions(window: str, phrase: str):
    yield f"{B} {window} {phrase}"          # window, then the phrase
    yield f"{B} {phrase} {window}"          # the phrase, then the window


# ── strong nouns: any determiner, or an adjective ─────────────────────────────────────────

@pytest.mark.parametrize("window", WINDOWS)
def test_a_strong_noun_with_a_determiner_or_an_adjective_asks(window):
    for phrase in itertools.chain(
        _phrases(STRONG_NOUNS, PLAIN + POSSESSIVE, [None, *ADJECTIVES[:6]]),
        _phrases(STRONG_NOUNS, [""], ADJECTIVES),
    ):
        phrase = phrase.replace("de  ", "de ")
        for text in _positions(window, phrase):
            must_ask(text)


@pytest.mark.parametrize("window", WINDOWS)
def test_the_contracted_article_asks_for_a_strong_noun(window):
    for noun in STRONG_NOUNS:
        for adj in (None, "anterior", "pasado", "actual"):
            phrase = f"del {noun}" + (f" {adj}" if adj else "")
            for text in _positions(window, phrase):
                must_ask(text)


@pytest.mark.parametrize("window", WINDOWS)
def test_a_strong_noun_alone_is_not_a_period(window):
    """«de año», «de periodo»: no article and no adjective (a name or a technical term)."""
    for noun in ("año", "periodo", "vigencia", "quincena", "pandemia"):
        for text in _positions(window, f"de {noun}"):
            assert not asks(text), text


# ── entity-office nouns (in buyer names): only an explicit temporal qualifier ──────────────────

@pytest.mark.parametrize("window", WINDOWS)
def test_an_entity_office_noun_asks_only_with_an_explicit_qualifier(window):
    for noun in ENTITY_OFFICE_NOUNS:
        for det in PLAIN + POSSESSIVE:
            for qualifier in QUALIFIERS:
                for text in _positions(window, f"de {det} {noun} {qualifier}"):
                    must_ask(text)
        for qualifier in QUALIFIERS:
            for text in _positions(window, f"del {noun} {qualifier}"):
                must_ask(text)


@pytest.mark.parametrize("window", WINDOWS)
def test_an_entity_office_noun_never_anchors_bare_with_a_possessive_or_with_a_name_adjective(window):
    for noun in ENTITY_OFFICE_NOUNS:
        phrases = [f"de {det} {noun}" for det in PLAIN + POSSESSIVE] + [f"del {noun}", f"de {noun}"]
        phrases += [f"del {noun} {adj}" for adj in NOT_QUALIFIERS]
        phrases += [f"de la {noun} {adj}" for adj in NOT_QUALIFIERS]
        for phrase in phrases:
            for text in _positions(window, phrase):
                assert not asks(text), text


# ── the other office nouns (not in buyer names): an adjective or a possessive ──────────────────

@pytest.mark.parametrize("window", WINDOWS)
def test_an_office_noun_asks_with_an_adjective_or_a_possessive(window):
    for phrase in itertools.chain(
        _phrases(OFFICE_NOUNS, PLAIN + POSSESSIVE, ADJECTIVES[:8]),
        _phrases(OFFICE_NOUNS, POSSESSIVE, [None]),
    ):
        for text in _positions(window, phrase):
            must_ask(text)
    for noun in OFFICE_NOUNS:
        for adj in ("anterior", "pasado", "actual", "fiscal"):
            for text in _positions(window, f"del {noun} {adj}"):
                must_ask(text)
        for phrase in (f"de la {noun}", f"del {noun}", f"de los {noun}"):
            for text in _positions(window, phrase):
                assert not asks(text), text


@pytest.mark.parametrize("window", WINDOWS)
def test_an_entity_named_with_an_office_noun_keeps_the_window(window):
    for phrase in (
        "del Gobierno del Tolima", "del Gobierno Departamental del Tolima", "de la Gobernación del Atlántico",
        "de la Administración Municipal de Galapa", "del gobierno nacional", "de la administración municipal",
        "de la Administración Pública ESAP", "del Gobierno Digital de Colombia",
        "de la Gestión de Bienes y Servicios de Cali", "del Instituto Distrital de Gestión de Riesgos",
    ):
        for text in _positions(window, phrase):
            assert not asks(text), text


@pytest.mark.parametrize("window", WINDOWS)
def test_a_person_noun_asks_only_with_an_adjective(window):
    for noun in PERSON_NOUNS:
        for adj in ("anterior", "pasado", "saliente", "actual"):
            for text in _positions(window, f"del {noun} {adj}"):
                must_ask(text)
        for text in _positions(window, f"del {noun}"):
            assert not asks(text), text
        for text in _positions(window, f"de su {noun}"):
            assert not asks(text), text


def test_the_ordinal_between_the_article_and_the_noun_is_allowed():
    for phrase in ("del primer semestre", "del segundo trimestre", "de los dos años anteriores",
                   "de la misma vigencia", "del tercer cuatrimestre"):
        must_ask(f"{B} en los últimos 3 meses {phrase}")


def test_the_orchestrator_examples():
    for text in (
        f"{B} en los últimos 3 meses de la vigencia anterior",
        f"{B} en los últimos 6 meses del gobierno anterior",
        f"{B} en los últimos 6 meses de dos mil veinticinco",
        f"{B} en los últimos 6 meses de 2.025",
        f"{B} en los últimos 6 meses de 25",
        f"{B} en los últimos 6 meses de la administración anterior",
        f"{B} en los últimos 3 meses de la vigencia pasada",
        f"{B} en los últimos 3 meses del periodo anterior",
        f"{B} en los últimos 3 meses del cuatrienio anterior",
        f"{B} en los últimos 3 meses de su mandato",
        f"{B} en los últimos 6 meses del año",
    ):
        assert asks(text), text


ORCHESTRATOR_NEGATIVES = [
    ("contratos de los últimos 3 meses del Gobierno del Tolima", WINDOW_RESULT),
    ("contratos de los últimos 3 meses del Gobierno Departamental del Tolima", WINDOW_RESULT),
    ("contratos de los últimos 3 meses de la Gobernación del Atlántico", WINDOW_RESULT),
    ("contratos de los últimos 3 meses de la Administración Municipal de Galapa", WINDOW_RESULT),
    ("procesos de los últimos 30 días de 25 municipios", ("2026-09-01", "2026-10-01")),
    ("procesos de los últimos 30 días de 25 entidades", ("2026-09-01", "2026-10-01")),
    ("procesos de los últimos 3 meses de 25", None),   # a year (end of clause): asks
]
ORCHESTRATOR_POSITIVES = [
    "contratos de los últimos 3 meses del gobierno anterior",
    "contratos de los últimos 3 meses de la administración pasada",
    "contratos de los últimos 3 meses de la vigencia anterior",
    "contratos de los últimos 3 meses del mandato anterior",
    "contratos de los últimos 3 meses del cuatrienio anterior",
    "contratos de los últimos 3 meses del gobierno saliente",
    "contratos de los últimos 3 meses de la gestión anterior",
]


@pytest.mark.parametrize("text,period", ORCHESTRATOR_NEGATIVES)
def test_the_orchestrator_negatives_behave_as_in_r3(text, period):
    found = _resolve(text)
    if period is None:
        assert asks(text), text
        return
    assert not found.error_code, text
    assert (found.fecha_desde, found.fecha_hasta) == period, text


@pytest.mark.parametrize("text", ORCHESTRATOR_POSITIVES)
def test_the_orchestrator_positives_ask(text):
    assert asks(text), text


def test_a_two_digit_year_needs_the_end_of_the_clause():
    for ending in ("", ".", ",", ";", " ?", ")"):
        assert asks(f"{B} en los últimos 3 meses de 25{ending}"), ending
    for tail in (" municipios", " entidades", " contratos", " de aseo", " en Galapa", " y 26", " mil"):
        assert not asks(f"{B} en los últimos 3 meses de 25{tail}"), tail


def _buyer_catalog_tokens() -> set[str]:
    import json
    from pathlib import Path

    tokens: set[str] = set()

    def walk(node):
        if isinstance(node, str):
            tokens.update(re.findall(r"[a-z0-9]+", tna.fold(node)))
        elif isinstance(node, dict):
            for key, value in node.items():
                walk(key)
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    for name in ("aliases_db.json", "buyer_source_names.json", "entidades_raw.json", "buyer_groups.json"):
        walk(json.loads((Path("app/data") / name).read_text(encoding="utf-8")))
    return tokens


def test_a_noun_that_appears_in_a_buyer_name_needs_an_explicit_qualifier():
    """Reads the repo's buyer catalogs: a lexicon noun that is part of a buyer name must be an
    ENTITY_OFFICE noun (qualifier only). Adding «gobierno» to STRONG would fail here."""
    in_names = _buyer_catalog_tokens()
    for table, label in ((tna.STRONG, "strong"), (tna.OFFICE, "office"), (tna.PERSON, "person")):
        for noun in (*table, *table.values()):
            assert noun not in in_names, f"{noun!r} ({label}) is part of a buyer name: make it an entity-office noun"
    for noun in tna.ENTITY_OFFICE:
        assert noun in in_names, f"{noun!r} is no longer in any buyer name: it can move to another class"


def test_accents_and_case_do_not_matter():
    for text in (
        f"{B} en los últimos 3 meses DEL PERÍODO ANTERIOR",
        f"{B} en los últimos 3 meses del periodo anterior",
        f"{B} en los últimos 3 meses Del Período Anterior",
        f"{B} en los últimos 3 meses de la ADMINISTRACION anterior",
        f"{B} en los ultimos 3 meses de la vigencia anterior",
    ):
        assert asks(text), text


# ── years the authority does not read ─────────────────────────────────────────────────────

_UNITS = ["", "uno", "dos", "tres", "cuatro", "cinco", "seis", "siete", "ocho", "nueve"]
_TEENS = ["diez", "once", "doce", "trece", "catorce", "quince", "dieciséis", "diecisiete",
          "dieciocho", "diecinueve"]
_VEINTI = ["veinte", "veintiuno", "veintidós", "veintitrés", "veinticuatro", "veinticinco",
           "veintiséis", "veintisiete", "veintiocho", "veintinueve"]
_TENS = {3: "treinta", 4: "cuarenta", 5: "cincuenta", 6: "sesenta", 7: "setenta", 8: "ochenta",
         9: "noventa"}


def year_in_words(year: int) -> str:
    rest = year - 2000
    if rest == 0:
        return "dos mil"
    if rest < 10:
        return f"dos mil {_UNITS[rest]}"
    if rest < 20:
        return f"dos mil {_TEENS[rest - 10]}"
    if rest < 30:
        return f"dos mil {_VEINTI[rest - 20]}"
    tens, unit = divmod(rest, 10)
    return f"dos mil {_TENS[tens]}" + (f" y {_UNITS[unit]}" if unit else "")


@pytest.mark.parametrize("window", WINDOWS[:4])
def test_every_year_2000_2099_in_words_asks(window):
    for year in range(2000, 2100):
        words = year_in_words(year)
        for text in _positions(window, f"de {words}"):
            assert asks(text), text
        must_ask(f"{B} {window} del año {words}")


def test_a_compound_veinte_written_as_veinte_y_cinco_asks():
    assert asks(f"{B} en los últimos 3 meses de dos mil veinte y cinco")


@pytest.mark.parametrize("window", WINDOWS[:4])
def test_every_year_with_a_thousands_separator_asks(window):
    for year in range(2000, 2100):
        for sep in (".", ","):
            number = f"{year // 1000}{sep}{year % 1000:03d}"
            for text in _positions(window, f"de {number}"):
                assert asks(text), text


@pytest.mark.parametrize("window", WINDOWS)
def test_a_two_digit_year_right_after_the_window_asks(window):
    for two in range(0, 100):
        assert asks(f"{B} {window} de {two:02d}"), two
        assert asks(f"{B} {window}, del {two:02d}"), two


def test_a_two_digit_year_is_a_quantity_when_it_is_not_right_after_the_window():
    assert not asks(f"{B} de 25 contratos en los últimos 3 meses")
    assert not asks(f"{B} de 25 en los últimos 3 meses")


QUANTITIES = [
    "de 25 millones", "de 25 años", "de 10 a 5", "de 25 mil pesos", "de 25%", "de 25 mm",
    "de 2.025 millones", "de 2,025 millones", "de dos mil millones", "de dos mil quinientos pesos",
    "de dos mil 500", "de dos mil cien", "de dos mil trescientos", "más de 2.000", "más de dos mil",
    "menos de 2.050", "mayor de 2.000", "de 2.0250", "de 12.025", "de 2.025.000",
    "de 25 smmlv", "de 25 meses", "de 25 días", "de 25 salarios",
]


@pytest.mark.parametrize("window", WINDOWS)
def test_a_quantity_is_never_read_as_a_year(window):
    for quantity in QUANTITIES:
        for text in _positions(window, quantity):
            assert not asks(text), text


def test_the_year_forms_recognise_nothing_else():
    start, end = ncy.find("meses de dos mil veinticinco")
    assert "meses de dos mil veinticinco"[start:end] == "dos mil veinticinco"
    assert ncy.find("contratos de aseo") is None
    assert ncy.find("de 2025") is None, "the canonical year is the authority's, not this module's"
    assert ncy.find_adjacent("meses de 25 millones", 5) is None
    assert ncy.find_adjacent("meses de 25", 5) == (9, 11)
    assert ncy.find_adjacent("meses de 25 de marzo", 5) is None


# ── what must NOT change: the control list ────────────────────────────────────────────────

NON_TEMPORAL = [
    "de la Alcaldía", "de la Gobernación", "de la Gobernación del Atlántico", "de aseo", "de obra",
    "del Atlántico", "de Barranquilla", "de la entidad", "del gobierno nacional",
    "de la administración municipal", "de los contratos", "de las obras", "de mantenimiento",
    "del municipio", "de la Contraloría", "de servicios de salud", "de infraestructura vial",
    "de la alcaldía de Soledad", "de CONSORCIO VIAL", "del Departamento de Bolívar",
    "de peajes en el Cauca", "de 300 millones", "de más de 10 mil millones",
]
CORPUS_CONTINUATIONS = (
    "DE 100 MILLONES",
    "DE ANTIOQUIA-HOMO",
    "DE BOGOTÁ JORGE TADEO",
    "DE COROZAL por mayor",
    "DE DIAGNÓSTICO MÉDICO S.A",
    "DE GUATAPE ACEC",
    "DE LEON por mayor",
    "DE NORMAS TÉCNICAS Y",
    "DE PAUL DE SAN",
    "DE RESTREPO AGUA VIVA",
    "DE SAN FRANCISCO",
    "DE SERVICIOS DE SALUD",
    "DEL ADULTO MAYOR",
    "DEL ESTADO DEL DEPARTAMENTO",
    "DEL PACIFICO por mayor",
    "Del Castillo Consultores Jurídicos",
    "de 10 a 5",
    "de 1000284463",
    "de 1028001493",
    "de 1096230998",
    "de 16715049",
    "de 200 millones en",
    "de 300 millones en",
    "de 500 millones abiertas",
    "de 53093734",
    "de 800173097",
    "de 830093106",
    "de 900221051",
    "de 901176934",
    "de ALMACEN COMERCIAL CUERPO",
    "de ASOCIACION DEL CAMBIO",
    "de Bogota",
    "de CESAR FELIPE SANCHEZ",
    "de CONSORCIO CONEXION SOGAMOSO",
    "de CORPORACION PARA EL",
    "de Cuervo Del Castillo",
    "de EMPRESA DE SERVICIOS",
    "de Educación de Cali",
    "de FUNDACIÓN UNIVERSIDAD DE",
    "de Helmer José De",
    "de Infraestructura de los",
    "de LIGA DE LEVANTAMIENTO",
    "de MOBILIARIO DE OFICINA",
    "de ORGANIZACION WIWA YUGUMAIUN",
    "de ROBOTEC COLOMBIA SAS",
    "de San Andrés, Providencia",
    "de UNION TEMPORAL PARA",
    "de YEISMIS CANDELARIA MUÑOZ",
    "de adulto mayor del",
    "de al menos 50",
    "de alimentacion en barranquilla",
    "de alimentación 100-200 millones",
    "de alimentación de 200",
    "de alimentación exceda 200",
    "de alimentación mayores 200",
    "de alimentación más contratos",
    "de alimentación no excedan",
    "de alimentación no sea",
    "de alimentación por mas",
    "de alimentación superan 200",
    "de apoyo a la",
    "de aseo",
    "de aseo de minimo",
    "de aseo en Armenia",
    "de aseo en atlantico",
    "de aseo en monteria",
    "de aseo la semana",
    "de aseo por encima",
    "de aseo, excepto los",
    "de belleza",
    "de comedores comunitarios en",
    "de consultoría",
    "de contratación directa en",
    "de dotacion abierto por",
    "de dotación de más",
    "de dotación por 500",
    "de eso sirve, no",
    "de interventoria tecnica de",
    "de jose cantillo con",
    "de la Secretaría de",
    "de la alcaldía, pero",
    "de la cédula 1016092632",
    "de la cédula 1116282281",
    "de la empresa NIT",
    "de la persona 1007070886",
    "de la persona 1088313303",
    "de limpieza y destronque",
    "de los ultimos 30",
    "de mantenimiento en bogotá",
    "de mantenimiento vial",
    "de mayor a menor",
    "de menor cuantía de",
    "de minima cuantia en",
    "de más de mil",
    "de méritos con precalificación",
    "de méritos sin que",
    "de obra",
    "de obra en cartagena",
    "de obra publica en",
    "de obras por 500",
    "de parques o similares",
    "de personal",
    "de primera infancia",
    "de proveedores de la",
    "de regimen especial de",
    "de servicios",
    "de subasta inversa",
    "de suministro entre 100-550",
    "de transporte escolar ordenados",
    "de vias para poder",
    "de vigilancia en atlantico",
    "de vigilancia por debajo",
    "de vivienda en Sucre",
    "del Huila",
    "del NIT 13848603",
    "del NIT 805016686",
    "del NIT 8300342337",
    "del NIT 8600077593",
    "del NIT 8600319092",
    "del NIT 890907479",
    "del NIT 8999990619",
    "del NIT 900.923.481-5",
    "del NIT 900870468",
    "del NIT 901086083",
    "del Sur en Antioquia",
    "del contratista ALCALDIA MUNICIPAL",
    "del contratista ASOCIACION DE",
    "del contratista COMPAÑÍA NACIONAL",
    "del contratista CONSORCIO RIESGO",
    "del contratista E.S.E. HOSPITAL",
    "del contratista FUNDACION HOGAR",
    "del contratista INDUSTRIAS METÁLICAS",
    "del contratista LA PREVISORA",
    "del contratista MIGUEL ANTONIO",
    "del contratista RGM ELECTRONICA",
    "del contratista UNION TEMPORAL",
    "del contratista ZL ZONA",
    "del cédula 1096230998",
    "del cédula 28917467",
    "del cédula 80854496",
    "del municipio de bucaramanga",
    "del proveedor ALCALDÍA LOCAL",
    "del proveedor ASOCIACION COMITE",
    "del proveedor C4 Pascal",
    "del proveedor CONSORCIO ACUEDUCTO",
    "del proveedor COOPERATIVA DE",
    "del proveedor Daniel Mauricio",
    "del proveedor ESTACION DE",
    "del proveedor FUNDACIÓN GUAYAQUIL",
    "del proveedor ICBF REGIONAL",
    "del proveedor Importaciones y",
    "del proveedor Luis Anderson",
    "del proveedor NIT 1027522925",
    "del proveedor NIT 830021022",
    "del proveedor NIT 900349565",
    "del proveedor NIT 901311950",
    "del proveedor SANATORIO DE",
    "del proveedor SUPERCOMERCIAL DEL",
    "del proveedor UNIVERSIDAD NACIONAL",
    "del proveedor YENNY GUAVITA",
    "del proveedor cédula 1110526744",
    "del proveedor cédula 37626970",
    "del proveedor lucia margarita",
)


@pytest.mark.parametrize("window", WINDOWS)
def test_a_non_temporal_continuation_keeps_the_window(window):
    for phrase in NON_TEMPORAL:
        for text in _positions(window, phrase):
            assert not asks(text), text


def test_the_corpus_continuations_keep_the_window_exactly_as_in_r3():
    """163 continuations taken from the repo's corpora («de <NIT>», «de <entity name>»…)."""
    for continuation in CORPUS_CONTINUATIONS:
        for text in (
            f"{B} en los últimos 3 meses {continuation}",
            f"{B} {continuation} en los últimos 3 meses",
        ):
            assert not asks(text), text
        assert keeps_window(f"{B} en los últimos 3 meses {continuation}"), continuation


def test_without_a_window_nothing_changes():
    for text in (
        f"{B} de la vigencia anterior", f"{B} del gobierno anterior", f"{B} de dos mil veinticinco",
        f"{B} de 2.025", f"{B} de 25", f"{B} del año",
    ):
        assert not asks(text), text


def test_an_explicit_anchor_keeps_its_own_question():
    found = _resolve(f"{B} en los últimos 3 meses de 2025")
    assert found.error_code == CODE and "de 2025" in found.clarification


def test_a_noun_inside_a_protected_span_is_not_an_anchor():
    text = "contratos del Colegio del Año Nuevo en los últimos 3 meses"
    start = text.index("del Año")
    assert asks(text)
    found = resolve_temporal_scope(
        text, today=TODAY, protected_spans=((start, start + len("del Año Nuevo")),),
    )
    assert not found.error_code and (found.fecha_desde, found.fecha_hasta) == WINDOW_RESULT


def test_a_phrase_that_overlaps_the_window_is_not_an_anchor():
    for text in (
        f"{B} en los dos últimos años", f"{B} en los últimos 2 años", f"{B} en el último año",
    ):
        found = _resolve(text)
        assert found.error_code != CODE, text


def test_a_window_inside_another_window_keeps_the_clock_window_declared():
    """«los últimos 3 meses del último año»: the inner window contains the outer one, so counting
    from today is the same period. Not an anchor (declared, unchanged from R3)."""
    assert keeps_window(f"{B} en los últimos 3 meses del último año")


def test_the_clarification_names_both_readings_and_the_way_to_answer():
    found = _resolve(f"{B} en los últimos 3 meses de la vigencia anterior")
    assert found.error_code == CODE
    question = found.clarification
    assert "ultimos 3 meses" in tna.fold(question)
    assert "vigencia anterior" in question
    for example in twa.EXAMPLES:
        assert f"«{example}»" in question
    assert not re.search(r"\(\d\)|opci[oó]n|completo", question, re.I)


# ── the declared decision table ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("kind,determiner,adjective,expected", [
    ("strong", None, False, False), ("strong", "plain", False, True), ("strong", "possessive", False, True),
    ("strong", None, True, True), ("office", None, False, False), ("office", "plain", False, False),
    ("office", "possessive", False, True), ("office", "plain", True, True), ("person", None, False, False),
    ("person", "plain", False, False), ("person", "possessive", False, False), ("person", None, True, True),
])
def test_the_decision_table(kind, determiner, adjective, expected):
    assert tna.is_anchor(kind, determiner, adjective) is expected


def test_every_declared_noun_is_in_the_independent_list_or_declared_here():
    """A noun added to the module without a decision here fails: the lexicon cannot drift unseen."""
    declared = set(tna.STRONG) | set(tna.STRONG.values())
    folded = {tna.fold(noun) for noun in STRONG_NOUNS}
    extras = {"legislatura", "legislaturas", "bienio", "bienios", "trienio", "trienios",
              "decada", "decadas", "temporada", "temporadas"}
    assert declared == folded | extras
    entity_office = set(tna.ENTITY_OFFICE) | set(tna.ENTITY_OFFICE.values())
    assert entity_office == {tna.fold(noun) for noun in ENTITY_OFFICE_NOUNS}
    office = set(tna.OFFICE) | set(tna.OFFICE.values())
    assert office == {tna.fold(noun) for noun in OFFICE_NOUNS}
    person = set(tna.PERSON) | set(tna.PERSON.values())
    assert person >= {tna.fold(noun) for noun in PERSON_NOUNS}
    assert person - {tna.fold(noun) for noun in PERSON_NOUNS} == {
        "alcaldesa", "alcaldesas", "gobernadora", "gobernadoras", "presidenta", "presidentas",
    }


def test_the_noun_anchor_runs_after_the_explicit_anchor_rule():
    """An explicit anchor keeps its own message; only when there is none does the noun rule look."""
    both = _resolve(f"{B} en los últimos 3 meses de 2025 de la vigencia anterior")
    assert both.error_code == CODE and "de 2025" in both.clarification
    assert ts.window_patterns(), "the noun rule shares the windows of the two lexicons"
