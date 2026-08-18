"""Money V2 / M1 — congelación del comportamiento monetario antes de tocar runtime.

Este módulo NO arregla nada. Su único trabajo es fijar por test dos cosas que el
bloque M0 (Money Resolution Audit) verificó empíricamente sobre el runtime:

  1. Lo que YA funciona, como PASS obligatorio. Cualquier fix posterior (M2-M6)
     que rompa uno de estos tests es una regresión, no un avance.
  2. Lo que está roto, como ``xfail(strict=True)``. Cada xfail afirma el
     comportamiento CORRECTO deseado; hoy falla. Si alguno pasa a XPASS sin que
     hayamos escrito el fix, el bloque falla a propósito para obligarnos a
     investigar por qué cambió el runtime.

Sobre la elección de xfail estricto en vez del corpus canónico: se inspeccionó
``scripts/run_query_corpus.py`` y el runner no tiene ningún concepto de
expected-failure (no hay campo ``status``/``known_fail``/``xfail`` en el esquema
YAML, ``CheckResult.passed`` es un bool plano y el exit code es
``1 if any(not r.passed ...)``). Meter un caso conocido-roto en
``query_corpus_v1.yaml`` pondría el gate canónico en rojo. Por eso los casos
nuevos viven aquí y el corpus queda intacto.

Ámbito deliberado: se cubren CLASES de expresión, no paráfrasis equivalentes.
La polaridad ya tiene 25 casos en ``tests/test_value_polarity.py`` y M0 la
verificó sana 13/13, así que aquí solo se ancla un representante por clase.
"""

from __future__ import annotations

import re

import pytest

from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.utils.money import money_to_cop

router = QueryRouter()
builder = SoQLBuilder()


def _params(query: str) -> dict:
    """Params del parser determinista para ``query``."""
    return dict(router.parse(query).params)


def _money_predicates(query: str) -> list[str]:
    """Predicados monetarios que la query proyecta al WHERE real de SoQL.

    Devuelve la lista de comparaciones sobre la columna de valor del dataset
    (``precio_base`` o ``valor_del_contrato``), tal como llegarían a Socrata.
    """
    params = _params(query)
    dataset_id = SoQLBuilder.dataset_id_for(params.get("dataset"))
    soql = builder.build(dataset_id, params)
    return re.findall(r"(?:precio_base|valor_del_contrato)\s*[<>]=\s*\d+", soql)


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 1 — legacy: unidades que ya funcionan. PASS obligatorio.
# ══════════════════════════════════════════════════════════════════════════

LEGACY_UNITS = [
    ("1 millon", 1_000_000),
    ("1 millón", 1_000_000),
    ("2 millones", 2_000_000),
    ("500 millones", 500_000_000),
    ("1000 millones", 1_000_000_000),
    ("mil millones", 1_000_000_000),
    ("2 mil millones", 2_000_000_000),
    ("1 billón", 1_000_000_000_000),
    ("2 billones", 2_000_000_000_000),
    ("1 palo", 1_000_000),
    ("2 palos", 2_000_000),
    ("1 barra", 1_000_000),
    ("2 barras", 2_000_000),
]


@pytest.mark.parametrize("raw,expected", LEGACY_UNITS)
def test_legacy_units_must_not_regress(raw: str, expected: int) -> None:
    """Las unidades sanas de M0/D1 no pueden cambiar de valor en Money V2."""
    assert money_to_cop(raw) == expected


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 2 — decimal: guardas anti-regresión del futuro fix de separadores.
#
# M2 hará que "punto + exactamente 3 dígitos" signifique separador de millares.
# Estos casos tienen 1 o 2 dígitos tras el punto y DEBEN seguir siendo decimales.
# Son la red que impide que el fix de H1 se lleve por delante los decimales.
# ══════════════════════════════════════════════════════════════════════════

DECIMAL_CASES = [
    ("1.5 millones", 1_500_000),
    ("1,5 millones", 1_500_000),
    ("1.25 millones", 1_250_000),
    ("1,25 millones", 1_250_000),
    ("1.5 billones", 1_500_000_000_000),
]


@pytest.mark.parametrize("raw,expected", DECIMAL_CASES)
def test_decimals_survive_the_separator_fix(raw: str, expected: int) -> None:
    assert money_to_cop(raw) == expected


def test_decimal_and_thousands_separator_are_distinguishable() -> None:
    """El discriminante de M2, ya aplicado.

    Un punto seguido de 1-2 dígitos es marca decimal; seguido de exactamente 3
    es agrupación de miles. En M1 este test documentaba el bug (ambos valían
    1.500.000); M2 lo cerró y aquí queda fijado el comportamiento correcto.
    Es el ÚNICO invariante de M1 cuya expectativa cambia, porque su objeto es
    exactamente la ambigüedad que M2 resuelve.
    """
    assert money_to_cop("1.5 millones") == 1_500_000
    # Mismo prefijo, tres dígitos: ahora agrupa miles, mil veces más.
    assert money_to_cop("1.500 millones") == 1_500_000_000
    assert money_to_cop("1.500 millones") == 1000 * money_to_cop("1.5 millones")


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 3 — invariante billón. La regresión histórica que NO puede reaparecer.
# ══════════════════════════════════════════════════════════════════════════

def test_billion_is_exactly_one_thousand_times_mil_millones() -> None:
    """Invariante explícito pedido por M0.

    ``tests/test_money.py`` fija ambos lados con literales absolutos; este test
    fija la RELACIÓN, que es lo que un tokenizador verbal nuevo (M4) podría
    romper si capturase "mil" antes que la rama literal "mil millones".
    """
    assert money_to_cop("1 billón") == 1000 * money_to_cop("mil millones")


def test_billion_scale_anchors() -> None:
    assert money_to_cop("mil millones") == 1_000_000_000
    assert money_to_cop("1 billón") == 1_000_000_000_000
    assert money_to_cop("1 billón") != money_to_cop("mil millones")


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 4 — polaridad: un representante por CLASE, no 13 paráfrasis.
# La cobertura exhaustiva vive en tests/test_value_polarity.py.
# ══════════════════════════════════════════════════════════════════════════

POLARITY_CLASSES = [
    # (query, campo esperado, valor, campo que debe quedar vacío)
    ("contratos de alimentacion de mas de 500 millones", "valor_min", 500_000_000, "valor_max"),
    ("contratos de alimentacion de menos de 500 millones", "valor_max", 500_000_000, "valor_min"),
    ("contratos de alimentacion que no superen 500 millones", "valor_max", 500_000_000, "valor_min"),
    ("contratos de alimentacion que no sean menores a 500 millones", "valor_min", 500_000_000, "valor_max"),
]


@pytest.mark.parametrize("query,field,value,empty_field", POLARITY_CLASSES)
def test_polarity_classes_must_not_regress(
    query: str, field: str, value: int, empty_field: str
) -> None:
    params = _params(query)
    assert params.get(field) == value
    assert params.get(empty_field) is None


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 5 — rangos numéricos sanos (sin separador de millares).
# ══════════════════════════════════════════════════════════════════════════

HEALTHY_RANGES = [
    ("contratos de alimentacion entre 1000 y 3000 millones", 1_000_000_000, 3_000_000_000),
    ("contratos de alimentacion de 1000 a 3000 millones", 1_000_000_000, 3_000_000_000),
    ("contratos de alimentacion 1000-3000 millones", 1_000_000_000, 3_000_000_000),
    # Herencia de unidad entre lados (DOGFOOD-RANGE-005): la maquinaria de
    # rangos es correcta y M2 no debe tocarla.
    ("contratos de alimentacion entre 500 y mil millones", 500_000_000, 1_000_000_000),
    ("contratos de alimentacion entre 1 y 2 mil millones", 1_000_000_000, 2_000_000_000),
]


@pytest.mark.parametrize("query,vmin,vmax", HEALTHY_RANGES)
def test_healthy_ranges_must_not_regress(query: str, vmin: int, vmax: int) -> None:
    params = _params(query)
    assert params.get("valor_min") == vmin
    assert params.get("valor_max") == vmax


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 6 — dataset semantics. M0 declaró H4 FALSE POSITIVE: esto ya es
# correcto y se congela para que Money V2 no lo toque.
# ══════════════════════════════════════════════════════════════════════════

def test_procesos_filters_on_precio_base() -> None:
    query = "procesos de alimentacion de mas de 2000 millones"
    assert _params(query)["dataset"] == "procesos"
    assert _money_predicates(query) == ["precio_base >= 2000000000"]


def test_contratos_filters_on_valor_del_contrato() -> None:
    query = "contratos de alimentacion de mas de 2000 millones"
    assert _params(query)["dataset"] == "contratos"
    assert _money_predicates(query) == ["valor_del_contrato >= 2000000000"]


def test_valor_min_max_are_dataset_agnostic_abstractions() -> None:
    """El mismo umbral produce params idénticos y columnas distintas."""
    procesos = _params("procesos de alimentacion de mas de 2000 millones")
    contratos = _params("contratos de alimentacion de mas de 2000 millones")
    assert procesos["valor_min"] == contratos["valor_min"] == 2_000_000_000
    assert SoQLBuilder.SPECS[SoQLBuilder.PROCESOS_DATASET].value == "precio_base"
    assert SoQLBuilder.SPECS[SoQLBuilder.CONTRATOS_DATASET].value == "valor_del_contrato"


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 7 — anti-confusión que HOY funciona. Es la parte de la discriminación
# NIT/dinero que M3 debe preservar mientras arregla el resto.
# ══════════════════════════════════════════════════════════════════════════

def test_nit_with_dv_yields_both_variants_and_no_money_filter() -> None:
    """NIT con dígito de verificación: hoy discrimina correctamente."""
    params = _params("contratos de alimentacion del nit 900.123.456-7")
    assert params.get("contratista") == ["900123456", "9001234567"]
    assert params.get("valor_min") is None
    assert params.get("valor_max") is None


def test_amount_with_unit_is_never_read_as_nit() -> None:
    """El lookahead anti-monto de _NIT_SHAPE_RE funciona y debe preservarse."""
    params = _params("contratos de alimentacion de mas de 900 millones")
    assert params.get("valor_min") == 900_000_000
    assert params.get("contratista") is None


def test_small_reference_numbers_do_not_become_money() -> None:
    for query in ("contratos de alimentacion contrato 5533", "proceso 338-2026 de alimentacion"):
        params = _params(query)
        assert params.get("valor_min") is None, query
        assert params.get("valor_max") is None, query


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 8 — XFAIL: puntuación colombiana (H1 CONFIRMED).
# El punto de millares se lee como separador decimal → error 1000x.
# ══════════════════════════════════════════════════════════════════════════

COLOMBIAN_SEPARATOR_CASES = [
    ("1.500 millones", 1_500_000_000),
    ("1.050 millones", 1_050_000_000),
    ("2.500 millones", 2_500_000_000),
    ("10.500 millones", 10_500_000_000),
    ("1.500.000", 1_500_000),
    ("1,500,000", 1_500_000),
    ("$1.500.000", 1_500_000),
    ("$1.500.000.000", 1_500_000_000),
    ("2.350.000.000 pesos", 2_350_000_000),
]


@pytest.mark.parametrize("raw,expected", COLOMBIAN_SEPARATOR_CASES)
def test_colombian_thousands_separator(raw: str, expected: int) -> None:
    assert money_to_cop(raw) == expected


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 9 — XFAIL: composiciones verbales (H2 CONFIRMED).
# money_to_cop lee UN solo número, así que el cuantificador se pierde.
# ══════════════════════════════════════════════════════════════════════════

VERBAL_NUMBER_CASES = [
    ("quinientos millones", 500_000_000),
    ("novecientos millones", 900_000_000),
    ("mil quinientos millones", 1_500_000_000),
    ("dos mil millones", 2_000_000_000),
    ("dos mil quinientos millones", 2_500_000_000),
    ("tres mil ochocientos millones", 3_800_000_000),
    ("diez mil millones", 10_000_000_000),
    ("dos billones", 2_000_000_000_000),
]


@pytest.mark.parametrize("raw,expected", VERBAL_NUMBER_CASES)
def test_verbal_number_composition(raw: str, expected: int) -> None:
    assert money_to_cop(raw) == expected


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 10 — XFAIL: rangos contaminados por H1.
# La maquinaria de rangos es correcta; hereda el defecto de money_to_cop.
# ══════════════════════════════════════════════════════════════════════════

SEPARATOR_RANGE_CASES = [
    "contratos de alimentacion entre 1.000 y 3.000 millones",
    "contratos de alimentacion 1.000-3.000 millones",
]


@pytest.mark.parametrize("query", SEPARATOR_RANGE_CASES)
def test_ranges_with_thousands_separator(query: str) -> None:
    params = _params(query)
    assert params.get("valor_min") == 1_000_000_000
    assert params.get("valor_max") == 3_000_000_000


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 11 — XFAIL: contratos end-to-end de los bugs peligrosos.
#
# No basta con money_to_cop: estos casos se prueban en QueryRouter.parse() y,
# donde corresponde, en la proyección al WHERE real. Son los cuatro contratos
# que M2/M3/M4 tienen que satisfacer.
# ══════════════════════════════════════════════════════════════════════════

def test_nit_query_must_not_produce_money_filter() -> None:
    query = "contratos de alimentacion del NIT 900123456"
    params = _params(query)
    # El filtro de contratista SÍ es correcto hoy; lo que sobra es el de valor.
    assert params.get("contratista") == ["900123456"]
    assert params.get("valor_min") is None
    assert params.get("valor_max") is None


def test_nit_query_must_not_project_money_predicate_to_where() -> None:
    assert _money_predicates("contratos de alimentacion del NIT 900123456") == []


def test_thousands_separator_reaches_where_with_correct_scale() -> None:
    query = "contratos de alimentacion de mas de 1.500 millones"
    assert _params(query).get("valor_min") == 1_500_000_000
    assert _money_predicates(query) == ["valor_del_contrato >= 1500000000"]


def test_verbal_amount_must_keep_min_polarity() -> None:
    query = "contratos de alimentacion de mas de dos mil millones"
    params = _params(query)
    assert params.get("valor_min") == 2_000_000_000
    assert params.get("valor_max") is None


@pytest.mark.parametrize(
    "query",
    [
        "contratos de alimentacion de mas de $1.500.000.000",
        "contratos de alimentacion por $1.500.000.000",
    ],
)
def test_currency_amount_is_money_never_nit(query: str) -> None:
    params = _params(query)
    assert params.get("contratista") is None
    assert params.get("valor_min") is not None or params.get("valor_max") is not None


def test_exact_peso_amount_produces_a_filter() -> None:
    query = "contratos de alimentacion de mas de 2.350.000.000 pesos"
    assert _params(query).get("valor_min") == 2_350_000_000


# ══════════════════════════════════════════════════════════════════════════
# BUCKET 12 — XFAIL: SMMLV (H3 CONFIRMED).
#
# El contrato congelado aquí NO es "soportar SMMLV" (eso es M6, opcional). Es
# el más básico: no inventar un filtro. Hoy "mil salarios mínimos" dispara
# amount=1000 por la rama `\bmil\b` y multiplier=1000 por `elif "mil" in text`,
# produciendo un valor_min=1_000_000 que el usuario nunca pidió.
# La aserción está escrita contra el VALOR ESPURIO concreto, no contra None,
# para que siga siendo correcta si M6 llega a resolver SMMLV de verdad.
# ══════════════════════════════════════════════════════════════════════════

_SPURIOUS_SMMLV_FILTER = 1_000_000


def test_smmlv_must_not_invent_a_money_filter() -> None:
    params = _params("contratos de alimentacion de mas de mil salarios minimos")
    assert params.get("valor_min") != _SPURIOUS_SMMLV_FILTER


def test_smmlv_phrase_is_not_a_peso_amount() -> None:
    assert money_to_cop("mil salarios minimos") != 1_000_000
