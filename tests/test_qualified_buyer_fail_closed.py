"""R1A — un comprador nombrado explícitamente falla CERRADO si no existe.

INV-NO-SILENT-SEMANTIC-FALLBACK (ADR-069) aplicado al comprador.

"contratos de la gobernación del Vichada" pide UN COMPRADOR. Si esa entidad no
está en el gazetteer, las respuestas honestas son pedir precisión o no
responder. Devolver *todos los contratos del departamento del Vichada* es
contestar otra pregunta con cara de haber contestado la que se hizo.

El defecto era gramatical y estaba repartido en tres módulos que reconocían
"gobernación DE x" pero no "gobernación DEL x":

    app/core/entity_types.py        ENTITY_REWRITES
    app/core/entity_resolver.py     _strict_rewrite_target
    app/core/query_plan.py          marcadores de entidad de P9

Con "del" no había reescritura de entidad, así que el span sobrevivía hasta el
scan de departamento y se convertía en territorio. ``generic_buyer.py`` —la
autoridad sobre compradores genéricos— ya trataba "gobernación del Atlántico"
como comprador calificado: la asimetría era de los otros tres.

Contrato de fail-closed (el que consume ``workflow/actions/planning.py``):

    params["entidad"]                          presente  → hubo mención
    params["entidad_resolved"]                 AUSENTE   → no se inventó
    entidad_resolution.clarification_needed    True      → se pide precisión
    params["departamento_resolved"]            AUSENTE   → no se fabricó territorio

R1A arregla la HONESTIDAD, no la cobertura: estas gobernaciones siguen sin
estar en el gazetteer y deben seguir sin resolver.
"""
from __future__ import annotations

import pytest

from app.core.query_router import QueryRouter
from app.core.query_plan import compute_query_plan

# Gobernaciones sin comprador raíz en SECOP II. Vichada, Guaviare y Vaupés
# salieron de esta lista en R2A —se verificó su registro y se movieron abajo,
# como este archivo prescribía—. Guainía se queda: su Gobernación sólo existe
# en SECOP I, con nit "NO DEFINIDO" (GOVERNORATE_CORE_R2A §4).
MISSING_GOVERNORSHIPS = ["Guainía"]

# Gobernaciones que hoy resuelven a un comprador real. Cubren los dos estilos
# de nombre en SECOP documentados en ADR-040 —el que NO se llama "gobernación"
# (Atlántico) y el que sí (Nariño)— y los tres roots que R2A verificó contra
# el registro, incluido Vichada, cuyo nombre físico tampoco dice "gobernación".
KNOWN_GOVERNORSHIPS = [
    ("Atlántico", "DEPARTAMENTO DEL ATLANTICO**"),
    ("Nariño", "GOBERNACION DE NARIÑO"),
    ("Vichada", "DEPARTAMENTO DE VICHADA"),
    ("Guaviare", "GOBERNACION DEL GUAVIARE"),
    ("Vaupés", "GOBERNACION DEL VAUPES"),
]


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter()


def _parse(router: QueryRouter, query: str) -> dict:
    return router.parse(query).params


def _assert_buyer_fail_closed(router: QueryRouter, params: dict, query: str) -> None:
    """El contrato completo de fail-closed para un comprador inexistente."""
    resolution = params.get("entidad_resolution") or {}

    # 1. La mención de comprador se reconoció.
    assert params.get("entidad"), (
        f"{query!r}: no se reconoció mención de comprador — "
        f"params={ {k: v for k, v in params.items() if 'entidad' in k} }"
    )

    # 2. No se inventó una entidad.
    assert not params.get("entidad_resolved"), (
        f"{query!r}: resolvió a {params.get('entidad_resolved')!r}, "
        "pero esa gobernación no está en el gazetteer"
    )

    # 3. Se pide precisión en vez de adivinar.
    assert resolution.get("clarification_needed") is True, (
        f"{query!r}: no pide clarificación — resolution={resolution!r}"
    )

    # 4. EL PUNTO DE R1A: no se fabricó un filtro territorial.
    assert not params.get("departamento_resolved"), (
        f"{query!r}: degradó a territorio "
        f"(departamento_resolved={params.get('departamento_resolved')!r}). "
        "Se pidió un COMPRADOR; devolver el departamento entero es responder "
        "otra pregunta — INV-NO-SILENT-SEMANTIC-FALLBACK."
    )
    assert not params.get("entidad_like"), (
        f"{query!r}: cayó a LIKE en vez de fallar cerrado"
    )

    # 5. El pipeline corta: es la vía que usa workflow/actions/planning.py
    #    (`entidad` sin `entidad_resolved` → resolve_entidad → clarification).
    downstream = router.entity_resolver.resolve_entidad(str(params["entidad"]))
    assert downstream.value is None
    assert downstream.clarification_needed is True, (
        f"{query!r}: planning.py no cortaría — resolve_entidad devolvió "
        f"method={downstream.method!r}"
    )


# ── Casos 1 y 2: gobernación ausente, formas "del" y "de" ────────────────

@pytest.mark.parametrize("departamento", MISSING_GOVERNORSHIPS)
def test_missing_governorship_del_fails_closed(router, departamento):
    """Caso 1 — "gobernación DEL X": la forma que degradaba a territorio."""
    query = f"contratos de la gobernación del {departamento} en 2025"
    _assert_buyer_fail_closed(router, _parse(router, query), query)


@pytest.mark.parametrize("departamento", MISSING_GOVERNORSHIPS)
def test_missing_governorship_de_fails_closed(router, departamento):
    """Caso 2 — "gobernación DE X" debe comportarse igual."""
    query = f"contratos de la gobernación de {departamento} en 2025"
    _assert_buyer_fail_closed(router, _parse(router, query), query)


@pytest.mark.parametrize("departamento", ["Guainía", "Vichada"])
def test_governorship_both_forms_agree(router, departamento):
    """de/del son la misma pregunta: no pueden dar respuestas distintas.

    Este es el invariante que el bug violaba de forma más visible: "de
    Vichada" pedía clarificación y "del Vichada" devolvía el departamento.
    Se prueba sobre una jurisdicción sin comprador raíz (Guainía) y sobre una
    que R2A pobló (Vichada): la simetría debe valer resuelva o no.
    """
    con_del = _parse(router, f"contratos de la gobernación del {departamento} en 2025")
    con_de = _parse(router, f"contratos de la gobernación de {departamento} en 2025")
    for key in ("entidad_resolved", "departamento_resolved"):
        assert con_del.get(key) == con_de.get(key), (
            f"de/del divergen en {key!r}: "
            f"del={con_del.get(key)!r} vs de={con_de.get(key)!r}"
        )
    assert (con_del["entidad_resolution"].get("clarification_needed")
            is con_de["entidad_resolution"].get("clarification_needed"))


# ── Casos 3 y 4: no romper lo que ya funciona ────────────────────────────

@pytest.mark.parametrize("departamento,esperado", KNOWN_GOVERNORSHIPS)
def test_known_governorship_del_still_resolves_buyer(router, departamento, esperado):
    """Caso 3 — "gobernación del Atlántico" sigue siendo COMPRADOR."""
    query = f"contratos de la gobernación del {departamento} en 2025"
    params = _parse(router, query)
    assert params.get("entidad_resolved") == esperado
    assert (params.get("entidad_resolution") or {}).get("kind") == "buyer_entity"
    assert not params.get("departamento_resolved"), (
        f"{query!r}: el comprador se convirtió en filtro territorial"
    )


@pytest.mark.parametrize("departamento,esperado", KNOWN_GOVERNORSHIPS)
def test_known_governorship_de_still_resolves_buyer(router, departamento, esperado):
    """Caso 4 — la forma "de" tampoco se toca."""
    query = f"contratos de la gobernación de {departamento} en 2025"
    params = _parse(router, query)
    assert params.get("entidad_resolved") == esperado
    assert (params.get("entidad_resolution") or {}).get("kind") == "buyer_entity"
    assert not params.get("departamento_resolved")


# ── Casos 5 y 6: el territorio sigue siendo territorio ───────────────────

def test_territorial_del_still_territorial(router):
    """Caso 5 — "departamento DEL Vichada" es TERRITORIO, no comprador.

    Complemento imprescindible del caso 1: al enseñarle "del" al reescritor de
    entidades, la frase territorial no puede empezar a leerse como comprador.
    INV-7 (ADR-040) sigue en pie.
    """
    params = _parse(router, "contratos en el departamento del Vichada en 2025")
    assert params.get("departamento_resolved") == "Vichada"
    assert (params.get("departamento_resolution") or {}).get("kind") == "territorial_department"
    assert not params.get("entidad_resolved")


def test_territorial_de_arauca_still_territorial(router):
    """Caso 6 — "departamento de Arauca" es territorio explícito."""
    params = _parse(router, "contratos en el departamento de Arauca en 2025")
    assert params.get("departamento_resolved") == "Arauca"
    assert not params.get("entidad_resolved")


def test_territorial_de_control_department_unaffected(router):
    """Control del caso 6: un departamento SIN capital homónima sí funciona.

    Prueba que el caso 6 falla por RC1 y no porque el patrón territorial esté
    roto en general.
    """
    params = _parse(router, "contratos en el departamento de Antioquia en 2025")
    assert params.get("departamento_resolved") == "Antioquia"
    assert (params.get("departamento_resolution") or {}).get("method") == "territorial_pattern"
    assert not params.get("entidad_resolved")


# ── Caso 7: query_plan reconoce ambas formas ─────────────────────────────

def test_query_plan_entity_marker_accepts_del(router):
    """Caso 7 — P9 debe leer "gobernación del X" como mención explícita.

    P9 purga la entidad cuando hay ciudad y el texto no nombra un comprador.
    Con la lista vieja de marcadores, "gobernación del Quindío" no contaba como
    mención y la entidad legítima se purgaba por autopromoción.
    """
    query = "contratos de la gobernación del Quindío en Armenia"
    params = _parse(router, query)
    plan = compute_query_plan(params, query)
    assert "entity_autopromotion_purged" not in plan.risk_flags, (
        "P9 purgó una entidad nombrada explícitamente con la forma 'del'"
    )
    assert plan.params.get("entidad_resolved"), (
        "la entidad explícita no sobrevivió al plan"
    )


def test_query_plan_entity_marker_de_and_del_agree(router):
    """Las dos formas deben producir el mismo veredicto de P9."""
    flags = {}
    for prep in ("de", "del"):
        query = f"contratos de la gobernación {prep} Quindío en Armenia"
        params = _parse(router, query)
        plan = compute_query_plan(params, query)
        flags[prep] = "entity_autopromotion_purged" in plan.risk_flags
    assert flags["de"] == flags["del"], f"P9 diverge entre de/del: {flags}"


# ── Guardia: R1A no agrega cobertura ─────────────────────────────────────

@pytest.mark.parametrize("departamento", MISSING_GOVERNORSHIPS)
def test_r1a_does_not_add_coverage(router, departamento):
    """Las gobernaciones ausentes siguen ausentes: R1A no las agrega.

    Si este test empieza a fallar es porque alguien pobló el gazetteer — que es
    un lote de cobertura, no R1A. En ese caso mover el caso a
    KNOWN_GOVERNORSHIPS, como hizo R2A con Vichada, Guaviare y Vaupés, y sólo
    con evidencia de registro: nombre físico SECOP II y NIT.
    """
    params = _parse(router, f"contratos de la gobernación del {departamento} en 2025")
    assert params.get("entidad_resolved") is None
