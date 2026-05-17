"""Tests for query_router v2.py (gazetteer-first, production stack).

Uses entity_resolver v3 as fixture — mirrors the production combination
that achieves 95.5% accuracy on the 53-query benchmark.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# ── Load modules with spaces in filename via importlib ───────────────────────
_CORE = Path("app/core")

def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

_qr_mod = _load_module("query_router_v2", _CORE / "query_router.py")
_er_mod = _load_module("entity_resolver_canon", _CORE / "entity_resolver.py")
QueryRouter = _qr_mod.QueryRouter
EntityResolver = _er_mod.EntityResolver

_ALIAS_DB = Path("app/data/aliases_db.json")


@pytest.fixture
def router() -> QueryRouter:
    """Production-stack router: V2 router + V3 entity resolver."""
    er = EntityResolver(_ALIAS_DB)
    return QueryRouter(entity_resolver=er)


# ═════════════════════════════════════════════════════════════════════════════
# Dataset selection
# ═════════════════════════════════════════════════════════════════════════════

def test_router_selects_procesos_by_default(router: QueryRouter) -> None:
    parsed = router.parse("licitaciones en Antioquia")
    assert parsed.params["dataset"] == "procesos"


def test_router_selects_contratos_on_signal(router: QueryRouter) -> None:
    parsed = router.parse("contratos del SENA en Bogota mayores a 200 millones")
    assert parsed.params["dataset"] == "contratos"


# ═════════════════════════════════════════════════════════════════════════════
# Territorial / multi-param queries
# ═════════════════════════════════════════════════════════════════════════════

def test_router_extracts_territorial_process_query(router: QueryRouter) -> None:
    parsed = router.parse(
        "licitaciones de mantenimiento vial en Atlantico por mas de 500 millones abiertas"
    )

    assert parsed.needs_llm is False
    assert parsed.params["dataset"] == "procesos"
    assert parsed.params.get("departamento_resolved") is not None
    assert "atlántico" in parsed.params["departamento_resolved"].lower() or \
           "atlantico" in parsed.params["departamento_resolved"].lower()
    assert parsed.params["estado"] == "Abierto"
    assert parsed.params["valor_min"] == 500_000_000
    assert "mantenimiento" in parsed.params.get("objeto", [])
    assert "vial" in parsed.params.get("objeto", [])


def test_router_extracts_entity_without_department_confusion(router: QueryRouter) -> None:
    """'gobernacion del Atlantico' should resolve as a single entity, not split."""
    parsed = router.parse("procesos de la gobernacion del Atlantico")

    assert parsed.needs_llm is False
    assert parsed.params.get("entidad_resolved") is not None
    # The entity should contain both gobernacion and atlantico
    resolved = parsed.params["entidad_resolved"].lower()
    assert "atlantico" in resolved or "atlántico" in resolved


def test_router_routes_contract_query_with_entity_and_amount(router: QueryRouter) -> None:
    parsed = router.parse("contratos del SENA en Bogota mayores a 200 millones")

    assert parsed.params["dataset"] == "contratos"
    assert parsed.params.get("entidad_resolved") is not None
    assert "sena" in parsed.params["entidad_resolved"].lower()
    assert parsed.params["valor_min"] == 200_000_000
    assert parsed.params.get("objeto") in (None, [])


# ═════════════════════════════════════════════════════════════════════════════
# Stopwords / noise filtering (regression tests for the actualmente/ahora bug)
# ═════════════════════════════════════════════════════════════════════════════

def test_actualmente_not_in_object(router: QueryRouter) -> None:
    """'actualmente' is a temporal adverb, not a contractual object."""
    parsed = router.parse("procesos de alimentacion abiertos actualmente?")

    obj = parsed.params.get("objeto", [])
    assert "actualmente" not in obj
    assert "actualmente?" not in obj
    assert "alimentacion" in obj


def test_ahora_not_in_object(router: QueryRouter) -> None:
    """'ahora' is a temporal adverb, not a contractual object."""
    parsed = router.parse("hay algun proceso de mantenimiento abierto ahora?")

    obj = parsed.params.get("objeto", [])
    assert "ahora" not in obj
    assert "ahora?" not in obj
    assert "algun" not in obj
    assert "mantenimiento" in obj


def test_hoy_not_in_object(router: QueryRouter) -> None:
    parsed = router.parse("que licitaciones hay hoy abiertas de construccion?")

    obj = parsed.params.get("objeto", [])
    assert "hoy" not in obj
    assert "construccion" in obj


def test_punctuation_stripped_from_tokens(router: QueryRouter) -> None:
    """Question marks and other punctuation should not stick to tokens."""
    parsed = router.parse("licitaciones de pavimentacion?")

    obj = parsed.params.get("objeto", [])
    assert "pavimentacion?" not in obj
    if obj:
        assert "pavimentacion" in obj


# ═════════════════════════════════════════════════════════════════════════════
# Ordering signals
# ═════════════════════════════════════════════════════════════════════════════

def test_ordering_signal_extracted(router: QueryRouter) -> None:
    parsed = router.parse("contratos mas caros de la alcaldia de barranquilla en 2025")

    assert parsed.params.get("ordering_signal") == "valor_desc"
    obj = parsed.params.get("objeto", [])
    # "caros" should NOT leak into object
    assert "caros" not in obj


# ═════════════════════════════════════════════════════════════════════════════
# Date extraction
# ═════════════════════════════════════════════════════════════════════════════

def test_year_extracted_as_date_range(router: QueryRouter) -> None:
    parsed = router.parse("contratos de obra en 2024")

    assert parsed.params.get("fecha_desde") == "2024-01-01"
    assert parsed.params.get("fecha_hasta") == "2024-12-31"


@pytest.mark.parametrize(
    "query",
    [
        "contratos de obra entre 2025 y 2026",
        "contratos de obra entre el 2025 y el 2026",
        "contratos de obra de 2025 a 2026",
        "contratos de obra desde 2025 hasta 2026",
        "contratos de obra 2025 y 2026",
        "contratos de obra 2025-2026",
        "contratos de obra 2025 a 2026",
    ],
)
def test_two_year_range_extracted_as_full_year_bounds(router: QueryRouter, query: str) -> None:
    parsed = router.parse(query)

    assert parsed.params.get("fecha_desde") == "2025-01-01"
    assert parsed.params.get("fecha_hasta") == "2026-12-31"


def test_reversed_two_year_range_is_normalized(router: QueryRouter) -> None:
    parsed = router.parse("contratos de obra entre 2026 y 2025")

    assert parsed.params.get("fecha_desde") == "2025-01-01"
    assert parsed.params.get("fecha_hasta") == "2026-12-31"


# ═════════════════════════════════════════════════════════════════════════════
# Month stopwords & month+year date extraction
# ═════════════════════════════════════════════════════════════════════════════

def test_months_not_in_object(router: QueryRouter) -> None:
    """'abril' is a month name, not a contractual object."""
    parsed = router.parse("contratos de abril 2026")

    obj = parsed.params.get("objeto", [])
    assert "abril" not in obj


def test_month_year_extraction(router: QueryRouter) -> None:
    """'abril de 2026' → fecha_desde=2026-04-01, fecha_hasta=2026-04-30."""
    parsed = router.parse("contratos de abril de 2026")

    assert parsed.params.get("fecha_desde") == "2026-04-01"
    assert parsed.params.get("fecha_hasta") == "2026-04-30"


def test_month_year_del_mes(router: QueryRouter) -> None:
    """'del mes de enero de 2025' → fecha_desde=2025-01-01, fecha_hasta=2025-01-31."""
    parsed = router.parse("del mes de enero de 2025")

    assert parsed.params.get("fecha_desde") == "2025-01-01"
    assert parsed.params.get("fecha_hasta") == "2025-01-31"


# ═════════════════════════════════════════════════════════════════════════════
# Regression: gobernacion de santander 2024 → SANTANDER, not CALDAS
# ═════════════════════════════════════════════════════════════════════════════

def test_gobernacion_de_santander_2024_resolves_correctly(router: QueryRouter) -> None:
    """'gobernacion de santander 2024' must resolve to SANTANDER, not CALDAS.

    Regression for bug where year stripping + department-first extraction caused
    'gobernacion de santander' to be split, leaving only 'gobernacion' which
    fuzzy-matched to GOBERNACION DE CALDAS.
    """
    parsed = router.parse("gobernacion de santander 2024")

    resolved = parsed.params.get("entidad_resolved", "")
    assert "SANTANDER" in resolved.upper(), (
        f"Expected SANTANDER in entity, got {resolved!r}"
    )
    assert "CALDAS" not in resolved.upper(), (
        f"Must NOT resolve to CALDAS, got {resolved!r}"
    )
    assert parsed.params.get("fecha_desde") == "2024-01-01"
    assert parsed.params.get("fecha_hasta") == "2024-12-31"


def test_contratos_mas_caros_gobernacion_de_santander_2024(router: QueryRouter) -> None:
    """Full phrase: 'contratos mas caros de la gobernacion de santander 2024'."""
    parsed = router.parse("contratos mas caros de la gobernacion de santander 2024")

    resolved = parsed.params.get("entidad_resolved", "")
    assert "SANTANDER" in resolved.upper(), (
        f"Expected SANTANDER in entity, got {resolved!r}"
    )
    assert "CALDAS" not in resolved.upper(), (
        f"Must NOT resolve to CALDAS, got {resolved!r}"
    )
    assert parsed.params["dataset"] == "contratos"
    assert parsed.params.get("ordering_signal") == "valor_desc"
    assert parsed.params.get("fecha_desde") == "2024-01-01"


# ═════════════════════════════════════════════════════════════════════════════
# LLM trigger (_needs_llm)
# ═════════════════════════════════════════════════════════════════════════════

def test_needs_llm_when_city_in_object(router: QueryRouter) -> None:
    """'en Puerto Salgar' should trigger LLM — city not resolved as department."""
    result = router.parse("procesos de ampliacion en puerto salgar")
    assert result.needs_llm is True
    assert result.route_reason == "heuristic_plus_llm"
    assert "ampliacion" in result.params.get("objeto", [])


def test_needs_llm_entity_like_words_in_object(router: QueryRouter) -> None:
    """Entity-like words NOT in gazetteer should trigger LLM."""
    # "secretaria de integracion social" IS in gazetteer, so use something that isn't
    result = router.parse("contratos de la secretaria de asuntos especiales de bogota")
    assert result.needs_llm is True


def test_no_llm_when_department_resolved(router: QueryRouter) -> None:
    """'en Atlantico' resolves to department — no LLM needed."""
    result = router.parse("procesos de mantenimiento vial en atlantico")
    assert result.needs_llm is False
    assert result.params.get("departamento_resolved") is not None


def test_no_llm_simple_object_query(router: QueryRouter) -> None:
    """Simple object search should not trigger LLM."""
    result = router.parse("licitaciones de mantenimiento vial")
    assert result.needs_llm is False


# ═════════════════════════════════════════════════════════════════════════════
# NIT detection (shape-based)
# ═════════════════════════════════════════════════════════════════════════════

def test_nit_with_dots_and_dv(router: QueryRouter) -> None:
    """NIT con puntos y DV — debe extraer base y completo."""
    result = router.parse("contratos con edubar nit 800.091.140-4")
    contratista = result.params.get("contratista")
    assert contratista == ["800091140", "8000911404"], f"got {contratista}"
    # El NIT crudo y la palabra 'nit' no deben estar en objeto
    objeto = result.params.get("objeto", [])
    assert "800091140" not in objeto
    assert "nit" not in objeto
    # Nota: "edubar" puede quedar como término de objeto (nombre de empresa no en gazetteer)
    # — esto no es un bug, puede refinar resultados por nombre de proveedor


def test_nit_solo_sin_puntuacion(router: QueryRouter) -> None:
    """NIT solo, sin puntuación ni contexto."""
    result = router.parse("800091140")
    assert result.params.get("contratista") == ["800091140"]


def test_nit_con_puntos_sin_dv(router: QueryRouter) -> None:
    """NIT con puntos pero sin DV."""
    result = router.parse("procesos del 900.123.456")
    assert result.params.get("contratista") == ["900123456"]


def test_monto_no_confundido_con_nit(router: QueryRouter) -> None:
    """'2000 millones' NO debe ser detectado como NIT."""
    result = router.parse("contratos por mas de 2000 millones")
    assert result.params.get("contratista") is None
    assert result.params.get("valor_min") == 2_000_000_000


def test_anno_no_confundido_con_nit(router: QueryRouter) -> None:
    """Año de 4 dígitos no debe ser detectado como NIT (menos de 8 dígitos)."""
    result = router.parse("contratos del 2023")
    assert result.params.get("contratista") is None
    assert result.params.get("fecha_desde") == "2023-01-01"


def test_nit_con_departamento(router: QueryRouter) -> None:
    """NIT + departamento — ambos deben resolverse."""
    result = router.parse("proveedor 800091140 en atlantico")
    contratista = result.params.get("contratista")
    assert contratista is not None
    assert "800091140" in contratista
    assert result.params.get("departamento_resolved") is not None


# ═════════════════════════════════════════════════════════════════════════════
# Semantic normalization layer tests
# ═════════════════════════════════════════════════════════════════════════════

def test_publicados_en_departamento_no_leak(router: QueryRouter) -> None:
    """'publicados' y 'departamento' NO deben aparecer en objeto."""
    result = router.parse("procesos publicados en el departamento del choco")
    # Estado: oferta_abierta con IN de varios estados
    assert result.params.get("estado_family") == "oferta_abierta"
    assert result.params.get("estado_del_procedimiento") is not None
    assert "Publicado" in result.params["estado_del_procedimiento"]
    # Departamento debe resolverse
    assert result.params.get("departamento_resolved") is not None
    assert "choco" in result.params["departamento_resolved"].lower() or \
           "chocó" in result.params["departamento_resolved"].lower()
    # Objeto NO debe contener "publicados" ni "departamento"
    objeto = [t.lower() for t in result.params.get("objeto", [])]
    assert "publicados" not in objeto
    assert "departamento" not in objeto
    assert "departamento" not in [t.lower() for t in (result.params.get("objeto") or [])]


def test_alcaldia_de_manizales_para_presentarme(router: QueryRouter) -> None:
    """Entidad rewrite + intent deben resolverse."""
    result = router.parse("que tiene la alcaldia de manizales para presentarme")
    # Entidad debe resolverse via rewrite
    entidad = result.params.get("entidad_resolved")
    assert entidad is not None
    assert "manizales" in entidad.lower()
    # Estado: oferta_abierta via intent
    assert result.params.get("estado_family") == "oferta_abierta"
    # Objeto debe ser vacío (todo fue consumido por las capas semánticas)
    objeto = result.params.get("objeto", [])
    # "tiene" está en STOPWORDS, "alcaldia" + "manizales" consumido por rewrite,
    # "para presentarme" consumido por intent → objeto debería estar vacío
    assert len(objeto) <= 1  # puede quedar algo residual, pero mínimo


def test_contratos_firmados_edubar_atlantico(router: QueryRouter) -> None:
    """\"contratos firmados\" ya no aplica estado_family — solo force dataset."""
    result = router.parse("contratos firmados con edubar en atlantico")
    assert result.params.get("dataset") == "contratos"
    # "firmados" ya no mapea a estado_family (no existe "con_contrato")
    assert result.params.get("estado_family") is None, \
        f"expected no estado_family for firmados, got {result.params.get('estado_family')}"
    assert result.params.get("departamento_resolved") is not None
    assert "atlantico" in result.params["departamento_resolved"].lower() or \
           "atlántico" in result.params["departamento_resolved"].lower()
    objeto = [t.lower() for t in result.params.get("objeto", [])]
    assert "firmados" not in objeto


def test_convocatoria_obras_bolivar(router: QueryRouter) -> None:
    """'en convocatoria' → oferta_abierta, 'obras' → objeto."""
    result = router.parse("procesos en convocatoria de obras en bolivar")
    assert result.params.get("estado_family") == "oferta_abierta"
    assert result.params.get("departamento_resolved") is not None
    assert "bolivar" in result.params["departamento_resolved"].lower() or \
           "bolívar" in result.params["departamento_resolved"].lower()
    objeto = [t.lower() for t in result.params.get("objeto", [])]
    assert "obras" in objeto or "obra" in objeto  # "obras" puede ser singularizado
    assert "convocatoria" not in objeto


def test_municipio_manizales_menos_100_millones_no_regresion(router: QueryRouter) -> None:
    """Debe funcionar exactamente como antes con la capa semántica."""
    result = router.parse("procesos abiertos del municipio de manizales por menos de 100 millones")
    # Estado debe ser detectado (abiertos → oferta_abierta)
    assert result.params.get("estado_family") == "oferta_abierta"
    # Dataset procesos
    assert result.params.get("dataset") == "procesos"
    # Monto máximo
    assert result.params.get("valor_max") is not None
    assert result.params.get("valor_max") <= 100_000_000
    # "abiertos" NO debe estar en objeto
    objeto = [t.lower() for t in result.params.get("objeto", [])]
    assert "abiertos" not in objeto


def test_oportunidades_diplomado_abiertas(router: QueryRouter) -> None:
    """'oportunidades' es filler, 'diplomado' es objeto, 'abiertas' es estado."""
    result = router.parse("que oportunidades de diplomado hay abiertas")
    assert result.params.get("estado_family") == "oferta_abierta"
    objeto = [t.lower() for t in result.params.get("objeto", [])]
    assert "diplomado" in objeto
    assert "oportunidades" not in objeto
    assert "abiertas" not in objeto


# ═════════════════════════════════════════════════════════════════════════════
# OR semántico en objeto: "X o Y" no debe convertirse en AND
# ═════════════════════════════════════════════════════════════════════════════

def test_o_conjunction_creates_or_group(router: QueryRouter) -> None:
    result = router.parse("canchas o parques")
    assert result.params.get("objeto") == [["canchas", "parques"]]


def test_o_conjunction_with_anchor_term(router: QueryRouter) -> None:
    result = router.parse("construccion de canchas o parques")
    assert result.params.get("objeto") == ["construccion", ["canchas", "parques"]]


def test_multiple_or_terms(router: QueryRouter) -> None:
    result = router.parse("canchas o parques o coliseos")
    assert result.params.get("objeto") == [["canchas", "parques", "coliseos"]]


def test_no_or_keeps_list_of_strings(router: QueryRouter) -> None:
    result = router.parse("pavimentacion en Bolivar")
    assert result.params.get("objeto") == ["pavimentacion"]


def test_bigram_in_or_group(router: QueryRouter) -> None:
    result = router.parse("adulto mayor o primera infancia")
    assert result.params.get("objeto") == [["adulto_mayor", "primera_infancia"]]


@pytest.mark.parametrize(
    "query,expected_current,note",
    [
        (
            "canchas deportivas o parques infantiles",
            ["canchas", ["deportiva", "parques"], "infantiles"],
            "GAP-N4: frases multipalabra sin entrada en KNOWN_BIGRAMS se rompen en OR. "
            "Ideal: [['canchas_deportivas', 'parques_infantiles']]. "
            "Requiere detección de cláusulas multipalabra pre-OR.",
        ),
        (
            "mantenimiento vial o parques infantiles",
            ["mantenimiento", ["vial", "parques"], "infantiles"],
            "GAP-N4: 'mantenimiento vial' y 'parques infantiles' son unidades semánticas.",
        ),
        (
            "construccion de canchas sinteticas o parques biosaludables",
            ["construccion", "canchas", ["sinteticas", "parques"], "biosaludables"],
            "GAP-N4: tres frases multipalabra se dispersan en tokens individuales.",
        ),
        (
            "adecuacion de vias terciarias o caminos veredales",
            ["adecuacion", "vias", ["terciarias", "caminos"], "veredales"],
            "GAP-N4: frases del dominio vial no se agrupan correctamente con OR.",
        ),
    ],
)
def test_or_mixed_multitoken_terms(
    router: QueryRouter, query: str, expected_current: list, note: str
) -> None:
    """N4: OR mixto con frases multipalabra — documenta gap conocido.

    Cuando cada lado del 'o' es una frase de 2+ palabras sin entrada en
    KNOWN_BIGRAMS, el tokenizador actual no reconoce las fronteras de cláusula
    y produce grupos incorrectos. Fix diferido: requiere refactor del tokenizador
    para detectar cláusulas multipalabra alrededor de 'o'.
    """
    result = router.parse(query)
    actual = result.params.get("objeto")
    assert actual == expected_current, (
        f"{query!r}: expected {expected_current!r}, got {actual!r}. {note}"
    )


# ═════════════════════════════════════════════════════════════════════════════
# Acrónimos de programas + verbos de acción del usuario
# ═════════════════════════════════════════════════════════════════════════════

def test_pae_recognized_as_objeto(router: QueryRouter) -> None:
    result = router.parse("contratos del pae")
    assert result.params.get("objeto") == ["alimentacion_escolar"]


def test_alimentacion_escolar_bigram(router: QueryRouter) -> None:
    result = router.parse("programa de alimentacion escolar")
    assert result.params.get("objeto") == ["alimentacion_escolar"]


def test_licitar_filtered_as_user_verb(router: QueryRouter) -> None:
    result = router.parse("quiero licitar en el pae")
    assert result.params.get("objeto") == ["alimentacion_escolar"]
    assert "licitar" not in result.params.get("objeto", [])


def test_presentarme_filtered_as_user_verb(router: QueryRouter) -> None:
    result = router.parse("quiero presentarme al concurso de pavimentacion")
    objeto = result.params.get("objeto", [])
    assert objeto == ["pavimentacion"]
    assert "presentarme" not in objeto


def test_user_verb_alone_triggers_clarification(router: QueryRouter) -> None:
    result = router.parse("quiero licitar")
    assert not any(k in result.params for k in ("objeto", "entidad_resolved", "entidad_like", "departamento_resolved", "ciudad", "contratista"))


def test_cultura_variants_normalize_to_single_root(router: QueryRouter) -> None:
    result = router.parse("gobernacion de cesar en temas culturales y cultura")
    assert result.params.get("objeto") == ["cultura"]


def test_patrimonio_cultural_normalizes_to_cultura(router: QueryRouter) -> None:
    result = router.parse("procesos de patrimonio cultural")
    assert result.params.get("objeto") == ["cultura"]


def test_temas_culturales_gobernacion_cesar_normalizes_to_cultura(router: QueryRouter) -> None:
    result = router.parse("que contratos sobre temas culturales tiene la gobernacion de cesar")
    objeto = result.params.get("objeto", [])

    assert result.params.get("dataset") == "contratos"
    assert result.params.get("entidad_resolved") == "GOBERNACION DEL DEPARTAMENTO DEL CESAR"
    assert objeto == ["cultura"]
    assert "temas" not in objeto
    assert "culturales" not in objeto


def test_patrimonio_cultural_gobernacion_cesar_normalizes_to_cultura(router: QueryRouter) -> None:
    result = router.parse("contratos de patrimonio cultural de la gobernacion del cesar")
    assert result.params.get("objeto") == ["cultura"]


def test_eventos_culturales_normalizes_to_cultura(router: QueryRouter) -> None:
    result = router.parse("procesos de eventos culturales")
    assert result.params.get("objeto") == ["cultura"]


def test_actividades_artisticas_normalizes_to_cultura(router: QueryRouter) -> None:
    result = router.parse("contratos de actividades artisticas")
    assert result.params.get("objeto") == ["cultura"]


def test_temas_deportivos_drops_structural_topic_word(router: QueryRouter) -> None:
    result = router.parse("temas deportivos")
    objeto = result.params.get("objeto", [])
    assert "temas" not in objeto
