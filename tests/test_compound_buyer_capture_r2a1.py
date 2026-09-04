"""R2A.1 — un nombre institucional no se corta por su propia juntura.

``de`` y ``del`` articulan casi todo nombre propio colombiano: Norte **de**
Santander, Valle **del** Cauca, Villa **de** Leyva, San Juan **de** Pasto,
Universidad Nacional **de** Colombia, Hospital San Rafael **de** Fusagasugá.
Las siete familias de ``ENTITY_REWRITES`` los usaban como frontera dura de
captura, así que la gramática partía el nombre justo donde se articula.

El daño tenía dos formas, y la segunda era la peligrosa:

* **contaminación** — "gobernación de Norte de Santander" dejaba "de Santander"
  suelto y el scan territorial lo leía como el departamento de Santander;
* **comprador equivocado y silencioso** — "municipio de Villa de Leyva" se
  encogía a "Villa" y el fuzzy acreditaba a VILLAVICENCIO; "alcaldía de San
  Juan de Pasto" acababa en MUNICIPIO DE SAN JOSE DE CUCUTA.

La corrección tiene tres piezas, y cada clase de abajo fija una:

1. ``CAPTURE_BOUNDARY`` — autoridad única, sin ``de|del``;
2. fragmento de nombre compuesto ⇒ sólo se acredita por resolución
   determinista, nunca por parecido;
3. dentro de un mismo span, un exacto le gana a un fuzzy anterior.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_AUDITS_DIR = Path(__file__).resolve().parent.parent / "scripts" / "audits"
if str(_AUDITS_DIR) not in sys.path:
    sys.path.insert(0, str(_AUDITS_DIR))

from app.core.entity_resolver import EntityResolver, is_department_root_name
from app.core.entity_types import (
    CAPTURE_BOUNDARY,
    ENTITY_REWRITES,
    is_name_connector,
    is_sectoral_name,
    name_tokens,
    rewrite_entity,
)
from app.core.query_router import QueryRouter

ALIASES_DB = Path("app/data/aliases_db.json")

NORTE_DE_SANTANDER = "Gobernación Norte de Santander*"
UNAL = "UNIVERSIDAD NACIONAL DE COLOMBIA"
FUSAGASUGA = "ESE HOSPITAL SAN RAFAEL DE FUSAGASUGÁ"
FACATATIVA = "EMPRESA SOCIAL DEL ESTADO HOSPITAL SAN RAFAEL DE FACATATIVA"
VILLAVICENCIO = "ALCALDIA MUNICIPIO DE VILLAVICENCIO"
CUCUTA = "MUNICIPIO DE SAN JOSE DE CUCUTA"


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


@pytest.fixture(scope="module")
def router(resolver: EntityResolver) -> QueryRouter:
    return QueryRouter(entity_resolver=resolver)


def _spans(query: str) -> list[str]:
    seen: list[str] = []
    for candidate in rewrite_entity(query):
        if candidate.consumed_span not in seen:
            seen.append(candidate.consumed_span)
    return seen


# ── 1. La autoridad compartida ───────────────────────────────────────────

class TestSharedCaptureBoundary:
    def test_every_family_uses_the_same_boundary(self) -> None:
        """Siete regex, un solo límite: el defecto no puede volver por una."""
        for pattern, _replacer in ENTITY_REWRITES:
            assert pattern.pattern.endswith(CAPTURE_BOUNDARY), pattern.pattern
        assert len(ENTITY_REWRITES) == 7

    def test_the_boundary_alternation_has_no_connector(self) -> None:
        """Los conectores siguen dentro del nombre, sin acoplarse al regex."""
        for connector in ("de", "del"):
            query = f"municipio de villa {connector} campo"
            assert _spans(query)[0] == query

    @pytest.mark.parametrize("connector", ["de", "del", "De", "DEL", "dél"])
    def test_connectors_are_recognized_as_name_articulation(self, connector: str) -> None:
        assert is_name_connector(connector)

    @pytest.mark.parametrize("word", ["para", "donde", "por", "con", "en", "sobre"])
    def test_clause_openers_still_cut_the_capture(self, word: str) -> None:
        """Conducta, no inspección: el conector de cláusula termina el nombre."""
        spans = _spans(f"contratos del municipio de Envigado {word} algo")
        assert spans[0] == "municipio de Envigado"

    def test_registry_noise_does_not_hide_a_token(self) -> None:
        """"ANTIOQUIA//" y "ATLANTICO**" siguen siendo esas palabras."""
        assert name_tokens("DEPARTAMENTO DE ANTIOQUIA//") == ["departamento", "de", "antioquia"]
        assert name_tokens("DEPARTAMENTO DEL ATLANTICO**") == ["departamento", "del", "atlantico"]
        assert name_tokens("MUNICIPIO DE SOACHA..") == ["municipio", "de", "soacha"]

    def test_noise_glued_inside_a_token_still_splits(self) -> None:
        """El ruido de SECOP no siempre está en el borde.

        "ALCALDIA LOCAL DE TEUSAQUILLO(oficial)" pega el paréntesis SIN espacio.
        Recortando extremos, "teusaquillo(oficial" quedaba como un solo token y
        el nombre se volvía inalcanzable — lo detectó el piso de precisión de
        ``test_rated_eval`` (RATED-040), no este archivo.
        """
        assert name_tokens("ALCALDIA LOCAL DE TEUSAQUILLO(oficial)") == [
            "alcaldia", "local", "de", "teusaquillo", "oficial",
        ]

    def test_teusaquillo_stays_reachable(self, router: QueryRouter) -> None:
        params = router.parse("procesos de alcaldia de teusaquillo sobre vias").params
        assert params.get("entidad_resolved") == "ALCALDIA LOCAL DE TEUSAQUILLO(oficial)"
        assert "vias" in (params.get("objeto") or [])


# ── 2. La captura completa ───────────────────────────────────────────────

class TestCompoundNamesAreCapturedWhole:
    @pytest.mark.parametrize(
        ("query", "expected_span"),
        [
            ("contratos de la gobernación de Norte de Santander en 2025",
             "gobernación de Norte de Santander"),
            ("contratos de la gobernación del Valle del Cauca en 2025",
             "gobernación del Valle del Cauca"),
            ("contratos del municipio de Villa de Leyva", "municipio de Villa de Leyva"),
            ("contratos de la alcaldía de Villa de Leyva", "alcaldía de Villa de Leyva"),
            ("contratos de la universidad Nacional de Colombia",
             "la universidad Nacional de Colombia"),
            ("contratos del municipio de San Andrés de Sotavento",
             "municipio de San Andrés de Sotavento"),
        ],
    )
    def test_longest_span_is_the_whole_name(self, query: str, expected_span: str) -> None:
        assert _spans(query)[0] == expected_span

    def test_shorter_spans_remain_available_as_fallback(self) -> None:
        """La escalera no desaparece: sólo deja de ser lo primero."""
        spans = _spans("contratos de la gobernación de Norte de Santander en 2025")
        assert spans[0] == "gobernación de Norte de Santander"
        assert "gobernación de Norte" in spans

    def test_a_prefix_never_ends_in_a_connector(self) -> None:
        """Un nombre no acaba en "de": ese candidato no se propone."""
        for span in _spans("contratos de la gobernación de Antioquia de 2025"):
            assert not is_name_connector(span.split()[-1])

    def test_connector_cuts_survive_beyond_the_shrink_budget(self) -> None:
        """El corte útil puede estar más lejos que ``_MAX_SHRINK_LEVELS``."""
        spans = _spans("contratos de la alcaldía de Medellín de obras de infraestructura de vías urbanas")
        assert "alcaldía de Medellín" in spans


# ── 3. Un fragmento no se acredita por parecido ──────────────────────────

class TestFragmentsNeedDeterministicEvidence:
    @pytest.mark.parametrize(
        ("query", "forbidden"),
        [
            ("contratos del municipio de Villa de Leyva", VILLAVICENCIO),
            ("contratos de la alcaldía de Villa de Leyva", VILLAVICENCIO),
            ("contratos del municipio de San Andrés de Sotavento", CUCUTA),
            ("contratos de la alcaldía de San Juan de Pasto", CUCUTA),
        ],
    )
    def test_absent_compound_name_fails_closed(
        self, frozen_municipal_baseline_router: QueryRouter, query: str, forbidden: str
    ) -> None:
        """Ausente del baseline congelado ⇒ precisión; el candidato tiene otro oráculo."""
        params = frozen_municipal_baseline_router.parse(query).params
        resolution = params.get("entidad_resolution") or {}
        assert params.get("entidad_resolved") is None
        assert params.get("entidad_resolved") != forbidden
        assert resolution.get("clarification_needed") is True

    def test_fragments_of_a_compound_are_marked(self) -> None:
        candidates = rewrite_entity("contratos del municipio de Villa de Leyva")
        full = [c for c in candidates if c.consumed_span == "municipio de Villa de Leyva"]
        fragment = [c for c in candidates if c.consumed_span == "municipio de Villa"]
        assert full and not any(c.requires_deterministic for c in full)
        assert fragment and all(c.requires_deterministic for c in fragment)

    def test_trailing_garbage_is_not_a_compound_fragment(self) -> None:
        """ENTITY-TRAILING-001 no queda atrapado por el guard.

        "pavimentacion" no es un conector, así que encoger la cola sigue
        permitiendo resolución por parecido, como antes de R2A.1.
        """
        candidates = rewrite_entity("la alcaldia de barranquilla pavimentacion")
        assert not any(c.requires_deterministic for c in candidates)

    def test_a_fragment_may_still_win_by_exact_alias(self, router: QueryRouter) -> None:
        """El guard prohíbe el parecido, no el fragmento.

        "Antioquia" es fragmento de la captura "Antioquia de 2025", y aun así
        resuelve: lo respalda un alias exacto.
        """
        params = router.parse("contratos de la gobernación de Antioquia de 2025").params
        assert params.get("entidad_resolved") == "DEPARTAMENTO DE ANTIOQUIA//"
        assert not params.get("departamento_resolved")

    def test_token_containment_is_by_whole_words(self, resolver: EntityResolver) -> None:
        """"villa" es prefijo de "villavicencio", no palabra suya."""
        assert resolver.resolve_entidad("Municipio de Villa").value != VILLAVICENCIO
        assert resolver.resolve_entidad("Municipio de Villavicencio").value == VILLAVICENCIO


# ── 4. Exacto le gana a fuzzy dentro del mismo span ──────────────────────

class TestExactBeatsFuzzyWithinASpan:
    def test_ese_form_order_does_not_decide_identity(self, router: QueryRouter) -> None:
        """El orden del replacer es redacción, no evidencia.

        "Empresa Social del Estado …" se prueba antes que "ESE …" y hacía
        fuzzy(95) contra el hospital de Facatativá, tapando el alias exacto del
        de Fusagasugá — que existe en el gazetteer.
        """
        params = router.parse("contratos de la ESE Hospital San Rafael de Fusagasugá").params
        assert params.get("entidad_resolved") == FUSAGASUGA
        assert params.get("entidad_resolved") != FACATATIVA


# ── 4b. Un hospital no es un departamento ────────────────────────────────

class TestSectoralNamesRejectTerritorialRoots:
    """Capturar el nombre completo destapó un error de categoría.

    Las familias hospital / ESE / universidad no tienen puerta de reescritura
    estricta, así que su fuzzy acepta cualquier vecino por encima del umbral.
    Con el nombre completo en la mano, "ESE Hospital Universitario de
    Santander" —ausente del gazetteer— pasó a puntuar alto contra
    DEPARTAMENTO DE SANTANDER: una pregunta por un hospital contestada con el
    comprador raíz del territorio. Ninguna respuesta era correcta, pero ésa es
    peor en clase, y la introdujo la captura completa.
    """

    def test_a_hospital_query_never_answers_with_a_department(
        self, router: QueryRouter
    ) -> None:
        params = router.parse("contratos de la ESE Hospital Universitario de Santander").params
        resolved = params.get("entidad_resolved")
        assert resolved != "DEPARTAMENTO DE SANTANDER"
        assert resolved is None or not is_department_root_name(resolved)

    @pytest.mark.parametrize(
        "canonical",
        ["Hospital San Rafael", "ESE Metrosalud", "Empresa Social del Estado X",
         "Universidad del Valle"],
    )
    def test_sectoral_canonical_forms_are_marked(self, canonical: str) -> None:
        assert is_sectoral_name(canonical)

    @pytest.mark.parametrize(
        "canonical",
        ["Municipio de Soacha", "Alcaldía de Pasto", "Departamento de Santander",
         "Gobernación del Cauca"],
    )
    def test_territorial_canonical_forms_are_not_sectoral(self, canonical: str) -> None:
        assert not is_sectoral_name(canonical)

    @pytest.mark.parametrize(
        ("official", "expected"),
        [
            ("DEPARTAMENTO DE SANTANDER", True),
            ("DEPARTAMENTO DE VICHADA", True),
            ("GOBERNACIÓN DEL CAUCA", True),
            ("GOBERNACION DEL VALLE DEL CAUCA - DADI", False),
            ("ESE HOSPITAL SAN RAFAEL DE FUSAGASUGÁ", False),
            ("HOSPITAL DEPARTAMENTAL DEL CAUCA", False),
        ],
    )
    def test_department_root_detection(self, official: str, expected: bool) -> None:
        assert is_department_root_name(official) is expected

    def test_the_guard_does_not_touch_territorial_queries(
        self, router: QueryRouter
    ) -> None:
        params = router.parse("contratos de la gobernación de Santander en 2025").params
        assert params.get("entidad_resolved") == "DEPARTAMENTO DE SANTANDER"

    @pytest.mark.parametrize(
        ("query", "expected"),
        [
            ("contratos de la ESE Metrosalud", "EMPRESA SOCIAL DEL ESTADO METROSALUD"),
            ("contratos de la universidad del Valle", "UNIVERSIDAD DEL VALLE"),
            ("procesos del hospital universitario de la Samaritana",
             "HOSPITAL UNIVERSITARIO LA SAMARITANA"),
        ],
    )
    def test_legitimate_sectoral_resolutions_survive(
        self, router: QueryRouter, query: str, expected: str
    ) -> None:
        assert router.parse(query).params.get("entidad_resolved") == expected


# ── 5. Canarios de la deuda cerrada ──────────────────────────────────────

class TestNoResidualTerritory:
    def test_norte_de_santander_is_buyer_only(self, router: QueryRouter) -> None:
        params = router.parse("contratos de la gobernación de Norte de Santander en 2025").params
        resolution = params.get("entidad_resolution") or {}
        assert params.get("entidad_resolved") == NORTE_DE_SANTANDER
        assert params.get("departamento_resolved") is None
        assert resolution["metadata"].get("root_evidence") == "unique_alias_owner"

    def test_norte_de_santander_is_not_recovered_by_fuzzy(
        self, resolver: EntityResolver
    ) -> None:
        result = resolver.resolve_entidad("Departamento de Norte de Santander")
        assert result.value == NORTE_DE_SANTANDER
        assert result.method == "exact"

    def test_valle_fails_closed_on_the_complete_territory(self, router: QueryRouter) -> None:
        params = router.parse("contratos de la gobernación del Valle del Cauca en 2025").params
        resolution = params.get("entidad_resolution") or {}
        assert params.get("entidad_resolved") is None
        assert params.get("departamento_resolved") is None
        assert resolution["metadata"].get("canonical_department") == "Valle del Cauca"

    def test_universidad_nacional_keeps_its_whole_name(self, router: QueryRouter) -> None:
        params = router.parse("contratos de la universidad Nacional de Colombia").params
        assert params.get("entidad_resolved") == UNAL
        # "colombia" ya no se derrama al objeto por haber quedado fuera del span.
        assert not (params.get("objeto") or [])


# ── 6. Controles de no sobrecaptura ──────────────────────────────────────

class TestNoOverCapture:
    def test_explicit_territorial_intersection_survives(self, router: QueryRouter) -> None:
        params = router.parse("contratos de la gobernación del Cauca en Antioquia").params
        assert params.get("entidad_resolved") == "GOBERNACIÓN DEL CAUCA"
        assert params.get("departamento_resolved") == "Antioquia"

    @pytest.mark.parametrize(
        ("query", "expected_token"),
        [
            ("contratos de la alcaldía de Medellín para mantenimiento", "mantenimiento"),
            ("la alcaldia de barranquilla pavimentacion", "pavimentacion"),
        ],
    )
    def test_clause_openers_still_end_the_name(
        self, router: QueryRouter, query: str, expected_token: str
    ) -> None:
        params = router.parse(query).params
        assert params.get("entidad_resolved")
        assert expected_token in (params.get("objeto") or [])

    def test_con_does_not_trigger_on_concordia(self) -> None:
        assert _spans("contratos del municipio de Concordia")[0] == "municipio de Concordia"

    @pytest.mark.parametrize(
        "query",
        [
            "contratos del municipio de Soacha con contratos de obra",
            "contratos de la universidad de Antioquia en Bogotá",
            "alcaldia de santa marta",
            "gobernacion del atlantico",
        ],
    )
    def test_known_resolutions_are_preserved(self, router: QueryRouter, query: str) -> None:
        assert router.parse(query).params.get("entidad_resolved")

    def test_ese_ano_is_still_not_an_entity(self) -> None:
        assert rewrite_entity("los mas caros de ese año") == []


# ── 7. La métrica separa el dato de la ejecución ─────────────────────────

class TestRuntimeMethodIsObservable:
    """R2A.1 §5 — ``ROOT_FUZZY=0`` debe medirse sobre la ejecución.

    Un alias exacto correcto en el gazetteer no prueba que el runtime lo haya
    usado: Norte de Santander lo tenía y aun así resolvía por ``fuzzy(90)``
    sobre la captura truncada. El nombre de la gramática
    ("rewrite_gobernación") tapaba el método real, así que una auditoría que
    sólo mirase ``method`` habría declarado cero fuzzy con un fuzzy dentro.
    """

    def test_resolution_exposes_the_inner_method(self, router: QueryRouter) -> None:
        params = router.parse("contratos de la gobernación de Norte de Santander en 2025").params
        metadata = (params.get("entidad_resolution") or {}).get("metadata") or {}
        assert "resolution_method" in metadata
        assert not str(metadata["resolution_method"]).startswith("fuzzy")

    def test_every_governorate_row_reports_both_axes(self) -> None:
        from governorate_core_matrix import build_matrix, summarize

        rows = build_matrix()
        assert len(rows) == 32
        for row in rows:
            assert "supporting_alias" in row      # propiedad del DATO
            assert "runtime_method" in row        # propiedad de la EJECUCIÓN
            assert "residual_department" in row

        summary = summarize(rows)
        assert summary["RUNTIME_FUZZY"] == 0
        assert summary["RESIDUAL_TERRITORY_ERRORS"] == 0
        # Hoy coinciden; el test existe porque NO son el mismo número.
        assert summary["RUNTIME_EXACT"] == summary["SUPPORTING_ALIAS_COUNT"] == summary["PASS"]
