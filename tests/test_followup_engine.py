"""Tests del follow-up engine — subsistema conversacional de SECOPPAL.

Escenarios multi-turn:
A. Paginación pura: "muéstrame más" → pagination_more con offset
B. "Más" + filtro nuevo: "muéstrame más contratos de adulto mayor de 2026" → refine_filter
C. Cambio de año: "solo 2026", "ahora en 2025" → change_year
D. Cambio de orden: "los más caros" → change_order
E. Sin historial + paginación → pide contexto
F. Sin historial + "más" → unclear/new_search
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone, timedelta

from app.core.followup_engine import (
    FollowupClassifier,
    FollowupGuards,
    detect_and_merge,
    TextNormalizer,
    ConversationContext,
    ContextDetector,
)
from app.core.query_frame import QueryFrame, frame_from_params, params_from_frame


# ── Helpers ────────────────────────────────────────────────────────────────

def _make_previous_frame(params: dict) -> QueryFrame:
    """Construye un QueryFrame a partir de params, simulando turno anterior."""
    return frame_from_params(params)


# ── Test A: Paginación pura ────────────────────────────────────────────────

class TestPaginationPure:
    """'muéstrame más' sin filtros nuevos → pagination_more con offset."""

    @pytest.fixture
    def prev_query(self):
        return {
            "dataset": "contratos",
            "objeto": ["adulto_mayor"],
            "entidad_resolved": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
            "fecha_desde": "2025-01-01",
            "fecha_hasta": "2026-12-31",
            "ordering_signal": "valor_desc",
            "offset": 0,
            "limit": 10,
        }

    def test_muestrame_mas_pagination(self, prev_query):
        """'muéstrame más' → pagination_more, offset=10."""
        intent, merged = detect_and_merge(
            text="muéstrame más",
            current_params={"dataset": "procesos"},  # default, no explicit
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent == "pagination_more", f"expected pagination_more, got {intent}"
        assert merged.get("offset") == 10, f"offset should be 10, got {merged.get('offset')}"
        assert merged.get("dataset") == "contratos", "dataset should be inherited"
        assert "adulto_mayor" in str(merged.get("objeto", [])), "topic should be inherited"
        assert merged.get("ordering_signal") == "valor_desc", "ordering should be inherited"

    def test_continuar_pagination(self, prev_query):
        """'continuar' → pagination_more."""
        intent, merged = detect_and_merge(
            text="continuar",
            current_params={},
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent == "pagination_more"
        assert merged.get("offset") == 10

    def test_siguientes_pagination(self, prev_query):
        """'siguientes' → pagination_more."""
        intent, _ = detect_and_merge(
            text="siguientes",
            current_params={},
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent == "pagination_more"

    def test_sin_historial_retorna_new_search(self):
        """Sin historial → new_search, no pagination."""
        intent, merged = detect_and_merge(
            text="muéstrame más",
            current_params={},
            previous_frame=None,
        )
        assert intent == "new_search", f"expected new_search without history, got {intent}"
        # Sin historial, no debe haber offset
        assert merged.get("offset") is None or merged.get("offset") == 0, "offset should be None/0 without history"

    def test_muestrame_mas_por_favor_pagination(self, prev_query):
        """'muéstrame más por favor' → pagination_more (cortesías se limpian)."""
        intent, _ = detect_and_merge(
            text="muéstrame más por favor",
            current_params={},
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent == "pagination_more"


# ── Test B: "Más" + filtro nuevo NO es paginación ─────────────────────────

class TestMasConFiltroNuevo:
    """'muéstrame más contratos de adulto mayor de 2026' → refine_filter."""

    @pytest.fixture
    def prev_query(self):
        return {
            "dataset": "contratos",
            "objeto": ["adulto_mayor"],
            "entidad_resolved": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
            "fecha_desde": "2025-01-01",
            "fecha_hasta": "2026-12-31",
            "ordering_signal": "valor_desc",
            "offset": 0,
            "limit": 10,
        }

    def test_mas_contratos_adulto_mayor_2026(self, prev_query):
        """'muéstrame más contratos de adulto mayor de 2026' → refine_filter."""
        # El parser produce: dataset=contratos, objeto=[adulto_mayor], año=2026
        current = {
            "dataset": "contratos",
            "dataset_explicit": True,
            "objeto": ["adulto_mayor"],
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
        }
        intent, merged = detect_and_merge(
            text="muéstrame más contratos de adulto mayor de 2026",
            current_params=current,
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent != "pagination_more", f"NOT pagination_more, got {intent}"
        assert intent in ("refine_filter", "contextual_requery"), f"got {intent}"

        # Offset debe ser 0 o None
        offset = merged.get("offset", 0)
        assert offset == 0 or offset is None, f"offset should be 0/None, got {offset}"

        # Fecha debe ser 2026 (del follow-up)
        fd = merged.get("fecha_desde")
        assert fd == "2026-01-01", f"fecha_desde should be 2026, got {fd}"
        fh = merged.get("fecha_hasta")
        assert fh == "2026-12-31", f"fecha_hasta should be 2026, got {fh}"

        # Scope debe heredarse
        er = merged.get("entidad_resolved", "")
        assert "BARRANQUILLA" in str(er).upper(), f"scope not inherited: {er}"

        # Ordenamiento debe heredarse
        assert merged.get("ordering_signal") == "valor_desc", "ordering_signal not inherited"

    def test_mas_solo_2026(self, prev_query):
        """'solo 2026' → change_year."""
        current = {
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
        }
        intent, merged = detect_and_merge(
            text="solo 2026",
            current_params=current,
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent == "change_year", f"expected change_year, got {intent}"
        assert merged.get("fecha_desde") == "2026-01-01"
        # Scope y tema deben heredarse
        er = merged.get("entidad_resolved", "")
        assert "BARRANQUILLA" in str(er).upper(), "scope not inherited"
        assert "adulto_mayor" in str(merged.get("objeto", [])), "topic not inherited"

    def test_ahora_2025(self, prev_query):
        """'ahora en 2025' → change_year."""
        current = {
            "fecha_desde": "2025-01-01",
            "fecha_hasta": "2025-12-31",
        }
        intent, merged = detect_and_merge(
            text="ahora en 2025",
            current_params=current,
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent == "change_year", f"expected change_year, got {intent}"
        assert merged.get("fecha_desde") == "2025-01-01"

    def test_mas_contratos_mantenimiento(self, prev_query):
        """'muéstrame más contratos de mantenimiento' → refine_filter."""
        current = {
            "dataset": "contratos",
            "dataset_explicit": True,
            "objeto": ["mantenimiento"],
        }
        intent, merged = detect_and_merge(
            text="muéstrame más contratos de mantenimiento",
            current_params=current,
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent != "pagination_more", f"NOT pagination_more, got {intent}" ""
        offset = merged.get("offset", 0)
        assert offset == 0 or offset is None, f"offset should be 0/None, got {offset}"

    def test_no_pagination_when_filters_present(self, prev_query):
        """Verificar que is_pagination_pure rechaza frases con filtros."""
        assert FollowupClassifier.is_pagination_pure("muéstrame más") is True
        assert FollowupClassifier.is_pagination_pure("muéstrame más contratos") is False
        assert FollowupClassifier.is_pagination_pure("muéstrame más de 2026") is False
        assert FollowupClassifier.is_pagination_pure("muéstrame más en barranquilla") is False


# ── Test C: Cambio de orden ────────────────────────────────────────────────

class TestChangeOrder:
    @pytest.fixture
    def prev_query(self):
        return {
            "dataset": "contratos",
            "objeto": ["adulto_mayor"],
            "entidad_resolved": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
        }

    def test_los_mas_caros_change_order(self, prev_query):
        """'los más caros' → change_order, hereda todo, offset=0."""
        current = {"ordering_signal": "valor_desc"}
        intent, merged = detect_and_merge(
            text="los más caros",
            current_params=current,
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent == "change_order", f"expected change_order, got {intent}"
        assert merged.get("ordering_signal") == "valor_desc"
        offset = merged.get("offset", 0)
        assert offset == 0 or offset is None, f"offset should be 0/None, got {offset}"
        assert merged.get("fecha_desde") == "2026-01-01", "fecha should be inherited"
        assert "adulto_mayor" in str(merged.get("objeto", [])), "topic should be inherited"

    def test_mas_recientes_change_order(self, prev_query):
        """Cambio a más recientes → quita ordering_signal."""
        current = {"ordering_signal": "fecha_desc"}
        intent, merged = detect_and_merge(
            text="los más recientes",
            current_params=current,
            previous_frame=_make_previous_frame(prev_query),
        )
        # Con fecha heredada, intent debe ser change_order
        assert intent == "change_order", f"expected change_order, got {intent}"


# ── Test D: Cambio de scope ────────────────────────────────────────────────

class TestChangeScope:
    @pytest.fixture
    def prev_query(self):
        return {
            "dataset": "procesos",
            "objeto": ["pavimentacion"],
            "departamento_resolved": "ATLÁNTICO",
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
        }

    def test_ahora_medellin_change_scope(self, prev_query):
        """'ahora en Medellín' → change_scope."""
        current = {
            "ciudad": "Medellín",
        }
        intent, merged = detect_and_merge(
            text="ahora en Medellín",
            current_params=current,
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent == "change_scope", f"expected change_scope, got {intent}"
        assert merged.get("ciudad") == "Medellín"
        # Dataset y tema deben heredarse
        assert merged.get("dataset") == "procesos", f"dataset should be inherited, got {merged.get('dataset')}"
        assert "pavimentacion" in str(merged.get("objeto", [])), "topic should be inherited"
        offset = merged.get("offset", 0)
        assert offset == 0 or offset is None, "offset should be 0 on scope change"

    def test_ahora_bogota_not_pagination(self, prev_query):
        """'ahora en Bogotá' con pagination_like start → NO pagination."""
        intent, _ = detect_and_merge(
            text="ahora en Bogotá",
            current_params={"ciudad": "Bogotá"},
            previous_frame=_make_previous_frame(prev_query),
        )
        assert intent != "pagination_more", f"NOT pagination_more, got {intent}"
        assert intent == "change_scope", f"expected change_scope, got {intent}"


# ── Test E: Sin historial ──────────────────────────────────────────────────

class TestNoHistory:
    def test_muestrame_mas_sin_historial(self):
        """Sin historial + 'muéstrame más' → new_search."""
        intent, merged = detect_and_merge(
            text="muéstrame más",
            current_params={},
            previous_frame=None,
        )
        assert intent == "new_search", f"expected new_search, got {intent}"

    def test_continuar_sin_historial(self):
        """Sin historial + 'continuar' → new_search."""
        intent, _ = detect_and_merge(
            text="continuar",
            current_params={},
            previous_frame=None,
        )
        assert intent == "new_search"

    def test_los_mas_caros_sin_historial(self):
        """Sin historial + 'los más caros' → new_search."""
        intent, _ = detect_and_merge(
            text="los más caros",
            current_params={"ordering_signal": "valor_desc"},
            previous_frame=None,
        )
        assert intent == "new_search"


# ── Test F: Conversación completa (E2E simulado) ──────────────────────────

class TestConversationScenario:
    """Escenario completo: dos turnos, verificar clasificación correcta."""

    def test_scenario_a_adulto_mayor_pagination(self):
        """
        Turno 1: "contratos mas altos de adulto mayor del distrito de barranquilla 2025-2026"
        Turno 2: "muestrame mas"
        → pagination_more, offset=10, fecha heredada 2025-2026, valor_desc
        """
        t1_params = {
            "dataset": "contratos",
            "objeto": ["adulto_mayor"],
            "entidad_resolved": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
            "fecha_desde": "2025-01-01",
            "fecha_hasta": "2026-12-31",
            "ordering_signal": "valor_desc",
            "offset": 0,
            "limit": 10,
        }
        t1_frame = _make_previous_frame(t1_params)

        intent, merged = detect_and_merge(
            text="muestrame mas",
            current_params={"dataset": "procesos"},  # default
            previous_frame=t1_frame,
        )
        assert intent == "pagination_more", f"expected pagination_more, got {intent}"
        assert merged.get("offset") == 10, f"offset should be 10, got {merged.get('offset')}"
        assert merged.get("fecha_desde") == "2025-01-01", "fecha should be inherited"
        assert merged.get("fecha_hasta") == "2026-12-31", "fecha should be inherited"
        assert merged.get("ordering_signal") == "valor_desc", "ordering should be inherited"

    def test_scenario_b_adulto_mayor_refine(self):
        """
        Turno 1: "contratos mas altos de adulto mayor del distrito de barranquilla 2025-2026"
        Turno 2: "muestrame mas contratos de adulto mayor de 2026"
        → refine_filter, fecha 2026, offset=0, valor_desc heredado, scope heredado
        """
        t1_params = {
            "dataset": "contratos",
            "objeto": ["adulto_mayor"],
            "entidad_resolved": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
            "fecha_desde": "2025-01-01",
            "fecha_hasta": "2026-12-31",
            "ordering_signal": "valor_desc",
            "offset": 10,
            "limit": 10,
        }
        t1_frame = _make_previous_frame(t1_params)

        t2_params = {
            "dataset": "contratos",
            "dataset_explicit": True,
            "objeto": ["adulto_mayor"],
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
        }

        intent, merged = detect_and_merge(
            text="muestrame mas contratos de adulto mayor de 2026",
            current_params=t2_params,
            previous_frame=t1_frame,
        )

        # NO debe ser pagination_more
        assert intent != "pagination_more", f"NOT pagination_more, got {intent}"
        # Debe ser refine_filter o contextual_requery
        assert intent in ("refine_filter", "contextual_requery"), f"got {intent}"

        # Offset debe ser 0
        offset = merged.get("offset", 0)
        assert offset == 0 or offset is None, f"offset should be 0/None, got {offset}"

        # Fecha debe ser SOLO 2026
        assert merged.get("fecha_desde") == "2026-01-01", f"fecha_desde={merged.get('fecha_desde')}"
        assert merged.get("fecha_hasta") == "2026-12-31", f"fecha_hasta={merged.get('fecha_hasta')}"

        # Scope heredado (Barranquilla)
        er = merged.get("entidad_resolved", "")
        assert "BARRANQUILLA" in str(er).upper(), f"scope not inherited: {er}"

        # Ordenamiento heredado
        assert merged.get("ordering_signal") == "valor_desc", "ordering not inherited"

    def test_scenario_c_pae_atlantico_change_year(self):
        """
        Turno 1: "procesos de pae atlantico 2026"
        Turno 2: "ahora en 2025"
        → change_year, fecha 2025, scope heredado, topic heredado, offset=0
        """
        t1_params = {
            "dataset": "procesos",
            "objeto": ["alimentacion_escolar"],
            "departamento_resolved": "ATLÁNTICO",
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
        }
        t1_frame = _make_previous_frame(t1_params)

        t2_params = {
            "fecha_desde": "2025-01-01",
            "fecha_hasta": "2025-12-31",
        }

        intent, merged = detect_and_merge(
            text="ahora en 2025",
            current_params=t2_params,
            previous_frame=t1_frame,
        )

        assert intent == "change_year", f"expected change_year, got {intent}"
        assert merged.get("fecha_desde") == "2025-01-01"
        assert merged.get("fecha_hasta") == "2025-12-31"
        assert merged.get("dataset") == "procesos", "dataset should be inherited"
        assert "alimentacion_escolar" in str(merged.get("objeto", [])), "topic should be inherited"
        assert "ATLÁNTICO" in str(merged.get("departamento_resolved", "")).upper(), "scope should be inherited"
        offset = merged.get("offset", 0)
        assert offset == 0 or offset is None, f"offset should be 0/None, got {offset}"

    def test_scenario_d_pae_atlantico_change_order(self):
        """
        Turno 1: "procesos de pae atlantico 2026"
        Turno 2: "los de mayor valor"
        → change_order, dataset procesos, topic alimentacion_escolar, scope Atlantico,
          ordering valor_desc, offset=0
        """
        t1_params = {
            "dataset": "procesos",
            "objeto": ["alimentacion_escolar"],
            "departamento_resolved": "ATLÁNTICO",
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
        }
        t1_frame = _make_previous_frame(t1_params)

        t2_params = {
            "ordering_signal": "valor_desc",
        }

        intent, merged = detect_and_merge(
            text="los de mayor valor",
            current_params=t2_params,
            previous_frame=t1_frame,
        )

        assert intent == "change_order", f"expected change_order, got {intent}"
        assert merged.get("ordering_signal") == "valor_desc"
        assert merged.get("dataset") == "procesos", "dataset should be inherited"
        assert "alimentacion_escolar" in str(merged.get("objeto", [])), "topic should be inherited"
        offset = merged.get("offset", 0)
        assert offset == 0 or offset is None, f"offset should be 0/None, got {offset}"

    def test_scenario_e_ahora_bolivar_change_scope_depa(self):
        """
        Turno 1: "procesos abiertos de alimentacion en Atlantico 2026"
        Turno 2: "ahora en Bolívar"
        → change_scope, conserva tema/dataset, cambia departamento a BOLÍVAR
        """
        t1_params = {
            "dataset": "procesos",
            "objeto": ["alimentacion_escolar"],
            "departamento_resolved": "ATLÁNTICO",
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
        }
        t1_frame = _make_previous_frame(t1_params)

        t2_params = {
            "departamento": "Bolívar",
        }

        intent, merged = detect_and_merge(
            text="ahora en Bolívar",
            current_params=t2_params,
            previous_frame=t1_frame,
        )

        assert intent == "change_scope", f"expected change_scope, got {intent}"
        # Dataset y tema deben heredarse
        assert merged.get("dataset") == "procesos", f"dataset should be inherited, got {merged.get('dataset')}"
        assert "alimentacion_escolar" in str(merged.get("objeto", [])), "topic should be inherited"
        # Scope debe ser el nuevo departamento
        assert merged.get("departamento") == "Bolívar", f"departamento should be Bolivar, got {merged.get('departamento')}"
        offset = merged.get("offset", 0)
        assert offset == 0 or offset is None, "offset should be 0 on scope change"

    def test_scenario_f_ahora_procesos_abiertos_change_dataset(self):
        """
        Turno 1: "contratos de adulto mayor en Barranquilla"
        Turno 2: "ahora procesos abiertos"
        → change_dataset, cambia a procesos, conserva tema/scope
        """
        t1_params = {
            "dataset": "contratos",
            "objeto": ["adulto_mayor"],
            "entidad_resolved": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
        }
        t1_frame = _make_previous_frame(t1_params)

        t2_params = {
            "dataset": "procesos",
            "dataset_explicit": True,
        }

        intent, merged = detect_and_merge(
            text="ahora procesos abiertos",
            current_params=t2_params,
            previous_frame=t1_frame,
        )

        assert intent == "change_dataset", f"expected change_dataset, got {intent}"
        assert merged.get("dataset") == "procesos", f"dataset should be procesos, got {merged.get('dataset')}"
        # Tema debe heredarse
        assert "adulto_mayor" in str(merged.get("objeto", [])), "topic should be inherited"
        offset = merged.get("offset", 0)
        assert offset == 0 or offset is None, "offset should be 0 on dataset change"

    def test_scenario_g_contratos_firmados_after_procesos(self):
        """
        Turno 1: "procesos de mantenimiento en Cali"
        Turno 2: "contratos firmados"
        → change_dataset, cambia a contratos, conserva tema/scope
        """
        t1_params = {
            "dataset": "procesos",
            "objeto": ["mantenimiento"],
            "ciudad": "Cali",
        }
        t1_frame = _make_previous_frame(t1_params)

        t2_params = {
            "dataset": "contratos",
            "dataset_explicit": True,
        }

        intent, merged = detect_and_merge(
            text="contratos firmados",
            current_params=t2_params,
            previous_frame=t1_frame,
        )

        assert intent == "change_dataset", f"expected change_dataset, got {intent}"
        assert merged.get("dataset") == "contratos", f"dataset should be contratos, got {merged.get('dataset')}"
        # Tema debe heredarse
        assert "mantenimiento" in str(merged.get("objeto", [])), "topic should be inherited"
        # Scope debe heredarse
        assert merged.get("ciudad") == "Cali", f"ciudad should be inherited, got {merged.get('ciudad')}"
        offset = merged.get("offset", 0)
        assert offset == 0 or offset is None, "offset should be 0 on dataset change"


# ── Test Guards ────────────────────────────────────────────────────────────

class TestGuards:
    def test_where_1_1_no_params(self):
        """Sin scope ni topic → False."""
        assert FollowupGuards.check_no_where_1_1({}) is False

    def test_where_1_1_objeto(self):
        """Solo objeto → True."""
        assert FollowupGuards.check_no_where_1_1({"objeto": ["mantenimiento"]}) is True

    def test_where_1_1_entidad(self):
        """Solo entidad → True."""
        assert FollowupGuards.check_no_where_1_1({"entidad_resolved": "ALCALDIA"}) is True

    def test_where_1_1_modifiers_only(self):
        """Solo modifiers → False (needs scope/topic)."""
        assert FollowupGuards.check_no_where_1_1({"ordering_signal": "valor_desc"}) is False
        assert FollowupGuards.check_no_where_1_1({"fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31"}) is False

    def test_offset_valid_only_pagination(self):
        assert FollowupGuards.check_no_offset_on_refinement("pagination_more") is True
        assert FollowupGuards.check_no_offset_on_refinement("refine_filter") is False
        assert FollowupGuards.check_no_offset_on_refinement("change_year") is False
        assert FollowupGuards.check_no_offset_on_refinement("change_order") is False
        assert FollowupGuards.check_no_offset_on_refinement("change_scope") is False
        assert FollowupGuards.check_no_offset_on_refinement("new_search") is False


# ── Test integrar con orchestrator ──────────────────────────────────────────

class TestOrchestratorIntegration:
    """Tests de regresión: el followup_engine debe comportarse igual o mejor que query_frame actual."""

    def test_merge_params_adulto_mayor(self):
        """
        Escenario de regresión: el bug original donde merge_params perdía contexto.
        Turno 1: "contratos de primera infancia alcaldia de barranquilla"
        Turno 2: "muestrame los de mayor valor de 2026"
        → debe conservar dataset=contratos, objeto=primera_infancia, scope=Barranquilla
        """
        t1_params = {
            "dataset": "contratos",
            "dataset_explicit": True,
            "objeto": ["primera", "infancia"],
            "entidad_resolved": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
        }
        t1_frame = _make_previous_frame(t1_params)

        t2_params = {
            "dataset": "procesos",  # default (dataset_explicit=False)
            "ordering_signal": "valor_desc",
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
        }

        intent, merged = detect_and_merge(
            text="muestrame los de mayor valor de 2026",
            current_params=t2_params,
            previous_frame=t1_frame,
        )

        # Debe ser refine o change_order
        assert intent in ("refine_filter", "change_order"), f"got {intent}"

        # Dataset debe conservarse (contratos)
        assert merged.get("dataset") == "contratos", f"dataset should be contratos, got {merged.get('dataset')}"

        # Objeto debe conservarse
        assert "primera" in str(merged.get("objeto", [])), "objeto should preserve primera"
        assert "infancia" in str(merged.get("objeto", [])), "objeto should preserve infancia"

        # Scope debe conservarse
        er = merged.get("entidad_resolved", "")
        assert "BARRANQUILLA" in str(er).upper(), f"scope not preserved: {er}"

        # Ordenamiento agregado
        assert merged.get("ordering_signal") == "valor_desc", "ordering_signal should be valor_desc"
