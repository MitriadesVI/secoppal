"""Tests E2E tarea 1.2.e — traza completa query→params para los casos
identificados en la auditoría de feedback (negativo + regresión morfológica).

No llaman a SECOP real. Verifican que la cadena
  QueryRouter.parse() + SoQLBuilder.build()
produce los parámetros y SoQL correctos.
"""
from __future__ import annotations

import pytest

from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder

QR = QueryRouter()
SB = SoQLBuilder()


# ── helpers ─────────────────────────────────────────────────────────────────

def parse(query: str):
    return QR.parse(query)

def build(params: dict) -> str:
    from app.core.soql_builder import SoQLBuilder as _SB
    ds_id = _SB.dataset_id_for(params.get("dataset"))
    return SB.build(ds_id, params)


# ── 1.2.a — stopwords ────────────────────────────────────────────────────────

class TestStopwordsExpanded:
    """Palabras de relleno conversacional no deben contaminar el objeto."""

    def test_han_sido_removed(self):
        p = parse("cuales contratos de mantenimiento han sido adjudicados")
        obj = p.params.get("objeto", [])
        assert "han" not in obj
        assert "sido" not in obj
        assert "mantenimiento" in obj

    def test_convocados_removed(self):
        p = parse("procesos convocados de obra en cundinamarca")
        obj = p.params.get("objeto", [])
        assert "convocados" not in obj
        assert "convocadas" not in obj

    def test_disponibles_removed(self):
        p = parse("contratos disponibles de consultoria en bogota")
        obj = p.params.get("objeto", [])
        assert "disponibles" not in obj
        assert "consultoria" in obj

    def test_entre_removed_from_objeto(self):
        """'entre' no debe aparecer como término de objeto en un rango de valor."""
        p = parse("contratos de obra entre 50 millones y 200 millones")
        obj = p.params.get("objeto", [])
        assert "entre" not in obj


# ── 1.2.b — rango entre X y Y ────────────────────────────────────────────────

class TestRangoValor:
    def test_rango_explicito_con_unidades(self):
        p = parse("contratos de obra entre 50 millones y 200 millones en antioquia")
        assert p.params.get("valor_min") == 50_000_000
        assert p.params.get("valor_max") == 200_000_000

    def test_rango_unidad_heredada(self):
        """'entre 100 y 500 millones' — 100 hereda 'millones' del lado derecho."""
        p = parse("contratos entre 100 y 500 millones de vias")
        assert p.params.get("valor_min") == 100_000_000
        assert p.params.get("valor_max") == 500_000_000

    def test_rango_miles_de_millones(self):
        p = parse("contratos entre 1000 millones y 5000 millones de infraestructura")
        assert p.params.get("valor_min") == 1_000_000_000
        assert p.params.get("valor_max") == 5_000_000_000

    def test_rango_no_interfiere_con_mayor_de(self):
        p = parse("contratos mayores de 1000 millones en bogota")
        assert p.params.get("valor_min") == 1_000_000_000
        assert p.params.get("valor_max") is None

    def test_rango_hasta(self):
        p = parse("contratos hasta 50 millones de dotacion")
        assert p.params.get("valor_max") == 50_000_000
        assert p.params.get("valor_min") is None


# ── 1.2.c/d — variantes morfológicas + OR expansion ─────────────────────────

class TestVariantesMorfologicas:
    def test_mantenimientos_normalizado_a_root(self):
        p = parse("contratos de mantenimientos en bogota")
        obj = p.params.get("objeto", [])
        assert "mantenimiento" in obj
        assert "mantenimientos" not in obj

    def test_dotaciones_normalizado(self):
        p = parse("dotaciones para colegios en cundinamarca")
        obj = p.params.get("objeto", [])
        assert "dotacion" in obj
        assert "dotaciones" not in obj

    def test_trigram_centros_de_vida(self):
        p = parse("contratos centros de vida en antioquia")
        obj = p.params.get("objeto", [])
        assert "centros de vida" in obj

    def test_trigram_trampas_de_grasas(self):
        p = parse("contratos de trampas de grasas adjudicados")
        obj = p.params.get("objeto", [])
        assert "trampas de grasas" in obj

    def test_soql_or_expansion_mantenimiento(self):
        """SoQL debe incluir OR para mantenimiento Y mantenimientos."""
        p = parse("contratos de mantenimiento en antioquia")
        soql = build(p.params)
        assert "mantenimiento" in soql.lower()
        assert "mantenimientos" in soql.lower()

    def test_soql_term_not_in_dict_no_expansion(self):
        """Término no en el diccionario no genera OR extra."""
        p = parse("contratos de demolicion en bogota")
        soql = build(p.params)
        # Solo un LIKE para demolicion, no OR espurio
        assert soql.lower().count("demolicion") == 2  # object_name + description (2 campos)


# ── Regresión queries de feedback negativo ───────────────────────────────────

class TestFeedbackNegativoRegresion:
    """Las 5 queries con rating negativo del 12 de mayo deben parsearse
    sin fallos y con parámetros razonables."""

    def test_reparaciones_convocatoria(self):
        p = parse("procesos de reparaciones convocatoria")
        # convocatoria/convocado no debe contaminar objeto
        obj = p.params.get("objeto", [])
        # reparacion(es) debe resolverse
        assert any("reparac" in t for t in obj)

    def test_alimentacion_hay(self):
        """'hay' debe removerse como stopword."""
        p = parse("que convocatoria de alimentacion hay")
        obj = p.params.get("objeto", [])
        assert "hay" not in obj
        assert any("alimentac" in t for t in obj)

    def test_mas_caros_alcaldia_bucaramanga(self):
        p = parse("contratos mas caros alcaldia bucaramanga en 2025")
        assert p.params.get("ordering_signal") == "valor_desc"
        assert p.params.get("fecha_desde", "").startswith("2025")
        assert p.params.get("fecha_hasta", "").startswith("2025")

    def test_mas_grandes_gobernacion_atlantico(self):
        p = parse("contratos mas grandes gobernacion de atlantico en 2024")
        assert p.params.get("ordering_signal") == "valor_desc"
        assert p.params.get("fecha_desde", "").startswith("2024")

    def test_procesos_abiertos_choco(self):
        # Chocó resuelve tanto con "en choco", "del choco", "departamento del choco"
        p = parse("procesos abiertos departamento del choco")
        assert p.params.get("dataset") == "procesos"
        assert p.params.get("estado") is not None or p.params.get("estado_de_apertura_del_proceso") is not None
        assert p.params.get("departamento_resolved") == "Chocó"

    def test_departamento_del_choco(self):
        p = parse("contratos del choco")
        assert p.params.get("departamento_resolved") == "Chocó"

    def test_departamento_del_atlantico(self):
        p = parse("contratos del atlantico")
        assert p.params.get("departamento_resolved") == "Atlántico"
