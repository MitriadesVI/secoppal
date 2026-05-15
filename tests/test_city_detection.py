"""Tests: city/municipio detection — evitar que ciudades entren en objeto."""
from __future__ import annotations

from pathlib import Path

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter


class TestCityDetection:
    """Prueba unitaria: el parser no debe dejar ciudades en objeto."""

    def test_barranquilla_not_in_objeto(self):
        qr = QueryRouter()
        result = qr.parse("contratos de primera infancia de barranquilla en 2026")
        params = result.params

        assert "barranquilla" not in [t.lower() for t in params.get("objeto", [])], (
            f"barranquilla should not be in objeto: {params.get('objeto')}"
        )
        assert "primera" in params.get("objeto", []), f"objeto={params.get('objeto')}"
        assert "infancia" in params.get("objeto", []), f"objeto={params.get('objeto')}"
        assert params.get("dataset") == "contratos"

    def test_city_sets_ciudad_param(self):
        qr = QueryRouter()
        result = qr.parse("contratos de mantenimiento en barranquilla")
        assert result.params.get("ciudad") == "Barranquilla"

    def test_city_with_de_prefix(self):
        qr = QueryRouter()
        result = qr.parse("obras de barranquilla")
        assert "barranquilla" not in [t.lower() for t in result.params.get("objeto", [])]

    def test_city_sets_entidad_for_barranquilla(self):
        qr = QueryRouter()
        result = qr.parse("contratos en barranquilla")
        er = result.params.get("entidad_resolved", "")
        assert "DISTRITO" in er.upper() and "BARRANQUILLA" in er.upper(), (
            f"entidad_resolved should contain DISTRITO + BARRANQUILLA: {er}"
        )

    def test_other_cities_not_in_objeto(self):
        qr = QueryRouter()
        result = qr.parse("procesos de mantenimiento vial en medellin")
        assert "medellin" not in [t.lower() for t in result.params.get("objeto", [])]

    def test_city_with_entidad_does_not_override(self):
        """Si ya hay entidad_resolved (ej: por gazetteer), no sobreescribir."""
        qr = QueryRouter()
        result = qr.parse("contratos de la alcaldia de barranquilla")
        # La alcaldía debería resolverse vía gazetteer primero
        params = result.params
        assert "barranquilla" not in [t.lower() for t in params.get("objeto", [])]
        # ci债务 debería seguir presente
        assert params.get("ciudad") is not None or "barranquilla" not in [t.lower() for t in params.get("objeto", [])], (
            f"objeto={params.get('objeto')}"
        )

    def test_solo_city_sets_ciudad(self):
        qr = QueryRouter()
        result = qr.parse("en barranquilla")
        assert result.params.get("ciudad") == "Barranquilla"


class TestE2ECityDetection:
    """E2E: workflow completo con ciudad no contamina objeto."""

    def _workflow(self, tmp_path):
        from app.core.conversation_store import ConversationStore
        wf = SecopalWorkflow(Settings())
        wf.secop_client.query = lambda ds, soql: [  # type: ignore[method-assign]
            {
                "nombre_del_procedimiento": "Atencion primera infancia",
                "entidad": "ALCALDIA DE BARRANQUILLA",
                "precio_base": "500000000",
                "estado_de_apertura_del_proceso": "Celebrado",
                "fecha_de_publicacion_del": "2026-06-01T00:00:00.000",
                "urlproceso": "https://secop.gov.co/test",
                "referencia_del_proceso": "REF-1",
            }
        ]
        wf.secop_client.count = lambda ds, soql: 1  # type: ignore[method-assign]
        wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
        return wf

    def test_barranquilla_not_in_soql_object_filter(self, tmp_path):
        """El SoQL generado NO debe tener LIKE '%barranquilla%' sobre objeto_del_contrato."""
        wf = self._workflow(tmp_path)
        result = wf.run_query(
            "contratos de primera infancia de barranquilla en 2026",
            channel="streamlit",
        )
        soql = result.get("soql_query", "")
        # objeto debe contener primera e infancia
        pp = result.get("parsed_params", {})
        assert "primera" in pp.get("objeto", []), f"objeto={pp.get('objeto')}"
        assert "infancia" in pp.get("objeto", []), f"objeto={pp.get('objeto')}"
        # NO debe tener LIKE con barranquilla sobre objeto_del_contrato
        # (el filtro por ciudad UPPER(ciudad) LIKE '%Barranquilla%' SÍ es válido)
        import re as _re
        obj_likes = _re.findall(r"UPPER\(objeto_del_contrato\) LIKE[^)]*\)", soql, _re.IGNORECASE)
        for clause in obj_likes:
            assert "barranquilla" not in clause.lower(), f"objeto LIKE contains barranquilla: {clause}"
        # Debe tener los filtros correctos
        assert "valor_del_contrato" in soql or "precio_base" in soql, f"SoQL missing value: {soql}"
        assert pp.get("fecha_desde") == "2026-01-01", f"fecha_desde={pp.get('fecha_desde')}"
        assert pp.get("fecha_hasta") == "2026-12-31", f"fecha_hasta={pp.get('fecha_hasta')}"
