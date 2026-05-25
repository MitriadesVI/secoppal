"""DOGFOOD-SOQL-006: boost 'activos primero' en Python tras quitar CASE WHEN del SoQL."""
from __future__ import annotations

from app.core.formatter import _reorder_active_first


class TestReorderActiveFirst:
    def test_procesos_activos_van_primero(self):
        rows = [
            {"id": "a", "estado_de_apertura_del_proceso": "Cerrado"},
            {"id": "b", "estado_de_apertura_del_proceso": "Abierto"},
            {"id": "c", "estado_de_apertura_del_proceso": "Publicado"},
            {"id": "d", "estado_de_apertura_del_proceso": "Adjudicado"},
        ]
        out = _reorder_active_first(rows, "p6dx-8zbt", params={})
        assert [r["id"] for r in out] == ["b", "c", "a", "d"]

    def test_contratos_activos_van_primero_con_tildes(self):
        rows = [
            {"id": "a", "estado_contrato": "Liquidado"},
            {"id": "b", "estado_contrato": "En ejecución"},
            {"id": "c", "estado_contrato": "Prorrogado"},
            {"id": "d", "estado_contrato": "Terminado"},
        ]
        out = _reorder_active_first(rows, "jbjy-vk9h", params={})
        assert [r["id"] for r in out] == ["b", "c", "a", "d"]

    def test_sin_reorder_si_usuario_filtro_estado(self):
        rows = [
            {"id": "a", "estado_contrato": "Liquidado"},
            {"id": "b", "estado_contrato": "En ejecución"},
        ]
        out = _reorder_active_first(rows, "jbjy-vk9h", params={"estado": "Liquidado"})
        assert [r["id"] for r in out] == ["a", "b"]

    def test_sin_reorder_si_opportunity_search(self):
        rows = [
            {"id": "a", "estado_de_apertura_del_proceso": "Cerrado"},
            {"id": "b", "estado_de_apertura_del_proceso": "Abierto"},
        ]
        out = _reorder_active_first(rows, "p6dx-8zbt", params={"intent_type": "opportunity_search"})
        assert [r["id"] for r in out] == ["a", "b"]

    def test_preserva_orden_dentro_del_mismo_bucket(self):
        # Estabilidad: dentro de "activos" debe respetar el orden por fecha del SoQL
        rows = [
            {"id": "a", "estado_contrato": "En ejecución"},
            {"id": "b", "estado_contrato": "Terminado"},
            {"id": "c", "estado_contrato": "En ejecución"},
            {"id": "d", "estado_contrato": "Terminado"},
        ]
        out = _reorder_active_first(rows, "jbjy-vk9h", params={})
        assert [r["id"] for r in out] == ["a", "c", "b", "d"]

    def test_lista_vacia(self):
        assert _reorder_active_first([], "p6dx-8zbt", params={}) == []
