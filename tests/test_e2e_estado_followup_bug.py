"""Test E2E: Bug follow-up estado + objeto contaminado.

Escenario exacto del reporte del asesor:
1. "hola muestrame contratos de mantenimiento en bogota"
2. "muestrame mas contratos que esten firmados o en ejecucion"

Bug original:
- objeto = ["esten"] (esten como falso objeto)
- estado_contrato = ['Cerrado'] (solo Cerrado, no la lista)
- Perdió "mantenimiento" del turno anterior

Fix aplicado (2026-05-16):
- STOPWORDS: "esten" ya no pasa como objeto
- estado_families: con_contrato → contrato_activo (solo En ejecución/Modificado/Prorrogado)
- "firmados" ya no aplica estado_contrato
- SoQLBuilder: listas de estado tienen prioridad sobre estado scalar legacy
"""
from __future__ import annotations

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.conversation_store import ConversationStore


def _make_workflow(tmp_path):
    wf = SecopalWorkflow(Settings())
    # Mock SECOP response
    wf.secop_client.query = lambda ds, soql: [  # type: ignore[method-assign]
        {
            "nombre_del_procedimiento": "Mantenimiento via urbana",
            "entidad": "ALCALDIA MAYOR DE BOGOTA",
            "precio_base": "500000000",
            "estado_contrato": "En ejecución",
            "fecha_de_firma": "2026-03-15T00:00:00.000",
            "urlproceso": "https://secop.gov.co/test",
            "referencia_del_proceso": "REF-1",
        }
    ]
    wf.secop_client.count = lambda ds, soql: 1  # type: ignore[method-assign]
    wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign] # disable narrator
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
    return wf


class TestBugEstadoFollowUp:
    """Bug report: follow-up pierde tema y contamina objeto."""

    def test_esten_is_stopword(self, tmp_path):
        """'esten' no debe aparecer como objeto contractual."""
        wf = _make_workflow(tmp_path)
        r1 = wf.run_query(
            "hola muestrame contratos de mantenimiento en bogota",
            channel="streamlit", chat_id="st_bug",
        )
        pp1 = r1.get("parsed_params", {})
        assert "mantenimiento" in pp1.get("objeto", []), \
            f"turno1 should have mantenimiento in objeto, got {pp1.get('objeto')}"
        assert pp1.get("departamento_resolved") is not None, \
            "turno1 should have departamento_resolved"

        # Turno 2: follow-up que causó el bug
        r2 = wf.run_query(
            "muestrame mas contratos que esten firmados o en ejecucion",
            channel="streamlit", chat_id="st_bug",
        )
        pp2 = r2.get("parsed_params", {})
        rp2 = r2.get("resolved_params", {})

        # 1. "esten" NO debe estar en objeto
        obj = rp2.get("objeto", []) or pp2.get("objeto", [])
        assert "esten" not in obj, f"'esten' should not be in objeto: {obj}"
        assert "este" not in obj, f"'este' should not be in objeto: {obj}"

        # 2. Debe heredar "mantenimiento" del turno anterior
        assert "mantenimiento" in obj, \
            f"mantenimiento should be inherited from turno1, got objeto={obj}"

        # 3. Debe tener departamento_resolved heredado
        dep = rp2.get("departamento_resolved") or pp2.get("departamento_resolved", "")
        assert dep, "departamento_resolved should be inherited"

        # 4. Estado debe ser contrato_activo (En ejecución, Modificado, Prorrogado)
        # NO debe ser Cerrado
        estado_contrato = rp2.get("estado_contrato") or pp2.get("estado_contrato", [])
        assert isinstance(estado_contrato, list), f"estado_contrato should be list, got {type(estado_contrato)}"
        assert "Cerrado" not in estado_contrato, \
            f"Cerrado should NOT be in estado_contrato: {estado_contrato}"
        assert "En ejecución" in estado_contrato, \
            f"En ejecución should be in estado_contrato: {estado_contrato}"

        # 5. "Firmados" no debe aplicar estado_contrato
        # El estado debe venir de "en ejecucion", no de "firmados"
        # Si viene de "en ejecucion", contrato_activo = [En ejecución, Modificado, Prorrogado]
        assert len(estado_contrato) <= 3 or estado_contrato == [], \
            f"estado_contrato should be small (activo), got {len(estado_contrato)} values"

        # 6. SoQL no debe contener estado_contrato = 'Cerrado' (solo IN si aplica)
        soql = r2.get("soql_query", "")
        # Si hay estado_contrato IN, debe incluir En ejecución
        if "estado_contrato" in soql:
            assert "Cerrado" not in soql, \
                f"SoQL should not contain 'Cerrado': {soql[:300]}"

        # 7. Respuesta no debe contener "esten"
        resp = r2.get("response", "")
        assert "esten" not in resp.lower(), \
            f"response should not contain 'esten': {resp[:200]}"
