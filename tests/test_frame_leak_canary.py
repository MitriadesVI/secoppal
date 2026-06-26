"""G0 — Frame-leak canary (5 vectores).

Preflight del sprint de recall semántico. Verifica que al pivotar el contexto
en un follow-up, el valor ANTERIOR no sobreviva por la puerta de atrás
(``QueryFrame.raw_params`` / merge ciego — "Causa 2" del documento técnico).

Cada escenario usa un ``chat_id`` fresco y dos turnos. La invariante del canary
es de AUSENCIA: tras el pivote, el valor del turno 1 no debe aparecer en el
SoQL del turno 2.

Estado al crear (2026-06-24):
- 4/5 vectores limpios (entidad, ciudad, fecha, dataset).
- 1/5 falla: pivote de ESTADO en follow-up delta ("y los cerrados?") conserva
  el ``estado_family=oferta_abierta`` heredado. El parser SÍ entiende "cerrados"
  (single-shot y explicit new-search resuelven a 'Cerrado'); la fuga es del
  merge delta. Marcado xfail(strict) hasta sellar (Fase C — merge ciego).

Convención de harness tomada de test_e2e_followup_context.py: workflow real con
``secop_client`` mockeado; la resolución de entidad/estado corre contra los datos
locales (determinista).
"""
from __future__ import annotations

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow


def _mock_workflow(tmp_path):
    """Workflow con SECOP mockeado + conv_store aislado por test."""
    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = lambda dataset_id, soql_query: [  # type: ignore[method-assign]
        {
            "nombre_del_procedimiento": "X",
            "entidad": "E",
            "precio_base": "1",
            "estado_de_apertura_del_proceso": "Abierto",
            "fecha_de_publicacion_del": "2026-01-01T00:00:00.000",
            "urlproceso": "u",
            "referencia_del_proceso": "R",
        }
    ]
    workflow.secop_client.count = lambda dataset_id, soql_query: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    return workflow


def _pivot(workflow, q1: str, q2: str, chat_id: str) -> str:
    """Corre dos turnos y devuelve el SoQL (uppercase) del turno 2."""
    workflow.run_query(q1, channel="streamlit", chat_id=chat_id)
    r2 = workflow.run_query(q2, channel="streamlit", chat_id=chat_id)
    soql = r2.get("soql_query", "") or ""
    assert "WHERE" in soql, f"T2 sin WHERE — la ausencia sería trivial: {soql!r}"
    return soql.upper()


def test_canary_entidad_pivot(tmp_path):
    """Pivote de departamento: Cundinamarca no debe sobrevivir al cambiar a Cesar."""
    soql = _pivot(
        _mock_workflow(tmp_path),
        "contratos de la gobernacion de cundinamarca de salud",
        "ahora en el departamento del cesar",
        "canary_ent",
    )
    assert "CUNDINAMARCA" not in soql, f"FUGA entidad anterior:\n{soql}"
    assert "CESAR" in soql, f"el nuevo scope (Cesar) no se aplicó:\n{soql}"


def test_canary_ciudad_pivot(tmp_path):
    """Pivote de ciudad: Barranquilla no debe sobrevivir al cambiar a Cartagena."""
    soql = _pivot(
        _mock_workflow(tmp_path),
        "obras de pavimentacion en barranquilla",
        "ahora en cartagena",
        "canary_ciu",
    )
    assert "BARRANQUILLA" not in soql, f"FUGA ciudad anterior:\n{soql}"
    assert "CARTAGENA" in soql, f"la nueva ciudad no se aplicó:\n{soql}"


def test_canary_fecha_pivot(tmp_path):
    """Pivote de año: 2024 no debe sobrevivir al cambiar a 2026."""
    soql = _pivot(
        _mock_workflow(tmp_path),
        "contratos de software de 2024 en bogota",
        "y los de 2026?",
        "canary_fec",
    )
    assert "2024" not in soql, f"FUGA rango de fecha anterior:\n{soql}"
    assert "2026-01-01" in soql, f"el nuevo año no se aplicó:\n{soql}"


def test_canary_dataset_pivot(tmp_path):
    """Pivote explícito de dataset: procesos (p6dx) no debe sobrevivir a contratos."""
    workflow = _mock_workflow(tmp_path)
    workflow.run_query(
        "procesos de dotacion hospitalaria en medellin",
        channel="streamlit",
        chat_id="canary_ds",
    )
    r2 = workflow.run_query(
        "busca contratos de dotacion hospitalaria en medellin",
        channel="streamlit",
        chat_id="canary_ds",
    )
    dataset_id = r2.get("dataset_id", "")
    soql = (r2.get("soql_query", "") or "").upper()
    assert dataset_id == "jbjy-vk9h", f"dataset no pivotó a contratos: {dataset_id}"
    assert "P6DX-8ZBT" not in soql, f"FUGA dataset procesos en SoQL:\n{soql}"


def test_canary_estado_pivot(tmp_path):
    """Pivote de estado: el filtro 'abierto' anterior no debe sobrevivir a 'cerrados'.

    Sellado por FRAME-LEAK-ESTADO (followup_engine._merge_modifiers): el estado
    es un cluster multi-clave; al traer el follow-up cualquier clave de estado,
    el estado heredado se reemplaza por completo en vez de coexistir.
    """
    soql = _pivot(
        _mock_workflow(tmp_path),
        "procesos abiertos de transporte escolar en cali",
        "y los cerrados?",
        "canary_est",
    )
    # El estado abierto heredado NO debe seguir filtrando tras pedir cerrados.
    assert "= 'CERRADO'" in soql or "'CERRADO'" in soql, (
        f"el pivote a cerrados no se aplicó:\n{soql}"
    )
    assert "= 'ABIERTO'" not in soql, f"FUGA estado abierto anterior:\n{soql}"
    assert "'BORRADOR'" not in soql, f"FUGA familia oferta_abierta heredada:\n{soql}"
