"""B3 — STATE-PRIORITY-001: conjugate both state dimensions for procesos."""

from app.core.soql_builder import SoQLBuilder


def _soql_for(params: dict) -> str:
    builder = SoQLBuilder()
    # Force allow_global=True so we can inspect the WHERE clause
    # even with minimal params.
    return builder.build(SoQLBuilder.PROCESOS_DATASET, params, allow_global=True)


def test_opportunity_explicit_abierto_filters_both_dimensions():
    """Query con 'abierto' explícito debe filtrar AMBAS dimensiones."""
    params = {
        "objeto": ["construccion"],
        "ciudad": "bogota",
        "intent_type": "opportunity_search",
        "estado_del_procedimiento": ["Publicado"],
        "estado_de_apertura_del_proceso": ["Abierto"],
    }
    soql = _soql_for(params)
    assert "estado_del_procedimiento" in soql
    assert "Publicado" in soql
    assert "estado_de_apertura_del_proceso = 'Abierto'" in soql


def test_implicit_opportunity_derives_apertura():
    """Sin 'abierto' explícito, intent=opportunity_search debe derivar apertura."""
    params = {
        "objeto": ["mantenimiento"],
        "departamento_resolved": "BOLIVAR",
        "intent_type": "opportunity_search",
        "estado_del_procedimiento": ["Publicado"],
    }
    soql = _soql_for(params)
    assert "estado_de_apertura_del_proceso = 'Abierto'" in soql


def test_history_search_does_NOT_apply_apertura_filter():
    """Búsquedas históricas (sin opportunity intent) no deben aplicar apertura."""
    params = {
        "objeto": ["procesos"],
        "ciudad": "bogota",
        "intent_type": "history_search",
    }
    soql = _soql_for(params)
    assert "estado_de_apertura_del_proceso = 'Abierto'" not in soql


def test_explicit_cerrado_NOT_overridden():
    """Si usuario pidió 'cerrado' explícitamente, no sobrescribir con Abierto."""
    params = {
        "objeto": ["construccion"],
        "ciudad": "bogota",
        "intent_type": "opportunity_search",
        "estado_de_apertura_del_proceso": ["Cerrado"],
    }
    soql = _soql_for(params)
    assert "estado_de_apertura_del_proceso = 'Cerrado'" in soql
    # El derivado NO debe aparecer
    assert "estado_de_apertura_del_proceso = 'Abierto'" not in soql
