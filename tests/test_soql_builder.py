from app.core.soql_builder import SoQLBuilder


def test_build_process_query_with_ordering_signal_uses_price() -> None:
    """When ordering_signal=valor_desc, ORDER BY should use price."""
    builder = SoQLBuilder()
    soql = builder.build(
        "p6dx-8zbt",
        {
            "departamento_resolved": "ATLANTICO",
            "objeto": ["mantenimiento", "vial"],
            "estado": "Abierto",
            "valor_min": 500_000_000,
            "ordering_signal": "valor_desc",
        },
    )

    assert "departamento_entidad = 'ATLANTICO'" in soql
    assert "precio_base >= 500000000" in soql
    assert "estado_de_apertura_del_proceso = 'Abierto'" in soql
    assert "ORDER BY precio_base DESC" in soql


def test_build_process_query_default_orders_by_date() -> None:
    """Without ordering_signal, ORDER BY should use date (most recent first)."""
    builder = SoQLBuilder()
    soql = builder.build(
        "p6dx-8zbt",
        {
            "departamento_resolved": "ATLANTICO",
            "objeto": ["mantenimiento", "vial"],
        },
    )

    assert "ORDER BY fecha_de_publicacion_del DESC" in soql
    assert "precio_base DESC" not in soql


def test_build_contract_query_default_orders_by_date() -> None:
    """Contracts without ordering_signal should order by fecha_de_firma DESC."""
    builder = SoQLBuilder()
    soql = builder.build(
        "jbjy-vk9h",
        {
            "entidad_resolved": "SERVICIO NACIONAL DE APRENDIZAJE -SENA-",
            "departamento_resolved": "BOGOTA",
            "contratista": "900123456",
            "valor_min": 200_000_000,
        },
    )

    assert "UPPER(nombre_entidad) LIKE UPPER('%SERVICIO NACIONAL DE APRENDIZAJE -SENA-%')" in soql
    assert "departamento = 'BOGOTA'" in soql
    assert "documento_proveedor = '900123456'" in soql
    assert "ORDER BY fecha_de_firma DESC" in soql


def test_build_contract_query_with_ordering_signal_uses_value() -> None:
    """When ordering_signal=valor_desc, contracts ORDER BY valor_del_contrato."""
    builder = SoQLBuilder()
    soql = builder.build(
        "jbjy-vk9h",
        {
            "departamento_resolved": "BOGOTA",
            "objeto": ["consultoria"],
            "ordering_signal": "valor_desc",
        },
    )

    assert "ORDER BY valor_del_contrato DESC" in soql
