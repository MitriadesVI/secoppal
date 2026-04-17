from app.core.soql_builder import SoQLBuilder


def test_build_process_query_with_ordering_signal_uses_price_only() -> None:
    """When ordering_signal=valor_desc, ORDER BY should use price only (no secondary sort)."""
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
    assert "ORDER BY precio_base DESC LIMIT" in soql


def test_build_process_query_default_orders_by_price_then_date() -> None:
    """Default ordering: price DESC, then date DESC as tiebreaker."""
    builder = SoQLBuilder()
    soql = builder.build(
        "p6dx-8zbt",
        {
            "departamento_resolved": "ATLANTICO",
            "objeto": ["mantenimiento", "vial"],
        },
    )

    assert "ORDER BY precio_base DESC, fecha_de_publicacion_del DESC" in soql


def test_build_contract_query_default_orders_by_value_then_date() -> None:
    """Contracts default: value DESC, then fecha_de_firma DESC."""
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
    assert "ORDER BY valor_del_contrato DESC, fecha_de_firma DESC" in soql


def test_build_contract_query_with_ordering_signal_uses_value_only() -> None:
    """When ordering_signal=valor_desc, contracts ORDER BY valor_del_contrato only."""
    builder = SoQLBuilder()
    soql = builder.build(
        "jbjy-vk9h",
        {
            "departamento_resolved": "BOGOTA",
            "objeto": ["consultoria"],
            "ordering_signal": "valor_desc",
        },
    )

    assert "ORDER BY valor_del_contrato DESC LIMIT" in soql
    # Should NOT have secondary sort
    assert "fecha_de_firma" not in soql.split("ORDER BY")[1]


def test_ciudad_filter_procesos() -> None:
    """Ciudad filter should use ciudad_entidad for procesos."""
    builder = SoQLBuilder()
    soql = builder.build(
        "p6dx-8zbt",
        {"objeto": ["ampliacion"], "ciudad": "Puerto Salgar"},
    )
    assert "UPPER(ciudad_entidad) LIKE UPPER('%Puerto Salgar%')" in soql
    assert "ampliacion" in soql


def test_ciudad_filter_contratos() -> None:
    """Ciudad filter should use ciudad for contratos."""
    builder = SoQLBuilder()
    soql = builder.build(
        "jbjy-vk9h",
        {"objeto": ["cuidado"], "ciudad": "Barranquilla"},
    )
    assert "UPPER(ciudad) LIKE UPPER('%Barranquilla%')" in soql
