from app.core.soql_builder import SoQLBuilder


def test_build_process_query_uses_price_ordering() -> None:
    builder = SoQLBuilder()
    soql = builder.build(
        "p6dx-8zbt",
        {
            "departamento_resolved": "ATLANTICO",
            "objeto": ["mantenimiento", "vial"],
            "estado": "Abierto",
            "valor_min": 500_000_000,
        },
    )

    assert "departamento_entidad = 'ATLANTICO'" in soql
    assert "precio_base >= 500000000" in soql
    assert "estado_de_apertura_del_proceso = 'Abierto'" in soql
    assert "ORDER BY precio_base DESC" in soql
    assert "fecha_de_publicacion_del DESC" not in soql


def test_build_contract_query_filters_contractor() -> None:
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
    assert "ORDER BY valor_del_contrato DESC" in soql

