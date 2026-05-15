from app.core.query_router import QueryRouter
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
    assert "ORDER BY precio_base DESC, fecha_de_publicacion_del DESC LIMIT" in soql


def test_build_process_query_default_orders_by_date_then_price() -> None:
    """Default ordering: date DESC (most recent first), then price DESC."""
    builder = SoQLBuilder()
    soql = builder.build(
        "p6dx-8zbt",
        {
            "departamento_resolved": "ATLANTICO",
            "objeto": ["mantenimiento", "vial"],
        },
    )

    assert "ORDER BY fecha_de_publicacion_del DESC, precio_base DESC" in soql


def test_build_contract_query_default_orders_by_date_then_value() -> None:
    """Contracts default: fecha_de_firma DESC, then value DESC."""
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
    assert "ORDER BY fecha_de_firma DESC, valor_del_contrato DESC" in soql


def test_query_router_year_range_reaches_contract_soql_upper_bound() -> None:
    parsed = QueryRouter().parse(
        "contratos de adulto mayor mas caros del distrito de barranquilla entre 2025 y 2026"
    )
    params = parsed.params
    builder = SoQLBuilder()
    dataset_id = builder.dataset_id_for(params.get("dataset"))
    soql = builder.build(dataset_id, params)

    assert params.get("fecha_desde") == "2025-01-01"
    assert params.get("fecha_hasta") == "2026-12-31"
    assert "fecha_de_firma >= '2025-01-01'" in soql
    assert "fecha_de_firma <= '2026-12-31'" in soql


def test_build_contract_query_with_ordering_signal_uses_value_first() -> None:
    """When ordering_signal=valor_desc, contracts ORDER BY value DESC, date DESC."""
    builder = SoQLBuilder()
    soql = builder.build(
        "jbjy-vk9h",
        {
            "departamento_resolved": "BOGOTA",
            "objeto": ["consultoria"],
            "ordering_signal": "valor_desc",
        },
    )

    assert "ORDER BY valor_del_contrato DESC, fecha_de_firma DESC" in soql


def test_ciudad_filter_procesos() -> None:
    """Ciudad filter should use ciudad_entidad for procesos."""
    builder = SoQLBuilder()
    soql = builder.build(
        "p6dx-8zbt",
        {"objeto": ["ampliacion"], "ciudad": "Puerto Salgar"},
    )
    assert "UPPER(ciudad_entidad) LIKE UPPER('%Puerto Salgar%')" in soql
    assert "ampliacion" in soql


def test_build_count_produces_count_star_no_limit() -> None:
    """build_count() must return SELECT count(*) with same filters, no ORDER BY / LIMIT."""
    builder = SoQLBuilder()
    soql = builder.build_count(
        "p6dx-8zbt",
        {
            "departamento_resolved": "ATLANTICO",
            "objeto": ["mantenimiento"],
            "estado": "Abierto",
        },
    )
    assert soql.startswith("SELECT count(*) WHERE ")
    assert "departamento_entidad = 'ATLANTICO'" in soql
    assert "LIMIT" not in soql
    assert "ORDER BY" not in soql


def test_build_count_where_matches_build_where() -> None:
    """build_count() and build() must produce the same WHERE clause."""
    builder = SoQLBuilder()
    params = {
        "departamento_resolved": "CUNDINAMARCA",
        "objeto": ["consultoria"],
        "valor_min": 100_000_000,
    }
    full = builder.build("p6dx-8zbt", params)
    count = builder.build_count("p6dx-8zbt", params)
    # Extract WHERE clause from full query (strip SELECT ... WHERE and ORDER BY ...)
    full_where = full.split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]
    count_where = count.split(" WHERE ", 1)[1]
    assert full_where == count_where


def test_build_count_empty_params() -> None:
    """build_count() with no filters uses WHERE 1=1."""
    builder = SoQLBuilder()
    soql = builder.build_count("jbjy-vk9h", {})
    assert soql == "SELECT count(*) WHERE 1=1"


def test_ciudad_filter_contratos() -> None:
    """Ciudad filter should use ciudad for contratos."""
    builder = SoQLBuilder()
    soql = builder.build(
        "jbjy-vk9h",
        {"objeto": ["cuidado"], "ciudad": "Barranquilla"},
    )
    assert "UPPER(ciudad) LIKE UPPER('%Barranquilla%')" in soql


def test_or_group_generates_or_in_soql() -> None:
    builder = SoQLBuilder()
    soql = builder.build("p6dx-8zbt", {"objeto": [["canchas", "parques"]]})
    where = soql.split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]

    assert "canchas" in where
    assert "parques" in where
    assert " AND " not in where
    assert " OR " in where


def test_or_group_with_anchor_combines_and_with_or_group() -> None:
    builder = SoQLBuilder()
    soql = builder.build("p6dx-8zbt", {"objeto": ["construccion", ["canchas", "parques"]]})
    where = soql.split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]

    assert "construccion" in where
    assert "canchas" in where
    assert "parques" in where
    assert " AND " in where
    assert where.index("construccion") < where.index(" AND ") < where.index("canchas")
    assert "canchas" in where.split(" AND ", 1)[1]
    assert " OR " in where.split(" AND ", 1)[1]


def test_simple_list_still_uses_and() -> None:
    builder = SoQLBuilder()
    soql = builder.build("p6dx-8zbt", {"objeto": ["pavimentacion", "bolivar"]})
    where = soql.split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]

    assert "pavimentacion" in where
    assert "bolivar" in where
    assert " AND " in where


def test_alimentacion_escolar_does_not_emit_bare_pae_like() -> None:
    builder = SoQLBuilder()
    soql = builder.build("p6dx-8zbt", {"objeto": ["alimentacion_escolar"]})
    where = soql.split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]

    assert "%pae%" not in where.lower()
    assert "%alimentacion escolar%" in where.lower()
    assert "%programa de alimentacion escolar%" in where.lower()
    assert "%complemento alimentario%" in where.lower()
    assert "%simat%" in where.lower()


def test_cultura_expands_domain_variants_without_temas() -> None:
    builder = SoQLBuilder()
    soql = builder.build("jbjy-vk9h", {
        "dataset": "contratos",
        "entidad_resolved": "GOBERNACION DEL DEPARTAMENTO DEL CESAR",
        "objeto": ["cultura"],
    })
    where = soql.split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]
    lower = where.lower()

    assert "gobernacion del departamento del cesar" in lower
    assert "%cultura%" in lower
    assert "%cultural%" in lower
    assert "%culturales%" in lower
    assert "%patrimonio cultural%" in lower
    assert "%eventos culturales%" in lower
    assert "%artísticas%" in lower
    assert "%temas%" not in lower
