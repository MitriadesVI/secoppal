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
    # Etapa 8A: desempate estable por id al final del ORDER BY.
    assert "ORDER BY precio_base DESC, fecha_de_publicacion_del DESC, id_del_proceso DESC LIMIT" in soql


def test_build_process_query_default_orders_by_date_then_price() -> None:
    """DOGFOOD-SOQL-006: default sin CASE WHEN (Socrata rechaza el rewrite #CASE).
    Solo fecha DESC + precio DESC. El boost 'activos primero' va en Python."""
    builder = SoQLBuilder()
    soql = builder.build(
        "p6dx-8zbt",
        {
            "departamento_resolved": "ATLANTICO",
            "objeto": ["mantenimiento", "vial"],
        },
    )

    assert "CASE WHEN" not in soql
    assert "ORDER BY fecha_de_publicacion_del DESC, precio_base DESC" in soql


def test_build_contract_query_default_orders_by_date_then_value() -> None:
    """DOGFOOD-SOQL-006: contratos sin CASE WHEN.
    El #CASE(... IN ('En ejecución',...)) que generaba Socrata internamente
    rompía con 400 Bad Request por strings acentuados."""
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

    assert "CASE WHEN" not in soql
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
    """Ciudad filter should use ciudad_entidad for procesos with accent folding."""
    builder = SoQLBuilder()
    soql = builder.build(
        "p6dx-8zbt",
        {"objeto": ["ampliacion"], "ciudad": "Puerto Salgar"},
    )
    assert "unaccent(UPPER(ciudad_entidad)) LIKE unaccent(UPPER('%Puerto Salgar%'))" in soql
    assert "ampliacion" in soql


def test_ciudad_filter_unaccent_acentuada() -> None:
    """Ciudad con tilde (Medellín, Bogotá) debe usar unaccent para matchear."""
    builder = SoQLBuilder()
    soql = builder.build(
        "p6dx-8zbt",
        {"objeto": ["construccion"], "ciudad": "Medellín"},
    )
    assert "unaccent(UPPER(ciudad_entidad)) LIKE unaccent(UPPER('%Medellín%'))" in soql
    # Adversarial: otros filtros no deben romperse
    assert "construccion" in soql
    # El departamento sigue sin unaccent (usa igualdad exacta, no LIKE)
    assert "WHERE" in soql


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
    """SAFE-SOQL-001: build_count() sin filtros DEBE rechazarse por defecto.

    El opt-in explícito ``allow_global=True`` queda cubierto por
    tests/test_safe_soql.py.
    """
    from app.core.soql_builder import UnsafeGlobalQueryError
    import pytest as _pytest

    builder = SoQLBuilder()
    with _pytest.raises(UnsafeGlobalQueryError):
        builder.build_count("jbjy-vk9h", {})


def test_ciudad_filter_contratos() -> None:
    """Ciudad filter should use ciudad for contratos with accent folding."""
    builder = SoQLBuilder()
    soql = builder.build(
        "jbjy-vk9h",
        {"objeto": ["cuidado"], "ciudad": "Barranquilla"},
    )
    assert "unaccent(UPPER(ciudad)) LIKE unaccent(UPPER('%Barranquilla%'))" in soql


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


def test_estado_no_duplicado_cuando_legacy_y_lista_coinciden():
    params = {
        "estado": "Abierto",
        "estado_field": "estado_de_apertura_del_proceso",
        "estado_de_apertura_del_proceso": ["Abierto"],
    }
    soql = SoQLBuilder().build("p6dx-8zbt", params)
    assert soql.count("estado_de_apertura_del_proceso = 'Abierto'") == 1


def test_oferta_abierta_incluye_funnel_pre_cierre():
    """DOGFOOD-2026-05-25: oferta_abierta cubre el funnel completo pre-cierre."""
    params = {
        "intent_type": "opportunity_search",
        "estado_family": "oferta_abierta",
        "estado_del_procedimiento": [
            "Borrador",
            "En aprobación",
            "Aprobado",
            "Publicado",
            "Abierto",
        ],
    }
    soql = SoQLBuilder().build("p6dx-8zbt", params, allow_global=True)
    for estado in ["Borrador", "En aprobación", "Aprobado", "Publicado", "Abierto"]:
        assert estado in soql, f"'{estado}' debe estar en el SoQL del funnel"
    for estado in ["Seleccionado", "Evaluación", "Cancelado"]:
        assert estado not in soql, f"'{estado}' no debe estar en el SoQL del funnel"


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
    # 'programa de alimentacion escolar' es deduped (contiene 'alimentacion escolar')
    # 'complemento alimentario' sobrevive (no contiene 'alimentacion escolar')
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
    # 'cultural', 'culturales', 'patrimonio cultural', 'eventos culturales'
    # son deduped (contienen 'cultura' como substring). Fixes VARIANT-EXPLOSION-001.
    assert "%artistica%" in lower
    assert "%festivales%" in lower
    assert "%temas%" not in lower


# ═══════════════════════════════════════════════════════════════════════════
# VARIANT-EXPLOSION-001 regression tests
# ═══════════════════════════════════════════════════════════════════════════

def test_variant_explosion_mantenimiento_no_redundant_likes() -> None:
    """Caso confirmado en produccion: 'contratos de mantenimiento 500-600M
    en Cundinamarca' generaba 12 LIKEs redundantes.  Despues del fix de
    dedup_like_variants(), solo debe emitir 2 LIKEs (objeto + descripcion)
    para 'mantenimiento'.  Ningun LIKE para variantes redundantes
    ('mantenimiento correctivo', 'mantenimientos', etc.)."""
    builder = SoQLBuilder()
    soql = builder.build(
        "jbjy-vk9h",
        {
            "dataset": "contratos",
            "objeto": ["mantenimiento"],
            "departamento_resolved": "Cundinamarca",
            "valor_min": 500_000_000,
            "valor_max": 600_000_000,
        },
    )
    where = soql.split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]
    lower = where.lower()

    # Exactamente 2 LIKEs para mantenimiento (objeto + descripcion)
    assert lower.count("%mantenimiento%") == 2, (
        f"Esperaba 2 %mantenimiento%, encontre {lower.count('%mantenimiento%')}"
    )

    # NO debe haber LIKEs para variantes redundantes
    for banned in (
        "mantenimiento correctivo", "mantenimiento preventivo",
        "mantenimientos", "mantenimiento preventivo y correctivo",
        "mantenimiento correctivo y preventivo",
    ):
        assert banned not in lower, f"Variante redundante '{banned}' presente en SoQL"

    # WHERE clause debe ser compacto (<300 caracteres vs ~1500 del bug)
    assert len(where) < 300, (
        f"WHERE clause muy largo: {len(where)} caracteres (debe ser <300)"
    )


def test_variant_explosion_adulto_mayor_underscore_stripped() -> None:
    """'adulto_mayor' con underscore debe transformarse a 'adulto mayor'
    (con espacio) en el LIKE.  SECOP no usa underscores en datos reales.
    Las variantes morfologicas distintas ('persona mayor', 'personas mayores')
    deben conservarse como LIKEs separados."""
    builder = SoQLBuilder()
    soql = builder.build(
        "jbjy-vk9h",
        {
            "dataset": "contratos",
            "objeto": ["adulto_mayor"],
            "departamento_resolved": "Cundinamarca",
            "valor_min": 500_000_000,
            "fecha_desde": "2025-01-01",
            "fecha_hasta": "2025-12-31",
        },
    )
    where = soql.split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]
    lower = where.lower()

    # Ningun underscore en LIKEs
    assert "adulto_mayor" not in lower, (
        "Underscore 'adulto_mayor' presente en SoQL: " + lower[:200] + "..."
    )

    # Debe contener 'adulto mayor' con espacio
    assert "%adulto mayor%" in lower, "Falta LIKE '%adulto mayor%' en SoQL"

    # Variantes distintas deben estar presentes
    assert "%persona mayor%" in lower, "Falta LIKE '%persona mayor%'"
    assert "%personas mayores%" in lower, "Falta LIKE '%personas mayores%'"


# ── Proyección de evento del radar (R2B-1B1) ────────────────────────────────
# El SELECT debe traer los datos que el radar congela en cada evento. Nada más:
# la ampliación es aditiva y no puede tocar filtro, orden ni corte.

_RADAR_PARAMS = {
    "departamento_resolved": "ATLANTICO",
    "objeto": ["alimentacion"],
}


def test_procesos_select_includes_closing_date_exactly_once() -> None:
    """La columna real de recepción de ofertas (DOGFOOD-DATE-002), una sola vez."""
    soql = SoQLBuilder().build("p6dx-8zbt", dict(_RADAR_PARAMS))
    select_clause = soql.split(" WHERE ")[0]
    assert select_clause.count("fecha_de_recepcion_de_respuestas_al_in") == 1


def test_procesos_closing_date_is_reception_not_publication() -> None:
    """Mapping exacto: publicación y cierre son hechos distintos. Si el spec
    apuntara a publicación, el radar anunciaría un cierre que nadie fijó."""
    spec = SoQLBuilder.SPECS["p6dx-8zbt"]
    assert spec.closing_date == "fecha_de_recepcion_de_respuestas_al_in"
    assert spec.closing_date != spec.date          # date = publicación
    assert spec.signature_date is None             # un proceso no se firma


def test_contratos_select_keeps_modality_and_signature() -> None:
    soql = SoQLBuilder().build("jbjy-vk9h", {"contratista": "ACME"})
    select_clause = soql.split(" WHERE ")[0]
    assert "modalidad_de_contratacion" in select_clause
    assert "fecha_de_firma" in select_clause
    assert "proveedor_adjudicado" in select_clause


def test_contratos_spec_maps_signature_and_no_closing() -> None:
    spec = SoQLBuilder.SPECS["jbjy-vk9h"]
    assert spec.signature_date == "fecha_de_firma"
    assert spec.closing_date is None               # un contrato ya no recibe ofertas
    assert spec.contractor == "proveedor_adjudicado"


def test_closing_date_column_does_not_leak_into_where_order_or_limit() -> None:
    """La columna nueva vive SOLO en el SELECT: no filtra, no ordena, no corta."""
    soql = SoQLBuilder().build("p6dx-8zbt", dict(_RADAR_PARAMS))
    _, rest = soql.split(" WHERE ", 1)
    assert "fecha_de_recepcion_de_respuestas_al_in" not in rest


def test_closing_date_absent_from_count_query() -> None:
    """build_count no proyecta columnas: mismo WHERE, sin SELECT de campos."""
    soql = SoQLBuilder().build_count("p6dx-8zbt", dict(_RADAR_PARAMS))
    assert "fecha_de_recepcion_de_respuestas_al_in" not in soql
    assert soql.startswith("SELECT count(*) WHERE ")


def test_radar_projection_preserves_where_order_and_limit() -> None:
    """El resto de la consulta queda idéntico salvo por la columna autorizada."""
    soql = SoQLBuilder().build(
        "p6dx-8zbt",
        {"departamento_resolved": "ATLANTICO", "objeto": ["vial"],
         "estado": "Abierto", "ordering_signal": "valor_desc"},
    )
    assert "departamento_entidad = 'ATLANTICO'" in soql
    assert "estado_de_apertura_del_proceso = 'Abierto'" in soql
    assert ("ORDER BY precio_base DESC, fecha_de_publicacion_del DESC, "
            "id_del_proceso DESC LIMIT 50") in soql


def test_anti_global_guard_still_refuses_query_without_predicate() -> None:
    """La proyección no relaja el guard: sin WHERE útil sigue siendo error."""
    import pytest

    from app.core.soql_builder import UnsafeGlobalQueryError

    with pytest.raises(UnsafeGlobalQueryError):
        SoQLBuilder().build("p6dx-8zbt", {"limit": 10})
