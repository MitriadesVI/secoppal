"""SIP-049: prueba de generalidad — 'aire acondicionado' por sola entrada de datos.

Mismo flujo que 'manual de contratacion', cero `if` especial: detección →
objeto_phrases → WHERE recall-first → validador. Si esto pasa, la capa es
reutilizable, no una solución a medida para manuales.
"""
from __future__ import annotations

from app.core.phrase_catalog import detect_phrases, spec_for_canonical
from app.core.phrase_validator import validate_phrase
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder

SPEC = spec_for_canonical("aire acondicionado")
DS = "jbjy-vk9h"


def test_spec_is_in_catalog():
    assert SPEC is not None
    assert SPEC.ordered_anchors == ("aire", "acondicionado")


def test_detection_singular_and_plural():
    assert detect_phrases("mantenimiento de aire acondicionado")[0] == ["aire acondicionado"]
    assert detect_phrases("compra de aires acondicionados")[0] == ["aire acondicionado"]


def test_parser_produces_objeto_phrases():
    params = QueryRouter().parse(
        "contratos para mantenimiento de aires acondicionados en 2024"
    ).params
    assert params.get("objeto_phrases") == ["aire acondicionado"]
    flat = [t for grp in params.get("objeto", []) for t in (grp if isinstance(grp, list) else [grp])]
    assert "aire" not in flat and "acondicionado" not in flat  # no recrea el objeto roto


def test_validator_levels():
    assert validate_phrase(SPEC, "SUMINISTRO DE AIRE ACONDICIONADO", "").level == "direct"
    assert validate_phrase(SPEC, "instalacion de aires acondicionados", "").level == "direct"
    # ancla en campos distintos → no es frase
    assert validate_phrase(SPEC, "compra de aire", "equipo acondicionado").level == "none"


def test_builder_recall_first_clause():
    sql = SoQLBuilder().build(DS, {"objeto_phrases": ["aire acondicionado"]}).lower()
    assert "aire" in sql and "acondicionado" in sql
    assert "1=1" not in sql
    # anclas co-ocurrentes en el mismo campo (objeto aparece para ambas anclas)
    assert sql.count("objeto_del_contrato") >= 2
