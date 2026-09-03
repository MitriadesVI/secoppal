"""La mención acreditada de SDIS conserva su intención antes del objeto."""
import pytest

from app.core.query_router import QueryRouter
from app.core.query_plan import compute_query_plan


@pytest.mark.parametrize("prefix", ["", "contratos de ", "muestrame que ha publicado "])
def test_sdis_territory_qualified_mention_survives_parser_and_plan(prefix):
    query = prefix + "integracion social de bogota en adulto mayor en 2026"
    params = QueryRouter().parse(query).params
    plan = compute_query_plan(params, query)
    for result in (params, plan.params):
        assert result.get("entidad_resolved") == "(Secretaría Distrital de Integración Social)"
        terms = str(result.get("objeto", [])).lower()
        assert "integracion" not in terms and "social" not in terms
        assert "adulto" in terms
        assert result["fecha_desde"] == "2026-01-01"
    assert plan.should_execute


def test_bogota_alone_is_geography():
    params = QueryRouter().parse("contratos en bogota").params
    assert not params.get("entidad_resolved")
    assert params["departamento_resolved"] == "Distrito Capital de Bogotá"
