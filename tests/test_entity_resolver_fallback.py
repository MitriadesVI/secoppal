from pathlib import Path

from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter


def _resolver() -> EntityResolver:
    return EntityResolver(Path("app/data/aliases_db.json"))


def test_alcaldia_paipa_returns_null_not_manizales():
    resolution = _resolver().resolve_entidad("Municipio de paipa")

    assert resolution.central_entity is None
    assert resolution.confidence == "low"
    assert resolution.clarification_needed is True
    assert resolution.ecosystem_entities == []


def test_alcaldia_medellin_returns_null_not_inder():
    resolution = _resolver().resolve_entidad("Municipio de medellin")

    if resolution.central_entity is not None:
        assert "MEDELLIN" in resolution.central_entity.upper()
        assert "INDER" not in resolution.central_entity.upper()
        assert "DEPORTES" not in resolution.central_entity.upper()
    else:
        assert resolution.clarification_needed is True


def test_gobernacion_bolivar_returns_null_not_icbf():
    resolution = _resolver().resolve_entidad("Departamento de bolivar")

    if resolution.central_entity is not None:
        assert "ICBF" not in resolution.central_entity.upper()
        assert "BIENESTAR FAMILIAR" not in resolution.central_entity.upper()
    else:
        assert resolution.clarification_needed is True


def test_rewrite_parser_does_not_accept_null_as_resolved_entity():
    parsed = QueryRouter(_resolver()).parse("contratos de la alcaldía de paipa")

    assert "entidad_resolved" not in parsed.params
    resolution = parsed.params.get("entidad_resolution", {})
    assert resolution.get("confidence") == "low"
    assert resolution.get("clarification_needed") is True
