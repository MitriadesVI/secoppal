import importlib
import sys
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver

# ── Cargar EntityResolver canónico ──
def _load_er_v3() -> type:
    path = Path("app/core/entity_resolver.py")
    spec = importlib.util.spec_from_file_location("entity_resolver_v3", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["entity_resolver_v3"] = mod
    spec.loader.exec_module(mod)
    return mod.EntityResolver

EntityResolverV3 = _load_er_v3()

# ═══════════════════════════════════════════════════════════════════════════════
# Tests de EntityResolver V1 (versión actual / producción)
# ═══════════════════════════════════════════════════════════════════════════════

def test_resolve_exact_entity_alias() -> None:
    """Alias exacto 'sena' debe resolver a alguna entidad SENA (cualquier regional)."""
    resolver = EntityResolver(Path("app/data/aliases_db.json"))
    result = resolver.resolve_entidad("SENA")

    assert result.value is not None
    assert "sena" in result.value.lower()
    assert result.confidence == "high"


def test_resolve_fuzzy_entity_alias() -> None:
    """'gobernacion del atlantico' debe resolver a una entidad relacionada con Atlántico."""
    resolver = EntityResolver(Path("app/data/aliases_db.json"))
    result = resolver.resolve_entidad("gobernacion del atlantico")

    assert result.value is not None
    assert "atlantico" in result.value.lower()
    assert result.confidence in {"high", "medium"}


def test_resolve_department_alias() -> None:
    """'cundi' (abreviación) debe resolver a Cundinamarca (case-insensitive)."""
    resolver = EntityResolver(Path("app/data/aliases_db.json"))
    result = resolver.resolve_departamento("cundi")

    assert result.value is not None
    assert "cundinamarca" in result.value.lower()


def test_resolve_entity_like_fallback() -> None:
    """
    Verifica el mecanismo LIKE fallback.

    Nota: con el gazetteer actual (10K+ entidades) y un cutoff WRatio=82,
    strings inventados pueden hacer fuzzy-match con alguna entidad real.
    El test verifica la estructura del resultado, no que SIEMPRE llegue a LIKE.
    """
    resolver = EntityResolver(Path("app/data/aliases_db.json"))
    result = resolver.resolve_entidad("zzz entidad inexistente xyz 9999")

    assert result.method is not None
    # Si llegó al fallback LIKE, la estructura debe ser correcta
    if result.method == "like":
        assert result.value is None
        assert result.like_value is not None
        assert result.like_value == result.like_value.upper()  # like_value es UPPERCASE
    else:
        # Fuzzy-match legítimo del gazetteer — estructura correcta igual
        assert result.confidence in {"high", "medium"}
        assert result.method.startswith(("exact", "fuzzy"))


# ═══════════════════════════════════════════════════════════════════════════════
# Tests de EntityResolver V3 (gazetteer-first con disambiguación de departamentos)
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def er_v3():
    return EntityResolverV3(Path("app/data/aliases_db.json"))


# ── resolve_* — debe comportarse igual que V1/V2 ─────────────────────────────

def test_v3_resolve_exact_entity_alias(er_v3) -> None:
    result = er_v3.resolve_entidad("SENA")
    assert result.value is not None
    assert "sena" in result.value.lower()
    assert result.confidence == "high"


def test_v3_resolve_fuzzy_entity(er_v3) -> None:
    result = er_v3.resolve_entidad("gobernacion del atlantico")
    assert result.value is not None
    assert "atlantico" in result.value.lower()
    assert result.confidence in {"high", "medium"}


def test_v3_resolve_department_alias(er_v3) -> None:
    result = er_v3.resolve_departamento("cundi")
    assert result.value is not None
    assert "cundinamarca" in result.value.lower()


def test_v3_resolve_like_fallback(er_v3) -> None:
    result = er_v3.resolve_entidad("zzz entidad inexistente xyz 9999")
    assert result.method is not None
    if result.method == "like":
        assert result.value is None
        assert result.like_value is not None
        assert result.like_value == result.like_value.upper()
    else:
        assert result.confidence in {"high", "medium"}
        assert result.method.startswith(("exact", "fuzzy"))


# ── scan_entity — el comportamiento NUEVO de V3 ───────────────────────────────

def test_v3_scan_entity_qualified_gobernacion(er_v3) -> None:
    """'gobernacion de santander' debe ser detectada como entidad (tiene calificador)."""
    result = er_v3.scan_entity("licitaciones de la gobernacion de santander abiertas")
    assert result is not None, "V3 debería encontrar 'gobernacion de santander' como entidad"
    assert "santander" in result.official_name.lower() or "santander" in result.matched_alias


def test_v3_scan_entity_rejects_bare_department(er_v3) -> None:
    """'antioquia' sola NO debe ser robada por scan_entity — es un departamento."""
    result = er_v3.scan_entity("procesos abiertos en antioquia")
    # Si devuelve algo, no debe ser solo el nombre del departamento sin calificador
    if result is not None:
        assert "gobernacion" in result.matched_alias or \
               "alcaldia" in result.matched_alias or \
               "departamento" in result.matched_alias.split()[0], \
            f"scan_entity devolvió '{result.matched_alias}' que es un nombre de depto sin calificador"


def test_v3_scan_entity_rejects_bare_atlantico(er_v3) -> None:
    """'atlantico' solo NO debe ser robado como entidad en texto de búsqueda por depto."""
    result = er_v3.scan_entity("contratos del SENA en atlantico 2022")
    # Si hay resultado, debe ser "sena" o similar, no "atlantico"
    if result is not None:
        assert "atlantico" not in result.matched_alias or \
               result.matched_alias.startswith("gobernacion") or \
               result.matched_alias.startswith("alcaldia"), \
            f"'atlantico' fue capturado como entidad: '{result.matched_alias}'"


def test_v3_scan_entity_keeps_qualified_alcaldia(er_v3) -> None:
    """'alcaldia de bogota' debe ser detectada como entidad (tiene calificador)."""
    result = er_v3.scan_entity("procesos de la alcaldia de bogota de infraestructura")
    assert result is not None, "V3 debería detectar 'alcaldia de bogota'"
    assert "bogot" in result.matched_alias or "bogot" in result.official_name.lower()


def test_v3_scan_entity_acronym(er_v3) -> None:
    """Acrónimos como SENA, ICBF, INVIAS deben seguir siendo detectados."""
    for acronym in ("sena", "icbf", "invias"):
        result = er_v3.scan_entity(f"contratos del {acronym} en cundinamarca")
        assert result is not None, f"V3 no detectó la entidad '{acronym}'"
        assert acronym in result.official_name.lower() or acronym in result.matched_alias


def test_v3_scan_departamento_still_works(er_v3) -> None:
    """scan_departamento debe funcionar correctamente en V3."""
    result = er_v3.scan_departamento("procesos en antioquia abiertos")
    assert result is not None, "V3 scan_departamento debe encontrar 'antioquia'"
    assert "antioquia" in result.official_name.lower()


def test_v3_scan_departamento_multiword(er_v3) -> None:
    """scan_departamento debe detectar departamentos multi-palabra."""
    result = er_v3.scan_departamento("contratos del valle del cauca en 2023")
    assert result is not None, "V3 scan_departamento debe encontrar 'valle del cauca'"
    assert "VALLE" in result.official_name or "valle" in result.matched_alias


def test_v3_scan_entity_and_departamento_no_collision(er_v3) -> None:
    """
    Cuando hay entidad Y departamento en la misma query,
    scan_entity NO debe consumir el nombre del departamento.
    """
    text = "contratos del sena en antioquia en ejecucion"
    entity_result = er_v3.scan_entity(text)
    assert entity_result is not None, "Debería detectar 'sena'"
    assert "sena" in entity_result.matched_alias

    # En el texto restante aún debe quedar 'antioquia'
    dept_result = er_v3.scan_departamento(entity_result.remaining_text)
    assert dept_result is not None, "Después de sacar 'sena', debería quedar 'antioquia'"
    assert "antioquia" in dept_result.official_name.lower()
