"""Tests for query_router v2.py (gazetteer-first, production stack).

Uses entity_resolver v3 as fixture — mirrors the production combination
that achieves 95.5% accuracy on the 53-query benchmark.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# ── Load modules with spaces in filename via importlib ───────────────────────
_CORE = Path("app/core")

def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

_qr_mod = _load_module("query_router_v2", _CORE / "query_router v2.py")
_er_mod = _load_module("entity_resolver_v3", _CORE / "entity_resolver v3.py")
QueryRouter = _qr_mod.QueryRouter
EntityResolver = _er_mod.EntityResolver

_ALIAS_DB = Path("app/data/aliases_db.json")


@pytest.fixture
def router() -> QueryRouter:
    """Production-stack router: V2 router + V3 entity resolver."""
    er = EntityResolver(_ALIAS_DB)
    return QueryRouter(entity_resolver=er)


# ═════════════════════════════════════════════════════════════════════════════
# Dataset selection
# ═════════════════════════════════════════════════════════════════════════════

def test_router_selects_procesos_by_default(router: QueryRouter) -> None:
    parsed = router.parse("licitaciones en Antioquia")
    assert parsed.params["dataset"] == "procesos"


def test_router_selects_contratos_on_signal(router: QueryRouter) -> None:
    parsed = router.parse("contratos del SENA en Bogota mayores a 200 millones")
    assert parsed.params["dataset"] == "contratos"


# ═════════════════════════════════════════════════════════════════════════════
# Territorial / multi-param queries
# ═════════════════════════════════════════════════════════════════════════════

def test_router_extracts_territorial_process_query(router: QueryRouter) -> None:
    parsed = router.parse(
        "licitaciones de mantenimiento vial en Atlantico por mas de 500 millones abiertas"
    )

    assert parsed.needs_llm is False
    assert parsed.params["dataset"] == "procesos"
    assert parsed.params.get("departamento_resolved") is not None
    assert "atlántico" in parsed.params["departamento_resolved"].lower() or \
           "atlantico" in parsed.params["departamento_resolved"].lower()
    assert parsed.params["estado"] == "Abierto"
    assert parsed.params["valor_min"] == 500_000_000
    assert "mantenimiento" in parsed.params.get("objeto", [])
    assert "vial" in parsed.params.get("objeto", [])


def test_router_extracts_entity_without_department_confusion(router: QueryRouter) -> None:
    """'gobernacion del Atlantico' should resolve as a single entity, not split."""
    parsed = router.parse("procesos de la gobernacion del Atlantico")

    assert parsed.needs_llm is False
    assert parsed.params.get("entidad_resolved") is not None
    # The entity should contain both gobernacion and atlantico
    resolved = parsed.params["entidad_resolved"].lower()
    assert "atlantico" in resolved or "atlántico" in resolved


def test_router_routes_contract_query_with_entity_and_amount(router: QueryRouter) -> None:
    parsed = router.parse("contratos del SENA en Bogota mayores a 200 millones")

    assert parsed.params["dataset"] == "contratos"
    assert parsed.params.get("entidad_resolved") is not None
    assert "sena" in parsed.params["entidad_resolved"].lower()
    assert parsed.params["valor_min"] == 200_000_000
    assert parsed.params.get("objeto") in (None, [])


# ═════════════════════════════════════════════════════════════════════════════
# Stopwords / noise filtering (regression tests for the actualmente/ahora bug)
# ═════════════════════════════════════════════════════════════════════════════

def test_actualmente_not_in_object(router: QueryRouter) -> None:
    """'actualmente' is a temporal adverb, not a contractual object."""
    parsed = router.parse("procesos de alimentacion abiertos actualmente?")

    obj = parsed.params.get("objeto", [])
    assert "actualmente" not in obj
    assert "actualmente?" not in obj
    assert "alimentacion" in obj


def test_ahora_not_in_object(router: QueryRouter) -> None:
    """'ahora' is a temporal adverb, not a contractual object."""
    parsed = router.parse("hay algun proceso de mantenimiento abierto ahora?")

    obj = parsed.params.get("objeto", [])
    assert "ahora" not in obj
    assert "ahora?" not in obj
    assert "algun" not in obj
    assert "mantenimiento" in obj


def test_hoy_not_in_object(router: QueryRouter) -> None:
    parsed = router.parse("que licitaciones hay hoy abiertas de construccion?")

    obj = parsed.params.get("objeto", [])
    assert "hoy" not in obj
    assert "construccion" in obj


def test_punctuation_stripped_from_tokens(router: QueryRouter) -> None:
    """Question marks and other punctuation should not stick to tokens."""
    parsed = router.parse("licitaciones de pavimentacion?")

    obj = parsed.params.get("objeto", [])
    assert "pavimentacion?" not in obj
    if obj:
        assert "pavimentacion" in obj


# ═════════════════════════════════════════════════════════════════════════════
# Ordering signals
# ═════════════════════════════════════════════════════════════════════════════

def test_ordering_signal_extracted(router: QueryRouter) -> None:
    parsed = router.parse("contratos mas caros de la alcaldia de barranquilla en 2025")

    assert parsed.params.get("ordering_signal") == "valor_desc"
    obj = parsed.params.get("objeto", [])
    # "caros" should NOT leak into object
    assert "caros" not in obj


# ═════════════════════════════════════════════════════════════════════════════
# Date extraction
# ═════════════════════════════════════════════════════════════════════════════

def test_year_extracted_as_date_range(router: QueryRouter) -> None:
    parsed = router.parse("contratos de obra en 2024")

    assert parsed.params.get("fecha_desde") == "2024-01-01"
    assert parsed.params.get("fecha_hasta") == "2024-12-31"


# ═════════════════════════════════════════════════════════════════════════════
# Month stopwords & month+year date extraction
# ═════════════════════════════════════════════════════════════════════════════

def test_months_not_in_object(router: QueryRouter) -> None:
    """'abril' is a month name, not a contractual object."""
    parsed = router.parse("contratos de abril 2026")

    obj = parsed.params.get("objeto", [])
    assert "abril" not in obj


def test_month_year_extraction(router: QueryRouter) -> None:
    """'abril de 2026' → fecha_desde=2026-04-01, fecha_hasta=2026-04-30."""
    parsed = router.parse("contratos de abril de 2026")

    assert parsed.params.get("fecha_desde") == "2026-04-01"
    assert parsed.params.get("fecha_hasta") == "2026-04-30"


def test_month_year_del_mes(router: QueryRouter) -> None:
    """'del mes de enero de 2025' → fecha_desde=2025-01-01, fecha_hasta=2025-01-31."""
    parsed = router.parse("del mes de enero de 2025")

    assert parsed.params.get("fecha_desde") == "2025-01-01"
    assert parsed.params.get("fecha_hasta") == "2025-01-31"


# ═════════════════════════════════════════════════════════════════════════════
# Regression: gobernacion de santander 2024 → SANTANDER, not CALDAS
# ═════════════════════════════════════════════════════════════════════════════

def test_gobernacion_de_santander_2024_resolves_correctly(router: QueryRouter) -> None:
    """'gobernacion de santander 2024' must resolve to SANTANDER, not CALDAS.

    Regression for bug where year stripping + department-first extraction caused
    'gobernacion de santander' to be split, leaving only 'gobernacion' which
    fuzzy-matched to GOBERNACION DE CALDAS.
    """
    parsed = router.parse("gobernacion de santander 2024")

    resolved = parsed.params.get("entidad_resolved", "")
    assert "SANTANDER" in resolved.upper(), (
        f"Expected SANTANDER in entity, got {resolved!r}"
    )
    assert "CALDAS" not in resolved.upper(), (
        f"Must NOT resolve to CALDAS, got {resolved!r}"
    )
    assert parsed.params.get("fecha_desde") == "2024-01-01"
    assert parsed.params.get("fecha_hasta") == "2024-12-31"


def test_contratos_mas_caros_gobernacion_de_santander_2024(router: QueryRouter) -> None:
    """Full phrase: 'contratos mas caros de la gobernacion de santander 2024'."""
    parsed = router.parse("contratos mas caros de la gobernacion de santander 2024")

    resolved = parsed.params.get("entidad_resolved", "")
    assert "SANTANDER" in resolved.upper(), (
        f"Expected SANTANDER in entity, got {resolved!r}"
    )
    assert "CALDAS" not in resolved.upper(), (
        f"Must NOT resolve to CALDAS, got {resolved!r}"
    )
    assert parsed.params["dataset"] == "contratos"
    assert parsed.params.get("ordering_signal") == "valor_desc"
    assert parsed.params.get("fecha_desde") == "2024-01-01"
