"""Tests STATE-CATALOG-INCOMPLETE-001 — catalog expansion + query matching."""
from __future__ import annotations

import pytest

from app.core.estado_families import detect_estado_family, resolve_estado, ESTADO_FAMILIES
from app.core.query_router import QueryRouter


# ── Catalog completeness ──────────────────────────────────────────────────

def test_catalog_has_aprobado_en_evaluacion_procesos():
    """Aprobado + En aprobación existen en estado_del_procedimiento (246K records)."""
    family = ESTADO_FAMILIES["en_evaluacion"]["procesos"]
    values = family["estado_del_procedimiento"]
    assert "Aprobado" in values
    assert "En aprobación" in values


def test_catalog_has_borrador_en_contratos():
    """Borrador existe en estado_contrato (233K records)."""
    family = ESTADO_FAMILIES["en_evaluacion"]["contratos"]
    values = family["estado_contrato"]
    assert "Borrador" in values


# ── Query resolution ─────────────────────────────────────────────────────

def test_procesos_publicados_tolima_resolves_publicado():
    """'procesos publicados en Tolima' → estado_del_procedimiento IN ('Publicado','Borrador','Abierto')"""
    r = detect_estado_family("procesos publicados en tolima", "procesos")
    assert r is not None
    assert r["family"] == "oferta_abierta"
    filters = r["filters"]
    assert "estado_del_procedimiento" in filters
    assert "Publicado" in filters["estado_del_procedimiento"]


def test_procesos_abiertos_en_tolima_resolves_abierto():
    """'procesos abiertos en Tolima' → oferta_abierta"""
    r = detect_estado_family("procesos abiertos en tolima", "procesos")
    assert r is not None
    assert r["family"] == "oferta_abierta"


def test_query_router_includes_publicado_for_publicados():
    """QueryRouter.parse incluye estado_del_procedimiento para 'publicados'."""
    router = QueryRouter()
    result = router.parse("procesos publicados en tolima")
    params = result.params
    # Debe tener estado_del_procedimiento con Publicado
    edp = params.get("estado_del_procedimiento")
    assert edp is not None
    assert "Publicado" in edp


# ── Known duplicates (Sprint 5) ───────────────────────────────────────

# Seleccionado aparece en en_evaluacion y contrato_activo. Es intencional:
# un proceso en "Seleccionado" puede ser evaluación pre-adjudicación
# o contrato activo ya adjudicado — depende de contexto adicional.
_KNOWN_OVERLAPS = {
    "procesos:estado_del_procedimiento:Seleccionado",
}


def test_catalog_no_unexpected_duplicates():
    """Verificar que solo los overlaps conocidos existen entre familias."""
    from app.core.estado_families import ESTADO_FAMILIES

    seen_procesos: dict[str, str] = {}
    for family_name, family in ESTADO_FAMILIES.items():
        for field, values in family.get("procesos", {}).items():
            for v in values:
                key = f"procesos:{field}:{v}"
                if key in seen_procesos:
                    # Allow known intentional overlaps
                    assert key in _KNOWN_OVERLAPS, (
                        f"Unexpected duplicate {key} in families "
                        f"{seen_procesos[key]} and {family_name}"
                    )
                seen_procesos[key] = family_name

    seen_contratos: dict[str, str] = {}
    for family_name, family in ESTADO_FAMILIES.items():
        for field, values in family.get("contratos", {}).items():
            for v in values:
                key = f"contratos:{field}:{v}"
                if key in seen_contratos:
                    assert key in _KNOWN_OVERLAPS, (
                        f"Unexpected duplicate {key} in families "
                        f"{seen_contratos[key]} and {family_name}"
                    )
                seen_contratos[key] = family_name
