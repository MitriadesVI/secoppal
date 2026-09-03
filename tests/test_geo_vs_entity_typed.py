"""Etapa 7 — resolución TIPADA: territorio (departamento) vs comprador (entidad).

Invariante (INV-7): "Gobernación de X" ≠ "departamento de X" → WHERE distintos.
NO se exige que el valor contenga la palabra del usuario: en SECOP el comprador de la
Gobernación del Atlántico se registra como "DEPARTAMENTO DEL ATLANTICO**" (no "GOBERNACION"),
mientras que el de Nariño sí es "GOBERNACION DE NARIÑO". Ambos estilos reales se cubren.
"""
from __future__ import annotations

import re

import pytest

from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


def _parse(q: str) -> dict:
    return QueryRouter().parse(q).params


def _where(params: dict) -> str:
    b = SoQLBuilder()
    did = SoQLBuilder.dataset_id_for(params.get("dataset"))
    soql = b.build(did, params)
    return soql.split("WHERE", 1)[1].split("ORDER BY", 1)[0] if "WHERE" in soql else ""


# ── Comprador (buyer_entity) ─────────────────────────────────────────────────

def test_gobernacion_atlantico_is_buyer_entity():
    p = _parse("contratos de la Gobernación del Atlántico")
    assert p.get("entidad_resolved") == "DEPARTAMENTO DEL ATLANTICO**"   # comprador REAL
    assert p["entidad_resolution"]["kind"] == "buyer_entity"
    assert not p.get("departamento_resolved")                            # NO es territorio
    assert re.search(r"entidad", _where(p))                              # filtra por entidad


def test_gobernacion_narino_is_buyer_entity_named_gobernacion():
    # Caso complementario: gobernación que SÍ se llama "GOBERNACION DE X" en SECOP.
    p = _parse("contratos de la Gobernación de Nariño")
    assert p.get("entidad_resolved") == "GOBERNACION DE NARIÑO"
    assert p["entidad_resolution"]["kind"] == "buyer_entity"
    assert not p.get("departamento_resolved")


@pytest.mark.parametrize(
    ("query", "buyer"),
    [
        (
            "contratos de la Gobernación de Arauca en 2025",
            "DEPARTAMENTO DEL ARAUCA",
        ),
        (
            "contratos de la Gobernación del Atlántico en 2025",
            "DEPARTAMENTO DEL ATLANTICO**",
        ),
    ],
)
def test_governacion_context_remains_buyer(query, buyer):
    p = _parse(query)
    assert p.get("entidad_resolved") == buyer
    assert p["entidad_resolution"]["kind"] == "buyer_entity"
    assert not p.get("departamento_resolved")


# ── Territorio (territorial_department) ───────────────────────────────────────

def test_departamento_atlantico_is_territorial():
    p = _parse("contratos del departamento del Atlántico")
    assert p.get("departamento_resolved") == "Atlántico"
    assert p["departamento_resolution"]["kind"] == "territorial_department"
    assert not p.get("entidad_resolved")                                 # NO es comprador
    w = _where(p)
    assert "departamento" in w


def test_departamento_multiword_territorial():
    p = _parse("contratos en el departamento del Valle del Cauca")
    assert p.get("departamento_resolved") == "Valle del Cauca"
    assert p["departamento_resolution"]["kind"] == "territorial_department"
    assert not p.get("entidad_resolved")


@pytest.mark.parametrize(
    ("query", "canonical"),
    [
        ("contratos en el departamento de Arauca en 2025", "Arauca"),
        ("contratos en el departamento del Arauca en 2025", "Arauca"),
        (
            "contratos en el departamento de San Andrés en 2025",
            "San Andrés, Providencia y Santa Catalina",
        ),
        ("contratos en el departamento de Antioquia en 2025", "Antioquia"),
    ],
)
def test_explicit_department_context_uses_territorial_authority(query, canonical):
    p = _parse(query)
    assert p.get("departamento_resolved") == canonical
    assert p["departamento_resolution"]["kind"] == "territorial_department"
    assert not p.get("entidad_resolved")
    assert not p.get("ciudad")


def test_untyped_arauca_preserves_department_candidate_but_requires_type():
    p = _parse("contratos en Arauca en 2025")
    assert p.get("departamento_resolved") == "Arauca"
    from app.core.query_plan import compute_query_plan
    assert p["municipal_territory"]["status"] == "ambiguous"
    assert not compute_query_plan(p, "contratos en Arauca en 2025").should_execute
    assert not p.get("entidad_resolved")
    assert not p.get("ciudad")


# ── Canarios (asesor): el patrón territorial NO debe colapsar lateralmente ───

def test_departamento_administrativo_is_not_territory():
    # "Departamento Administrativo de la Presidencia" es una ENTIDAD, no territorio.
    # El patrón territorial ("departamento de X") NO debe dispararse (no es "departamento de").
    p = _parse("contratos del Departamento Administrativo de la Presidencia")
    assert (p.get("departamento_resolution") or {}).get("method") != "territorial_pattern"
    assert not p.get("departamento_resolved") or \
        (p.get("departamento_resolution") or {}).get("method") == "scan_exact"
    assert p.get("entidad_resolved")                                     # resuelve a entidad


def test_territorial_pattern_only_fires_for_real_departments():
    # Dispara con un departamento real…
    p = _parse("contratos del departamento de Antioquia")
    assert (p["departamento_resolution"]).get("method") == "territorial_pattern"
    assert p["departamento_resolved"] == "Antioquia"
    # …pero NO con tokens que no son un departamento reconocido.
    q = _parse("contratos del departamento de cosas raras")
    assert (q.get("departamento_resolution") or {}).get("method") != "territorial_pattern"


# ── INV-7: gobernación ≠ departamento → WHERE distintos ──────────────────────

def test_inv7_buyer_and_territory_produce_distinct_where():
    buyer = _parse("contratos de la Gobernación del Atlántico")
    territory = _parse("contratos del departamento del Atlántico")
    wb, wt = _where(buyer), _where(territory)
    assert wb != wt                                                      # WHERE distintos
    # el comprador filtra por nombre_entidad; el territorio por el campo departamento.
    assert "nombre_entidad" in wb and "nombre_entidad" not in wt
    assert re.search(r"\bdepartamento\b\s*=", wt)                        # territorio: departamento = 'X'
