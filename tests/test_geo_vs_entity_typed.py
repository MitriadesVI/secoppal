"""Etapa 7 — resolución TIPADA: territorio (departamento) vs comprador (entidad).

Invariante (INV-7): "Gobernación de X" ≠ "departamento de X" → WHERE distintos.
NO se exige que el valor contenga la palabra del usuario: en SECOP el comprador de la
Gobernación del Atlántico se registra como "DEPARTAMENTO DEL ATLANTICO**" (no "GOBERNACION"),
mientras que el de Nariño sí es "GOBERNACION DE NARIÑO". Ambos estilos reales se cubren.
"""
from __future__ import annotations

import re

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


# ── INV-7: gobernación ≠ departamento → WHERE distintos ──────────────────────

def test_inv7_buyer_and_territory_produce_distinct_where():
    buyer = _parse("contratos de la Gobernación del Atlántico")
    territory = _parse("contratos del departamento del Atlántico")
    wb, wt = _where(buyer), _where(territory)
    assert wb != wt                                                      # WHERE distintos
    # el comprador filtra por nombre_entidad; el territorio por el campo departamento.
    assert "nombre_entidad" in wb and "nombre_entidad" not in wt
    assert re.search(r"\bdepartamento\b\s*=", wt)                        # territorio: departamento = 'X'
