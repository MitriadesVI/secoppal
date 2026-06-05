"""ENTITY-TRAILING-001 — resolución de entidad robusta a tokens trailing.

Bug (preexistente): el patrón "alcaldía de X" en entity_types.py capturaba X de
forma greedy hasta el siguiente conector o el fin de línea, tragándose tokens
que NO son parte del nombre de la entidad ("este mes", "pavimentacion", basura)
→ no matcheaba el gazetteer y NO había fallback → entidad=None.

Fix: shrink-fallback en rewrite_entity (span más largo→más corto, el caller se
queda con el prefijo válido más largo) + scrub de "mes/meses" y verbos
interrogativos ("hecho/hizo/...") para que el leftover no contamine el objeto.

Disparador original: la pregunta natural "¿qué ha hecho la alcaldía de X
[este mes]?" del doc de Radar (docs/ideas/secoppal-radar.md).
"""
from __future__ import annotations

from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter

_BQ = "BARRANQUILLA"  # substring estable del nombre oficial del Distrito


def _parse(q: str) -> dict:
    return dict(QueryRouter().parse(q).params)


def _plan(q: str) -> dict:
    params = _parse(q)
    return compute_query_plan(params, q, enabled_rules=None).params


def _objeto(p: dict) -> list[str]:
    return [t for t in (p.get("objeto") or []) if isinstance(t, str)]


# ── EL FIX: entidad seguida de un token trailing ──────────────────────────

def test_entidad_con_tema_trailing_resuelve_ambos():
    """'la alcaldia de barranquilla pavimentacion' → entidad + objeto, no None."""
    p = _parse("la alcaldia de barranquilla pavimentacion")
    assert p.get("entidad_resolved") and _BQ in p["entidad_resolved"]
    assert "pavimentacion" in _objeto(p)


def test_entidad_con_temporal_relativo_trailing_resuelve():
    """'la alcaldia de barranquilla este mes' → entidad resuelve, objeto limpio."""
    p = _parse("la alcaldia de barranquilla este mes")
    assert p.get("entidad_resolved") and _BQ in p["entidad_resolved"]
    assert "mes" not in _objeto(p)


def test_entidad_con_token_basura_trailing_resuelve():
    p = _parse("la alcaldia de barranquilla zzz")
    assert p.get("entidad_resolved") and _BQ in p["entidad_resolved"]


def test_pregunta_natural_que_ha_hecho_end_to_end():
    """El caso del doc de Radar: entidad + verbo scrubeado + fecha relativa."""
    p = _plan("que ha hecho la alcaldia de barranquilla este mes")
    assert p.get("entidad_resolved") and _BQ in p["entidad_resolved"]
    assert _objeto(p) == []  # 'hecho' y 'mes' no contaminan el objeto
    assert p.get("fecha_desde") and p.get("fecha_hasta")  # 'este mes' → rango


def test_que_hizo_verbo_no_contamina_objeto():
    p = _parse("que hizo la alcaldia de barranquilla")
    assert p.get("entidad_resolved") and _BQ in p["entidad_resolved"]
    assert "hizo" not in _objeto(p)


# ── REGRESIÓN: casos que ya funcionaban deben seguir igual ────────────────

def test_regresion_entidad_sola():
    p = _parse("la alcaldia de barranquilla")
    assert p.get("entidad_resolved") and _BQ in p["entidad_resolved"]


def test_regresion_tema_antes_de_entidad():
    p = _parse("pavimentacion de la alcaldia de barranquilla")
    assert p.get("entidad_resolved") and _BQ in p["entidad_resolved"]
    assert "pavimentacion" in _objeto(p)


def test_regresion_entidad_multipalabra_no_sobre_encoge():
    """'santa marta' NO debe encogerse a 'santa' — longest-prefix-that-matches."""
    p = _parse("alcaldia de santa marta")
    assert p.get("entidad_resolved")
    assert "SANTA MARTA" in p["entidad_resolved"].upper()


def test_regresion_gobernacion():
    p = _parse("gobernacion del atlantico")
    assert p.get("entidad_resolved") and "ATLANTICO" in p["entidad_resolved"].upper()


def test_regresion_multiregional_con_temporal():
    p = _plan("que ha hecho el sena este mes")
    assert p.get("entidad_resolved") and "SENA" in p["entidad_resolved"].upper()
    assert p.get("fecha_desde")
