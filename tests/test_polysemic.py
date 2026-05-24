"""Tests POLYSEMIC-TOPIC-001 — polysemic topic resolution."""
from __future__ import annotations

from app.core.polysemic import detect_polysemic_terms, resolve_polysemic, build_clarification


def test_detect_alojamiento_is_polysemic():
    terms = ["alojamiento", "obra"]
    detected = detect_polysemic_terms(terms)
    assert "alojamiento" in detected
    assert "obra" not in detected


def test_detect_transporte_is_polysemic():
    terms = ["transporte", "dotacion"]
    detected = detect_polysemic_terms(terms)
    assert "transporte" in detected


def test_auto_resolve_alojamiento_hotelero():
    """'alojamiento para turistas y huespedes' → hotelero (2+ markers)"""
    r = resolve_polysemic("alojamiento", query_text="alojamiento para turistas y huespedes en hoteles")
    assert r.auto_resolved is True
    assert r.sense_id == "hospedaje_hotelero"


def test_auto_resolve_transporte_escolar():
    """'transporte escolar para estudiantes' → pasajeros (2+ markers)"""
    r = resolve_polysemic("transporte", query_text="transporte escolar con buses para estudiantes")
    assert r.auto_resolved is True
    assert r.sense_id == "transporte_pasajeros"


def test_no_context_returns_clarification():
    """'alojamiento' solo → sin contexto, pregunta"""
    r = resolve_polysemic("alojamiento", query_text="necesito alojamiento")
    assert r.auto_resolved is False
    assert r.clarification_hint is not None
    assert "hospedaje" in r.clarification_hint or "hosting" in r.clarification_hint


def test_unknown_term_returns_unresolved():
    r = resolve_polysemic("pavimentacion")
    assert r.resolved is False
    assert r.auto_resolved is False


def test_build_clarification_for_unresolved():
    r = resolve_polysemic("alojamiento", query_text="busco alojamiento")
    msg = build_clarification(r)
    assert msg is not None
    assert "hospedaje" in msg or "hosting" in msg


def test_build_clarification_for_resolved_returns_none():
    r = resolve_polysemic("alojamiento", query_text="hoteles y hospedaje para turistas")
    msg = build_clarification(r)
    assert msg is None


def test_dotacion_personal_auto_resolves():
    """'dotacion de uniformes para empleados' → personal"""
    r = resolve_polysemic("dotacion", query_text="dotacion de uniformes y calzado para empleados")
    assert r.auto_resolved is True
    assert r.sense_id == "dotacion_personal"
