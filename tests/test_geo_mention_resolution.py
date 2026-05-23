"""Tests for geo_mention_resolution — pure gazetteer lookup."""

from app.core.entity_resolver import GeoMention, geo_mention_resolution


def test_geo_mention_city_known():
    res = geo_mention_resolution("ipiales")
    assert res.kind == "city"
    assert res.canonical == "Ipiales"
    assert res.confidence == "high"


def test_geo_mention_department_known():
    res = geo_mention_resolution("bolívar")
    assert res.kind == "department"
    assert res.canonical == "Bolívar"


def test_geo_mention_unknown_returns_unknown():
    res = geo_mention_resolution("xyzzyfoobar")
    assert res.kind == "unknown"
    assert res.canonical is None


def test_geo_mention_case_insensitive():
    assert geo_mention_resolution("IPIALES").canonical == "Ipiales"
    assert geo_mention_resolution("Ipiales").canonical == "Ipiales"


def test_geo_mention_strips_whitespace():
    res = geo_mention_resolution("  barranquilla  ")
    assert res.kind == "city"
    assert res.canonical == "Barranquilla"


def test_geo_mention_empty_string():
    res = geo_mention_resolution("")
    assert res.kind == "unknown"


def test_geo_mention_backlog_cities():
    # Cities from CORPUS_BACKLOG.md that must be recognized
    for token, canonical in [
        ("ipiales", "Ipiales"),
        ("olaya", "Olaya"),
        ("yarumal", "Yarumal"),
        ("barranquilla", "Barranquilla"),
    ]:
        res = geo_mention_resolution(token)
        assert res.kind == "city", f"{token} should be a city"
        assert res.canonical == canonical


def test_geo_mention_city_before_department():
    # "arauca" is both a city AND a department — city wins
    # (checked first in GAZETTEER_CIUDADES)
    res = geo_mention_resolution("arauca")
    assert res.kind == "city"
    assert res.canonical == "Arauca"
