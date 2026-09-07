"""Global parity for occurrence-owned procurement source words."""
from __future__ import annotations

import json

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter


@pytest.mark.parametrize(("query", "kept", "consumed"), [
    (
        "contratos de gestion de procesos industriales",
        {"gestion", "procesos", "industriales"},
        {"contratos"},
    ),
    (
        "contratos de automatizacion de procesos",
        {"automatizacion", "procesos"},
        {"contratos"},
    ),
    (
        "contratos de administracion de procesos",
        {"administracion", "procesos"},
        {"contratos"},
    ),
    (
        "procesos de administracion de contratos",
        {"administracion", "contratos"},
        {"procesos"},
    ),
    (
        "procesos de gestion contractual",
        {"gestion"},
        {"procesos"},
    ),
])
def test_only_the_source_head_is_consumed_from_topic(query, kept, consumed):
    topic = set(QueryRouter().parse(query).params.get("objeto", ()))
    assert kept <= topic
    assert topic.isdisjoint(consumed)


@pytest.mark.parametrize("query", ["procesos y contratos", "procesos o contratos"])
def test_real_multi_source_language_does_not_become_topic(query):
    topic = QueryRouter().parse(query).params.get("objeto", ())
    assert "procesos" not in topic and "contratos" not in topic


@pytest.mark.parametrize("buyer", [
    "agencia nacional de procesos",
    "unidad administrativa de contratos",
])
def test_source_words_inside_an_accredited_buyer_name_are_not_topic(tmp_path, buyer):
    aliases = tmp_path / "aliases.json"
    aliases.write_text(json.dumps([{
        "official_name": buyer.upper(),
        "aliases": [buyer],
        "source_datasets": ["procesos", "contratos"],
    }]), encoding="utf-8")
    params = QueryRouter(EntityResolver(aliases)).parse(
        f"contratos de software de {buyer}",
    ).params
    assert params["entidad_resolved"] == buyer.upper()
    assert params.get("objeto") == ["software"]
