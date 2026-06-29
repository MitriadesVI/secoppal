"""Etapa 3 — auditoría diferencial de recall (predicate waterfall por existencia).

Tests deterministas: un cliente falso decide existencia inspeccionando el SoQL
(substring), así se verifica que el WHERE del builder REALMENTE incluye cada
predicado y que el waterfall localiza el que vacía el conjunto. Sin red.
"""
from __future__ import annotations

from app.core.recall_audit import (
    build_predicate_waterfall,
    killer_predicate,
    present_layers,
    waterfall_to_dict,
)


class FakeClient:
    """Devuelve [] cuando el SoQL contiene cualquiera de los marcadores 'asesinos'."""

    def __init__(self, empty_when_contains: list[str] | None = None):
        self.empty_when_contains = empty_when_contains or []
        self.queries: list[str] = []
        self.counts: list[str] = []

    def query(self, dataset_id: str, soql_query: str) -> list[dict]:
        self.queries.append(soql_query)
        if any(marker in soql_query for marker in self.empty_when_contains):
            return []
        return [{"id_del_proceso": "CO1.NTC.1"}]

    def count(self, dataset_id: str, soql_count_query: str) -> int:
        self.counts.append(soql_count_query)
        return 0 if any(m in soql_count_query for m in self.empty_when_contains) else 7


_ASCENSORES = {
    "objeto": ["mantenimiento", "ascensores"],
    "ciudad": "Medellin",
    "valor_min": 100000000,
    "valor_max": 150000000,
    "dataset": "procesos",
}


def test_present_layers_orders_and_filters():
    layers = present_layers(_ASCENSORES)
    names = [name for name, _ in layers]
    assert names == ["tema", "scope", "valor"]  # fecha/estado/modalidad ausentes


def test_value_predicate_is_the_killer():
    # El filtro de valor genera el rango sobre precio_base; lo marcamos asesino.
    client = FakeClient(empty_when_contains=["100000000"])
    steps = build_predicate_waterfall(_ASCENSORES, client)

    assert [s.predicate for s in steps] == ["tema", "scope", "valor"]
    assert [s.exists for s in steps] == [True, True, False]
    assert killer_predicate(steps) == "valor"
    # se detiene en el killer: no audita capas posteriores
    assert steps[-1].killer_predicate is True


def test_scope_predicate_is_the_killer_stops_early():
    client = FakeClient(empty_when_contains=["MEDELLIN", "Medellin", "medellin"])
    steps = build_predicate_waterfall(_ASCENSORES, client)

    assert killer_predicate(steps) == "scope"
    # valor nunca se evalúa porque scope ya vació el conjunto
    assert [s.predicate for s in steps] == ["tema", "scope"]


def test_no_killer_when_everything_exists():
    client = FakeClient(empty_when_contains=[])
    steps = build_predicate_waterfall(_ASCENSORES, client)

    assert killer_predicate(steps) is None
    assert all(s.exists for s in steps)
    assert len(steps) == 3


def test_probe_uses_limit_one_not_count():
    client = FakeClient()
    build_predicate_waterfall(_ASCENSORES, client)

    assert client.queries, "debe ejecutar queries de existencia"
    assert all("LIMIT 1" in q for q in client.queries)
    assert client.counts == []  # sin deep, NO ejecuta count


def test_deep_adds_count_only_when_requested():
    client = FakeClient(empty_when_contains=["100000000"])
    steps = build_predicate_waterfall(_ASCENSORES, client, deep=True)

    assert client.counts, "deep=True debe ejecutar count"
    assert any(s.count is not None for s in steps)


def test_waterfall_to_dict_is_serializable():
    client = FakeClient(empty_when_contains=["100000000"])
    steps = build_predicate_waterfall(_ASCENSORES, client)
    payload = waterfall_to_dict(steps)

    assert payload["killer_predicate"] == "valor"
    assert payload["predicate_waterfall"][-1]["killer_predicate"] is True
    assert payload["predicate_waterfall"][0]["predicate"] == "tema"


def test_empty_params_audits_nothing():
    client = FakeClient()
    steps = build_predicate_waterfall({"dataset": "procesos"}, client)
    assert steps == []
