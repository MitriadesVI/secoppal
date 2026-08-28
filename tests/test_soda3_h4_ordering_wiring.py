"""T1B — la política de ordenamiento viaja desde la ruta, nunca desde el SoQL.

Gates cubiertos: H4_EXPLICIT_NO_PRESERVE, NO_SOQL_ORDER_HEURISTIC,
NORMAL_ORDERING_POLICY.

El discovery adjudicó ``no_preserve`` **solo** para el fallback H4, la ruta
donde ``execution.py`` retira el ``ORDER BY`` a propósito. La adjudicación
prohíbe explícitamente inferirlo del texto:

    if "ORDER BY" not in soql: no_preserve      # PROHIBIDO

porque 42 de las 83 operaciones del corpus congelado no llevan ``ORDER BY`` y
la mayoría son ``count``, ``aggregate`` y direct lookup legítimamente no
ordenados. Estos tests fijan esa frontera por control flow.
"""
from __future__ import annotations

from burr.core import State

from app.core.order_semantics import ORDER_SAMPLE
from app.core.secop_client import SecopClient
from app.core.socrata_transport import Soda3OrderingSpecifier
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions.execution import execute_query
from app.core.workflow.helpers import _query_first_then_count

DS = "p6dx-8zbt"
TOTAL = Soda3OrderingSpecifier.TOTAL
NO_PRESERVE = Soda3OrderingSpecifier.NO_PRESERVE


class RecordingTransport:
    """Transporte que registra (operación, ordering, soql) y simula H4.

    Una query CON ``ORDER BY`` agota el presupuesto —la condición que abre el
    ``except`` de ``execute_query``—; sin ``ORDER BY`` devuelve filas.
    """

    name = "recording"

    def __init__(self, rows=None, count_value=4321):
        self.rows = rows if rows is not None else [{"precio_base": "10"}] * 50
        self.count_value = count_value
        self.calls: list[tuple[str, Soda3OrderingSpecifier, str]] = []

    def _record(self, operation, soql, ordering_specifier):
        self.calls.append((operation, ordering_specifier, soql))

    def query(self, dataset_id, soql, *, timeout_seconds, ordering_specifier):
        self._record("query", soql, ordering_specifier)
        if "ORDER BY" in soql:
            raise TimeoutError("read timed out")
        return list(self.rows)

    def count(self, dataset_id, soql, *, timeout_seconds, ordering_specifier):
        self._record("count", soql, ordering_specifier)
        return [{"count": str(self.count_value)}]

    def aggregate(self, dataset_id, soql, *, timeout_seconds, ordering_specifier):
        self._record("aggregate", soql, ordering_specifier)
        return list(self.rows)

    # Vistas cómodas sobre el ledger
    def orderings_for(self, operation):
        return [ordering for op, ordering, _ in self.calls if op == operation]

    def ordered_soqls(self, operation):
        return [soql for op, _, soql in self.calls if op == operation]


def _client(transport):
    return SecopClient("www.datos.gov.co", None, timeout=15, transport=transport)


def _state(params):
    builder = SoQLBuilder()
    return State({
        "resolved_params": params,
        "dataset_id": DS,
        "soql_query": builder.build(DS, params),
        "user_query": "x",
        "timings_ms": {},
    }), builder


PRESENTATIONAL = {"objeto_phrases": ["aire"], "departamento_resolved": "Santander"}


# ── H4: la única ruta que declara no_preserve ────────────────────────────

def test_h4_fallback_query_declares_no_preserve():
    transport = RecordingTransport()
    state, builder = _state(PRESENTATIONAL)

    out = execute_query(state, _client(transport), builder)

    # El fallback ocurrió de verdad (no estamos midiendo un camino muerto).
    assert out["fallback_used"] is True
    assert out["result_order_guarantee"] == ORDER_SAMPLE

    queries = transport.calls and [c for c in transport.calls if c[0] == "query"]
    primary, fallback = queries[0], queries[1]
    assert "ORDER BY" in primary[2] and primary[1] is TOTAL
    assert "ORDER BY" not in fallback[2] and fallback[1] is NO_PRESERVE


def test_h4_count_does_not_inherit_no_preserve():
    """El conteo es un eje independiente del orden: conserva su política."""
    transport = RecordingTransport()
    state, builder = _state(PRESENTATIONAL)

    execute_query(state, _client(transport), builder)

    counts = transport.orderings_for("count")
    assert counts, "el fallback debe haber consultado el count"
    assert all(ordering is TOTAL for ordering in counts)
    # Y ese count tampoco lleva ORDER BY — la ausencia no lo convirtió en H4.
    assert all("ORDER BY" not in soql for soql in transport.ordered_soqls("count"))


# ── Todo lo demás es total, incluso sin ORDER BY ─────────────────────────

def test_primary_query_without_fallback_declares_total():
    transport = RecordingTransport()
    # Sin ordering presentacional que falle: la principal responde.
    transport.query = _always_ok_query(transport)
    state, builder = _state(PRESENTATIONAL)

    execute_query(state, _client(transport), builder)

    assert transport.orderings_for("query") == [TOTAL]


def _always_ok_query(transport):
    def query(dataset_id, soql, *, timeout_seconds, ordering_specifier):
        transport._record("query", soql, ordering_specifier)
        return list(transport.rows)
    return query


def test_count_without_order_by_is_not_h4():
    transport = RecordingTransport()
    _client(transport).count(DS, "SELECT count(*) WHERE x = '1'")
    assert transport.orderings_for("count") == [TOTAL]


def test_aggregate_without_order_by_is_not_h4():
    transport = RecordingTransport()
    _client(transport).aggregate(DS, "SELECT entidad, count(*) AS cnt GROUP BY entidad")
    assert transport.orderings_for("aggregate") == [TOTAL]


def test_plain_query_without_order_by_is_not_h4():
    """Direct lookup: `LIMIT 1` sin ORDER BY, y sigue siendo total."""
    transport = RecordingTransport()
    _client(transport).query(DS, "SELECT * WHERE id_del_proceso = 'CO1.NTC.7' LIMIT 1")
    assert transport.orderings_for("query") == [TOTAL]


def test_direct_lookup_handler_does_not_declare_no_preserve():
    """El handler real de lookup por id, no una imitación."""
    from app.core.workflow.handlers import direct_lookup as module
    import inspect

    source = inspect.getsource(module)
    assert "ordering_specifier" not in source, (
        "direct_lookup no debe declarar una política de ordering: no es H4"
    )


# ── El helper no inventa la señal ────────────────────────────────────────

def test_query_first_then_count_defaults_to_no_declaration():
    transport = RecordingTransport()
    _query_first_then_count(
        _client(transport), DS, "SELECT count(*)", "SELECT * LIMIT 50", limit=50,
    )
    assert transport.orderings_for("query") == [TOTAL]
    assert transport.orderings_for("count") == [TOTAL]


def test_query_first_then_count_forwards_an_explicit_specifier():
    transport = RecordingTransport()
    _query_first_then_count(
        _client(transport), DS, "SELECT count(*)", "SELECT * LIMIT 50", limit=50,
        query_ordering_specifier=NO_PRESERVE,
    )
    assert transport.orderings_for("query") == [NO_PRESERVE]
    assert transport.orderings_for("count") == [TOTAL]


def test_fake_clients_without_the_kwarg_still_work():
    """Los fakes de la suite no tienen transporte: el eje no existe para ellos."""

    class FakeClient:
        def query(self, dataset_id, soql):
            return [{"id": 1}]

        def count(self, dataset_id, soql):
            return 7

    total, results, _t, approx = _query_first_then_count(
        FakeClient(), DS, "cnt", "soql", limit=50,
        query_ordering_specifier=NO_PRESERVE,
    )
    assert results == [{"id": 1}] and total == 1 and approx is False
