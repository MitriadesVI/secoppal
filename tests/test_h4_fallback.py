"""H4 — fallback degradado sin ORDER BY (gate semántico + presupuesto 8/3/4).

Cierra los puntos del asesor:
1. fallback_attempted ≠ fallback_used (intentado vs exitoso)
2. la RESPUESTA visible no afirma orden sobre una muestra (E2E por el formatter)
3. deadline agotado: ni la principal corre, sin falso EMPTY
4. el reparto 8/3/4 es real (math del Deadline con caps)
5. el fallback conserva el WHERE byte-a-byte (solo cambia ORDER BY + LIMIT)
6. orden y conteo son EJES INDEPENDIENTES (muestra sin orden + conteo exacto)
7. el fallback SOLO se dispara ante errores recuperables (timeout), no SoQL inválido
8. el gate es por-turno: lee ordering_signal actual (semántico bloquea)
"""
from __future__ import annotations

import re

from burr.core import State

from app.core import deadline as dl_mod
from app.core.deadline import (
    CLOSING_RESERVE_SECONDS,
    FALLBACK_FETCH_SECONDS,
    PRIMARY_FETCH_SECONDS,
    Deadline,
)
from app.core.formatter import Formatter
from app.core.order_semantics import ORDER_SAMPLE, ORDER_UNAVAILABLE
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions.execution import execute_query

DS = "p6dx-8zbt"


class OrderAwareClient:
    """Query con ORDER BY → timeout; sin ORDER BY → devuelve filas. Cuenta llamadas."""

    timeout = 15

    def __init__(self, rows, fail_unordered=False, count_value=4321):
        self.rows = rows
        self.fail_unordered = fail_unordered
        self.count_value = count_value
        self.query_soqls: list[str] = []

    def query(self, dataset_id, soql):
        self.query_soqls.append(soql)
        if "ORDER BY" in soql:
            raise TimeoutError("read timed out")
        if self.fail_unordered:
            raise TimeoutError("read timed out")
        return list(self.rows)

    def count(self, dataset_id, soql):
        return self.count_value


def _state(params):
    builder = SoQLBuilder()
    return State({
        "resolved_params": params,
        "dataset_id": DS,
        "soql_query": builder.build(DS, params),
        "user_query": "x",
        "timings_ms": {},
    }), builder


def _where_of(soql: str) -> str:
    """Extrae el WHERE (entre 'WHERE ' y ' ORDER BY' / ' LIMIT')."""
    m = re.search(r" WHERE (.+?)(?: ORDER BY | LIMIT )", soql)
    return m.group(1) if m else ""


# ── 1. presentacional → fallback usado; attempted ≠ used ─────────────────────

def test_presentational_order_allows_fallback():
    state, builder = _state({"objeto_phrases": ["aire"], "departamento_resolved": "Santander"})
    client = OrderAwareClient(rows=[{"precio_base": "10"}] * 50)

    out = execute_query(state, client, builder)

    assert out["fallback_attempted"] is True
    assert out["fallback_used"] is True
    assert out["result_order_guarantee"] == ORDER_SAMPLE
    assert out["fallback_strategy"] == "removed_order_by"
    assert out["fallback_query_status"] == "completed"
    assert out["primary_query_status"] == "timed_out"
    assert len(out["results"]) == 50
    assert out["degraded"] is True


def test_fallback_attempted_but_failed_is_not_used():
    # El fallback se INTENTÓ pero también falló → attempted=True, used=False.
    state, builder = _state({"objeto_phrases": ["aire"], "departamento_resolved": "Santander"})
    client = OrderAwareClient(rows=[], fail_unordered=True)

    out = execute_query(state, client, builder)

    assert out["fallback_attempted"] is True
    assert out["fallback_used"] is False
    assert out["fallback_query_status"] == "timed_out"
    assert out["result_order_guarantee"] == ORDER_UNAVAILABLE


# ── 2. (E2E) la respuesta NO afirma orden sobre una muestra ──────────────────

def test_response_does_not_claim_order_on_sample():
    fmt = Formatter()
    rows = [{"precio_base": "9", "nombre_del_procedimiento": f"P{i}",
             "entidad": "E", "fecha_de_publicacion_del": "2026-01-01"} for i in range(50)]
    # con orden garantizado SÍ dice "más recientes"; con muestra NO debe decirlo.
    exact, _ = fmt.format_for_channel(rows, DS, "whatsapp", params={},
                                      total_count=4321, order_guarantee="exact_order")
    sample, _ = fmt.format_for_channel(rows, DS, "whatsapp", params={},
                                       total_count=4321, order_guarantee="unordered_sample")

    assert "más recientes" in exact            # baseline: el claim existe normalmente
    assert "más recientes" not in sample
    assert "de mayor valor" not in sample
    assert "muestra sin orden garantizado" in sample
    # el conteo (eje independiente) SÍ puede seguir presente:
    assert "4,321" in sample or "4321" in sample


# ── 3. semántico bloquea el fallback (valor) ─────────────────────────────────

def test_semantic_value_order_blocks_fallback():
    state, builder = _state({"objeto_phrases": ["salud"], "ordering_signal": "valor_desc"})
    client = OrderAwareClient(rows=[{"precio_base": "10"}] * 50)

    out = execute_query(state, client, builder)

    assert out["fallback_attempted"] is False
    assert out["fallback_used"] is False
    assert out["result_order_guarantee"] == ORDER_UNAVAILABLE
    assert out["results"] == []
    assert out["risk_flag_timeout"] is True
    assert all("ORDER BY" in s for s in client.query_soqls)  # nunca una 2ª query sin orden


# ── 3b. semántico bloquea el fallback (recencia) ─────────────────────────────

def test_semantic_recency_order_blocks_fallback():
    state, builder = _state({"objeto_phrases": ["obras"], "ordering_signal": "fecha_desc"})
    client = OrderAwareClient(rows=[{"precio_base": "10"}] * 50)

    out = execute_query(state, client, builder)

    assert out["fallback_attempted"] is False
    assert out["result_order_guarantee"] == ORDER_UNAVAILABLE
    assert out["results"] == []


# ── 4. el reparto 8/3/4 es real (math del Deadline con caps) ─────────────────

def test_budget_split_8_3_4():
    assert PRIMARY_FETCH_SECONDS == 8.0
    assert FALLBACK_FETCH_SECONDS == 3.0
    assert CLOSING_RESERVE_SECONDS == 4.0
    assert PRIMARY_FETCH_SECONDS + FALLBACK_FETCH_SECONDS + CLOSING_RESERVE_SECONDS == 15.0

    dl_mod.set_clock(lambda: 0.0)
    try:
        dl = Deadline(started_at=0.0)  # budget 15, reserve 4
        # principal: min(fuente 60, restante−reserva=11, cap 8) = 8
        assert dl.fetch_budget_s(60, cap_s=PRIMARY_FETCH_SECONDS) == 8.0
        # fallback: min(fuente 60, restante−reserva=11, cap 3) = 3
        assert dl.fetch_budget_s(60, cap_s=FALLBACK_FETCH_SECONDS) == 3.0
        # sin cap (compat): min(60, 11) = 11
        assert dl.fetch_budget_s(60) == 11.0
    finally:
        dl_mod.reset_clock()


# ── 5. el fallback conserva el WHERE byte-a-byte (solo cambia ORDER BY+LIMIT) ─

def test_fallback_preserves_where_exactly():
    params = {
        "objeto_phrases": ["adulto mayor"],
        "departamento_resolved": "Bolivar",
        "fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31",
        "estado": "Abierto", "valor_min": 1_000_000,
    }
    state, builder = _state(params)
    primary_soql = state["soql_query"]
    client = OrderAwareClient(rows=[{"precio_base": "1"}] * 50)

    out = execute_query(state, client, builder)

    assert out["substantive_filters_preserved"] is True
    fb_soql = client.query_soqls[-1]
    assert "ORDER BY" in primary_soql and "ORDER BY" not in fb_soql
    # El WHERE es IDÉNTICO: el fallback no tocó ningún filtro sustantivo.
    assert _where_of(primary_soql) == _where_of(fb_soql)
    assert _where_of(fb_soql) != ""


# ── 5b. WIDE_LIMIT: el fallback recorta LIMIT 200 → 50 ───────────────────────

def test_fallback_reduces_wide_limit():
    state, builder = _state({"objeto_phrases": ["aire"], "departamento_resolved": "Santander"})
    assert "LIMIT 200" in state["soql_query"]
    client = OrderAwareClient(rows=[{"precio_base": "1"}] * 50)

    out = execute_query(state, client, builder)

    assert set(out["transformations_applied"]) == {"removed_order_by", "reduced_limit"}
    assert "LIMIT 50" in client.query_soqls[-1]


# ── 6. orden y conteo son EJES INDEPENDIENTES ────────────────────────────────

def test_order_and_count_are_independent_axes():
    # Muestra sin orden garantizado PERO con conteo exacto (el count terminó).
    state, builder = _state({"objeto_phrases": ["aire"], "departamento_resolved": "Santander"})
    client = OrderAwareClient(rows=[{"precio_base": "9"}] * 50, count_value=4321)

    out = execute_query(state, client, builder)

    assert out["result_order_guarantee"] == ORDER_SAMPLE     # sin garantía de orden
    assert out["total_count"] == 4321                        # conteo EXACTO igual válido
    assert out["total_count_approximate"] is False           # no se fuerza aproximado


# ── 7. solo errores recuperables: SoQL inválido NO dispara el fallback ───────

def test_non_timeout_error_does_not_trigger_fallback():
    state, builder = _state({"objeto_phrases": ["aire"], "departamento_resolved": "Santander"})

    class BadSoQLClient:
        timeout = 15

        def __init__(self):
            self.query_soqls = []

        def query(self, dataset_id, soql):
            self.query_soqls.append(soql)
            raise ValueError("400 Bad Request: malformed SoQL")

        def count(self, dataset_id, soql):
            return 0

    client = BadSoQLClient()
    out = execute_query(state, client, builder)

    assert out["fallback_attempted"] is False     # NO se ocultó el defecto sintáctico
    assert out["fallback_used"] is False
    assert len(client.query_soqls) == 1           # una sola query, sin reintento sin orden
    assert out["risk_flag_timeout"] is False      # no es timeout


# ── 8. el gate es por-turno: nuevo search sin ordering_signal permite fallback ─

def test_gate_reads_current_turn_only():
    # Aunque un turno anterior hubiera pedido "más caros", este turno no trae
    # ordering_signal → presentacional → fallback permitido (lee SOLO params actuales).
    state, builder = _state({"objeto_phrases": ["alimentacion"]})  # sin ordering_signal
    client = OrderAwareClient(rows=[{"precio_base": "1"}] * 50)

    out = execute_query(state, client, builder)

    assert out["fallback_used"] is True
    assert out["result_order_guarantee"] == ORDER_SAMPLE


# ── deadline agotado: ni la principal corre; sin falso EMPTY ─────────────────

def test_deadline_exhausted_skips_request_no_false_empty():
    state, builder = _state({"objeto_phrases": ["aire"], "departamento_resolved": "Santander"})
    client = OrderAwareClient(rows=[{"precio_base": "1"}] * 50)

    fake = {"t": 0.0}
    dl_mod.set_clock(lambda: fake["t"])
    try:
        tokens = dl_mod.begin_deadline(0.0)   # budget 15, reserve 4
        fake["t"] = 12.5                       # restante 2.5 < reserva 4 → sin presupuesto
        out = execute_query(state, client, builder)
        tel = dl_mod.collected_deadline()
    finally:
        dl_mod.end_deadline(tokens)
        dl_mod.reset_clock()

    assert client.query_soqls == []                          # la principal NO corrió
    assert tel.get("request_skipped_due_to_budget") is True
    assert tel.get("deadline_exhausted") is True
    assert out["results"] == []                              # vacío honesto…
    assert out["query_error"] == ""                          # …NO un error/timeout falso
    assert out["fallback_attempted"] is False                # sin excepción → sin fallback
