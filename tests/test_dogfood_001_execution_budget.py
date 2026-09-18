"""DOGFOOD-001 — el carril PROFILE recibe el presupuesto que legacy ya tenía.

Incidente congelado (traza `d85de5f5773f`, 2026-09-17T20:28:44Z):

    consulta   «cuales han sido los contratistas con mas contratos
                con la alcaldia de soledad en 2026?»
    Alpha      MATCHED, ANALYZE/SUPPLIERS, 1.379 s
    plan       2 jobs (supplier_count, supplier_value), foco `suppliers`
    ejecución  entity_profile_total_latency_ms = 6 411 ms
    resultado  0 filas, accredited_metrics = 0

Seis segundos de socket. El MISMO turno por el carril legacy seis minutos antes
(traza `8eb7b3ba13fa`) midió 8 691 ms: legacy abre una ventana de presupuesto
nueva antes de ejecutar y el carril PROFILE no lo hacía, así que la latencia de
la fase lingüística salía del socket de SECOP.

Lo que la traza NO decía —y por eso el incidente no era legible— era el estado
de cada job, si se intentaron y cuántas filas volvieron.
"""
from datetime import date
from unittest.mock import Mock
import json
import sqlite3

import pytest

from app.config import Settings, get_settings
from app.core import deadline
from app.core.conversation_store import ConversationStore
from app.core.entity_profile.execution_trace import (
    job_receipts,
    profile_trace_metadata,
)
from app.core.entity_profile.failure_language import NOT_ZERO, total_failure_text
from app.core.entity_profile.query_runner import ENTITY_PROFILE_TIMEOUT_PER_CALL
from app.core.natural_alpha.budget import FULL_BUDGET_MS, legacy_budget, profile_budget
from app.core.natural_alpha.provider import ProviderResult
from app.core.orchestrator import SecopalWorkflow
from tests.test_natural_alpha_contract import IDENTITY, payload


# Lo que la fase lingüística consumió en el incidente, redondeado hacia arriba:
# Alpha 1,38 s más resolución de comprador, admisión y compilación.
LINGUISTIC_PHASE_S = 4.5
BUYER = "alcaldia de baranoa"
TODAY = date(2026, 9, 7)


def _fetch_budget():
    from app.core.entity_profile.frame_runner import _fetch_budget as fetch

    return fetch()


# ── A · paridad de presupuesto ────────────────────────────────────────────


@pytest.fixture
def frozen_clock(monkeypatch):
    clock = [100.0]
    monkeypatch.setitem(deadline._clock, "now", lambda: clock[0])
    return clock


def test_the_linguistic_phase_stops_charging_its_latency_to_the_socket(frozen_clock):
    """Mismo instante, mismos 15 s: los dos carriles dan lo mismo a la ejecución."""
    tokens = deadline.begin_deadline(frozen_clock[0])
    try:
        frozen_clock[0] += LINGUISTIC_PHASE_S
        without = _fetch_budget()
        with profile_budget(True):
            profile = _fetch_budget()
        with legacy_budget(True):
            legacy = _fetch_budget()
    finally:
        deadline.end_deadline(tokens)
    # El defecto: 6 s de socket donde la política dice 8. `max(1, int(budget))`
    # es lo que convirtió 6,5 en el timeout de 6 s que midió el incidente.
    assert without == pytest.approx(15.0 - LINGUISTIC_PHASE_S - 4.0)
    assert max(1, int(without)) == 6
    assert profile == legacy == float(ENTITY_PROFILE_TIMEOUT_PER_CALL)
    assert max(1, int(profile)) == 8


def test_without_an_observation_neither_lane_opens_a_new_window(frozen_clock):
    """La condición es la de legacy: sin fase lingüística no hay nada que devolver."""
    tokens = deadline.begin_deadline(frozen_clock[0])
    try:
        frozen_clock[0] += LINGUISTIC_PHASE_S
        with profile_budget(False):
            profile = _fetch_budget()
        with legacy_budget(False):
            legacy = _fetch_budget()
    finally:
        deadline.end_deadline(tokens)
    assert profile == legacy == pytest.approx(15.0 - LINGUISTIC_PHASE_S - 4.0)


def test_each_lane_restores_the_enclosing_turn_even_when_execution_raises(frozen_clock):
    tokens = deadline.begin_deadline(frozen_clock[0], budget_s=77)
    outer = deadline.current_deadline()
    try:
        for lane in (profile_budget, legacy_budget):
            with pytest.raises(RuntimeError, match="boom"), lane(True):
                raise RuntimeError("boom")
            assert deadline.current_deadline() is outer
            assert deadline.current_deadline().budget_s == 77
    finally:
        deadline.end_deadline(tokens)


def test_the_profile_lane_records_its_own_window_without_touching_legacy(frozen_clock):
    tokens = deadline.begin_deadline(frozen_clock[0])
    try:
        with profile_budget(True):
            frozen_clock[0] += 3
        telemetry = deadline.collected_deadline()
    finally:
        deadline.end_deadline(tokens)
    assert telemetry["natural_alpha_profile_budget_ms"] == FULL_BUDGET_MS == 15_000
    assert telemetry["natural_alpha_profile_elapsed_ms"] == 3_000
    assert not any(key.startswith("natural_alpha_legacy_") for key in telemetry)


# ── A · el mismo presupuesto, por la ruta real ────────────────────────────


class TimingOutSecop:
    """Un SECOP que nunca contesta y que ANOTA el timeout con el que se le llamó."""

    timeout = 15

    def __init__(self):
        self.derived_timeouts: list[int] = []
        self.calls: list[tuple[str, str, str]] = []

    def with_timeout(self, timeout):
        self.derived_timeouts.append(timeout)
        return self

    derived_for_timeout = with_timeout

    def _fail(self, dataset, soql):
        self.calls.append(("aggregate", dataset, soql))
        raise TimeoutError("timed out")

    aggregate = _fail
    query = _fail

    def count(self, dataset, soql):
        raise TimeoutError("timed out")


@pytest.fixture
def profile_flow(tmp_path, monkeypatch):
    """Frame ON, Alpha ON, disco temporal y CERO red. Un workflow por turno."""
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "true")
    get_settings.cache_clear()
    root = tmp_path / "profile"
    created = []

    def build(client, *, alpha_cost=0.0, clock=None):
        settings = Settings(
            _env_file=None, deepseek_api_key=None,
            secoppal_natural_alpha_mode="alpha",
            secoppal_natural_alpha_email="alpha@example.test",
            secop_feedback_path=root / "traces.jsonl",
            secop_feedback_db_path=root / "feedback.db",
        )
        flow = SecopalWorkflow(settings)
        flow.conv_store = ConversationStore(root / "conversations")
        flow.narrator.narrate_with_grounding = lambda **kwargs: None
        flow.secop_client = client

        def observe(*args):
            if clock is not None:
                clock[0] += alpha_cost
            return ProviderResult(payload(quote="contratos"), provider_calls=1)

        flow.natural_alpha_provider = Mock(observe=Mock(side_effect=observe))
        created.append(flow)
        return flow

    yield build, root
    get_settings.cache_clear()


def _say(flow, query, chat_id):
    return flow.run_query(query, channel="streamlit", chat_id=chat_id,
                          user_identity=IDENTITY, today=TODAY)


def test_soledad_shaped_turn_reaches_secop_with_the_full_per_call_budget(
    profile_flow, monkeypatch,
):
    """El gate del incidente: 8 s de socket, no 6, con Alpha ya cobrado."""
    from app.core import orchestrator

    build, _root = profile_flow
    clock = [100.0]
    monkeypatch.setattr(orchestrator.time, "perf_counter", lambda: clock[0])
    monkeypatch.setitem(deadline._clock, "now", lambda: clock[0])
    client = TimingOutSecop()
    flow = build(client, alpha_cost=LINGUISTIC_PHASE_S, clock=clock)
    result = _say(flow, f"analiza contratos de {BUYER} en agosto", "dogfood-001")

    assert flow.natural_alpha_provider.observe.called
    assert result["route_reason"].startswith("entity_profile_")
    assert client.derived_timeouts, "el plan no llegó a emitir ninguna consulta"
    assert set(client.derived_timeouts) == {ENTITY_PROFILE_TIMEOUT_PER_CALL}
    assert result["natural_alpha_profile_budget_ms"] == 15_000


# ── B · observabilidad ────────────────────────────────────────────────────


def test_a_two_of_two_timeout_leaves_a_receipt_per_job(profile_flow, monkeypatch):
    from app.core import orchestrator

    build, root = profile_flow
    clock = [100.0]
    monkeypatch.setattr(orchestrator.time, "perf_counter", lambda: clock[0])
    monkeypatch.setitem(deadline._clock, "now", lambda: clock[0])
    flow = build(TimingOutSecop(), alpha_cost=LINGUISTIC_PHASE_S, clock=clock)
    result = _say(flow, f"analiza contratos de {BUYER} en agosto", "dogfood-001-b")

    receipts = result["profile_job_receipts"]
    assert receipts, "una ejecución sin recibos vuelve a ser un incidente ilegible"
    for receipt in receipts:
        assert set(receipt) == {
            "job_id", "dataset_id", "attempted", "status",
            "rows_received", "latency_ms",
        }
        assert receipt["status"] == "TIMEOUT"
        assert receipt["attempted"] is True
        assert receipt["rows_received"] == 0
        assert "sql" not in receipt and "soql" not in receipt

    # Y lo mismo, ESCRITO: la traza es la única evidencia que sobrevive al turno.
    stored = _stored_trace(root / "feedback.db", result["trace_id"])
    assert stored["profile_job_receipts"] == receipts


def _stored_trace(db_path, trace_id) -> dict:
    connection = sqlite3.connect(db_path)
    try:
        row = connection.execute(
            "SELECT trace_json FROM traces WHERE trace_id = ?", (trace_id,),
        ).fetchone()
    finally:
        connection.close()
    assert row is not None, "el turno no dejó traza"
    return json.loads(row[0])


def test_a_job_that_never_left_is_not_a_job_that_timed_out():
    """`attempted=False` y `TIMEOUT` no pueden colapsar en la misma huella."""

    class Evidence:
        calls = ()
        rows: dict = {}
        latency_ms: dict = {}

        def rows_for(self, job_id):
            return ()

    exhausted = type("Call", (), {
        "job_id": "supplier_count", "dataset_id": "jbjy-vk9h",
        "status": "BUDGET_EXHAUSTED", "attempted": False,
    })()
    timed_out = type("Call", (), {
        "job_id": "supplier_value", "dataset_id": "jbjy-vk9h",
        "status": "TIMEOUT", "attempted": True,
    })()
    evidence = Evidence()
    evidence.calls = (exhausted, timed_out)
    evidence.latency_ms = {"supplier_value": 6411.4}
    receipts = job_receipts(evidence)
    assert [(r["status"], r["attempted"], r["latency_ms"]) for r in receipts] == [
        ("BUDGET_EXHAUSTED", False, 0.0), ("TIMEOUT", True, 6411.4),
    ]


@pytest.mark.parametrize("broken", [
    {"profile_job_receipts": [{"job_id": "x", "status": "INVENTADO",
                               "attempted": True, "rows_received": 0,
                               "latency_ms": 1.0}]},
    {"profile_job_receipts": [{"job_id": "x", "status": "TIMEOUT",
                               "attempted": "si", "rows_received": 0,
                               "latency_ms": 1.0}]},
    {"profile_job_receipts": [{"job_id": "x", "status": "TIMEOUT",
                               "attempted": True, "rows_received": -1,
                               "latency_ms": 1.0}]},
    {"query_router_calls": "cero"},
    {"legacy_bypassed": "true"},
    {"authority_status": 7},
])
def test_an_unrecognized_receipt_is_dropped_never_coerced(broken):
    assert profile_trace_metadata(broken) == {}


def test_a_credentialed_turn_persists_its_authorities_and_its_verifier():
    result = {
        "credential_capability": "entity_profile.suppliers",
        "admission_reason": "accredited",
        "legacy_bypassed": True,
        "query_router_calls": 0,
        "authority_status": "READY",
        "authority_reason": "",
        "residual_verifier_called": True,
        "residual_verifier_status": "CONFIRMED",
        "residual_units": [{"id": "u1", "text": "con mas contratos"}],
        "residual_labels": {"u1": "JOB_LANGUAGE"},
        "weak_actor_guard_called": True,
    }
    persisted = profile_trace_metadata(result)
    for field in (
        "credential_capability", "admission_reason", "legacy_bypassed",
        "query_router_calls", "authority_status", "residual_verifier_called",
        "residual_verifier_status", "residual_units", "residual_labels",
        "weak_actor_guard_called",
    ):
        assert field in persisted, field
    assert persisted["query_router_calls"] == 0
    assert persisted["legacy_bypassed"] is True
    # `authority_reason` vacío no se inventa como presente.
    assert "authority_reason" not in persisted


# ── C · UX de fallo ───────────────────────────────────────────────────────


class _Plan:
    def __init__(self, focus):
        self.focus = focus


class _Evidence:
    def __init__(self, statuses):
        self.calls = tuple(
            type("Call", (), {"status": status})() for status in statuses
        )


@pytest.mark.parametrize("statuses,expected", [
    (("TIMEOUT", "TIMEOUT"),
     "SECOP no respondió a tiempo en las dos consultas sobre los proveedores."),
    (("ERROR", "ERROR"),
     "SECOP falló al responder las dos consultas sobre los proveedores."),
    (("BUDGET_EXHAUSTED", "BUDGET_EXHAUSTED"),
     "No alcancé a emitir las dos consultas sobre los proveedores: el turno se "
     "quedó sin presupuesto antes de consultar a SECOP."),
])
def test_the_three_causes_do_not_share_one_sentence(statuses, expected):
    text = total_failure_text(_Plan("suppliers"), _Evidence(statuses))
    assert text.startswith(expected)
    assert NOT_ZERO in text, "un fallo total no puede leerse como cero actividad"


def test_a_mixed_failure_enumerates_what_happened():
    text = total_failure_text(_Plan("suppliers"), _Evidence(("TIMEOUT", "ERROR")))
    assert "una expiró" in text and "una falló" in text
    assert NOT_ZERO in text


def test_a_section_never_claims_the_source_answered_when_nothing_was_sent():
    from app.core.entity_profile.frame_sections import _warnings
    from app.core.entity_profile.models import SourceCall

    call = SourceCall(
        call_id="supplier_count", aspect="suppliers", dataset_id="jbjy-vk9h",
        operation="aggregate", status="BUDGET_EXHAUSTED", rows_received=0,
        latency_ms=0.0, soql="", attempted=True,
    )
    (warning,) = _warnings("los proveedores", ("supplier_count",),
                           {"supplier_count": call})
    assert "no se emitió por falta de presupuesto" in warning
    assert "la fuente no respondió" not in warning


def test_the_rendered_profile_names_the_cause_of_a_total_failure(profile_flow, monkeypatch):
    from app.core import orchestrator

    build, _root = profile_flow
    clock = [100.0]
    monkeypatch.setattr(orchestrator.time, "perf_counter", lambda: clock[0])
    monkeypatch.setitem(deadline._clock, "now", lambda: clock[0])
    flow = build(TimingOutSecop(), alpha_cost=LINGUISTIC_PHASE_S, clock=clock)
    result = _say(flow, f"analiza contratos de {BUYER} en agosto", "dogfood-001-c")
    response = result["response"]
    assert "no respondió a tiempo" in response
    assert NOT_ZERO in response


def test_a_profile_turn_without_alpha_keeps_the_original_window(
    profile_flow, monkeypatch,
):
    """La condición del wrapper es la de legacy, no `True`.

    Sin observación no se consumió latencia lingüística: abrir una ventana nueva
    ahí no sería paridad, sería un deadline de 15 s más largo por la puerta de
    atrás.
    """
    from app.core import orchestrator

    build, _root = profile_flow
    clock = [100.0]
    monkeypatch.setattr(orchestrator.time, "perf_counter", lambda: clock[0])
    monkeypatch.setitem(deadline._clock, "now", lambda: clock[0])
    client = TimingOutSecop()
    flow = build(client)
    flow.natural_alpha_provider = None
    flow.settings.secoppal_natural_alpha_mode = "off"
    result = _say(flow, f"analiza contratos de {BUYER} en agosto", "dogfood-001-noalpha")

    assert not any(key.startswith("natural_alpha_") for key in result)
    assert "natural_alpha_profile_budget_ms" not in result
    assert client.derived_timeouts, "el plan no llegó a emitir ninguna consulta"


# ── C · la traza de la UI no esconde a quien decidió ──────────────────────


def _streamlit_functions(names, namespace):
    import ast
    from pathlib import Path

    tree = ast.parse(Path("streamlit_app.py").read_text())
    functions = [node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
    assert len(functions) == len(names)
    exec(compile(ast.Module(body=functions, type_ignores=[]), "streamlit_app.py",
                 "exec"), namespace)
    return namespace


class _FakeStreamlit:
    """Lo mínimo que `_render_trace` toca, y un registro de lo que escribió."""

    def __init__(self):
        self.written: list[str] = []
        self.tables: list = []

    def expander(self, *args, **kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def columns(self, count):
        return [self] * count

    def markdown(self, text):
        self.written.append(str(text))

    def json(self, *args, **kwargs):
        pass

    def code(self, *args, **kwargs):
        pass

    def dataframe(self, rows, **kwargs):
        self.tables.append(rows)


def test_the_trace_header_says_who_decided_and_shows_each_job():
    fake = _FakeStreamlit()
    namespace = _streamlit_functions({"_render_trace"}, {"st": fake})
    receipts = [{"job_id": "supplier_count", "dataset_id": "jbjy-vk9h",
                 "attempted": True, "status": "TIMEOUT", "rows_received": 0,
                 "latency_ms": 6411.4}]
    namespace["_render_trace"]({
        "route_reason": "entity_profile_suppliers",
        "needs_llm": False,
        "natural_alpha_status": "MATCHED",
        "profile_job_receipts": receipts,
        "results_count": 0,
    })
    written = "\n".join(fake.written)
    # El indicador legacy sigue diciendo la verdad sobre SÍ MISMO...
    assert "**LLM:** No" in written
    # ...y ya no puede leerse como «aquí no intervino ningún modelo».
    assert "**Natural Alpha:** Si" in written and "MATCHED" in written
    assert fake.tables == [receipts]


def test_the_trace_header_does_not_invent_an_alpha_that_never_ran():
    fake = _FakeStreamlit()
    namespace = _streamlit_functions({"_render_trace"}, {"st": fake})
    namespace["_render_trace"]({"route_reason": "heuristic_only", "needs_llm": False})
    written = "\n".join(fake.written)
    assert "**Natural Alpha:** No" in written
    assert fake.tables == []
