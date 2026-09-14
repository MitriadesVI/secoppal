"""Full legacy execution against a SQL-evaluated source with decoy rows."""
import json
import re
import socket
import sqlite3
import threading
import unicodedata
from datetime import date, datetime, timezone
from unittest.mock import Mock

import pytest

from app.config import Settings, get_settings
from app.core.conversation_store import ConversationStore
from app.core.natural_alpha.provider import ProviderResult
from app.core.orchestrator import SecopalWorkflow
from app.core.soql_builder import SoQLBuilder
from tests.test_natural_alpha_contract import IDENTITY, payload

RAW = "busca oportunidades de mantenimiento"
MULTI_RAW = "busca contratos de parques y dime cuánto suman"
MULTI = json.dumps({"jobs": [
    {"action": "SEARCH", "aspect": "CONTRACTS", "evidence_quote": "busca contratos", "occurrence": 1},
    {"action": "ANALYZE", "aspect": "TOTAL_VALUE", "evidence_quote": "dime cuánto suman", "occurrence": 1},
]})
DUPLICATE = json.dumps({"jobs": json.loads(payload())["jobs"] * 2})


class SqlSource:
    timeout = 15

    def __init__(self):
        self.calls, self.errors = [], []
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.create_function("unaccent", 1, lambda text: "".join(
            c for c in unicodedata.normalize("NFD", str(text)) if not unicodedata.combining(c)))
        from tests.contract_universe_sql_dates import register_dates
        register_dates(self.conn)
        columns = SoQLBuilder.SPECS["p6dx-8zbt"].select_fields
        self.conn.execute("CREATE TABLE processes (" + ",".join(
            column + (" INTEGER" if column == "precio_base" else " TEXT") for column in columns) + ")")
        contract_columns = SoQLBuilder.SPECS["jbjy-vk9h"].select_fields
        self.conn.execute("CREATE TABLE contracts (" + ",".join(column + " TEXT" for column in contract_columns) + ")")
        good = dict.fromkeys(columns, "")
        good.update(id_del_proceso="CO1.BDOS.123456", referencia_del_proceso="POSITIVE",
                    entidad="ALCALDIA DE GALAPA", departamento_entidad="Atlántico",
                    ciudad_entidad="Galapa", nombre_del_procedimiento="MANTENIMIENTO DE PARQUES",
                    descripci_n_del_procedimiento="MANTENIMIENTO DE PARQUES",
                    precio_base=500_000_000, estado_de_apertura_del_proceso="Abierto",
                    estado_del_procedimiento="Publicado", fase="Presentación de ofertas",
                    fecha_de_publicacion_del="2026-09-01T00:00:00.000",
                    fecha_de_recepcion_de="2026-10-01T00:00:00.000",
                    urlproceso="https://example.invalid/positive")
        rows = [good, dict(good, id_del_proceso="DECOY-TOPIC", referencia_del_proceso="DECOY-TOPIC",
                           nombre_del_procedimiento="SUMINISTRO DE COMPUTADORES",
                           descripci_n_del_procedimiento="SUMINISTRO DE COMPUTADORES"),
                dict(good, id_del_proceso="DECOY-CLOSED", referencia_del_proceso="DECOY-CLOSED",
                     estado_de_apertura_del_proceso="Cerrado", estado_del_procedimiento="Adjudicado")]
        self.conn.executemany("INSERT INTO processes VALUES (" + ",".join("?" for _ in columns) + ")",
                              [list(row.values()) for row in rows])

    def with_timeout(self, timeout):
        return self

    derived_for_timeout = with_timeout

    def query(self, dataset, soql):
        with self.lock:
            self.calls.append((dataset, soql))
            try:
                table = {"p6dx-8zbt": "processes", "jbjy-vk9h": "contracts"}[dataset]
                sql = re.sub(r"\bWHERE\b", f"FROM {table} WHERE", soql, count=1, flags=re.I)
                # SoQL's four-argument case is SQL CASE, not a SQLite function.
                sql = re.sub(r"\bcase\(([^(),]+), ([^(),]+), true, ([^(),]+)\)",
                             r"(CASE WHEN \1 THEN \2 ELSE \3 END)", sql, flags=re.I)
                return [dict(row) for row in self.conn.execute(sql)]
            except Exception as exc:
                self.errors.append(repr(exc))
                raise

    aggregate = query

    def count(self, dataset, soql):
        return int(next(iter(self.query(dataset, soql)[0].values())))


@pytest.fixture
def make_flow(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "false")
    get_settings.cache_clear()
    import burr.telemetry
    monkeypatch.setattr(burr.telemetry, "g_telemetry_enabled", False)
    from app.core.workflow.actions import planning

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 13, 12, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(planning, "datetime", FrozenDateTime)
    attempts = []

    def forbidden(*args, **kwargs):
        attempts.append(True)
        raise AssertionError("Synthetic Alpha runtime must not open a network socket")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    sources = []

    def build(mode="alpha", response=None):
        root = tmp_path / str(len(sources))
        settings = Settings(_env_file=None, deepseek_api_key=None,
                            secoppal_natural_alpha_mode=mode,
                            secoppal_natural_alpha_email="alpha@example.test",
                            secop_feedback_path=root / "traces.jsonl",
                            secop_feedback_db_path=root / "feedback.db")
        flow = SecopalWorkflow(settings)
        flow.conv_store = ConversationStore(root / "conversations")
        flow.narrator.narrate_with_grounding = lambda **kwargs: None
        flow.secop_client = SqlSource()
        sources.append(flow.secop_client)
        flow.natural_alpha_provider = Mock(observe=Mock(
            return_value=response or ProviderResult(payload(), provider_calls=1)))
        return flow

    yield build
    for source in sources:
        source.conn.close()
        assert not source.errors, source.errors
    assert not attempts
    get_settings.cache_clear()


def run(flow, raw=RAW, **kwargs):
    return flow.run_query(raw, channel="streamlit", user_identity=IDENTITY,
                          today=date(2026, 9, 13), **kwargs)


@pytest.mark.parametrize("mode,response", [
    ("off", ProviderResult(payload(), provider_calls=1)),
    ("shadow", ProviderResult(MULTI, provider_calls=1)),
    ("alpha", ProviderResult(payload(), provider_calls=1)),
    ("alpha", ProviderResult(error="PROVIDER_TIMEOUT", provider_calls=1)),
    ("alpha", ProviderResult("not JSON", provider_calls=1)),
    ("alpha", ProviderResult(payload("NOT IN RAW"), provider_calls=1)),
    ("alpha", ProviderResult(payload(action="OTHER", aspect="OTHER"), provider_calls=1)),
    ("alpha", ProviderResult('{"jobs": []}', provider_calls=1)),
    ("alpha", ProviderResult(DUPLICATE, provider_calls=1)),
])
def test_legacy_material_parity_and_single_trace(make_flow, mode, response):
    baseline, candidate = make_flow("off"), make_flow(mode, response)
    # The shadow composite quotes are both literal even in a single search;
    # it observes multi-job but still delegates the exact raw query.
    if mode == "shadow":
        jobs = json.loads(MULTI)["jobs"]
        for job in jobs:
            job["evidence_quote"] = "busca"
        candidate.natural_alpha_provider.observe.return_value = ProviderResult(json.dumps({"jobs": jobs}), provider_calls=1)
    parser = candidate.query_router.parse
    received = []

    def recording_parser(query, **kwargs):
        received.append(query)
        return parser(query, **kwargs)

    candidate.query_router.parse = recording_parser
    before = run(baseline, chat_id="legacy")
    after = run(candidate, chat_id="candidate")
    assert RAW in received
    for field in ("parsed_params", "resolved_params", "query_plan", "soql_query", "dataset_id",
                  "rows", "results", "total_count", "needs_clarification", "route_reason", "response"):
        assert after[field] == before[field], field
    assert {row["referencia_del_proceso"] for row in after["results"]} == {"POSITIVE"}
    assert before["total_count"] == after["total_count"] == 1
    assert candidate.secop_client.calls == baseline.secop_client.calls
    traces = candidate.feedback.get_recent()
    assert len(traces) == 1
    assert traces[0]["trace_id"] == after["trace_id"]
    if mode == "off":
        candidate.natural_alpha_provider.observe.assert_not_called()
        assert not any(key.startswith("natural_alpha_") for key in after)
    else:
        candidate.natural_alpha_provider.observe.assert_called_once()
        assert traces[0]["natural_alpha_status"] == after["natural_alpha_status"]
        assert traces[0]["natural_alpha_legacy_route_reason"] == after["route_reason"]
        if response.payload == DUPLICATE:
            assert after["natural_alpha_status"] == "INVALID"
            assert after["natural_alpha_jobs"] == []


@pytest.mark.parametrize("frame", [False, True])
def test_multi_terminal_zero_sql_one_trace_no_pending(make_flow, monkeypatch, frame):
    flow = make_flow(response=ProviderResult(MULTI, provider_calls=1))
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", str(frame).lower())
    get_settings.cache_clear()
    result = run(flow, MULTI_RAW, chat_id="multi")
    assert result["needs_clarification"] is True
    assert result["route_reason"] == "natural_alpha_multi_job_stepwise"
    assert result["_continuation_owner"] == "NONE"
    assert not flow.secop_client.calls
    assert result["soql_query"] == ""
    assert not result.get("query_plan")
    assert not result["resolved_params"]
    assert len(flow.feedback.get_recent()) == 1
    assert flow.conv_store.get_history("multi") == []
    assert not any("pending" in key for key in result)
    flow.natural_alpha_provider.observe.assert_called_once()
    assert "1. Buscar contratos." in result["response"]
    assert "2. Calcular el valor total." in result["response"]
    if frame:
        envelope = flow.conv_store.load_envelope("multi").envelope
        assert envelope.continuation["owner"] == "NONE"
        assert not envelope.execution_record


@pytest.mark.parametrize("raw,kwargs,route", [
    ("/reset", {}, "reset"),
    ("mis radares", {}, "radar"),
    ("muestrame mas", {}, "pagination_no_context"),
    ("CO1.BDOS.123456", {}, "direct_lookup"),
    ("opción 1", {"actor_candidate_selection": {"operation": "SELECT_ACTOR_CANDIDATE",
                                                "candidate_rank": 1, "pending_token": "stale"}}, "actor_selection"),
    ("1", {}, "suggestion_invalid"),
    ("refinar", {"seed_resolved_params": {"dataset": "procesos", "objeto": ["mantenimiento"]}}, "refinement"),
])
def test_actual_fast_paths_never_call_provider(make_flow, raw, kwargs, route):
    flow = make_flow()
    result = run(flow, raw, chat_id="fast", **kwargs)
    assert result["route_reason"].startswith(route)
    flow.natural_alpha_provider.observe.assert_not_called()
    assert not any(key.startswith("natural_alpha_") for key in result)
