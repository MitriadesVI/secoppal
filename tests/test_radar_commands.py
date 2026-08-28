"""Tests de app/core/radar/commands.py y el handler handle_radar_command."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.conversation_store import ConversationStore
from app.core.formatter import Formatter
from app.core.radar import follows
from app.core.radar.commands import (
    LIST,
    RUN,
    SAVE,
    parse_radar_command,
    radar_owner,
)
from app.core.radar.store import RadarStore
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.context import HandlerContext
from app.core.workflow.handlers.radar import handle_radar_command


# ── Parsing ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("text,name", [
    ("guardar esta búsqueda como Radar PAE Caribe", "PAE Caribe"),
    ("guarda esto como radar Adulto Mayor Barranquilla", "Adulto Mayor Barranquilla"),
    ("guardar como Radar Cultura Cesar", "Cultura Cesar"),
    ("crear radar Obras Costa Atlántica", "Obras Costa Atlántica"),
    ("crea un radar llamado Mantenimiento Eje Cafetero", "Mantenimiento Eje Cafetero"),
])
def test_parse_save(text, name):
    cmd = parse_radar_command(text)
    assert cmd is not None and cmd.kind == SAVE
    assert cmd.name == name


@pytest.mark.parametrize("text", [
    "mis radares", "ver radares", "listar radares",
    "ver mis radares", "lista de radares", "mostrar radares",
])
def test_parse_list(text):
    cmd = parse_radar_command(text)
    assert cmd is not None and cmd.kind == LIST


@pytest.mark.parametrize("text,name", [
    ("ejecutar radar PAE Caribe", "PAE Caribe"),
    ("ejecuta el radar Cultura Cesar", "Cultura Cesar"),
    ("revisar radar Adulto Mayor", "Adulto Mayor"),
    ("revisa mi radar Obras", "Obras"),
    ("actualizar radar PAE", "PAE"),
])
def test_parse_run(text, name):
    cmd = parse_radar_command(text)
    assert cmd is not None and cmd.kind == RUN
    assert cmd.name == name


@pytest.mark.parametrize("text", [
    "contratos de PAE en Atlántico",
    "muéstrame procesos de obra",
    "qué es un radar de velocidad",  # menciona radar pero no es comando
    "",
])
def test_parse_non_command(text):
    # "qué es un radar de velocidad" no encaja en ningún patrón de comando.
    assert parse_radar_command(text) is None


def test_owner_prefers_channel_user_key():
    assert radar_owner({"channel_user_key": "email:a@b.com"}, "st_x") == "email:a@b.com"
    assert radar_owner({}, "st_x") == "st_x"
    assert radar_owner(None, None) == "anon"


# ── Handler ──────────────────────────────────────────────────────────────

class FakeClient:
    def __init__(self, rows, query_error=None):
        self.rows = rows
        self.query_error = query_error
        self.queries = []

    def query(self, dataset_id, soql):
        self.queries.append((dataset_id, soql))
        if self.query_error:
            raise self.query_error
        return list(self.rows)

    def count(self, dataset_id, soql):
        return len(self.rows)


class ReentrantClient(FakeClient):
    """Dispara un segundo comando «ejecutar radar» mientras el primero está
    en Socrata — reproduce la carrera scheduler-vs-chat a nivel de handler."""

    def __init__(self, rows, *, on_first_query):
        super().__init__(rows)
        self._on_first_query = on_first_query
        self.reentrant_result = None
        self._fired = False

    def query(self, dataset_id, soql):
        if not self._fired:
            self._fired = True
            self.reentrant_result = self._on_first_query()
        return super().query(dataset_id, soql)


@pytest.fixture
def env(tmp_path):
    conv = ConversationStore(base_path=tmp_path / "conv")
    store = RadarStore(tmp_path / "radar.db")
    rows = [{
        "id_del_proceso": "P-1",
        "referencia_del_proceso": "REF-1",
        "nombre_del_procedimiento": "Suministro PAE",
        "entidad": "ALCALDIA",
        "departamento_entidad": "ATLÁNTICO",
        "precio_base": "1000000",
        "estado_de_apertura_del_proceso": "Abierto",
        "urlproceso": "http://x/P-1",
        "fecha_de_publicacion_del": "2026-05-01",
    }]
    deps = SimpleNamespace(
        radar_store=store,
        conv_store=conv,
        soql_builder=SoQLBuilder(),
        secop_client=FakeClient(rows),
        formatter=Formatter(),
    )
    return deps, store, conv


def _ctx(query, chat_id="st_anon_test"):
    return HandlerContext(
        user_query=query, channel="streamlit", chat_id=chat_id,
        user_identity={"channel_user_key": "anon:test"},
    )


def test_handler_returns_none_without_chat_id(env):
    deps, _, _ = env
    ctx = HandlerContext(user_query="mis radares", chat_id=None)
    assert handle_radar_command(deps, ctx) is None


def test_handler_passthrough_non_command(env):
    deps, _, _ = env
    assert handle_radar_command(deps, _ctx("contratos de obra en Bogotá")) is None


def test_save_without_prior_search(env):
    deps, store, _ = env
    res = handle_radar_command(deps, _ctx("guardar esta búsqueda como Radar X"))
    assert res is not None
    assert res["route_reason"] == "radar_save_no_context"
    assert store.list_radars("anon:test") == []


def test_save_from_last_turn(env):
    deps, store, conv = env
    conv.append_turn(
        chat_id="st_anon_test",
        user_query="procesos de PAE en Atlántico",
        response="...",
        parsed_params={"objeto": ["alimentacion_escolar"],
                       "departamento_resolved": "ATLÁNTICO", "dataset": "procesos"},
        result_ids=[],
    )
    res = handle_radar_command(deps, _ctx("guardar esta búsqueda como Radar PAE Atlántico"))
    assert res["route_reason"] == "radar_saved"
    radars = store.list_radars("anon:test")
    assert len(radars) == 1
    assert radars[0].name == "PAE Atlántico"
    assert radars[0].frame_dict()["scope"]["departamento_resolved"] == "ATLÁNTICO"


def test_save_strips_ecosystem_and_resolution(env):
    """Regresión: entidad_ecosystem y *_resolution no deben persistirse.

    normal_flow inyecta entidad_ecosystem en resolved_params pero NO re-ejecuta
    la query, así que replayearlo divergía (≈0 resultados). El radar debe
    guardar la búsqueda que el usuario realmente vio.
    """
    deps, store, conv = env
    conv.append_turn(
        chat_id="st_anon_test", user_query="mantenimiento en cundinamarca",
        response="...",
        parsed_params={
            "objeto": ["mantenimiento"],
            "departamento_resolved": "Cundinamarca",
            "departamento_resolution": {"value": "Cundinamarca", "method": "scan_exact"},
            "entidad_ecosystem": ["DEPARTAMENTO DE CUNDINAMARCA", "SECRETARÍA X"],
            "dataset": "procesos",
        },
        result_ids=[],
    )
    handle_radar_command(deps, _ctx("guarda esta busqueda como radar mant-cund"))
    radar = store.get_radar_by_name("anon:test", "mant-cund")
    frame_blob = __import__("json").dumps(radar.frame_dict(), ensure_ascii=False)
    assert "entidad_ecosystem" not in frame_blob
    assert "departamento_resolution" not in frame_blob
    # pero sí conserva el filtro real
    assert radar.frame_dict()["scope"]["departamento_resolved"] == "Cundinamarca"
    assert radar.frame_dict()["topic"] == ["mantenimiento"]


def test_save_strips_ordering_signal(env):
    deps, store, conv = env
    conv.append_turn(
        chat_id="st_anon_test", user_query="procesos PAE de mayor valor",
        response="...",
        parsed_params={
            "objeto": ["alimentacion_escolar"],
            "departamento_resolved": "ATLÁNTICO",
            "dataset": "procesos",
            "ordering_signal": "valor_desc",
        },
        result_ids=[],
    )
    handle_radar_command(deps, _ctx("guarda esta busqueda como radar pae-reciente"))
    radar = store.get_radar_by_name("anon:test", "pae-reciente")
    assert "ordering_signal" not in radar.frame_dict().get("modifiers", {})


def test_save_duplicate(env):
    deps, store, conv = env
    conv.append_turn(
        chat_id="st_anon_test", user_query="q", response="r",
        parsed_params={"departamento_resolved": "ATLÁNTICO"}, result_ids=[],
    )
    handle_radar_command(deps, _ctx("guardar esto como Radar Dup"))
    res = handle_radar_command(deps, _ctx("guardar esto como Radar Dup"))
    assert res["route_reason"] == "radar_save_duplicate"


def test_save_with_the_reserved_name_is_refused_with_its_own_reason(env):
    """El nombre de la watchlist no es un duplicado del usuario ni un error.

    Sin este catch, RadarReservedNameError subía como excepción no manejada.
    Tiene route_reason propio para no contaminar la telemetría de duplicados.
    """
    deps, store, conv = env
    conv.append_turn(
        chat_id="st_anon_test", user_query="q", response="r",
        parsed_params={"departamento_resolved": "ATLÁNTICO"}, result_ids=[],
    )

    res = handle_radar_command(
        deps, _ctx(f"guardar esto como Radar {follows.FOLLOWS_NAME}"))

    assert res["route_reason"] == "radar_save_reserved_name"
    assert "procesos seguidos" in res["response"].lower()
    # no se creó nada ni se tocó la watchlist
    assert store.get_radar_by_name("anon:test", follows.FOLLOWS_NAME) is None


def test_save_with_a_case_variant_of_the_reserved_name_is_refused(env):
    deps, store, conv = env
    conv.append_turn(
        chat_id="st_anon_test", user_query="q", response="r",
        parsed_params={"departamento_resolved": "ATLÁNTICO"}, result_ids=[],
    )
    variant = follows.FOLLOWS_NAME.replace("Procesos", "procesos")

    res = handle_radar_command(deps, _ctx(f"guardar esto como Radar {variant}"))

    assert res["route_reason"] == "radar_save_reserved_name"


def test_list_empty_then_populated(env):
    deps, store, _ = env
    res = handle_radar_command(deps, _ctx("mis radares"))
    assert res["route_reason"] == "radar_list_empty"

    store.create_radar(name="PAE Caribe", owner="anon:test",
                       query_frame_json='{"scope":{"departamento_resolved":"ATLÁNTICO"}}',
                       dataset="procesos")
    res = handle_radar_command(deps, _ctx("mis radares"))
    assert res["route_reason"] == "radar_list"
    assert "PAE Caribe" in res["response"]


def test_run_not_found(env):
    deps, _, _ = env
    res = handle_radar_command(deps, _ctx("ejecutar radar Inexistente"))
    assert res["route_reason"] == "radar_run_not_found"


def test_run_end_to_end(env):
    deps, store, conv = env
    store.create_radar(
        name="PAE Atlántico", owner="anon:test",
        query_frame_json='{"dataset":"procesos","scope":{"departamento_resolved":"ATLÁNTICO"},'
                         '"topic":["alimentacion_escolar"]}',
        dataset="procesos",
    )
    res = handle_radar_command(deps, _ctx("ejecutar radar PAE Atlántico"))
    assert res["route_reason"] == "radar_run_ok"
    assert "Radar PAE Atlántico" in res["response"]
    # segunda corrida: sin novedades
    res2 = handle_radar_command(deps, _ctx("ejecutar radar PAE Atlántico"))
    assert "No hay novedades desde la última revisión completa" in res2["response"]


# ── P1A · route_reason no confunde control con error ──────────────────────

def _save_and_get(store, name="PAE Atlántico"):
    store.create_radar(
        name=name, owner="anon:test",
        query_frame_json='{"dataset":"procesos","scope":{"departamento_resolved":"ATLÁNTICO"},'
                         '"topic":["alimentacion_escolar"]}',
        dataset="procesos",
    )


def _deps_with_client(base_deps, client):
    return SimpleNamespace(
        radar_store=base_deps.radar_store, conv_store=base_deps.conv_store,
        soql_builder=base_deps.soql_builder, secop_client=client,
        formatter=base_deps.formatter,
    )


def test_run_ok_route_reason_intact(env):
    """radar_ok conserva su route_reason existente (no regresión)."""
    deps, store, _ = env
    _save_and_get(store)
    res = handle_radar_command(deps, _ctx("ejecutar radar PAE Atlántico"))
    assert res["route_reason"] == "radar_run_ok"


def test_run_timeout_still_maps_to_error(env):
    """Un fallo real de corrida (timeout) sigue siendo radar_run_error."""
    deps, store, _ = env
    _save_and_get(store)
    timeout_deps = _deps_with_client(deps, FakeClient([], query_error=TimeoutError("timed out")))
    res = handle_radar_command(timeout_deps, _ctx("ejecutar radar PAE Atlántico"))
    assert res["route_reason"] == "radar_run_error"


def test_run_permanent_error_still_maps_to_error(env):
    """Fallo permanente (QueryFrame inválido) también es radar_run_error."""
    deps, store, _ = env
    store.create_radar(name="Roto", owner="anon:test", query_frame_json="{no-json")
    res = handle_radar_command(deps, _ctx("ejecutar radar Roto"))
    assert res["route_reason"] == "radar_run_error"


def test_run_already_running_is_not_telemetrized_as_error(env):
    """already_running es un skip de control, no un radar_run_error."""
    deps, store, _ = env
    _save_and_get(store)
    second_client = FakeClient([])
    second_deps = _deps_with_client(deps, second_client)

    def second_call():
        return handle_radar_command(second_deps, _ctx("ejecutar radar PAE Atlántico"))

    first_client = ReentrantClient([], on_first_query=second_call)
    first_deps = _deps_with_client(deps, first_client)
    handle_radar_command(first_deps, _ctx("ejecutar radar PAE Atlántico"))
    second_res = first_client.reentrant_result

    assert second_res["route_reason"] == "radar_run_skipped"
    assert second_res["route_reason"] != "radar_run_error"
    assert second_client.queries == []          # el segundo no tocó SECOP
    assert second_res["response"]               # conserva el texto de control


def test_run_not_found_race_maps_to_dedicated_route_reason(env, monkeypatch):
    """Radar borrado entre la resolución del handler y el lease del runner.

    Distinto de 'radar_run_not_found' (miss por nombre, ya cubierto arriba):
    aquí el handler SÍ encontró el radar, pero desapareció justo después.
    """
    deps, store, _ = env
    _save_and_get(store)
    stale = store.get_radar_by_name("anon:test", "PAE Atlántico")
    store.delete_radar(stale.id)
    monkeypatch.setattr(store, "get_radar_by_name", lambda owner, name: stale)

    res = handle_radar_command(deps, _ctx("ejecutar radar PAE Atlántico"))

    assert res["route_reason"] == "radar_not_found"
    assert res["route_reason"] != "radar_run_not_found"
