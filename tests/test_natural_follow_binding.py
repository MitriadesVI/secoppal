"""Alpha FOLLOW/LIST and FOLLOW/RUN open the existing Radar commands.

Natural phrasings never match the literal radar grammar and used to reach the
tabular search as a topic («Procesos de busquedas, tengo, guardadas»). The pair
chooses the command; RUN's radar is accredited by the owner's store, not by the
model's quote.
"""
import json
import socket
import sqlite3
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.config import Settings, get_settings
from app.core.capability_binding.registry import FOLLOW_LIST, FOLLOW_RUN, capability_for
from app.core.conversation_store import ConversationStore
from app.core.natural_alpha.models import GroundedJob, Observation
from app.core.natural_alpha.provider import ProviderResult
from app.core.orchestrator import SecopalWorkflow
from app.core.radar.builder import save_radar_from_params
from app.core.radar.commands import radar_owner
from app.core.radar.name_mentions import radars_named_in
from tests.test_natural_alpha_contract import IDENTITY

CHAT = "chat-follow"


class RecordingSource:
    timeout = 15

    def __init__(self):
        self.calls = []

    def with_timeout(self, timeout):
        return self

    derived_for_timeout = with_timeout

    def aggregate(self, dataset, soql, **kwargs):
        self.calls.append(soql)
        return []

    def query(self, dataset, soql, **kwargs):
        self.calls.append(soql)
        return []

    def count(self, dataset, soql, **kwargs):
        self.calls.append(soql)
        return 0


def _jobs(*jobs):
    return json.dumps({"jobs": [
        {"action": action, "aspect": aspect, "evidence_quote": quote, "occurrence": 1}
        for action, aspect, quote in jobs
    ]})


@pytest.fixture(params=["false", "true"], ids=["frame_off", "frame_on"])
def make_flow(request, tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", request.param)
    get_settings.cache_clear()
    import burr.telemetry
    monkeypatch.setattr(burr.telemetry, "g_telemetry_enabled", False)

    def forbidden(*args, **kwargs):
        raise AssertionError("no network in this test")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    counter = iter(range(1000))

    def build(*jobs, enabled=True, mode="alpha", radars=()):
        root = tmp_path / str(next(counter))
        settings = Settings(
            _env_file=None, deepseek_api_key=None, secoppal_natural_alpha_mode=mode,
            secoppal_natural_alpha_email="alpha@example.test",
            secoppal_natural_follow_binding_enabled=enabled,
            secop_feedback_path=root / "traces.jsonl", secop_feedback_db_path=root / "feedback.db",
        )
        flow = SecopalWorkflow(settings)
        flow.conv_store = ConversationStore(root / "conversations")
        flow.secop_client = RecordingSource()
        flow.natural_alpha_provider = Mock(observe=Mock(
            return_value=ProviderResult(_jobs(*jobs), provider_calls=1)))
        owner = radar_owner(IDENTITY, CHAT)
        for name in radars:
            save_radar_from_params(
                flow.radar_store, owner=owner, name=name,
                params={"objeto": ["alimentacion escolar"], "departamento_resolved": "Atlántico"},
            )
        return flow

    yield build
    get_settings.cache_clear()


def _run(flow, text, chat_id=CHAT):
    return flow.run_query(text, channel="streamlit", chat_id=chat_id,
                          user_identity=IDENTITY, today=date(2026, 9, 15))


def _traces(flow):
    with sqlite3.connect(flow.settings.secop_feedback_db_path) as conn:
        return conn.execute(
            "SELECT route_reason, natural_alpha_status, natural_alpha_jobs FROM traces",
        ).fetchall()


# ── Positives: natural phrasings reach the Radar owner ─────────────────────

@pytest.mark.parametrize("text", [
    "qué búsquedas tengo guardadas",
    "muéstrame lo que estoy siguiendo",
    "cuáles son mis alertas?",
    "enséñame mis seguimientos por favor",
])
def test_list_phrasings_open_the_radar_list(make_flow, text):
    flow = make_flow(("FOLLOW", "LIST", text), radars=("PAE Caribe",))
    result = _run(flow, text)
    assert result["route_reason"] == "radar_list"
    assert "PAE Caribe" in result["response"]
    assert flow.secop_client.calls == []
    (route, status, jobs), = _traces(flow)
    assert (route, status) == ("radar_list", "MATCHED")
    assert json.loads(jobs)[0]["aspect"] == "LIST"


def test_list_without_radars_uses_the_existing_empty_answer(make_flow):
    flow = make_flow(("FOLLOW", "LIST", "qué tengo guardado"))
    result = _run(flow, "qué tengo guardado")
    assert result["route_reason"] == "radar_list_empty"
    assert flow.secop_client.calls == []


@pytest.mark.parametrize("text", [
    "¿hay novedades en PAE Caribe?",
    "qué hay de nuevo en pae caribe",
    "revisa cómo va el de PAE CARIBE, porfa",
])
def test_run_phrasings_run_the_one_named_radar(make_flow, text):
    flow = make_flow(("FOLLOW", "RUN", text), radars=("PAE Caribe", "Obras Cesar"))
    result = _run(flow, text)
    assert result["route_reason"].startswith("radar_run")
    assert result["route_reason"] not in {"radar_run_name_required", "radar_run_name_ambiguous",
                                          "radar_run_not_found"}
    assert flow.secop_client.calls, "the radar runner must query its own frame"
    assert all("alimentacion" in soql.lower() for soql in flow.secop_client.calls)


def test_run_accepts_a_name_with_accents_the_person_omitted(make_flow):
    flow = make_flow(("FOLLOW", "RUN", "novedades"), radars=("Educación Atlántico",))
    result = _run(flow, "novedades de educacion atlantico")
    assert result["route_reason"].startswith("radar_run")
    assert result["route_reason"] != "radar_run_name_required"


# ── Negatives: RUN never chooses a radar the store did not accredit ────────

@pytest.mark.parametrize("text", [
    "¿hay novedades?",
    "revisa mis alertas",
    "qué hay de nuevo en Caribe",      # part of a name is not the name
])
def test_run_without_a_named_radar_lists_and_runs_nothing(make_flow, text):
    flow = make_flow(("FOLLOW", "RUN", text), radars=("PAE Caribe", "Obras Cesar"))
    result = _run(flow, text)
    assert result["route_reason"] == "radar_run_name_required"
    assert result["needs_clarification"] is True
    assert "PAE Caribe" in result["response"] and "Obras Cesar" in result["response"]
    assert flow.secop_client.calls == []


def test_run_naming_two_radars_does_not_pick_one(make_flow):
    text = "qué hay nuevo en PAE Caribe"
    flow = make_flow(("FOLLOW", "RUN", text), radars=("PAE", "PAE Caribe"))
    result = _run(flow, text)
    assert result["route_reason"] == "radar_run_name_ambiguous"
    assert flow.secop_client.calls == []


def test_run_without_radars_answers_with_the_empty_list(make_flow):
    flow = make_flow(("FOLLOW", "RUN", "hay novedades?"))
    result = _run(flow, "hay novedades?")
    assert result["route_reason"] == "radar_list_empty"
    assert flow.secop_client.calls == []


def test_search_with_follow_vocabulary_as_topic_stays_a_search(make_flow):
    text = "procesos de seguimiento a obras en Soledad"
    flow = make_flow(("SEARCH", "PROCESSES", text), radars=("PAE Caribe",))
    result = _run(flow, text)
    assert not result["route_reason"].startswith("radar_")


def test_save_stays_unbound_and_creates_no_radar(make_flow):
    flow = make_flow(("FOLLOW", "SAVE", "guárdame esta búsqueda"))
    result = _run(flow, "guárdame esta búsqueda")
    assert not result["route_reason"].startswith("radar_")
    assert flow.radar_store.list_radars(radar_owner(IDENTITY, CHAT)) == []


# ── Rollout, shadow, multi-job, invalid quote, precedence ──────────────────

def test_flag_off_keeps_the_legacy_route(make_flow):
    text = "qué búsquedas tengo guardadas"
    result = _run(make_flow(("FOLLOW", "LIST", text), enabled=False, radars=("PAE",)), text)
    assert not result["route_reason"].startswith("radar_")


def test_shadow_never_binds(make_flow):
    text = "qué búsquedas tengo guardadas"
    result = _run(make_flow(("FOLLOW", "LIST", text), mode="shadow", radars=("PAE",)), text)
    assert not result["route_reason"].startswith("radar_")


def test_quote_outside_the_text_binds_nothing(make_flow):
    text = "qué búsquedas tengo guardadas"
    flow = make_flow(("FOLLOW", "LIST", "mis radares favoritos"), radars=("PAE",))
    result = _run(flow, text)
    assert result["natural_alpha_status"] == "INVALID"
    assert not result["route_reason"].startswith("radar_")


@pytest.mark.parametrize("jobs, text", [
    ((("SEARCH", "OPEN_OPPORTUNITIES", "oportunidades de aseo"), ("FOLLOW", "SAVE", "guárdalas")),
     "oportunidades de aseo y guárdalas"),
    ((("FOLLOW", "LIST", "mis alertas"), ("FOLLOW", "RUN", "revisa PAE")),
     "mis alertas y revisa PAE"),
])
def test_multi_job_stays_stepwise(make_flow, jobs, text):
    flow = make_flow(*jobs, radars=("PAE",))
    result = _run(flow, text)
    assert result["route_reason"] == "natural_alpha_multi_job_stepwise"
    assert flow.secop_client.calls == []


def test_literal_radar_command_keeps_its_fast_path_without_alpha(make_flow):
    flow = make_flow(("FOLLOW", "LIST", "mis radares"), radars=("PAE",))
    result = _run(flow, "mis radares")
    assert result["route_reason"] == "radar_list"
    flow.natural_alpha_provider.observe.assert_not_called()


def test_without_a_chat_the_turn_stays_legacy(make_flow):
    text = "qué búsquedas tengo guardadas"
    result = _run(make_flow(("FOLLOW", "LIST", text), radars=("PAE",)), text, chat_id=None)
    assert not result["route_reason"].startswith("radar_")


# ── Registry and name authority ────────────────────────────────────────────

def _observation(action, aspect, mode="alpha", status="MATCHED"):
    return Observation(mode, status, (GroundedJob(action, aspect, 0, 4),))


@pytest.mark.parametrize("flags, expected", [
    ({"secoppal_natural_follow_binding_enabled": True}, FOLLOW_RUN),
    ({"secoppal_a2_suppliers_binding_enabled": True,
      "secoppal_contract_activity_binding_enabled": True}, None),
    ({}, None),
])
def test_follow_has_its_own_rollout_line(flags, expected):
    assert capability_for(_observation("FOLLOW", "RUN"), SimpleNamespace(**flags)) == expected


def test_follow_flag_opens_neither_save_paginate_nor_suppliers():
    settings = SimpleNamespace(secoppal_natural_follow_binding_enabled=True)
    assert capability_for(_observation("FOLLOW", "LIST"), settings) == FOLLOW_LIST
    for pair in (("FOLLOW", "SAVE"), ("RESULT_ACTION", "PAGINATE"),
                 ("RESULT_ACTION", "EXPORT_XLSX"), ("ANALYZE", "SUPPLIERS")):
        assert capability_for(_observation(*pair), settings) is None


def _radar(name):
    return SimpleNamespace(name=name)


def test_name_authority_is_literal_whole_word_and_accent_blind():
    radars = (_radar("Obras"), _radar("PAE Caribe"), _radar("Educación-Atlántico"))
    names = lambda text: [r.name for r in radars_named_in(text, radars)]  # noqa: E731
    assert names("maniobras en el caribe") == []
    assert names("novedades de pae  caribe?") == ["PAE Caribe"]
    assert names("revisa educacion atlantico") == ["Educación-Atlántico"]
    assert names("OBRAS y PAE Caribe") == ["Obras", "PAE Caribe"]


# ── R2: prefix names, owner scoping at the call-site, the follows watchlist ─

def test_run_with_only_the_head_of_a_name_runs_nothing(make_flow):
    # «PAE» is the head of «PAE Caribe», not its name: first-token matching
    # would run the radar the person did not name.
    flow = make_flow(("FOLLOW", "RUN", "revisa PAE"), radars=("PAE Caribe",))
    result = _run(flow, "revisa PAE")
    assert result["route_reason"] == "radar_run_name_required"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []
    assert [r.name for r in radars_named_in("revisa PAE", (_radar("PAE Caribe"),))] == []


FOREIGN = {"channel_user_key": "email:otro@example.test"}
FOREIGN_CHAT = "chat-ajeno"


def _save(flow, owner, name):
    save_radar_from_params(
        flow.radar_store, owner=owner, name=name,
        params={"objeto": ["alimentacion escolar"], "departamento_resolved": "Atlántico"},
    )
    return flow.radar_store.get_radar_by_name(owner, name)


def test_run_uses_only_the_owners_radar_when_another_owner_has_the_same_name(make_flow):
    text = "revisa PAE Caribe"
    flow = make_flow(("FOLLOW", "RUN", text), radars=("PAE Caribe",))
    foreign = _save(flow, radar_owner(FOREIGN, FOREIGN_CHAT), "PAE Caribe")
    result = _run(flow, text)
    assert result["route_reason"] not in {"radar_run_name_required", "radar_run_name_ambiguous",
                                          "radar_run_not_found"}
    own = flow.radar_store.get_radar_by_name(radar_owner(IDENTITY, CHAT), "PAE Caribe")
    assert own.last_run_at is not None
    assert flow.radar_store.get_radar(foreign.id) == foreign


@pytest.mark.parametrize("own_radars, reason", [
    (("Obras Cesar",), "radar_run_name_required"),
    ((), "radar_list_empty"),
])
def test_another_owners_radar_is_never_a_run_candidate(make_flow, own_radars, reason):
    text = "revisa PAE Caribe"
    flow = make_flow(("FOLLOW", "RUN", text), radars=own_radars)
    foreign = _save(flow, radar_owner(FOREIGN, FOREIGN_CHAT), "PAE Caribe")
    result = _run(flow, text)
    assert result["route_reason"] == reason
    assert flow.secop_client.calls == []
    assert flow.radar_store.get_radar(foreign.id) == foreign


def test_follows_watchlist_is_not_runnable_by_natural_run(make_flow):
    from app.core.radar.follows import get_or_create_follows_radar

    text = "revisa mis procesos seguidos"
    flow = make_flow(("FOLLOW", "RUN", text), radars=("Obras Cesar",))
    watchlist = get_or_create_follows_radar(flow.radar_store, radar_owner(IDENTITY, CHAT))
    for _ in range(3):  # three permanent failures would pause the watchlist
        result = _run(flow, text)
        assert result["route_reason"] == "radar_run_name_required"
        assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []
    after = flow.radar_store.get_radar(watchlist.id)
    assert (after.failure_count, after.permanent_failure_streak, after.needs_attention,
            after.enabled, after.last_run_at) == (0, 0, 0, 1, None)


def test_search_radar_named_like_the_watchlist_runs_by_its_full_name(make_flow):
    from app.core.radar.follows import get_or_create_follows_radar

    text = "revisa procesos seguidos Cartagena"
    flow = make_flow(("FOLLOW", "RUN", text), radars=("Procesos seguidos Cartagena",))
    watchlist = get_or_create_follows_radar(flow.radar_store, radar_owner(IDENTITY, CHAT))
    result = _run(flow, text)
    assert result["route_reason"] not in {"radar_run_name_required", "radar_run_name_ambiguous",
                                          "radar_run_not_found"}
    assert flow.secop_client.calls
    assert flow.radar_store.get_radar(watchlist.id) == watchlist
