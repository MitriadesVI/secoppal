"""Pruebas end-to-end offline del piloto real Adulto Mayor (TG-P2D1, endurecido).

Sin red, sin Telegram, sin bases reales: un ``SecopClient`` falso y un proveedor
falso. La ruta canónica ``REAL_PILOT_DB_PATH`` se monkeypatcha a un ``tmp_path``;
el código productivo nunca acepta una ruta por argumento. Cubre el ciclo operativo
completo, los guards de ruta, la identidad exacta del endpoint, el drift de config,
el baseline sin posibilidad de batch, el aislamiento por par, el preview-antes-de-
consulta, el preflight de envío y el status de solo lectura.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.core.formatter import Formatter
from app.core.notify.endpoint_store import EndpointStore
from app.core.notify.masking import Masker
from app.core.notify.outbox_store import BatchEventRef, OutboxStore
from app.core.notify.preference_store import PreferenceStore
from app.core.notify.provider import (
    CONFIRMED_EXTERNAL_EFFECT,
    NO_EXTERNAL_EFFECT,
    ProviderResult,
)
from app.core.notify.telegram_provider import (
    TELEGRAM_ALLOWED_CHAT_IDS_ENV,
    TELEGRAM_BOT_TOKEN_ENV,
)
from app.core.radar.runner import run_radar
from app.core.radar.store import RadarStore
from app.core.soql_builder import SoQLBuilder
from scripts import pilot_real
from scripts.pilot_real import PilotRealError


TOKEN = "123456:pilot-real-secret"
CHAT_ID = "555000111"
MASKING_KEY = "pilot-real-test-masking-key"
# Fuera de las quiet hours America/Bogota (21:00–07:00): 15:00 UTC = 10:00 Bogotá.
NOW = datetime(2026, 7, 15, 15, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    monkeypatch.setenv(TELEGRAM_BOT_TOKEN_ENV, TOKEN)
    monkeypatch.setenv(TELEGRAM_ALLOWED_CHAT_IDS_ENV, CHAT_ID)
    monkeypatch.setenv(pilot_real.NOTIFICATION_MASKING_KEY_ENV, MASKING_KEY)


@pytest.fixture
def pilot(tmp_path, monkeypatch):
    """Fija la ruta canónica del piloto a un tmp_path resuelto y la devuelve."""
    db = (tmp_path / "telegram_p2_real.db").resolve()
    monkeypatch.setattr(pilot_real, "REAL_PILOT_DB_PATH", db)
    return db


# ── Dobles de prueba ──────────────────────────────────────────────────────────

class FakeClient:
    def __init__(self, rows, query_error=None):
        self.rows = rows
        self.query_error = query_error
        self.queries = []

    def query(self, dataset_id, soql):
        self.queries.append((dataset_id, soql))
        if " in(" in soql and (
            "id_del_proceso in(" in soql or "id_contrato in(" in soql
        ):
            return []
        if self.query_error is not None:
            raise self.query_error
        return list(self.rows)

    def count(self, dataset_id, soql):
        return len(self.rows)


def _deps(rows, query_error=None):
    return SimpleNamespace(
        soql_builder=SoQLBuilder(),
        secop_client=FakeClient(rows, query_error=query_error),
        formatter=Formatter(),
    )


class _BombClient:
    def query(self, *a, **k):
        raise AssertionError("no debe consultarse SECOP en este camino")

    def count(self, *a, **k):
        raise AssertionError("no debe consultarse SECOP en este camino")


def _bomb_deps():
    return SimpleNamespace(
        soql_builder=SoQLBuilder(), secop_client=_BombClient(), formatter=Formatter()
    )


def _proc(pid, ref, nombre, desc, valor="186600000", estado="Abierto"):
    return {
        "id_del_proceso": pid,
        "referencia_del_proceso": ref,
        "nombre_del_procedimiento": nombre,
        "descripci_n_del_procedimiento": desc,
        "entidad": "ALCALDIA MUNICIPAL DE RAQUIRA",
        "departamento_entidad": "BOYACÁ",
        "precio_base": valor,
        "estado_de_apertura_del_proceso": estado,
        "urlproceso": f"http://secop/{pid}",
        "fecha_de_publicacion_del": "2026-05-01",
    }


_BASELINE_ROWS = [
    _proc("CO1.REQ.1", "REF-1", "Programa adulto mayor uno",
          "SUMINISTRO DE KITS ADULTO MAYOR " * 6),
    _proc("CO1.REQ.2", "REF-2", "Programa persona mayor dos",
          "APOYO NUTRICIONAL PERSONA MAYOR " * 6),
]

_RAQUIRA_LONG_DESC = (
    "SUMINISTRO DE KITS DE APOYO NUTRICIONAL PARA POBLACION VULNERABLE "
    "PROGRAMAS ADULTO MAYOR Y POBLACION EN CONDICION DE DISCAPACIDAD DEL "
    "MUNICIPIO DE RAQUIRA BOYACA PARA LA VIGENCIA 2026 "
) + "TEXTO DE RELLENO " * 40
_NOVELTY_ROW = _proc(
    "CO1.REQ.10686701", "MR- SASIP-002-2026", "MR- SASIP-002-2026", _RAQUIRA_LONG_DESC
)


class FakeProvider:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def send(self, request):
        self.calls.append(request)
        return self.result


def _sent_provider(message_id="9001"):
    return FakeProvider(
        ProviderResult(
            effect=CONFIRMED_EXTERNAL_EFFECT, retryable=False,
            provider_message_id=message_id,
        )
    )


def _bomb_factory(*args, **kwargs):
    raise AssertionError("no debe construirse el worker en este camino")


# ── Helpers de escenario ──────────────────────────────────────────────────────

def _setup():
    return pilot_real.setup_real_pilot(output=lambda _: None)


def _baseline(deps):
    return pilot_real.baseline_real_pilot(deps=deps, output=lambda _: None)


def _make_pending(deps):
    """setup ya hecho: baseline + una novedad → exactamente un batch pending."""
    _baseline(deps)
    deps.secop_client.rows = _BASELINE_ROWS + [_NOVELTY_ROW]
    return pilot_real.run_real_pilot(deps=deps, now=NOW, output=lambda _: None)


# ══ 1 · RUTA CANÓNICA ═════════════════════════════════════════════════════════

def test_guard_acepta_la_ruta_canonica(pilot):
    assert pilot_real._require_db_matches(pilot) == pilot


def test_guard_rechaza_tmp(pilot):
    with pytest.raises(PilotRealError, match="requires_canonical"):
        pilot_real._require_db_matches("/tmp/telegram_p2_real.db")


def test_guard_rechaza_otra_raiz(pilot, tmp_path):
    otra = tmp_path / "otra_raiz" / "data" / "telegram_p2_real.db"
    with pytest.raises(PilotRealError, match="requires_canonical"):
        pilot_real._require_db_matches(otra)
    assert not otra.exists()


def test_guard_rechaza_symlink_a_ruta_distinta(pilot, tmp_path):
    decoy = tmp_path / "decoy.db"
    decoy.write_bytes(b"")
    # Symlink en la ubicación canónica que apunta a otra parte: resolve() lo sigue.
    pilot.symlink_to(decoy)
    with pytest.raises(PilotRealError, match="requires_canonical"):
        pilot_real._require_db_matches(pilot)


def test_guard_razones_especificas_para_bases_prohibidas(pilot):
    with pytest.raises(PilotRealError, match="refuses_primary_database"):
        pilot_real._require_db_matches(pilot_real.SECOPPAL_DB_PATH)
    with pytest.raises(PilotRealError, match="refuses_demo_database"):
        pilot_real._require_db_matches(pilot_real.DEMO_DB_PATH)


def test_comandos_que_requieren_existencia_no_crean_la_base(pilot):
    for fn in (
        lambda: pilot_real.baseline_real_pilot(deps=_bomb_deps(), output=lambda _: None),
        lambda: pilot_real.run_real_pilot(deps=_bomb_deps(), output=lambda _: None),
        lambda: pilot_real.status_real_pilot(output=lambda _: None),
    ):
        with pytest.raises(PilotRealError, match="database_not_found"):
            fn()
    assert not pilot.exists()


# ══ 2 · IDENTIDAD EXACTA DEL ENDPOINT ════════════════════════════════════════

def test_setup_repetido_mismo_chat_conserva_endpoint(pilot):
    first = _setup()
    second = _setup()
    assert first == second

    conn = sqlite3.connect(pilot)
    conn.row_factory = sqlite3.Row
    try:
        assert conn.execute(
            "SELECT COUNT(*) n FROM notification_endpoints"
        ).fetchone()["n"] == 1
    finally:
        conn.close()


def test_setup_chat_distinto_falla_cerrado_y_no_retargetea(pilot, monkeypatch):
    ids = _setup()
    recipient_before = pilot_real._endpoint_recipient(pilot, ids["endpoint_id"])
    # Un chat distinto con los mismos últimos cuatro dígitos: no debe confundirse
    # (identidad por hash, no last4) ni reapuntar el endpoint existente.
    otro_chat = "999" + CHAT_ID  # dirección distinta
    monkeypatch.setenv(TELEGRAM_ALLOWED_CHAT_IDS_ENV, otro_chat)

    with pytest.raises(PilotRealError, match="endpoint_config_drift"):
        _setup()

    assert pilot_real._endpoint_recipient(pilot, ids["endpoint_id"]) == recipient_before


# ══ 3 · CONFIG DRIFT ═════════════════════════════════════════════════════════

def test_setup_dos_veces_no_duplica_y_deja_preferencia_apagada(pilot):
    first = _setup()
    second = _setup()
    assert first == second

    conn = sqlite3.connect(pilot)
    conn.row_factory = sqlite3.Row
    try:
        for table in ("principals", "notification_endpoints", "radars",
                      "radar_notification_preferences"):
            assert conn.execute(f"SELECT COUNT(*) n FROM {table}").fetchone()["n"] == 1
        pref = conn.execute(
            "SELECT enabled, frequency, timezone, max_items, quiet_hours_start, "
            "quiet_hours_end FROM radar_notification_preferences"
        ).fetchone()
        # Nace y permanece APAGADA: encender es trabajo de baseline.
        assert pref["enabled"] == 0
        assert pref["frequency"] == "per_run"
        assert pref["timezone"] == "America/Bogota"
        assert pref["max_items"] == pilot_real.REAL_PILOT_MAX_ITEMS
        assert pref["quiet_hours_start"] == "21:00"
        assert pref["quiet_hours_end"] == "07:00"
        endpoint = conn.execute(
            "SELECT verified_at, channel FROM notification_endpoints"
        ).fetchone()
        assert endpoint["verified_at"] is not None
        assert endpoint["channel"] == "telegram"
    finally:
        conn.close()


def test_setup_no_filtra_chat_id_ni_token(pilot):
    output = []
    pilot_real.setup_real_pilot(output=output.append)
    assert CHAT_ID not in output[0]
    assert TOKEN not in output[0]


def test_setup_exige_exactamente_un_chat_id(pilot, monkeypatch):
    monkeypatch.setenv(TELEGRAM_ALLOWED_CHAT_IDS_ENV, f"{CHAT_ID},999")
    with pytest.raises(PilotRealError, match="exactly_one_allowed_chat_id"):
        _setup()


def test_setup_drift_de_radar_falla_cerrado(pilot):
    ids = _setup()
    conn = sqlite3.connect(pilot)
    try:
        conn.execute(
            "UPDATE radars SET query_frame_json = ? WHERE id = ?",
            (json.dumps({"objeto": ["otra_cosa"]}), ids["radar_id"]),
        )
        conn.commit()
    finally:
        conn.close()
    with pytest.raises(PilotRealError, match="radar_config_drift"):
        _setup()


def test_setup_drift_de_preferencia_falla_cerrado(pilot):
    _setup()
    conn = sqlite3.connect(pilot)
    try:
        conn.execute(
            "UPDATE radar_notification_preferences SET max_items = 99"
        )
        conn.commit()
    finally:
        conn.close()
    with pytest.raises(PilotRealError, match="preference_config_drift"):
        _setup()


# ══ 4 · BASELINE SIN POSIBILIDAD DE BATCH ════════════════════════════════════

def test_baseline_habilita_planta_cursor_y_no_produce_batch(pilot):
    _setup()
    deps = _deps(_BASELINE_ROWS)
    summary = _baseline(deps)

    assert summary["baseline_events"] == 1
    assert summary["batches"] == 0
    assert summary["attempts"] == 0
    assert summary["cursor"] == summary["max_event_seq"]

    radar, pref = pilot_real._resolve_pilot_preference(pilot)
    assert pref.is_enabled  # baseline la encendió


def test_baseline_es_recuperable_tras_crash_antes_de_habilitar(pilot):
    _setup()
    deps = _deps(_BASELINE_ROWS)
    radar, pref = pilot_real._resolve_pilot_preference(pilot)
    # Simula crash: run_radar corrió (baseline materializado), no se habilitó.
    run_radar(deps, RadarStore(pilot), radar)
    assert not pilot_real._resolve_pilot_preference(pilot)[1].is_enabled

    summary = _baseline(deps)  # segunda ejecución reconoce y habilita
    assert summary["batches"] == 0 and summary["attempts"] == 0
    assert summary["cursor"] == summary["max_event_seq"]
    assert pilot_real._resolve_pilot_preference(pilot)[1].is_enabled


def test_baseline_falla_si_la_corrida_trae_novedad(pilot):
    _setup()
    deps = _deps(_BASELINE_ROWS)
    radar, _ = pilot_real._resolve_pilot_preference(pilot)
    run_radar(deps, RadarStore(pilot), radar)  # baseline materializado
    deps.secop_client.rows = _BASELINE_ROWS + [_NOVELTY_ROW]  # novedad en recuperación

    with pytest.raises(PilotRealError, match="non_baseline"):
        _baseline(deps)
    assert not pilot_real._resolve_pilot_preference(pilot)[1].is_enabled


def test_baseline_ya_habilitado_falla_cerrado(pilot):
    _setup()
    _baseline(_deps(_BASELINE_ROWS))
    with pytest.raises(PilotRealError, match="already_enabled"):
        _baseline(_deps(_BASELINE_ROWS))


def test_baseline_falla_si_ya_existen_batches(pilot):
    _setup()
    deps = _deps(_BASELINE_ROWS)
    _make_pending(deps)  # deja un batch pending y la preferencia encendida
    radar, pref = pilot_real._resolve_pilot_preference(pilot)
    PreferenceStore(pilot).disable(pref.preference_id)

    with pytest.raises(PilotRealError, match="clean_pre_state"):
        _baseline(deps)


# ══ 5 · AISLAMIENTO POR PAR ══════════════════════════════════════════════════

def test_pending_y_status_se_limitan_al_par(pilot):
    ids = _setup()
    deps = _deps(_BASELINE_ROWS)
    _make_pending(deps)  # el radar del piloto tiene 1 batch pending
    radar, endpoint_id = pilot_real._resolve_enabled_pair(pilot)

    # Radar ajeno en el MISMO endpoint, con su propio batch pending.
    radars = RadarStore(pilot)
    foreign = radars.create_radar(
        name="Radar ajeno", owner=pilot_real.REAL_PILOT_OWNER_KEY,
        query_frame_json="{}",
    )
    OutboxStore(pilot).create_batch(
        principal_id=ids["principal_id"], endpoint_id=endpoint_id, radar_id=foreign,
        payload_json={"version": "p1b-payload-v3", "channel": "telegram"},
        events=[BatchEventRef("foreign-ev-1", 1)],
        created_at="2026-07-15T00:00:00+00:00",
        expires_at="2099-01-01T00:00:00+00:00",
    )

    # El pending del par cuenta solo el batch del radar del piloto, no el ajeno.
    assert len(pilot_real._pending_batches(pilot, endpoint_id, radar.id)) == 1
    status = pilot_real.status_real_pilot(output=lambda _: None)
    assert status["batches_by_status"].get("pending") == 1


# ══ 6 · PREVIEW ANTES DE NUEVA CONSULTA ══════════════════════════════════════

def test_run_previsualiza_un_batch_v3(pilot):
    _setup()
    deps = _deps(_BASELINE_ROWS)
    _make_pending(deps)
    output = []
    result = pilot_real.run_real_pilot(deps=deps, now=NOW, output=output.append)

    assert result["outcome"] == "PREVIEW_READY"
    assert result["payload_version"] == "p1b-payload-v3"
    assert result["byte_count"] < 3800
    preview = output[0]
    assert "Objeto:" in preview and "…" in preview
    assert "$186,600,000" in preview
    assert "Referencia:" not in preview
    assert preview.count("MR- SASIP-002-2026") == 1
    assert CHAT_ID not in preview and TOKEN not in preview


def test_run_con_un_pending_no_consulta_secop(pilot):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))
    # Con exactamente un pending, run previsualiza sin tocar SECOP (deps bomba).
    result = pilot_real.run_real_pilot(deps=_bomb_deps(), output=lambda _: None)
    assert result["outcome"] == "PREVIEW_READY"


def test_run_con_mas_de_un_pending_falla_sin_consultar_secop(pilot):
    _setup()
    deps = _deps(_BASELINE_ROWS)
    _make_pending(deps)
    radar, endpoint_id = pilot_real._resolve_enabled_pair(pilot)
    # Segundo batch pending del MISMO par, creado directo (sin el guard de run).
    from app.core.notify.selector import NotificationSelector
    deps.secop_client.rows = _BASELINE_ROWS + [
        _NOVELTY_ROW,
        _proc("CO1.REQ.999", "REF-999", "Otro adulto mayor",
              "APOYO ADULTO MAYOR OTRO " * 6),
    ]
    run_radar(deps, RadarStore(pilot), radar)
    NotificationSelector(pilot).select_pair(endpoint_id, radar.id, now=NOW)

    with pytest.raises(PilotRealError, match="more_than_one_batch_ready"):
        pilot_real.run_real_pilot(deps=_bomb_deps(), output=lambda _: None)


def test_segunda_corrida_identica_no_produce_novedad(pilot):
    _setup()
    deps = _deps(_BASELINE_ROWS)
    _baseline(deps)
    result = pilot_real.run_real_pilot(deps=deps, now=NOW, output=lambda _: None)
    assert result["outcome"] == "NO_NOVELTY"


# ══ 7/8 · ENVÍO DIRIGIDO + PREFLIGHT DE SEND ═════════════════════════════════

def test_send_despacha_exactamente_una_vez_y_no_reenvia(pilot):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))
    provider = _sent_provider("9001")
    output = []

    report = pilot_real.send_ready_pilot(
        confirm_send=True, provider=provider, clock=lambda: NOW, output=output.append
    )
    assert len(provider.calls) == 1
    assert report["status"] == "sent"
    assert report["provider_message_id"] == "9001"
    assert report["attempt_count"] == 1
    assert CHAT_ID not in output[0] and TOKEN not in output[0]

    provider_again = _sent_provider("NO")
    with pytest.raises(PilotRealError, match="exactly_one_ready_batch"):
        pilot_real.send_ready_pilot(
            confirm_send=True, provider=provider_again, clock=lambda: NOW,
            output=lambda _: None,
        )
    assert provider_again.calls == []


def test_send_sin_confirm_no_construye_worker(pilot):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))
    with pytest.raises(PilotRealError, match="confirm_send_required"):
        pilot_real.send_ready_pilot(
            confirm_send=False, provider=object(), worker_factory=_bomb_factory,
            output=lambda _: None,
        )


def test_send_allowlist_vacia_rechaza_antes_de_worker(pilot, monkeypatch):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))
    monkeypatch.setenv(TELEGRAM_ALLOWED_CHAT_IDS_ENV, "")
    with pytest.raises(PilotRealError, match="allowlist_required"):
        pilot_real.send_ready_pilot(
            confirm_send=True, provider=object(), worker_factory=_bomb_factory,
            output=lambda _: None,
        )


def test_send_allowlist_no_incluye_destinatario_rechaza_antes_de_worker(pilot, monkeypatch):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))
    monkeypatch.setenv(TELEGRAM_ALLOWED_CHAT_IDS_ENV, "999888777")  # no es el recipient
    with pytest.raises(PilotRealError, match="allowlist_recipient_mismatch"):
        pilot_real.send_ready_pilot(
            confirm_send=True, provider=object(), worker_factory=_bomb_factory,
            output=lambda _: None,
        )


def test_send_provider_retryable_deja_pending_sin_message_id(pilot):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))
    provider = FakeProvider(
        ProviderResult(
            effect=NO_EXTERNAL_EFFECT, retryable=True,
            error_class="rate_limited", error_message_safe="telegram_rate_limited",
        )
    )
    report = pilot_real.send_ready_pilot(
        confirm_send=True, provider=provider, clock=lambda: NOW, output=lambda _: None
    )
    assert len(provider.calls) == 1
    assert report["status"] == "pending"
    assert report["provider_message_id"] is None


def _corrupt_pending(pilot, mutate):
    """Aplica ``mutate(payload_dict)->payload_json_str`` al batch pending."""
    conn = sqlite3.connect(pilot)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT notification_id, payload_json FROM radar_notification_batches "
            "WHERE status = 'pending'"
        ).fetchone()
        nid = row["notification_id"]
        new_payload = mutate(json.loads(row["payload_json"]))
        conn.execute(
            "UPDATE radar_notification_batches SET payload_json = ? "
            "WHERE notification_id = ?",
            (new_payload, nid),
        )
        conn.commit()
    finally:
        conn.close()
    return nid


def test_send_payload_corrupto_cero_provider_calls_y_batch_sin_mutar(pilot):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))
    _corrupt_pending(pilot, lambda _p: '{"roto": ')  # JSON ilegible

    with pytest.raises(PilotRealError, match="payload_unreadable"):
        pilot_real.send_ready_pilot(
            confirm_send=True, provider=object(), worker_factory=_bomb_factory,
            output=lambda _: None,
        )
    # Batch sin mutar (sigue pending) y sin attempts nuevos.
    radar, endpoint_id = pilot_real._resolve_enabled_pair(pilot)
    assert len(pilot_real._pending_batches(pilot, endpoint_id, radar.id)) == 1
    assert pilot_real._count_attempts(pilot, endpoint_id, radar.id) == 0


def test_send_payload_version_desconocida_rechazada(pilot):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))

    def to_v1(payload):
        payload["version"] = "p1b-payload-v1"
        return json.dumps(payload, ensure_ascii=False)

    _corrupt_pending(pilot, to_v1)
    with pytest.raises(PilotRealError, match="unexpected_version"):
        pilot_real.send_ready_pilot(
            confirm_send=True, provider=object(), worker_factory=_bomb_factory,
            output=lambda _: None,
        )


def test_send_payload_mal_asociado_rechazado(pilot):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))

    def wrong_pair(payload):
        payload["endpoint_id"] = "otro-endpoint"
        return json.dumps(payload, ensure_ascii=False)

    _corrupt_pending(pilot, wrong_pair)
    with pytest.raises(PilotRealError, match="pair_mismatch"):
        pilot_real.send_ready_pilot(
            confirm_send=True, provider=object(), worker_factory=_bomb_factory,
            output=lambda _: None,
        )


# ══ 9 · STATUS (solo lectura, exacto al par) ═════════════════════════════════

def test_status_es_solo_lectura_y_no_muta(pilot):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))
    before = pilot.read_bytes()
    before_stat = pilot.stat()
    output = []
    status = pilot_real.status_real_pilot(output=output.append)
    after_stat = pilot.stat()

    assert pilot.read_bytes() == before
    assert after_stat.st_mtime == before_stat.st_mtime
    assert status["radar_name"] == pilot_real.REAL_PILOT_RADAR_NAME
    assert status["batches_by_status"].get("pending") == 1
    assert status["dispatching_residual"] == 0
    assert CHAT_ID not in output[0] and TOKEN not in output[0]


def test_status_reporta_provider_message_id_tras_envio(pilot):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))
    pilot_real.send_ready_pilot(
        confirm_send=True, provider=_sent_provider("42"), clock=lambda: NOW,
        output=lambda _: None,
    )
    status = pilot_real.status_real_pilot(output=lambda _: None)
    assert status["batches_by_status"].get("sent") == 1
    assert status["last_provider_message_id"] == "42"
    assert status["dispatching_residual"] == 0


def test_status_exige_exactamente_una_preferencia(pilot):
    ids = _setup()
    # Segunda preferencia del piloto (segundo endpoint) → ambigüedad explícita.
    from app.core.notify.endpoint_store import EndpointStore
    from app.core.notify.masking import Masker
    endpoints = EndpointStore(pilot, masker=Masker.from_key(MASKING_KEY))
    second_ep = endpoints.create_endpoint(
        principal_id=ids["principal_id"], channel="telegram", address="otro-chat-xyz"
    )
    endpoints.mark_verified(second_ep, external_recipient_id="otro-chat-xyz")
    PreferenceStore(pilot).create_preference(radar_id=ids["radar_id"], endpoint_id=second_ep)

    with pytest.raises(PilotRealError, match="exactly_one_pilot_preference"):
        pilot_real.status_real_pilot(output=lambda _: None)


# ══ FASE 3 · config congelada del radar Adulto Mayor ═════════════════════════

def test_query_frame_es_busqueda_clasica_adulto_mayor(pilot):
    frame = json.loads(pilot_real.adulto_mayor_query_frame_json())
    assert frame["dataset"] == "procesos"
    assert frame["topic"] == ["adulto_mayor"]
    assert frame["raw_params"] == {"objeto": ["adulto_mayor"]}
    assert not frame.get("scope")

    _setup()
    radar, _ = pilot_real._resolve_pilot_preference(pilot)
    assert json.loads(radar.query_frame_json) == frame


# ══ REV · Validación del resultado de run_radar ══════════════════════════════

def test_baseline_con_fallo_de_secop_no_habilita(pilot):
    _setup()
    deps = _deps(_BASELINE_ROWS, query_error=RuntimeError("secop caído"))
    radar, pref = pilot_real._resolve_pilot_preference(pilot)

    with pytest.raises(PilotRealError, match="radar_run_not_successful"):
        _baseline(deps)

    # Preferencia apagada, sin cursor, sin batch.
    assert not pilot_real._resolve_pilot_preference(pilot)[1].is_enabled
    assert pilot_real._cursor_seq(pilot, pref.endpoint_id, radar.id) is None
    assert pilot_real._count_all_batches(pilot, pref.endpoint_id, radar.id) == 0


def test_baseline_con_already_running_no_habilita(pilot):
    _setup()
    deps = _deps(_BASELINE_ROWS)
    radar, pref = pilot_real._resolve_pilot_preference(pilot)
    # Otro ejecutor tiene el lease: run_radar devolverá already_running (skipped).
    claim = RadarStore(pilot).try_acquire_run_lease(radar.id)
    assert claim.acquired

    with pytest.raises(PilotRealError, match="radar_run_not_successful"):
        _baseline(deps)

    assert not pilot_real._resolve_pilot_preference(pilot)[1].is_enabled
    assert pilot_real._cursor_seq(pilot, pref.endpoint_id, radar.id) is None
    assert pilot_real._count_all_batches(pilot, pref.endpoint_id, radar.id) == 0


def test_run_con_fallo_de_secop_no_devuelve_no_novelty(pilot):
    _setup()
    _baseline(_deps(_BASELINE_ROWS))  # baseline exitoso
    radar, endpoint_id = pilot_real._resolve_enabled_pair(pilot)
    failing = _deps(_BASELINE_ROWS, query_error=RuntimeError("secop caído"))

    with pytest.raises(PilotRealError, match="radar_run_not_successful"):
        pilot_real.run_real_pilot(deps=failing, now=NOW, output=lambda _: None)

    assert pilot_real._count_all_batches(pilot, endpoint_id, radar.id) == 0


# ══ REV · Clave HMAC desde el entorno ════════════════════════════════════════

def test_setup_sin_clave_masker_no_crea_la_base(pilot, monkeypatch):
    monkeypatch.delenv(pilot_real.NOTIFICATION_MASKING_KEY_ENV, raising=False)
    with pytest.raises(PilotRealError, match="masking_key_required"):
        _setup()
    assert not pilot.exists()


def test_setup_clave_distinta_sobre_base_existente_es_drift_sin_nuevo_endpoint(pilot, monkeypatch):
    _setup()
    monkeypatch.setenv(pilot_real.NOTIFICATION_MASKING_KEY_ENV, "otra-clave-distinta")
    with pytest.raises(PilotRealError, match="endpoint_config_drift"):
        _setup()

    conn = sqlite3.connect(pilot)
    conn.row_factory = sqlite3.Row
    try:
        assert conn.execute(
            "SELECT COUNT(*) n FROM notification_endpoints"
        ).fetchone()["n"] == 1
    finally:
        conn.close()


# ══ REV · Allowlist exacta ═══════════════════════════════════════════════════

def test_send_allowlist_con_chat_adicional_rechaza_antes_de_worker(pilot, monkeypatch):
    _setup()
    _make_pending(_deps(_BASELINE_ROWS))
    monkeypatch.setenv(TELEGRAM_ALLOWED_CHAT_IDS_ENV, f"{CHAT_ID},999888777")
    with pytest.raises(PilotRealError, match="allowlist_recipient_mismatch"):
        pilot_real.send_ready_pilot(
            confirm_send=True, provider=object(), worker_factory=_bomb_factory,
            output=lambda _: None,
        )


# ══ REV · Drift completo de endpoint / preferencia / radar ═══════════════════

def test_setup_endpoint_pausado_es_drift(pilot):
    ids = _setup()
    EndpointStore(pilot, masker=Masker.from_key(MASKING_KEY)).set_enabled(
        ids["endpoint_id"], False
    )
    with pytest.raises(PilotRealError, match="endpoint_not_eligible"):
        _setup()


def test_setup_preferencia_ajena_del_radar_es_ambiguo(pilot):
    ids = _setup()
    endpoints = EndpointStore(pilot, masker=Masker.from_key(MASKING_KEY))
    ep2 = endpoints.create_endpoint(
        principal_id=ids["principal_id"], channel="telegram", address="otro-chat-xyz"
    )
    endpoints.mark_verified(ep2, external_recipient_id="otro-chat-xyz")
    PreferenceStore(pilot).create_preference(radar_id=ids["radar_id"], endpoint_id=ep2)

    with pytest.raises(PilotRealError, match="exactly_one_pilot_preference"):
        _setup()


def test_setup_radar_frame_ilegible_es_drift(pilot):
    ids = _setup()
    conn = sqlite3.connect(pilot)
    try:
        conn.execute(
            "UPDATE radars SET query_frame_json = ? WHERE id = ?",
            ("{roto-no-es-json", ids["radar_id"]),
        )
        conn.commit()
    finally:
        conn.close()
    with pytest.raises(PilotRealError, match="radar_config_drift"):
        _setup()


# ══ REV2 · Historial completo del baseline (no solo la corrida actual) ═══════

def test_baseline_no_traga_novedad_en_recuperacion_de_dos_pasos(pilot):
    """Reproduce el hueco reportado: 1ª corrida baseline; crash; 2ª intento trae
    una novedad y falla correctamente (non_baseline); 3er intento con los MISMOS
    datos ya no emite el evento (quedó "visto"), pero el evento no-baseline sigue
    en el historial — debe seguir fallando, no encender ni mover el cursor."""
    _setup()
    deps = _deps(_BASELINE_ROWS)
    radar, pref = pilot_real._resolve_pilot_preference(pilot)

    # 1) Primera corrida baseline (crash simulado: no se llega a habilitar).
    run_radar(deps, RadarStore(pilot), radar)
    assert not pilot_real._resolve_pilot_preference(pilot)[1].is_enabled

    # 2) Reintento con una novedad real: falla correctamente (non_baseline).
    deps.secop_client.rows = _BASELINE_ROWS + [_NOVELTY_ROW]
    with pytest.raises(PilotRealError, match="non_baseline"):
        _baseline(deps)
    assert not pilot_real._resolve_pilot_preference(pilot)[1].is_enabled

    # 3) Mismos datos (la novedad ya quedó "vista"): la corrida de esta vez no
    # emite eventos propios, pero el historial YA tiene un evento "new" — debe
    # seguir fallando, no tragarse la novedad encendiendo la preferencia.
    with pytest.raises(PilotRealError, match="non_baseline"):
        _baseline(deps)

    pref_final = pilot_real._resolve_pilot_preference(pilot)[1]
    assert not pref_final.is_enabled
    assert pilot_real._cursor_seq(pilot, pref.endpoint_id, radar.id) is None
    history = pilot_real._baseline_history(pilot, radar.id)
    assert history.baseline_count >= 1 and history.non_baseline_count >= 1


# ══ REV2 · cardinalidad total (no solo "una habilitada") ═════════════════════

def test_run_falla_si_existe_otra_preferencia_deshabilitada_del_mismo_radar(pilot):
    """Dos preferencias para el radar del piloto — una encendida, otra apagada —
    es un estado que el piloto nunca debería alcanzar. run/send deben tratarlo
    como ambigüedad, no aceptar la habilitada como si fuera la única."""
    ids = _setup()
    _baseline(_deps(_BASELINE_ROWS))  # enciende la preferencia original

    # Preferencia adicional deshabilitada del MISMO radar hacia OTRO endpoint.
    endpoints = EndpointStore(pilot, masker=Masker.from_key(MASKING_KEY))
    ep2 = endpoints.create_endpoint(
        principal_id=ids["principal_id"], channel="telegram", address="otro-chat-xyz"
    )
    endpoints.mark_verified(ep2, external_recipient_id="otro-chat-xyz")
    PreferenceStore(pilot).create_preference(radar_id=ids["radar_id"], endpoint_id=ep2)
    # queda apagada (create_preference nace apagada) — nunca se habilita.

    with pytest.raises(PilotRealError, match="exactly_one_pilot_preference"):
        pilot_real.run_real_pilot(deps=_bomb_deps(), output=lambda _: None)

    with pytest.raises(PilotRealError, match="exactly_one_pilot_preference"):
        pilot_real.send_ready_pilot(
            confirm_send=True, provider=object(), worker_factory=_bomb_factory,
            output=lambda _: None,
        )


# ══ REV3 · Atomicidad real entre comprobación baseline-only y encendido ══════

def test_A_historial_contaminado_no_llama_secop_ni_habilita(pilot):
    """Escenario A: el historial ya contiene un evento no-baseline antes de
    correr el comando. Debe fallar cerrado ANTES de consultar SECOP."""
    _setup()
    deps = _deps(_BASELINE_ROWS)
    radar, pref = pilot_real._resolve_pilot_preference(pilot)

    # Contamina el historial directamente (equivalente a un intento previo que
    # detectó una novedad y falló, dejándola persistida).
    RadarStore(pilot).record_events([{
        "radar_id": radar.id, "owner": pilot_real.REAL_PILOT_OWNER_KEY,
        "radar_name": pilot_real.REAL_PILOT_RADAR_NAME, "run_id": "prior-run",
        "ts": "2026-07-15T10:00:00+00:00", "kind": "new",
        "result_key": "K-PRIOR", "dataset": "procesos", "title": "Previo",
        "entity": "X", "value": "1", "status": "abierto",
        "prev_status": None, "prev_value": None,
        "urlproceso": "https://secop/prior", "referencia": "REF-PRIOR",
    }])

    bomb = _bomb_deps()  # cualquier .query()/.count() debe abortar el test
    with pytest.raises(PilotRealError, match="non_baseline"):
        _baseline(bomb)

    assert not pilot_real._resolve_pilot_preference(pilot)[1].is_enabled
    assert pilot_real._cursor_seq(pilot, pref.endpoint_id, radar.id) is None


def test_B_evento_concurrente_entre_snapshot_y_enable_falla_cerrado(pilot, monkeypatch):
    """Escenario B: un evento se materializa después de que baseline_real_pilot
    toma su snapshot del historial post-corrida, pero antes de que
    enable_without_backfill tome su lock. El compare-and-enable
    (expected_max_event_seq) debe detectarlo y revertir todo."""
    _setup()
    deps = _deps(_BASELINE_ROWS)
    radar, pref = pilot_real._resolve_pilot_preference(pilot)

    real_preference_store = pilot_real.PreferenceStore

    class RacingPreferenceStore(real_preference_store):
        def enable_without_backfill(self, preference_id, **kwargs):
            # El "Proceso B": inserta un evento nuevo justo antes de que el
            # compare-and-enable tome su lock — la ventana exacta que
            # expected_max_event_seq debe cerrar.
            RadarStore(pilot).record_events([{
                "radar_id": radar.id, "owner": pilot_real.REAL_PILOT_OWNER_KEY,
                "radar_name": pilot_real.REAL_PILOT_RADAR_NAME,
                "run_id": "concurrent-run", "ts": "2026-07-15T10:05:00+00:00",
                "kind": "new", "result_key": "K-CONCURRENT", "dataset": "procesos",
                "title": "Concurrente", "entity": "X", "value": "1",
                "status": "abierto", "prev_status": None, "prev_value": None,
                "urlproceso": "https://secop/concurrent",
                "referencia": "REF-CONCURRENT",
            }])
            return super().enable_without_backfill(preference_id, **kwargs)

    monkeypatch.setattr(pilot_real, "PreferenceStore", RacingPreferenceStore)

    with pytest.raises(PilotRealError, match="baseline_changed_before_enable"):
        _baseline(deps)

    pref_after = pilot_real._resolve_pilot_preference(pilot)[1]
    assert not pref_after.is_enabled
    # El cursor no avanzó: ni sobre el baseline ni, mucho menos, por encima del
    # evento concurrente.
    assert pilot_real._cursor_seq(pilot, pref.endpoint_id, radar.id) is None
    # El evento concurrente sigue vivo y sin consumir: la siguiente corrida
    # limpia (constatando el historial ya contaminado) lo verá y fallará cerrado
    # también, en vez de perderlo.
    history = pilot_real._baseline_history(pilot, radar.id)
    assert history.non_baseline_count >= 1


def test_C_recuperacion_baseline_only_sigue_habilitando(pilot):
    """Escenario C: crash tras run_radar (baseline materializado) y antes de
    habilitar. Una segunda invocación limpia reconoce el baseline y enciende."""
    _setup()
    deps = _deps(_BASELINE_ROWS)
    radar, pref = pilot_real._resolve_pilot_preference(pilot)
    run_radar(deps, RadarStore(pilot), radar)  # simula el crash: nunca se habilita
    assert not pilot_real._resolve_pilot_preference(pilot)[1].is_enabled

    summary = _baseline(deps)

    assert summary["batches"] == 0 and summary["attempts"] == 0
    assert summary["cursor"] == summary["max_event_seq"]
    assert pilot_real._resolve_pilot_preference(pilot)[1].is_enabled


def test_D_regresion_recuperacion_dos_pasos_sigue_verde(pilot):
    """La regresión de la ronda anterior (novedad tragada en dos intentos) debe
    seguir bloqueada con el fix de atomicidad encima."""
    _setup()
    deps = _deps(_BASELINE_ROWS)
    radar, pref = pilot_real._resolve_pilot_preference(pilot)

    run_radar(deps, RadarStore(pilot), radar)
    assert not pilot_real._resolve_pilot_preference(pilot)[1].is_enabled

    deps.secop_client.rows = _BASELINE_ROWS + [_NOVELTY_ROW]
    with pytest.raises(PilotRealError, match="non_baseline"):
        _baseline(deps)
    assert not pilot_real._resolve_pilot_preference(pilot)[1].is_enabled

    with pytest.raises(PilotRealError, match="non_baseline"):
        _baseline(deps)  # mismos datos: la novedad ya "vista", pero persistida

    pref_final = pilot_real._resolve_pilot_preference(pilot)[1]
    assert not pref_final.is_enabled
    assert pilot_real._cursor_seq(pilot, pref.endpoint_id, radar.id) is None
