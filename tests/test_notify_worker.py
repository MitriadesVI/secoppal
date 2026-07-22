"""Tests de app/core/notify/worker.py — el ciclo completo de entrega (P1B-4, R2C).

Convenciones heredadas de ``test_notify_worker_store.py``: reloj inyectado, cero
sleeps coordinadores, ``threading.Barrier``/``Event``, joins con timeout, 50
iteraciones en las carreras. Ningún test toca la red: el proveedor es el fake, y la
propiedad que casi todos comprueban al final es la misma —
``provider.external_effect_count <= 1``.
"""
from __future__ import annotations

import itertools
import json
import threading
from datetime import datetime, timedelta, timezone

import pytest

from app.core.db import connect_sqlite
from app.core.notify import claim
from app.core.notify.endpoint_store import EndpointStore
from app.core.notify.fake_provider import (
    MODE_ACCEPT,
    MODE_PERMANENT_BEFORE_EFFECT,
    MODE_RAISE_UNCLASSIFIED,
    MODE_RETRY_BEFORE_EFFECT,
    MODE_TIMEOUT_AFTER_EFFECT,
    FakeProvider,
)
from app.core.notify.identity_store import IdentityStore
from app.core.notify.ledger_store import LedgerStore
from app.core.notify.masking import Masker
from app.core.notify.outbox_store import BatchEventRef, OutboxStore
from app.core.notify.preference_store import PreferenceStore
from app.core.notify.provider import (
    NO_EXTERNAL_EFFECT,
    POSSIBLE_EXTERNAL_EFFECT,
    ProviderResult,
)
from app.core.notify.retry import RetryPolicy
from app.core.notify.worker import (
    PHASE_AFTER_PROVIDER_SEND,
    PHASE_BEFORE_PROVIDER_SEND,
    NotificationWorker,
    WorkerRunReport,
)
from app.core.notify.worker_store import (
    WorkerStore,
    WorkerStoreError,
    external_idempotency_key,
)
from app.core.radar.store import RadarStore

NOW = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
LATER = NOW + timedelta(hours=1)
FOREVER = "2099-01-01T00:00:00+00:00"
JOIN_TIMEOUT = 30
RACE_ITERATIONS = 50

_seq = itertools.count(1)


class _Clock:
    """Devuelve los instantes en orden y repite el último."""

    def __init__(self, *moments: datetime) -> None:
        self.moments = list(moments) or [NOW]
        self.calls: list[datetime] = []

    def __call__(self) -> datetime:
        moment = self.moments[min(len(self.calls), len(self.moments) - 1)]
        self.calls.append(moment)
        return moment


def _payload(*, channel="telegram", version="p1b-payload-v2", items=None, **extra):
    payload = {
        "version": version,
        "principal_id": "p",
        "endpoint_id": "e",
        "radar_id": "r",
        "radar_name": "Radar PAE",
        "channel": channel,
        "timezone": "America/Bogota",
        "created_at": "2026-07-15T00:00:00+00:00",
        "max_items": 10,
        "items": items if items is not None else [
            {"kind": "new", "title": "Suministro PAE Bogotá", "dataset": "procesos",
             "entity": "SED", "urlproceso": "https://example.org/p1"},
        ],
    }
    payload["total_items"] = len(payload["items"])
    payload["links_missing"] = 0
    payload.update(extra)
    return payload


@pytest.fixture
def env(tmp_path):
    db = tmp_path / "worker.db"
    radars = RadarStore(db)
    identity = IdentityStore(db)
    endpoints = EndpointStore(db, masker=Masker.from_key("worker-test-key"))
    preferences = PreferenceStore(db)
    owner = "email:ana@example.com"
    principal = identity.create_principal("ana@example.com")
    identity.link_owner_key(owner, principal)
    endpoint = endpoints.create_endpoint(
        principal_id=principal, channel="telegram", address="ana-chat"
    )
    endpoints.mark_verified(endpoint, external_recipient_id="chat-ana-12345")
    radar = radars.create_radar(name="Radar PAE", owner=owner, query_frame_json="{}")
    preference = preferences.create_preference(radar_id=radar, endpoint_id=endpoint)
    preferences.set_enabled(preference, True)
    return {
        "db": db, "radars": radars, "identity": identity, "endpoints": endpoints,
        "preferences": preferences, "principal": principal, "endpoint": endpoint,
        "radar": radar, "preference": preference, "owner": owner,
        "store": WorkerStore(db), "outbox": OutboxStore(db), "ledger": LedgerStore(db),
    }


def _batch(env, *, payload=None, created_at="2026-07-15T00:00:00+00:00",
           expires_at=FOREVER, not_before=None, endpoint=None):
    number = next(_seq)
    result = env["outbox"].create_batch(
        principal_id=env["principal"], endpoint_id=endpoint or env["endpoint"],
        radar_id=env["radar"], payload_json=payload if payload is not None else _payload(),
        events=[BatchEventRef(f"ev-{number}", number)],
        created_at=created_at, expires_at=expires_at, not_before=not_before,
    )
    return result.notification_id


def _worker(env, provider=None, *, clock=None, rng=lambda: 0.0, policy=None,
            providers=None):
    if providers is None:
        providers = {"telegram": provider} if provider is not None else {}
    return NotificationWorker(
        env["db"], providers, clock=clock or _Clock(NOW), rng=rng,
        policy=policy or RetryPolicy(),
    )


def _sql(env, statement, params=()):
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(statement, params)
    finally:
        conn.close()


def _row(env, nid):
    conn = connect_sqlite(env["db"])
    try:
        return conn.execute(
            "SELECT * FROM radar_notification_batches WHERE notification_id=?", (nid,)
        ).fetchone()
    finally:
        conn.close()


def _attempts(env, nid):
    conn = connect_sqlite(env["db"])
    try:
        return conn.execute(
            "SELECT * FROM radar_delivery_attempts WHERE notification_id=? "
            "ORDER BY attempt_number", (nid,)
        ).fetchall()
    finally:
        conn.close()


def _open_orphan(env, nid, *, now=NOW, duration=timedelta(seconds=120)):
    """Deja un intento 'dispatching' abierto y el claim ya vencido: un huérfano."""
    conn = connect_sqlite(env["db"])
    try:
        token = claim.try_claim(conn, nid, now, duration)
    finally:
        conn.close()
    assert token is not None
    attempt_id = env["ledger"].begin_attempt(nid, token, now=now)
    return attempt_id


# ══ A · Camino feliz ═════════════════════════════════════════════════════════════

def test_accept_produce_sent(env):
    """1 · Accept → sent. Un efecto externo, un intento cerrado, batch terminal."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider).run_once()

    assert _row(env, nid)["status"] == "sent"
    assert provider.external_effect_count == 1
    assert provider.call_count == 1
    attempts = _attempts(env, nid)
    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "sent"
    assert attempts[0]["phase"] == "finished"
    assert attempts[0]["provider_message_id"]
    assert (report.sent, report.authorized, report.provider_calls) == (1, 1, 1)


def test_el_request_lleva_la_clave_write_once_y_el_texto_renderizado(env):
    """5 · La clave viene de la autorización, no se recalcula por intento."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    _worker(env, provider).run_once()

    call = provider.calls[0]
    assert call.external_idempotency_key == external_idempotency_key(nid)
    assert call.payload_version == "p1b-payload-v2"
    assert call.channel == "telegram"
    assert _row(env, nid)["external_idempotency_key"] == call.external_idempotency_key


# ══ B · Reintentos ═══════════════════════════════════════════════════════════════

def test_retry_sin_efecto_deja_el_batch_pending(env):
    """2 · Retry sin efecto → pending con cuarentena, cero efectos externos."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_RETRY_BEFORE_EFFECT)
    report = _worker(env, provider).run_once()

    row = _row(env, nid)
    assert row["status"] == "pending"
    assert provider.external_effect_count == 0
    assert _attempts(env, nid)[0]["outcome"] == "retry"
    assert report.retried == 1
    # Backoff del primer intento: 30 s con jitter 0.
    assert row["not_before"] == (NOW + timedelta(seconds=30)).isoformat()


def test_segundo_intento_conserva_la_idempotency_key(env):
    """2 · **Mata M3.** La clave es del mensaje lógico, no del intento: si se derivara
    del attempt_id, un reintento llevaría una clave nueva y el proveedor no podría
    deduplicar — que es exactamente lo contrario de para lo que existe."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_RETRY_BEFORE_EFFECT)
    _worker(env, provider, clock=_Clock(NOW)).run_once()
    _worker(env, provider, clock=_Clock(LATER)).run_once()

    assert len(_attempts(env, nid)) == 2
    assert provider.seen_keys[0] == provider.seen_keys[1]
    assert provider.external_effect_count == 0


def test_retry_y_luego_accept_produce_un_solo_efecto(env):
    """3 · Dos intentos, un efecto externo."""
    nid = _batch(env)
    provider = FakeProvider([MODE_RETRY_BEFORE_EFFECT, MODE_ACCEPT])
    _worker(env, provider, clock=_Clock(NOW)).run_once()
    _worker(env, provider, clock=_Clock(LATER)).run_once()

    assert _row(env, nid)["status"] == "sent"
    assert provider.external_effect_count == 1
    outcomes = [a["outcome"] for a in _attempts(env, nid)]
    assert outcomes == ["retry", "sent"]


def test_retry_agotado_muere_como_retries_exhausted(env):
    """25 · **Mata la regla del vocabulario.** Lo que se agotó es la paciencia, no el
    TTL: registrarlo como 'expired' sería mentir sobre por qué no se entregó."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_RETRY_BEFORE_EFFECT)
    policy = RetryPolicy(max_attempts=2)
    _worker(env, provider, clock=_Clock(NOW), policy=policy).run_once()
    report = _worker(env, provider, clock=_Clock(LATER), policy=policy).run_once()

    row = _row(env, nid)
    assert row["status"] == "dead"
    assert row["dead_reason"] == "retries_exhausted"
    assert provider.external_effect_count == 0
    assert report.dead == 1
    último = _attempts(env, nid)[-1]
    assert último["error_class"] == "network_error", "conserva la causa del último intento"


def test_el_worker_respeta_una_policy_inyectada(env):
    """**Mata M7.** Si el worker duplicara el algoritmo de backoff en vez de delegar en
    ``retry.py``, ignoraría esta política y el not_before sería otro."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_RETRY_BEFORE_EFFECT)
    policy = RetryPolicy(base=timedelta(seconds=7), max_attempts=9)
    _worker(env, provider, policy=policy).run_once()

    assert _row(env, nid)["not_before"] == (NOW + timedelta(seconds=7)).isoformat()


# ══ C · retry_after ══════════════════════════════════════════════════════════════

class _RetryAfterProvider:
    """Proveedor mínimo que dicta un ``retry_after``. No usa el fake porque lo que se
    prueba es la aritmética del worker, no los modos."""

    def __init__(self, retry_after):
        self.retry_after = retry_after
        self.calls = 0

    def send(self, request):
        self.calls += 1
        return ProviderResult(
            effect=NO_EXTERNAL_EFFECT, retryable=True,
            error_class="rate_limited", error_message_safe="rate_limited",
            retry_after=self.retry_after,
        )


def test_retry_after_posterior_al_backoff_gana(env):
    """23 · **Mata M6.** Un 429 que dice 'no antes de las 13:00' manda sobre nuestros
    30 segundos: ignorarlo nos gana un bloqueo."""
    nid = _batch(env)
    when = NOW + timedelta(hours=1)
    _worker(env, _RetryAfterProvider(when)).run_once()

    assert _row(env, nid)["not_before"] == when.isoformat()


def test_backoff_interno_posterior_al_retry_after_gana(env):
    """24 · Y al revés: un ``retry_after`` de un segundo no nos hace martillear al
    proveedor. Manda el que espera más."""
    nid = _batch(env)
    _worker(env, _RetryAfterProvider(NOW + timedelta(seconds=1))).run_once()

    assert _row(env, nid)["not_before"] == (NOW + timedelta(seconds=30)).isoformat()


def test_retry_after_se_persiste_en_segundos(env):
    nid = _batch(env)
    _worker(env, _RetryAfterProvider(NOW + timedelta(seconds=90))).run_once()

    assert _attempts(env, nid)[0]["retry_after"] == 90


# ══ D · Fallos permanentes y ambiguos ════════════════════════════════════════════

def test_permanent_sin_efecto_muere(env):
    """4 · Permanent sin efecto → dead. Ningún reintento va a arreglar una dirección
    que el proveedor rechaza."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_PERMANENT_BEFORE_EFFECT)
    report = _worker(env, provider).run_once()

    row = _row(env, nid)
    assert row["status"] == "dead"
    assert row["dead_reason"] == "permanent_provider_error"
    assert _attempts(env, nid)[0]["error_class"] == "invalid_address", (
        "la causa concreta se conserva en error_class"
    )
    assert provider.external_effect_count == 0
    assert report.dead == 1


def test_timeout_despues_del_envio_deja_ambiguous_sin_retry(env):
    """5 · **Mata M1.** El mensaje pudo llegar. Reintentar sería apostar con el
    teléfono de alguien."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_TIMEOUT_AFTER_EFFECT)
    report = _worker(env, provider).run_once()

    row = _row(env, nid)
    assert row["status"] == "ambiguous"
    assert row["not_before"] is None or row["status"] != "pending"
    assert _attempts(env, nid)[0]["outcome"] == "ambiguous"
    assert report.ambiguous == 1 and report.retried == 0


def test_ambiguous_no_se_vuelve_a_reclamar(env):
    """Corolario del anterior: 'ambiguous' es terminal. Una segunda corrida no puede
    resucitarlo, o el 'sin retry automático' sería solo una intención."""
    _batch(env)
    provider = FakeProvider(default_mode=MODE_TIMEOUT_AFTER_EFFECT)
    _worker(env, provider, clock=_Clock(NOW)).run_once()
    report = _worker(env, provider, clock=_Clock(LATER)).run_once()

    assert provider.call_count == 1
    assert report.claimed == 0


def test_excepcion_inesperada_deja_ambiguous(env):
    """6 · **Mata M2.** La excepción no dice si el mensaje salió antes o después de
    reventar. Se asume lo peor."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_RAISE_UNCLASSIFIED)
    report = _worker(env, provider).run_once()

    assert _row(env, nid)["status"] == "ambiguous"
    assert report.ambiguous == 1 and report.retried == 0


class _DefectiveProvider:
    """Un proveedor mal escrito que intenta afirmar una clase de error que ni
    ``provider.py`` ni la base admiten. La construcción de ``ProviderResult`` revienta
    **dentro** de ``send()``, así que lo que le llega al worker no es un resultado con
    un ``error_class`` inválido — es una excepción sin clasificar, indistinguible de
    cualquier otro fallo del implementador."""

    def __init__(self):
        self.calls = 0

    def send(self, request):
        self.calls += 1
        return ProviderResult(
            effect=NO_EXTERNAL_EFFECT, retryable=True, error_class="dns_error",
        )


def test_provider_result_con_clase_desconocida_no_llega_vivo_al_worker(env):
    """**ADR-053-corrección, caso c.** Sin la validación en ``provider.py``, este
    escenario terminaría en ``finish_attempt`` reventando por el CHECK de la base
    —después de haber llamado al proveedor—, dejando el intento ``dispatching`` para
    que otra corrida lo reconciliara. Con la validación en el constructor, el fallo
    ocurre antes de que ``send()`` pueda siquiera retornar, y el worker lo trata
    exactamente igual que cualquier excepción sin clasificar."""
    nid = _batch(env)
    provider = _DefectiveProvider()
    report = _worker(env, provider).run_once()

    assert provider.calls == 1, "cuenta como una llamada al proveedor"
    row = _row(env, nid)
    assert row["status"] == "ambiguous", "possible_external_effect: no se sabe qué pasó"
    attempt = _attempts(env, nid)[0]
    assert attempt["phase"] == "finished", "no queda dispatching abierto"
    assert attempt["error_class"] == "provider_error"
    assert attempt["error_class"] != "dns_error", "la clase inventada nunca se persiste"
    assert attempt["error_message_safe"] == "unclassified_provider_exception"
    assert "dns_error" not in (attempt["error_message_safe"] or "")
    assert report.ambiguous == 1 and report.retried == 0


def test_el_mensaje_crudo_de_la_excepcion_no_se_persiste(env):
    """7 · El mensaje del fake trae la dirección y el texto a propósito."""
    nid = _batch(env)
    _worker(env, FakeProvider(default_mode=MODE_RAISE_UNCLASSIFIED)).run_once()

    attempt = _attempts(env, nid)[0]
    assert attempt["error_message_safe"] == "unclassified_provider_exception"
    assert "chat-ana-12345" not in (attempt["error_message_safe"] or "")
    assert "boom" not in (attempt["error_message_safe"] or "")


# ══ E · Render ═══════════════════════════════════════════════════════════════════

def test_payload_corrupto_muere_sin_llamar_al_proveedor(env):
    """8 · Payload corrupto → dead/render_error, cero llamadas."""
    nid = _batch(env)
    _sql(env, "UPDATE radar_notification_batches SET payload_json='{no json' "
              "WHERE notification_id=?", (nid,))
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider).run_once()

    row = _row(env, nid)
    assert (row["status"], row["dead_reason"]) == ("dead", "render_error")
    assert _attempts(env, nid)[0]["error_class"] == "render_error"
    assert provider.call_count == 0
    assert report.provider_calls == 0 and report.dead == 1


def test_version_desconocida_muere_sin_llamar_al_proveedor(env):
    """9 · Adivinar 'debe ser la vigente' convertiría un payload corrupto en un mensaje
    plausible."""
    nid = _batch(env, payload=_payload(version="p1b-payload-v99"))
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    _worker(env, provider).run_once()

    row = _row(env, nid)
    assert (row["status"], row["dead_reason"]) == ("dead", "render_error")
    assert provider.call_count == 0


def test_canal_del_payload_distinto_del_endpoint_falla_cerrado(env):
    """10 · **Mata M8.** El payload se armó para email —12.000 bytes de presupuesto—;
    dejarlo salir por Telegram bajo el presupuesto equivocado es lo que este fallo
    cerrado impide."""
    nid = _batch(env, payload=_payload(channel="email"))
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    _worker(env, provider).run_once()

    row = _row(env, nid)
    assert (row["status"], row["dead_reason"]) == ("dead", "render_error")
    assert provider.call_count == 0, "el canal externo confirma, nunca sustituye"


def test_payload_que_no_es_objeto_muere_como_render_error(env):
    nid = _batch(env)
    _sql(env, "UPDATE radar_notification_batches SET payload_json='[1,2,3]' "
              "WHERE notification_id=?", (nid,))
    _worker(env, FakeProvider(default_mode=MODE_ACCEPT)).run_once()

    assert _row(env, nid)["dead_reason"] == "render_error"


# ══ F · Proveedor ausente ════════════════════════════════════════════════════════

def test_canal_sin_proveedor_reintenta_como_provider_unavailable(env):
    """7 · Configuración que falta, no dato corrupto: registrar el proveedor lo
    resuelve, así que no mata el batch."""
    nid = _batch(env)
    report = _worker(env, providers={}).run_once()

    row = _row(env, nid)
    assert row["status"] == "pending"
    attempt = _attempts(env, nid)[0]
    assert attempt["outcome"] == "retry"
    assert attempt["error_class"] == "provider_unavailable"
    assert report.retried == 1 and report.provider_calls == 0


def test_provider_unavailable_agotado_muere_como_retries_exhausted(env):
    nid = _batch(env)
    policy = RetryPolicy(max_attempts=1)
    _worker(env, providers={}, policy=policy).run_once()

    row = _row(env, nid)
    assert (row["status"], row["dead_reason"]) == ("dead", "retries_exhausted")
    assert _attempts(env, nid)[0]["error_class"] == "provider_unavailable"


def test_un_canal_ausente_no_tumba_los_demas(env):
    """El worker no revienta por configuración incompleta: los canales que sí están
    siguen entregando."""
    otro = env["endpoints"].create_endpoint(
        principal_id=env["principal"], channel="email", address="ana@example.com"
    )
    env["endpoints"].mark_verified(otro, external_recipient_id="ana@example.com")
    pref = env["preferences"].create_preference(
        radar_id=env["radar"], endpoint_id=otro
    )
    env["preferences"].set_enabled(pref, True)
    telegram_nid = _batch(env)
    email_nid = _batch(env, payload=_payload(channel="email"), endpoint=otro)

    provider = FakeProvider(default_mode=MODE_ACCEPT)
    _worker(env, providers={"telegram": provider}).run_once()

    assert _row(env, telegram_nid)["status"] == "sent"
    assert _row(env, email_nid)["status"] == "pending"


# ══ G · Salidas que no llaman al proveedor (M10) ═════════════════════════════════

def test_quiet_hours_no_llama_al_proveedor(env):
    """11 · **Mata M10.**"""
    env["preferences"].update_preference(
        env["preference"], quiet_hours_start="22:00", quiet_hours_end="07:00",
        timezone="America/Bogota",
    )
    # 12:00 UTC = 07:00 en Bogotá… justo fuera. 04:00 UTC = 23:00 del día anterior.
    madrugada = datetime(2026, 7, 16, 4, 0, tzinfo=timezone.utc)
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider, clock=_Clock(madrugada)).run_once()

    assert provider.call_count == 0
    assert _row(env, nid)["status"] == "pending"
    assert report.held == 1 and report.provider_calls == 0


def test_opt_out_no_llama_al_proveedor(env):
    """12 · **Mata M10.** El batch muere por lo que la persona decidió, y queda
    registrado como tal."""
    nid = _batch(env)
    env["preferences"].set_enabled(env["preference"], False)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider).run_once()

    row = _row(env, nid)
    assert (row["status"], row["dead_reason"]) == ("dead", "opted_out")
    assert provider.call_count == 0
    assert report.dead == 1 and report.provider_calls == 0
    assert _attempts(env, nid) == [], "no se abre intento sobre lo que no se envía"


def test_endpoint_revocado_no_llama_al_proveedor(env):
    nid = _batch(env)
    env["endpoints"].revoke(env["endpoint"])
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    _worker(env, provider).run_once()

    assert _row(env, nid)["dead_reason"] == "endpoint_revoked"
    assert provider.call_count == 0


def test_security_blocked_no_llama_al_proveedor(env):
    """14 · **Mata M10.** El endpoint es de otra persona que la dueña del radar."""
    nid = _batch(env)
    otra = env["identity"].create_principal("beto@example.com")
    _sql(env, "UPDATE principal_owner_keys SET principal_id=? WHERE owner_key=?",
         (otra, env["owner"]))
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider).run_once()

    assert provider.call_count == 0
    assert report.security_blocked == 1 and report.provider_calls == 0
    assert _attempts(env, nid) == []


def test_claim_perdido_no_llama_al_proveedor(env):
    """13 · Entre reclamar y autorizar, otro worker se lleva el batch."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    worker = _worker(env, provider)
    original = worker.store.claim_candidate

    def robar(**kwargs):
        claimed = original(**kwargs)
        if claimed is not None:
            _sql(env, "UPDATE radar_notification_batches SET claim_token='otro' "
                      "WHERE notification_id=?", (claimed.notification_id,))
        return claimed

    worker.store.claim_candidate = robar
    report = worker.run_once(dispatch_limit=1)

    assert provider.call_count == 0
    assert report.claim_lost == 1 and report.provider_calls == 0
    assert _attempts(env, nid) == []


def test_batch_expirado_se_barre_sin_llamar_al_proveedor(env):
    """15 · Del sweep, no del dispatch."""
    nid = _batch(env, expires_at=(NOW - timedelta(seconds=1)).isoformat())
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider).run_once()

    row = _row(env, nid)
    assert (row["status"], row["dead_reason"]) == ("dead", "expired")
    assert provider.call_count == 0
    assert report.swept == 1 and report.provider_calls == 0


# ══ H · Reconciliación (M4, M5) ══════════════════════════════════════════════════

def test_huerfano_vivo_se_reconcilia_como_ambiguous_sin_llamar_al_proveedor(env):
    """16 · **Mata M4.** Un intento abierto significa efecto externo desconocido:
    preguntarle al proveedor no es posible y reenviar sería duplicar."""
    nid = _batch(env)
    attempt_id = _open_orphan(env, nid)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider, clock=_Clock(LATER)).run_once()

    assert provider.call_count == 0, "la reconciliación no llama al proveedor"
    assert _row(env, nid)["status"] == "ambiguous"
    attempt = env["ledger"].get_attempt(attempt_id)
    assert (attempt.phase, attempt.outcome) == ("finished", "ambiguous")
    assert report.reconciled == 1 and report.provider_calls == 0


def test_huerfano_expirado_se_reconcilia_como_ambiguous_nunca_como_expired(env):
    """17 · **Mata M5.** Marcarlo 'expired' afirmaría 'nada salió' sobre un mensaje que
    puede estar en el teléfono de alguien."""
    nid = _batch(env, expires_at=(NOW + timedelta(minutes=1)).isoformat())
    _open_orphan(env, nid)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider, clock=_Clock(LATER)).run_once()

    row = _row(env, nid)
    assert row["status"] == "ambiguous"
    assert row["dead_reason"] != "expired"
    assert provider.call_count == 0
    assert report.reconciled == 1 and report.swept == 0


def test_el_orden_es_reconciliar_barrer_despachar(env):
    """29 · **Mata M5 estructuralmente.** ``sweep_expired`` ya se niega a tocar batches
    con intento abierto, así que el orden no es la única defensa —es la primera, y
    tiene que quedar fijado aunque hoy sea redundante."""
    _batch(env)
    worker = _worker(env, FakeProvider(default_mode=MODE_ACCEPT))
    orden: list[str] = []

    def spy(name, original):
        def wrapper(*args, **kwargs):
            orden.append(name)
            return original(*args, **kwargs)
        return wrapper

    worker.store.claim_for_reconciliation = spy(
        "reconcile", worker.store.claim_for_reconciliation
    )
    worker.store.sweep_expired = spy("sweep", worker.store.sweep_expired)
    worker.store.claim_candidate = spy("dispatch", worker.store.claim_candidate)
    worker.run_once()

    assert orden.index("reconcile") < orden.index("sweep") < orden.index("dispatch")


# ══ I · Crash (M11) — el caso central del bloque ═════════════════════════════════

def test_crash_despues_del_proveedor_no_reenvia(env):
    """18 · **Mata M11.** El proveedor ya entregó y el ledger no lo sabe. La corrida
    siguiente reconcilia como ambiguous y **no** vuelve a llamar: un segundo efecto
    externo sería un mensaje duplicado que ya nadie puede distinguir de un reintento
    legítimo."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)

    def crash(phase):
        if phase == PHASE_AFTER_PROVIDER_SEND:
            raise RuntimeError("el proceso muere aquí")

    with pytest.raises(RuntimeError):
        _worker(env, provider, clock=_Clock(NOW)).run_once(_fault_hook=crash)

    # El intento quedó abierto: evidencia, no basura.
    assert provider.external_effect_count == 1
    assert _attempts(env, nid)[0]["phase"] == "dispatching"

    report = _worker(env, provider, clock=_Clock(LATER)).run_once()

    assert provider.call_count == 1, "no se volvió a llamar al proveedor"
    assert provider.external_effect_count == 1, "un solo efecto externo"
    assert _row(env, nid)["status"] == "ambiguous"
    assert report.reconciled == 1


def test_crash_antes_del_proveedor_tambien_reconcilia_a_ambiguous(env):
    """19 · El resultado conservador no cambia: desde fuera, 'abrí el intento y morí'
    es indistinguible de 'llamé y morí'. El ledger no puede saber cuál fue."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)

    def crash(phase):
        if phase == PHASE_BEFORE_PROVIDER_SEND:
            raise RuntimeError("el proceso muere aquí")

    with pytest.raises(RuntimeError):
        _worker(env, provider, clock=_Clock(NOW)).run_once(_fault_hook=crash)

    assert provider.call_count == 0
    assert _attempts(env, nid)[0]["phase"] == "dispatching"

    _worker(env, provider, clock=_Clock(LATER)).run_once()

    assert provider.call_count == 0, "nunca se llamó: reenviar sería inventar"
    assert _row(env, nid)["status"] == "ambiguous"


# ══ J · Concurrencia ═════════════════════════════════════════════════════════════

def test_dos_workers_concurrentes_llaman_exactamente_una_vez(env):
    """20 · La exclusión mutua es del claim; aquí se comprueba que el worker no la
    rompe por arriba."""
    _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def run():
        try:
            barrier.wait(timeout=JOIN_TIMEOUT)
            _worker(env, provider, clock=_Clock(NOW)).run_once()
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=JOIN_TIMEOUT)
        assert not thread.is_alive()

    assert errors == []
    assert provider.call_count == 1
    assert provider.external_effect_count == 1


def test_cuatro_workers_cincuenta_iteraciones_un_solo_efecto_por_batch(env):
    """21 · La propiedad que da sentido a todo el bloque, bajo contención real."""
    for _ in range(RACE_ITERATIONS):
        nid = _batch(env)
        provider = FakeProvider(default_mode=MODE_ACCEPT)
        barrier = threading.Barrier(4)
        errors: list[BaseException] = []

        def run():
            try:
                barrier.wait(timeout=JOIN_TIMEOUT)
                _worker(env, provider, clock=_Clock(NOW)).run_once()
            except BaseException as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=run) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=JOIN_TIMEOUT)
            assert not thread.is_alive()

        assert errors == []
        assert provider.external_effect_count <= 1
        assert _row(env, nid)["status"] == "sent"
        assert len([a for a in _attempts(env, nid) if a["outcome"] == "sent"]) == 1


# ══ K · Reporte ══════════════════════════════════════════════════════════════════

def test_corrida_sin_trabajo_devuelve_reporte_vacio(env):
    """27 · El worker corre en bucle: una excepción por 'nada que hacer' convertiría el
    estado normal en una alarma."""
    report = _worker(env, FakeProvider()).run_once()
    assert report == WorkerRunReport()


def test_sweep_limit_int_cero_omite_el_barrido_sin_reventar(env):
    """``sweep_limit=0`` (int exacto) es 'omití esta fase', igual que
    ``reconcile_limit=0`` y ``dispatch_limit=0`` -que ya lo toleran vía
    ``range(0)``-. ``sweep_expired`` se llama una sola vez fuera de todo bucle,
    así que sin el guard en ``run_once`` el 0 llegaba directo a ``WorkerStore``,
    que lo rechaza como bug de programación (ver ``scripts.telegram_pilot``, que
    corre con los tres límites en 0 salvo ``dispatch_limit``). El batch expirado
    NO se barre: quedarse ``pending`` prueba que la fase se omitió de verdad."""
    nid = _batch(env, expires_at=(NOW - timedelta(seconds=1)).isoformat())
    report = _worker(env, FakeProvider()).run_once(
        dispatch_limit=0, reconcile_limit=0, sweep_limit=0
    )

    assert report == WorkerRunReport()
    assert _row(env, nid)["status"] == "pending"


@pytest.mark.parametrize("bad_limit", [-1, False, 0.0])
def test_sweep_limit_no_es_int_cero_sigue_validando_en_worker_store(env, bad_limit):
    """El guard trata como omisión **solo** el int 0 exacto. ``-1``, ``False`` y
    ``0.0`` siguen llegando a ``sweep_expired`` para que ``WorkerStore`` los
    rechace: debilitar esa validación dejaría pasar ``limit=True`` (que es 1) y
    barrería un batch en silencio."""
    _batch(env, expires_at=(NOW - timedelta(seconds=1)).isoformat())
    with pytest.raises(WorkerStoreError, match="entero positivo"):
        _worker(env, FakeProvider()).run_once(
            dispatch_limit=0, reconcile_limit=0, sweep_limit=bad_limit
        )


def test_sweep_limit_positivo_barre_normalmente(env):
    """Un límite positivo NO es omisión: el batch expirado se barre a
    ``dead``/``expired`` como en cualquier corrida."""
    nid = _batch(env, expires_at=(NOW - timedelta(seconds=1)).isoformat())
    report = _worker(env, FakeProvider()).run_once(
        dispatch_limit=0, reconcile_limit=0, sweep_limit=1
    )

    assert report.swept == 1
    row = _row(env, nid)
    assert (row["status"], row["dead_reason"]) == ("dead", "expired")


def test_el_limite_de_dispatch_se_respeta(env):
    """28 · Tres batches, límite de dos."""
    for _ in range(3):
        _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider).run_once(dispatch_limit=2)

    assert provider.call_count == 2
    assert report.claimed == 2 and report.sent == 2


def test_el_reporte_no_contiene_pii(env):
    """26 · **Mata M9.** Este objeto se loguea entero al final de cada corrida."""
    _batch(env)
    report = _worker(env, FakeProvider(default_mode=MODE_ACCEPT)).run_once()
    text = repr(report)

    assert "chat-ana-12345" not in text
    assert "Radar PAE" not in text
    assert "Suministro" not in text
    for field in ("recipient", "token", "payload", "text"):
        assert field not in text


def test_el_reporte_es_determinista(env):
    _batch(env)
    _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    assert _worker(env, provider).run_once() == WorkerRunReport(
        claimed=2, authorized=2, sent=2, provider_calls=2
    )


# ══ L · Invariantes estructurales ════════════════════════════════════════════════

def test_el_worker_no_abre_intentos_por_begin_attempt(env):
    """30 · ``begin_attempt`` no revalida el consentimiento: abrir el intento por ahí
    dejaría una ventana donde una revocación se pierde. La única puerta es
    ``authorize_and_begin_dispatch``."""
    _batch(env)
    worker = _worker(env, FakeProvider(default_mode=MODE_ACCEPT))
    llamadas: list[str] = []
    original = worker.ledger.begin_attempt

    def spy(*args, **kwargs):  # pragma: no cover — debe no llamarse
        llamadas.append("begin_attempt")
        return original(*args, **kwargs)

    worker.ledger.begin_attempt = spy
    worker.run_once()

    assert llamadas == []


def test_el_worker_no_importa_sdk_ni_red():
    """Ningún import de red en el módulo: el contrato es con un Protocol, no con
    Telegram."""
    source = (
        __import__("pathlib").Path("app/core/notify/worker.py").read_text(encoding="utf-8")
    )
    for prohibido in ("import httpx", "import requests", "urllib", "telegram", "socket"):
        assert prohibido not in source


def test_el_intento_registra_la_version_congelada_del_payload(env):
    """El worker no reconstruye ni enriquece la metadata: usa lo que se congeló."""
    nid = _batch(env, payload=_payload(radar_name="Radar Viejo"))
    _sql(env, "UPDATE radars SET name='Radar Nuevo' WHERE id=?", (env["radar"],))
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    _worker(env, provider).run_once()

    payload = json.loads(_row(env, nid)["payload_json"])
    assert payload["radar_name"] == "Radar Viejo"
    assert provider.calls[0].payload_version == "p1b-payload-v2"


# ══ K · Dispatch dirigido (notification_id) ══════════════════════════════════════

def test_dispatch_dirigido_solo_envia_el_id_pedido(env):
    """Con un batch anterior también reclamable, el dispatch dirigido envía solo el
    ID pedido y deja el otro intacto."""
    other = _batch(env)          # anterior, también due
    target = _batch(env)         # el objetivo
    provider = FakeProvider(default_mode=MODE_ACCEPT)

    report = _worker(env, provider).run_once(
        dispatch_limit=1, reconcile_limit=0, sweep_limit=0, notification_id=target
    )

    assert _row(env, target)["status"] == "sent"
    assert _row(env, other)["status"] == "pending"
    assert provider.call_count == 1
    assert report.sent == 1


def test_dispatch_dirigido_no_cae_a_otro_si_target_no_reclamable(env):
    """Target no due (not_before futuro): cero provider calls y no se envía otro,
    aunque haya un batch reclamable disponible."""
    ready = _batch(env)                       # reclamable
    target = _batch(env, not_before=FOREVER)  # no due
    provider = FakeProvider(default_mode=MODE_ACCEPT)

    report = _worker(env, provider).run_once(
        dispatch_limit=1, reconcile_limit=0, sweep_limit=0, notification_id=target
    )

    assert provider.call_count == 0
    assert provider.external_effect_count == 0
    assert _row(env, target)["status"] == "pending"
    assert _row(env, ready)["status"] == "pending"
    assert report.claimed == 0


def test_dispatch_dirigido_target_terminal_no_hace_fallback(env):
    """Target ya sent: el dispatch dirigido no cae a otro batch reclamable."""
    target = _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    _worker(env, provider).run_once(
        dispatch_limit=1, reconcile_limit=0, sweep_limit=0, notification_id=target
    )
    assert _row(env, target)["status"] == "sent"

    other = _batch(env)  # reclamable, pero no es el objetivo
    provider2 = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider2).run_once(
        dispatch_limit=1, reconcile_limit=0, sweep_limit=0, notification_id=target
    )

    assert provider2.call_count == 0
    assert _row(env, other)["status"] == "pending"
    assert report.claimed == 0


def test_dispatch_dirigido_exige_dispatch_limit_uno(env):
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    with pytest.raises(ValueError, match="dispatch_limit == 1"):
        _worker(env, provider).run_once(dispatch_limit=2, notification_id="cualquiera")


def test_sin_notification_id_el_comportamiento_es_identico(env):
    """Sin dispatch dirigido, un batch reclamable se envía como siempre."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider).run_once(
        dispatch_limit=1, reconcile_limit=0, sweep_limit=0
    )
    assert _row(env, nid)["status"] == "sent"
    assert report.sent == 1


# ══ L · dispatch_limit dirigido: igualdad estricta de tipo (no ``!=``) ═══════

@pytest.mark.parametrize("bad_limit", [True, 1.0, 0, 2])
def test_dispatch_dirigido_rechaza_limit_no_entero_exacto(env, bad_limit):
    """En Python ``True == 1`` y ``1.0 == 1``: un ``!=`` simple dejaría pasar un
    bool/float disfrazado de "exactamente uno". La validación exige el tipo
    ``int`` además del valor, y debe dispararse antes de reclamar nada."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    worker = _worker(env, provider)

    with pytest.raises(ValueError, match="dispatch_limit == 1"):
        worker.run_once(dispatch_limit=bad_limit, notification_id=nid)

    # No debe haberse reclamado nada: la validación es previa a cualquier I/O.
    assert _row(env, nid)["status"] == "pending"
    assert provider.call_count == 0


def test_dispatch_dirigido_int_uno_exacto_si_pasa(env):
    """``dispatch_limit=1`` (int puro) es el único valor que la validación acepta;
    con él, el dispatch dirigido procede con normalidad."""
    nid = _batch(env)
    provider = FakeProvider(default_mode=MODE_ACCEPT)
    report = _worker(env, provider).run_once(
        dispatch_limit=1, reconcile_limit=0, sweep_limit=0, notification_id=nid
    )
    assert _row(env, nid)["status"] == "sent"
    assert report.sent == 1
