"""Tests de app/core/notify/authorization.py — la matriz única de "¿se puede
entregar esto ahora?" que selector (P1B-3) y worker (P1B-4) comparten."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from app.core.db import connect_sqlite
from app.core.notify import authorization
from app.core.notify.authorization import (
    AUTHORIZED,
    HOLD,
    SECURITY_BLOCKED,
    TERMINAL,
    AuthorizationSnapshot,
    authorize_row,
    evaluate,
    fetch_target_row,
    skip_reason,
    snapshot_from_row,
)
from app.core.notify.endpoint_store import EndpointStore
from app.core.notify.identity_store import IdentityStore
from app.core.notify.masking import Masker
from app.core.notify.preference_store import PreferenceStore
from app.core.notify.selector import NotificationSelector
from app.core.radar.store import RadarStore

NOW = datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc)


def _valid(**overrides) -> AuthorizationSnapshot:
    """Un snapshot que autoriza. Cada test degrada exactamente un hecho, para que la
    prueba mida esa condición y no una mezcla."""
    base = AuthorizationSnapshot(
        principal_id="pri-1",
        endpoint_id="end-1",
        radar_id="rad-1",
        channel="telegram",
        preference_enabled=True,
        endpoint_verified=True,
        endpoint_enabled=True,
        endpoint_revoked=False,
        principal_status="active",
        radar_archived=False,
        owner_principal_id="pri-1",
        external_recipient_id="chat-123",
    )
    return replace(base, **overrides) if overrides else base


@pytest.fixture
def env(tmp_path):
    """Base real con el par completo autorizado: prueba que la matriz se alimenta del
    mismo join que el selector usa en producción, no de un snapshot inventado."""
    db = tmp_path / "authorization.db"
    radars = RadarStore(db)
    identity = IdentityStore(db)
    endpoints = EndpointStore(db, masker=Masker.from_key("authz-test-key"))
    preferences = PreferenceStore(db)
    owner = "email:ana@example.com"
    principal = identity.create_principal("ana@example.com")
    identity.link_owner_key(owner, principal)
    endpoint = endpoints.create_endpoint(
        principal_id=principal, channel="telegram", address="ana-chat"
    )
    endpoints.mark_verified(endpoint, external_recipient_id="chat-ana")
    radar = radars.create_radar(name="Radar PAE", owner=owner, query_frame_json="{}")
    preference = preferences.create_preference(radar_id=radar, endpoint_id=endpoint)
    preferences.set_enabled(preference, True)
    return {
        "db": db, "radars": radars, "identity": identity, "endpoints": endpoints,
        "preferences": preferences, "principal": principal, "endpoint": endpoint,
        "radar": radar, "preference": preference, "owner": owner,
    }


def _decide(env, endpoint=None, radar=None):
    conn = connect_sqlite(env["db"])
    try:
        row = fetch_target_row(conn, endpoint or env["endpoint"], radar or env["radar"])
        return authorize_row(row)
    finally:
        conn.close()


def _sql(env, statement, params=()):
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(statement, params)
    finally:
        conn.close()


# ── A1–A11 · la matriz sobre snapshots puros ─────────────────────────────────────

def test_1_valid_snapshot_is_authorized():
    """1 · Todas las condiciones válidas → authorized, sin razón ni dead_reason."""
    decision = evaluate(_valid())
    assert decision.category == AUTHORIZED
    assert decision.reason is None
    assert decision.dead_reason is None


def test_2_preference_not_found_is_terminal_opted_out():
    """2 · Sin preferencia no hubo consentimiento nunca: terminal/opted_out."""
    decision = evaluate(None)
    assert (decision.category, decision.reason, decision.dead_reason) == (
        TERMINAL, "preference_not_found", "opted_out",
    )


def test_3_preference_disabled_is_terminal_opted_out():
    """3 · preference disabled → opted_out."""
    decision = evaluate(_valid(preference_enabled=False))
    assert (decision.category, decision.reason, decision.dead_reason) == (
        TERMINAL, "preference_disabled", "opted_out",
    )


def test_4_endpoint_revoked_is_terminal():
    """4 · endpoint revocado → terminal/endpoint_revoked."""
    decision = evaluate(_valid(endpoint_revoked=True))
    assert (decision.category, decision.reason, decision.dead_reason) == (
        TERMINAL, "endpoint_revoked", "endpoint_revoked",
    )


def test_5_radar_archived_is_terminal():
    """5 · radar archivado → terminal/radar_archived."""
    decision = evaluate(_valid(radar_archived=True))
    assert (decision.category, decision.reason, decision.dead_reason) == (
        TERMINAL, "radar_archived", "radar_archived",
    )


def test_6_endpoint_disabled_is_hold():
    """6 · endpoint deshabilitado → hold: la pausa es reversible."""
    decision = evaluate(_valid(endpoint_enabled=False))
    assert (decision.category, decision.reason) == (HOLD, "endpoint_disabled")
    assert decision.dead_reason is None


def test_7_endpoint_unverified_is_hold():
    """7 · endpoint sin verificar → hold."""
    decision = evaluate(_valid(endpoint_verified=False))
    assert (decision.category, decision.reason) == (HOLD, "endpoint_unverified")


@pytest.mark.parametrize("recipient", [None, "", "   "])
def test_8_recipient_missing_is_hold(recipient):
    """8 · recipient ausente → hold, nunca dead: es restituible con mark_verified."""
    decision = evaluate(_valid(external_recipient_id=recipient))
    assert (decision.category, decision.reason) == (HOLD, "endpoint_missing_recipient")
    assert decision.dead_reason is None


def test_9_principal_suspended_is_hold():
    """9 · principal suspendido → hold: la suspensión se levanta."""
    decision = evaluate(_valid(principal_status="suspended"))
    assert (decision.category, decision.reason) == (HOLD, "principal_suspended")


def test_10_owner_not_linked_is_hold():
    """10 · owner sin vincular → hold."""
    decision = evaluate(_valid(owner_principal_id=None))
    assert (decision.category, decision.reason) == (HOLD, "owner_not_linked")


def test_11_principal_mismatch_is_security_blocked():
    """11 · El endpoint es de otra persona que la dueña del radar. No es un hold: es
    el fallo de aislamiento que P1B-1 existe para impedir."""
    decision = evaluate(_valid(owner_principal_id="pri-otra"))
    assert (decision.category, decision.reason) == (SECURITY_BLOCKED,
                                                    "principal_mismatch")
    assert decision.dead_reason is None


def test_hold_decisions_never_carry_dead_reason():
    """Ningún hold puede persistirse como muerte: un hold no es terminal."""
    holds = [
        _valid(endpoint_enabled=False), _valid(endpoint_verified=False),
        _valid(external_recipient_id=None), _valid(principal_status="suspended"),
        _valid(owner_principal_id=None),
    ]
    for snapshot in holds:
        assert evaluate(snapshot).dead_reason is None


def test_terminal_dead_reasons_are_accepted_by_the_schema_check(tmp_path):
    """Toda decisión terminal debe poder escribirse: un dead_reason que el CHECK
    rechace sería una muerte inexpresable."""
    from app.core.notify.outbox_store import BatchEventRef, OutboxStore

    db = tmp_path / "check.db"
    store = OutboxStore(db)
    decisions = [
        evaluate(None),
        evaluate(_valid(preference_enabled=False)),
        evaluate(_valid(endpoint_revoked=True)),
        evaluate(_valid(radar_archived=True)),
    ]
    assert all(d.category == TERMINAL for d in decisions)
    reasons = {d.dead_reason for d in decisions}
    conn = connect_sqlite(db)
    try:
        for index, reason in enumerate(sorted(reasons)):
            res = store.create_batch(
                principal_id="p", endpoint_id=f"e{index}", radar_id="r",
                payload_json={}, events=[BatchEventRef(f"ev{index}", index + 1)],
                created_at="2026-07-15T00:00:00+00:00",
                expires_at="2099-01-01T00:00:00+00:00",
            )
            conn.execute(
                "UPDATE radar_notification_batches SET status='dead', dead_reason=? "
                "WHERE notification_id=?",
                (reason, res.notification_id),
            )
    finally:
        conn.close()


# ── A12–A14 · lo que la autorización NO mira ─────────────────────────────────────

def test_12_radar_disabled_does_not_block(env):
    """12 · radar.enabled=0 describe la salud del scrapeo, no el consentimiento. Un
    evento ya materializado es un hecho ocurrido."""
    _sql(env, "UPDATE radars SET enabled=0 WHERE id=?", (env["radar"],))
    assert _decide(env).category == AUTHORIZED


def test_13_needs_attention_does_not_block(env):
    """13 · needs_attention=1 no retira el permiso de contar lo que ya pasó."""
    _sql(env, "UPDATE radars SET needs_attention=1 WHERE id=?", (env["radar"],))
    assert _decide(env).category == AUTHORIZED


def test_14_future_next_run_after_does_not_block(env):
    """14 · next_run_after futuro tampoco: es cuándo volveremos a mirar, no si
    podemos avisar de lo ya visto."""
    _sql(env, "UPDATE radars SET next_run_after='2099-01-01T00:00:00+00:00' WHERE id=?",
         (env["radar"],))
    assert _decide(env).category == AUTHORIZED


def test_operational_health_columns_never_block(env):
    """Los cinco a la vez, en el peor estado posible: sigue autorizado (INV-W12)."""
    _sql(env, "UPDATE radars SET enabled=0, needs_attention=1, failure_count=99, "
              "permanent_failure_streak=99, next_run_after='2099-01-01T00:00:00+00:00' "
              "WHERE id=?", (env["radar"],))
    assert _decide(env).category == AUTHORIZED


# ── A15 · equivalencia con el selector ───────────────────────────────────────────

@pytest.mark.parametrize(
    "statement,reason",
    [
        ("UPDATE radar_notification_preferences SET enabled=0", "preference_disabled"),
        ("UPDATE notification_endpoints SET verified_at=NULL", "endpoint_unverified"),
        ("UPDATE notification_endpoints SET external_recipient_id=''",
         "endpoint_missing_recipient"),
        ("UPDATE notification_endpoints SET enabled=0", "endpoint_disabled"),
        ("UPDATE notification_endpoints SET revoked_at='x'", "endpoint_revoked"),
        ("UPDATE principals SET status='suspended'", "principal_suspended"),
        ("UPDATE radars SET archived_at='x'", "radar_archived"),
        ("DELETE FROM principal_owner_keys", "owner_not_linked"),
    ],
)
def test_15_skip_reason_matches_selector_contract(env, statement, reason):
    """15 · El selector conserva su contrato exacto tras el refactor: la razón que
    reporta sale de la matriz compartida y es la misma string que ya reportaba."""
    _sql(env, statement)
    conn = connect_sqlite(env["db"])
    try:
        row = fetch_target_row(conn, env["endpoint"], env["radar"])
    finally:
        conn.close()
    assert skip_reason(row) == reason

    selector = NotificationSelector(env["db"])
    result = selector.select_pair(env["endpoint"], env["radar"], now=NOW)
    assert result.skipped_reason == reason


def test_15b_selector_and_worker_read_the_same_sql():
    """15b · Una sola definición: el selector importa el SQL y la matriz, no los
    copia. Si alguien reintroduce una copia local, esto cae."""
    import app.core.notify.selector as selector_module

    assert selector_module._TARGET_SQL is authorization.TARGET_SQL
    assert selector_module._target_skip_reason is authorization.skip_reason


def test_15c_missing_preference_skips_as_not_found(env):
    """15c · Sin preferencia el selector salta por preference_not_found, igual que la
    matriz."""
    _sql(env, "DELETE FROM radar_notification_preferences")
    assert skip_reason(None) == "preference_not_found"
    selector = NotificationSelector(env["db"])
    result = selector.select_pair(env["endpoint"], env["radar"], now=NOW)
    assert result.skipped_reason == "preference_not_found"


def test_15d_authorized_pair_is_not_skipped(env):
    """15d · El par válido no produce razón de salto: authorized ⇒ skip_reason None."""
    conn = connect_sqlite(env["db"])
    try:
        row = fetch_target_row(conn, env["endpoint"], env["radar"])
    finally:
        conn.close()
    assert skip_reason(row) is None
    assert authorize_row(row).category == AUTHORIZED


# ── A16 · privacidad ─────────────────────────────────────────────────────────────

def test_16_snapshot_repr_never_exposes_recipient():
    """16 · El snapshot es justo lo que alguien loguea entero al depurar. El destino
    real de una persona no puede viajar en un repr."""
    snapshot = _valid(external_recipient_id="chat-secreto-123")
    assert "chat-secreto-123" not in repr(snapshot)
    assert "external_recipient_id" not in repr(snapshot)
    # Pero sigue siendo legible para quien lo pide explícitamente.
    assert snapshot.external_recipient_id == "chat-secreto-123"


def test_16b_snapshot_from_row_carries_recipient_without_reprinting_it(env):
    """16b · El recipient sobrevive al join (el worker lo necesita) y sigue sin
    aparecer en el repr."""
    conn = connect_sqlite(env["db"])
    try:
        row = fetch_target_row(conn, env["endpoint"], env["radar"])
    finally:
        conn.close()
    snapshot = snapshot_from_row(row, notification_id="n-1")
    assert snapshot.external_recipient_id == "chat-ana"
    assert "chat-ana" not in repr(snapshot)
    assert snapshot.notification_id == "n-1"
    assert snapshot.channel == "telegram"


def test_16c_decision_repr_carries_no_recipient():
    """16c · La decisión que viaja a logs y métricas solo lleva vocabulario cerrado."""
    decision = evaluate(_valid(external_recipient_id="chat-secreto-123"))
    assert "chat-secreto-123" not in repr(decision)


# ── R2B-1A · precedencia combinada ───────────────────────────────────────────────
#
# Los tests de arriba degradan **un** hecho cada uno, y por eso ninguno vio el
# problema: con una sola condición activa, cualquier orden de la cadena da la misma
# respuesta. La precedencia solo se observa cuando dos condiciones son ciertas a la
# vez, que es justo lo que pasa en producción. Estos son esos casos.

def test_13_mismatch_beats_preference_disabled():
    """13 · Un fallo de aislamiento no se registra como una baja rutinaria.

    Antes salía ``opted_out`` (terminal): la etiqueta decía "se dio de baja" sobre un
    endpoint que pertenece a otra persona. Ninguna alerta podía verlo.
    """
    decision = evaluate(_valid(owner_principal_id="otra-persona",
                               preference_enabled=False))
    assert (decision.category, decision.reason) == (SECURITY_BLOCKED,
                                                    "principal_mismatch")


def test_14_mismatch_beats_endpoint_disabled():
    """14 · Tampoco lo tapa un hold."""
    decision = evaluate(_valid(owner_principal_id="otra-persona",
                               endpoint_enabled=False))
    assert (decision.category, decision.reason) == (SECURITY_BLOCKED,
                                                    "principal_mismatch")


def test_14b_mismatch_beats_every_other_condition_at_once():
    """14b · La precedencia no depende de cuántas condiciones acompañen."""
    decision = evaluate(_valid(
        owner_principal_id="otra-persona", preference_enabled=False,
        endpoint_revoked=True, endpoint_enabled=False, endpoint_verified=False,
        radar_archived=True, principal_status="suspended",
        external_recipient_id=None,
    ))
    assert (decision.category, decision.reason) == (SECURITY_BLOCKED,
                                                    "principal_mismatch")


def test_14c_unlinked_owner_is_not_a_mismatch():
    """14c · Sin dueño conocido no hay dos partes que puedan discrepar.

    ``owner_principal_id is None`` no es "de otra persona", es "no se sabe de quién".
    Acusar de mismatch sería inventar la acusación; autorizar sería inventar el
    permiso. Se sostiene como hold.
    """
    decision = evaluate(_valid(owner_principal_id=None))
    assert (decision.category, decision.reason) == (HOLD, "owner_not_linked")


def test_15_revoked_beats_unverified():
    """15 · **El caso que motivó todo el reordenamiento.**

    Antes daba ``endpoint_unverified`` (hold): la revocación quedaba tapada, el batch
    se reevaluaba cada quince minutos y acababa muriendo por TTL como ``expired``. El
    ledger terminaba afirmando "se nos hizo tarde" sobre una persona que había
    retirado el consentimiento.
    """
    decision = evaluate(_valid(endpoint_revoked=True, endpoint_verified=False))
    assert (decision.category, decision.reason, decision.dead_reason) == (
        TERMINAL, "endpoint_revoked", "endpoint_revoked",
    )


def test_15b_revocation_can_never_be_recorded_as_expired():
    """15b · La regla, no el caso: con el endpoint revocado, **ninguna** combinación
    de condiciones reversibles puede devolver algo que no sea terminal.

    Un hold habría dejado el batch vivo hasta el TTL, y su lápida habría sido
    ``expired``. Que la decisión sea terminal es lo que garantiza que la causa que se
    registra es la revocación.
    """
    for extra in (
        {"endpoint_verified": False},
        {"endpoint_enabled": False},
        {"principal_status": "suspended"},
        {"external_recipient_id": None},
        {"owner_principal_id": None},
    ):
        decision = evaluate(_valid(endpoint_revoked=True, **extra))
        assert decision.category == TERMINAL, extra
        assert decision.dead_reason == "endpoint_revoked", extra


def test_16_revoked_beats_preference_disabled():
    """16 · Ambas son retiro de consentimiento; se reporta la más amplia.

    La revocación es a nivel de canal —afecta a todos los radares—; el opt-out es a
    nivel de este radar.
    """
    decision = evaluate(_valid(endpoint_revoked=True, preference_enabled=False))
    assert (decision.category, decision.reason, decision.dead_reason) == (
        TERMINAL, "endpoint_revoked", "endpoint_revoked",
    )


def test_17_archived_beats_suspended():
    """17 · Terminal por encima de hold: el radar archivado no vuelve solo."""
    decision = evaluate(_valid(radar_archived=True, principal_status="suspended"))
    assert (decision.category, decision.reason, decision.dead_reason) == (
        TERMINAL, "radar_archived", "radar_archived",
    )


def test_18_suspended_beats_unverified():
    """18 · Entre dos holds, el de la persona por delante del de su destino."""
    decision = evaluate(_valid(principal_status="suspended",
                               endpoint_verified=False))
    assert (decision.category, decision.reason) == (HOLD, "principal_suspended")


def test_19_unknown_category_cannot_authorize():
    """19 · Exhaustividad: toda decisión cae en una de las cuatro categorías, y
    ninguna que no sea ``authorized`` deja enviar.

    Es lo que permite al worker hacer dispatch sin rama por defecto: una categoría
    nueva —o un typo— no puede convertirse en permiso de envío por omisión.
    """
    from app.core.notify.authorization import CATEGORIES

    for snapshot in (
        None,
        _valid(),
        _valid(owner_principal_id="otra-persona"),
        _valid(endpoint_revoked=True),
        _valid(preference_enabled=False),
        _valid(radar_archived=True),
        _valid(principal_status="suspended"),
        _valid(endpoint_verified=False),
        _valid(external_recipient_id=None),
        _valid(endpoint_enabled=False),
        _valid(owner_principal_id=None),
    ):
        decision = evaluate(snapshot)
        assert decision.category in CATEGORIES
        if decision.category != AUTHORIZED:
            assert decision.reason is not None


def test_precedence_holds_against_the_real_join(env):
    """La precedencia no es una propiedad de los snapshots de laboratorio: se
    comprueba contra el mismo join que el selector y el worker ejecutan."""
    _sql(env, "UPDATE notification_endpoints SET revoked_at = ?, verified_at = NULL "
              "WHERE endpoint_id = ?", ("2026-07-15T00:00:00+00:00", env["endpoint"]))

    decision = _decide(env)

    assert (decision.category, decision.reason) == (TERMINAL, "endpoint_revoked")
