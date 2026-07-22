#!/usr/bin/env python3
"""pilot_real.py — orquestación del primer piloto real de Radar Adulto Mayor por Telegram.

Un módulo de una sola responsabilidad: encadenar las piezas **productivas** ya
existentes en un ciclo operativo seguro y aislado. No construye una tubería
paralela ni escribe eventos/batches a mano: todo pasa por los stores existentes.

    run_radar → RadarStore/radar_events → NotificationSelector → payload v3
    congelado → OutboxStore → render_payload → NotificationWorker → TelegramProvider

La base del piloto es **una sola ruta canónica** (``REAL_PILOT_DB_PATH``), no un
argumento arbitrario: el CLI valida su ``--db`` contra ella y las funciones de
orquestación operan siempre sobre esa ruta. El token y la allowlist viven
exclusivamente en el entorno.

La separación revisión/envío es deliberada: ``run_real_pilot`` inspecciona los
pending del par **antes** de consultar SECOP; ``send_ready_pilot`` corre un
preflight completo y solo entonces despacha, dirigido al ``notification_id`` ya
congelado, tras el flag literal ``--confirm-send``.
"""
from __future__ import annotations

import dataclasses
import json
import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.notify.endpoint_store import EndpointStore
from app.core.notify.identity_store import IdentityStore
from app.core.notify.masking import Masker
from app.core.notify.preference_store import (
    EventWatermarkChangedError,
    PreferenceStore,
)
from app.core.notify.render import (
    CHANNEL_BYTE_BUDGETS,
    PAYLOAD_VERSION_V3,
    render_payload,
)
from app.core.notify.schema import FREQUENCY_PER_RUN
from app.core.notify.selector import NotificationSelector
from app.core.notify.telegram_provider import (
    TelegramConfiguration,
    TelegramProvider,
)
from app.core.notify.worker import NotificationWorker, WorkerRunReport
from app.core.query_frame import frame_from_params
from app.core.radar.runner import run_radar
from app.core.radar.store import RadarStore


# ── Ruta canónica del piloto ──────────────────────────────────────────────────
# La ubicación productiva se deriva de la raíz real del repositorio, no de una
# cadena relativa dependiente del CWD. Las pruebas monkeypatchan
# ``REAL_PILOT_DB_PATH`` a un tmp_path; el código productivo nunca acepta una ruta
# arbitraria por argumento: solo confirma que el ``--db`` del operador resuelve a
# esta ruta y opera sobre ella.
REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_PILOT_DB_PATH = (REPO_ROOT / "data" / "telegram_p2_real.db").resolve()
# Bases prohibidas con razón específica (además del contraste con la canónica).
SECOPPAL_DB_PATH = (REPO_ROOT / "data" / "secoppal.db").resolve()
DEMO_DB_PATH = (REPO_ROOT / "data" / "telegram_pilot.db").resolve()

REAL_PILOT_OWNER_KEY = "telegram-p2-real-pilot"
REAL_PILOT_PRINCIPAL_EMAIL = "telegram-p2-real-pilot@local.invalid"
REAL_PILOT_RADAR_NAME = "SECOPPAL Radar · Adulto Mayor (piloto real)"
REAL_PILOT_DATASET = "procesos"
REAL_PILOT_TIMEZONE = "America/Bogota"
REAL_PILOT_QUIET_HOURS_START = "21:00"
REAL_PILOT_QUIET_HOURS_END = "07:00"
# Conservador a propósito: el primer piloto real prefiere un digest corto.
REAL_PILOT_MAX_ITEMS = 5
# Clave HMAC del ``Masker``: identifica los endpoints por ``address_hash`` y por eso
# tiene que ser secreta (un hash de chat_id con clave pública es enumerable),
# estable (si cambia, los endpoints existentes se vuelven invisibles) y **distinta**
# del token de Telegram (que se puede rotar). Vive solo en el entorno; nunca en el
# código ni en la salida.
NOTIFICATION_MASKING_KEY_ENV = "SECOPPAL_NOTIFICATION_MASKING_KEY"

# Razones seguras y estables. Nunca incorporan token, chat_id ni dirección cruda.
SAFE_REAL_MAIN_DB_REFUSED = "pilot_real_refuses_primary_database"
SAFE_REAL_DEMO_DB_REFUSED = "pilot_real_refuses_demo_database"
SAFE_REAL_WRONG_DB = "pilot_real_requires_canonical_telegram_p2_real_db"
SAFE_REAL_DB_NOT_FOUND = "pilot_real_database_not_found"
SAFE_REAL_CHAT_ID_INVALID = "pilot_real_requires_exactly_one_allowed_chat_id"
SAFE_REAL_NO_PILOT = "pilot_real_not_set_up"
SAFE_REAL_AMBIGUOUS_PILOT = "pilot_real_requires_exactly_one_pilot_preference"
SAFE_REAL_ENDPOINT_DRIFT = "pilot_real_endpoint_config_drift"
SAFE_REAL_RADAR_DRIFT = "pilot_real_radar_config_drift"
SAFE_REAL_PREFERENCE_DRIFT = "pilot_real_preference_config_drift"
SAFE_REAL_BASELINE_ALREADY_DONE = "pilot_real_baseline_already_enabled"
SAFE_REAL_BASELINE_PRECONDITION = "pilot_real_baseline_requires_clean_pre_state"
SAFE_REAL_BASELINE_NON_BASELINE = "pilot_real_baseline_run_produced_non_baseline_event"
SAFE_REAL_BASELINE_CURSOR_MISMATCH = "pilot_real_baseline_cursor_not_at_max_event_seq"
SAFE_REAL_BASELINE_CHANGED_BEFORE_ENABLE = "pilot_real_baseline_changed_before_enable"
SAFE_REAL_BASELINE_PRODUCED_BATCH = "pilot_real_baseline_must_not_produce_a_batch"
SAFE_REAL_MULTIPLE_READY = "pilot_real_more_than_one_batch_ready"
SAFE_REAL_NOT_EXACTLY_ONE_READY = "pilot_real_requires_exactly_one_ready_batch"
SAFE_REAL_PAYLOAD_UNREADABLE = "pilot_real_payload_unreadable"
SAFE_REAL_PAYLOAD_UNEXPECTED_VERSION = "pilot_real_payload_unexpected_version"
SAFE_REAL_PAYLOAD_MISASSOCIATED = "pilot_real_payload_pair_mismatch"
SAFE_REAL_PAYLOAD_OVER_BUDGET = "pilot_real_payload_over_channel_budget"
SAFE_REAL_CONFIRM_REQUIRED = "pilot_real_literal_confirm_send_required"
SAFE_REAL_ALLOWLIST_REQUIRED = "pilot_real_allowlist_required"
SAFE_REAL_ALLOWLIST_MISMATCH = "pilot_real_allowlist_recipient_mismatch"
SAFE_REAL_RUN_NOT_SUCCESSFUL = "pilot_real_radar_run_not_successful"
SAFE_REAL_MASKING_KEY_REQUIRED = "pilot_real_notification_masking_key_required"
SAFE_REAL_ENDPOINT_NOT_ELIGIBLE = "pilot_real_endpoint_not_eligible"


class PilotRealError(Exception):
    """Fallo operativo seguro del piloto real. Su texto es una razón estable."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _require_aware(now: datetime) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise PilotRealError("pilot_real_now_must_be_timezone_aware")
    return now


def _require_masking_key() -> str:
    """Clave HMAC del ``Masker``, obligatoria, desde el entorno. Nunca se imprime.

    Se lee antes de crear o abrir la base: sin ella no se materializa nada, porque
    el ``address_hash`` del endpoint —su identidad— depende de esta clave, y una
    clave ausente produciría hashes que nadie más puede reproducir.
    """
    raw = (os.environ.get(NOTIFICATION_MASKING_KEY_ENV) or "").strip()
    if not raw:
        raise PilotRealError(SAFE_REAL_MASKING_KEY_REQUIRED)
    return raw


def _require_successful_run(run: Any) -> Any:
    """Una corrida de radar realmente exitosa, o falla cerrado.

    Un timeout, un error material o un estado de control (``already_running``,
    ``lease_lost``, ``not_found``) **no** es "no hubo novedades": es "no sabemos".
    Los estados de control traen ``run_id=""`` y ``status='skipped'``; los fallos
    materiales, ``status='error'``. Solo ``status=='ok'`` con ``run_id`` y ``diff``
    autoriza a leer eventos o a correr el selector sobre esa corrida.
    """
    if run.status != "ok" or not run.run_id or run.diff is None:
        raise PilotRealError(SAFE_REAL_RUN_NOT_SUCCESSFUL)
    return run


# ── Configuración del radar (FASE 3, búsqueda clásica, congelada) ──────────────

def adulto_mayor_query_frame_json() -> str:
    """QueryFrame congelado y auditable del radar Adulto Mayor.

    Búsqueda **clásica** derivada de la consulta que encontró correctamente
    ``CO1.REQ.10686701`` / ``MR- SASIP-002-2026`` (ALCALDÍA MUNICIPAL DE RÁQUIRA):
    ``objeto=['adulto_mayor']`` sobre ``procesos``. ``adulto_mayor`` es una frase
    de dominio protegida (ver ``app/data/morphological_variants.py``) que el
    ``SoQLBuilder`` expande a los LIKE ``%adulto mayor%`` / ``%persona mayor%`` /
    ``%personas mayores%``. No se fija ninguna semilla territorial: la primera
    corrida es el baseline general del conjunto, no el seed de ese único proceso.

    ``classification_source`` sigue siendo búsqueda clásica: no se importa el nodo
    Adulto Mayor del warehouse ni se añaden ``node_signals``.
    """
    frame = frame_from_params(
        {"objeto": ["adulto_mayor"], "dataset": REAL_PILOT_DATASET}
    )
    # ``sort_keys`` para que el frame congelado sea byte-estable y auditable.
    return json.dumps(dataclasses.asdict(frame), ensure_ascii=False, sort_keys=True)


# ── Guards de ruta ────────────────────────────────────────────────────────────

def _require_db_matches(db_path: str | Path) -> Path:
    """Valida que ``db_path`` resuelve **exactamente** a la ruta canónica.

    ``expanduser().resolve()`` colapsa ``~``, ``..`` y symlinks: un symlink que
    apunte a otro sitio resuelve a su destino y por tanto deja de igualar la
    canónica. No toca el disco (``resolve`` no crea el archivo), así que corre
    antes de abrir nada. Devuelve la ruta canónica, nunca la pasada: el resto del
    módulo opera sobre ``REAL_PILOT_DB_PATH`` y no sobre lo que llegó por argumento.
    """
    resolved = Path(db_path).expanduser().resolve()
    if resolved == SECOPPAL_DB_PATH:
        raise PilotRealError(SAFE_REAL_MAIN_DB_REFUSED)
    if resolved == DEMO_DB_PATH:
        raise PilotRealError(SAFE_REAL_DEMO_DB_REFUSED)
    if resolved != REAL_PILOT_DB_PATH:
        raise PilotRealError(SAFE_REAL_WRONG_DB)
    return REAL_PILOT_DB_PATH


def _pilot_db(*, require_exists: bool) -> Path:
    """La ruta canónica del piloto. El código productivo no toma otra."""
    target = REAL_PILOT_DB_PATH
    if require_exists and not target.exists():
        raise PilotRealError(SAFE_REAL_DB_NOT_FOUND)
    return target


# ── Lectores de solo lectura (mode=ro, sin construir stores) ───────────────────

def _ro_conn(target: Path) -> sqlite3.Connection:
    """Abre la base en modo estrictamente de solo lectura.

    ``mode=ro`` no crea el archivo, no aplica pragmas de escritura y no corre
    ``ensure_schema``: es la vía correcta para inspeccionar sin mutar ni un byte
    ni migrar el esquema.
    """
    conn = sqlite3.connect(f"file:{target}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _count_all_batches(target: Path, endpoint_id: str, radar_id: str) -> int:
    conn = _ro_conn(target)
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM radar_notification_batches "
            "WHERE endpoint_id = ? AND radar_id = ?",
            (endpoint_id, radar_id),
        ).fetchone()
    finally:
        conn.close()
    return int(row["n"])


def _count_attempts(target: Path, endpoint_id: str, radar_id: str) -> int:
    conn = _ro_conn(target)
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM radar_delivery_attempts a "
            "JOIN radar_notification_batches b "
            "  ON b.notification_id = a.notification_id "
            "WHERE b.endpoint_id = ? AND b.radar_id = ?",
            (endpoint_id, radar_id),
        ).fetchone()
    finally:
        conn.close()
    return int(row["n"])


def _pending_batches(
    target: Path, endpoint_id: str, radar_id: str
) -> list[dict[str, Any]]:
    conn = _ro_conn(target)
    try:
        rows = conn.execute(
            "SELECT notification_id, payload_json, status, not_before, attempt_count "
            "FROM radar_notification_batches "
            "WHERE endpoint_id = ? AND radar_id = ? AND status = 'pending' "
            "ORDER BY created_at, notification_id",
            (endpoint_id, radar_id),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


@dataclass(frozen=True)
class BaselineHistory:
    baseline_count: int
    non_baseline_count: int
    max_event_seq: int


def _baseline_history(target: Path, radar_id: str) -> BaselineHistory:
    """Snapshot único —una consulta, una conexión— del historial del radar.

    Los tres números se leen **juntos**, de la misma fila de agregación: separar
    el conteo del ``MAX(event_seq)`` en dos consultas —incluso sobre la misma
    conexión— dejaría una ventana entre ambas en la que un evento nuevo podría
    materializarse y hacer que el ``MAX`` ya no corresponda al conteo que se
    acaba de leer. Con una sola fila, ambos números describen exactamente el
    mismo instante.

    Deliberadamente no se limita a una corrida (``run_id``): un evento no-baseline
    persistido por una corrida *anterior* ya es una novedad ocurrida, y sigue
    siéndolo aunque la corrida de ahora no emita nada nuevo (porque ya quedó
    "visto" en el diff). Mirar solo la corrida actual dejaría una recuperación en
    dos pasos tragarse esa novedad; mirar el historial completo no.

    El ``max_event_seq`` de este snapshot es también el watermark que
    ``baseline_real_pilot`` pasa como ``expected_max_event_seq`` al encender: si
    algo escribe un evento entre este snapshot y el ``BEGIN IMMEDIATE`` de
    ``enable_without_backfill``, el compare-and-enable de esa primitiva detecta
    el cambio y revierte, en vez de plantar el cursor sobre un historial que ya
    dejó de ser el que aquí se leyó.
    """
    conn = _ro_conn(target)
    try:
        row = conn.execute(
            "SELECT "
            "  SUM(CASE WHEN kind = 'baseline' THEN 1 ELSE 0 END) AS baselines, "
            "  SUM(CASE WHEN kind <> 'baseline' THEN 1 ELSE 0 END) AS non_baselines, "
            "  COALESCE(MAX(event_seq), 0) AS max_seq "
            "FROM radar_events WHERE radar_id = ?",
            (radar_id,),
        ).fetchone()
    finally:
        conn.close()
    return BaselineHistory(
        baseline_count=int(row["baselines"] or 0),
        non_baseline_count=int(row["non_baselines"] or 0),
        max_event_seq=int(row["max_seq"]),
    )


def _cursor_seq(target: Path, endpoint_id: str, radar_id: str) -> int | None:
    conn = _ro_conn(target)
    try:
        row = conn.execute(
            "SELECT last_examined_seq FROM radar_notification_cursors "
            "WHERE endpoint_id = ? AND radar_id = ?",
            (endpoint_id, radar_id),
        ).fetchone()
    finally:
        conn.close()
    return int(row["last_examined_seq"]) if row else None


def _endpoint_recipient(target: Path, endpoint_id: str) -> str | None:
    conn = _ro_conn(target)
    try:
        row = conn.execute(
            "SELECT external_recipient_id FROM notification_endpoints "
            "WHERE endpoint_id = ?",
            (endpoint_id,),
        ).fetchone()
    finally:
        conn.close()
    return row["external_recipient_id"] if row else None


def _notification_report(target: Path, notification_id: str) -> dict[str, Any]:
    conn = _ro_conn(target)
    try:
        batch = conn.execute(
            "SELECT status, attempt_count FROM radar_notification_batches "
            "WHERE notification_id = ?",
            (notification_id,),
        ).fetchone()
        pmid = conn.execute(
            "SELECT provider_message_id FROM radar_delivery_attempts "
            "WHERE notification_id = ? AND provider_message_id IS NOT NULL "
            "ORDER BY attempt_number DESC LIMIT 1",
            (notification_id,),
        ).fetchone()
    finally:
        conn.close()
    return {
        "notification_id": notification_id,
        "status": batch["status"] if batch else None,
        "attempt_count": int(batch["attempt_count"]) if batch else None,
        "provider_message_id": pmid["provider_message_id"] if pmid else None,
    }


# ── Resolución del par (radar, endpoint) del piloto ───────────────────────────

def _resolve_enabled_pair(target: Path) -> tuple[Any, str]:
    """Radar Adulto Mayor + endpoint de su preferencia, exigida encendida.

    Cardinalidad estricta en dos pasos: primero **exactamente una** preferencia
    en total para el radar (vía ``_resolve_pilot_preference``, la misma regla que
    usa baseline), y solo entonces se exige que esa única preferencia esté
    encendida. Filtrar por habilitadas *antes* de contar dejaría pasar un estado
    imposible —dos preferencias, una encendida y otra apagada— como si fuera el
    único par válido del piloto; contar primero lo trata como la ambigüedad que es.
    """
    radar, preference = _resolve_pilot_preference(target)
    if not preference.is_enabled:
        raise PilotRealError(SAFE_REAL_AMBIGUOUS_PILOT)
    return radar, preference.endpoint_id


def _resolve_pilot_preference(target: Path) -> tuple[Any, Any]:
    """Radar Adulto Mayor + su **única** preferencia (encendida o no).

    Para baseline/inspección: baseline necesita la preferencia apagada previa a
    encenderla, así que no puede filtrar por ``enabled``.
    """
    radars = RadarStore(target)
    radar = radars.get_radar_by_name(REAL_PILOT_OWNER_KEY, REAL_PILOT_RADAR_NAME)
    if radar is None:
        raise PilotRealError(SAFE_REAL_NO_PILOT)
    prefs = PreferenceStore(target).list_preferences_for_radar(radar.id)
    if len(prefs) != 1:
        raise PilotRealError(SAFE_REAL_AMBIGUOUS_PILOT)
    return radar, prefs[0]


# ── 1. setup-real-pilot (idempotente, sin encender, con detección de drift) ────

def _ensure_endpoint(endpoints: EndpointStore, principal: str, chat_id: str) -> str:
    """Endpoint Telegram exacto de ese chat, idempotente y sin retargetear.

    La identidad es el ``address_hash`` canónico (``find_endpoint``), nunca
    ``last4``: dos chats con los mismos cuatro dígitos son endpoints distintos. Si
    ya existe **este** chat, se reutiliza; si existe otro endpoint Telegram (otro
    chat) o el destino verificado no coincide, es drift y se falla cerrado —jamás
    se apunta el endpoint existente a un chat nuevo.
    """
    match = endpoints.find_endpoint(principal, "telegram", chat_id)
    if match is not None:
        if match.is_revoked or match.external_recipient_id not in (None, chat_id):
            raise PilotRealError(SAFE_REAL_ENDPOINT_DRIFT)
        endpoints.mark_verified(match.endpoint_id, external_recipient_id=chat_id)
        endpoint_id = match.endpoint_id
    else:
        others = [
            e for e in endpoints.list_endpoints(principal)
            if e.channel == "telegram" and not e.is_revoked
        ]
        if others:
            # Existe un endpoint Telegram para otro chat: retargetearlo cambiaría el
            # destinatario. Se rechaza en vez de reescribir a quién se le escribe.
            raise PilotRealError(SAFE_REAL_ENDPOINT_DRIFT)
        endpoint_id = endpoints.create_endpoint(
            principal_id=principal, channel="telegram", address=chat_id
        )
        endpoints.mark_verified(endpoint_id, external_recipient_id=chat_id)
    # Elegibilidad completa (enabled, verificado, recipient, no revocado, principal
    # activo): un endpoint exacto pero pausado o de un principal suspendido es drift,
    # no algo que setup deba reactivar en silencio.
    if not endpoints.is_eligible(endpoint_id):
        raise PilotRealError(SAFE_REAL_ENDPOINT_NOT_ELIGIBLE)
    return endpoint_id


def _ensure_radar(radars: RadarStore) -> str:
    """Radar Adulto Mayor idempotente; drift de frame/dataset falla cerrado."""
    expected = json.loads(adulto_mayor_query_frame_json())
    existing = radars.get_radar_by_name(REAL_PILOT_OWNER_KEY, REAL_PILOT_RADAR_NAME)
    if existing is None:
        return radars.create_radar(
            name=REAL_PILOT_RADAR_NAME,
            owner=REAL_PILOT_OWNER_KEY,
            query_frame_json=adulto_mayor_query_frame_json(),
            dataset=REAL_PILOT_DATASET,
        )
    # Equivalencia canónica: comparar los objetos, no el texto (orden de claves).
    # Un frame guardado ilegible o no-objeto es drift, no un crash: se traduce a la
    # razón segura en vez de propagar un JSONDecodeError.
    try:
        current = json.loads(existing.query_frame_json)
    except (json.JSONDecodeError, TypeError):
        raise PilotRealError(SAFE_REAL_RADAR_DRIFT) from None
    if not isinstance(current, dict) or current != expected:
        raise PilotRealError(SAFE_REAL_RADAR_DRIFT)
    if existing.dataset != REAL_PILOT_DATASET:
        raise PilotRealError(SAFE_REAL_RADAR_DRIFT)
    return existing.id


def _ensure_preference(preferences: PreferenceStore, radar_id: str, endpoint: str) -> str:
    """Preferencia per_run **apagada** idempotente; drift de config falla cerrado.

    Nace apagada (``create_preference`` no enciende) y setup **no** la enciende:
    encenderla es trabajo exclusivo de ``baseline-real-pilot``, que primero planta
    el cursor. Si ya existe, se verifica la config exacta sin tocar ``enabled`` ni
    el cursor.
    """
    all_prefs = preferences.list_preferences_for_radar(radar_id)
    others = [p for p in all_prefs if p.endpoint_id != endpoint]
    if others:
        # El piloto es un solo par (radar, endpoint). Una preferencia del radar
        # hacia otro endpoint es ambigüedad: no se crea una segunda encima de ella
        # ni se deja que ``status`` la descubra luego.
        raise PilotRealError(SAFE_REAL_AMBIGUOUS_PILOT)
    existing = [p for p in all_prefs if p.endpoint_id == endpoint]
    if not existing:
        return preferences.create_preference(
            radar_id=radar_id,
            endpoint_id=endpoint,
            frequency=FREQUENCY_PER_RUN,
            quiet_hours_start=REAL_PILOT_QUIET_HOURS_START,
            quiet_hours_end=REAL_PILOT_QUIET_HOURS_END,
            timezone=REAL_PILOT_TIMEZONE,
            max_items=REAL_PILOT_MAX_ITEMS,
        )
    pref = existing[0]
    drift = (
        pref.frequency != FREQUENCY_PER_RUN
        or pref.timezone != REAL_PILOT_TIMEZONE
        or pref.quiet_hours_start != REAL_PILOT_QUIET_HOURS_START
        or pref.quiet_hours_end != REAL_PILOT_QUIET_HOURS_END
        or pref.max_items != REAL_PILOT_MAX_ITEMS
    )
    if drift:
        raise PilotRealError(SAFE_REAL_PREFERENCE_DRIFT)
    return pref.preference_id


def setup_real_pilot(
    *,
    output: Callable[[str], Any] = print,
) -> dict[str, str]:
    """Crea (idempotentemente) principal, endpoint verificado, radar y preferencia.

    La preferencia nace **apagada**: encenderla es trabajo de baseline. Ejecutable
    dos veces sin duplicar nada; ante drift de radar/preferencia/endpoint falla
    cerrado sin corregir en silencio ni cambiar el destinatario. El token, la
    allowlist y la clave HMAC del masker viven exclusivamente en el entorno, y la
    clave se valida **antes** de crear o abrir la base.
    """
    target = _pilot_db(require_exists=False)

    config = TelegramConfiguration.from_environment()
    if len(config.allowed_chat_ids) != 1:
        raise PilotRealError(SAFE_REAL_CHAT_ID_INVALID)
    chat_id = next(iter(config.allowed_chat_ids))
    # Antes de tocar el disco: sin clave HMAC no se materializa nada.
    masking_key = _require_masking_key()

    identity = IdentityStore(target)
    endpoints = EndpointStore(target, masker=Masker.from_key(masking_key))
    preferences = PreferenceStore(target)
    radars = RadarStore(target)

    principal_row = identity.get_principal_by_email(REAL_PILOT_PRINCIPAL_EMAIL)
    principal = (
        principal_row.principal_id
        if principal_row is not None
        else identity.create_principal(REAL_PILOT_PRINCIPAL_EMAIL)
    )
    identity.link_owner_key(REAL_PILOT_OWNER_KEY, principal)

    endpoint = _ensure_endpoint(endpoints, principal, chat_id)
    radar = _ensure_radar(radars)
    preference_id = _ensure_preference(preferences, radar, endpoint)

    result = {
        "principal_id": principal,
        "endpoint_id": endpoint,
        "radar_id": radar,
        "preference_id": preference_id,
    }
    output(json.dumps(result, ensure_ascii=False))
    return result


# ── 2. baseline-real-pilot (baseline sin posibilidad de batch) ────────────────

def baseline_real_pilot(
    *,
    deps: Any,
    output: Callable[[str], Any] = print,
) -> dict[str, Any]:
    """Materializa el baseline silencioso: no puede producir un batch, por diseño.

    No corre el ``NotificationSelector``. En su lugar: corre el radar (que en la
    primera corrida solo emite un evento ``baseline``), verifica que **todo el
    historial** de eventos del radar es baseline-only, y **luego** enciende la
    preferencia con un **compare-and-enable** —``enable_without_backfill`` recibe
    ``expected_max_event_seq`` del mismo snapshot que validó el historial, y
    revierte entero si el ``MAX(event_seq)`` cambió bajo su lock—. Como la
    preferencia estaba apagada y nunca corre el selector, no existe forma de
    materializar un batch.

    La verificación mira el historial completo (``_baseline_history``), no solo
    los eventos de la corrida en curso: un evento no-baseline de una corrida
    *anterior* sigue siendo una novedad ocurrida aunque la corrida de ahora no
    emita nada nuevo (porque ya quedó "visto" en el diff). Se comprueba **dos
    veces**: antes de ``run_radar`` (fail closed sin gastar una consulta a SECOP
    si el historial ya está contaminado) y después de la corrida exitosa.

    Entre esa segunda lectura y el ``enable_without_backfill`` sigue existiendo
    una ventana de dos transacciones separadas: si otro proceso (una segunda
    invocación concurrente del comando) materializa un evento justo ahí, el
    watermark que se validó ya no es el vigente. El ``expected_max_event_seq``
    cierra esa ventana: ``enable_without_backfill`` relee el ``MAX`` bajo su
    propio lock y compara contra el que aquí se leyó, en la misma transacción que
    escribe cursor y ``enabled``. Un mismatch (``EventWatermarkChangedError``) se
    traduce a una razón segura del piloto —no se enciende, no se planta el cursor
    sobre eventos que este snapshot nunca vio—.

    Recuperable ante crash entre ``run_radar`` y el encendido **solo** cuando el
    historial sigue siendo baseline-only: una segunda corrida idéntica no agrega
    eventos nuevos (ya están en "visto"), el historial sigue en baselines>=1 y
    non_baselines==0, y se enciende sin backfill.
    """
    target = _pilot_db(require_exists=True)
    radar, preference = _resolve_pilot_preference(target)
    endpoint_id = preference.endpoint_id

    # Estado pre-baseline esperado, verificado **antes** de la red.
    if preference.is_enabled:
        raise PilotRealError(SAFE_REAL_BASELINE_ALREADY_DONE)
    if _count_all_batches(target, endpoint_id, radar.id) != 0:
        raise PilotRealError(SAFE_REAL_BASELINE_PRECONDITION)
    if _count_attempts(target, endpoint_id, radar.id) != 0:
        raise PilotRealError(SAFE_REAL_BASELINE_PRECONDITION)

    # Fail closed ANTES de la red: si el historial ya está contaminado (una
    # novedad de un intento anterior sigue sin resolverse), no hay razón para
    # gastar una consulta a SECOP que de todas formas no se aprovecharía.
    pre_history = _baseline_history(target, radar.id)
    if pre_history.non_baseline_count > 0:
        raise PilotRealError(SAFE_REAL_BASELINE_NON_BASELINE)

    run = run_radar(deps, RadarStore(target), radar)
    # Validar el éxito **antes** de leer eventos o encender: un timeout, un error o
    # un estado de control (already_running/lease_lost/not_found) nunca puede dar por
    # terminado un baseline que no ocurrió. Ante fallo, la preferencia sigue apagada
    # y el cursor intacto —no se llega a ``enable_without_backfill``—.
    _require_successful_run(run)

    history = _baseline_history(target, radar.id)
    if history.non_baseline_count > 0 or history.baseline_count == 0:
        # Cualquier novedad histórica, o ningún baseline todavía: en ambos casos
        # no se enciende. Fail closed sin tocar estados a mano; una novedad ya
        # persistida exige revisión o recrear la base del piloto, no se "resuelve"
        # moviendo el cursor por encima de ella.
        raise PilotRealError(SAFE_REAL_BASELINE_NON_BASELINE)

    try:
        PreferenceStore(target).enable_without_backfill(
            preference.preference_id,
            expected_max_event_seq=history.max_event_seq,
        )
    except EventWatermarkChangedError:
        # Alguien materializó un evento entre este snapshot y el lock de
        # enable_without_backfill: no se enciende sobre un historial que ya
        # cambió. La preferencia queda apagada y el cursor, intacto.
        raise PilotRealError(SAFE_REAL_BASELINE_CHANGED_BEFORE_ENABLE) from None

    # El cursor plantado debe ser exactamente el watermark que se validó y que el
    # compare-and-enable confirmó vigente — no un MAX releído ahora, que podría
    # ya ser mayor por actividad legítima posterior al commit.
    cursor = _cursor_seq(target, endpoint_id, radar.id)
    if cursor != history.max_event_seq:
        raise PilotRealError(SAFE_REAL_BASELINE_CURSOR_MISMATCH)
    if (_count_all_batches(target, endpoint_id, radar.id) != 0
            or _count_attempts(target, endpoint_id, radar.id) != 0):
        raise PilotRealError(SAFE_REAL_BASELINE_PRODUCED_BATCH)

    summary = {
        "run_outcome": run.outcome,
        "baseline_events": history.baseline_count,
        "cursor": cursor,
        "max_event_seq": history.max_event_seq,
        "batches": 0,
        "attempts": 0,
    }
    output(json.dumps(summary, ensure_ascii=False))
    return summary


# ── 3. run-real-pilot (preview antes de nueva consulta) ───────────────────────

def _render_frozen(payload_json: str) -> tuple[str, int, str]:
    """Renderiza el payload congelado sin mutarlo. Devuelve (texto, bytes, versión)."""
    try:
        payload = json.loads(payload_json)
        rendered = render_payload(payload)
    except Exception:
        # ``from None`` corta el encadenamiento: el mensaje crudo cita el payload,
        # que puede llevar identificadores del destinatario.
        raise PilotRealError(SAFE_REAL_PAYLOAD_UNREADABLE) from None
    version = payload.get("version")
    if version != PAYLOAD_VERSION_V3:
        raise PilotRealError(SAFE_REAL_PAYLOAD_UNEXPECTED_VERSION)
    return rendered.text, rendered.byte_count, version


def _preview(batch: dict[str, Any], *, run_outcome: str | None,
             output: Callable[[str], Any]) -> dict[str, Any]:
    text, byte_count, version = _render_frozen(batch["payload_json"])
    output(text)
    summary = {
        "outcome": "PREVIEW_READY",
        "run_outcome": run_outcome,
        "notification_id": batch["notification_id"],
        "payload_version": version,
        "byte_count": byte_count,
        "attempt_count": int(batch["attempt_count"]),
    }
    output(json.dumps(summary, ensure_ascii=False))
    return summary


def run_real_pilot(
    *,
    deps: Any,
    now: datetime | None = None,
    output: Callable[[str], Any] = print,
) -> dict[str, Any]:
    """Previsualiza el batch nuevo **sin enviar**, consultando SECOP solo si hace falta.

    Orden normativo: primero inspecciona los pending del par; con más de uno falla
    cerrado **sin** llamar a SECOP; con exactamente uno valida/renderiza ese payload
    y lo muestra **sin** consultar SECOP; con cero corre run_radar + selector y
    vuelve a inspeccionar. No llama al proveedor ni modifica el payload congelado.
    """
    target = _pilot_db(require_exists=True)
    radar, endpoint_id = _resolve_enabled_pair(target)

    pending = _pending_batches(target, endpoint_id, radar.id)
    if len(pending) > 1:
        raise PilotRealError(SAFE_REAL_MULTIPLE_READY)
    if len(pending) == 1:
        return _preview(pending[0], run_outcome=None, output=output)

    moment = _require_aware(now or _utc_now())
    run = run_radar(deps, RadarStore(target), radar)
    # Validar el éxito **antes** del selector: un timeout no es "no hubo novedades",
    # es "no sabemos". Sin esto, un fallo devolvería NO_NOVELTY como si la ausencia
    # de batch fuera una respuesta, en vez de un desconocido.
    _require_successful_run(run)
    NotificationSelector(target).select_pair(endpoint_id, radar.id, now=moment)

    pending = _pending_batches(target, endpoint_id, radar.id)
    if not pending:
        summary = {"outcome": "NO_NOVELTY", "run_outcome": run.outcome}
        output(json.dumps(summary, ensure_ascii=False))
        return summary
    if len(pending) > 1:
        raise PilotRealError(SAFE_REAL_MULTIPLE_READY)
    return _preview(pending[0], run_outcome=run.outcome, output=output)


# ── 4. send-ready-pilot (preflight + envío dirigido) ──────────────────────────

def send_ready_pilot(
    *,
    confirm_send: bool,
    provider: Any = None,
    worker_factory: Callable[..., NotificationWorker] = NotificationWorker,
    clock: Callable[[], datetime] | None = None,
    output: Callable[[str], Any] = print,
) -> dict[str, Any]:
    """Despacha exactamente el batch ya congelado, tras un preflight completo.

    **Antes** de construir ``TelegramProvider`` o ``NotificationWorker`` valida:
    ``--confirm-send``; exactamente un pending del par; payload JSON legible;
    versión v3; ``endpoint_id``/``radar_id``/``channel`` del payload coherentes;
    render bajo presupuesto; allowlist compatible con el ``external_recipient_id``
    congelado. Cualquier fallo → cero llamadas al proveedor, cero attempts nuevos,
    batch sin mutar. Luego corre el worker **dirigido** a ese ``notification_id``:
    no re-consulta SECOP ni reconstruye el payload, y un segundo intento no reenvía
    (el batch ya quedó ``sent`` y deja de estar ``pending``).
    """
    target = _pilot_db(require_exists=True)
    if not confirm_send:
        raise PilotRealError(SAFE_REAL_CONFIRM_REQUIRED)

    radar, endpoint_id = _resolve_enabled_pair(target)
    pending = _pending_batches(target, endpoint_id, radar.id)
    if len(pending) != 1:
        raise PilotRealError(SAFE_REAL_NOT_EXACTLY_ONE_READY)
    batch = pending[0]
    notification_id = batch["notification_id"]

    try:
        payload = json.loads(batch["payload_json"])
        if not isinstance(payload, dict):
            raise ValueError("payload no es objeto")
    except Exception:
        raise PilotRealError(SAFE_REAL_PAYLOAD_UNREADABLE) from None
    if payload.get("version") != PAYLOAD_VERSION_V3:
        raise PilotRealError(SAFE_REAL_PAYLOAD_UNEXPECTED_VERSION)
    if (payload.get("endpoint_id") != endpoint_id
            or payload.get("radar_id") != radar.id
            or payload.get("channel") != "telegram"):
        raise PilotRealError(SAFE_REAL_PAYLOAD_MISASSOCIATED)
    try:
        rendered = render_payload(payload, channel="telegram")
    except Exception:
        raise PilotRealError(SAFE_REAL_PAYLOAD_UNREADABLE) from None
    budget = CHANNEL_BYTE_BUDGETS["telegram"]
    if rendered.byte_count > budget:
        raise PilotRealError(SAFE_REAL_PAYLOAD_OVER_BUDGET)

    # Allowlist **exactamente** el destinatario congelado: no basta pertenencia. El
    # piloto tiene un único destinatario, así que una allowlist vacía, con otro chat
    # o con destinatarios adicionales falla aquí, antes de construir provider/worker.
    recipient = _endpoint_recipient(target, endpoint_id)
    allowlist = TelegramConfiguration.from_environment().allowed_chat_ids
    if not allowlist:
        raise PilotRealError(SAFE_REAL_ALLOWLIST_REQUIRED)
    if not recipient or allowlist != frozenset({recipient}):
        raise PilotRealError(SAFE_REAL_ALLOWLIST_MISMATCH)

    # Recién aquí se construyen proveedor y worker: todo lo anterior fue preflight.
    selected_provider = provider if provider is not None else TelegramProvider()
    factory_kwargs: dict[str, Any] = {} if clock is None else {"clock": clock}
    worker = worker_factory(target, {"telegram": selected_provider}, **factory_kwargs)
    worker.run_once(
        dispatch_limit=1, reconcile_limit=0, sweep_limit=0,
        notification_id=notification_id,
    )

    report = _notification_report(target, notification_id)
    output(json.dumps(report, ensure_ascii=False))
    return report


# ── 5. status-real-pilot (preflight de solo lectura, limitado al par) ──────────

def status_real_pilot(
    *,
    output: Callable[[str], Any] = print,
) -> dict[str, Any]:
    """Preflight de solo lectura. Exige exactamente una preferencia del piloto.

    Abre en ``mode=ro`` (sin store, sin migración). Todos los conteos se limitan a
    ``endpoint_id`` + ``radar_id`` del par. Nunca muestra token, dirección cruda ni
    secretos.
    """
    target = _pilot_db(require_exists=True)
    conn = _ro_conn(target)
    try:
        radar = conn.execute(
            "SELECT id, name, enabled, archived_at, last_run_at, next_run_after, "
            "       needs_attention "
            "  FROM radars "
            " WHERE owner = ? AND name = ? COLLATE NOCASE AND archived_at IS NULL",
            (REAL_PILOT_OWNER_KEY, REAL_PILOT_RADAR_NAME),
        ).fetchone()
        if radar is None:
            raise PilotRealError(SAFE_REAL_NO_PILOT)
        radar_id = radar["id"]

        prefs = conn.execute(
            "SELECT endpoint_id FROM radar_notification_preferences "
            "WHERE radar_id = ? ORDER BY created_at",
            (radar_id,),
        ).fetchall()
        if len(prefs) != 1:
            raise PilotRealError(SAFE_REAL_AMBIGUOUS_PILOT)
        endpoint_id = prefs[0]["endpoint_id"]

        last_run = conn.execute(
            "SELECT started_at, status, outcome, reason FROM radar_runs "
            "WHERE radar_id = ? ORDER BY started_at DESC LIMIT 1",
            (radar_id,),
        ).fetchone()

        cursor_row = conn.execute(
            "SELECT last_examined_seq, updated_at FROM radar_notification_cursors "
            "WHERE endpoint_id = ? AND radar_id = ?",
            (endpoint_id, radar_id),
        ).fetchone()

        batches_by_status: dict[str, int] = {}
        for row in conn.execute(
            "SELECT status, COUNT(*) AS n FROM radar_notification_batches "
            "WHERE endpoint_id = ? AND radar_id = ? GROUP BY status",
            (endpoint_id, radar_id),
        ):
            batches_by_status[row["status"]] = int(row["n"])

        attempts_by_outcome: dict[str, int] = {}
        for row in conn.execute(
            "SELECT COALESCE(a.outcome, '(open)') AS o, COUNT(*) AS n "
            "  FROM radar_delivery_attempts a "
            "  JOIN radar_notification_batches b "
            "    ON b.notification_id = a.notification_id "
            " WHERE b.endpoint_id = ? AND b.radar_id = ? GROUP BY o",
            (endpoint_id, radar_id),
        ):
            attempts_by_outcome[row["o"]] = int(row["n"])

        dispatching_residual = int(conn.execute(
            "SELECT COUNT(*) AS n FROM radar_delivery_attempts a "
            "  JOIN radar_notification_batches b "
            "    ON b.notification_id = a.notification_id "
            " WHERE b.endpoint_id = ? AND b.radar_id = ? "
            "   AND a.phase = 'dispatching' AND a.finished_at IS NULL",
            (endpoint_id, radar_id),
        ).fetchone()["n"])

        pmid_row = conn.execute(
            "SELECT a.provider_message_id FROM radar_delivery_attempts a "
            "  JOIN radar_notification_batches b "
            "    ON b.notification_id = a.notification_id "
            " WHERE b.endpoint_id = ? AND b.radar_id = ? "
            "   AND a.provider_message_id IS NOT NULL "
            " ORDER BY a.finished_at DESC LIMIT 1",
            (endpoint_id, radar_id),
        ).fetchone()
        last_pmid = pmid_row["provider_message_id"] if pmid_row else None
    finally:
        conn.close()

    status = {
        "db_path": str(target),
        "radar_id": radar_id,
        "radar_name": radar["name"],
        "radar_enabled": bool(radar["enabled"]),
        "radar_needs_attention": bool(radar["needs_attention"]),
        "last_run_at": radar["last_run_at"],
        "last_run": dict(last_run) if last_run else None,
        "cursor": dict(cursor_row) if cursor_row else None,
        "batches_by_status": batches_by_status,
        "attempts_by_outcome": attempts_by_outcome,
        "dispatching_residual": dispatching_residual,
        "last_provider_message_id": last_pmid,
    }
    output(json.dumps(status, ensure_ascii=False))
    return status
