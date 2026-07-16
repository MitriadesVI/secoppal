"""follows.py — "seguir proceso": watchlist de procesos individuales.

A diferencia de un radar de búsqueda (rastrea lo que matchea una query), aquí el
usuario sigue **procesos concretos** elegidos a mano desde su tarjeta. Se modela
como un **radar sintético** por usuario (``frequency='follows'`` — el nombre
``📌 Procesos seguidos`` es presentación, la identidad es la frecuencia) para
reutilizar intacta la maquinaria del radar:

- ``radar_seen_results`` guarda los procesos seguidos (clave estable + snapshot);
- ``radar_events`` es el feed (su FK a ``radars`` queda satisfecha);
- ``keys.py`` da la clave y el hash de cambio.

El scheduler (``scheduler.run_due_radars``) salta este radar en el lado query
(``is_due`` es False para 'follows') y en su lugar llama ``refresh_follows``.

Al seguir (``follow``) el snapshot queda vacío a propósito: la **primera** corrida
de ``refresh_follows`` establece la línea base en silencio (sin evento de cambio);
de ahí en adelante un cambio de estado/valor sí emite evento al feed.

``refresh_follows`` corre bajo el **mismo run lease** que ``run_radar`` y escribe
snapshot y eventos en **una** transacción, con ``UPDATE`` y nunca ``UPSERT``:
crear membresía es de ``follow()``, y un refresco que pudiera insertar
resucitaría lo que ``unfollow`` borró mientras había red. Ver
``docs/radar/FOLLOWS_LEASE_INVESTIGATION.md``.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.core.radar.keys import (
    extract_event_metadata,
    extract_fields,
    radar_result_key,
    snapshot_hash,
)
from app.core.radar.store import (
    FOLLOWS_FREQUENCY,
    FOLLOWS_RADAR_NAME,
    EventRecord,
    FollowsSeenUpdate,
    Radar,
    RadarExistsError,
    RadarStore,
    SeenRecord,
    is_canonical_follows_name,
)
from app.core.soql_builder import SoQLBuilder

logger = logging.getLogger(__name__)

# Identidad del radar sintético. Vive en ``store`` —es una regla de integridad de
# la tabla ``radars``— y se re-exporta aquí, que es la API que usan la UI y el
# scheduler. ``FOLLOWS_NAME`` es solo presentación: la identidad es la frecuencia.
FOLLOWS_NAME = FOLLOWS_RADAR_NAME

_FOLLOW_RUN_ID = "follow_manual"
# Centinela para ``radar_seen_results.last_run_id``: declara "esto no vino de una
# corrida", igual que ``_FOLLOW_RUN_ID``. Follows no crea filas en ``radar_runs``.
_REFRESH_RUN_ID = "follows_refresh"

# Estados de control de ``refresh_follows``. Ninguno es un error operativo: los
# fallos reales siguen siendo excepción y los captura el scheduler.
REFRESH_REFRESHED = "refreshed"
REFRESH_NO_ITEMS = "no_items"
REFRESH_ALREADY_RUNNING = "already_running"
REFRESH_LEASE_LOST = "lease_lost"

# Estados en los que el refresco no escribió nada **por concurrencia legítima**:
# el scheduler los cuenta como omisión, no como fallo. Una violación de
# invariante NO entra aquí: es un error y debe verse como tal.
REFRESH_SKIPPED = frozenset({REFRESH_ALREADY_RUNNING, REFRESH_LEASE_LOST})


class FollowsRadarError(RuntimeError):
    """Invariante del radar sintético violado. Nunca es un estado normal.

    Se resuelve fail-closed: la watchlist no opera sobre un radar del que no
    puede probar la identidad. Ninguna de estas condiciones modifica filas —
    reparar una base heredada es una decisión humana, no un efecto colateral.
    """


class FollowsRadarNameConflictError(FollowsRadarError):
    """Un radar de búsqueda preexistente ocupa el nombre del radar sintético.

    Solo puede darse en bases anteriores a la reserva de nombre de
    ``store.create_radar``. No se resuelve convirtiendo ni borrando el radar del
    usuario: se informa para que lo renombre.
    """


class FollowsRadarIntegrityError(FollowsRadarError):
    """La fila ``frequency='follows'`` del owner no tiene el nombre canónico.

    Base heredada: el ``UNIQUE (owner, name)`` no la protege contra un segundo
    follows, así que la watchlist no puede confiar en ser única.
    """


class FollowsRadarCardinalityError(FollowsRadarError):
    """El owner tiene más de un radar ``frequency='follows'``.

    Elegir uno partiría la watchlist en dos en silencio: unos seguidos vivirían
    en una fila y otros en la otra, y cada refresco vería medio mundo.
    """


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_follows_radar(store: RadarStore, owner: str) -> Radar | None:
    """El radar sintético canónico del owner, ``None`` si no existe, o error.

    Fail-closed sobre la cardinalidad: pide **hasta dos** filas y decide con las
    dos a la vista. Un ``LIMIT 1`` devolvería una fila cualquiera y ocultaría
    justo la corrupción que hay que detectar; entre operar sobre la watchlist
    equivocada y detenerse, la watchlist se detiene.
    """
    rows = store.list_radars_by_frequency(owner, FOLLOWS_FREQUENCY, limit=2)
    if not rows:
        return None
    if len(rows) > 1:
        raise FollowsRadarCardinalityError(
            f"{owner} tiene {len(rows)} radares de procesos seguidos "
            f"({', '.join(r.id for r in rows)}). Debe haber exactamente uno; "
            f"revisar manualmente cuál conservar."
        )
    radar = rows[0]
    if not is_canonical_follows_name(radar.name):
        raise FollowsRadarIntegrityError(
            f"El radar de seguidos de {owner} (id={radar.id}) se llama "
            f"'{radar.name}' y no '{FOLLOWS_NAME}'; su unicidad no está "
            f"garantizada. Renombrarlo para restaurar el invariante."
        )
    return radar


def get_or_create_follows_radar(store: RadarStore, owner: str) -> Radar:
    """Devuelve (creándolo si hace falta) el radar sintético de seguidos del owner.

    Resuelve por ``frequency``, no por nombre: un radar de búsqueda que llevara
    el nombre reservado no es la watchlist, y aceptarlo como tal metía los
    seguidos del owner en los ``radar_seen_results`` de esa búsqueda.

    La carrera de creación la decide el ``UNIQUE (owner, name)``: si otro proceso
    gana, ``create_radar`` levanta ``RadarExistsError`` y se re-lee la fila
    ganadora. Nunca INSERT OR REPLACE, nunca borrar y recrear: todos los callers
    concurrentes devuelven el mismo ``radar.id``.
    """
    existing = _resolve_follows_radar(store, owner)
    if existing is not None:
        return existing
    try:
        store.create_radar(
            name=FOLLOWS_NAME, owner=owner, query_frame_json="{}",
            description="Procesos seguidos individualmente", frequency=FOLLOWS_FREQUENCY,
            channel="web",
        )
    except RadarExistsError:
        pass  # carrera: otro hilo lo creó entre el get y el create
    radar = _resolve_follows_radar(store, owner)
    if radar is None:
        # El UNIQUE rechazó el INSERT y aun así no hay follows: el nombre lo
        # ocupa un radar de búsqueda de una base pre-reserva.
        raise FollowsRadarNameConflictError(
            f"El radar '{FOLLOWS_NAME}' de {owner} no se puede crear porque ya "
            f"existe un radar de búsqueda con ese nombre. Renómbralo para poder "
            f"seguir procesos."
        )
    return radar


def is_following(store: RadarStore, owner: str, key: str) -> bool:
    radar = get_or_create_follows_radar(store, owner)
    return key in store.get_seen(radar.id)


def list_follows(store: RadarStore, owner: str) -> list[dict]:
    """Procesos seguidos del owner, más recientes primero."""
    radar = get_or_create_follows_radar(store, owner)
    seen = store.get_seen(radar.id).values()
    return sorted(seen, key=lambda r: r.get("first_seen_at") or "", reverse=True)


def follow(store: RadarStore, owner: str, row: dict) -> bool:
    """Sigue el proceso de ``row`` (fila de tarjeta enriquecida con identidad).

    Devuelve ``False`` si la fila no trae clave estable (no se puede seguir), y
    ``True`` solo si el alta se materializó de verdad —creada ahora o ya
    existente—. Si el radar dejó de ser la watchlist canónica entre que se
    resolvió y que se fue a escribir, levanta ``FollowsRadarIntegrityError``:
    devolver ``True`` sin haber escrito le diría al usuario que sigue un proceso
    que nadie está vigilando.

    Idempotente: re-seguir no duplica ni vuelve a emitir evento.
    """
    dataset_id = row.get("dataset_id") or ""
    key = radar_result_key(row, dataset_id)
    if not key:
        return False

    radar = get_or_create_follows_radar(store, owner)

    fields = {
        "title": row.get("titulo", ""),
        "entity": row.get("entidad", ""),
        "value": row.get("valor", ""),
        "status": row.get("estado", ""),
        "urlproceso": row.get("url", ""),
        "referencia": (row.get("referencia_del_proceso")
                       or row.get("referencia_del_contrato") or ""),
        "contractor": row.get("contratista", ""),
    }
    # La tarjeta trae el contratista bajo su alias visible (``contratista``), no
    # bajo el nombre crudo de SECOP (``proveedor_adjudicado``) que busca la
    # proyección. Sin este rescate la membresía lo conserva y el evento —que es
    # lo inmutable— lo perdería. Solo aplica al contratista: la tarjeta también
    # trae ``fecha``, pero esa fecha puede ser publicación, apertura o recepción
    # (formatter._best_date_for_row la elige por cascada), así que promoverla a
    # ``closing_date`` afirmaría un cierre que nadie fijó. Ahí NULL es la verdad.
    event_metadata = extract_event_metadata(row, dataset_id)
    if event_metadata["contractor"] is None:
        event_metadata["contractor"] = fields["contractor"].strip() or None

    # snapshot vacío → la 1ª refresh establece la línea base sin emitir cambio.
    # Solo se aplica al **alta**: si ya se seguía, la transacción no toca la
    # fila, así que un re-follow no borra la línea base ya fijada (lo que
    # silenciaría el próximo cambio real al tratarlo como baseline).
    result = store.materialize_follow_if_absent(
        radar_id=radar.id,
        seen=SeenRecord(
            result_key=key, dataset=dataset_id, fields=fields, snapshot_hash="",
        ),
        event=EventRecord(
            kind="followed", ts=_now(), result_key=key, dataset=dataset_id,
            title=fields["title"], entity=fields["entity"], value=fields["value"],
            status=fields["status"], urlproceso=fields["urlproceso"],
            referencia=fields["referencia"],
            **event_metadata,
        ),
        run_id=_FOLLOW_RUN_ID,
    )
    if result.not_follows:
        raise FollowsRadarIntegrityError(
            f"El radar id={radar.id} de {owner} no es una lista de procesos "
            f"seguidos canónica; no se dio de alta '{key}'."
        )
    return True


def unfollow(store: RadarStore, owner: str, key: str) -> None:
    radar = get_or_create_follows_radar(store, owner)
    store.delete_seen(radar.id, key)


def _fetch_current_rows_batch(deps, dataset_id: str, keys: list[str]) -> list[dict]:
    """Trae el estado actual por id/referencia para un lote pequeño de claves."""
    if not keys:
        return []
    spec = SoQLBuilder.SPECS[dataset_id]
    id_field = "id_del_proceso" if dataset_id == SoQLBuilder.PROCESOS_DATASET else "id_contrato"
    ref_field = spec.reference or id_field
    fields = ", ".join(spec.select_fields)
    quoted = ",".join(f"'{SoQLBuilder._escape(k)}'" for k in keys)
    soql = (
        f"SELECT {fields} "
        f"WHERE {id_field} in({quoted}) OR {ref_field} in({quoted}) "
        f"LIMIT {max(len(keys) * 2, 50)}"
    )
    return list(deps.secop_client.query(dataset_id, soql) or [])


def fetch_current_rows_by_keys(
    deps,
    dataset_id: str,
    keys: list[str],
    *,
    batch_size: int = 100,
) -> list[dict]:
    """Trae filas actuales de SECOP por clave estable, en batches.

    Reutilizado por ``refresh_follows`` y por radares de búsqueda para revisar
    resultados ya vistos que salieron de la ventana reciente.
    """
    rows: list[dict] = []
    clean_keys = [k for k in keys if k]
    if not clean_keys:
        return rows
    batch_size = max(1, int(batch_size or 100))
    for i in range(0, len(clean_keys), batch_size):
        rows.extend(_fetch_current_rows_batch(deps, dataset_id, clean_keys[i:i + batch_size]))
    return rows


def _fetch_current_rows(deps, dataset_id: str, keys: list[str]) -> list[dict]:
    """Compat wrapper: trae el estado actual de los procesos seguidos."""
    return fetch_current_rows_by_keys(deps, dataset_id, keys)


def _summary(status: str, **over) -> dict:
    """Resumen del refresco. Es un dict (y no un dataclass) porque la UI y el
    scheduler ya lo consumen por clave; ``status`` es aditivo."""
    base = {"status": status, "checked": 0, "baseline": 0, "changed_state": 0,
            "changed_value": 0, "missing": 0, "events": 0, "lookup_errors": 0}
    return {**base, **over}


def _build_updates(deps, seen: dict[str, dict], owner: str) -> tuple[list[FollowsSeenUpdate], dict]:
    """Consulta SECOP y arma en memoria lo que se va a escribir. Sin transacción.

    Toda la red ocurre aquí: la materialización recibe datos ya cocinados. Un
    error de lookup **no** confirma desaparición — se cuenta y se sigue, sin
    borrar membresías ni emitir eventos falsos.
    """
    by_dataset: dict[str, list[str]] = {}
    for key, rec in seen.items():
        ds = rec.get("dataset") or SoQLBuilder.PROCESOS_DATASET
        by_dataset.setdefault(ds, []).append(key)

    ts = _now()
    # Solo lo que se sabe en la fase de red. baseline/changed_* NO se cuentan
    # aquí: una clave puede quedar descartada en la transacción (unfollow), y
    # entonces el conteo construido mentiría. Los aporta el materializador.
    counts = {"lookup_errors": 0}
    updates: list[FollowsSeenUpdate] = []
    found_keys: set[str] = set()

    for dataset_id, keys in by_dataset.items():
        try:
            rows = _fetch_current_rows(deps, dataset_id, keys)
        except Exception:
            logger.exception("refresh_follows: fallo al traer dataset=%s owner=%s",
                             dataset_id, owner)
            counts["lookup_errors"] += len(keys)
            continue

        for raw in rows:
            rk = radar_result_key(raw, dataset_id)
            # El SoQL busca `id in(...) OR referencia in(...)`: dos campos que
            # ambos identifican la fila, así que SECOP puede devolverla dos
            # veces. Sin este corte, un duplicado emite el evento dos veces.
            if rk is None or rk not in seen or rk in found_keys:
                continue
            found_keys.add(rk)
            stored = seen[rk]
            new_hash = snapshot_hash(raw, dataset_id)
            new_fields = extract_fields(raw, dataset_id)
            # Se proyecta una vez por fila y se congela en cada evento que salga
            # de ella: describe la fila observada AHORA, no el estado guardado.
            new_meta = extract_event_metadata(raw, dataset_id)
            old_hash = stored.get("snapshot_hash") or ""

            events: list[EventRecord] = []
            is_baseline = old_hash == ""
            if is_baseline:
                # Línea base silenciosa (ADR-035): se fija el snapshot sin
                # anunciar un cambio que nadie llegó a ver.
                pass
            elif new_hash != old_hash:
                old_status = stored.get("status") or ""
                old_value = stored.get("value") or ""
                if new_fields["status"] != old_status:
                    events.append(EventRecord(
                        kind="changed_state", ts=ts, dataset=dataset_id, result_key=rk,
                        title=new_fields["title"], entity=new_fields["entity"],
                        value=new_fields["value"], status=new_fields["status"],
                        prev_status=old_status, urlproceso=new_fields["urlproceso"],
                        referencia=new_fields["referencia"], **new_meta,
                    ))
                if new_fields["value"] != old_value:
                    events.append(EventRecord(
                        kind="changed_value", ts=ts, dataset=dataset_id, result_key=rk,
                        title=new_fields["title"], entity=new_fields["entity"],
                        value=new_fields["value"], status=new_fields["status"],
                        prev_value=old_value, urlproceso=new_fields["urlproceso"],
                        referencia=new_fields["referencia"], **new_meta,
                    ))
            else:
                continue  # sin cambio: nada que escribir

            updates.append(FollowsSeenUpdate(
                result_key=rk,
                # Versión de la membresía leída en este snapshot: si un
                # unfollow+follow la reemplaza durante la red, la fila nueva
                # tiene otro first_seen_at y este update no la toca.
                expected_first_seen_at=stored["first_seen_at"],
                fields=new_fields, snapshot_hash=new_hash,
                events=tuple(events), is_baseline=is_baseline,
            ))

    counts["missing"] = len(seen) - len(found_keys)
    return updates, counts


def _refresh_locked(deps, store: RadarStore, radar: Radar, owner: str, token: str) -> dict:
    """Refresco con el lease ya tomado: snapshot → red → memoria → transacción."""
    seen = store.get_seen(radar.id)
    if not seen:
        return _summary(REFRESH_NO_ITEMS)

    updates, counts = _build_updates(deps, seen, owner)
    checked = len(seen)

    # Última llamada remota hecha. De aquí en adelante solo se escribe.
    # seen_run_id ≠ el run_id de los eventos: el snapshot necesita distinguir
    # "de dónde vino este refresh" en su historial; el evento no tiene corrida
    # que auditar y su run_id es NULL siempre, sin excepción (ver
    # materialize_follows_refresh y docs/radar/FOLLOWS_LEASE_INVESTIGATION.md §F.2).
    materialized = store.materialize_follows_refresh(
        radar_id=radar.id, token=token, updates=updates,
        seen_run_id=_REFRESH_RUN_ID,
    )
    if materialized.lease_lost:
        logger.warning("refresh_follows: lease perdido durante la red owner=%s", owner)
        return _summary(REFRESH_LEASE_LOST, checked=checked)
    if materialized.not_follows:
        # No es concurrencia legítima: el radar que resolvimos como watchlist
        # dejó de serlo. Es un invariante roto y debe verse como error.
        raise FollowsRadarIntegrityError(
            f"El radar id={radar.id} de {owner} dejó de ser un radar de "
            f"procesos seguidos durante el refresco."
        )

    # Conteos de lo materializado: si un unfollow descartó una clave, su cambio
    # no se anuncia porque no se escribió.
    return _summary(
        REFRESH_REFRESHED, checked=checked,
        events=materialized.events, baseline=materialized.baseline,
        changed_state=materialized.changed_state,
        changed_value=materialized.changed_value,
        **counts,
    )


def refresh_follows(deps, store: RadarStore, owner: str) -> dict:
    """Revisa los procesos seguidos del owner y emite cambios al feed.

    Línea base silenciosa la 1ª vez (snapshot vacío). Después, un cambio de
    estado o de valor emite ``changed_state`` / ``changed_value``.

    Corre bajo el mismo lease que ``run_radar``: el radar sintético es una fila
    de ``radars`` como cualquier otra y la exclusión que hace falta es
    exactamente por esa fila. Sin él, dos schedulers consultan a la vez y
    duplican los eventos. Owners distintos tienen radares distintos y no se
    bloquean entre sí.
    """
    radar = get_or_create_follows_radar(store, owner)
    claim = store.try_acquire_run_lease(radar.id)
    if not claim.acquired:
        # Otro proceso lo está refrescando: no es un error. No se consulta
        # SECOP, no se escribe seen y no se insertan eventos.
        logger.info("refresh_follows: ya se está refrescando owner=%s; se omite", owner)
        return _summary(REFRESH_ALREADY_RUNNING)
    try:
        return _refresh_locked(deps, store, radar, owner, claim.token)
    finally:
        store.release_run_lease(radar.id, claim.token)
