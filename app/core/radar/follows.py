"""follows.py — "seguir proceso": watchlist de procesos individuales.

A diferencia de un radar de búsqueda (rastrea lo que matchea una query), aquí el
usuario sigue **procesos concretos** elegidos a mano desde su tarjeta. Se modela
como un **radar sintético** por usuario (``frequency='follows'``, nombre
``📌 Procesos seguidos``) para reutilizar intacta la maquinaria del radar:

- ``radar_seen_results`` guarda los procesos seguidos (clave estable + snapshot);
- ``radar_events`` es el feed (su FK a ``radars`` queda satisfecha);
- ``keys.py`` da la clave y el hash de cambio.

El scheduler (``scheduler.run_due_radars``) salta este radar en el lado query
(``is_due`` es False para 'follows') y en su lugar llama ``refresh_follows``.

Al seguir (``follow``) el snapshot queda vacío a propósito: la **primera** corrida
de ``refresh_follows`` establece la línea base en silencio (sin evento de cambio);
de ahí en adelante un cambio de estado/valor sí emite evento al feed.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.core.radar.keys import extract_fields, radar_result_key, snapshot_hash
from app.core.radar.store import Radar, RadarExistsError, RadarStore
from app.core.soql_builder import SoQLBuilder

logger = logging.getLogger(__name__)

FOLLOWS_FREQUENCY = "follows"
FOLLOWS_NAME = "📌 Procesos seguidos"
_FOLLOW_RUN_ID = "follow_manual"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_or_create_follows_radar(store: RadarStore, owner: str) -> Radar:
    """Devuelve (creándolo si hace falta) el radar sintético de seguidos del owner."""
    existing = store.get_radar_by_name(owner, FOLLOWS_NAME)
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
    return store.get_radar_by_name(owner, FOLLOWS_NAME)


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

    Devuelve False si la fila no trae clave estable (no se puede seguir).
    Idempotente: re-seguir no duplica ni vuelve a emitir evento.
    """
    dataset_id = row.get("dataset_id") or ""
    key = radar_result_key(row, dataset_id)
    if not key:
        return False

    radar = get_or_create_follows_radar(store, owner)
    already = key in store.get_seen(radar.id)

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
    # snapshot vacío → la 1ª refresh establece la línea base sin emitir cambio.
    store.upsert_seen(
        radar_id=radar.id, result_key=key, dataset=dataset_id, fields=fields,
        snapshot_hash="", run_id=_FOLLOW_RUN_ID,
    )

    if not already:
        store.record_events([{
            "radar_id": radar.id, "owner": owner, "radar_name": FOLLOWS_NAME,
            "run_id": _FOLLOW_RUN_ID, "ts": _now(), "kind": "followed",
            "result_key": key, "dataset": dataset_id, "title": fields["title"],
            "entity": fields["entity"], "value": fields["value"],
            "status": fields["status"], "urlproceso": fields["urlproceso"],
            "referencia": fields["referencia"],
        }])
    return True


def unfollow(store: RadarStore, owner: str, key: str) -> None:
    radar = get_or_create_follows_radar(store, owner)
    store.delete_seen(radar.id, key)


def _fetch_current_rows(deps, dataset_id: str, keys: list[str]) -> list[dict]:
    """Trae el estado actual de los procesos seguidos por id/referencia, en lote."""
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


def refresh_follows(deps, store: RadarStore, owner: str) -> dict:
    """Revisa los procesos seguidos del owner y emite cambios al feed.

    Línea base silenciosa la 1ª vez (snapshot vacío). Después, un cambio de
    estado o de valor emite ``changed_state`` / ``changed_value``.
    """
    radar = get_or_create_follows_radar(store, owner)
    seen = store.get_seen(radar.id)
    summary = {"checked": len(seen), "baseline": 0, "changed_state": 0,
               "changed_value": 0, "missing": 0}
    if not seen:
        return summary

    # Agrupar claves por dataset.
    by_dataset: dict[str, list[str]] = {}
    for key, rec in seen.items():
        ds = rec.get("dataset") or SoQLBuilder.PROCESOS_DATASET
        by_dataset.setdefault(ds, []).append(key)

    ts = _now()
    events: list[dict] = []
    found_keys: set[str] = set()

    for dataset_id, keys in by_dataset.items():
        try:
            rows = _fetch_current_rows(deps, dataset_id, keys)
        except Exception:
            logger.exception("refresh_follows: fallo al traer dataset=%s owner=%s",
                             dataset_id, owner)
            continue

        for raw in rows:
            rk = radar_result_key(raw, dataset_id)
            if rk is None or rk not in seen:
                continue
            found_keys.add(rk)
            stored = seen[rk]
            new_hash = snapshot_hash(raw, dataset_id)
            new_fields = extract_fields(raw, dataset_id)
            old_hash = (stored.get("snapshot_hash") or "")

            base_event = {
                "radar_id": radar.id, "owner": owner, "radar_name": FOLLOWS_NAME,
                "run_id": radar.id, "ts": ts, "result_key": rk, "dataset": dataset_id,
                "title": new_fields["title"], "entity": new_fields["entity"],
                "value": new_fields["value"], "status": new_fields["status"],
                "urlproceso": new_fields["urlproceso"], "referencia": new_fields["referencia"],
            }

            if old_hash == "":
                summary["baseline"] += 1
            elif new_hash != old_hash:
                old_status = (stored.get("status") or "")
                old_value = (stored.get("value") or "")
                if new_fields["status"] != old_status:
                    summary["changed_state"] += 1
                    events.append({**base_event, "kind": "changed_state",
                                   "prev_status": old_status})
                if new_fields["value"] != old_value:
                    summary["changed_value"] += 1
                    events.append({**base_event, "kind": "changed_value",
                                   "prev_value": old_value})

            if old_hash == "" or new_hash != old_hash:
                store.upsert_seen(
                    radar_id=radar.id, result_key=rk, dataset=dataset_id,
                    fields=new_fields, snapshot_hash=new_hash, run_id=radar.id,
                )

    summary["missing"] = len(seen) - len(found_keys)
    if events:
        store.record_events(events)
    return summary
