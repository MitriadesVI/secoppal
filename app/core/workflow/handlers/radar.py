"""Radar command handler — guardar / listar / ejecutar radares.

Intercepta tres comandos conversacionales (ver ``app.core.radar.commands``)
antes del flujo normal:

- **guardar** — serializa el QueryFrame del último turno de búsqueda como un
  radar nuevo.
- **listar**  — muestra los radares del usuario con su última revisión.
- **ejecutar**— corre un radar y devuelve el digest asesor.

Devuelve ``None`` (cae al flujo normal) cuando no hay ``chat_id``, no hay
``radar_store`` o el texto no es un comando de radar. Sigue el patrón de
``handle_reset``: comando de estado/acción, sin tocar la red SECOP salvo en
``ejecutar`` (que sí consulta vía el runner).
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.core.radar.builder import has_scope_or_topic, save_radar_from_params
from app.core.radar.commands import LIST, RUN, SAVE, parse_radar_command, radar_owner
from app.core.radar.runner import run_radar
from app.core.radar.store import RadarExistsError
from app.core.workflow.results import empty_result

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


def handle_radar_command(deps: "WorkflowDeps", ctx: "HandlerContext") -> dict | None:
    if not ctx.chat_id or deps.radar_store is None:
        return None
    cmd = parse_radar_command(ctx.user_query)
    if cmd is None:
        return None

    store = deps.radar_store
    owner = radar_owner(ctx.user_identity, ctx.chat_id)

    if cmd.kind == LIST:
        return _do_list(store, owner)
    if cmd.kind == SAVE:
        return _do_save(deps, ctx, store, owner, cmd.name)
    if cmd.kind == RUN:
        return _do_run(deps, store, owner, cmd.name)
    return None


def _resp(text: str, route_reason: str) -> dict:
    return empty_result(response=text, route_reason=route_reason, followup=True)


def _last_search_params(deps: "WorkflowDeps", chat_id: str) -> dict | None:
    """Params resueltos del último turno que fue una búsqueda real.

    normal_flow persiste ``resolved_params`` en ``Turn.parsed_params``; se
    recorre el historial de atrás hacia adelante y se toma el primero que
    tenga alcance o tema (descarta turnos sin búsqueda).
    """
    history = deps.conv_store.get_history(chat_id, last_n=10)
    for turn in reversed(history):
        params = turn.parsed_params or {}
        if has_scope_or_topic(params):
            return params
    return None


# ---------------------------------------------------------------------------
# guardar
# ---------------------------------------------------------------------------

def _do_save(deps, ctx, store, owner, name) -> dict:
    params = _last_search_params(deps, ctx.chat_id)
    if params is None:
        return _resp(
            "No encontré una búsqueda reciente para guardar como radar. "
            "Haz primero una búsqueda (por ejemplo: «procesos de PAE en Atlántico») "
            "y luego dime «guardar esta búsqueda como Radar PAE Atlántico».",
            route_reason="radar_save_no_context",
        )

    try:
        radar_id = save_radar_from_params(
            store, owner=owner, name=name, params=params, channel=ctx.channel,
        )
    except RadarExistsError:
        return _resp(
            f"Ya tienes un radar llamado «{name}». Usa otro nombre o "
            f"dime «ejecutar radar {name}» para revisarlo.",
            route_reason="radar_save_duplicate",
        )

    # Primera revisión inmediata: establece la línea base y puebla el feed,
    # para que el radar no nazca vacío.
    radar = store.get_radar(radar_id)
    total = 0
    if radar is not None:
        try:
            total = run_radar(deps, store, radar).total
        except Exception:
            pass

    return _resp(
        f"📡 Radar «{name}» guardado y revisado "
        f"({total} resultados en seguimiento).\n\n"
        f"Te avisará de novedades. Comandos:\n"
        f"• «ejecutar radar {name}» — revisar novedades ahora.\n"
        f"• «mis radares» — ver todos tus radares.",
        route_reason="radar_saved",
    )


# ---------------------------------------------------------------------------
# listar
# ---------------------------------------------------------------------------

def _do_list(store, owner) -> dict:
    radars = store.list_radars(owner)
    if not radars:
        return _resp(
            "Aún no tienes radares. Haz una búsqueda y dime "
            "«guardar esta búsqueda como Radar <nombre>».",
            route_reason="radar_list_empty",
        )

    lines = [f"📡 Tienes {len(radars)} radar(es):", ""]
    for r in radars:
        estado = "● activo" if r.is_enabled else "⏸ pausado"
        last = r.last_run_at[:10] if r.last_run_at else "nunca"
        runs = store.get_runs(r.id, limit=1)
        novel = ""
        if runs:
            run = runs[0]
            novel = f" · últimos nuevos: {run.new_count}, cambios: {run.changed_count}"
        lines.append(f"• {r.name} — {estado} · última revisión: {last}{novel}")
    lines.append("")
    lines.append("Dime «ejecutar radar <nombre>» para revisar novedades.")
    return _resp("\n".join(lines), route_reason="radar_list")


# ---------------------------------------------------------------------------
# ejecutar
# ---------------------------------------------------------------------------

def _do_run(deps, store, owner, name) -> dict:
    radar = store.get_radar_by_name(owner, name)
    if radar is None:
        return _resp(
            f"No tengo un radar llamado «{name}». Dime «mis radares» para ver "
            f"los que tienes.",
            route_reason="radar_run_not_found",
        )

    result = run_radar(deps, store, radar)
    text = result.digest.get("text") or f"🔔 Radar {radar.name}: corrida completada."
    route = "radar_run_ok" if result.status == "ok" else "radar_run_error"
    return _resp(text, route_reason=route)
