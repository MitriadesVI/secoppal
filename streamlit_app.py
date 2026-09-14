"""
SECOPPAL — Streamlit UI
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

import extra_streamlit_components as stx
import streamlit as st

from app.entity_profile_ui import (
    render_entity_profile_content,
)
from app.core.actor_candidate_ui import actor_candidate_options
from app.core.export_xlsx import build_results_xlsx, export_filename, xlsx_mime_type
from app.core.formatter import _estado_raw_nota
from app.core.radar import follows
from app.core.radar.builder import has_scope_or_topic, save_radar_from_params
from app.core.radar.commands import radar_owner
from app.core.radar.detail import build_opportunity_detail, is_opportunity_event
from app.core.radar.inbox import project_inbox
from app.core.radar.keys import radar_result_key
from app.core.radar.presentation import (
    format_colombia_timestamp,
    format_relative_timestamp,
    scheduler_heartbeat_recent,
    timeline_label,
)
from app.core.radar.runner import run_radar
from app.core.radar.receipts import (
    DISCARDED,
    RadarReceiptError,
    RadarReceiptStore,
)
from app.core.radar.store import RadarExistsError
from app.core.radar.workspace import (
    build_workspace_snapshot,
    is_saveable,
    workspace_item_type,
    workspace_reference_id,
    workspace_title,
)
from app.core.secop_access import resolve_secop_access
from app.config import get_settings
from app.core.auth import AccountError, AccountExistsError, AccountStore
from app.core.web_session import (
    new_streamlit_chat_id,
    reset_user_session,
    scratch_chat_id,
)
from app.core.workspace_store import WorkspaceIntegrityError, WorkspaceStore
from app.service import get_workflow

# ---------------------------------------------------------------------------
# Page config & workflow singleton
# ---------------------------------------------------------------------------

st.set_page_config(page_title="SECOPPAL", page_icon="📋", layout="wide")

workflow = get_workflow()


@st.cache_resource
def get_account_store() -> AccountStore:
    """AccountStore singleton + seed del admin inicial (idempotente)."""
    settings = get_settings()
    store = AccountStore(settings.secop_feedback_db_path)
    store.ensure_admin_seed(
        settings.secoppal_admin_email,
        settings.secoppal_admin_password,
        settings.secoppal_admin_name,
    )
    return store


@st.cache_resource
def get_workspace_store() -> WorkspaceStore:
    """WorkspaceStore singleton sobre la misma SQLite del feedback."""
    return WorkspaceStore(get_settings().secop_feedback_db_path)


@st.cache_resource
def get_radar_receipt_store() -> RadarReceiptStore:
    """Interacciones humanas de Radar sobre la misma SQLite del feedback."""
    return RadarReceiptStore(get_settings().secop_feedback_db_path)


_COOKIE_NAME = "secoppal_session"
_REMEMBER_DAYS = 30


def get_cookie_manager() -> stx.CookieManager:
    """Una instancia de CookieManager por sesión (evita keys duplicadas)."""
    if "_cookie_mgr" not in st.session_state:
        st.session_state["_cookie_mgr"] = stx.CookieManager(key="secoppal_cookies")
    return st.session_state["_cookie_mgr"]


# ═══════════════════════════════════════════════════════════════════════════
# Helper functions — MUST be defined before any code that calls them
# ═══════════════════════════════════════════════════════════════════════════


def detect_test_or_duplicate_from_row(row: dict) -> str | None:
    """Wrapper para detect_test_or_duplicate sin dataset_id explícito.
    Infiere dataset del row por campos disponibles.
    """
    try:
        from app.core.contextual_suggestions import detect_test_or_duplicate
        dataset_id = "p6dx-8zbt" if "nombre_del_procedimiento" in row else "jbjy-vk9h"
        return detect_test_or_duplicate(row, dataset_id)
    except Exception:
        return None


def _render_contextual_filters(
    filters: dict,
    key_scope: str,
    *,
    show_header: bool = True,
    stacked: bool = False,
) -> None:
    """Renderiza barra de filtros contextuales con botones clickeables.
    Un clic = un filtro aplicado inmediatamente.

    Args:
        filters: dict {category: [{label, value, count}, ...]}
        key_scope: prefijo único para las widget keys (evita colisión entre
            la barra inline y la barra del sidebar).
        show_header: muestra "🎚️ Refinar:" arriba (omitir en sidebar).
        stacked: usa una columna por chip (vertical) en vez de fila horizontal.
            Útil en sidebar donde el ancho es limitado.
    """
    if not filters or not any(options for options in filters.values()):
        return

    icons = {"estado": "📋", "fecha": "📅", "monto": "💰", "entidad": "🏛️", "ciudad": "🌆"}

    if show_header:
        st.markdown("🎚️ **Refinar:**")

    for category, options in filters.items():
        if not options:
            continue
        icon = icons.get(category, "🔹")
        if stacked:
            for opt in options:
                label = f"{icon} {opt['label']}"
                if st.button(
                    label,
                    key=f"refine_{key_scope}_{category}_{opt['value']}",
                    use_container_width=True,
                ):
                    st.session_state._pending_refinement = {
                        "category": category,
                        "value": opt["value"],
                        "label": opt["label"],
                    }
                    st.rerun()
        else:
            cols = st.columns(len(options))
            for i, opt in enumerate(options):
                with cols[i]:
                    label = f"{icon} {opt['label']}"
                    if st.button(label, key=f"refine_{key_scope}_{category}_{opt['value']}"):
                        st.session_state._pending_refinement = {
                            "category": category,
                            "value": opt["value"],
                            "label": opt["label"],
                        }
                        st.rerun()


def _state_presentation(
    row: dict, raw_icons: dict[str, str],
) -> tuple[str, str, str]:
    """Devuelve label, icono y nota ya resueltos para una fila visible.

    Streamlit no interpreta señales SECOP: solo expresa el resumen que añadió el
    formatter. Contratos, fallbacks y filas legacy sin resumen conservan el
    comportamiento raw anterior.
    """
    operational_label = row.get("estado_op_label")
    if operational_label:
        label = str(operational_label)
        icon = str(row.get("estado_op_icono") or "⚪")
        return label, icon, _estado_raw_nota(row)

    raw = str(row.get("estado", "") or "")
    return raw, raw_icons.get(raw, "⚪"), ""


def _render_cards(rows: list[dict], owner: str | None = None, idx: int = 0) -> None:
    """Render results as compact cards with SECOP links + botón 'seguir'."""
    estado_icons = {
        "Abierto": "🟢", "Publicado": "🔵", "Cerrado": "🔴",
        "Adjudicado": "🟡", "Celebrado": "🟢", "En ejecucion": "🔵",
        "Liquidado": "⚪", "Desierto": "🔴", "Cancelado": "🚫",
    }

    # Conjunto de claves ya seguidas (una sola lectura para todas las tarjetas).
    store = getattr(workflow, "radar_store", None)
    followed_keys: set[str] = set()
    if store is not None and owner:
        try:
            radar = follows.get_or_create_follows_radar(store, owner)
            followed_keys = set(store.get_seen(radar.id))
        except Exception:
            followed_keys = set()

    for i, row in enumerate(rows, 1):
        titulo = row.get("titulo", "Sin titulo")[:120]
        entidad = row.get("entidad", "")
        valor = row.get("valor", "")
        estado, icon, estado_nota = _state_presentation(row, estado_icons)
        fecha = (row.get("fecha") or "")[:10]
        fecha_label = row.get("fecha_label", "")
        contratista = row.get("contratista", "")
        url = row.get("url", "")
        referencia = (row.get("referencia_del_proceso")
                      or row.get("referencia_del_contrato") or "")
        # Warning for duplicate/test entries
        warning = detect_test_or_duplicate_from_row(row)
        if warning:
            st.caption(f"⚠️ _{warning}_")

        parts = [
            f"**{i}.** {titulo}",
            f"🏛️ {entidad}",
            f"💰 {valor} {icon} {estado}",
        ]
        if estado_nota:
            parts.append(estado_nota)
        if fecha:
            # Mostrar etiqueta de fuente si no es la publicación canónica
            if fecha_label and fecha_label != "Publicación":
                parts.append(f"📅 {fecha} _({fecha_label})_")
            else:
                parts.append(f"📅 {fecha}")
        else:
            parts.append(f"📅 _{fecha_label or 'Pendiente de publicación'}_")
        if contratista:
            parts.append(f"🤝 {contratista}")
        # ADR-032 #4: la URL no determina publicación. Sin enlace directo se
        # ofrece el buscador oficial con la referencia observada, nunca una
        # afirmación de etapa que estos datos no sostienen.
        acceso = resolve_secop_access(url, referencia)
        if acceso.has_direct_link:
            parts.append(f"[🔗 Ver en SECOP]({acceso.direct_url})")
        else:
            parts.append(f"[🔎 Buscar en SECOP II]({acceso.search_url})")
            if acceso.reference:
                parts.append(f"🏷️ Referencia: `{acceso.reference}`")
            else:
                parts.append("🔒 _Enlace directo no disponible en los datos observados._")

        with st.container(border=True):
            st.markdown("  \n".join(parts))
            # Botón "seguir proceso" (solo si hay identidad estable y sesión).
            if store is not None and owner:
                key = radar_result_key(row, row.get("dataset_id", ""))
                if key:
                    siguiendo = key in followed_keys
                    label = "✓ Siguiendo" if siguiendo else "👁 Seguir"
                    if st.button(label, key=f"follow_{idx}_{i}",
                                 help="Vigila cambios de estado/valor de este proceso"):
                        if siguiendo:
                            follows.unfollow(store, owner, key)
                        else:
                            follows.follow(store, owner, row)
                        st.rerun()


def _render_detail_card(row: dict, dataset_id: str = "") -> None:
    """Render a single result with full detail — used for direct lookups."""
    estado_icons = {
        "Abierto": "🟢", "Publicado": "🔵", "Cerrado": "🔴",
        "Adjudicado": "🟡", "Celebrado": "🟢", "En ejecucion": "🔵",
        "Liquidado": "⚪", "Desierto": "🔴", "Cancelado": "🚫",
    }

    titulo = row.get("titulo", "Sin titulo")
    entidad = row.get("entidad", "")
    valor = row.get("valor", "")
    estado, icon, estado_nota = _state_presentation(row, estado_icons)
    fecha = (row.get("fecha") or "")[:10]
    fecha_label = row.get("fecha_label", "")
    contratista = row.get("contratista", "")
    url = row.get("url", "")
    referencia = row.get("referencia_del_proceso") or row.get("referencia_del_contrato", "")
    id_proceso = row.get("id_del_proceso") or row.get("id_contrato", "")
    modalidad = row.get("modalidad", "")
    departamento = row.get("departamento", "")
    ciudad = row.get("ciudad", "")
    descripcion = row.get("descripcion", "") or row.get("nombre_del_procedimiento", "")
    es_contrato = dataset_id == "jbjy-vk9h"
    with st.expander(f"📋 Informacion completa — {titulo[:80]}...", expanded=True):
        st.markdown(f"**{'Objeto' if es_contrato else 'Nombre del procedimiento'}:** {titulo}")

        c1, c2, c3 = st.columns(3)
        c1.markdown(f"**ID:** `{id_proceso}`")
        c2.markdown(f"**Referencia:** `{referencia}`")
        c3.markdown(f"**Estado:** {icon} {estado}")
        if estado_nota:
            st.caption(estado_nota)

        c4, c5 = st.columns(2)
        c4.markdown(f"**Entidad:** {entidad}")
        c5.markdown(f"**Valor:** {valor}")

        c6, c7, c8 = st.columns(3)
        c6.markdown(f"**Departamento:** {departamento}")
        c7.markdown(f"**Ciudad:** {ciudad}")
        c8.markdown(f"**Modalidad:** {modalidad}")

        if fecha:
            label_str = f" _({fecha_label})_" if fecha_label and fecha_label != "Publicación" else ""
            st.markdown(f"**Fecha:** {fecha}{label_str}")
        else:
            st.markdown(f"**Fecha:** _{fecha_label or 'Pendiente de publicación'}_")

        if contratista:
            st.markdown(f"**Proveedor adjudicado:** {contratista}")

        if descripcion and descripcion != titulo:
            st.markdown(f"**Descripcion:** {descripcion}")

    # Link prominente a SECOP. Sin enlace directo NO se afirma una etapa: solo
    # se dice lo que se sabe (no hay enlace) y se ofrece la búsqueda oficial.
    acceso = resolve_secop_access(url, referencia)
    if acceso.has_direct_link:
        st.markdown(f"### 🔗 [Ver proceso completo en SECOP II]({acceso.direct_url})")
    else:
        st.markdown(f"### 🔎 [Buscar en SECOP II]({acceso.search_url})")
        if acceso.reference:
            st.info("Enlace directo no disponible en los datos observados. "
                    f"Busca el proceso en SECOP II con la referencia `{acceso.reference}`.")
        else:
            st.info("Enlace directo no disponible en los datos observados.")

    # Sugerencia de lookup relacionado
    if not es_contrato and estado == "Adjudicado" and id_proceso:
        st.info(
            "💡 Este proceso fue adjudicado. Para buscar el contrato asociado, "
            "pega el ID del contrato (CO1.PCCNTR.XXXXX) en el buscador."
        )
    elif es_contrato and id_proceso:
        st.info(
            "💡 Esto es un contrato. Busca el proceso original con su ID si lo tienes a mano."
        )


def _render_trace(debug: dict) -> None:
    """Show the full pipeline trace in a collapsible expander."""
    with st.expander("🔍 Trace del pipeline", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(f"**Ruta:** `{debug.get('route_reason', '')}`")
        c2.markdown(f"**LLM:** {'Si' if debug.get('needs_llm') else 'No'}")
        c3.markdown(f"**Dataset:** `{debug.get('dataset_id', '')}`")
        c4.markdown(f"**Resultados:** {debug.get('results_count', '?')}")

        st.markdown("**Parametros extraidos:**")
        st.json(debug.get("parsed_params", {}))

        resolved = debug.get("resolved_params", {})
        resolutions_found = False
        for key in ("departamento_resolution", "entidad_resolution"):
            res = resolved.get(key)
            if not res:
                continue
            if not resolutions_found:
                st.markdown("**Resolucion de entidades:**")
                resolutions_found = True

            method = res.get("method", "")
            confidence = res.get("confidence", "")
            value = res.get("value") or res.get("like_value", "—")
            label = key.replace("_resolution", "").capitalize()
            conf_icon = {
                "high": "🟢", "medium": "🟡", "low": "🟠", "none": "🔴",
            }.get(confidence, "⚪")
            st.markdown(f"  {conf_icon} **{label}:** `{value}` — {method} ({confidence})")

        st.markdown("**SoQL generado:**")
        st.code(debug.get("soql_query", ""), language="sql")

        results_count = debug.get("results_count", "?")
        st.markdown(f"**Resultados:** {results_count}")


def _render_rating(trace_id: str, current_rating: int | None, history_idx: int) -> None:
    """Show thumbs up/down with optional comment. Once rated, show result + comment."""
    if current_rating is not None:
        icon = "👍 Relevante" if current_rating == 1 else "👎 Incorrecto"
        comment = st.session_state.history[history_idx].get("rating_comment")
        st.caption(f"Calificacion: {icon}")
        if comment:
            st.caption(f"💬 {comment}")
        return

    comment_key = f"comment_{trace_id}"
    comment = st.text_input(
        "Comentario (opcional)",
        key=comment_key,
        placeholder="Ej: entidad bien pero faltó filtrar por año",
    )

    c1, c2, c3 = st.columns([1, 1, 10])
    with c1:
        if st.button("👍", key=f"up_{trace_id}", help="Resultados relevantes"):
            workflow.rate_query(trace_id, rating=1, comment=comment or None)
            st.session_state.history[history_idx]["rating"] = 1
            st.session_state.history[history_idx]["rating_comment"] = comment or None
            st.rerun()
    with c2:
        if st.button("👎", key=f"down_{trace_id}", help="Resultados incorrectos"):
            workflow.rate_query(trace_id, rating=0, comment=comment or None)
            st.session_state.history[history_idx]["rating"] = 0
            st.session_state.history[history_idx]["rating_comment"] = comment or None
            st.rerun()


def _clean_resolved(resolved: dict) -> dict:
    """Keep resolved_params readable in the trace."""
    clean = {}
    for k, v in resolved.items():
        if isinstance(v, dict) and len(str(v)) > 500:
            clean[k] = {sk: sv for sk, sv in v.items() if sk != "metadata"}
        else:
            clean[k] = v
    return clean


def _continuation_ref(result: dict) -> dict | None:
    """La referencia mínima al turno publicado: nada semántico viaja aquí."""
    result_id = result.get("result_id")
    revision = result.get("conversation_revision")
    if not result_id or revision is None:
        return None
    reference = {"result_id": result_id, "conversation_revision": revision}
    if result.get("chat_id"):
        reference["chat_id"] = result["chat_id"]
    return reference


def _profile_action_ref(result: dict) -> dict | None:
    """Opaque identity exists only for a published Frame result with actions."""
    view = result.get("entity_profile_view")
    if not isinstance(view, dict) or not view.get("actions"):
        return None
    chat_id, result_id = result.get("chat_id"), result.get("result_id")
    revision = result.get("conversation_revision")
    if type(chat_id) is not str or not chat_id or type(result_id) is not str or not result_id:
        return None
    if type(revision) is not int or revision < 1:
        return None
    return {
        "kind": "FRAME_ENVELOPE",
        "chat_id": chat_id,
        "result_id": result_id,
        "conversation_revision": revision,
    }


def _assistant_history_item(result: dict, *, query: str = "") -> dict:
    """Build one rerun-stable assistant item, including safe actor controls."""
    from app.core.natural_alpha.presentation import project

    alpha_view = project(result)
    resolved_params = result.get("resolved_params", {})
    debug_payload = {
        "route_reason": result.get("route_reason", ""),
        "needs_llm": result.get("needs_llm", False),
        "dataset_id": result.get("dataset_id", ""),
        "parsed_params": result.get("parsed_params", {}),
        "resolved_params": _clean_resolved(resolved_params),
        "soql_query": result.get("soql_query", ""),
        "results_count": len(result.get("results", [])),
    }
    return {
        **({"natural_alpha": alpha_view} if alpha_view else {}),
        "role": "assistant",
        "content": result.get("response", ""),
        "query": query,
        "rows": result.get("rows", []),
        "debug": debug_payload,
        # La REFERENCIA al resultado publicado, no una copia de sus params. Es
        # lo que permite que un chip de refinamiento diga sobre qué resultado
        # se aplicó, en vez de que el servidor lo adivine buscando hacia atrás.
        "continuation_ref": _continuation_ref(result),
        "trace_id": result.get("trace_id"),
        "rating": None,
        "contextual_filters": result.get("contextual_filters", {}),
        "total_count": result.get("total_count", 0),
        "actor_candidates": [
            option.as_dict() for option in actor_candidate_options(resolved_params)
        ],
        "entity_profile_view": result.get("entity_profile_view"),
        "profile_action_ref": _profile_action_ref(result),
    }


def _render_assistant_content(item: dict) -> None:
    """Prefer the structured overview and keep profile Markdown currency safe."""
    from app.core.natural_alpha.presentation import render

    render(st, item.get("natural_alpha"), settings=get_settings(), identity=_current_user_identity())
    view = item.get("entity_profile_view")
    route_reason = item.get("debug", {}).get("route_reason", "")
    render_entity_profile_content(
        st, view, item["content"],
        is_profile=route_reason.startswith("entity_profile_"),
        action_reference=item.get("profile_action_ref"),
    )


def _render_excel_export(item: dict, history_idx: int) -> None:
    """Offer an XLSX for exactly the rows already shown in this assistant turn."""
    rows = item.get("rows") or []
    if not rows:
        return
    debug = item.get("debug") or {}
    from app.core.contract_universe import compatible
    if not compatible({**(debug.get("resolved_params") or {}), "dataset": debug.get("dataset_id", "")}):
        st.info("El universo de esta exportación requiere revisión. Repite la búsqueda.")
        return
    payload = build_results_xlsx(
        rows=rows,
        query=item.get("query", ""),
        resolved_params=debug.get("resolved_params") or {},
        dataset_id=debug.get("dataset_id", ""),
        total_count=item.get("total_count", 0),
    )
    st.download_button(
        "⬇️ Descargar Excel",
        data=payload,
        file_name=export_filename(),
        mime=xlsx_mime_type(),
        key=f"export_xlsx_{history_idx}",
        help="Exporta las filas mostradas y los filtros interpretados de esta búsqueda.",
    )


def _render_actor_candidates(candidates: list[dict], history_idx: int) -> None:
    """Render current candidates without interpreting or submitting identity."""
    for candidate in candidates:
        rank = candidate.get("rank")
        token = candidate.get("pending_token")
        if not isinstance(rank, int) or not isinstance(token, str):
            continue
        with st.container(border=True):
            text_col, action_col = st.columns([4, 1])
            text_col.markdown(
                f"**{candidate.get('observed_name', 'Nombre observado')}**  \n"
                f"{candidate.get('identity_text', '')}"
            )
            if action_col.button(
                "Seleccionar",
                key=f"actor_select_{history_idx}_{rank}_{token}",
                use_container_width=True,
            ):
                st.session_state._pending_actor_selection = {
                    "candidate_rank": rank,
                    "pending_token": token,
                }
                st.rerun()


def _current_user_identity() -> dict:
    """Quién es el usuario. **No decide dónde vive su conversación.**

    Antes fijaba aquí ``chat_id = st_email_<email>`` en cada rerun. El id
    representaba la cuenta, no la conversación: dejaba el email en el nombre de
    un archivo de ``data/conversations/`` y hacía que la misma cuenta reutilizara
    ese hilo entre sesiones o después de un F5. Una cuenta B no heredaba el
    ``ConversationStore`` de A, porque este helper reasignaba el id usando el
    email de B.

    El P0 cross-user era distinto: ``session_state`` no se limpiaba por completo
    y las operaciones Radar se autorizaban únicamente por id. El ``chat_id`` lo
    asigna ahora la sesión (ver ``_seed_session_defaults``), que es de quien
    realmente es.
    """
    email = (st.session_state.get("user_email") or "").strip().lower()
    name = (st.session_state.get("user_name") or "").strip()
    if email:
        channel_user_key = f"email:{email}"
        identity = {
            "channel": "streamlit",
            "channel_user_key": channel_user_key,
            "name": name or None,
            "email": email,
            "is_anonymous": False,
        }
        from app.core.natural_alpha.identity import issue_streamlit_identity

        account = globals().get("_account")
        if account is None or account is not st.session_state.get("account"):
            return identity
        return issue_streamlit_identity(
            identity, account, auth_enabled=get_settings().secoppal_auth_enabled,
        )

    anon_id = st.session_state.anonymous_user_id
    return {
        "channel": "streamlit",
        "channel_user_key": f"anon:{anon_id}",
        "name": name or None,
        "email": None,
        "is_anonymous": True,
    }


def _workspace_user_id() -> str | None:
    """``users.user_id`` del usuario actual, o ``None`` si no puede guardar.

    La forma del id la decide una sola autoridad —``FeedbackStore.ensure_user``,
    la misma que ata cada trace a su usuario—; aquí no se construye ninguna.

    Un usuario anónimo devuelve ``None`` a propósito: puede usar Radar y abrir
    una oportunidad, pero no guardarla. La alternativa sería inventar un
    ``user_id="anonymous"`` compartido, que mezclaría los escritorios de todas
    las sesiones sin dueño en uno solo.

    El resultado se cachea por identidad en ``session_state`` para no escribir
    en ``users`` una vez por rerun de Streamlit.
    """
    identity = _current_user_identity()
    if identity.get("is_anonymous") or not identity.get("email"):
        return None
    cache_key = identity.get("channel_user_key") or ""
    cached = st.session_state.get("_workspace_user")
    if cached and cached.get("key") == cache_key:
        return cached["user_id"]
    feedback = getattr(workflow, "feedback", None)
    if feedback is None:
        return None
    user_id = feedback.ensure_user("streamlit", identity)
    if user_id:
        st.session_state["_workspace_user"] = {"key": cache_key, "user_id": user_id}
    return user_id


def _flash(message: str) -> None:
    """Deja un aviso para el rerun siguiente (el actual ya no se pinta)."""
    st.session_state["_workspace_flash"] = message


def _render_flash() -> None:
    message = st.session_state.pop("_workspace_flash", None)
    if message:
        st.toast(message)


# ═══════════════════════════════════════════════════════════════════════════
# Autenticación
# ═══════════════════════════════════════════════════════════════════════════


def _seed_session_defaults() -> None:
    """Estado mínimo de una sesión recién nacida. Idempotente.

    Se llama al arrancar el script y otra vez después de cada frontera de sesión
    (``_end_user_session``), que deja el estado vacío a mitad de la pasada: sin
    volver a sembrar, lo que queda del render buscaría un ``history`` y un
    ``chat_id`` que ya no existen.

    ``anonymous_user_id`` se persiste en la URL a propósito: Streamlit crea una
    sesión nueva en cada F5, y sin esto el dueño anónimo cambiaría al refrescar
    y sus radares —guardados bajo el dueño anterior— "desaparecerían".
    """
    st.session_state.setdefault("history", [])
    st.session_state.setdefault("chat_id", new_streamlit_chat_id())
    if "anonymous_user_id" not in st.session_state:
        _qp_uid = st.query_params.get("uid")
        if _qp_uid:
            st.session_state.anonymous_user_id = _qp_uid
        else:
            _new_uid = uuid.uuid4().hex[:12]
            st.session_state.anonymous_user_id = _new_uid
            st.query_params["uid"] = _new_uid
    st.session_state.setdefault("user_name", "")
    st.session_state.setdefault("user_email", "")


def _end_user_session() -> None:
    """Frontera dura entre dos personas en el mismo navegador.

    Nada ligado al usuario anterior puede sobrevivir a este punto: ni su
    ``history`` (calificable desde la UI), ni el ``_open_radar`` que dejó
    abierto, ni ``_open_event``, ni su ``chat_id``, ni filtros, cachés o avisos.
    Por eso borra **todo** y vuelve a sembrar los valores por defecto, en vez de
    quitar las claves que hoy conocemos: la lista de claves era el bug, y volver
    a escribirla —más larga— solo aplaza el mismo fallo a la próxima feature.

    El ``chat_id`` que siembra es nuevo: la conversación siguiente empieza
    limpia, y no puede volver a direccionar el hilo del usuario anterior.
    """
    reset_user_session(st.session_state)
    _seed_session_defaults()


def _activate_session(acc) -> None:
    """Marca la cuenta como autenticada en session_state.

    Atar email/nombre → cada trace queda a nombre del usuario real.

    **Empieza por vaciar la sesión.** El aislamiento no puede depender de que el
    usuario anterior pulsara "Cerrar sesión": también se llega aquí desde la
    cookie "recordarme" y desde el formulario de login, y un navegador donde
    alguien cerró la pestaña sin salir todavía tiene su estado en pie. Autenticar
    es el momento en que se decide de quién es esta sesión, así que es el momento
    de que no quede nada del anterior.

    Solo corre en la activación real de una identidad: una vez presente
    ``account``, ``_require_login`` retorna antes de llegar aquí, de modo que los
    reruns de una sesión ya autenticada no tocan nada.
    """
    _end_user_session()
    st.session_state.account = {"email": acc.email, "name": acc.name, "role": acc.role}
    st.session_state.user_email = acc.email
    st.session_state.user_name = acc.name or ""


def _render_login_form(cookies: stx.CookieManager) -> None:
    st.title("📋 SECOPPAL")
    st.caption("Inicia sesión para continuar")
    store = get_account_store()
    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Contraseña", type="password")
        remember = st.checkbox("Recordarme en este equipo", value=True)
        submitted = st.form_submit_button("Entrar", use_container_width=True)
    if submitted:
        acc = store.authenticate(email, password)
        if acc is None:
            st.error("Credenciales inválidas o cuenta desactivada.")
        else:
            _activate_session(acc)
            if remember:
                token = store.create_session(acc.email, ttl_days=_REMEMBER_DAYS)
                st.session_state["_session_token"] = token
                cookies.set(
                    _COOKIE_NAME,
                    token,
                    expires_at=datetime.now(timezone.utc) + timedelta(days=_REMEMBER_DAYS),
                    key="set_login_cookie",
                )
            st.rerun()


def _require_login() -> dict | None:
    """Bloquea la app hasta que haya sesión. Devuelve la cuenta o detiene.

    Orden: sesión en memoria → cookie "recordarme" → formulario de login.
    Con ``SECOPPAL_AUTH_ENABLED=false`` (dev/tests) no bloquea y devuelve None.
    """
    if not get_settings().secoppal_auth_enabled:
        return st.session_state.get("account")
    if st.session_state.get("account"):
        return st.session_state["account"]

    cookies = get_cookie_manager()
    cookies.get_all()  # fuerza al componente a reportar las cookies del navegador
    token = cookies.get(_COOKIE_NAME)
    if token:
        acc = get_account_store().resolve_session(token)
        if acc is not None:
            _activate_session(acc)
            st.session_state["_session_token"] = token
            return st.session_state["account"]

    # Primera pasada: el componente de cookies aún puede no haber respondido.
    # Damos un rerun para evitar el "flash" del formulario cuando sí hay cookie.
    if not st.session_state.get("_cookie_probed"):
        st.session_state["_cookie_probed"] = True
        st.rerun()

    _render_login_form(cookies)
    st.stop()


def _render_admin_panel() -> None:
    store = get_account_store()
    with st.expander("👤 Gestión de usuarios", expanded=False):
        with st.form("create_user_form", clear_on_submit=True):
            st.caption("Crear usuario")
            new_email = st.text_input("Email", key="admin_new_email")
            new_name = st.text_input("Nombre", key="admin_new_name")
            new_pw = st.text_input("Contraseña", type="password", key="admin_new_pw")
            new_role = st.selectbox("Rol", ["user", "admin"], key="admin_new_role")
            create = st.form_submit_button("Crear", use_container_width=True)
        if create:
            try:
                store.create_account(new_email, new_pw, name=new_name, role=new_role)
                st.success(f"Usuario {new_email} creado.")
                st.rerun()
            except AccountExistsError:
                st.error("Ese email ya está registrado.")
            except AccountError as exc:
                st.error(str(exc))

        st.divider()
        st.caption("Cuentas")
        for acc in store.list_accounts():
            cols = st.columns([3, 1, 1])
            estado = "🟢" if acc.is_active else "⚪"
            rol_badge = " 👑" if acc.is_admin else ""
            cols[0].write(f"{estado} {acc.email}{rol_badge}")
            toggle_label = "Desactivar" if acc.is_active else "Activar"
            if cols[1].button(toggle_label, key=f"toggle_{acc.email}"):
                try:
                    store.set_active(acc.email, not acc.is_active)
                    st.rerun()
                except AccountError as exc:
                    st.error(str(exc))
            with cols[2].popover("🔑"):
                rp = st.text_input("Nueva contraseña", type="password", key=f"rp_{acc.email}")
                if st.button("Resetear", key=f"rpbtn_{acc.email}"):
                    if rp:
                        store.reset_password(acc.email, rp)
                        st.success("Contraseña actualizada.")
                    else:
                        st.error("La contraseña no puede estar vacía.")

        usage = workflow.get_usage_by_user()
        if usage:
            st.divider()
            st.caption("Uso por usuario")
            st.dataframe(
                [
                    {
                        "Email": u["email"],
                        "Consultas": u["queries"],
                        "Calificadas": u["rated"],
                        "Precisión": (
                            f"{u['precision']:.0%}" if u["precision"] is not None else "—"
                        ),
                        "Última actividad": (u["last_activity"] or "")[:10],
                    }
                    for u in usage
                ],
                use_container_width=True,
                hide_index=True,
            )


# ═══════════════════════════════════════════════════════════════════════════
# Session state
# ═══════════════════════════════════════════════════════════════════════════

_seed_session_defaults()

# ═══════════════════════════════════════════════════════════════════════════
# Gate de autenticación — bloquea todo lo que sigue hasta iniciar sesión
# ═══════════════════════════════════════════════════════════════════════════

_account = _require_login()
_auth_enabled = get_settings().secoppal_auth_enabled
# Sin auth (dev/tests) se ve todo; con auth, solo el rol admin ve métricas/panel.
_is_admin = (not _auth_enabled) or bool(_account and _account.get("role") == "admin")
# Owner estable del usuario (email:... autenticado) para radares y procesos seguidos.
_owner = radar_owner(_current_user_identity(), st.session_state.get("chat_id"))

# ═══════════════════════════════════════════════════════════════════════════
# Sidebar
# ═══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("📋 SECOPPAL")
    st.caption("Buscador conversacional SECOP II")
    st.divider()

    # Feedback metrics — solo admin (estadísticas de acierto)
    if _is_admin:
        st.subheader("Metricas")
        stats = workflow.get_feedback_stats()

        c1, c2 = st.columns(2)
        c1.metric("Queries", stats["total_queries"])
        c2.metric("Precision", f"{stats['precision']:.0%}" if stats["rated"] else "—")

        c3, c4 = st.columns(2)
        c3.metric("Calificadas", stats["rated"])
        c4.metric("Sin resultado", stats["no_results"])

        c5, c6 = st.columns(2)
        c5.metric("Solo regex", stats["heuristic_only"])
        c6.metric("Con LLM", stats["with_llm"])

        st.divider()

    st.subheader("Sesión")
    if _auth_enabled and _account:
        st.write(f"**{_account['email']}**")
        st.caption(f"Rol: {_account['role']}")
        if st.button("Cerrar sesión", use_container_width=True):
            # El orden importa: el token y el gestor de cookies viven en el
            # estado que estamos a punto de vaciar, así que se usan antes.
            # Revocar es lo que de verdad cierra la sesión: si el navegador se
            # queda con la cookie, el token ya no resuelve a nadie.
            _token = st.session_state.get("_session_token")
            get_account_store().revoke_session(_token)
            try:
                get_cookie_manager().delete(_COOKIE_NAME, key="del_login_cookie")
            except Exception:
                pass  # la cookie puede no existir (sesión sin "recordarme")
            _end_user_session()
            st.rerun()
    else:
        # Modo sin auth: identidad de texto libre (comportamiento legacy).
        st.text_input("Nombre", key="user_name")
        st.text_input("Email", key="user_email")

    if _is_admin and _auth_enabled:
        _render_admin_panel()

    st.divider()

    # Examples as clickable buttons
    st.subheader("Ejemplos")
    examples = [
        "licitaciones de mantenimiento vial en Atlantico por mas de 500 millones abiertas",
        "procesos de la gobernacion del Atlantico",
        "contratos del SENA en Bogota mayores a 200 millones",
        "contratos firmados en Cundinamarca en 2024",
        "procesos de minima cuantia de dotacion escolar",
    ]
    for ex in examples:
        if st.button(ex, key=f"ex_{hash(ex)}", use_container_width=True):
            st.session_state._pending_example = ex
            st.rerun()

    # ── Refinar última búsqueda (chips sticky en sidebar) ──────────────
    # Encuentra el turno assistant más reciente con filtros contextuales
    # y los renderiza acá para que sigan accesibles mientras scrolleas.
    _last_filters: dict | None = None
    for _item in reversed(st.session_state.history):
        if _item.get("role") == "assistant":
            _cf = _item.get("contextual_filters") or {}
            if _cf and _item.get("total_count", 0) >= 5:
                _last_filters = _cf
                break
    if _last_filters:
        st.divider()
        st.subheader("🎚️ Refinar última búsqueda")
        _render_contextual_filters(
            _last_filters, "sidebar", show_header=False, stacked=True,
        )

    st.divider()
    show_trace = st.toggle("Mostrar trace del pipeline", value=True)
    show_cards = st.toggle("Tarjetas en vez de tabla", value=True)

# ═══════════════════════════════════════════════════════════════════════════
# Radar tab
# ═══════════════════════════════════════════════════════════════════════════

def _render_radar_detail(store, owner: str, radar) -> None:
    """Vista de detalle: timeline de corridas con digests expandibles.

    Recibe ``owner`` explícito —no lo deduce de una global— porque cada acción de
    esta vista escribe o lee, y el dueño es la autorización. El ``radar`` que
    llega ya viene validado por ``get_radar_for_owner``; las lecturas y
    escrituras del store que hay aquí lo exigen otra vez, para que la vista no
    dependa de cómo la invocaron.

    ``run_radar`` es la excepción deliberada: recibe un radar ya autorizado y no
    autentica. Meterle la sesión web dentro lo obligaría a saber de cuentas, y el
    scheduler —que corre los radares de todos los dueños— no tiene ninguna.
    """
    if st.button("← Volver a mis radares", key="radar_back"):
        del st.session_state["_open_radar"]
        st.rerun()

    estado = "● activo" if radar.is_enabled else "⏸ pausado"
    st.subheader(f"📡 {radar.name}")
    st.caption(f"{estado} · dataset: {radar.dataset or 'procesos'} · "
               f"última revisión: {format_colombia_timestamp(radar.last_run_at)} "
               "(hora Colombia)")
    if radar.needs_attention:
        detail = radar.last_failure_reason or radar.last_error_message
        st.error(f"⚠ Requiere atención{f': {detail}' if detail else ''}")
    if radar.last_error_message:
        st.warning(f"Último error: {radar.last_error_message}")
    _render_scheduler_status(store, radar.frequency, enabled=radar.is_enabled)

    c1, c2 = st.columns(2)
    if c1.button("▶ Ejecutar ahora", key="radar_detail_run", use_container_width=True):
        with st.spinner("Revisando SECOP..."):
            run_radar(workflow.deps, store, radar)
        st.rerun()
    toggle_label = "⏸ Pausar" if radar.is_enabled else "● Reactivar"
    if c2.button(toggle_label, key="radar_detail_toggle", use_container_width=True):
        store.set_enabled_for_owner(owner, radar.id, not radar.is_enabled)
        st.rerun()

    st.divider()
    runs = store.get_runs_for_owner(owner, radar.id, limit=50)
    if not runs:
        st.info("Este radar no se ha ejecutado todavía. Pulsa «Ejecutar ahora».")
        return

    st.markdown("#### Línea de tiempo")
    for i, run in enumerate(runs):
        digest = run.digest()
        label = timeline_label(run)
        with st.expander(label, expanded=(i == 0)):
            st.text(digest.get("text", "(sin digest)"))


_FREQ_LABELS = {
    "manual": "Manual (solo cuando pulso Actualizar)",
    "cada_6h": "Cada 6 horas",
    "diaria": "Diaria",
    "semanal": "Semanal",
}


def _render_scheduler_status(store, frequency: str, *, enabled: bool = True) -> None:
    """Muestra automatización solo cuando existe evidencia persistida."""
    if frequency == "manual":
        return
    frequency_label = _FREQ_LABELS.get(frequency, frequency)
    if not enabled:
        st.info(
            "⏸ Automatización pausada para este Radar.\n\n"
            f"Frecuencia configurada: {frequency_label}."
        )
        return
    state = store.get_scheduler_state()
    if scheduler_heartbeat_recent(state):
        last_pass = format_relative_timestamp(state.last_heartbeat_at)
        suffix = (
            " La última pasada terminó con errores aislados."
            if state.last_pass_status == "completed_with_errors" else ""
        )
        st.success(
            f"● Automatización activa\n\n"
            f"Última pasada del scheduler: {last_pass}.\n\n"
            f"Frecuencia configurada: {frequency_label}.{suffix}"
        )
    else:
        st.warning(
            "⚠ No hay heartbeat reciente de la automatización.\n\n"
            "El Radar puede ejecutarse manualmente.\n\n"
            f"Frecuencia configurada: {frequency_label}."
        )


def _do_crear_radar(store, owner, nombre, consulta, dataset, tema, departamento,
                    entidad, valor_min, valor_max, fecha_desde, fecha_hasta,
                    frequency) -> None:
    """Resuelve la búsqueda vía el pipeline, guarda el radar y lo corre una vez."""
    nombre = (nombre or "").strip()
    if not nombre:
        st.error("Ponle un nombre al radar.")
        return

    # Construir la query en lenguaje natural para resolución (reusa el parser
    # y el entity_resolver del pipeline). Prioriza la búsqueda rápida; si no
    # hay, compone desde los campos estructurados de texto.
    base = (consulta or "").strip()
    if not base:
        parts = []
        parts.append(tema.strip() if tema.strip()
                     else ("contratos" if dataset == "contratos" else "procesos"))
        if departamento.strip():
            parts.append(f"en {departamento.strip()}")
        if entidad.strip():
            parts.append(f"de {entidad.strip()}")
        base = " ".join(parts).strip()
        if dataset == "contratos" and "contrato" not in base.lower():
            base = "contratos de " + base

    has_text = bool(consulta.strip() or tema.strip() or departamento.strip()
                    or entidad.strip())
    if not has_text:
        st.error("Agrega una búsqueda rápida, o al menos temática / departamento / entidad.")
        return

    with st.spinner("Resolviendo búsqueda contra SECOP..."):
        result = workflow.run_query(
            base, channel="streamlit", chat_id=scratch_chat_id("radarcreate", owner),
            user_identity={"channel": "streamlit", "channel_user_key": owner},
        )
    params = dict(result.get("resolved_params") or {})
    if not has_scope_or_topic(params):
        st.error("No pude resolver esa búsqueda (sin alcance ni tema). Sé más específico.")
        return

    # Overlay de los campos estructurados exactos (no necesitan resolución).
    if valor_min and valor_min > 0:
        params["valor_min"] = int(valor_min)
    if valor_max and valor_max > 0:
        params["valor_max"] = int(valor_max)
    if fecha_desde:
        params["fecha_desde"] = fecha_desde.isoformat()
    if fecha_hasta:
        params["fecha_hasta"] = fecha_hasta.isoformat()

    try:
        rid = save_radar_from_params(store, owner=owner, name=nombre, params=params,
                                     channel="web", frequency=frequency)
    except RadarExistsError:
        st.error(f"Ya tienes un radar llamado «{nombre}».")
        return

    # Ejecutar la primera revisión ya, para que el feed y el timeline no queden
    # vacíos tras crear (establece la línea base).
    # Por ``rid`` bastaría —acaba de crearse bajo este dueño—, pero la frontera
    # web lee siempre con dueño: una sola regla, sin excepciones que revisar.
    radar = store.get_radar_for_owner(owner, rid)
    with st.spinner("Ejecutando primera revisión..."):
        run_radar(workflow.deps, store, radar, run_trigger="create")
    st.session_state["_radar_refresh_ts"] = time.time()
    st.success(f"📡 Radar «{nombre}» creado y revisado. Mira sus movimientos abajo.")
    st.rerun()


def _render_crear_radar(store, owner) -> None:
    """Formulario «➕ Crear radar»: búsqueda rápida o por campos."""
    with st.expander("➕ Crear radar nuevo", expanded=False):
        nombre = st.text_input("Nombre del radar", key="cr_nombre",
                               placeholder="ej: Mantenimiento Costa Atlántica")
        st.markdown("**Forma rápida** — descríbelo en una frase:")
        consulta = st.text_input(
            "Búsqueda", key="cr_consulta", label_visibility="collapsed",
            placeholder="ej: oportunidades de mantenimiento en costa atlántica",
        )

        st.markdown("**O por campos** — se combinan con la frase (o la reemplazan):")
        c1, c2 = st.columns(2)
        dataset = c1.selectbox("Dataset", ["procesos", "contratos"], key="cr_ds")
        tema = c2.text_input("Temática / objeto", key="cr_tema",
                             placeholder="ej: mantenimiento")
        departamento = c1.text_input("Región / departamento", key="cr_dep",
                                     placeholder="ej: Atlántico, Costa Atlántica")
        entidad = c2.text_input("Entidad (opcional)", key="cr_ent",
                                placeholder="ej: Gobernación del Cesar")
        valor_min = c1.number_input("Monto mínimo (0 = sin mínimo)", min_value=0,
                                    value=0, step=1_000_000, key="cr_vmin")
        valor_max = c2.number_input("Monto máximo (0 = sin máximo)", min_value=0,
                                    value=0, step=1_000_000, key="cr_vmax")
        fecha_desde = c1.date_input("Desde (opcional)", value=None, key="cr_fd")
        fecha_hasta = c2.date_input("Hasta (opcional)", value=None, key="cr_fh")

        freq = st.selectbox(
            "Tiempo del radar (frecuencia de revisión)",
            options=list(_FREQ_LABELS.keys()),
            format_func=lambda k: _FREQ_LABELS[k], key="cr_freq",
        )
        if freq != "manual":
            _render_scheduler_status(store, freq)

        if st.button("Crear radar", key="cr_submit", type="primary"):
            _do_crear_radar(store, owner, nombre, consulta, dataset, tema,
                            departamento, entidad, valor_min, valor_max,
                            fecha_desde, fecha_hasta, freq)


def _fmt_money_feed(raw) -> str:
    if not raw:
        return "s/d"
    try:
        return f"${float(raw):,.0f}".replace(",", ".")
    except (ValueError, TypeError):
        return str(raw)


def _fmt_feed_ts(ts) -> str:
    """Tiempo relativo tipo feed: 'hace 3 min', 'hace 2 h', 'ayer'."""
    if ts is None:
        return ""
    try:
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            dt = datetime.fromisoformat(str(ts))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return "fecha desconocida"
    delta = datetime.now(timezone.utc) - dt
    secs = int(delta.total_seconds())
    if secs < 60:
        return "hace un momento"
    if secs < 3600:
        return f"hace {secs // 60} min"
    if secs < 86400:
        return f"hace {secs // 3600} h"
    if secs < 172800:
        return "ayer"
    return f"hace {secs // 86400} d"


_EVENT_META = {
    "baseline": ("📡", "empezó a vigilar"),
    "new": ("🆕", "nuevo"),
    "changed_state": ("🔁", "cambio de estado"),
    "changed_value": ("💰", "cambio de valor"),
    "disappeared": ("⬇️", "ya no aparece"),
    "followed": ("👁", "empezaste a seguir"),
}


# Encabezado del detalle por tipo de movimiento. Lenguaje de producto: el
# usuario lee "Nueva oportunidad", nunca `kind: new`.
_DETAIL_HEADLINE = {
    "new": "Nueva oportunidad",
    "changed_state": "Cambió el estado",
    "changed_value": "Cambió el valor",
    "disappeared": "Ya no aparece en el radar",
    "followed": "Proceso que sigues",
}


def _closing_hint(closing) -> str:
    """Relación de la fecha registrada con hoy. No afirma que el proceso esté
    abierto ni cerrado: eso depende de estado y fase, que el evento no guarda."""
    if closing.relation == "future":
        dias = closing.days_remaining or 0
        return "falta 1 día" if dias == 1 else f"faltan {dias} días"
    if closing.relation == "today":
        return "es hoy"
    if closing.relation == "past":
        return "ya pasó"
    return ""


def _render_event_card(ev: dict, *, unread: bool = False) -> None:
    icon, label = _EVENT_META.get(ev.get("kind", ""), ("•", ""))
    with st.container(border=True):
        st.markdown(
            f"{icon} **{ev.get('radar_name', '')}** · {label} · "
            f"{_fmt_feed_ts(ev.get('ts'))}"
        )
        if unread:
            st.caption("● Sin revisar")
        title = (ev.get("title") or "").strip()
        kind = ev.get("kind")
        if kind == "baseline":
            # Un hecho operativo del radar, no una oportunidad: no ofrece
            # ficha ni enlace a SECOP porque no hay un proceso que abrir.
            st.caption(title)
            return
        if title:
            st.markdown(title)
        entity = ev.get("entity") or ""
        if kind == "changed_state":
            st.caption(f"{entity}")
            st.markdown(f"Estado: *{ev.get('prev_status') or 's/d'} → "
                        f"{ev.get('status') or 's/d'}*")
        elif kind == "changed_value":
            st.caption(f"{entity}")
            st.markdown(f"Valor: *{_fmt_money_feed(ev.get('prev_value'))} → "
                        f"{_fmt_money_feed(ev.get('value'))}*")
        else:  # new
            st.caption(f"{entity} · {_fmt_money_feed(ev.get('value'))} · "
                       f"{ev.get('status') or 's/d'}")
        # Enlace directo solo si SECOP entregó uno: nunca se arma a partir del
        # id. Sin él queda la búsqueda pública, para que ningún movimiento del
        # feed deje al usuario sin camino a la fuente.
        acceso = resolve_secop_access(ev.get("urlproceso"), ev.get("referencia"))
        event_id = str(ev.get("id") or "")
        accion, enlace = st.columns([1, 2])
        if is_opportunity_event(kind) and event_id and ev.get("result_key"):
            if accion.button("Ver oportunidad", key=f"open_ev_{event_id}",
                             use_container_width=True):
                st.session_state["_open_event"] = event_id
                st.session_state.pop("_open_radar", None)
                st.rerun()
        if acceso.has_direct_link:
            enlace.markdown(f"[🔗 Ver en SECOP]({acceso.direct_url})")
        else:
            enlace.markdown(f"[🔎 Buscar en SECOP II]({acceso.search_url})")


def _render_detail_header(detail) -> None:
    """Cabecera compacta: qué es, quién contrata, cuánto vale, cuándo cierra."""
    st.subheader(detail.title or "(sin nombre en los datos observados)")
    encabezado = " · ".join(x for x in (detail.entity, detail.location) if x)
    if encabezado:
        st.caption(encabezado)

    izq, der = st.columns(2)
    if detail.value_display:
        izq.metric("Valor", detail.value_display)
    if detail.closing.is_known:
        pista = _closing_hint(detail.closing)
        der.metric(
            "Recepción de ofertas",
            detail.closing.display,
            delta=pista or None,
            delta_color="off",
        )
        # Sin hora ni mención de zona: la columna es un calendar_date y solo
        # afirma el día. Un "hora Colombia" aquí sugeriría una precisión que
        # la fuente no da.
        der.caption("Fecha registrada en SECOP.")
    elif detail.closing.supported:
        # §6: no se inventa un plazo ni se sustituye por la publicación.
        der.info("Fecha de cierre no disponible en los datos observados.")


def _render_detail_change(detail) -> None:
    """Qué cambió, cuando el movimiento fue un cambio."""
    if detail.kind == "changed_state" and detail.previous_status:
        st.markdown(f"Estado: *{detail.previous_status} → "
                    f"{detail.source_status or 's/d'}*")
    elif detail.kind == "changed_value" and detail.previous_value_display:
        st.markdown(f"Valor: *{detail.previous_value_display} → "
                    f"{detail.value_display or 's/d'}*")


def _render_detail_fields(detail) -> None:
    """Ficha. Solo lo observado: un campo ausente se omite, no se rotula s/d."""
    filas = [
        ("Objeto", detail.object_excerpt),
        ("Entidad", detail.entity),
        ("Departamento / ciudad", detail.location),
        ("Valor", detail.value_display),
        ("Modalidad", detail.modality),
        # El estado se muestra tal como lo reporta la fuente. SECOPPAL no
        # deriva aquí una lectura operativa ("recibe ofertas", "ya no"): esa
        # interpretación tiene dueño (process_operational_state, sobre el Truth
        # Layer) y exige estado_del_procedimiento y fase, que el snapshot del
        # evento no conserva. Inventar una segunda respuesta sería peor.
        ("Estado (según SECOP)", detail.source_status),
        ("Contratista", detail.contractor),
        ("Fecha de firma", detail.signature_date),
        ("Referencia", detail.reference),
    ]
    for etiqueta, valor in filas:
        if valor:
            st.markdown(f"**{etiqueta}** — {valor}")


def _render_workspace_action(evento: dict, detail) -> None:
    """Guardar / ✓ Guardada / Quitar, sobre la oportunidad abierta.

    Guardar es una escritura LOCAL: copia el snapshot que el radar ya observó.
    No consulta SECOP —eso haría el click lento, no determinista y dependiente
    de la red para conservar algo que ya se tenía—.
    """
    if not is_saveable(detail):
        # Sin dataset reconocido o sin clave de resultado no hay identidad
        # estable: guardarlo produciría algo que después no se puede reconocer.
        return
    user_id = _workspace_user_id()
    if user_id is None:
        st.button("Inicia sesión para guardar", key="opp_save_locked",
                  disabled=True, use_container_width=True)
        return

    store = get_workspace_store()
    item_type = workspace_item_type(detail)
    reference_id = workspace_reference_id(detail)
    try:
        guardada = store.get_by_reference(user_id, item_type, reference_id)
    except WorkspaceIntegrityError:
        st.warning(
            "Esta oportunidad aparece más de una vez en tu escritorio. "
            "No la modifico hasta resolverlo."
        )
        return

    if guardada is not None and not guardada.archived:
        izq, der = st.columns(2)
        izq.button("✓ Guardada", key="opp_saved", disabled=True,
                   use_container_width=True)
        if der.button("Quitar de guardadas", key="opp_unsave",
                      use_container_width=True):
            store.archive_item(user_id, guardada.item_id)
            _flash("Quitada de tu escritorio")
            st.rerun()
        return

    if st.button("Guardar", key="opp_save", use_container_width=True):
        saved_at = datetime.now(timezone.utc).isoformat()
        resultado = store.save_item(
            user_id,
            item_type,
            reference_id,
            title=workspace_title(detail),
            metadata=build_workspace_snapshot(evento, detail, saved_at),
        )
        if resultado.created:
            _flash("Guardada en tu escritorio")
        elif resultado.restored:
            _flash("Volvió a tu escritorio")
        else:
            _flash("Ya estaba guardada — actualicé la ficha")
        st.rerun()


def _render_radar_management(
    receipts: RadarReceiptStore, owner: str, evento: dict,
) -> None:
    """Dos decisiones de Inbox; ninguna modifica Workspace ni el evento."""
    st.markdown("#### Gestionar en Radar")
    descartar, no_relevante = st.columns(2)
    with descartar:
        st.caption("No quiero seguir viendo esta oportunidad en este Radar.")
        discard_clicked = st.button(
            "Descartar", key="opp_discard", use_container_width=True,
        )
    with no_relevante:
        st.caption(
            "Esta oportunidad no corresponde a lo que este Radar debería encontrar."
        )
        not_relevant_clicked = st.button(
            "No es relevante", key="opp_not_relevant", use_container_width=True,
        )

    try:
        if discard_clicked:
            receipts.discard(owner, str(evento.get("id") or ""))
            _flash("Oportunidad descartada")
        elif not_relevant_clicked:
            receipts.mark_not_relevant(owner, str(evento.get("id") or ""))
            _flash(
                "Gracias. La oculté y guardé tu señal para mejorar la selección."
            )
        else:
            return
    except RadarReceiptError as exc:
        st.error(str(exc))
        return
    st.session_state.pop("_open_event", None)
    st.rerun()


def _render_opportunity_detail(
    store, receipts: RadarReceiptStore, owner: str, event_id: str,
) -> bool:
    """Detalle de una oportunidad detectada. False si ya no se puede abrir."""
    evento = store.get_event(owner, event_id)
    if evento is None:
        return False
    detail = build_opportunity_detail(evento, now=datetime.now(timezone.utc))
    if detail is None:
        return False

    try:
        # Abrir la ficha —no pintar su tarjeta— es el hecho humano ``viewed``.
        # La store lo vuelve idempotente para que los reruns no generen ruido.
        receipts.mark_viewed(owner, event_id)
    except RadarReceiptError as exc:
        st.warning(f"No pude registrar que revisaste esta oportunidad: {exc}")

    if st.button("← Volver a movimientos", key="opp_back"):
        del st.session_state["_open_event"]
        st.rerun()

    titular = _DETAIL_HEADLINE.get(detail.kind, "Movimiento del radar")
    origen = f" · 📡 {detail.radar_name}" if detail.radar_name else ""
    st.markdown(f"**{titular}**{origen} · detectado "
                f"{format_relative_timestamp(detail.observed_at)}")

    _render_detail_header(detail)
    _render_detail_change(detail)
    st.divider()
    _render_detail_fields(detail)

    _render_workspace_action(evento, detail)
    # Siempre hay una vía a la fuente: la ficha observada si SECOP la entregó,
    # y si no el buscador oficial con la referencia que el evento congeló.
    acceso = resolve_secop_access(detail.secop_url, detail.reference)
    if acceso.has_direct_link:
        st.link_button("Abrir en SECOP", acceso.direct_url, type="primary")
    else:
        st.link_button("Buscar en SECOP II", acceso.search_url, type="primary")
        if acceso.reference:
            st.caption("Enlace directo no disponible en los datos observados. "
                       f"Busca el proceso en SECOP II con la referencia {acceso.reference}.")
        else:
            st.caption("Enlace directo no disponible en los datos observados.")

    _render_radar_management(receipts, owner, evento)

    st.caption(
        "Estos datos son los que el radar observó el "
        f"{format_colombia_timestamp(detail.observed_at)} (hora Colombia) y no "
        "cambian después. SECOP sigue siendo la fuente para ofertar."
    )
    return True


def _refresh_all_radares(store, owner) -> None:
    """Corre todos los radares de búsqueda activos + refresca los seguidos."""
    # El radar sintético de seguidos no es una búsqueda: se refresca aparte.
    radars = [r for r in store.list_radars(owner)
              if r.is_enabled and r.frequency != follows.FOLLOWS_FREQUENCY]
    total_steps = len(radars) + 1  # +1 por el refresco de seguidos
    prog = st.progress(0.0, text="Actualizando...")
    for i, radar in enumerate(radars):
        prog.progress(i / total_steps, text=f"Actualizando: {radar.name}")
        try:
            run_radar(workflow.deps, store, radar)
        except Exception:
            pass  # un radar que falla no debe tumbar el refresco global
    prog.progress((total_steps - 1) / total_steps, text="Revisando procesos seguidos...")
    try:
        follows.refresh_follows(workflow.deps, store, owner)
    except Exception:
        pass
    prog.empty()
    st.session_state["_radar_refresh_ts"] = time.time()


def _render_manage_row(store, owner: str, radar) -> None:
    estado = "● activo" if radar.is_enabled else "⏸ pausado"
    last = format_colombia_timestamp(radar.last_run_at)
    st.markdown(
        f"**📡 {radar.name}** — {estado} · última revisión: {last} (hora Colombia)"
    )
    if radar.needs_attention:
        detail = radar.last_failure_reason or radar.last_error_message
        st.error(f"⚠ Requiere atención{f': {detail}' if detail else ''}")
        if radar.last_error_message and radar.last_error_message != detail:
            st.caption(f"Último error: {radar.last_error_message}")
    _render_scheduler_status(store, radar.frequency, enabled=radar.is_enabled)
    c1, c2, c3, c4 = st.columns(4)
    if c1.button("Abrir", key=f"mng_open_{radar.id}", use_container_width=True):
        st.session_state["_open_radar"] = radar.id
        st.session_state.pop("_open_event", None)
        st.rerun()
    if c2.button("Ejecutar", key=f"mng_run_{radar.id}", use_container_width=True):
        with st.spinner("Revisando SECOP..."):
            run_radar(workflow.deps, store, radar)
        st.rerun()
    toggle = "Pausar" if radar.is_enabled else "Reactivar"
    # Las filas vienen de ``list_radars(owner)``, que ya es una lectura del
    # dueño. Las mutaciones lo exigen igualmente: si mañana esta lista se
    # construye de otra forma, el ``id`` no habrá vuelto a ser la autorización.
    if c3.button(toggle, key=f"mng_toggle_{radar.id}", use_container_width=True):
        store.set_enabled_for_owner(owner, radar.id, not radar.is_enabled)
        st.rerun()
    if c4.button("Eliminar", key=f"mng_del_{radar.id}", use_container_width=True):
        store.delete_radar_for_owner(owner, radar.id)
        st.rerun()
    st.divider()


def _render_follows_section(store, owner) -> None:
    """Sección '📌 Procesos seguidos': lista con estado + dejar de seguir."""
    seguidos = follows.list_follows(store, owner)
    with st.expander(f"📌 Procesos seguidos ({len(seguidos)})",
                     expanded=bool(seguidos)):
        if not seguidos:
            st.caption("Sigue un proceso desde el botón 👁 en su tarjeta de resultados.")
            return
        for rec in seguidos:
            key = rec.get("result_key")
            titulo = (rec.get("title") or "(sin título)")[:100]
            entidad = rec.get("entity") or ""
            estado = rec.get("status") or "s/d"
            valor = _fmt_money_feed(rec.get("value"))
            acceso = resolve_secop_access(rec.get("urlproceso"),
                                          rec.get("referencia"))
            with st.container(border=True):
                st.markdown(f"**{titulo}**")
                st.caption(f"{entidad} · {valor} · {estado}")
                c1, c2 = st.columns([3, 1])
                if acceso.has_direct_link:
                    c1.markdown(f"[🔗 Ver en SECOP]({acceso.direct_url})")
                else:
                    c1.markdown(f"[🔎 Buscar en SECOP II]({acceso.search_url})")
                if c2.button("Dejar de seguir", key=f"unfollow_{key}",
                             use_container_width=True):
                    follows.unfollow(store, owner, key)
                    st.rerun()


def _render_radares() -> None:
    """Pestaña 📡 Radares: feed de movimientos + gestión → detalle (timeline)."""
    store = getattr(workflow, "radar_store", None)
    if store is None:
        st.info("El módulo de radares no está disponible.")
        return

    identity = _current_user_identity()
    owner = radar_owner(identity, st.session_state.get("chat_id"))
    receipts = get_radar_receipt_store()

    # Detalle de una oportunidad concreta (tiene prioridad: se llega a él
    # desde el feed, y el botón que lo abre cierra cualquier radar abierto).
    open_event = st.session_state.get("_open_event")
    if open_event:
        if _render_opportunity_detail(store, receipts, owner, open_event):
            return
        # El evento ya no existe o no identifica un proceso: se vuelve al feed
        # en vez de dejar una vista muerta.
        del st.session_state["_open_event"]

    # Vista de detalle si hay un radar abierto.
    # El id abierto es un dato que sobrevive en ``session_state``: si por lo que
    # sea cruzó una frontera de sesión, el dueño —no el id— decide qué se ve.
    # Un radar ajeno se trata como uno inexistente: se vuelve a "mis radares"
    # sin un error que confirme que existe.
    open_id = st.session_state.get("_open_radar")
    if open_id:
        radar = store.get_radar_for_owner(owner, open_id)
        if radar is not None:
            _render_radar_detail(store, owner, radar)
            return
        del st.session_state["_open_radar"]

    _render_crear_radar(store, owner)
    _render_follows_section(store, owner)

    # Excluir el radar sintético de "procesos seguidos" del listado de búsquedas.
    radars = [r for r in store.list_radars(owner)
              if r.frequency != follows.FOLLOWS_FREQUENCY]

    # Barra de refresco (pull-to-refresh). El feed muestra movimientos ya
    # persistidos al instante; este botón corre los radares para traer nuevos.
    c1, c2 = st.columns([1, 3])
    if c1.button("🔄 Actualizar", key="radar_refresh_all",
                 type="primary", use_container_width=True):
        _refresh_all_radares(store, owner)
        st.rerun()
    activos = sum(1 for r in radars if r.is_enabled)
    last_ts = st.session_state.get("_radar_refresh_ts")
    last_txt = f" · actualizado {_fmt_feed_ts(last_ts)}" if last_ts else ""
    c2.caption(f"{len(radars)} radares · {activos} activos{last_txt}. "
               "Baja para ver movimientos; pulsa Actualizar para traer nuevos.")

    # Feed de movimientos (todos los radares juntos, cronológico).
    feed = store.get_feed(owner, limit=80)
    viewed = receipts.get_viewed_event_ids(
        owner, [str(event.get("id") or "") for event in feed]
    )
    dispositions = receipts.get_current_dispositions(owner)
    inbox = project_inbox(
        feed, viewed_event_ids=viewed, dispositions=dispositions,
    )
    suppressed = receipts.list_suppressed(owner)
    if not feed and not suppressed:
        st.info("Sin movimientos todavía. Crea un radar y pulsa 🔄 Actualizar.")
    else:
        st.markdown("#### Movimientos")
        st.caption(f"{inbox.unread_count} sin revisar")
        if not inbox.visible_events:
            st.info("No hay movimientos visibles en este momento.")
        for ev in inbox.visible_events:
            _render_event_card(
                dict(ev), unread=str(ev.get("id") or "") in inbox.unread_event_ids,
            )

    if suppressed:
        with st.expander(f"Ocultadas ({len(suppressed)})"):
            for item in suppressed:
                with st.container(border=True):
                    st.markdown(f"**{item.title or item.result_key}**")
                    if item.radar_name:
                        st.caption(f"📡 {item.radar_name}")
                    reason = (
                        "Descartada" if item.disposition == DISCARDED
                        else "Marcada como no relevante"
                    )
                    st.caption(f"Motivo: {reason}")
                    if st.button(
                        "Restaurar",
                        key=f"restore_{item.radar_id}_{item.result_key}",
                        use_container_width=True,
                    ):
                        try:
                            receipts.restore(owner, item.radar_id, item.result_key)
                        except RadarReceiptError as exc:
                            st.error(str(exc))
                        else:
                            _flash("Oportunidad restaurada")
                            st.rerun()

    # Gestión de radares (secundario): pausar, abrir detalle, eliminar.
    if radars:
        with st.expander(f"⚙️ Gestionar radares ({len(radars)})"):
            for radar in radars:
                _render_manage_row(store, owner, radar)


# ═══════════════════════════════════════════════════════════════════════════
# Mi escritorio tab
# ═══════════════════════════════════════════════════════════════════════════

# Las filas de la tarjeta salen del snapshot guardado, nunca de una relectura.
# Una clave ausente se OMITE: una ficha llena de "s/d" no informa más, solo
# estorba, y aquí además mentiría sobre lo que se observó el día que se guardó.
_WORKSPACE_CARD_ROWS = (
    ("Objeto", "object_excerpt"),
    ("Entidad", "entity"),
    ("Departamento / ciudad", "location"),
    ("Valor", "value_display"),
    ("Estado (según SECOP)", "source_status"),
    ("Modalidad", "modality"),
    ("Fecha de cierre", "closing_date"),
    ("Contratista", "contractor"),
    ("Fecha de firma", "signature_date"),
    ("Referencia", "reference"),
)


def _render_workspace_card(item, user_id: str) -> None:
    """Una oportunidad guardada. Se pinta solo con lo que trae el snapshot."""
    meta = item.metadata or {}
    with st.container(border=True):
        st.markdown(f"**{item.title}**")
        guardada = meta.get("saved_at") or item.created_at
        radar = meta.get("radar_name")
        origen = f"📡 {radar} · " if radar else ""
        st.caption(f"{origen}guardada {format_relative_timestamp(guardada)}")

        for etiqueta, clave in _WORKSPACE_CARD_ROWS:
            valor = meta.get(clave)
            if valor:
                st.markdown(f"**{etiqueta}** — {valor}")

        # R2 no migra snapshots: un item viejo sin `secop_url` sigue siendo útil
        # porque su referencia observada alimenta la búsqueda pública.
        acceso = resolve_secop_access(meta.get("secop_url"), meta.get("reference"))
        izq, der = st.columns(2)
        if acceso.has_direct_link:
            izq.link_button("Abrir en SECOP", acceso.direct_url,
                            use_container_width=True)
        else:
            izq.link_button("Buscar en SECOP II", acceso.search_url,
                            use_container_width=True)
        if der.button("Quitar", key=f"ws_remove_{item.item_id}",
                      use_container_width=True):
            # Archivar, no borrar: quitar del escritorio es una decisión
            # reversible y la UI normal no expone hard-delete.
            get_workspace_store().archive_item(user_id, item.item_id)
            _flash("Quitada de tu escritorio")
            st.rerun()


def _render_escritorio() -> None:
    """Pestaña 🗂 Mi escritorio: lo que el usuario decidió conservar."""
    user_id = _workspace_user_id()
    if user_id is None:
        st.info(
            "Inicia sesión para guardar oportunidades y volver a encontrarlas aquí."
        )
        return

    items = get_workspace_store().list_items(user_id, limit=100)
    if not items:
        st.info(
            "Todavía no has guardado oportunidades.\n\n"
            "Cuando encuentres una interesante en Radar, ábrela y pulsa Guardar."
        )
        return

    st.caption(f"{len(items)} guardadas")
    for item in items:
        _render_workspace_card(item, user_id)


# ═══════════════════════════════════════════════════════════════════════════
# Main area
# ═══════════════════════════════════════════════════════════════════════════

st.title("SECOPPAL")
st.caption("Busqueda conversacional sobre contratacion publica colombiana — SECOP II")

_render_flash()

tab_buscar, tab_radares, tab_escritorio = st.tabs(
    ["🔎 Buscar", "📡 Radares", "🗂 Mi escritorio"]
)

with tab_buscar:
    # Onboarding cuando no hay historial
    if not st.session_state.history:
        st.info(
            "👋 **Bienvenido a SECOPPAL** — tu asistente para buscar oportunidades "
            "y contratos en SECOP II.\n\n"
            "**Ejemplos de consultas:**\n"
            "• contratos de obra en Bogotá\n"
            "• procesos abiertos en INVIAS\n"
            "• contratos mayores a 500M en Antioquia\n\n"
            "Haz clic en un ejemplo de la barra lateral o escribe tu consulta abajo."
        )

    # ═══════════════════════════════════════════════════════════════════════
    # Render chat history
    # ═══════════════════════════════════════════════════════════════════════

    # Índice del último mensaje assistant — usado para mostrar "Ver más"
    # solo en el turno actual y evitar botones obsoletos en turnos viejos.
    _last_assistant_idx = next(
        (i for i in range(len(st.session_state.history) - 1, -1, -1)
         if st.session_state.history[i].get("role") == "assistant"),
        -1,
    )

    for idx, item in enumerate(st.session_state.history):
        if item["role"] == "user":
            with st.chat_message("user"):
                st.markdown(item["content"])
            continue

        with st.chat_message("assistant"):
            _render_assistant_content(item)

            if idx == _last_assistant_idx and item.get("actor_candidates"):
                _render_actor_candidates(item["actor_candidates"], idx)

            # Contextual filter bar — aparece incluso con pocos resultados (≥5)
            # para que los refinamientos sucesivos sigan teniendo opciones visibles.
            contextual_filters = item.get("contextual_filters", {})
            total_count = item.get("total_count", 0)
            if contextual_filters and total_count >= 5:
                _render_contextual_filters(contextual_filters, f"inline_{idx}")

            rows = item.get("rows", [])
            route_reason = item.get("debug", {}).get("route_reason", "")
            dataset_id = item.get("debug", {}).get("dataset_id", "")
            is_direct_lookup = route_reason.startswith("direct_lookup")

            if is_direct_lookup and rows:
                _render_detail_card(rows[0], dataset_id=dataset_id)
            elif rows and show_cards:
                _render_cards(rows, owner=_owner, idx=idx)
            elif rows:
                st.dataframe(rows, use_container_width=True)

            if rows:
                _render_excel_export(item, idx)

            # Botón "Ver más resultados" — solo en el turno actual, cuando hay
            # más resultados de los mostrados y no es un direct lookup.
            if (
                idx == _last_assistant_idx
                and not is_direct_lookup
                and rows
                and total_count > len(rows)
            ):
                remaining = total_count - len(rows)
                if st.button(
                    f"📄 Ver más resultados ({remaining} restantes)",
                    key=f"paginate_{idx}",
                    use_container_width=True,
                ):
                    st.session_state._pending_pagination = True
                    st.rerun()

            debug = item.get("debug")
            if debug and show_trace:
                _render_trace(debug)

            trace_id = item.get("trace_id")
            rating = item.get("rating")
            if trace_id:
                _render_rating(trace_id, rating, idx)

with tab_radares:
    _render_radares()

with tab_escritorio:
    _render_escritorio()

# ═══════════════════════════════════════════════════════════════════════════
# Chat input — handle pending refinement and examples
# ═══════════════════════════════════════════════════════════════════════════

incoming_query: str | None = None
seed_resolved_params: dict | None = None
actor_selection_event: dict | None = None
profile_action_event: dict | None = None

if getattr(st.session_state, "_pending_actor_selection", None):
    actor_selection_event = st.session_state._pending_actor_selection
    del st.session_state._pending_actor_selection

if getattr(st.session_state, "_pending_profile_action", None):
    profile_action_event = st.session_state._pending_profile_action
    del st.session_state._pending_profile_action

# Pending refinement from filter bar
if hasattr(st.session_state, "_pending_refinement") and st.session_state._pending_refinement:
    ref = st.session_state._pending_refinement
    del st.session_state._pending_refinement

    category = ref.get("category", "")
    label = ref.get("label", "")
    filter_value = ref.get("value")

    from app.core.contextual_suggestions import apply_refinement
    from app.core.workflow.admission_rules import CONTINUATION_REF_KEY

    # El chip se aplica sobre el ÚLTIMO resultado publicado, y lo dice. Antes
    # rebuscaba hacia atrás el primer turno con params: si el último resultado
    # era un análisis institucional, ese seed entraba igual y `handle_refinement`
    # lo convertía en una búsqueda tabular sobre un universo distinto.
    last_params = {}
    last_ref = None
    for item in reversed(st.session_state.history):
        debug = item.get("debug", {})
        if debug:
            last_params = debug.get("resolved_params", {})
            last_ref = item.get("continuation_ref")
            break

    if last_params:
        seed_resolved_params = apply_refinement(last_params, category, filter_value)
        if last_ref:
            seed_resolved_params[CONTINUATION_REF_KEY] = last_ref
        incoming_query = f"🎚️ {label}"
    else:
        # No hay contexto previo — degrade silenciosamente, sin enviar query
        incoming_query = None

if hasattr(st.session_state, "_pending_example"):
    incoming_query = st.session_state._pending_example
    del st.session_state._pending_example

# Pending pagination from "Ver más resultados" button
if getattr(st.session_state, "_pending_pagination", False):
    del st.session_state._pending_pagination
    # "más resultados" coincide con is_pagination_phrase y reusa el flujo
    # de paginación del orchestrator (avanza offset desde el último turno).
    incoming_query = "más resultados"
    seed_resolved_params = None

typed_query = st.chat_input("Escribe tu consulta sobre procesos o contratos SECOP")
if typed_query:
    incoming_query = typed_query
    seed_resolved_params = None  # query natural manda
    actor_selection_event = None

if actor_selection_event:
    user_identity = _current_user_identity()
    with st.spinner("Consultando SECOP..."):
        result = workflow.select_actor_candidate(
            actor_selection_event.get("candidate_rank"),
            actor_selection_event.get("pending_token"),
            channel="streamlit",
            chat_id=st.session_state.chat_id,
            user_identity=user_identity,
        )
    st.session_state.history.append(_assistant_history_item(result))
    st.rerun()

if profile_action_event:
    user_identity = _current_user_identity()
    with st.spinner("Consultando SECOP..."):
        result = workflow.select_entity_profile_action(
            profile_action_event.get("action_id"),
            profile_action_event.get("reference"),
            channel="streamlit",
            chat_id=st.session_state.chat_id,
            user_identity=user_identity,
        )
    phrase = result.get("profile_action_phrase", "Acción de perfil")
    st.session_state.history.append({"role": "user", "content": phrase})
    st.session_state.history.append(_assistant_history_item(result, query=phrase))
    st.rerun()

if incoming_query:
    st.session_state.history.append({"role": "user", "content": incoming_query})
    user_identity = _current_user_identity()

    with st.spinner("Consultando SECOP..."):
        result = workflow.run_query(incoming_query, channel="streamlit",
                                     chat_id=st.session_state.chat_id,
                                     user_identity=user_identity,
                                     seed_resolved_params=seed_resolved_params)

    st.session_state.history.append(_assistant_history_item(result, query=incoming_query))
    st.rerun()
