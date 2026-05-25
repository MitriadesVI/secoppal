"""
SECOPPAL — Streamlit UI
"""
from __future__ import annotations

import re
import uuid

import streamlit as st

from app.service import get_workflow

# ---------------------------------------------------------------------------
# Page config & workflow singleton
# ---------------------------------------------------------------------------

st.set_page_config(page_title="SECOPPAL", page_icon="📋", layout="wide")

workflow = get_workflow()


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


def _render_contextual_filters(filters: dict) -> None:
    """Renderiza barra de filtros contextuales con botones clickeables.
    Un clic = un filtro aplicado inmediatamente.
    """
    if not filters:
        return

    icons = {"estado": "📋", "fecha": "📅", "monto": "💰", "entidad": "🏛️", "ciudad": "🌆"}

    st.markdown("🎚️ **Refinar:**")
    for category, options in filters.items():
        if not options:
            continue
        cols = st.columns(len(options))
        icon = icons.get(category, "🔹")
        for i, opt in enumerate(options):
            with cols[i]:
                label = f"{icon} {opt['label']}"
                if st.button(label, key=f"refine_{category}_{opt['value']}_{uuid.uuid4().hex[:6]}"):
                    st.session_state._pending_refinement = {
                        "category": category,
                        "value": opt["value"],
                        "label": opt["label"],
                    }
                    st.rerun()


def _render_cards(rows: list[dict]) -> None:
    """Render results as compact cards with SECOP links."""
    estado_icons = {
        "Abierto": "🟢", "Publicado": "🔵", "Cerrado": "🔴",
        "Adjudicado": "🟡", "Celebrado": "🟢", "En ejecucion": "🔵",
        "Liquidado": "⚪", "Desierto": "🔴", "Cancelado": "🚫",
    }

    for i, row in enumerate(rows, 1):
        titulo = row.get("titulo", "Sin titulo")[:120]
        entidad = row.get("entidad", "")
        valor = row.get("valor", "")
        estado = row.get("estado", "")
        fecha = (row.get("fecha") or "")[:10]
        contratista = row.get("contratista", "")
        url = row.get("url", "")
        icon = estado_icons.get(estado, "⚪")

        # Warning for duplicate/test entries
        warning = detect_test_or_duplicate_from_row(row)
        if warning:
            st.caption(f"⚠️ _{warning}_")

        parts = [
            f"**{i}.** {titulo}",
            f"🏛️ {entidad}",
            f"💰 {valor} {icon} {estado}",
        ]
        if fecha:
            parts.append(f"📅 {fecha}")
        if contratista:
            parts.append(f"🤝 {contratista}")
        if url:
            parts.append(f"[🔗 Ver en SECOP]({url})")

        st.markdown("  \n".join(parts))
        st.divider()


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
    estado = row.get("estado", "")
    fecha = (row.get("fecha") or "")[:10]
    contratista = row.get("contratista", "")
    url = row.get("url", "")
    referencia = row.get("referencia_del_proceso") or row.get("referencia_del_contrato", "")
    id_proceso = row.get("id_del_proceso") or row.get("id_contrato", "")
    modalidad = row.get("modalidad", "")
    departamento = row.get("departamento", "")
    ciudad = row.get("ciudad", "")
    descripcion = row.get("descripcion", "") or row.get("nombre_del_procedimiento", "")
    es_contrato = dataset_id == "jbjy-vk9h"
    icon = estado_icons.get(estado, "⚪")

    with st.expander(f"📋 Informacion completa — {titulo[:80]}...", expanded=True):
        st.markdown(f"**{'Objeto' if es_contrato else 'Nombre del procedimiento'}:** {titulo}")

        c1, c2, c3 = st.columns(3)
        c1.markdown(f"**ID:** `{id_proceso}`")
        c2.markdown(f"**Referencia:** `{referencia}`")
        c3.markdown(f"**Estado:** {icon} {estado}")

        c4, c5 = st.columns(2)
        c4.markdown(f"**Entidad:** {entidad}")
        c5.markdown(f"**Valor:** {valor}")

        c6, c7, c8 = st.columns(3)
        c6.markdown(f"**Departamento:** {departamento}")
        c7.markdown(f"**Ciudad:** {ciudad}")
        c8.markdown(f"**Modalidad:** {modalidad}")

        if fecha:
            st.markdown(f"**Fecha:** {fecha}")

        if contratista:
            st.markdown(f"**Proveedor adjudicado:** {contratista}")

        if descripcion and descripcion != titulo:
            st.markdown(f"**Descripcion:** {descripcion}")

    # Link prominente a SECOP
    if url:
        st.markdown(f"### 🔗 [Ver proceso completo en SECOP II]({url})")

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


def _safe_key(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_")[:80]


def _current_user_identity() -> dict:
    email = (st.session_state.get("user_email") or "").strip().lower()
    name = (st.session_state.get("user_name") or "").strip()
    if email:
        channel_user_key = f"email:{email}"
        st.session_state.chat_id = f"st_email_{_safe_key(email)}"
        return {
            "channel": "streamlit",
            "channel_user_key": channel_user_key,
            "name": name or None,
            "email": email,
            "is_anonymous": False,
        }

    anon_id = st.session_state.anonymous_user_id
    st.session_state.chat_id = f"st_anon_{anon_id}"
    return {
        "channel": "streamlit",
        "channel_user_key": f"anon:{anon_id}",
        "name": name or None,
        "email": None,
        "is_anonymous": True,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Session state
# ═══════════════════════════════════════════════════════════════════════════

if "history" not in st.session_state:
    st.session_state.history: list[dict] = []

if "chat_id" not in st.session_state:
    st.session_state.chat_id = "st_default"

if "anonymous_user_id" not in st.session_state:
    st.session_state.anonymous_user_id = uuid.uuid4().hex[:12]

if "user_name" not in st.session_state:
    st.session_state.user_name = ""

if "user_email" not in st.session_state:
    st.session_state.user_email = ""

# ═══════════════════════════════════════════════════════════════════════════
# Sidebar
# ═══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("📋 SECOPPAL")
    st.caption("Buscador conversacional SECOP II")
    st.divider()

    # Feedback metrics
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

    st.subheader("Usuario")
    st.text_input("Nombre", key="user_name")
    st.text_input("Email", key="user_email")

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

    st.divider()
    show_trace = st.toggle("Mostrar trace del pipeline", value=True)
    show_cards = st.toggle("Tarjetas en vez de tabla", value=True)

# ═══════════════════════════════════════════════════════════════════════════
# Main area
# ═══════════════════════════════════════════════════════════════════════════

st.title("SECOPPAL")
st.caption("Busqueda conversacional sobre contratacion publica colombiana — SECOP II")

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

# ═══════════════════════════════════════════════════════════════════════════
# Render chat history
# ═══════════════════════════════════════════════════════════════════════════

for idx, item in enumerate(st.session_state.history):
    if item["role"] == "user":
        with st.chat_message("user"):
            st.markdown(item["content"])
        continue

    with st.chat_message("assistant"):
        st.markdown(item["content"])

        # Contextual filter bar
        contextual_filters = item.get("contextual_filters", {})
        total_count = item.get("total_count", 0)
        if contextual_filters and total_count > 20:
            _render_contextual_filters(contextual_filters)

        rows = item.get("rows", [])
        route_reason = item.get("debug", {}).get("route_reason", "")
        dataset_id = item.get("debug", {}).get("dataset_id", "")
        is_direct_lookup = route_reason.startswith("direct_lookup")

        if is_direct_lookup and rows:
            _render_detail_card(rows[0], dataset_id=dataset_id)
        elif rows and show_cards:
            _render_cards(rows)
        elif rows:
            st.dataframe(rows, use_container_width=True)

        debug = item.get("debug")
        if debug and show_trace:
            _render_trace(debug)

        trace_id = item.get("trace_id")
        rating = item.get("rating")
        if trace_id:
            _render_rating(trace_id, rating, idx)

# ═══════════════════════════════════════════════════════════════════════════
# Chat input — handle pending refinement and examples
# ═══════════════════════════════════════════════════════════════════════════

incoming_query: str | None = None

# Pending refinement from filter bar
if hasattr(st.session_state, "_pending_refinement") and st.session_state._pending_refinement:
    ref = st.session_state._pending_refinement
    del st.session_state._pending_refinement

    # Build implicit query from refinement
    category = ref.get("category", "")
    label = ref.get("label", "")
    filter_value = ref.get("value")

    from app.core.contextual_suggestions import apply_refinement

    # Get last assistant params from history
    last_params = {}
    for item in reversed(st.session_state.history):
        debug = item.get("debug", {})
        if debug:
            last_params = debug.get("resolved_params", {})
            break

    # Apply refinement
    refined_params = apply_refinement(last_params, category, filter_value)

    # Rebuild query — use the original query with refinement
    incoming_query = f"filtrando por {label}"

if hasattr(st.session_state, "_pending_example"):
    incoming_query = st.session_state._pending_example
    del st.session_state._pending_example

typed_query = st.chat_input("Escribe tu consulta sobre procesos o contratos SECOP")
if typed_query:
    incoming_query = typed_query

if incoming_query:
    st.session_state.history.append({"role": "user", "content": incoming_query})
    user_identity = _current_user_identity()

    with st.spinner("Consultando SECOP..."):
        result = workflow.run_query(incoming_query, channel="streamlit",
                                     chat_id=st.session_state.chat_id,
                                     user_identity=user_identity)

    debug_payload = {
        "route_reason": result.get("route_reason", ""),
        "needs_llm": result.get("needs_llm", False),
        "dataset_id": result.get("dataset_id", ""),
        "parsed_params": result.get("parsed_params", {}),
        "resolved_params": _clean_resolved(result.get("resolved_params", {})),
        "soql_query": result.get("soql_query", ""),
        "results_count": len(result.get("results", [])),
    }

    st.session_state.history.append(
        {
            "role": "assistant",
            "content": result.get("response", ""),
            "rows": result.get("rows", []),
            "debug": debug_payload,
            "trace_id": result.get("trace_id"),
            "rating": None,
            "contextual_filters": result.get("contextual_filters", {}),
            "total_count": result.get("total_count", 0),
        }
    )
    st.rerun()
