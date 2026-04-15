"""
SECOPPAL — Streamlit UI
"""
from __future__ import annotations

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


# ═══════════════════════════════════════════════════════════════════════════
# Session state
# ═══════════════════════════════════════════════════════════════════════════

if "history" not in st.session_state:
    st.session_state.history: list[dict] = []

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

        rows = item.get("rows", [])
        if rows and show_cards:
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
# Chat input
# ═══════════════════════════════════════════════════════════════════════════

incoming_query: str | None = None
if hasattr(st.session_state, "_pending_example"):
    incoming_query = st.session_state._pending_example
    del st.session_state._pending_example

typed_query = st.chat_input("Escribe tu consulta sobre procesos o contratos SECOP")
if typed_query:
    incoming_query = typed_query

if incoming_query:
    st.session_state.history.append({"role": "user", "content": incoming_query})

    with st.spinner("Consultando SECOP..."):
        result = workflow.run_query(incoming_query, channel="streamlit")

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
        }
    )
    st.rerun()
