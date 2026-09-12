"""Small Streamlit renderer for the versioned Entity Profile overview view."""
from __future__ import annotations

from app.config import entity_profile_frame_enabled
from app.core.entity_profile.presentation import (
    ENTITY_PROFILE_VIEW_KIND,
    ENTITY_PROFILE_VIEW_SCHEMA,
)
from app.core.entity_profile.executive_actions import presented_action_ids


_SOURCE_INFO = {
    "p6dx-8zbt": ("SECOP II Procesos", "https://www.datos.gov.co/resource/p6dx-8zbt"),
    "jbjy-vk9h": ("SECOP II Contratos", "https://www.datos.gov.co/resource/jbjy-vk9h"),
}


def escape_entity_profile_markdown(text: str) -> str:
    """Prevent paired currency signs from becoming Streamlit math delimiters."""
    return text.replace("$", r"\$")


def render_entity_profile_content(
    st, view: object, content: str, *, is_profile: bool,
    action_reference: object = None,
) -> None:
    """Render a view or its profile-only currency-safe textual fallback."""
    if render_entity_profile_view(st, view, action_reference=action_reference):
        return
    st.markdown(
        escape_entity_profile_markdown(content)
        if is_profile or view is not None else content,
    )


def render_entity_profile_view(st, view: object, *, action_reference: object = None) -> bool:
    """Render a supported overview; return False for canonical-text fallback."""
    if not _supported(view):
        return False
    header = view["header"]
    st.subheader(_escape_text(header["display_name"]))
    context = " · ".join(
        _escape_text(value) for value in (header.get("period"), header.get("territory"))
        if isinstance(value, str) and value
    )
    if context:
        st.caption(context)

    columns = st.columns(4)
    for column, metric in zip(columns, view["kpis"], strict=True):
        column.metric(metric["label"], metric["display"])

    _render_modalities(st, view.get("modalities"))
    _render_suppliers(st, view.get("suppliers"))
    _render_recent(st, view.get("recent"))
    _render_actions(st, view.get("actions"), action_reference)
    _render_scope(st, view)
    warnings = view.get("warnings")
    for warning in warnings if isinstance(warnings, (list, tuple)) else ():
        if isinstance(warning, str) and warning:
            st.warning(_escape_text(warning))
    if isinstance(view.get("next_steps"), str) and view["next_steps"]:
        st.caption(_escape_text(view["next_steps"]))
    return True


def _render_actions(st, raw: object, reference: object) -> None:
    if not entity_profile_frame_enabled():
        return
    action_ids = presented_action_ids(raw)
    if not action_ids or type(reference) is not dict:
        return
    labels = {item["id"]: item["label"] for item in raw}
    columns = st.columns(len(action_ids))
    for column, action_id in zip(columns, action_ids, strict=True):
        if column.button(
            labels[action_id],
            key=f"profile_action_{reference.get('result_id', 'missing')}_{action_id}",
            use_container_width=True,
        ):
            st.session_state._pending_profile_action = {
                "action_id": action_id,
                "reference": dict(reference),
            }
            st.rerun()


def _supported(view: object) -> bool:
    if not isinstance(view, dict):
        return False
    if (
        view.get("kind") != ENTITY_PROFILE_VIEW_KIND
        or view.get("schema_version") != ENTITY_PROFILE_VIEW_SCHEMA
        or view.get("focus") != "overview"
    ):
        return False
    header, kpis = view.get("header"), view.get("kpis")
    return (
        isinstance(header, dict)
        and isinstance(header.get("display_name"), str)
        and isinstance(kpis, list)
        and len(kpis) == 4
        and all(_valid_metric(item) for item in kpis)
    )


def _valid_metric(item: object) -> bool:
    return (
        isinstance(item, dict)
        and isinstance(item.get("label"), str)
        and isinstance(item.get("display"), str)
        and item.get("status") in {"available", "unavailable"}
    )


def _escape_text(value: str) -> str:
    for char in "\\`*_{}[]<>()#+-.!|$":
        value = value.replace(char, "\\" + char)
    return value


def _render_modalities(st, rows: object) -> None:
    if not isinstance(rows, list) or not rows:
        return
    st.markdown("#### Cómo contrata")
    for row in rows:
        if isinstance(row, dict) and all(
            isinstance(row.get(key), str) for key in ("name", "count", "share")
        ):
            st.text(f"{row['name']} · {row['share']} · {row['count']} procesos")


def _render_suppliers(st, suppliers: object) -> None:
    if not isinstance(suppliers, dict):
        return
    cards = (("Por valor", suppliers.get("by_value")), ("Por número", suppliers.get("by_count")))
    if not any(isinstance(card, dict) for _, card in cards):
        return
    st.markdown("#### Principales contratistas")
    for label_text, card in cards:
        if isinstance(card, dict) and isinstance(card.get("name"), str) and isinstance(card.get("value"), str):
            st.caption(label_text)
            st.text(card["name"])
            st.text(card["value"])


def _render_recent(st, recent: object) -> None:
    if not isinstance(recent, dict):
        return
    cards = (("Proceso reciente", recent.get("process")), (recent.get("contract_label", "Contrato reciente"), recent.get("contract")))
    if not any(isinstance(card, dict) for _, card in cards):
        return
    st.markdown("#### Actividad reciente")
    for label_text, card in cards:
        if not isinstance(card, dict):
            continue
        st.caption(_escape_text(str(label_text)))
        st.text(card.get("excerpt", ""))
        if isinstance(card.get("date"), str) and card["date"]:
            st.caption(_escape_text(card["date"]))
        full = card.get("full_title")
        if isinstance(full, str) and full != card.get("excerpt"):
            with st.expander("Ver título completo"):
                st.text(full)


def _render_scope(st, view: dict) -> None:
    with st.expander("Fuentes y alcance"):
        sources = view.get("sources")
        for source in sources if isinstance(sources, list) else ():
            if not isinstance(source, dict) or source.get("id") not in _SOURCE_INFO:
                continue
            source_label, source_url = _SOURCE_INFO[source["id"]]
            st.markdown(f"[{source_label}]({source_url})")
        universe = view.get("universe")
        if isinstance(universe, dict) and isinstance(universe.get("label"), str):
            st.caption(_escape_text(universe["label"]))
        scope = view.get("scope")
        for line in scope if isinstance(scope, list) else ():
            if isinstance(line, str) and line:
                st.text(line)


__all__ = [
    "escape_entity_profile_markdown",
    "render_entity_profile_content",
    "render_entity_profile_view",
]
