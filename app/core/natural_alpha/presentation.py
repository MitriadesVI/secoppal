"""Rerun-stable UI projection: proposal labels separated from host resolution."""
from .admission import eligible, mode
from .catalog import LABELS
from .telemetry import trace_metadata


def project(result: dict) -> dict | None:
    metadata = trace_metadata(result)
    if metadata.get("natural_alpha_status") not in {"MATCHED", "MULTI_JOB_BLOCKED"}:
        return None
    jobs = metadata["natural_alpha_jobs"]
    if not jobs:
        return None
    blocked = metadata["natural_alpha_status"] == "MULTI_JOB_BLOCKED"
    resolved = []
    if not blocked:
        params = result.get("resolved_params") or {}
        for key, label in (("entidad", "Entidad"), ("departamento", "Departamento"),
                           ("ciudad", "Ciudad"), ("objeto", "Tema"),
                           ("valor_min", "Valor mínimo"), ("valor_max", "Valor máximo")):
            value = params.get(key)
            if type(value) is str and value:
                resolved.append([label, value])
            elif type(value) in (int, float):
                resolved.append([label, f"${value:,.0f}"])
            elif type(value) is list and value and all(type(item) is str for item in value):
                resolved.append([label, ", ".join(value)])
        resolved.append(["Ruta", _host_route(result)])
    return {"labels": [LABELS[job["aspect"]] for job in jobs], "resolved": resolved,
            "legacy_ran": not blocked}


def _host_route(result: dict) -> str:
    if result.get("needs_clarification"):
        return "Solicitud de aclaración"
    if result.get("query_error"):
        return "Consulta sin completar"
    reason = result.get("route_reason") or ""
    if reason.startswith("entity_profile"):
        return "Perfil de entidad"
    if reason.startswith("actor"):
        return "Perfil de actor"
    if result.get("analytical_intent"):
        return "Análisis de contratación"
    params = result.get("resolved_params") or {}
    if params.get("intent_type") == "opportunity_search":
        return "Búsqueda de oportunidades"
    return "Búsqueda"


def render(st, view: dict | None, *, settings, identity: dict | None) -> None:
    if not view or mode(settings) == "off" or not eligible(settings, "streamlit", identity):
        return
    with st.expander("Alfa entendió", expanded=False):
        for index, label in enumerate(view["labels"], 1):
            st.text(f"{index}. {label}" if len(view["labels"]) > 1 else label)
        if view["legacy_ran"]:
            st.caption("SECOPPAL resolvió")
            for label, value in view["resolved"]:
                st.text(f"{label}: {value}")
