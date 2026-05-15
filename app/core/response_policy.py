"""response_policy.py — Bloque 3: Política de respuesta asesora para SECOPPAL.

Consolida la construcción de respuestas en un solo lugar determinístico,
con tres caminos según el estado de la consulta:

1. Consulta ambigua (needs_clarification) → mensaje de aclaración con caminos
2. Sin resultados (rows vacío) → opciones sin ejecutar relajación
3. Consulta clara con resultados → estructura completa de 5 secciones

El narrator solo se usa en la sección "lectura rápida" y solo si hay
señales del observer.
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING

from app.utils.money import format_cop, safe_money

if TYPE_CHECKING:
    from app.core.observer import UniverseInsights
    from app.core.narrator import NarratorHandler


# ---------------------------------------------------------------------------
# build_advisor_response — punto de entrada público
# ---------------------------------------------------------------------------

def build_advisor_response(
    context: dict,
    narrator: "NarratorHandler | None" = None,
) -> str:
    """Construye la respuesta asesora completa según el contexto.

    Args:
        context: dict con user_query, resolved_params, total_count, rows,
                 universe_insights, suggestions, dataset_id, channel,
                 needs_clarification, clarification_reason, degraded, degraded_hint
        narrator: NarratorHandler opcional para la sección "lectura rápida"

    Returns:
        str con la respuesta formateada para el canal.
    """
    needs_clarification = context.get("needs_clarification", False)
    clarification_reason = context.get("clarification_reason", "")
    query_error = context.get("query_error", "")
    timeout_suggestions = context.get("timeout_suggestions", [])
    resolved = context.get("resolved_params", {})
    rows = context.get("rows", [])
    total_count = context.get("total_count", 0)
    suggestions = context.get("suggestions", [])
    insights = context.get("universe_insights")
    channel = context.get("channel", "streamlit")

    # ── Camino 0: Error de SECOP ─────────────────────────────────────
    if query_error:
        return (
            "SECOP no respondio a tiempo. La consulta esta bien formada, "
            "pero el portal se demoro.\n\n"
            "Puedo intentar:\n"
            "1. Reducir el rango a este ano.\n"
            "2. Quitar el orden por valor.\n"
            "3. Buscar sin filtro de fecha.\n\n"
            "El detalle tecnico queda registrado en el trace para revision."
        )

    # ── Camino 1: Consulta ambigua ──────────────────────────────────────
    if needs_clarification:
        return _build_ambiguous_response(clarification_reason, resolved)

    # ── Camino 2: Sin resultados ────────────────────────────────────────
    if not rows:
        return _build_no_results_response(resolved, context.get("dataset_id", ""))

    # ── Camino 3: Consulta clara con resultados ─────────────────────────
    response = _build_clear_response(
        resolved=resolved,
        rows=rows,
        total_count=total_count,
        insights=insights,
        suggestions=suggestions,
        narrator=narrator,
        query=context.get("user_query", ""),
        channel=channel,
        degraded=context.get("degraded", False),
        degraded_hint=context.get("degraded_hint", ""),
    )
    # Timeout retry: aviso de estrategia cambiada
    if timeout_suggestions:
        note = (
            "Entendi la busqueda, pero SECOP se demoro al ordenar todos los historicos por valor. "
            "Cambie a orden por fecha mas reciente.\n\n"
        )
        response = note + response
    return response


# ---------------------------------------------------------------------------
# Camino 1: Consulta ambigua
# ---------------------------------------------------------------------------

def _build_ambiguous_response(clarification_reason: str, resolved: dict) -> str:
    """Construye respuesta para consulta ambigua con caminos sugeridos."""
    lines = [clarification_reason]
    paths = _suggest_paths_from_partial_params(resolved)
    if paths:
        lines.append("")
        lines.append("Puedo buscar de varias formas:")
        for i, p in enumerate(paths, start=1):
            lines.append(f"{i}. {p}")
        lines.append("")
        lines.append("Cual camino quieres?")
    return "\n".join(lines)


def _suggest_paths_from_partial_params(resolved: dict) -> list[str]:
    """Genera caminos de búsqueda a partir de params parciales."""
    paths: list[str] = []
    dataset = resolved.get("dataset", "")
    label = "Contratos" if dataset == "contratos" else "Procesos"
    depto = resolved.get("departamento_resolved", "")
    ciudad = resolved.get("ciudad", "")
    loc = depto or ciudad

    if loc:
        paths.append(f"{label} mas recientes en {loc}")
        paths.append(f"{label} de mayor valor en {loc}")
        paths.append(f"{label} de mantenimiento en {loc}")
    else:
        paths.append(f"{label} mas recientes")
        paths.append(f"{label} de mayor valor")
        paths.append(f"{label} de mantenimiento vial")
    return paths[:3]


# ---------------------------------------------------------------------------
# Camino 2: Sin resultados
# ---------------------------------------------------------------------------

def _build_no_results_response(resolved: dict, dataset_id: str) -> str:
    """Construye respuesta para cero resultados con opciones sin relajar."""
    filters = _summarize_active_filters(resolved)
    lines = ["No encontre resultados exactos con esos filtros."]
    if filters:
        lines.append("")
        lines.append("La busqueda combinaba:")
        for f in filters:
            lines.append(f"- {f}")
        lines.append("")
        lines.append("Puedo intentar:")
        # Opciones basadas en filtros activos
        opts = []
        for f_str in filters:
            if "fecha" in f_str.lower():
                opts.append("quitar el filtro de fecha")
            elif "valor" in f_str.lower():
                opts.append("quitar el filtro de valor")
            elif "estado" in f_str.lower():
                opts.append("quitar el filtro de estado")
            elif "departamento" in f_str.lower() or "ciudad" in f_str.lower():
                opts.append("buscar en todo el pais")
        if not opts:
            opts = ["ampliar el tema", "quitar filtros", f"buscar en {'procesos' if 'jbjy' in dataset_id else 'contratos'}"]
        for i, opt in enumerate(opts[:3], start=1):
            lines.append(f"{i}. {opt}")
    return "\n".join(lines)


def _format_objeto_terms(objeto: list) -> list[str]:
    terms: list[str] = []
    for item in objeto:
        if isinstance(item, list):
            terms.append(" o ".join(str(sub_item) for sub_item in item))
        else:
            terms.append(str(item))
    return terms


def _summarize_active_filters(resolved: dict) -> list[str]:
    """Lista los filtros activos en formato humano."""
    filters: list[str] = []
    if resolved.get("objeto"):
        filters.append(f"tema: {', '.join(_format_objeto_terms(resolved['objeto']))}")
    if resolved.get("entidad_resolved"):
        filters.append(f"entidad: {resolved['entidad_resolved']}")
    if resolved.get("departamento_resolved"):
        filters.append(f"departamento: {resolved['departamento_resolved']}")
    if resolved.get("ciudad"):
        filters.append(f"ciudad: {resolved['ciudad']}")
    if resolved.get("fecha_desde") or resolved.get("fecha_hasta"):
        desde = resolved.get("fecha_desde", "")
        hasta = resolved.get("fecha_hasta", "")
        if desde and hasta:
            filters.append(f"fecha: {desde} a {hasta}")
        elif desde:
            filters.append(f"desde: {desde}")
        elif hasta:
            filters.append(f"hasta: {hasta}")
    if resolved.get("valor_min") is not None:
        filters.append(f"valor minimo: {format_cop(resolved['valor_min'])}")
    if resolved.get("valor_max") is not None:
        filters.append(f"valor maximo: {format_cop(resolved['valor_max'])}")
    if resolved.get("estado"):
        filters.append(f"estado: {resolved['estado']}")
    return filters


# ---------------------------------------------------------------------------
# Camino 3: Consulta clara con resultados
# ---------------------------------------------------------------------------

def _build_clear_response(
    resolved: dict,
    rows: list[dict],
    total_count: int,
    insights: "UniverseInsights | None",
    suggestions: list,
    narrator: "NarratorHandler | None",
    query: str,
    channel: str,
    degraded: bool,
    degraded_hint: str,
) -> str:
    """Construye respuesta completa con 5 secciones."""
    sections: list[str] = []

    # Nota de degradación si aplica
    if degraded and degraded_hint:
        sections.append(f"⚠️ No encontre resultados exactos. Te muestro resultados {degraded_hint}:\n")

    # 1. Interpretación
    interpretation = _build_interpretation(resolved)
    sections.append(f"📋 {interpretation}")

    # 2. Universo
    shown = len(rows)
    ordering = _build_ordering_label(resolved)
    if total_count > shown:
        sections.append(f"Encontre {total_count:,} resultados. Te muestro los {shown} {ordering}:\n")
    else:
        sections.append(f"Encontre {shown} resultados {ordering}:\n")

    # 3. Lectura rápida
    lectura = _build_lectura_rapida(insights, narrator, rows, query, channel, resolved, total_count)
    if lectura:
        sections.append(lectura + "\n")

    # 4. Resultados (lista numerada)
    # En Streamlit, las filas se renderizan aparte como tarjetas/tabla. Incluir
    # la lista aquí duplica visualmente los mismos resultados.
    if channel != "streamlit":
        result_lines = _format_results_list(rows)
        sections.append(result_lines)

    # 5. Sugerencias
    if suggestions:
        sug_lines = _format_suggestions_list(suggestions)
        sections.append(sug_lines)

    return "\n".join(sections)


# ── Helpers ──────────────────────────────────────────────────────────────

def _build_interpretation(resolved: dict) -> str:
    """Convierte resolved_params a frase humana."""
    dataset = resolved.get("dataset", "")
    label = "Contratos" if dataset == "contratos" else "Procesos"
    obj = resolved.get("objeto", [])
    ent = resolved.get("entidad_resolved") or resolved.get("entidad_like") or resolved.get("entidad", "")
    depto = resolved.get("departamento_resolved", "")
    ciudad = resolved.get("ciudad", "")

    parts: list[str] = [label]

    if obj:
        parts.append("de " + ", ".join(_format_objeto_terms(obj[:3])))

    if depto:
        parts.append(f"en {depto}")
    elif ciudad:
        parts.append(f"en {ciudad}")
    elif ent:
        parts.append(f"de {ent}")

    desde = resolved.get("fecha_desde", "")
    if desde:
        parts.append(f"desde {desde[:4]}")

    valor_min = resolved.get("valor_min")
    if valor_min is not None:
        parts.append(f"mayores a {format_cop(valor_min)}")

    return " ".join(parts)


def _build_ordering_label(params: dict) -> str:
    """Etiqueta de ordenamiento para el header del universo."""
    ordering = params.get("ordering_signal", "")
    if ordering == "valor_desc":
        return "de mayor valor"
    return "más recientes"


def _build_lectura_rapida(
    insights: "UniverseInsights | None",
    narrator: "NarratorHandler | None",
    rows: list[dict],
    query: str,
    channel: str,
    resolved: dict | None = None,
    total_count: int = 0,
) -> str | None:
    """Construye sección 'lectura rápida'.

    Guardrail: con ordering_signal=valor_desc, el LLM no participa. La lectura
    usa solo hechos determinísticos de rows/insights para evitar rankings o
    montos inventados.
    """
    resolved = resolved or {}

    if resolved.get("ordering_signal") == "valor_desc":
        value_summary = _top_values_summary(rows)
        if value_summary:
            return value_summary
        if insights is None:
            return None

    if insights is None:
        return None

    # Verificar si hay al menos una señal True
    has_any_signal = any([
        getattr(insights, "has_dominant_entity", False),
        getattr(insights, "has_temporal_concentration", False),
        getattr(insights, "has_value_outlier", False),
        getattr(insights, "has_diverse_modalities", False),
    ])

    if not has_any_signal:
        return None

    # Intentar narrator si está disponible
    if narrator:
        try:
            result = narrator.narrate(
                rows=rows, query=query, channel=channel,
                strict=True, universe_insights=insights, suggestions=None,
            )
            if result and not _contains_numeric_claim(result):
                return result
        except Exception:
            pass

    # Fallback determinístico
    return _insights_summary_deterministico(insights)


def _top_values_summary(rows: list[dict], limit: int = 3) -> str | None:
    """Resume top valores desde rows ya formateados, sin LLM."""
    valued_rows: list[tuple[float, dict]] = []
    for row in rows:
        try:
            value = safe_money(row.get("valor"))
        except Exception:
            continue
        if value > 0:
            valued_rows.append((value, row))

    if not valued_rows:
        return None

    valued_rows.sort(key=lambda item: item[0], reverse=True)
    parts: list[str] = []
    labels = ["mayor valor mostrado", "segundo", "tercero"]
    for idx, (value, row) in enumerate(valued_rows[:limit]):
        provider = row.get("contratista") or row.get("entidad") or "sin contratista visible"
        date = (row.get("fecha") or "")[:10]
        suffix = f" ({provider}"
        if date:
            suffix += f", {date}"
        suffix += ")"
        if idx == 0:
            parts.append(f"El {labels[idx]} es {format_cop(value)}{suffix}")
        else:
            parts.append(f"{labels[idx]}: {format_cop(value)}{suffix}")

    return "Lectura rapida: " + "; ".join(parts) + "."


def _contains_numeric_claim(text: str) -> bool:
    """True si el narrator intenta introducir cifras/rankings en lectura rápida."""
    return bool(re.search(r"\d", text))


def _insights_summary_deterministico(insights) -> str:
    """Versión determinística del resumen de insights (sin LLM)."""
    lines: list[str] = []
    if getattr(insights, "has_dominant_entity", False):
        name = getattr(insights, "dominant_entity_name", None)
        pct = getattr(insights, "dominant_entity_pct", None)
        if name and pct:
            lines.append(f"El {pct:.0f}% son de {name}.")
    if getattr(insights, "has_temporal_concentration", False):
        yr = getattr(insights, "dominant_year", None)
        if yr:
            lines.append(f"La mayoria son de {yr}.")
    if getattr(insights, "has_value_outlier", False):
        val = getattr(insights, "outlier_value", None)
        vs = getattr(insights, "value_stats", None)
        if val:
            median_str = ""
            if vs and vs.get("median"):
                try:
                    median_str = f" (la mediana es {format_cop(vs['median'])})"
                except Exception:
                    pass
            lines.append(f"Hay uno de {format_cop(val)}{median_str}.")
    if getattr(insights, "has_diverse_modalities", False):
        lines.append("Hay variedad de modalidades de contratacion.")
    return "\n".join(lines)


def _format_results_list(rows: list[dict]) -> str:
    """Formatea rows como lista numerada estilo asesor."""
    lines: list[str] = []
    for i, row in enumerate(rows, start=1):
        titulo = row.get("titulo", "Sin titulo")
        entidad = row.get("entidad", "")
        valor = row.get("valor", "")
        estado = row.get("estado", "")
        fecha = (row.get("fecha") or "")[:10]
        contratista = row.get("contratista", "")
        url = row.get("url", "")

        parts = [f"{i}. {titulo}"]
        if entidad:
            parts.append(f"   {entidad}")
        detail = f"   {valor}"
        if estado:
            detail += f" | {estado}"
        if fecha:
            detail += f" | {fecha}"
        parts.append(detail)
        if contratista:
            parts.append(f"   {contratista}")
        if url:
            parts.append(f"   {url}")
        lines.append("\n".join(parts))
    return "\n\n".join(lines)


def _format_suggestions_list(suggestions: list) -> str:
    """Formatea sugerencias como lista numerada."""
    lines = ["\n💡 Sugerencias:"]
    for i, s in enumerate(suggestions, start=1):
        label = getattr(s, "label", str(s)) if not isinstance(s, dict) else s.get("label", "")
        if label:
            lines.append(f"{i}. {label}")
    return "\n".join(lines)
