"""digest.py — convierte un RadarDiff en lectura asesora.

No manda 50 filas: manda una lectura (sección 3, Nivel 3 del doc de producto).
Plantilla en español, tono asesor. Incluye:

- conteo de novedades (nuevos / cambios de estado / cambios de valor / desaparecidos);
- top 3 resultados nuevos con valor y estado;
- bandera de valor atípico (heurística simple sobre el rango ya observado);
- ``risk_flag`` de ventana: si el universo excede la ventana revisada, avisa que
  puede haber novedades fuera de los N resultados más recientes (evita certeza falsa).

Devuelve un dict con ``text`` (lo que ve el usuario) + estructura para auditoría.
"""
from __future__ import annotations

from app.core.radar.diff import DiffItem, RadarDiff


def _coverage_note(status: str | None, *, baseline: bool = False) -> str | None:
    if status == "PARTIAL_DISCOVERY":
        if baseline:
            return "La cobertura inicial de nuevos resultados no fue exhaustiva."
        return "La revisión de nuevos resultados no fue exhaustiva."
    if status == "PARTIAL_MAINTENANCE":
        if baseline:
            return "No pude actualizar por completo el conjunto incorporado a vigilancia."
        return "No pude actualizar todo el conjunto vigilado."
    if status == "UNKNOWN":
        return "No puedo confirmar el estado completo de este Radar en esta revisión."
    return None


def _fmt_money(raw: str) -> str:
    if not raw:
        return "s/d"
    try:
        n = float(raw)
    except (ValueError, TypeError):
        return raw
    return f"${n:,.0f}".replace(",", ".")


def _to_float(raw: str) -> float | None:
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None


def _atypical_threshold(seen_map: dict[str, dict]) -> float | None:
    """Umbral simple de valor atípico: 2× el máximo valor ya observado.

    Devuelve ``None`` si no hay valores previos suficientes para comparar.
    """
    values = [
        v for v in (_to_float(rec.get("value", "")) for rec in seen_map.values())
        if v and v > 0
    ]
    if len(values) < 3:
        return None
    return max(values) * 2.0


def _format_item(item: DiffItem) -> str:
    title = item.fields.get("title") or "(sin título)"
    if len(title) > 90:
        title = title[:87] + "…"
    entity = item.fields.get("entity") or ""
    value = _fmt_money(item.fields.get("value", ""))
    status = item.fields.get("status") or "s/d"
    line = f"• {title}"
    if entity:
        line += f"\n  {entity}"
    line += f"\n  Valor: {value} · Estado: {status}"
    if item.contractor_new and item.fields.get("contractor"):
        line += f"\n  🆕 Operador nuevo: {item.fields['contractor']}"
    return line


def build_digest(
    radar_name: str,
    diff: RadarDiff,
    run_meta: dict,
    seen_map: dict[str, dict] | None = None,
) -> dict:
    """Construye el digest asesor de una corrida.

    ``run_meta`` espera: ``total``, ``approx_total``, ``window_limit``,
    ``approximate``. ``seen_map`` es el estado PREVIO (antes del upsert),
    usado para el umbral de valor atípico.
    """
    seen_map = seen_map or {}
    lines: list[str] = [f"🔔 Radar {radar_name}"]
    risk_flags: list[str] = []

    # Bandera de ventana: el universo supera lo que se alcanzó a revisar.
    window_limit = int(run_meta.get("window_limit", 100))
    total = int(run_meta.get("total", 0) or 0)
    approximate = bool(run_meta.get("approximate"))
    if approximate or total > window_limit:
        risk_flags.append(
            f"⚠️ Radar amplio: se revisaron los {window_limit} resultados más "
            "recientes. Puede haber novedades fuera de esta ventana."
        )
    if run_meta.get("seen_refresh_limited"):
        checked = int(run_meta.get("seen_refresh_checked", 0) or 0)
        limit = int(run_meta.get("seen_refresh_limit", checked) or checked)
        risk_flags.append(
            f"⚠️ Radar con muchos resultados vistos: se refrescaron {checked} "
            f"por clave en esta corrida (límite {limit})."
        )
    if run_meta.get("seen_refresh_errors"):
        errors = int(run_meta.get("seen_refresh_errors", 0) or 0)
        risk_flags.append(
            f"⚠️ No se pudieron refrescar {errors} resultado(s) visto(s) por clave. "
            "No se marcaron como desaparecidos."
        )

    coverage_status = run_meta.get("coverage_status")
    run_kind = run_meta.get("run_kind")
    baseline_count = int(run_meta.get("baseline_count", 0) or 0)
    structural_risk_count = len(risk_flags)

    # La primera materialización tiene novedades técnicas respecto de un store
    # vacío, pero en producto establece vigilancia: nunca se presenta como
    # oportunidades nuevas ni muestra el top de backfill.
    if run_kind == "baseline":
        lines.append("")
        lines.append(
            f"Línea base creada · {baseline_count} resultados bajo vigilancia."
        )
        coverage_note = _coverage_note(coverage_status, baseline=True)
        if coverage_note:
            lines.append(coverage_note)
        return _result(lines, diff, risk_flags, atypical=[], run_meta=run_meta)

    if not diff.has_novelty:
        lines.append("")
        if coverage_status == "COMPLETE":
            lines.append(
                "Todo al día. No hay novedades desde la última revisión completa."
            )
        elif coverage_status == "PARTIAL_DISCOVERY":
            lines.append(
                "No detecté novedades en la cobertura revisada. "
                "La revisión de nuevos resultados no fue exhaustiva."
            )
        elif coverage_status == "PARTIAL_MAINTENANCE":
            lines.append(
                "No detecté cambios en los procesos revisados, pero no pude "
                "actualizar todo el conjunto vigilado."
            )
        else:
            lines.append(
                "No puedo confirmar el estado completo de este Radar en esta revisión."
            )
        return _result(lines, diff, risk_flags, atypical=[], run_meta=run_meta)

    # Resumen de conteos.
    parts: list[str] = []
    if diff.new:
        parts.append(f"{len(diff.new)} nuevos")
    if diff.changed_state:
        parts.append(f"{len(diff.changed_state)} cambios de estado")
    if diff.changed_value:
        parts.append(f"{len(diff.changed_value)} cambios de valor")
    if diff.disappeared:
        parts.append(f"{len(diff.disappeared)} desaparecidos")
    total_nov = diff.new_count + diff.changed_count + len(diff.disappeared)
    lines.append("")
    lines.append(f"Desde la última revisión encontré {total_nov} novedades: "
                 + ", ".join(parts) + ".")

    # Valor atípico entre los nuevos.
    atypical: list[str] = []
    threshold = _atypical_threshold(seen_map)
    if threshold is not None:
        for item in diff.new:
            v = _to_float(item.fields.get("value", ""))
            if v and v >= threshold:
                atypical.append(item.key)
        if atypical:
            risk_flags.append(
                f"⚠️ {len(atypical)} resultado(s) con valor atípico frente al "
                "rango observado — conviene revisar."
            )

    # Top 3 nuevos.
    if diff.new:
        lines.append("")
        lines.append("Nuevos (top 3):")
        for item in diff.new[:3]:
            lines.append(_format_item(item))

    # Cambios de estado.
    if diff.changed_state:
        lines.append("")
        lines.append("Cambios de estado:")
        for item in diff.changed_state[:3]:
            title = (item.fields.get("title") or "(sin título)")[:80]
            lines.append(
                f"• {title}\n  {item.prev_status or 's/d'} → "
                f"{item.fields.get('status') or 's/d'}"
            )

    coverage_note = _coverage_note(coverage_status)
    if coverage_note:
        lines.append("")
        lines.append(coverage_note)

    # Con coverage explícita, el copy anterior ya comunica la limitación una
    # vez. Conservamos todos los flags en la estructura de auditoría y solo
    # renderizamos aquí riesgos adicionales (p. ej. valor atípico).
    human_flags = (
        risk_flags[structural_risk_count:]
        if coverage_status is not None else risk_flags
    )
    for flag in human_flags:
        lines.append("")
        lines.append(flag)

    return _result(lines, diff, risk_flags, atypical, run_meta=run_meta)


def _result(lines: list[str], diff: RadarDiff, risk_flags: list[str],
            atypical: list[str], run_meta: dict | None = None) -> dict:
    run_meta = run_meta or {}
    return {
        "text": "\n".join(lines),
        "new_count": diff.new_count,
        "changed_count": diff.changed_count,
        "disappeared_count": len(diff.disappeared),
        "window_churn_count": len(diff.window_churn),
        "seen_refresh_checked": int(run_meta.get("seen_refresh_checked", 0) or 0),
        "seen_refresh_found": int(run_meta.get("seen_refresh_found", 0) or 0),
        "seen_refresh_limited": bool(run_meta.get("seen_refresh_limited")),
        "risk_flags": risk_flags,
        "atypical_keys": atypical,
        "has_novelty": diff.has_novelty,
        "run_kind": run_meta.get("run_kind"),
        "baseline_count": run_meta.get("baseline_count"),
        "run_trigger": run_meta.get("run_trigger"),
        "coverage_status": run_meta.get("coverage_status"),
        "coverage_reason": run_meta.get("coverage_reason"),
    }
