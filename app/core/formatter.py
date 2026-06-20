from __future__ import annotations

import re
from typing import Any

from app.utils.money import format_cop, format_money
from app.utils.value_sanity import detect_value_anomaly



def _resultado(n: int) -> str:
    return "resultado" if n == 1 else "resultados"

# B2: patrones que indican que el "título" del proceso es en realidad un
# nombre de razón social o persona, no la descripción del objeto.
_LEGAL_FORM_SUFFIX_RE = re.compile(
    r"\b(?:SAS|S\.A\.S\.?|SA|S\.A\.?|LTDA|L\.T\.D\.A\.?|"
    r"E\.S\.E\.?|ESE|E\.U\.?|EU|S\s*EN\s*C|"
    r"CIA|COMPAÑIA|COMPAÑÍA|EMPRESA UNIPERSONAL)\b\.?$",
    re.IGNORECASE,
)
_PERSON_NAME_RE = re.compile(
    r"^[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-záéíóúñ']+(?:\s+[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-záéíóúñ']+){1,3}$"
)
_CONTRACTUAL_KEYWORDS = (
    "mantenimiento", "construccion", "construcción", "suministro",
    "servicio", "obra", "consultoria", "consultoría",
    "interventoria", "interventoría", "compra", "adquisicion",
    "adquisición", "transporte", "alimentacion", "alimentación",
    "prestacion", "prestación", "contratar", "contratacion",
    "contratación", "dotacion", "dotación",
)


def _looks_like_provider_name(text: str) -> bool:
    """True si el texto parece razón social o nombre propio, no descripción."""
    if not text:
        return False
    text = text.strip()
    if len(text) < 5:
        return False
    if _LEGAL_FORM_SUFFIX_RE.search(text):
        return True
    word_count = len(text.split())
    if 2 <= word_count <= 4 and _PERSON_NAME_RE.match(text):
        if not any(kw in text.lower() for kw in _CONTRACTUAL_KEYWORDS):
            return True
    return False


def _smart_title(item: dict, dataset_id: str) -> str:
    """Elige el mejor título: descripción si el nombre parece razón social."""
    if dataset_id == "p6dx-8zbt":
        primary = item.get("nombre_del_procedimiento", "")
        fallback = item.get("descripci_n_del_procedimiento", "")
    else:
        primary = item.get("objeto_del_contrato", "")
        fallback = item.get("descripcion_del_proceso", "")
    primary = str(primary or "").strip()
    fallback = str(fallback or "").strip()
    if _looks_like_provider_name(primary) and fallback:
        return fallback
    return primary or fallback or "Sin nombre"


def escape_dollars_for_streamlit(text: str) -> str:
    """B3: Streamlit interpreta `$X$` como LaTeX inline math y rompe el render
    de cifras monetarias ("Hay uno de $363M" termina mostrándose con backticks
    o como ecuación corrupta). Escapamos `$` solo cuando va antes de un dígito
    para evitar romper otros usos legítimos del símbolo.
    """
    if not text:
        return text
    return re.sub(r"\$(?=\d)", r"\\$", text)


def _fix_mojibake(text: str) -> str:
    """MOJIBAKE-DISPLAY-001: Normalize SECOP mojibake (Ã³→ó, Ã±→ñ).
    Only applied to SECOP-sourced text, not emoji or formatting chars."""
    replacements = {
        "Ã¡": "á", "Ã©": "é", "Ã­": "í", "Ã³": "ó", "Ãº": "ú",
        "Ã±": "ñ", "Ã": "Á", "Ã‰": "É", "Ã“": "Ó", "Ãš": "Ú",
        "Ã‘": "Ñ",
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    return text


_ACTIVE_STATES_PROCESOS = frozenset({"Abierto", "Publicado"})
_ACTIVE_STATES_CONTRATOS = frozenset({"En ejecución", "Modificado", "Prorrogado"})


def _reorder_active_first(results: list[dict], dataset_id: str, params: dict | None) -> list[dict]:
    """DOGFOOD-SOQL-006: pone activos primero en Python.

    Antes lo hacía SoQL con CASE WHEN IN (...), pero Socrata revienta con
    strings acentuados ("En ejecución"). Sólo se aplica cuando el usuario
    no filtró estado y no es búsqueda histórica/oportunidad — mismas
    condiciones que el ORDER BY antes activaba.
    """
    if not results:
        return results
    params = params or {}
    has_estado = bool(
        params.get("estado") or params.get("estado_family")
        or params.get("estado_de_apertura_del_proceso")
        or params.get("estado_del_procedimiento")
        or params.get("estado_contrato")
    )
    intent = (params.get("intent_type") or "").lower()
    if has_estado or "historical" in intent or "analysis" in intent or intent == "opportunity_search":
        return results

    if dataset_id == "p6dx-8zbt":
        active = _ACTIVE_STATES_PROCESOS
        field = "estado_de_apertura_del_proceso"
    else:
        active = _ACTIVE_STATES_CONTRATOS
        field = "estado_contrato"

    def _key(idx_row: tuple[int, dict]) -> tuple[int, int]:
        idx, row = idx_row
        is_active = 0 if str(row.get(field, "") or "").strip() in active else 1
        return (is_active, idx)  # estable: idx preserva orden por fecha del SoQL

    return [r for _, r in sorted(enumerate(results), key=_key)]


def _dedupe_results(results: list[dict], dataset_id: str) -> list[dict]:
    """B1: deduplica por id estable conservando el primero encontrado.

    SECOP entrega varias 'versiones' del mismo proceso/contrato cuando hay
    modificaciones. Mostrar 10 versiones del mismo proceso ocupa el cupo de
    la respuesta. Se conserva el primero (más reciente por ORDER BY del SoQL).
    """
    seen: set[str] = set()
    deduped: list[dict] = []
    if dataset_id == "p6dx-8zbt":
        key_fields = ("id_del_proceso", "referencia_del_proceso")
    else:
        key_fields = ("id_contrato", "referencia_del_contrato")
    for item in results:
        key = ""
        for f in key_fields:
            v = item.get(f)
            if v:
                key = str(v)
                break
        if not key:
            deduped.append(item)
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def rank_and_dedupe(results: list[dict], dataset_id: str, params: dict | None = None) -> list[dict]:
    """Pipeline de display SIN el corte a max_results.

    Reproduce, en un solo lugar, el orden que el usuario ve en el primer
    turno: BM25 (cuando el objeto tiene ≥3 tokens) → activos primero → dedupe.
    Es el mismo orden que ``normal_flow`` (BM25) + ``to_rows`` (reorder+dedupe)
    producen, pero devuelve la lista COMPLETA en vez de los primeros 25.

    La paginación A-buffer lo usa para re-pescar la misma ventana SECOP,
    reconstruir determinísticamente el bloque ordenado y cortar la siguiente
    rebanada (``[display_offset : display_offset+25]``) sin solaparse con lo ya
    mostrado. Determinista por construcción: BM25, ``_reorder_active_first``
    (sort estable) y ``_dedupe_results`` no dependen de estado externo.
    """
    out = list(results or [])
    if not out:
        return out
    params = params or {}
    objeto_terms = params.get("objeto", []) or []
    topic_flat: list[str] = []
    for t in objeto_terms:
        if isinstance(t, list):
            topic_flat.extend(t)
        else:
            topic_flat.append(str(t))
    # BM25 re-rank — misma condición que normal_flow (≥3 tokens de objeto).
    if len(topic_flat) >= 3:
        try:
            from app.core.relevance import RelevanceScorer
            field = "objeto_del_contrato" if dataset_id == "jbjy-vk9h" else "nombre_del_procedimiento"
            scored = RelevanceScorer().score_rows(topic_flat, out, field=field)
            out = [r for r, _ in scored]
        except Exception:
            pass  # best-effort, igual que en normal_flow
    out = _reorder_active_first(out, dataset_id, params)
    out = _dedupe_results(out, dataset_id)
    return out


def build_no_results_message(params: dict | None, dataset_id: str | None = None) -> str:
    """Generate a helpful 'no results' message with specific relaxation suggestions.

    Inspects the params used in the failed query and suggests which filters
    to relax, in order of likely impact.
    """
    if not params:
        return (
            "No encontre resultados. Intenta con terminos mas generales "
            "o verifica el nombre de la entidad y el departamento."
        )

    suggestions: list[str] = []

    # Suggest relaxing estado — most common cause of zero results
    if params.get("estado") or params.get("estado_de_apertura_del_proceso") or params.get("estado_contrato"):
        suggestions.append("quita el filtro de estado (abierto/cerrado/adjudicado)")

    # Suggest relaxing date range
    if params.get("fecha_desde") or params.get("fecha_hasta"):
        suggestions.append("amplia el rango de fechas")

    # Suggest relaxing value range
    if params.get("valor_min") is not None or params.get("valor_max") is not None:
        suggestions.append("cambia el rango de valor")

    # Suggest relaxing entity
    if params.get("entidad_like") or params.get("entidad_resolved"):
        suggestions.append("verifica el nombre exacto de la entidad")

    # Suggest relaxing department
    if params.get("departamento_resolved"):
        suggestions.append("busca en todo el pais quitando el departamento")

    # Suggest simplifying object terms
    obj = params.get("objeto", [])
    if len(obj) > 1:
        suggestions.append(f'busca solo por \"{obj[0]}\" sin los otros terminos')
    elif len(obj) == 1:
        suggestions.append("usa un termino mas corto o general")

    # Dataset switch suggestion
    is_contratos = (dataset_id == "jbjy-vk9h") or params.get("dataset") == "contratos"
    if is_contratos:
        suggestions.append("busca en procesos abiertos en vez de contratos")
    else:
        suggestions.append("busca en contratos firmados en vez de procesos")

    if not suggestions:
        return (
            "No encontre resultados. Intenta con terminos mas generales "
            "o verifica el nombre de la entidad y el departamento."
        )

    suggestion_text = "; ".join(suggestions[:3])  # max 3 sugerencias
    return f"No encontre resultados con esos filtros. Puedes intentar: {suggestion_text}."


class Formatter:
    def __init__(self, max_results: int = 10):
        self.max_results = max_results

    def format_for_channel(self, results: list[dict], dataset_id: str, channel: str,
                           params: dict | None = None, total_count: int = 0,
                           universe_insights: Any = None,
                           suggestions: list | None = None,
                           total_count_approximate: bool = False) -> tuple[str, list[dict]]:
        rows = self.to_rows(results, dataset_id, params=params)
        if channel == "telegram":
            return self.format_telegram(rows, params=params, dataset_id=dataset_id, total_count=total_count, universe_insights=universe_insights, suggestions=suggestions, total_count_approximate=total_count_approximate), rows
        if channel == "streamlit":
            return self.format_streamlit(rows, params=params, dataset_id=dataset_id, total_count=total_count, universe_insights=universe_insights, suggestions=suggestions, total_count_approximate=total_count_approximate), rows
        return self.format_whatsapp(rows, params=params, dataset_id=dataset_id, total_count=total_count, universe_insights=universe_insights, suggestions=suggestions, total_count_approximate=total_count_approximate), rows

    @staticmethod
    def _best_date_for_row(item: dict, dataset_id: str) -> tuple[str, str]:
        """Cascada de fechas disponibles en p6dx-8zbt cuando publicación está vacía.

        Returns: (fecha_iso, etiqueta). Etiqueta indica fuente para que la UI
        pueda mostrarla con contexto. fecha_iso vacío → "Pendiente de publicación".
        """
        if dataset_id == "p6dx-8zbt":
            cascade = (
                ("fecha_de_publicacion_del", "Publicación"),
                ("fecha_de_publicacion_fase_3", "Publicación fase 3"),
                ("fecha_de_recepcion_de_respuestas_al_in", "Recepción ofertas"),
                ("fecha_de_apertura_efectiva", "Apertura"),
            )
        else:
            cascade = (
                ("fecha_de_firma", "Firma"),
                ("fecha_de_inicio_del_contrato", "Inicio"),
            )
        for field, label in cascade:
            val = item.get(field)
            if val:
                return str(val), label
        return "", "Pendiente de publicación"

    def to_rows(self, results: list[dict], dataset_id: str, params: dict | None = None) -> list[dict]:
        # DOGFOOD-SOQL-006: boost activos primero antes de dedupe/cut.
        reordered = _reorder_active_first(results, dataset_id, params)
        # B1: deduplicar ANTES de cortar al max_results.
        deduped = _dedupe_results(reordered, dataset_id)
        # MOJIBAKE-DISPLAY-001: normalize malformed UTF-8 from SECOP
        # Only apply to string fields, not numeric/None values
        for r in deduped:
            for k in list(r.keys()):
                if isinstance(r[k], str):
                    r[k] = _fix_mojibake(r[k])
        rows: list[dict] = []
        for item in deduped[: self.max_results]:
            url = self._safe_url(item)
            fecha_iso, fecha_label = self._best_date_for_row(item, dataset_id)
            if dataset_id == "p6dx-8zbt":
                rows.append(
                    {
                        "titulo": _smart_title(item, dataset_id),  # B2
                        "entidad": item.get("entidad", ""),
                        "valor": format_cop(item.get("precio_base")),
                        "estado": item.get("estado_de_apertura_del_proceso", ""),
                        "fecha": fecha_iso,
                        "fecha_label": fecha_label,
                        "url": url,
                        # Identidad estable para "seguir proceso" (radar/follows).
                        # Campos aditivos: no afectan el render de la tarjeta.
                        "id_del_proceso": item.get("id_del_proceso", ""),
                        "referencia_del_proceso": item.get("referencia_del_proceso", ""),
                        "dataset_id": dataset_id,
                    }
                )
            else:
                anomaly = detect_value_anomaly(item, dataset_id)
                row_data = {
                    "titulo": _smart_title(item, dataset_id),  # B2
                    "entidad": item.get("nombre_entidad", ""),
                    "valor": format_cop(item.get("valor_del_contrato")),
                    "estado": item.get("estado_contrato", ""),
                    "fecha": fecha_iso,
                    "fecha_label": fecha_label,
                    "contratista": item.get("proveedor_adjudicado", ""),
                    "url": url,
                    # Identidad estable para "seguir proceso" (radar/follows).
                    "id_contrato": item.get("id_contrato", ""),
                    "referencia_del_contrato": item.get("referencia_del_contrato", ""),
                    "dataset_id": dataset_id,
                }
                if anomaly:
                    row_data.update(anomaly)
                rows.append(row_data)
        return rows

    @staticmethod
    def _safe_url(item: dict) -> str:
        """Extract URL, handling dict format and broken login URLs.

        URL repair hierarchy (FIX-URL-FREEFORM-001 + DOGFOOD-URL-005):
        1. Original URL if valid
        2. Build from CO1.NTC IDs (públicos)
        3. CO1.REQ.X → vacío (requisición pre-publicación, no consultable)
        4. Fallback referencia url-encoded
        5. Empty string
        """
        url_raw = item.get("urlproceso", "")
        if isinstance(url_raw, dict):
            url_raw = url_raw.get("url", "")
        url = str(url_raw or "")
        if "Login" not in url and url:
            return url

        from urllib.parse import quote

        pid = str(item.get("id_del_proceso") or item.get("id_contrato") or "").strip()
        # CO1.REQ.X = requisiciones pre-publicación; OpportunityDetail
        # devuelve "argument out of range". No mostrar link roto.
        if pid.upper().startswith("CO1.REQ."):
            return ""
        if pid:
            return (
                "https://community.secop.gov.co/Public/Tendering/"
                f"OpportunityDetail/Index?noticeUID={quote(pid)}"
            )
        ref = str(item.get("referencia_del_proceso") or item.get("referencia_del_contrato") or "").strip()
        if ref:
            return (
                "https://community.secop.gov.co/Public/Tendering/"
                f"OpportunityDetail/Index?noticeUID={quote(ref)}"
            )
        return ""

    def format_whatsapp(self, rows: list[dict], params: dict | None = None, dataset_id: str | None = None, total_count: int = 0, universe_insights: Any = None, suggestions: list | None = None, total_count_approximate: bool = False) -> str:
        if not rows:
            return build_no_results_message(params, dataset_id)

        shown = len(rows)
        if total_count > shown:
            ordering_label = self._ordering_label(params)
            remaining = total_count - shown
            header = (
                f"📋 Encontre {total_count:,} {_resultado(total_count)} en SECOP. "
                f"Te muestro los {shown} {ordering_label}; hay {remaining:,} mas:\n"
            )
        elif total_count_approximate and shown == total_count:
            header = f"📋 Encontre {shown}+ {_resultado(shown)} en SECOP. " \
                     f"Te muestro los {shown} {self._ordering_label(params)}:\n"
        else:
            header = f"📋 Encontre {shown} {_resultado(shown)}:\n"
        insights = self._insights_summary(universe_insights, params)
        if insights:
            header += insights + "\n"
        lines = [header]
        for index, row in enumerate(rows, start=1):
            lines.append(f"*{index}.* {self._truncate(row['titulo'], 90)}")
            lines.append(f"🏛️ {row['entidad']}")
            fecha = (row.get("fecha") or "")[:10]
            if fecha:
                lines.append(f"📅 {fecha}")
            tail = f"💰 {row['valor']}"
            if row.get("estado"):
                tail += f" | {row['estado']}"
            lines.append(tail)
            if row.get("contratista"):
                lines.append(f"🤝 {row['contratista']}")
            if row.get("value_quality") == "suspect":
                ref_val = row.get("value_reference")
                ref_str = f" (valor facturado: {format_cop(ref_val)})" if ref_val else ""
                lines.append(f"⚠️ valor atípico en datos abiertos{ref_str}")
            if row.get("url"):
                lines.append(f"🔗 {row['url']}")
            else:
                lines.append("🔒 Sin enlace público disponible en datos abiertos")
            lines.append("")
        result = "\n".join(lines).strip()
        sug_text = self._format_suggestions(suggestions)
        if sug_text:
            result += "\n\n" + sug_text
        # Header warning: if highest-value result is suspect
        if rows and rows[0].get("value_quality") == "suspect":
            result = "⚠️ El mayor valor mostrado tiene inconsistencia de fuente; verificar SECOP.\n\n" + result
        return result

    def format_telegram(self, rows: list[dict], params: dict | None = None, dataset_id: str | None = None, total_count: int = 0, universe_insights: Any = None, suggestions: list | None = None, total_count_approximate: bool = False) -> str:
        if not rows:
            return build_no_results_message(params, dataset_id)

        shown = len(rows)
        if total_count > shown:
            ordering_label = self._ordering_label(params)
            remaining = total_count - shown
            header = (
                f"Encontre {total_count:,} {_resultado(total_count)} en SECOP. "
                f"Te muestro los {shown} {ordering_label}; hay {remaining:,} mas:\n"
            )
        elif total_count_approximate and shown == total_count:
            header = f"Encontre {shown}+ {_resultado(shown)} en SECOP. " \
                     f"Te muestro los {shown} {self._ordering_label(params)}:\n"
        else:
            header = f"Encontre {shown} {_resultado(shown)}:\n"
        insights = self._insights_summary(universe_insights, params)
        if insights:
            header += insights + "\n"
        lines = [header]
        for index, row in enumerate(rows, start=1):
            lines.append(f"{index}. {self._truncate(row['titulo'], 100)}")
            lines.append(f"Entidad: {row['entidad']}")
            fecha = (row.get("fecha") or "")[:10]
            if fecha:
                lines.append(f"Fecha: {fecha}")
            lines.append(f"Valor: {row['valor']} | Estado: {row.get('estado', 'N/D')}")
            if row.get("contratista"):
                lines.append(f"Contratista: {row['contratista']}")
            if row.get("value_quality") == "suspect":
                ref_val = row.get("value_reference")
                ref_str = f" (valor facturado: {format_cop(ref_val)})" if ref_val else ""
                lines.append(f"⚠️ valor atípico en datos abiertos{ref_str}")
            if row.get("url"):
                lines.append(str(row["url"]))
            else:
                lines.append("🔒 Sin enlace público disponible en datos abiertos")
            lines.append("")
        result = "\n".join(lines).strip()
        sug_text = self._format_suggestions(suggestions)
        if sug_text:
            result += "\n\n" + sug_text
        # Header warning: if highest-value result is suspect
        if rows and rows[0].get("value_quality") == "suspect":
            result = "⚠️ El mayor valor mostrado tiene inconsistencia de fuente; verificar SECOP.\n\n" + result
        return result

    def format_streamlit(self, rows: list[dict], params: dict | None = None, dataset_id: str | None = None, total_count: int = 0, universe_insights: Any = None, suggestions: list | None = None, total_count_approximate: bool = False) -> str:
        if not rows:
            return build_no_results_message(params, dataset_id)
        shown = len(rows)
        if total_count > shown:
            ordering_label = self._ordering_label(params)
            remaining = total_count - shown
            base = (
                f"Encontre {total_count:,} {_resultado(total_count)} en SECOP. "
                f"Te muestro los {shown} {ordering_label}; hay {remaining:,} mas "
                "listos para explorar en tabla y tarjetas."
            )
        elif total_count_approximate and shown == total_count:
            base = (
                f"Encontre {shown}+ {_resultado(shown)} en SECOP. "
                f"Te muestro los {shown} {self._ordering_label(params)} "
                "listos para explorar en tabla y tarjetas."
            )
        else:
            base = f"Encontre {shown} {_resultado(shown)} listos para explorar en tabla y tarjetas."
        insights = self._insights_summary(universe_insights, params)
        if insights:
            base += f"\n\n{insights}"
        # Header warning: if highest-value result is suspect
        if rows and rows[0].get("value_quality") == "suspect":
            base += "\n\n⚠️ El mayor valor mostrado tiene inconsistencia de fuente; verificar SECOP."
        sug_text = self._format_suggestions(suggestions)
        if sug_text:
            base += f"\n\n{sug_text}"
        return base

    @staticmethod
    def _insights_summary(insights, params: dict | None = None) -> str:
        """Build a short insight string from UniverseInsights, or empty if none.
        Skips tautological insights (e.g. "100% son de X" when user already searched for X).
        """
        if not insights:
            return ""
        params = params or {}
        lines = []
        resolved_entity = (params.get("entidad_resolved") or "").lower()
        resolved_modality = (params.get("modalidad") or "").lower()
        has_date_filter = bool(params.get("fecha_desde") or params.get("fecha_hasta"))

        # 1. Entidad concentración (Observer 2.0)
        ec_name = getattr(insights, "entity_concentration_name", None)
        ec_pct = getattr(insights, "entity_concentration_pct", None)
        if ec_name and ec_pct is not None and ec_name.lower() not in resolved_entity:
            lines.append(f"{ec_name} concentra el {ec_pct:.0f}% del universo.")

        # 2. Modalidad dominancia (Observer 2.0)
        md_name = getattr(insights, "modality_dominance_name", None)
        md_pct = getattr(insights, "modality_dominance_pct", None)
        if md_name and md_pct is not None and md_name.lower() not in resolved_modality:
            lines.append(f"{md_name} domina con el {md_pct:.0f}%.")

        # 3. Temporal skew por trimestre (Observer 2.0)
        ts_q = getattr(insights, "temporal_skew_quarter", None)
        ts_pct = getattr(insights, "temporal_skew_pct", None)
        if ts_q and ts_pct is not None and not has_date_filter:
            lines.append(f"Concentrado en {ts_q} ({ts_pct:.0f}%).")

        # 4. Value outlier ratio (Observer 2.0)
        vor = getattr(insights, "value_outlier_ratio", None)
        if vor:
            lines.append(f"El mayor valor supera el promedio {vor:.0f}x.")

        # Legacy signals (mantenidos para compatibilidad)
        if getattr(insights, "has_dominant_entity", False):
            name = getattr(insights, "dominant_entity_name", None)
            pct = getattr(insights, "dominant_entity_pct", None)
            if name and pct and name.lower() not in resolved_entity:
                lines.append(f"El {pct:.0f}% son de {name}.")

        if getattr(insights, "has_temporal_concentration", False):
            yr = getattr(insights, "dominant_year", None)
            pct = getattr(insights, "dominant_year_pct", None)
            if yr and pct and not has_date_filter:
                lines.append(f"La mayoria son de {yr}.")

        if getattr(insights, "has_value_outlier", False):
            val = getattr(insights, "outlier_value", None)
            vs = getattr(insights, "value_stats", None)
            median_str = ""
            if vs and vs.get("median"):
                try:
                    median_str = f" (la mediana es {format_cop(vs['median'])})"
                except Exception:
                    pass
            if val:
                lines.append(f"Hay uno de {format_cop(val)}{median_str}.")

        if getattr(insights, "has_diverse_modalities", False):
            lines.append("Hay variedad de modalidades de contratacion.")

        return "\n".join(lines)

    @staticmethod
    def _format_suggestions(suggestions: list | None) -> str:
        """Build suggestion text from list of Suggestion objects, or empty if none."""
        if not suggestions:
            return ""
        lines = ["💡 *Sugerencias:*"]
        for i, s in enumerate(suggestions, start=1):
            label = getattr(s, "label", str(s)) if not isinstance(s, dict) else s.get("label", "")
            if label:
                lines.append(f"{i}. {label}")
        return "\n".join(lines)

    @staticmethod
    def _truncate(value: str, max_length: int) -> str:
        if len(value) <= max_length:
            return value
        return value[: max_length - 3].rstrip() + "..."

    @staticmethod
    def _ordering_label(params: dict | None) -> str:
        """Retorna etiqueta de ordenación para el header.
        valor_desc → 'de mayor valor'
        default    → 'más recientes'
        """
        if params and params.get("ordering_signal") == "valor_desc":
            return "de mayor valor"
        return "más recientes"
