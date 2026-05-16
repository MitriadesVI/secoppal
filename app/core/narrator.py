"""narrator.py — NarratorHandler

Convierte una lista de rows de SECOP en una narrativa conversacional
usando DeepSeek, con validación de grounding sobre cifras monetarias y
nombres de entidades.

Flujo:
  1. narrate(rows, query, channel, history) -> str | None
  2. Si narrativa contiene cifras o entidades que no matchean los rows,
     reintenta con prompt más estricto.
  3. Segundo intento también falla -> retorna None (orchestrator usa formatter clásico).

La validación cubre:
  - Cifras monetarias (±1% de tolerancia)
  - Nombres de entidades (substring matching contra rows)
"""

from __future__ import annotations

import logging
import re

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover
    OpenAI = None

logger = logging.getLogger(__name__)

NARRATOR_TIMEOUT = 20  # segundos

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_BASE_SYSTEM = """Eres el asistente conversacional de SECOPPAL, un buscador de contratación pública colombiana.
Recibirás resultados de una búsqueda en SECOP II y debes presentarlos al usuario de forma clara y útil.

Reglas de redacción:
- Tuteá al usuario. Nunca uses "usted".
- Sé conciso. Máximo 3-4 oraciones para el resumen principal.
- Menciona cifras exactas que estén en los datos. No inventes ni redondees más allá del contexto.
- Si hay un solo resultado, descríbelo directamente.
- Si hay múltiples, resalta los más relevantes (mayor valor, más reciente, abiertos).
- Cierra con una observación útil o una pregunta de seguimiento si aplica.
- NO uses listas con bullets a menos que el canal sea telegram y haya 3+ items.
- NO incluyas URLs completas en el texto.

Formato de salida: texto plano conversacional. Sin markdown excepto donde se indique por canal.""".strip()

_STRICT_SYSTEM = _BASE_SYSTEM + """

ADVERTENCIA: En el intento anterior generaste una cifra que no corresponde a los datos reales.
Esta vez cita las cifras EXACTAMENTE como aparecen en los datos. No redondees, no estimes."""


def _channel_hint(channel: str) -> str:
    hints = {
        "telegram": "Puedes usar *negrita* con asteriscos para resaltar cifras o entidades clave. Máximo 2 bullets si hay varios resultados.",
        "whatsapp": "Solo texto plano. Sin markdown. Puedes usar *negrita* para cifras importantes.",
        "streamlit": "Texto narrativo puro, fluido. Sin bullets. Sin markdown.",
    }
    return hints.get(channel, "Texto plano conversacional.")


# ---------------------------------------------------------------------------
# Normalización de montos
# ---------------------------------------------------------------------------

# Multipliers para sufijos
_MULTIPLIERS: list[tuple[re.Pattern, float]] = [
    (re.compile(r"(?:mil\s+millones|mm\b)", re.I), 1_000_000_000),
    (re.compile(r"millones?\b", re.I), 1_000_000),
    (re.compile(r"\bm\b", re.I), 1_000_000),
    (re.compile(r"mil\b", re.I), 1_000),
]

# Patrones para extraer cifras del texto narrativo
# Captura: signo $, número (con . como separador de miles o , decimal), sufijo opcional
_AMOUNT_PATTERN = re.compile(
    r"\$?\s*"                                    # signo $ opcional
    r"(\d[\d.,]*)"                               # número
    r"\s*"
    r"(mil millones|millones?|mm|m\b|cop|mil)?"  # sufijo opcional
    r"(?:\s+cop)?",                              # COP al final opcional
    re.I,
)


def _parse_amount_str(num_str: str, suffix: str) -> float | None:
    """Convierte un string de número + sufijo a float en pesos colombianos."""
    # Normaliza separadores: si hay punto y coma, el punto es separador de miles
    # Si solo hay punto, podría ser decimal (1.5) o separador de miles (750.000)
    num_str = num_str.strip()
    suffix = (suffix or "").strip().lower()

    # Elimina espacios internos
    num_str = num_str.replace(" ", "")

    # Determina si el punto es separador de miles o decimal
    dots = num_str.count(".")
    commas = num_str.count(",")

    if commas > 0 and dots > 0:
        # 1.500.000,50 — punto = miles, coma = decimal
        num_str = num_str.replace(".", "").replace(",", ".")
    elif commas > 0:
        # 750,000 — coma como miles (formato anglosajón) O coma decimal
        # Si hay más de un dígito después de la última coma → miles
        after_comma = num_str.split(",")[-1]
        if len(after_comma) == 3:
            num_str = num_str.replace(",", "")
        else:
            num_str = num_str.replace(",", ".")
    elif dots > 1:
        # 750.000.000 — todos los puntos son separadores de miles
        num_str = num_str.replace(".", "")
    elif dots == 1:
        after_dot = num_str.split(".")[-1]
        if len(after_dot) == 3:
            # 750.000 → separador de miles
            num_str = num_str.replace(".", "")
        # else: decimal legítimo (1.5)

    try:
        value = float(num_str)
    except ValueError:
        return None

    # Aplica multiplicador por sufijo
    for pattern, mult in _MULTIPLIERS:
        if suffix and pattern.search(suffix):
            value *= mult
            break

    return value


def extract_monetary_values(text: str) -> set[float]:
    """Extrae todas las cifras monetarias del texto y las normaliza a float."""
    results: set[float] = set()
    for m in _AMOUNT_PATTERN.finditer(text):
        num_str = m.group(1)
        suffix = m.group(2) or ""
        val = _parse_amount_str(num_str, suffix)
        if val is not None and val >= 1_000:  # ignora valores menores a $1.000 (ruido)
            results.add(val)
    return results


def extract_real_monetary_values(rows: list[dict]) -> set[float]:
    """Extrae valores monetarios reales de los rows de SECOP."""
    fields = ("precio_base", "valor_del_contrato", "valor_contrato_con_adiciones")
    results: set[float] = set()
    for row in rows:
        for field in fields:
            raw = row.get(field)
            if raw is None:
                continue
            try:
                val = float(str(raw).replace(",", ".").replace(" ", ""))
                if val >= 1_000:
                    results.add(val)
            except (ValueError, TypeError):
                continue
    return results


def validate_grounding(narrative: str, rows: list[dict], tolerance: float = 0.01) -> bool:
    """Verifica que las cifras monetarias de la narrativa correspondan a rows reales.

    Retorna True si pasa (narrativa OK), False si hay cifra inventada.
    Si la narrativa no menciona cifras, pasa automáticamente.
    """
    narrative_values = extract_monetary_values(narrative)
    if not narrative_values:
        return True  # sin cifras → no hay qué validar

    real_values = extract_real_monetary_values(rows)
    if not real_values:
        return True  # sin datos monetarios → no podemos validar, confiamos

    for val in narrative_values:
        if not any(
            abs(val - real) / max(real, 1.0) <= tolerance
            for real in real_values
        ):
            logger.warning(
                "Grounding fail: %.0f no encontrado en rows (tolerancia %.0f%%)",
                val,
                tolerance * 100,
            )
            return False
    return True


# ---------------------------------------------------------------------------
# Validación de entidades
# ---------------------------------------------------------------------------

# Patrón para extraer potenciales nombres de entidad de la narrativa:
# frases con mayúscula inicial de 2+ palabras, típicas de nombres institucionales.
_ENTITY_PHRASE_RE = re.compile(
    r"(?:Gobernaci[oó]n|Alcald[ií]a|Municipio|Ministerio|Secretar[ií]a|"
    r"Departamento|Distrito|Instituto|Corporaci[oó]n|Empresa|"
    r"ESE|EPS|SENA|ICBF|INV[ií]AS|ANI|ANM|DNP|FONADE|ECOPETROL|"
    r"Gobernaci[oó]n\s+\w+|Alcald[ií]a\s+\w+|"
    r"\b[A-ZÁÉÍÓÚ][a-záéíóú]+(?:\s+(?:de\s+)?[A-ZÁÉÍÓÚ][a-záéíóú]+)+)",
)

# Palabras que no son entidades aunque empiecen con mayúscula en el texto
_ENTITY_FALSE_POSITIVES = frozenset({
    "Encontré", "SECOP", "Te muestro", "Resultados", "Contratos",
    "Procesos", "COP", "Millones", "Año", "Años", "Mientras",
    "Además", "También", "Según", "Como", "Porque", "Para",
    "Entre", "Desde", "Hasta", "Durante", "Sobre", "Bajo",
})


def _extract_entity_phrases(text: str) -> set[str]:
    """Extrae frases que parecen nombres de entidad de un texto narrativo."""
    matches = _ENTITY_PHRASE_RE.findall(text)
    return {
        m.strip().rstrip(".,;:!?")
        for m in matches
        if m.strip() not in _ENTITY_FALSE_POSITIVES
        and len(m.strip()) > 4
    }


def _extract_entity_strings_from_rows(rows: list[dict]) -> set[str]:
    """Extrae todos los nombres de entidad presentes en los rows de SECOP."""
    entity_fields = ("entidad", "nombre_entidad", "proveedor_adjudicado",
                     "nombre_del_procedimiento", "objeto_del_contrato")
    entities: set[str] = set()
    for row in rows:
        for field in entity_fields:
            val = row.get(field)
            if isinstance(val, str) and val.strip():
                entities.add(val.strip())
    return entities


def validate_entity_grounding(narrative: str, rows: list[dict]) -> bool:
    """Verifica que los nombres de entidad en la narrativa aparezcan en los rows.

    Si la narrativa menciona una entidad que no está en ningún row,
    es potencial alucinación y se rechaza.
    """
    narrative_entities = _extract_entity_phrases(narrative)
    if not narrative_entities:
        return True  # sin nombres de entidad → no hay qué validar

    row_entities = _extract_entity_strings_from_rows(rows)
    if not row_entities:
        return True  # sin entidades en rows → no podemos validar, confiamos

    for phrase in narrative_entities:
        # Buscar la frase de la narrativa como substring en alguna entidad de los rows
        phrase_lower = phrase.lower()
        found = any(phrase_lower in ent.lower() or ent.lower() in phrase_lower
                    for ent in row_entities)
        if not found:
            logger.warning(
                "Entity grounding fail: '%s' no encontrada en rows", phrase
            )
            return False
    return True


# ---------------------------------------------------------------------------
# NarratorHandler
# ---------------------------------------------------------------------------


class NarratorHandler:
    """Genera narrativas conversacionales grounded en los datos de SECOP."""

    def __init__(
        self,
        api_key: str | None,
        model: str = "deepseek-chat",
        base_url: str = "https://api.deepseek.com/v1",
    ):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url

    @property
    def enabled(self) -> bool:
        return bool(self.api_key and OpenAI is not None)

    def narrate(
        self,
        rows: list[dict],
        query: str,
        channel: str = "streamlit",
        history: list[dict] | None = None,
        total_count: int = 0,
        strict: bool = False,
        universe_insights: Any = None,
        suggestions: list[dict] | None = None,
    ) -> str | None:
        """Genera una narrativa para los rows dados.

        Args:
            rows: resultados de SECOP (lista de dicts).
            query: consulta original del usuario.
            channel: canal de entrega (telegram, whatsapp, streamlit).
            history: turnos previos de conversación (lista de dicts con role/content).
            total_count: total de resultados en SECOP (puede ser mayor que len(rows)).
            strict: si True, usa prompt más estricto (segundo intento tras grounding fail).
            universe_insights: UniverseInsights con estadísticas calculadas.
            suggestions: lista de Suggestion para acciones sugeridas.

        Returns:
            str con narrativa, o None si LLM no disponible o falla.
        """
        if not self.enabled:
            return None

        system = _STRICT_SYSTEM if strict else _BASE_SYSTEM
        channel_hint = _channel_hint(channel)

        rows_summary = _summarize_rows(rows, total_count)

        # Observaciones del observer
        obs_lines = []
        if universe_insights:
            ui = universe_insights
            if getattr(ui, "has_dominant_entity", False) and getattr(ui, "dominant_entity_name", None):
                obs_lines.append(f"- El {getattr(ui, 'dominant_entity_pct', 0):.0f}% son de {ui.dominant_entity_name}.")
            if getattr(ui, "has_temporal_concentration", False) and getattr(ui, "dominant_year", None):
                obs_lines.append(f"- La mayoria son de {ui.dominant_year}.")
            if getattr(ui, "has_value_outlier", False) and getattr(ui, "outlier_value", None):
                obs_lines.append(f"- Hay un valor atipico de {ui.outlier_value:,.0f} COP.")
            if getattr(ui, "has_diverse_modalities", False):
                obs_lines.append("- Hay variedad de modalidades de contratacion.")

        # Sugerencias
        sug_lines = []
        if suggestions:
            for s in suggestions:
                label = getattr(s, "label", str(s)) if not isinstance(s, dict) else s.get("label", "")
                if label:
                    sug_lines.append(f"- {label}")

        user_content = (
            f"El usuario preguntó: {query}\n\n"
            f"Datos encontrados ({len(rows)} de {total_count or len(rows)} resultados):\n"
            f"{rows_summary}\n\n"
        )
        if obs_lines:
            user_content += "Observaciones sobre el universo de resultados:\n" + "\n".join(obs_lines) + "\n\n"
        if sug_lines:
            user_content += "Sugerencias accionables (mencionalas si aplican):\n" + "\n".join(sug_lines) + "\n\n"
        user_content += (
            f"Instrucción de formato para canal {channel}: {channel_hint}\n\n"
            "Genera la respuesta narrativa ahora."
        )

        messages: list[dict] = [{"role": "system", "content": system}]
        if history:
            # Incluye últimos 3 turnos para contexto conversacional
            for turn in history[-3:]:
                role = turn.get("role", "user")
                content = turn.get("content", "")
                if role in ("user", "assistant") and content:
                    messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": user_content})

        try:
            client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url,
                timeout=NARRATOR_TIMEOUT,
            )
            response = client.chat.completions.create(
                model=self.model,
                temperature=0.3,  # algo de variación para narrativa natural
                messages=messages,
            )
            return response.choices[0].message.content
        except Exception as exc:
            logger.warning("Narrator LLM call failed: %s", exc)
            return None

    def narrate_with_grounding(
        self,
        rows: list[dict],
        query: str,
        channel: str = "streamlit",
        history: list[dict] | None = None,
        total_count: int = 0,
        universe_insights: Any = None,
        suggestions: list | None = None,
    ) -> str | None:
        """Wrapper con validación de grounding (cifras + entidades) y reintento automático.

        Flujo:
          1. narrate() normal
          2. validate_grounding() + validate_entity_grounding() — si falla:
          3. narrate(strict=True) — segundo intento
          4. validate_grounding() + validate_entity_grounding() — si falla: retorna None

        Returns:
            Narrativa validada, o None para que el orchestrator use formatter clásico.
        """
        narrative = self.narrate(
            rows, query, channel, history, total_count,
            strict=False, universe_insights=universe_insights, suggestions=suggestions,
        )
        if narrative is None:
            return None

        money_ok = validate_grounding(narrative, rows)
        entity_ok = validate_entity_grounding(narrative, rows)
        if money_ok and entity_ok:
            return narrative

        failed = []
        if not money_ok:
            failed.append("cifras")
        if not entity_ok:
            failed.append("entidades")
        logger.info("Grounding fail en intento 1 (%s) — reintentando con prompt estricto", ", ".join(failed))
        narrative = self.narrate(
            rows, query, channel, history, total_count,
            strict=True, universe_insights=universe_insights, suggestions=suggestions,
        )
        if narrative is None:
            return None

        money_ok = validate_grounding(narrative, rows)
        entity_ok = validate_entity_grounding(narrative, rows)
        if money_ok and entity_ok:
            return narrative

        logger.warning("Grounding fail en intento 2 — usando formatter clásico")
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DISPLAY_FIELDS = [
    "nombre_del_procedimiento",
    "entidad",
    "precio_base",
    "valor_del_contrato",
    "estado_del_procedimiento",
    "fecha_de_publicacion_del_proceso",
    "departamento_entidad",
    "ciudad_entidad",
    "objeto_del_contrato_a_celebrar",
]


def _summarize_rows(rows: list[dict], total_count: int = 0) -> str:
    """Formatea los rows como texto compacto para el prompt del narrador."""
    if not rows:
        return "No se encontraron resultados."

    lines: list[str] = []
    for i, row in enumerate(rows[:10], 1):  # máximo 10 rows en el prompt
        parts: list[str] = []
        for field in _DISPLAY_FIELDS:
            val = row.get(field)
            if val not in (None, "", "null"):
                parts.append(f"{field}: {val}")
        lines.append(f"[{i}] " + " | ".join(parts))

    if total_count > len(rows):
        lines.append(f"... y {total_count - len(rows)} resultados más en SECOP.")

    return "\n".join(lines)
