from __future__ import annotations

import json
import logging

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - guarded for environments without deps
    OpenAI = None

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
Eres un asistente que interpreta consultas sobre contratacion publica colombiana (SECOP II).
Tu unica tarea es extraer parametros de busqueda y llamar la herramienta buscar_procesos.

Reglas:
1. Solo incluye parametros mencionados explicitamente.
2. Distingue entre TERRITORIO, MUNICIPIO/CIUDAD y ENTIDAD ESPECIFICA:
   - "en Atlantico", "del Atlantico" -> departamento
   - "en Puerto Salgar", "en Soacha", "en Barranquilla" -> ciudad (nombre del municipio)
   - "de la gobernacion del Atlantico", "del SENA", "de la alcaldia de..." -> entidad
   - "de la Secretaria de Integracion Social" -> entidad
   - "contratos de la alcaldia de Puerto Salgar" -> entidad="alcaldia de Puerto Salgar", ciudad no necesario
3. Convierte valores monetarios:
   - "500 millones" = 500000000
   - "mil millones" y "un billon" (uso coloquial) = 1000000000
   - "200 palos" = 200000000
4. "abiertas" o "vigentes" -> estado "Abierto"
5. "contratos firmados" -> dataset "contratos", estado "Celebrado"
6. "en ejecucion" -> dataset "contratos", estado "En ejecucion"
7. "liquidados" -> dataset "contratos", estado "Liquidado"
8. Default: dataset "procesos"
9. objeto son las palabras clave de LO QUE SE CONTRATA (mantenimiento, vial, construccion, etc.)
   NO incluyas nombres de ciudades, departamentos ni entidades en objeto.
""".strip()

SECOPAL_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "buscar_procesos",
            "description": (
                "Busca procesos o contratos en SECOP II Colombia. "
                "Incluye solo parametros presentes en la consulta del usuario."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "enum": ["procesos", "contratos"],
                    },
                    "departamento": {"type": "string"},
                    "ciudad": {
                        "type": "string",
                        "description": "Municipio o ciudad donde se ejecuta (Puerto Salgar, Soacha, Barranquilla, etc.)",
                    },
                    "entidad": {"type": "string"},
                    "objeto": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "valor_min": {"type": "number"},
                    "valor_max": {"type": "number"},
                    "estado": {
                        "type": "string",
                        "enum": [
                            # Procesos states
                            "Abierto",
                            "Cerrado",
                            "Adjudicado",
                            "Desierto",
                            "Publicado",
                            "Borrador",
                            "Cancelado",
                            # Contratos states
                            "Celebrado",
                            "En ejecucion",
                            "Liquidado",
                            "Terminado",
                        ],
                    },
                    "modalidad": {"type": "string"},
                    "contratista": {"type": "string"},
                    "fecha_desde": {"type": "string"},
                    "fecha_hasta": {"type": "string"},
                },
                "required": [],
            },
        },
    }
]

# Keys that the regex parser handles well — LLM should NOT overwrite these
# unless the regex didn't extract them
REGEX_PRIORITY_KEYS = {"departamento", "estado", "valor_min", "valor_max", "fecha_desde", "fecha_hasta"}

# Timeout for DeepSeek API calls (seconds)
LLM_TIMEOUT_SECONDS = 15


class LLMHandler:
    """Uses DeepSeek's OpenAI-compatible API for structured fallback parsing."""

    def __init__(self, api_key: str | None, model: str = "deepseek-chat", base_url: str = "https://api.deepseek.com/v1"):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url

    @property
    def enabled(self) -> bool:
        return bool(self.api_key and OpenAI is not None)

    def parse(self, user_query: str, existing_params: dict | None = None) -> dict:
        merged = dict(existing_params or {})
        if not self.enabled:
            return merged

        try:
            client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url,
                timeout=LLM_TIMEOUT_SECONDS,
            )
            response = client.chat.completions.create(
                model=self.model,
                temperature=0,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_query},
                ],
                tools=SECOPAL_TOOLS,
                tool_choice="auto",
            )
        except Exception as exc:
            logger.warning("LLM call failed (timeout or error), using regex-only params: %s", exc)
            return merged

        message = response.choices[0].message
        tool_calls = message.tool_calls or []
        for tool_call in tool_calls:
            if tool_call.function.name != "buscar_procesos":
                continue
            try:
                tool_args = json.loads(tool_call.function.arguments)
            except json.JSONDecodeError:
                continue

            for key, value in tool_args.items():
                if value in (None, "", []):
                    continue
                # Don't overwrite keys that regex already extracted with confidence
                if key in REGEX_PRIORITY_KEYS and key in merged and merged[key] not in (None, "", []):
                    continue
                merged[key] = value

        return merged
