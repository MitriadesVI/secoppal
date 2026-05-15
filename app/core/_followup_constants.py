"""Fuente única de verdad para constantes del subsistema follow-up de SECOPPAL.

ADR-001 y hallazgo H3 de la auditoría 2026-05-15: conversation_store.py y
followup_engine.py tenían copias literales de CONTINUATION_WORDS, NOISE_PREFIXES,
NEW_SEARCH_VERBS, FOLLOWUP_WINDOW_MINUTES. Este módulo centraliza las definiciones
y ambos módulos las importan de aquí.

No debe haber duplicación de estas constantes en ningún otro módulo.
"""

from __future__ import annotations

# Ventana temporal para follow-up: si el último turno tiene más de N minutos,
# no se aplica contexto conversacional.
FOLLOWUP_WINDOW_MINUTES: float = 30.0

# Palabras que al inicio de frase indican continuación (no búsqueda nueva).
# Incluye bigrams como "quiero ver" para detección greedy.
CONTINUATION_WORDS: tuple[str, ...] = (
    "y ", "ahora", "también", "tambien", "pero ", "solo ",
    "muéstrame", "muestrame", "dame", "ordena", "ordénalos",
    "ordenalos", "filtremos", "filtra",
    "quiero ver", "quiero mirar", "quiero revisar", "quiero mostrar",
    "ver los", "ver las",
    "mostrar los", "mostrar las",
    "los de", "las de",
)

# Verbos que indican búsqueda completamente nueva — si la query empieza
# con uno de estos, NO es follow-up aunque sea corta.
NEW_SEARCH_VERBS: tuple[str, ...] = (
    "busca ", "encuentra ", "necesito ", "consulta ",
    "lista ", "listar ",
    "muestra procesos ", "muestra contratos ",
)

# Prefijos conversacionales que son ruido y no afectan la intención.
# Se eliminan al inicio del texto antes de clasificar.
NOISE_PREFIXES: tuple[str, ...] = (
    "hola ", "holaa ", "holaaa ", "buenas ", "buenass ",
    "ok ", "okay ", "oye ", "ey ",
)

# Comandos de reset reconocidos.
RESET_COMMANDS: frozenset[str] = frozenset({
    "/reset", "reset", "nueva", "limpiar", "nuevo",
})

# Claves de scope/topic que deben existir para que una consulta pase
# el guard anti-WHERE 1=1.
SCOPE_TOPIC_KEYS: frozenset[str] = frozenset({
    "objeto", "entidad_resolved", "entidad_like",
    "departamento_resolved", "ciudad", "contratista",
})
