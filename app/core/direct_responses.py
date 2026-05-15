"""direct_responses.py — Respuestas unificadas para ramas directas pre-Burr.

Centraliza el tono y formato de las respuestas que NO pasan por el pipeline
completo: reset, paginación sin historial, sugerencias inválidas.
Comparte el mismo espíritu asesor que response_policy.py (formato numerado,
lenguaje profesional, sugerencias accionables).
"""
from __future__ import annotations


def reset_response() -> str:
    """Confirmación de reinicio de conversación con sugerencias."""
    return (
        "Conversacion reiniciada. Olvide el contexto anterior.\n\n"
        "Sobre que quieres buscar ahora? Puedo ayudarte con:\n"
        "1. Contratos firmados de una entidad o tema.\n"
        "2. Procesos abiertos para licitar.\n"
        "3. Analisis de contratistas o concentracion por sector."
    )


def pagination_no_history_response() -> str:
    """Mensaje cuando se pide 'más' sin historial previo."""
    return (
        "Mas de que busqueda? Todavia no hemos hablado de nada.\n\n"
        "Hace primero una consulta como:\n"
        '1. "contratos de adulto mayor en Atlantico 2025"\n'
        '2. "procesos abiertos de pavimentacion"\n'
        '3. "que ha contratado la gobernacion del Cesar"'
    )


def suggestion_invalid_response(max_options: int, attempted: str) -> str:
    """Mensaje cuando el usuario selecciona un número inválido de sugerencia."""
    return (
        f"Solo tengo {max_options} sugerencias disponibles. "
        f"Elegi un numero entre 1 y {max_options}, o hace otra consulta."
    )


def suggestion_selection_header(label: str) -> str:
    """Header para anteponer a la respuesta cuando se aplica una sugerencia."""
    return f"Aplicando: {label}.\n\n"
