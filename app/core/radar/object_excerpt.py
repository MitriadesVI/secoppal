"""object_excerpt.py — extracto normalizado y acotado del objeto contractual.

Neutral y sin dependencias de dominio (solo stdlib). Una sola implementación
de "qué es un extracto seguro para mostrar" que comparten quien lo ESCRIBE y
quien lo MUESTRA, para que la política no pueda divergir entre ambos:

- ``radar/keys.py`` lo aplica al proyectar el evento, así ``radar_events.
  object_excerpt`` guarda ya el extracto acotado —nunca la descripción
  completa—.
- ``notify/render.py`` lo reexporta y lo reaplica defensivamente sobre
  payloads armados a mano (idempotente sobre un extracto ya acotado).

El sentido de la dependencia es el natural: ``notify`` (que consume eventos)
depende de ``radar``. Este módulo no importa nada de ``notify`` ni de
``radar`` para no poder cerrar un ciclo.

No decide QUÉ objeto/proceso mostrar ni por qué apareció en el radar: es el
texto del objeto contractual (dato público), normalizado y acotado. No es un
``match_reason`` ni una ``node signal``.
"""
from __future__ import annotations

import re

DEFAULT_LIMIT = 180

_SPACE = re.compile(r"\s+")


def _clean(value) -> str:
    if value is None:
        return ""
    return _SPACE.sub(" ", str(value)).strip()


def build_object_excerpt(text: str | None, *, limit: int = DEFAULT_LIMIT) -> str | None:
    """Extracto legible del objeto/descripción contractual.

    Pura y determinista: normaliza espacios, nunca corta a media palabra, y
    solo añade elipsis si de verdad hubo truncamiento. El resultado final
    —elipsis incluida— nunca supera ``limit`` caracteres. Ausencia (``None``,
    cadena vacía o solo espacios) se preserva como ``None``, nunca como cadena
    vacía.

    Si el texto (ya sin espacios de sobra) es una única palabra/token más
    largo que ``limit``, no hay dónde cortar sin partirla: la política segura
    es devolver ``None`` antes que mostrar (o persistir) una palabra partida a
    la mitad.

    Idempotente sobre su propia salida: un extracto ya acotado vuelve a salir
    igual, porque mide ``<= limit`` y no se vuelve a tocar.
    """
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit debe ser un entero positivo")
    cleaned = _clean(text)
    if not cleaned:
        return None
    if len(cleaned) <= limit:
        return cleaned
    # El resultado final (con la elipsis de 1 carácter) no puede superar
    # ``limit``: el contenido recortado dispone de ``limit - 1``.
    budget = limit - 1
    truncated = cleaned[:budget]
    last_space = truncated.rfind(" ")
    if last_space <= 0:
        # Ni un espacio dentro del presupuesto: la primera "palabra" ya excede
        # el límite. Cortarla la partiría a la mitad; la política segura es
        # devolver None antes que un fragmento de palabra.
        return None
    truncated = truncated[:last_space].rstrip()
    if not truncated:
        return None
    return truncated + "…"


__all__ = ["DEFAULT_LIMIT", "build_object_excerpt"]
