"""Cómo se llama cada foco cuando se le habla a una persona.

Los focos son identificadores internos —`suppliers`, `values`—, y una
aclaración que dice «suppliers no se puede responder sobre procesos» le pide a
la persona que hable el idioma del código. Un solo mapa, para que la aclaración
del compilador y la etiqueta de la alternativa no diverjan.
"""
from __future__ import annotations


FOCUS_LABEL = {
    "overview": "la vista general",
    "suppliers": "los proveedores",
    "values": "los montos",
    "modalities": "las modalidades",
    "recent": "la actividad reciente",
    "temporal": "la evolución",
}


def focus_label(focus: str) -> str:
    return FOCUS_LABEL.get(focus, focus)


__all__ = ["FOCUS_LABEL", "focus_label"]
