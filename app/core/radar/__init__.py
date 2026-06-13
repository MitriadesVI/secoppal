"""SECOPPAL Radar — búsquedas guardadas con seguimiento y lectura asesora.

Radar = QueryFrame guardado + corrida manual + diff contra la última
revisión + digest asesor. v1.0 es manual (sin cron ni push).

Capas:
- ``store``   — persistencia SQLite (radars, radar_runs, radar_seen_results).
- ``keys``    — clave estable de resultado, extracción de campos, snapshot hash.
- ``diff``    — clasifica nuevos / cambios de estado / cambios de valor / desaparecidos.
- ``runner``  — ejecuta un radar reutilizando el núcleo de búsqueda puro.
- ``digest``  — convierte el diff en lectura asesora.
"""
from __future__ import annotations
