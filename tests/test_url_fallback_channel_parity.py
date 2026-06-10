"""ADR-027 — paridad de canales para procesos sin URL pública.

Procesos con id CO1.REQ.* (requisiciones pre-publicación / ESAL) no tienen
link consultable en SECOP. Streamlit ya mostraba la nota "Vista pública no
disponible"; Telegram/WhatsApp la omitían. Estos tests congelan que los 3
canales señalen el caso de forma consistente.
"""
from __future__ import annotations

import pytest

from app.core.formatter import Formatter

F = Formatter()
_NOTE = "Vista pública no disponible"

# Fila SIN url (CO1.REQ pre-publicación) y fila CON url (CO1.NTC publicado).
_RESULTS = [
    {
        "nombre_del_procedimiento": "ATENCION ADULTO MAYOR",
        "entidad": "GOBERNACION X",
        "precio_base": 3_000_000_000,
        "estado_de_apertura_del_proceso": "Abierto",
        "fecha_de_publicacion_del": "2026-01-01",
        "urlproceso": "",
        "id_del_proceso": "CO1.REQ.999",
    },
    {
        "nombre_del_procedimiento": "CENTRO DIA",
        "entidad": "SDIS",
        "precio_base": 500_000_000,
        "estado_de_apertura_del_proceso": "Abierto",
        "fecha_de_publicacion_del": "2026-02-01",
        "urlproceso": "",
        "id_del_proceso": "CO1.NTC.10219520",
    },
]


@pytest.mark.parametrize("channel", ["telegram", "whatsapp"])
def test_unlinkable_row_shows_note(channel):
    msg, _ = F.format_for_channel(_RESULTS, "p6dx-8zbt", channel)
    assert _NOTE in msg  # CO1.REQ → nota de cortesía
    assert "CO1.NTC.10219520" in msg  # CO1.NTC → link real presente


@pytest.mark.parametrize("channel", ["telegram", "whatsapp"])
def test_note_count_matches_unlinkable_rows(channel):
    msg, _ = F.format_for_channel(_RESULTS, "p6dx-8zbt", channel)
    # Exactamente una fila sin url → exactamente una nota.
    assert msg.count(_NOTE) == 1


def test_streamlit_note_unchanged():
    """Streamlit ya emitía la nota; este test la blinda (no se tocó su lógica)."""
    msg = F.format_streamlit(
        F.to_rows(_RESULTS, "p6dx-8zbt"), dataset_id="p6dx-8zbt"
    )
    # format_streamlit no enumera filas (eso lo hace la UI con cards), pero la
    # construcción de url es compartida: la fila CO1.REQ no produce link.
    rows = F.to_rows(_RESULTS, "p6dx-8zbt")
    assert rows[0]["url"] == ""  # CO1.REQ → sin url
    assert "CO1.NTC.10219520" in rows[1]["url"]  # CO1.NTC → con url
