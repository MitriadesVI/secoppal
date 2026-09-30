"""FOUNDATION-PRESENTATION: published Foundation rows reach every channel as cards."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

from app.core.formatter import Formatter
from app.core.foundation_presentation import group_cards, response_text

NOW = datetime(2026, 9, 30, 15, tzinfo=timezone.utc)
PROCESS = {
    "id_del_proceso": "CO1.REQ.1", "referencia_del_proceso": "LP-001-2026",
    "entidad": "MUNICIPIO DE SOLEDAD", "nombre_del_procedimiento": "Construcción de vía",
    "descripci_n_del_procedimiento": "Obra", "precio_base": "1500000000",
    "estado_de_apertura_del_proceso": "Abierto", "estado_del_procedimiento": "Publicado",
    "fecha_de_publicacion_del": "2026-09-01T00:00:00.000", "fecha_de_recepcion_de": "2026-10-15T00:00:00.000",
}
CONTRACT = {
    "id_contrato": "CO1.PCCNTR.9", "referencia_del_contrato": "CD-08-2026-1",
    "nombre_entidad": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
    "objeto_del_contrato": "Apoyo a la estrategia de primera infancia", "descripcion_del_proceso": "",
    "valor_del_contrato": "25000000", "estado_contrato": "En ejecución", "fecha_de_firma": "2026-08-01T00:00:00.000",
}


def test_display_rows_emits_the_same_card_as_to_rows():
    formatter = Formatter(max_results=10)
    for dataset, row in (("p6dx-8zbt", PROCESS), ("jbjy-vk9h", CONTRACT)):
        legacy = formatter.to_rows([deepcopy(row)], dataset, reference_instant=NOW)
        assert formatter.display_rows([row], dataset, reference_instant=NOW) == legacy
        assert legacy[0]["titulo"] and legacy[0]["entidad"] and legacy[0]["valor"] != "Sin valor"


def test_display_rows_keeps_order_count_duplicates_and_source_untouched():
    rows = [dict(PROCESS, id_del_proceso=f"CO1.REQ.{i}") for i in range(30)]
    rows.append(dict(rows[0]))  # an exact source duplicate stays: Foundation published it
    rows[3]["entidad"] = "ConstrucciÃ³n de vÃ­a"  # mojibake is repaired on the card, never on the evidence
    source = deepcopy(rows)
    cards = Formatter(max_results=5).display_rows(rows, "p6dx-8zbt", reference_instant=NOW)
    assert [card["id_del_proceso"] for card in cards] == [row["id_del_proceso"] for row in source]
    assert cards[3]["entidad"] == "Construcción de vía"
    assert rows == source


def _group(universe, dataset, rows, *, total, quality="exact", state="ok", fallback=False):
    return {"universe": universe, "dataset": dataset, "rows": rows, "total": total, "total_quality": quality,
            "state": state, "technical_reason": None, "fallback_attempted": fallback}


def test_response_never_lists_raw_identifiers_and_text_channels_carry_the_cards():
    groups = [_group("contracts", "jbjy-vk9h", [CONTRACT], total=1372)]
    cards = group_cards(Formatter(max_results=10), groups, reference_instant=NOW)
    web = response_text(groups, cards, channel="streamlit", limit=10)
    assert "CO1.PCCNTR" not in web and "Referencia:" not in web
    assert "encontré 1,372" in web and "Contratos celebrados" in web
    telegram = response_text(groups, cards, channel="telegram", limit=10)
    assert CONTRACT["objeto_del_contrato"] in telegram and CONTRACT["nombre_entidad"] in telegram


def test_lower_bound_and_order_sample_are_declared():
    lower = [_group("processes", "p6dx-8zbt", [PROCESS], total=50, quality="lower_bound")]
    text = response_text(lower, group_cards(Formatter(), lower, reference_instant=NOW), channel="streamlit", limit=10)
    assert "al menos 50" in text
    sample = [_group("processes", "p6dx-8zbt", [PROCESS], total=7, fallback=True)]
    text = response_text(sample, group_cards(Formatter(), sample, reference_instant=NOW), channel="streamlit", limit=10)
    assert "muestra sin orden garantizado" in text
