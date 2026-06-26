"""Commit 3: la frase sobrevive desde la consulta hasta los params (objeto_phrases).

Cubre los gates acordados: detección antes del scrub, no-duplicación, negativos
sin frase accidental, serialización JSON-safe, y herencia/reemplazo en follow-up.
NO toca SoQL ni resultados: el benchmark de retrieval sigue rojo.
"""
from __future__ import annotations

import json

import pytest

from app.core._followup_constants import SCOPE_TOPIC_KEYS
from app.core.followup_engine import _RAW_TOPIC_KEYS, _merge_raw
from app.core.phrase_catalog import detect_phrases
from app.core.query_router import QueryRouter


# ── detección pura (detect_phrases) ──────────────────────────────────────────
def test_positive_detects_phrase_and_consumes_span():
    canon, remaining = detect_phrases("elaboracion o actualizacion de manuales de contratacion")
    assert canon == ["manual de contratacion"]
    # no-duplicación: los tokens de la frase NO quedan en el texto restante
    assert "manuales" not in remaining
    assert "contratacion" not in remaining
    assert "elaboracion" in remaining and "actualizacion" in remaining


def test_intercalated_variant_detected():
    canon, _ = detect_phrases("actualizacion del manual interno de contratacion")
    assert canon == ["manual de contratacion"]


@pytest.mark.parametrize(
    "query",
    [
        "manual de funciones para procesos de contratacion",  # gap > 2
        "elaboracion de contratos y actualizacion de manuales",  # no 'contratacion'
        "manualidades para capacitacion",  # ni 'manual' ni 'contratacion' como tokens
    ],
)
def test_negatives_form_no_accidental_phrase(query):
    canon, _ = detect_phrases(query)
    assert canon == []


# ── nivel parser (QueryRouter.parse) ─────────────────────────────────────────
def test_parser_separates_phrase_from_actions():
    params = QueryRouter().parse(
        "contratos para elaboracion o actualizacion de manuales de contratacion en 2024"
    ).params
    assert params.get("objeto_phrases") == ["manual de contratacion"]
    # la acción sobrevive como alternativa; la frase NO recrea tokens de objeto
    flat = [t for grp in params.get("objeto", []) for t in (grp if isinstance(grp, list) else [grp])]
    assert "manuales" not in flat
    assert "contratacion" not in flat
    assert set(flat) >= {"elaboracion", "actualizacion"}


def test_objeto_phrases_is_json_safe():
    params = QueryRouter().parse(
        "actualizacion de manuales de contratacion en 2024"
    ).params
    # gate duro: el dict de params debe serializar sin tipos exóticos
    json.dumps(params)
    assert isinstance(params["objeto_phrases"], list)
    assert all(isinstance(s, str) for s in params["objeto_phrases"])


# ── follow-up: herencia y reemplazo (política topic-bound) ───────────────────
def test_objeto_phrases_is_topic_bound():
    assert "objeto_phrases" in _RAW_TOPIC_KEYS
    assert "objeto_phrases" in SCOPE_TOPIC_KEYS


def test_followup_inherits_phrase_on_year_change():
    prev = {"objeto_phrases": ["manual de contratacion"], "objeto": [["elaboracion", "actualizacion"]]}
    curr = {"fecha_desde": "2025-01-01", "fecha_hasta": "2025-12-31"}
    merged = _merge_raw(prev, curr, topic_inherited=True)
    assert merged["objeto_phrases"] == ["manual de contratacion"]


def test_followup_drops_phrase_when_topic_replaced():
    prev = {"objeto_phrases": ["manual de contratacion"], "objeto": [["elaboracion", "actualizacion"]]}
    curr = {"objeto": [["pavimentacion"]]}  # nuevo topic, sin frase
    merged = _merge_raw(prev, curr, topic_inherited=False)
    assert "objeto_phrases" not in merged
