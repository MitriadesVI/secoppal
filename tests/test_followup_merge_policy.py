"""Explicit per-field raw_params merge policy (anti follow-up poisoning).

Verifies the behavior of _merge_raw via detect_and_merge:
  - topic-class keys (objeto_or) expire when the topic is replaced
  - topic-class keys survive when the topic is only refined
  - classification sub-flags reset when the search intent changes
  - unlisted keys keep legacy inherit-current-wins behavior
"""

from app.core.followup_engine import detect_and_merge
from app.core.query_frame import frame_from_params


def _merge(prev_params, text, curr_params):
    prev = frame_from_params(prev_params)
    _, merged = detect_and_merge(text=text, current_params=curr_params, previous_frame=prev)
    return merged


def test_objeto_or_expires_when_topic_replaced():
    prev = {
        "intent_type": "opportunity_search",
        "estado_family": "oferta_abierta",
        "ciudad": "Barranquilla",
        "objeto_or": ["aseo", "vigilancia"],
    }
    merged = _merge(prev, "construcción", {"objeto": ["construcción"]})
    assert "construcción" in (merged.get("objeto") or [])
    assert "objeto_or" not in merged          # stale OR-group gone
    assert merged.get("ciudad") == "Barranquilla"


def test_objeto_or_survives_when_topic_only_refined():
    prev = {
        "intent_type": "opportunity_search",
        "estado_family": "oferta_abierta",
        "ciudad": "Barranquilla",
        "objeto_or": ["aseo", "vigilancia"],
    }
    # weak/empty current topic + a numeric modifier → refine, topic inherited
    merged = _merge(prev, "más de 100 millones", {"valor_min": 100_000_000})
    assert merged.get("objeto_or") == ["aseo", "vigilancia"]


def test_classification_subflag_resets_on_intent_change():
    prev = {
        "intent_type": "opportunity_search",
        "opportunity_mode": "bidder",
        "ciudad": "Barranquilla",
        "objeto": ["mantenimiento"],
    }
    merged = _merge(prev, "contratos firmados de obra",
                    {"intent_type": "history_search", "objeto": ["obra"]})
    assert merged.get("intent_type") == "history_search"
    assert "opportunity_mode" not in merged    # opportunity flag did not leak


def test_classification_preserved_when_intent_unchanged():
    prev = {
        "intent_type": "opportunity_search",
        "opportunity_mode": "bidder",
        "departamento_resolved": "Bolívar",
        "objeto": ["mantenimiento"],
    }
    # follow-up adds geography only; intent stays opportunity
    merged = _merge(prev, "en sucre", {"departamento_resolved": "Sucre"})
    assert merged.get("opportunity_mode") == "bidder"


def test_date_does_not_leak_across_dataset_change():
    # Bug en testeo: "contratos ... 2026" → "procesos para presentarme ..."
    # heredaba fecha 2026 (fecha_de_firma) hacia una búsqueda de procesos.
    prev = {
        "dataset": "contratos",
        "ciudad": "Barranquilla",
        "fecha_desde": "2026-01-01",
        "fecha_hasta": "2026-12-31",
    }
    merged = _merge(prev, "procesos de mantenimiento en antioquia", {
        "dataset": "procesos",
        "dataset_explicit": True,
        "departamento_resolved": "Antioquia",
        "objeto": ["mantenimiento"],
    })
    assert merged.get("dataset") == "procesos"
    assert merged.get("fecha_desde") is None       # fecha del otro dataset no se hereda
    assert merged.get("fecha_hasta") is None


def test_contentless_followup_inherits_instead_of_wiping_context():
    # Bug en testeo: tras bloquear "antioquia", decir "quiero ver todo"
    # perdía Antioquia y devolvía un WHERE 1=1. Debe heredar el contexto.
    prev = {
        "intent_type": "opportunity_search",
        "estado_family": "oferta_abierta",
        "departamento_resolved": "Antioquia",
        "objeto": [],
    }
    merged = _merge(prev, "quiero ver todo", {"dataset": "procesos"})
    assert merged.get("departamento_resolved") == "Antioquia"
    assert merged.get("intent_type") == "opportunity_search"


def test_unlisted_raw_key_keeps_legacy_inherit():
    # force_dataset is not in the policy → inherited (current wins if present).
    prev = {
        "intent_type": "history_search",
        "force_dataset": "contratos",
        "departamento_resolved": "Atlántico",
        "objeto": ["obra"],
    }
    merged = _merge(prev, "más recientes", {"ordering_signal": "fecha"})
    assert merged.get("force_dataset") == "contratos"
