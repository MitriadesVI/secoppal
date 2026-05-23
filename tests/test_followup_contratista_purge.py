"""B4 — FOLLOWUP-NIT-LEAK-001: purga herencia en contratista-only pivot."""

from app.core.followup_engine import detect_and_merge
from app.core.query_frame import QueryFrame


def test_contratista_followup_purges_previous_context():
    """Cuando el follow-up es contratista-only, purgar topic/fecha/valor heredados."""
    prev = QueryFrame(
        dataset="contratos",
        topic=["atencion", "mujeres"],
        modifiers={
            "fecha_desde": "2025-01-01",
            "fecha_hasta": "2025-12-31",
            "valor_min": 500_000_000,
        },
        raw_params={
            "objeto": ["atencion", "mujeres"],
            "fecha_desde": "2025-01-01",
            "fecha_hasta": "2025-12-31",
            "valor_min": 500_000_000,
        },
    )
    curr = {"contratista": ["72287442"], "dataset": "contratos"}

    intent, merged = detect_and_merge("buscar proveedor 72287442", curr, prev)

    assert "atencion" not in merged.get("objeto", [])
    assert "mujeres" not in merged.get("objeto", [])
    assert merged.get("fecha_desde") is None
    assert merged.get("valor_min") is None
    assert "72287442" in merged.get("contratista", [])


def test_contratista_explicit_intent_preserved():
    """Si el follow-up incluye tema nuevo + contratista, NO purgar."""
    prev = QueryFrame(
        dataset="contratos",
        topic=[],
        modifiers={},
        raw_params={},
    )
    curr = {
        "objeto": ["obras", "mantenimiento"],
        "contratista": ["72287442"],
        "dataset": "contratos",
    }

    intent, merged = detect_and_merge(
        "obras de mantenimiento del proveedor 72287442", curr, prev
    )

    # Tema NUEVO se preserva, contratista nuevo también.
    assert "mantenimiento" in merged.get("objeto", []) or \
           "obras" in merged.get("objeto", [])
    assert "72287442" in merged.get("contratista", [])
