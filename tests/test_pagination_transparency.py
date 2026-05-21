from app.core.formatter import Formatter
from app.core.observer import _compute_from_sample


def _rows(n: int) -> list[dict]:
    return [
        {
            "entidad": f"Entidad {idx}",
            "precio_base": str(1000000 + idx),
            "estado_de_apertura_del_proceso": "Publicado",
            "fecha_de_publicacion_del": "2026-01-01T00:00:00",
            "modalidad_de_contratacion": "Licitacion publica",
            "nombre_del_procedimiento": f"Proceso {idx}",
        }
        for idx in range(n)
    ]


def test_sample_insights_marks_truncated_universe() -> None:
    insights = _compute_from_sample(_rows(50), total_count=1647, limit=50)

    assert insights.truncation_warning is True
    assert insights.truncation_ratio == 50 / 1647


def test_sample_insights_omits_truncation_when_total_fits() -> None:
    insights = _compute_from_sample(_rows(12), total_count=12, limit=50)

    assert insights.truncation_warning is False
    assert insights.truncation_ratio is None


def test_formatter_names_remaining_results_when_paginated() -> None:
    formatter = Formatter(max_results=50)
    rows = formatter.to_rows(_rows(50), "p6dx-8zbt")

    message = formatter.format_telegram(
        rows,
        params={},
        dataset_id="p6dx-8zbt",
        total_count=1647,
    )

    assert "1,647" in message
    assert "50" in message
    assert "1,597" in message
    assert "mas" in message
