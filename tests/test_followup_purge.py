from app.core.followup_engine import detect_and_merge
from app.core.query_frame import frame_from_params


def test_new_topic_drops_inherited_value_filter_but_keeps_scope() -> None:
    previous = frame_from_params(
        {
            "dataset": "procesos",
            "departamento_resolved": "Antioquia",
            "objeto": ["residuos"],
            "valor_max": 20000000,
        }
    )

    _intent, merged = detect_and_merge(
        "silvopastoriles",
        {"objeto": ["silvopastoriles"]},
        previous,
    )

    assert merged["departamento_resolved"] == "Antioquia"
    assert merged["objeto"] == ["silvopastoriles"]
    assert "valor_max" not in merged


def test_overlapping_topic_keeps_inherited_value_filter() -> None:
    previous = frame_from_params(
        {
            "dataset": "procesos",
            "departamento_resolved": "Huila",
            "objeto": ["transporte", "escolar"],
            "valor_max": 30000000,
        }
    )

    _intent, merged = detect_and_merge(
        "transporte escolar",
        {"objeto": ["transporte", "escolar"]},
        previous,
    )

    assert merged["valor_max"] == 30000000


def test_city_scope_drops_stale_entity_but_inherits_topic() -> None:
    previous = frame_from_params(
        {
            "dataset": "procesos",
            "entidad": "gobernacion de bolivar",
            "entidad_resolved": "GOBERNACION DE BOLIVAR",
            "departamento_resolved": "Bolívar",
            "objeto": ["mejoramiento"],
        }
    )

    _intent, merged = detect_and_merge(
        "ahora en magangue",
        {"ciudad": "Magangue"},
        previous,
    )

    assert merged["ciudad"] == "Magangue"
    assert merged["objeto"] == ["mejoramiento"]
    assert "entidad" not in merged
    assert "entidad_resolved" not in merged
