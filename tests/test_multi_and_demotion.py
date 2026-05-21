import re

from app.core.soql_builder import SoQLBuilder


def _where(params: dict) -> str:
    soql = SoQLBuilder().build(SoQLBuilder.PROCESOS_DATASET, params)
    return soql.split(" WHERE ", 1)[1].split(" ORDER BY ", 1)[0]


def test_short_object_list_still_uses_and_between_terms() -> None:
    where = _where({"objeto": ["aseo", "jardineria"]})

    assert "aseo" in where
    assert "jardineria" in where
    assert " AND " in where


def test_long_object_list_demotes_common_terms_to_or_group() -> None:
    where = _where(
        {
            "objeto": [
                "aseo",
                "cafeteria",
                "jardineria",
                "mantenimiento",
                "servicios",
            ]
        }
    )

    assert "aseo" in where
    assert "cafeteria" in where
    assert "jardineria" in where
    assert "mantenimiento" in where
    assert "servicios" in where
    assert re.search(r"mantenimiento.* OR .*servicios|servicios.* OR .*mantenimiento", where)
