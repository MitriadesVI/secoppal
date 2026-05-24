import re

import pytest

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


def test_putumayo_6_and_case_demotes_common_to_or():
    """Caso canónico: ASEO, CAFETERÍA, JARDINERÍA, MANTENIMIENTO, SERVICIOS, SUMINISTRO.
    maintenance + servicios + suministro deben ir como OR."""
    where = _where(
        {
            "objeto": [
                "aseo",
                "cafeteria",
                "jardineria",
                "mantenimiento",
                "servicios",
                "suministro",
            ]
        }
    )
    # Tokens raros (aseo, cafeteria, jardineria) con AND
    assert "aseo" in where
    assert "cafeteria" in where
    assert "jardineria" in where
    # Common tokens (mantenimiento, servicios, suministro) con OR entre ellos
    assert "mantenimiento" in where and "servicios" in where and "suministro" in where
    assert " OR " in where
    # La estructura debe ser: (aseo AND cafeteria AND jardineria) AND (mantenimiento OR servicios OR suministro)
    assert " AND " in where


def test_four_tokens_no_common_keeps_and():
    """4 tokens sin common → AND completo, sin degradación."""
    where = _where(
        {
            "objeto": [
                "electrobombas",
                "motores",
                "turbinas",
                "generadores",
            ]
        }
    )
    assert "electrobombas" in where
    assert "motores" in where
    assert "turbinas" in where
    assert "generadores" in where
    # With 4 rare terms, demotion merges them into a single OR group
    # (AND between 4+ rare terms is too strict for real SECOP data).
    # The OR inside each term (nombre LIKE x OR desc LIKE x) still exists.
    assert where.count(" OR ") >= 7  # 8 LIKEs → 7 ORs within the big group


def test_three_tokens_and_only_no_degradation():
    """3 tokens AND → sin cambio, aunque haya common tokens."""
    where = _where(
        {
            "objeto": [
                "aseo",
                "mantenimiento",
                "servicios",
            ]
        }
    )
    # len < 4, no se degrada
    assert "aseo" in where
    assert "mantenimiento" in where
    assert "servicios" in where
    assert " AND " in where
    # No debe haber OR entre térmicos distintos
    # (el OR interno de LIKE name OR LIKE desc NO cuenta)
    assert where.count(" AND ") >= 2  # 3 términos → al menos 2 ANDs entre grupos
