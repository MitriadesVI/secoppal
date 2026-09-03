"""Institutional-family compatibility for strict buyer rewrites.

Territory tokens are not buyer identity.  A municipal executive mention may
share both territory and NIT with a personeria, and a local mayoralty may share
its locality token with a military accounting unit.  Strict rewrites must keep
the requested institutional family before accepting exact or fuzzy ownership.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver

ALIASES_DB = Path("app/data/aliases_db.json")
VILLA_DEL_ROSARIO = "MUNICIPIO DE VILLA DEL ROSARIO**"
PERSONERIA_VILLA_DEL_ROSARIO = "PERSONERIA MUNICIPIO DE VILLA DEL ROSARIO"
PUENTE_ARANDA_LOCAL = (
    "ALCALDIA LOCAL DE PUENTE ARANDA-FONDO DE DESARROLLO LOCAL"
)
CENAC_PUENTE_ARANDA = "CENAC PUENTE ARANDA+"
CHAPINERO_LOCAL = (
    "ALCALDÍA LOCAL DE CHAPINERO - FONDO DE DESARROLLO LOCAL"
)


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


def _resolver_with_single_claimant(
    tmp_path: Path,
    *,
    official: str,
    alias: str,
) -> EntityResolver:
    path = tmp_path / "aliases.json"
    path.write_text(
        json.dumps([{"official_name": official, "aliases": [alias]}]),
        encoding="utf-8",
    )
    return EntityResolver(path)


class TestMunicipalExecutiveCompatibility:
    @pytest.mark.parametrize("head", ["PERSONERIA", "POLICIA", "CONCEJO", "SECRETARIA"])
    @pytest.mark.parametrize("tail", [
        "MUNICIPIO DE VILLA DEL ROSARIO",
        "ALCALDIA LOCAL DE CHAPINERO",
        "FONDO DE DESARROLLO LOCAL DE CHAPINERO",
    ])
    def test_another_institution_cannot_hide_before_executive_words(
        self, head: str, tail: str,
    ) -> None:
        assert not EntityResolver._official_matches_institutional_family(
            f"{head} {tail}", "municipality"
        )
        assert not EntityResolver._official_matches_institutional_family(
            f"{head} {tail}", "local_executive"
        )

    def test_villa_del_rosario_keeps_the_municipal_executive(
        self, resolver: EntityResolver
    ) -> None:
        result = resolver.resolve_entidad("alcaldia de villa del rosario")
        assert result.value == VILLA_DEL_ROSARIO

    @pytest.mark.parametrize(
        ("phrase", "expected"),
        [
            ("alcaldia de pereira, risaralda", "MUNICIPIO DE PEREIRA- OFICIAL"),
            (
                "alcaldia de mosquera cundinamarca",
                "CUNDINAMARCA-ALCALDIA MUNICIPIO MOSQUERA",
            ),
        ],
    )
    def test_exact_alias_may_carry_more_territory_than_the_physical_name(
        self, resolver: EntityResolver, phrase: str, expected: str
    ) -> None:
        assert resolver.resolve_entidad(phrase).value == expected

    def test_personeria_is_not_compatible_with_municipal_executive_rewrite(
        self,
    ) -> None:
        strict = ("municipality", "villa del rosario")
        assert not EntityResolver._official_matches_strict_rewrite(
            PERSONERIA_VILLA_DEL_ROSARIO,
            strict,
        )

    def test_wrong_exact_claimant_fails_closed(self, tmp_path: Path) -> None:
        resolver = _resolver_with_single_claimant(
            tmp_path,
            official=PERSONERIA_VILLA_DEL_ROSARIO,
            alias="municipio de villa del rosario",
        )
        result = resolver.resolve_entidad("municipio de villa del rosario")
        assert result.value is None
        assert result.method == "rewrite_unresolved"
        assert result.clarification_needed is True

    def test_wrong_scan_claimant_is_rejected(self, tmp_path: Path) -> None:
        resolver = _resolver_with_single_claimant(
            tmp_path,
            official=PERSONERIA_VILLA_DEL_ROSARIO,
            alias="alcaldia de villa del rosario",
        )
        assert resolver.scan_entity(
            "contratos de la alcaldia de villa del rosario"
        ) is None


class TestLocalExecutiveCompatibility:
    @pytest.mark.parametrize(
        ("phrase", "expected"),
        [
            ("alcaldia local de puente aranda", PUENTE_ARANDA_LOCAL),
            ("alcaldia local de chapinero", CHAPINERO_LOCAL),
        ],
    )
    def test_verified_local_executives_stay_resolved(
        self, resolver: EntityResolver, phrase: str, expected: str
    ) -> None:
        assert resolver.resolve_entidad(phrase).value == expected

    def test_cenac_is_not_compatible_with_local_executive_rewrite(self) -> None:
        strict = ("local_executive", "puente aranda")
        assert not EntityResolver._official_matches_strict_rewrite(
            CENAC_PUENTE_ARANDA,
            strict,
        )

    def test_wrong_exact_claimant_fails_closed(self, tmp_path: Path) -> None:
        resolver = _resolver_with_single_claimant(
            tmp_path,
            official=CENAC_PUENTE_ARANDA,
            alias="alcaldia local de puente aranda",
        )
        result = resolver.resolve_entidad("alcaldia local de puente aranda")
        assert result.value is None
        assert result.method == "rewrite_unresolved"
        assert result.clarification_needed is True

    def test_wrong_scan_claimant_is_rejected(self, tmp_path: Path) -> None:
        resolver = _resolver_with_single_claimant(
            tmp_path,
            official=CENAC_PUENTE_ARANDA,
            alias="alcaldia local de puente aranda",
        )
        assert resolver.scan_entity(
            "contratos de la alcaldia local de puente aranda"
        ) is None
