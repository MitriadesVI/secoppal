"""R2A — la puerta de ownership múltiple mira el CONJUNTO, no el cardinal.

El gate anterior comparaba ``len(multi_owner)`` contra un tope. Un lote que
salda una colisión vieja e introduce otra distinta deja el cardinal intacto y
pasa la puerta mientras crea deuda nueva. Estas pruebas fijan que:

* la deuda histórica puede encogerse sin romper nada;
* una clave nueva falla aunque el cardinal no aumente;
* el baseline congelado sigue describiendo el estado real del gazetteer.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS_DIR = _PROJECT_ROOT / "scripts"
for _path in (str(_SCRIPTS_DIR), str(_PROJECT_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from gazetteer_manager import do_validate  # noqa: E402
from gazetteer_ownership_baseline import (  # noqa: E402
    DEFAULT_BASELINE_PATH,
    load_baseline,
    multi_owner_groups,
    new_multi_owner_keys,
)

ALIASES_DB = _PROJECT_ROOT / "app" / "data" / "aliases_db.json"


def _records(pairs: list[tuple[str, list[str]]]) -> list[dict]:
    return [{"official_name": name, "aliases": aliases} for name, aliases in pairs]


# Baseline sintético con tres colisiones: A, B, C.
_BASELINE_RECORDS = _records([
    ("ENTIDAD UNO", ["colision a", "colision b", "colision c"]),
    ("ENTIDAD DOS", ["colision a", "colision b", "colision c"]),
])


@pytest.fixture()
def synthetic_baseline(tmp_path: Path) -> Path:
    keys = sorted(multi_owner_groups(_BASELINE_RECORDS))
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps({"count": len(keys), "keys": keys}), encoding="utf-8")
    return path


def _write_db(tmp_path: Path, records: list[dict]) -> Path:
    path = tmp_path / "aliases.json"
    path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
    return path


class TestSetIdentityGate:
    def test_baseline_state_itself_passes(self, synthetic_baseline: Path) -> None:
        assert new_multi_owner_keys(_BASELINE_RECORDS, load_baseline(synthetic_baseline)) == []

    def test_settled_debt_is_allowed_to_shrink(self, synthetic_baseline: Path) -> None:
        """173 → 170 es válido: no aparecen claves nuevas."""
        shrunk = _records([
            ("ENTIDAD UNO", ["colision a", "colision b", "colision c"]),
            ("ENTIDAD DOS", ["colision a"]),
        ])
        assert len(multi_owner_groups(shrunk)) < len(multi_owner_groups(_BASELINE_RECORDS))
        assert new_multi_owner_keys(shrunk, load_baseline(synthetic_baseline)) == []

    def test_substitution_at_equal_cardinality_fails(self, synthetic_baseline: Path) -> None:
        """La mutación central: se retira C, entra D, el cardinal no cambia."""
        mutated = _records([
            ("ENTIDAD UNO", ["colision a", "colision b", "colision d"]),
            ("ENTIDAD DOS", ["colision a", "colision b", "colision d"]),
        ])
        assert len(multi_owner_groups(mutated)) == len(multi_owner_groups(_BASELINE_RECORDS))
        assert new_multi_owner_keys(mutated, load_baseline(synthetic_baseline)) == ["colision d"]

    def test_validator_fails_on_substitution_at_equal_cardinality(
        self, tmp_path: Path, synthetic_baseline: Path
    ) -> None:
        """El cardinal pasa el tope; ``do_validate`` debe fallar igual."""
        mutated = _records([
            ("ENTIDAD UNO", ["colision a", "colision b", "colision d"]),
            ("ENTIDAD DOS", ["colision a", "colision b", "colision d"]),
        ])
        db_path = _write_db(tmp_path, mutated)
        result = do_validate(db_path, baseline_path=synthetic_baseline)

        assert result["exact_multi_owner_groups"] == 3
        assert result["new_multi_owner_keys"] == ["colision d"]
        assert any(e.startswith("NEW_MULTI_OWNER_KEYS") for e in result["errors"])

    def test_validator_passes_when_debt_only_shrinks(
        self, tmp_path: Path, synthetic_baseline: Path
    ) -> None:
        shrunk = _records([
            ("ENTIDAD UNO", ["colision a", "colision b", "colision c"]),
            ("ENTIDAD DOS", ["colision a"]),
        ])
        db_path = _write_db(tmp_path, shrunk)
        result = do_validate(db_path, baseline_path=synthetic_baseline)

        assert result["new_multi_owner_keys"] == []
        assert not [e for e in result["errors"] if "MULTI_OWNER" in e]

    def test_normalization_is_the_runtime_one(self, synthetic_baseline: Path) -> None:
        """Una grafía distinta de la misma frase colisiona igual."""
        accented = _records([
            ("ENTIDAD UNO", ["Colisión A"]),
            ("ENTIDAD DOS", ["colision a"]),
        ])
        assert list(multi_owner_groups(accented)) == ["colision a"]
        assert new_multi_owner_keys(accented, load_baseline(synthetic_baseline)) == []


class TestFrozenBaselineMatchesGazetteer:
    def test_real_gazetteer_introduces_no_new_keys(self) -> None:
        with ALIASES_DB.open(encoding="utf-8") as handle:
            records = json.load(handle)
        assert new_multi_owner_keys(records, load_baseline()) == []

    def test_baseline_never_grows_beyond_r1c(self) -> None:
        """El tope BAJA cuando se salda deuda; no sube."""
        assert len(load_baseline()) <= 173

    def test_frozen_baseline_is_reachable_from_its_default_path(self) -> None:
        assert DEFAULT_BASELINE_PATH.exists()
