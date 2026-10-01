"""M1 wave 1 PARIDAD: in ``legacy`` mode the Foundation-mode modules are IMPORTED but never RUN.

``orchestrator`` imports ``foundation_runtime`` lazily at every turn; that is the declared MODE FRONTIER of the
import-closure pin (``test_m1_wave1_parity_pins.py``). This test is the execution half of the same argument: with
spies on the public entries of the Foundation-mode modules and of the D-MONEY quote-route lexicon, a legacy-mode
``run_query`` never calls them, while ``run_foundation_search`` IS called and returns ``None`` (the frontier is
really exercised).

``M1_PV_SEQUENCES=<json file>`` replaces the curated sequences with a list of turn sequences (the parity gate
runs the 2.092 e2e sequences through this very function).
"""
from __future__ import annotations

import inspect
import json
import os
import sys
from datetime import date
from pathlib import Path

import pytest

from app.core import foundation_admission, foundation_runtime, foundation_value_direction, money_comparators
from app.core.soql_builder import SoQLBuilder
from tests.product_contract.harness import ProductHarness

try:
    from app.core.foundation_projection import SOURCE_SELECT_FIELDS
except ImportError:  # pragma: no cover
    SOURCE_SELECT_FIELDS = {}

TODAY = date(2026, 10, 1)

CURATED = [
    ["contratos de aseo universidad nacional sin superar 60 millones"],
    ["contratos de aseo del ICBF que no deben superar 60 millones"],
    ["contratos de por lo menos 300 millones"],
    ["contratos de la Alcaldía de Galapa de 300 millones o mayores Galapa"],
    ["contratos de la Alcaldía de Galapa más o menos 300 millones"],
    ["contratos de obra de menos de 50 millones"],
    ["procesos de aseo con valor mayor o igual a 400 millones"],
    ["contratos de aseo de la alcaldía de soledad en 2025"],
    ["procesos del ICBF este año"],
    ["contratos de vías de la gobernación de antioquia desde enero de 2025 hasta marzo de 2025"],
    ["contratos de consultoría del SENA en el primer trimestre de 2025"],
    ["contratos de aseo de la alcaldía de barranquilla de menos de 50 millones en 2024"],
    ["contratos de aseo de la alcaldía de soledad en 2025", "y en 2024"],
    ["contratos de vías de la gobernación de antioquia de más de 300 millones", "sin superar 60 millones"],
    ["contratos de obra en Galapa el año pasado", "ahora de más de 100 millones"],
    ["proveedores de la Alcaldía de Soledad"],
    ["perfil de la Gobernación del Atlántico"],
    ["busca procesos de licitación abiertos"],
    ["hola"],
]


def _sequences():
    path = os.environ.get("M1_PV_SEQUENCES")
    return json.loads(Path(path).read_text(encoding="utf-8")) if path else CURATED


def _spy_everywhere(monkeypatch, calls, owner, names):
    """Replace ``owner.<name>`` in every ``app.*`` module that bound the same function object."""
    for name in names:
        original = getattr(owner, name)

        def wrapper(*args, __original=original, __name=f"{owner.__name__}.{name}", **kwargs):
            calls.append(__name)
            return __original(*args, **kwargs)

        for module in list(sys.modules.values()):
            if getattr(module, "__name__", "").startswith("app.") and getattr(module, name, None) is original:
                monkeypatch.setattr(module, name, wrapper)


def _public_functions(module):
    return [name for name, value in vars(module).items()
            if inspect.isfunction(value) and value.__module__ == module.__name__ and not name.startswith("_")]


@pytest.fixture
def spies(monkeypatch):
    calls: list[str] = []
    _spy_everywhere(monkeypatch, calls, foundation_value_direction, ["operator_reason", "directions"])
    _spy_everywhere(monkeypatch, calls, foundation_admission, ["admit_proposal"])
    _spy_everywhere(monkeypatch, calls, money_comparators, _public_functions(money_comparators))
    return calls


def test_the_spies_are_live(spies):
    foundation_value_direction.operator_reason("greater_than", "más de 300 millones")
    money_comparators.read_comparators("más de 300 millones")
    assert "app.core.foundation_value_direction.operator_reason" in spies
    assert "app.core.money_comparators.read_comparators" in spies
    # operator_reason reads through read_comparators: both are seen even from a module-level import
    assert spies.count("app.core.money_comparators.read_comparators") >= 2


def test_in_legacy_mode_the_foundation_modules_are_imported_but_never_run(tmp_path, monkeypatch, spies):
    frontier = []
    original = foundation_runtime.run_foundation_search

    def frontier_spy(*args, **kwargs):
        result = original(*args, **kwargs)
        frontier.append(result)
        return result

    monkeypatch.setattr(foundation_runtime, "run_foundation_search", frontier_spy)
    rows = {dataset: [dict.fromkeys({*SoQLBuilder.SPECS[dataset].select_fields, *SOURCE_SELECT_FIELDS.get(dataset, ())})]
            for dataset in (SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET)}
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows, today=TODAY,
                             settings_overrides={"secoppal_foundation_mode": "legacy"})
    assert harness.settings.secoppal_foundation_mode == "legacy"
    sequences = _sequences()
    turns = 0
    for index, sequence in enumerate(sequences):
        for text in sequence:
            harness.flow.run_query(text, chat_id=f"pv-{index}", today=TODAY)
            turns += 1
    assert turns >= len(sequences) and turns > 0
    assert frontier, "the frontier was never exercised: run_foundation_search was not called"
    assert all(result is None for result in frontier), "legacy mode must never be claimed by Foundation"
    assert spies == [], f"Foundation-mode / D-MONEY entries ran in legacy mode: {sorted(set(spies))}"
