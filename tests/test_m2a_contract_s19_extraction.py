"""§19 as the ONE authority of dependencies (R3, U19/U23): the generated extraction fails closed on every malformation.

Each failure mode the user listed has its own test, on a copy of the contract text (or of the sealed records) given to
``extract`` in memory; nothing in the repository is modified. The positive control is that the committed extraction
reproduces byte for byte and names SF075's owner.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "extract_contract_dependencies", ROOT / "scripts/audits/m2a_conversation/extract_contract_dependencies.py")
X = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(X)
CONTRACT = X.text_mod.read()
RECORDS = [json.loads(x) for x in X.RECORDS.read_text(encoding="utf-8").splitlines() if x.strip()]


def fails(message: str, contract: str = CONTRACT, records=None) -> None:
    with pytest.raises(SystemExit) as caught:
        X.extract(contract_text=contract, records=RECORDS if records is None else records)
    assert message in str(caught.value), str(caught.value)


def mutate(old: str, new: str) -> str:
    assert CONTRACT.count(old) == 1, old
    return CONTRACT.replace(old, new)


SF075_ROW = "| [SF075] | 1 | COMPOSICION |"


def test_the_extraction_reproduces_and_names_sf075s_owner():
    assert X.OUT.read_text(encoding="utf-8") == X.render()
    extraction = X.extract()
    assert extraction["case_owners"]["SF075"] == ["FOUNDATION_O1B_EXPLICIT_TIME_PARITY", "M2A-1b"]
    assert extraction["case_impacto_m2a3"]["SF192"] == "COMPOSICION"
    assert {d["dep_id"]: d["cases"] for d in extraction["dependencies"]}["DEP-VALUE-EQUALITY"] == []


def test_unparseable_table_fails():
    fails("table header found 0 times", mutate("| Id | Alcance | Ids | Conteo | impacto_m2a3 | Necesitan | Dueño |",
                                               "| Id | Alcance | Ids | Conteo | efecto | Necesitan | Dueño |"))


def test_missing_section_fails():
    fails("missing section start", mutate("## 19. Frontera: M2b, M3 y E-*", "## 19. Frontera"))


@pytest.mark.parametrize("written", ["sf075", "SF-075", "SF75", " SF075"])
def test_a_case_id_with_another_shape_fails(written):
    fails("malformed case id", mutate(SF075_ROW, f"| [{written}] | 1 | COMPOSICION |"))


def test_an_unknown_case_id_fails():
    fails("cases not in the sealed M2A_GOLD: ['SF999']", mutate(SF075_ROW, "| [SF999] | 1 | COMPOSICION |"))


def test_a_count_that_differs_from_the_ids_fails():
    fails("declares 2 cases but lists 1", mutate(SF075_ROW, "| [SF075] | 2 | COMPOSICION |"))


def test_count_zero_with_ids_and_empty_ids_with_a_count_fail():
    fails("declares 0 cases but lists 1", mutate(SF075_ROW, "| [SF075] | 0 | COMPOSICION |"))
    fails("declares 1 cases but lists 0", mutate("| [] | 0 | LECTURA | lectura como par", "| [] | 1 | LECTURA | lectura como par"))


def test_a_count_that_is_not_an_integer_fails():
    fails("is not a non-negative integer", mutate(SF075_ROW, "| [SF075] | uno | COMPOSICION |"))


def test_an_unknown_owner_fails():
    fails("is not in the declared closed list", mutate("| E-ORDER |\n| `DEP-E-ORDER-CANDIDATES`", "| E-ORDEN |\n| `DEP-E-ORDER-CANDIDATES`"))


@pytest.mark.parametrize("value", ["EFECTO", "lectura", "COMPOSICIÓN", ""])
def test_an_unknown_impacto_value_fails(value):
    fails("impacto_m2a3", mutate(SF075_ROW, f"| [SF075] | 1 | {value} |"))


def test_gap_ids_must_be_exactly_the_sealed_frontier_records_of_the_gap():
    fails("ids differ from the sealed FOLLOWUP_FRONTIER", mutate("| [SF147, SF148] | 2 |", "| [SF147] | 1 |"))


def test_a_sealed_gap_absent_from_19_fails():
    row = next(line for line in CONTRACT.splitlines() if line.startswith("| G-FU-17 |"))
    fails("absent from §19", CONTRACT.replace(row + "\n", ""))


def test_unsorted_or_repeated_ids_fail():
    fails("ids are not sorted", mutate("| [SF037, SF042, SF174] | 3 |", "| [SF042, SF037, SF174] | 3 |"))
    fails("repeated case id", mutate("| [SF037, SF042, SF174] | 3 |", "| [SF037, SF037, SF174] | 3 |"))


def test_a_malformed_dependency_id_fails():
    fails("malformed dependency id", mutate("| `DEP-VALUE-EQUALITY` |", "| DEP-VALUE-EQUALITY |"))


def test_a_sealed_partial_declaration_must_name_a_composicion_row_listing_the_case():
    records = [dict(r) for r in RECORDS]
    sf192 = next(r for r in records if r["gold_id"] == "SF192")
    assert sf192["partial_alternatives"]["dependency"] == "DEP-E-ORDER-CANDIDATES"
    sf192["partial_alternatives"] = {**sf192["partial_alternatives"], "dependency": "DEP-E-ORDER"}  # a LECTURA row
    fails("not a COMPOSICION row of §19 listing the case", records=records)
    fails("not a COMPOSICION row of §19 listing the case",
          contract=mutate("| [SF192] | 1 | COMPOSICION |", "| [] | 0 | COMPOSICION |"))
