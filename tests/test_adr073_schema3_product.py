"""Schema 3 product contract against the frozen Schema 2 projection oracle."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest
from hypothesis import given, strategies as st

from app.core.buyer_fiscal_evidence import FiscalAccreditation, SourceObservation
from app.core.buyer_groups import BuyerGroup
from app.core.buyer_source_names import BuyerSourceNameError, load_registry, parse_registry
from app.utils.money import normalize_text
from scripts.audits.adr073_historical_projection import parse_historical_projection

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "tests/fixtures/adr073_schema3/schema2_projection_baseline.json"
SIDECAR = ROOT / "app/data/buyer_source_names.json"
DATASETS = ("p6dx-8zbt", "jbjy-vk9h")


def _payload() -> dict:
    return json.loads(SIDECAR.read_text(encoding="utf-8"))


def _fiscal(kind="ACCREDITATION", outcome=None, raw="899999061") -> dict:
    evidence = {
        "scheme": "NIT", "base_number": raw, "raw_forms": [raw],
        "check_digit": {"declared": None, "computed": None, "split_validated": False},
        "shared": True, "kind": kind, "source": "fixture",
        "observed_at": "2026-09-23", "evidence": "fixture",
    }
    if outcome is not None:
        evidence["outcome"] = outcome
    return evidence


def _unit(canonical: str, fiscal=None, names=None) -> dict:
    return {
        "canonical_name": canonical,
        "source_names": names or {DATASETS[0]: [canonical], DATASETS[1]: [canonical]},
        "projection_evidence": {"observed_at": "2026-09-23", "evidence": "fixture"},
        "fiscal_evidence": [] if fiscal is None else fiscal,
    }


def _registry(*units: dict):
    return parse_registry({"schema_version": 3, "authority": "ADR-073", "buyer_units": list(units)})


def test_schema2_to_schema3_projection_and_census_parity():
    frozen = json.loads(BASELINE.read_text(encoding="utf-8"))
    after = _payload()
    registry = load_registry()
    before = {b["canonical_name"]: b for b in frozen["bindings"]}
    units = {u["canonical_name"]: u for u in after["buyer_units"]}
    assert frozen["schema_version_before"] == 2
    assert after["schema_version"] == 3
    assert len(before) == len(units) == len(registry)
    assert set(before) == set(units)  # canonical diff = 0
    assert all(before[name]["sources"] == units[name]["source_names"] for name in before)
    assert all(before[name]["projection_evidence"] == units[name]["projection_evidence"]
               for name in before)

    before_index = {normalize_text(name): value for name, value in before.items()}
    census = frozen["census_canonicals"]
    with_binding = [name for name in census if normalize_text(name) in before_index]
    complement = [name for name in census if normalize_text(name) not in before_index]
    assert len({normalize_text(name) for name in census}) == len(census)

    def old_project(name, dataset):
        row = before_index.get(normalize_text(name))
        return tuple(row["sources"][dataset]) if row and dataset in row["sources"] else None

    def compare(names):
        pairs = [(name, dataset) for name in names for dataset in DATASETS]
        diffs = [(name, dataset, old_project(name, dataset), registry.project(name, dataset))
                 for name, dataset in pairs
                 if old_project(name, dataset) != registry.project(name, dataset)]
        return len(pairs), diffs

    binding_checks, binding_diffs = compare(before)
    complement_checks, complement_diffs = compare(complement)
    sweep_checks, sweep_diffs = compare(census)
    assert len(before) == len(with_binding) == 593
    assert len(complement) == 518
    assert len(census) == 1111
    assert binding_checks == 1186
    assert complement_checks == 1036
    assert sweep_checks == 2222
    assert binding_checks + complement_checks == sweep_checks
    assert len(with_binding) + len(complement) == len(census)
    assert not binding_diffs and not complement_diffs and not sweep_diffs
    assert all(old_project(name, dataset) is None
               for name in complement for dataset in DATASETS)


def test_A_shared_nit_keeps_two_buyer_units():
    reg = _registry(_unit("A", [_fiscal()]), _unit("B", [_fiscal()]))
    assert len(reg) == 2
    assert reg.project("A", DATASETS[0]) == ("A",)
    assert reg.project("B", DATASETS[0]) == ("B",)
    assert reg.binding_for("A").fiscal_attestations[0].base_number == (
        reg.binding_for("B").fiscal_attestations[0].base_number
    )


def test_B_no_nit_is_valid_and_projectable():
    reg = _registry(_unit("SIN NIT"))
    binding = reg.binding_for("SIN NIT")
    assert binding.fiscal_evidence == ()
    assert binding.fiscal_attestations == ()
    assert reg.project("SIN NIT", DATASETS[1]) == ("SIN NIT",)


def test_C_and_D_check_digit_forms_remain_distinct():
    valid = _fiscal(raw="890399011")
    valid["raw_forms"] = ["890399011", "8903990113"]
    valid["check_digit"] = {"declared": "3", "computed": 3, "split_validated": True}
    invalid = _fiscal(raw="8999990615")
    invalid["check_digit"] = {"declared": None, "computed": 9, "split_validated": False}
    reg = _registry(_unit("VALID", [valid]), _unit("INVALID", [invalid]))
    assert reg.binding_for("VALID").fiscal_attestations[0].raw_forms == (
        "890399011", "8903990113")
    assert reg.binding_for("VALID").fiscal_attestations[0].base_number == "890399011"
    assert reg.binding_for("INVALID").fiscal_attestations[0].base_number == "8999990615"


def test_E_directory_not_found_does_not_erase_accreditation():
    reg = _registry(_unit("A", [_fiscal(), _fiscal("SOURCE_OBSERVATION", "NOT_FOUND")]))
    binding = reg.binding_for("A")
    assert len(binding.fiscal_attestations) == 1
    assert isinstance(binding.fiscal_attestations[0], FiscalAccreditation)
    assert len(binding.source_observations) == 1
    assert isinstance(binding.source_observations[0], SourceObservation)
    assert binding.source_observations[0].outcome == "NOT_FOUND"


def test_directory_observation_alone_never_accredits_nit():
    reg = _registry(_unit("A", [_fiscal("SOURCE_OBSERVATION", "NOT_FOUND")]))
    binding = reg.binding_for("A")
    assert binding.fiscal_evidence  # An observation exists.
    assert binding.fiscal_attestations == ()
    assert binding.source_observations[0].outcome == "NOT_FOUND"


def test_historical_schema2_adapter_reads_projection_without_fiscal_authority():
    historical = {
        "schema_version": 2,
        "buyer_units": [],
        "bindings": [{
            "canonical_name": "HISTORICAL", "identity_key": {"type": "NIT", "value": "123"},
            "sources": {DATASETS[0]: ["HISTORICAL SOURCE"]},
            "observed_at": "2026-09-02", "evidence": "historical-fixture",
        }],
    }
    with pytest.raises(BuyerSourceNameError, match="schema_version"):
        parse_registry(historical)
    binding = parse_historical_projection(historical).binding_for("HISTORICAL")
    assert binding.names_for(DATASETS[0]) == ("HISTORICAL SOURCE",)
    assert binding.fiscal_attestations == ()


def test_F_group_keeps_two_members_with_shared_nit(monkeypatch):
    reg = _registry(_unit("A", [_fiscal()], {DATASETS[0]: ["A SOURCE"]}),
                    _unit("B", [_fiscal()], {DATASETS[0]: ["B SOURCE"]}))
    monkeypatch.setattr("app.core.buyer_groups.project_buyer_names", reg.project)
    group = BuyerGroup(group_id="FIXTURE", version="1", label="fixture",
                       aliases=(), members=("A", "B"))
    assert group.members == ("A", "B")
    assert group.names_for(DATASETS[0]) == ("A SOURCE", "B SOURCE")


def test_G_dataset_split_is_exact():
    reg = _registry(_unit("SOACHA", names={DATASETS[0]: ["MUNICIPIO DE SOACHA.."],
                                              DATASETS[1]: ["MUNICIPIO DE SOACHA."]}))
    assert reg.project("SOACHA", DATASETS[0]) == ("MUNICIPIO DE SOACHA..",)
    assert reg.project("SOACHA", DATASETS[1]) == ("MUNICIPIO DE SOACHA.",)


def test_real_shared_nit_canaries_are_observations_not_fiscal_identity():
    reg = load_registry()
    counts = Counter(ob.base_number for binding in reg.bindings()
                     for ob in binding.source_observations)
    # The declared census leading-zero form 0899999061 now normalizes to this
    # same nine-digit base, adding one observation to the Schema 3 draft count.
    assert counts["899999061"] == 38
    assert counts["899999239"] == 32
    assert len({b.canonical_name for b in reg.bindings()
                if any(o.base_number == "899999061" for o in b.source_observations)}) == 37


D0_CANONICALS = (
    "UNIATLANTICO", "UNIVERSIDAD DE CÓRDOBA", "UNIVERSIDAD DE LA AMAZONIA",
    "UNIVERSIDAD DEL PACÍFICO",
    "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA",
    "UNIVERSIDAD POPULAR DEL CESAR",
)


def test_six_D0_canaries_preserve_accreditation_and_directory_absence():
    reg = load_registry()
    frozen = json.loads(BASELINE.read_text(encoding="utf-8"))
    old_sources = {row["canonical_name"]: row["sources"] for row in frozen["bindings"]}
    assert len(D0_CANONICALS) == 6
    for canonical in D0_CANONICALS:
        binding = reg.binding_for(canonical)
        assert binding is not None
        assert binding.fiscal_attestations
        assert all(a.source != "SECOP_DIRECTORY" for a in binding.fiscal_attestations)
        observations = [o for o in binding.source_observations
                        if o.source == "SECOP_DIRECTORY"]
        assert len(observations) == 1
        assert observations[0].outcome == "NOT_FOUND"
        assert observations[0].outcome_note
        assert {dataset: list(names) for dataset, names in binding.source_names.items()} == (
            old_sources[canonical]
        )


def test_loader_rejects_old_identity_key_and_observation_without_outcome():
    payload = {"schema_version": 3, "buyer_units": [_unit("A")]}
    with pytest.raises(BuyerSourceNameError, match="identity_key"):
        parse_registry({**payload, "identity_key": "NIT:1"})
    with pytest.raises(BuyerSourceNameError, match="identity_key"):
        parse_registry({**payload, "buyer_units": [{**_unit("A"), "identity_key": "NIT:1"}]})
    malformed = _fiscal("SOURCE_OBSERVATION")
    with pytest.raises(BuyerSourceNameError, match="outcome"):
        _registry(_unit("A", [malformed]))


@given(st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ", min_size=1, max_size=20),
       st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ", min_size=1, max_size=20))
def test_shared_nit_never_merges_distinct_canonical_names(left, right):
    if normalize_text(left) == normalize_text(right):
        return
    reg = _registry(_unit(left, [_fiscal()]), _unit(right, [_fiscal()]))
    assert len(reg) == 2
    assert reg.project(left, DATASETS[0]) == (left,)
    assert reg.project(right, DATASETS[0]) == (right,)


def test_570_admissible_remain_unpromoted():
    matrix = ROOT / "artifacts/audits/adr073_schema3_design_v1/admissible_570_matrix.jsonl"
    rows = [json.loads(line) for line in matrix.read_text(encoding="utf-8").splitlines()
            if line.strip()]
    assert len(rows) == 570
    assert {row["promotion_status"] for row in rows} == {"NOT_EVALUATED"}
    assert {row["requires_gazetteer_reaudit"] for row in rows} == {"yes"}
    assert not any(row["promotion_status"] == "READY_TO_PROMOTE" for row in rows)
