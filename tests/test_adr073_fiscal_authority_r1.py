"""Regression for Schema 2 fiscal authority preserved by Schema 3."""

import json
from pathlib import Path

import pytest
from hypothesis import given, strategies as st

from app.core.actor_identity import compute_check_digit
from app.core.buyer_source_names import load_registry
from scripts.audits.adr073_fiscal_authority_gate import (
    D0_CANONICALS,
    audit_entries,
    expected_form,
    verify,
)

ROOT = Path(__file__).resolve().parents[1]
SIDECAR = ROOT / "app/data/buyer_source_names.json"
BEFORE = ROOT / "artifacts/audits/adr073_fiscal_r1/fiscal_evidence_audit_before.jsonl"
SCHEMA2 = ROOT / "tests/fixtures/adr073_fiscal_r1/schema2_nit_authorities.json"


def _fiscal_report():
    payload = json.loads(SIDECAR.read_text(encoding="utf-8"))
    before = [json.loads(line) for line in BEFORE.read_text(encoding="utf-8").splitlines()]
    return verify(payload, audit_entries(payload), before)


@pytest.mark.parametrize("canonical,accredited_nit", [
    ("DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA", "899999083"),
    ("UNIATLANTICO", "890102257"),
    ("UNIVERSIDAD COLEGIO MAYOR DE CUNDINAMARCA", "800144829"),
    ("UNIVERSIDAD DE CÓRDOBA", "891080031"),
])
def test_schema2_nine_digit_nit_is_not_reinterpreted_as_eight_plus_dv(
    canonical, accredited_nit,
):
    binding = load_registry().binding_for(canonical)
    assert binding is not None
    assert len(binding.fiscal_attestations) == 1
    accredited = binding.fiscal_attestations[0]
    assert accredited.raw_forms == (accredited_nit,)
    assert accredited.base_number == accredited_nit
    assert accredited.check_digit.declared is None
    assert accredited.check_digit.split_validated is False


def test_positive_control_unsplit_nine_digit_nit_is_observed():
    accredited = load_registry().binding_for(
        "ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS"
    ).fiscal_attestations[0]
    assert accredited.base_number == "890480184"
    assert accredited.check_digit.split_validated is False


def test_negative_control_authorized_ten_digit_form_remains_split():
    accredited = load_registry().binding_for(
        "FONDO FINANCIERO DISTRITAL DE SALUD"
    ).fiscal_attestations[0]
    assert accredited.raw_forms == ("8002469532",)
    assert accredited.base_number == "800246953"
    assert accredited.check_digit.declared == "2"
    assert accredited.check_digit.split_validated is True


def test_all_18_schema2_fiscal_authorities_are_preserved():
    report = _fiscal_report()
    assert report["fiscal_authority"]["schema2_nit_authorities"] == 18
    assert report["fiscal_authority"]["schema3_accreditations"] == 18
    assert report["fiscal_authority"]["preserved"] == 18
    assert report["fiscal_authority"]["pass"] is True


def test_all_594_directory_observations_keep_raw_facts_and_normalization_boundary():
    report = _fiscal_report()
    assert report["source_observations_audited"] == 594
    assert report["source_facts_unchanged"] is True
    assert report["unauthorized_9_to_8_after"] == 0
    assert report["review_after"] == 0


def test_six_D0_canaries_keep_concrete_schema2_nits_and_directory_absence():
    report = _fiscal_report()
    frozen = {row["canonical_name"]: row["schema2_nit"] for row in
              json.loads(SCHEMA2.read_text(encoding="utf-8"))["authorities"]}
    assert len(report["d0_concrete_authorities"]) == len(D0_CANONICALS) == 6
    for item in report["d0_concrete_authorities"]:
        assert item["schema2_nit"] == frozen[item["canonical"]]
        assert item["accreditation_raw_preserved"] is True
        assert item["accreditation_authority_preserved"] is True
        assert item["directory_not_found_preserved"] is True


def test_leading_zero_normalization_is_the_declared_census_case():
    binding = load_registry().binding_for("SECRETARÍA DISTRITAL DE CULTURA, RECREACIÓN Y DEPORTE")
    observation = next(item for item in binding.source_observations
                       if "0899999061" in item.raw_forms)
    assert observation.raw_forms == ("0899999061",)
    assert observation.base_number == "899999061"
    assert observation.check_digit.declared is None
    assert observation.check_digit.split_validated is False


@pytest.mark.parametrize("canonical,raw", [
    ("FONDO DE DESARROLLO LOCAL DE BOSA", "8999990615"),
    ("ICBF REGIONAL SANTANDER", "8999992391"),
])
def test_invalid_ten_digit_dv_keeps_raw_whole(canonical, raw):
    observation = load_registry().binding_for(canonical).source_observations[0]
    assert observation.raw_forms == (raw,)
    assert observation.base_number == raw
    assert observation.check_digit.split_validated is False


@given(st.integers(min_value=10_000_000, max_value=99_999_999))
def test_accidental_nine_digit_modulo_11_match_never_splits(first_eight):
    prefix = str(first_eight)
    raw = prefix + str(compute_check_digit(prefix))
    policy = expected_form(raw)
    assert policy["reason"] == "NAKED_9_IS_COMPLETE_NIT"
    assert policy["base"] == raw
    assert policy["declared"] is None
    assert policy["split"] is False


@given(st.integers(min_value=100_000_000, max_value=999_999_999))
def test_valid_ten_digit_dv_keeps_raw_and_split_base(first_nine):
    base = str(first_nine)
    raw = base + str(compute_check_digit(base))
    policy = expected_form(raw)
    assert policy["reason"] == "VALIDATED_10_TO_9_DV"
    assert policy["base"] == base
    assert policy["declared"] == raw[-1]
    assert policy["split"] is True
