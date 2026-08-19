"""AP2-A — el contrato de la fuente contractual, congelado y atado al código.

Por qué este archivo existe
---------------------------
AP0 documentó que cinco de las seis fuentes del dossier llevaban catorce meses
siendo ``DOCUMENTED_ONLY``: descritas en un ADR, nunca verificadas contra el
servicio vivo. AP2 no repite ese error con la única fuente que sí usa. El
archivo ``tests/fixtures/actor_sources/secop2_contratos_source_contract.json``
registra lo que se observó el 2026-08-18 con probes acotadas, y estas pruebas
lo amarran a lo que el código realmente proyecta: si alguien cambia el
``DatasetSpec`` de contratos y no actualiza el contrato, esto rompe.

Lo que NO hace: tocar la red. Las cifras son las observadas y viven en el
fixture; aquí se verifican formas, invariantes y coherencia con el repo.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.actor_identity import normalize_identity
from app.core.soql_builder import SoQLBuilder

CONTRACT_PATH = Path("tests/fixtures/actor_sources/secop2_contratos_source_contract.json")


@pytest.fixture(scope="module")
def contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# El contrato describe el dataset que el código consulta de verdad.
# ---------------------------------------------------------------------------


def test_contract_targets_the_dataset_the_builder_uses(contract):
    assert contract["dataset_id"] == SoQLBuilder.CONTRATOS_DATASET


def test_every_field_the_profile_reads_is_projected_by_the_dataset_spec(contract):
    """Un campo que el perfil use pero el SELECT no transporte llegaría vacío.

    Es el fallo silencioso clásico: la métrica sale en cero y parece un actor
    sin actividad. Se ata aquí para que no dependa de la memoria de nadie.
    """
    spec = SoQLBuilder.SPECS[SoQLBuilder.CONTRATOS_DATASET]
    required = {
        contract["fields"][key]["name"]
        for key in (
            "document", "provider_name", "contract_id", "date", "value",
            "buyer", "modality", "territory_department", "territory_city",
            "state", "url",
        )
    }
    assert required <= set(spec.select_fields), required - set(spec.select_fields)


def test_the_spec_points_at_the_contract_document_and_name_fields(contract):
    spec = SoQLBuilder.SPECS[SoQLBuilder.CONTRATOS_DATASET]
    assert spec.contractor_document == contract["fields"]["document"]["name"]
    assert spec.contractor == contract["fields"]["provider_name"]["name"]
    assert spec.date == contract["fields"]["date"]["name"]
    assert spec.value == contract["fields"]["value"]["name"]
    assert spec.entity == contract["fields"]["buyer"]["name"]
    assert spec.department == contract["fields"]["territory_department"]["name"]


def test_procesos_has_no_provider_field_which_is_why_ap2_blocks_it():
    """La razón estructural de INV-DATASET-CAPABILITY, verificada en el código.

    No es una preferencia de producto: el dataset de procesos no publica quién
    contrata, así que un filtro de actor allí no tiene contra qué evaluarse.
    """
    procesos = SoQLBuilder.SPECS[SoQLBuilder.PROCESOS_DATASET]
    assert procesos.contractor is None
    assert procesos.contractor_document is None


# ---------------------------------------------------------------------------
# PII — lista blanca, no lista negra.
# ---------------------------------------------------------------------------


_PII_COLUMNS_THE_DATASET_EXPOSES = (
    "nombre_representante_legal",
    "identificaci_n_representante_legal",
    "domicilio_representante_legal",
    "g_nero_representante_legal",
    "nacionalidad_representante_legal",
    "n_mero_de_cuenta",
    "nombre_del_banco",
)


@pytest.mark.parametrize("column", _PII_COLUMNS_THE_DATASET_EXPOSES)
def test_inv_pii_the_profile_never_projects_personal_columns(column):
    """Lo que no se proyecta no puede filtrarse al render.

    El dataset publica domicilio, género e identificación del representante
    legal —datos de un tercero que nadie consultó— y el número de cuenta
    bancaria. Ninguno entra al SELECT.
    """
    spec = SoQLBuilder.SPECS[SoQLBuilder.CONTRATOS_DATASET]
    assert column not in spec.select_fields


# ---------------------------------------------------------------------------
# Los hallazgos de formato, expresados como comportamiento de AP1.
# ---------------------------------------------------------------------------


def test_the_canonical_nit_form_of_the_source_matches_the_identity_key(contract):
    """90% de los NIT se publican con 9 dígitos, sin DV — igual que base_number.

    Es el hallazgo que hace viable el filtro exacto: la llave canónica de AP1 y
    la forma dominante de la fuente coinciden sin conversión.
    """
    nit_lengths = contract["document_format_findings"]["lengths_when_type_is_nit"]
    assert max(nit_lengths, key=lambda k: nit_lengths[k]) == "9"

    match = normalize_identity("860002400", document_type="NIT")
    assert match.base_number == "860002400"
    assert len(match.base_number) == 9


def test_the_source_also_publishes_separated_check_digits(contract):
    """Existe '900454994-9' publicado tal cual: el filtro exacto necesita variantes."""
    findings = " ".join(contract["document_format_findings"]["findings"])
    assert "900454994-9" in findings

    match = normalize_identity("900454994-9", document_type="NIT")
    assert match.base_number == "900454994"
    assert match.check_digit == "9"


def test_the_sentinel_the_source_publishes_is_rejected_by_ap1(contract):
    """'No Definido' aparece 175.764 veces, a veces con tipo declarado NIT.

    Tratarlo como documento crearía un actor que agrupa a todos los registros
    sin dato: la peor fusión posible.
    """
    assert contract["fields"]["document"]["sentinel_values"] == ["No Definido"]
    assert normalize_identity("No Definido", document_type="NIT").key is None


def test_the_dominant_cedula_length_is_the_one_the_type_guard_protects(contract):
    """2,49M de cédulas tienen 10 dígitos: el caso que el módulo 11 mutilaría."""
    cedula_lengths = contract["document_format_findings"]["lengths_when_type_is_cedula"]
    assert max(cedula_lengths, key=lambda k: cedula_lengths[k]) == "10"


# ---------------------------------------------------------------------------
# La colisión de actores es real, no hipotética.
# ---------------------------------------------------------------------------


def test_the_same_document_appears_under_more_than_one_document_type(contract):
    """Verificado en vivo: 13848603 es cédula de una persona Y NIT de una UT.

    Filtrar solo por el número mezcla dos actores. Es INV-ID-TYPE dejando de
    ser doctrina para volverse un dato del dataset.
    """
    collision = " ".join(contract["duplicate_findings"]["actor_collision"])
    assert "13848603" in collision
    assert "Cédula de Ciudadanía" in collision and "NIT" in collision

    as_cedula = normalize_identity("13848603", document_type="CC")
    as_nit = normalize_identity("13848603", document_type="NIT")
    assert as_cedula.key != as_nit.key


def test_the_contract_identifier_is_unique_so_rows_need_no_dedup(contract):
    contract_id = contract["fields"]["contract_id"]
    assert contract_id["unique"] is True
    assert contract_id["distinct_count"] == contract["total_rows_observed"]


def test_one_document_accumulates_many_observed_names(contract):
    """Once nombres para un NIT, incluidas UT que solo lo contienen.

    Sostiene INV-OBSERVED-NAME: estos nombres son "visto en SECOP", nunca
    "razón social registrada".
    """
    names = contract["observed_names_sample"]["names"]
    assert len(names) >= 4
    assert any("UNIÓN TEMPORAL" in n["name"] or "UT " in n["name"] for n in names)


# ---------------------------------------------------------------------------
# Nulos y estados: lo que el perfil debe declarar en vez de esconder.
# ---------------------------------------------------------------------------


def test_the_date_field_is_the_only_one_with_material_nulls(contract):
    """7,1% de contratos sin fecha de firma: las métricas temporales son parciales."""
    fields = contract["fields"]
    assert fields["date"]["null_count"] > 0
    assert fields["value"]["null_count"] == 0
    assert fields["buyer"]["null_count"] == 0
    assert fields["modality"]["null_count"] == 0
    assert fields["territory_department"]["null_count"] == 0


def test_the_universe_includes_draft_and_cancelled_contracts(contract):
    """Por eso la métrica se nombra 'reportado' y no 'ejecutado' (INV-SOURCE-VALUE)."""
    states = {row["estado_contrato"] for row in contract["frozen_rows"]}
    assert "Borrador" in states or "Cancelado" in states
    assert any("Borrador" in note or "reportado" in note
               for note in contract["state_findings"])


def test_frozen_rows_carry_every_never_null_field(contract):
    """Las filas congeladas son el doble de la fuente para los tests de AP2-C."""
    required = {
        contract["fields"][key]["name"]
        for key in ("document", "document_type", "provider_name", "contract_id",
                    "value", "buyer", "modality", "territory_department")
    }
    for row in contract["frozen_rows"]:
        assert required <= set(row), required - set(row)


def test_socrata_omits_the_key_instead_of_sending_null(contract):
    """Hallazgo de AP2-A: un campo nulo no llega como null, llega ausente.

    Ambas filas congeladas —una en 'Borrador', otra 'Cancelado'— carecen por
    completo de ``fecha_de_firma``. Un consumidor que hiciera ``row[campo]``
    reventaría, y uno que asumiera ``null`` leería un dato que no existe. El
    perfil usa ``.get`` y trata la ausencia como ausencia, nunca como cero.
    """
    date_field = contract["fields"]["date"]["name"]
    assert contract["fields"]["date"]["omitted_when_null"] is True
    assert all(date_field not in row for row in contract["frozen_rows"])


def test_the_explicit_document_type_column_exists_and_is_the_best_evidence(contract):
    """El hallazgo de AP2-A: tipodocproveedor vive en el dataset que ya usamos.

    AP0 lo ubicaba solo en SECOP I (no integrado). Aquí no cuesta integración
    alguna, y es la evidencia declarada que AP1 prefiere sobre cualquier
    inferencia por dígito de verificación.
    """
    document_type = contract["fields"]["document_type"]
    assert document_type["name"] == "tipodocproveedor"
    assert {"NIT", "Cédula de Ciudadanía"} <= set(document_type["observed_values"])

    declared = normalize_identity("8600024001", document_type="CC")
    assert declared.confidence == "declared"
    assert declared.document_type == "CC"
