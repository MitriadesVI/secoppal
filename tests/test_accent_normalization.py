from app.core.soql_builder import SoQLBuilder


def test_soql_normalizes_accents_in_object_field():
    builder = SoQLBuilder()
    soql = builder.build(
        SoQLBuilder.PROCESOS_DATASET,
        {"objeto": ["consultoria"], "departamento_resolved": "Bolívar"},
    )

    assert "unaccent" in soql
    assert "nombre_del_procedimiento" in soql
    assert "descripci_n_del_procedimiento" in soql
    assert "consultoria" in soql


def test_soql_normalizes_n_with_tilde_for_object_search():
    builder = SoQLBuilder()
    soql = builder.build(SoQLBuilder.PROCESOS_DATASET, {"objeto": ["diseno"]})

    assert "unaccent" in soql
    assert "diseno" in soql


def test_contract_object_fields_are_accent_normalized_too():
    builder = SoQLBuilder()
    soql = builder.build(SoQLBuilder.CONTRATOS_DATASET, {"objeto": ["jardineria"]})

    assert "unaccent" in soql
    assert "objeto_del_contrato" in soql
    assert "descripcion_del_proceso" in soql
