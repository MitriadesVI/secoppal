"""Tests de app/core/radar/keys.py — identidad y snapshot de resultados."""
from __future__ import annotations

from app.core.radar.keys import (
    extract_event_metadata,
    extract_fields,
    radar_result_key,
    snapshot_hash,
)
from app.core.soql_builder import SoQLBuilder

PROC = SoQLBuilder.PROCESOS_DATASET
CONT = SoQLBuilder.CONTRATOS_DATASET


def test_result_key_procesos_prefers_id():
    row = {"id_del_proceso": "P-1", "referencia_del_proceso": "REF-1"}
    assert radar_result_key(row, PROC) == "P-1"


def test_result_key_procesos_falls_back_to_referencia():
    row = {"referencia_del_proceso": "REF-9"}
    assert radar_result_key(row, PROC) == "REF-9"


def test_result_key_contratos():
    row = {"id_contrato": "C-7", "referencia_del_contrato": "RC-7"}
    assert radar_result_key(row, CONT) == "C-7"


def test_result_key_none_when_missing():
    assert radar_result_key({"foo": "bar"}, PROC) is None


def test_extract_fields_procesos():
    row = {
        "nombre_del_procedimiento": "Suministro PAE",
        "entidad": "ALCALDIA X",
        "precio_base": "1000000",
        "estado_de_apertura_del_proceso": "Abierto",
        "urlproceso": "http://x",
        "referencia_del_proceso": "REF-1",
    }
    f = extract_fields(row, PROC)
    assert f["title"] == "Suministro PAE"
    assert f["entity"] == "ALCALDIA X"
    assert f["value"] == "1000000"
    assert f["status"] == "Abierto"
    assert f["contractor"] == ""  # procesos no tiene contratista


def test_extract_fields_contratos_has_contractor():
    row = {
        "objeto_del_contrato": "Operación PAE",
        "nombre_entidad": "GOBERNACION",
        "valor_del_contrato": "5000000",
        "estado_contrato": "En ejecución",
        "proveedor_adjudicado": "FUNDACARIBE",
    }
    f = extract_fields(row, CONT)
    assert f["title"] == "Operación PAE"
    assert f["contractor"] == "FUNDACARIBE"


def test_snapshot_stable_for_same_status_and_value():
    row = {"estado_contrato": "En ejecución", "valor_del_contrato": "1000"}
    assert snapshot_hash(row, CONT) == snapshot_hash(dict(row), CONT)


def test_snapshot_value_normalized():
    # 1000 y 1000.00 deben producir el mismo hash.
    a = {"estado_contrato": "X", "valor_del_contrato": "1000"}
    b = {"estado_contrato": "X", "valor_del_contrato": "1000.00"}
    assert snapshot_hash(a, CONT) == snapshot_hash(b, CONT)


def test_snapshot_changes_on_status():
    a = {"estado_contrato": "En ejecución", "valor_del_contrato": "1000"}
    b = {"estado_contrato": "Terminado", "valor_del_contrato": "1000"}
    assert snapshot_hash(a, CONT) != snapshot_hash(b, CONT)


def test_snapshot_changes_on_value():
    a = {"estado_contrato": "X", "valor_del_contrato": "1000"}
    b = {"estado_contrato": "X", "valor_del_contrato": "2000"}
    assert snapshot_hash(a, CONT) != snapshot_hash(b, CONT)


# ── extract_event_metadata (R2B-1B1) ────────────────────────────────────────
# Proyección de los datos que describen un evento ya decidido. Separada de
# extract_fields a propósito: estos campos NO deciden si hubo cambio.


def test_event_metadata_procesos_projects_modality_and_closing():
    """Procesos aporta modalidad y cierre; firma/contratista no existen ahí."""
    row = {
        "modalidad_de_contratacion": "Licitación pública",
        "fecha_de_recepcion_de": "2026-08-20T17:00:00.000",
        # Ruido: la fecha de publicación NO debe colarse como cierre.
        "fecha_de_publicacion_del": "2026-07-01T08:00:00.000",
    }
    assert extract_event_metadata(row, PROC) == {
        "modality": "Licitación pública",
        "closing_date": "2026-08-20T17:00:00.000",
        "contractor": None,
        "signature_date": None,
    }


def test_event_metadata_contratos_projects_contractor_and_signature():
    """Contratos aporta modalidad, contratista y firma; no tiene cierre."""
    row = {
        "modalidad_de_contratacion": "Contratación directa",
        "proveedor_adjudicado": "UNION TEMPORAL PAE 2026",
        "fecha_de_firma": "2026-03-11T00:00:00.000",
    }
    assert extract_event_metadata(row, CONT) == {
        "modality": "Contratación directa",
        "closing_date": None,
        "contractor": "UNION TEMPORAL PAE 2026",
        "signature_date": "2026-03-11T00:00:00.000",
    }


def test_event_metadata_missing_and_whitespace_normalize_to_none():
    """Ausente, "" y "   " son el mismo hecho: no sabemos el dato."""
    row = {
        "modalidad_de_contratacion": "",
        "proveedor_adjudicado": "   ",
        "fecha_de_firma": None,
        # id_contrato presente pero fecha ausente del dict por completo.
    }
    assert extract_event_metadata(row, CONT) == {
        "modality": None, "closing_date": None,
        "contractor": None, "signature_date": None,
    }


def test_event_metadata_preserves_source_text_without_interpreting():
    """Se recortan bordes; el texto fuente no se traduce ni se reformatea."""
    row = {
        "modalidad_de_contratacion": "  Selección abreviada  ",
        "fecha_de_recepcion_de": " 2026-08-20T17:00:00.000 ",
    }
    meta = extract_event_metadata(row, PROC)
    assert meta["modality"] == "Selección abreviada"
    assert meta["closing_date"] == "2026-08-20T17:00:00.000"


def test_unknown_dataset_with_process_shaped_fields_returns_no_metadata():
    """Dataset desconocido: falla CERRADO aunque la fila TENGA forma de proceso.

    Este es el caso peligroso, y el único que prueba algo: una fila ajena con
    claves ajenas devuelve None sola, sin mérito del código. Si la proyección
    cayera al spec de procesos ante un dataset que no conoce, leería estas
    columnas y congelaría en el evento una modalidad y un cierre que nadie
    verificó que signifiquen eso en ese dataset.
    """
    row = {
        "modalidad_de_contratacion": "Licitación pública",
        "fecha_de_recepcion_de": "2026-08-20",
        "proveedor_adjudicado": "ACME",
        "fecha_de_firma": "2026-01-01",
    }
    assert extract_event_metadata(row, "dataset-desconocido") == {
        "modality": None, "closing_date": None,
        "contractor": None, "signature_date": None,
    }


def test_event_metadata_unknown_dataset_with_foreign_row_also_empty():
    """Complemento del anterior: una fila ajena tampoco filtra nada."""
    foreign = {"algo": "x", "fecha": "2026-01-01", "proveedor": "ACME"}
    assert extract_event_metadata(foreign, "zzzz-9999") == {
        "modality": None, "closing_date": None,
        "contractor": None, "signature_date": None,
    }


def test_extract_fields_keeps_its_inherited_procesos_fallback():
    """El fallo cerrado es de la proyección de eventos, NO de extract_fields:
    su fallback al spec de procesos es comportamiento heredado que este bloque
    no toca. Fijarlo aquí evita 'arreglarlo' por simetría y mover el diff."""
    row = {"nombre_del_procedimiento": "PAE", "precio_base": "1000"}
    assert extract_fields(row, "dataset-desconocido")["title"] == "PAE"


def test_event_metadata_is_not_part_of_the_diff_contract():
    """extract_fields no crece con los campos de presentación: si creciera,
    entrarían al hash/diff y un cambio de modalidad fingiría un cambio real."""
    row = {
        "modalidad_de_contratacion": "Licitación pública",
        "fecha_de_recepcion_de": "2026-08-20",
    }
    assert set(extract_fields(row, PROC)) == {
        "title", "entity", "value", "status", "urlproceso", "referencia",
        "contractor",
    }
    assert "modality" not in extract_fields(row, PROC)
