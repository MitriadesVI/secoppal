"""Tests de app/core/radar/keys.py — identidad y snapshot de resultados."""
from __future__ import annotations

from app.core.radar.keys import (
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
