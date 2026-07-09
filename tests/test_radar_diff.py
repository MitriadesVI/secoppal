"""Tests de app/core/radar/diff.py — clasificación de novedades."""
from __future__ import annotations

from app.core.radar.diff import classify
from app.core.radar.keys import extract_fields, snapshot_hash
from app.core.soql_builder import SoQLBuilder

CONT = SoQLBuilder.CONTRATOS_DATASET


def _row(cid, estado="En ejecución", valor="1000", proveedor="ACME", objeto="Obra"):
    return {
        "id_contrato": cid,
        "objeto_del_contrato": objeto,
        "nombre_entidad": "ENTIDAD",
        "valor_del_contrato": valor,
        "estado_contrato": estado,
        "proveedor_adjudicado": proveedor,
    }


def _seen_from_row(row):
    f = extract_fields(row, CONT)
    return {
        "snapshot_hash": snapshot_hash(row, CONT),
        "status": f["status"],
        "value": f["value"],
        "contractor": f["contractor"],
        "title": f["title"],
        "entity": f["entity"],
        "urlproceso": f["urlproceso"],
        "referencia": f["referencia"],
    }


def test_all_new_on_empty_seen():
    results = [_row("C-1"), _row("C-2")]
    diff = classify(results, {}, CONT)
    assert diff.new_count == 2
    assert diff.changed_count == 0
    assert not diff.disappeared


def test_unchanged_when_same_snapshot():
    r = _row("C-1")
    seen = {"C-1": _seen_from_row(r)}
    diff = classify([r], seen, CONT)
    assert diff.new_count == 0
    assert len(diff.unchanged) == 1
    assert not diff.has_novelty


def test_changed_state():
    old = _row("C-1", estado="En ejecución")
    new = _row("C-1", estado="Terminado")
    seen = {"C-1": _seen_from_row(old)}
    diff = classify([new], seen, CONT)
    assert len(diff.changed_state) == 1
    assert not diff.changed_value
    assert diff.changed_state[0].prev_status == "En ejecución"


def test_changed_value_only():
    old = _row("C-1", valor="1000")
    new = _row("C-1", valor="9999")
    seen = {"C-1": _seen_from_row(old)}
    diff = classify([new], seen, CONT)
    assert len(diff.changed_value) == 1
    assert not diff.changed_state


def test_seen_missing_from_current_window_is_churn_not_novelty():
    old = _row("C-1")
    seen = {"C-1": _seen_from_row(old)}
    diff = classify([], seen, CONT)
    assert not diff.disappeared
    assert len(diff.window_churn) == 1
    assert diff.window_churn[0].key == "C-1"
    assert diff.has_novelty is False


def test_refreshed_out_of_window_changed_state_keeps_churn_marker():
    old = _row("C-1", estado="En ejecución")
    new = _row("C-1", estado="Terminado")
    seen = {"C-1": _seen_from_row(old)}
    diff = classify([new], seen, CONT, current_window_keys=set())
    assert len(diff.changed_state) == 1
    assert len(diff.window_churn) == 1
    assert not diff.disappeared
    assert diff.has_novelty is True


def test_disappeared_requires_explicit_confirmation():
    old = _row("C-1")
    seen = {"C-1": _seen_from_row(old)}
    diff = classify([], seen, CONT, confirmed_disappeared_keys={"C-1"})
    assert len(diff.disappeared) == 1
    assert diff.disappeared[0].key == "C-1"
    assert not diff.window_churn
    assert diff.has_novelty is True


def test_contractor_new_flagged():
    # ACME ya conocido; FOO es operador nuevo.
    seen = {"C-0": _seen_from_row(_row("C-0", proveedor="ACME"))}
    results = [_row("C-1", proveedor="FOO")]
    diff = classify(results, seen, CONT)
    assert diff.new[0].contractor_new is True


def test_contractor_not_new_when_known():
    seen = {"C-0": _seen_from_row(_row("C-0", proveedor="ACME"))}
    results = [_row("C-1", proveedor="ACME")]
    diff = classify(results, seen, CONT)
    assert diff.new[0].contractor_new is False


def test_rows_without_key_are_skipped():
    results = [{"objeto_del_contrato": "sin id"}]
    diff = classify(results, {}, CONT)
    assert diff.new_count == 0
