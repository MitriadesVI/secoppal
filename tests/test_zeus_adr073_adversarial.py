"""ZEUS — pruebas adversariales independientes sobre la auditoría ADR-073.

No se tocan expectativas productivas: cada caso intenta MATAR una frontera de
autoridad del auditor. Un fallo aquí es un hallazgo, no un test que ajustar.
"""
from __future__ import annotations

import argparse
import json

import pytest

from scripts.audits import audit_gazetteer_source_bindings_v1 as audit
from tests.test_gazetteer_source_binding_audit import (  # harness del candidato
    FakeSource, P6DX, JBBY, binding, canonical, observe,
)


def _args(tmp_path, **kw):
    base = dict(workers=1, limit=0, only=None, start_after=None, recheck_existing=False,
                summary_only=False, transport="soda3", timeout=1.0, retries=0, resume=False)
    base.update(kw)
    return argparse.Namespace(output=str(tmp_path), **base)


# 1 ── mismo NIT + unidades hija nunca es candidato
def test_z01_same_nit_with_child_units_is_not_a_candidate():
    record = canonical("MUNICIPIO DE X", nit="900")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO DE X", "900", 10), ("MUNICIPIO DE X SECRETARIA DE SALUD", "900", 5)],
              JBBY: [("MUNICIPIO DE X", "900", 10)]},
        counts={P6DX: (15, 15), JBBY: (10, 10)})
    _, _, outcome, candidate = observe(source, record)
    assert outcome["status"] in ("BUYER_GROUP_CANDIDATE", "MULTI_NAME_REVIEW")
    assert candidate is None


# 2 ── un NIT observado sin acreditación nunca se convierte en identidad
def test_z02_single_observed_nit_never_becomes_identity():
    record = canonical("ENTIDAD SIN LLAVE")  # identity None
    source = FakeSource(
        like={P6DX: [("ENTIDAD SIN LLAVE", "800123", 50)],
              JBBY: [("ENTIDAD SIN LLAVE", "800123", 50)]},
        counts={P6DX: (50, 50), JBBY: (50, 50)})
    measurement, _, outcome, candidate = observe(source, record, phase_b=audit.needs_equivalence(record))
    assert outcome["status"] == "NO_STABLE_IDENTITY"
    assert candidate is None
    assert record.identity is None, "el auditor promovió un NIT observado a identidad"


# 3 ── identidad ajena en el LIKE bloquea el candidato
def test_z03_foreign_identity_blocks_candidate():
    record = canonical("ENTIDAD AJENA", nit="111")
    source = FakeSource(
        like={P6DX: [("ENTIDAD AJENA", "111", 10), ("ENTIDAD AJENA DEL SUR", "222", 3)],
              JBBY: [("ENTIDAD AJENA", "111", 10)]},
        counts={P6DX: (13, 13), JBBY: (10, 10)})
    _, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "FOREIGN_IDENTITY_COLLISION"
    assert candidate is None


# 4 ── timeout != equivalencia
@pytest.mark.parametrize("failure", ["TIMEOUT", "ERROR"])
def test_z04_timeout_is_never_equivalence(failure):
    record = canonical("ENTIDAD LENTA", nit="333")
    source = FakeSource(
        like={P6DX: [("ENTIDAD LENTA", "333", 10)], JBBY: [("ENTIDAD LENTA", "333", 10)]},
        counts={P6DX: (10, 10), JBBY: (10, 10)},
        failures={f"count:{JBBY}": failure})
    measurement, _, outcome, candidate = observe(source, record)
    assert measurement["datasets"][JBBY]["equivalence"]["same_universe"] is None
    assert outcome["status"] == "INCONCLUSIVE"
    assert candidate is None


# 5 ── 0 filas != mismo universo
def test_z05_zero_rows_is_not_same_universe():
    record = canonical("ENTIDAD NO PUBLICADA", nit="444")
    source = FakeSource(like={P6DX: [], JBBY: []}, counts={P6DX: (0, 0), JBBY: (0, 0)})
    measurement, _, outcome, candidate = observe(source, record)
    for ds in (P6DX, JBBY):
        assert measurement["datasets"][ds]["equivalence"]["same_universe"] is None
    assert outcome["status"] == "INCONCLUSIVE"
    assert candidate is None


# 6 ── un ecosistema padre nunca produce candidato, POR SU ROL (no por su `kind`)
@pytest.mark.parametrize("role", ["national_parent", "regional_ecosystem", "macro_region"])
def test_z06_ecosystem_role_never_produces_candidate_even_as_buyer_unit(role):
    """El gate que separa unidad de composición es `role`, no `kind`.

    Si sólo `kind == "ecosystem"` protege, el día que una composición entre al
    inventario como `buyer_unit` con identidad acreditada se emitirá un binding
    padre -> todos los nombres físicos.
    """
    record = canonical("SENA", kind="buyer_unit")
    record.identity = audit.Identity("BUYER_UNIT", "SENA", "sena_units.json")
    record.ecosystem = {"family": "SENA", "role": role}
    measurement = {"datasets": {P6DX: {"equivalence": {"candidate_names": ["SENA"],
                                                       "same_universe": True},
                                       "like_universe": [{"name": "SENA", "nit": "899999034", "rows": 5}]}},
                   "errors": []}
    assert audit.build_candidate(record, measurement, "EXACT_BINDABLE") is None


# 7 ── una unidad concreta declarada SÍ puede ser candidato
def test_z07_declared_unit_can_be_candidate():
    record = canonical("SENA SECRETARIA GENERAL")
    record.identity = audit.Identity("BUYER_UNIT", "SENA SECRETARIA GENERAL", "sena_units.json")
    record.ecosystem = {"family": "SENA", "role": "declared_unit"}
    source = FakeSource(
        like={P6DX: [("SENA SECRETARIA GENERAL", "899999034", 11)],
              JBBY: [("SENA SECRETARIA GENERAL", "899999034", 10)]},
        counts={P6DX: (11, 11), JBBY: (10, 10)})
    _, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "EXACT_BINDABLE" and candidate is not None


# 8 ── dos estados finales contradictorios para el mismo canónico deben detectarse
def test_z08_contradictory_duplicate_progress_record_is_detected(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    progress = tmp_path / "progress.jsonl"
    record = json.loads(progress.read_text().splitlines()[0])
    poisoned = dict(record, status="EXACT_BINDABLE",
                    candidate={"canonical_name": "A", "sources": {P6DX: ["A"]}})
    with progress.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(poisoned, ensure_ascii=False) + "\n")
    audit.run(_args(tmp_path, summary_only=True), source_factory=lambda settings, args: source)
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary.get("contradictory_records") or summary["total"] != 1, (
        "un segundo registro contradictorio para el mismo canónico sustituye al "
        "primero sin dejar rastro en el resumen"
    )


# 9 ── un --only no puede reducir el lote
def test_z09_only_cannot_shrink_the_batch(tmp_path, monkeypatch):
    inventory = [canonical("A"), canonical("B"), canonical("C")]
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: list(inventory))
    audit.run(_args(tmp_path), source_factory=lambda s, a: FakeSource(like={}))
    assert json.loads((tmp_path / "summary.json").read_text())["total"] == 3
    audit.run(_args(tmp_path, only=["A"]), source_factory=lambda s, a: FakeSource(like={}))
    assert json.loads((tmp_path / "summary.json").read_text())["total"] == 3, (
        "un reproceso --only reescribió el resumen con su propia selección")


# 9b ── un --only que SÍ mide no puede borrar la procedencia del lote
def test_z09b_only_remeasure_keeps_batch_provenance(tmp_path, monkeypatch):
    inventory = [canonical("A"), canonical("B")]
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: list(inventory))
    audit.run(_args(tmp_path), source_factory=lambda s, a: FakeSource(like={}))
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    manifest["batch_totals"] = {"records": 2, "run_log": "prueba"}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    # se reprocesa un canónico concreto (pendiente => mide)
    audit.run(_args(tmp_path, only=["A"], recheck_existing=True),
              source_factory=lambda s, a: FakeSource(like={}))
    after = json.loads((tmp_path / "manifest.json").read_text())
    assert "batch_totals" in after, "un --only que mide borró la procedencia del lote"


# 10 ── drift en un binding vigente produce revisión, no VERIFIED
def test_z10_binding_drift_is_review():
    record = canonical("MUNICIPIO BINDEADO", nit="555",
                       bound=binding({P6DX: ["NOMBRE VIEJO"], JBBY: ["NOMBRE VIEJO"]},
                                     identity_key="NIT:555"))
    source = FakeSource(bound={P6DX: [], JBBY: []})
    _, verdict, outcome, candidate = observe(source, record, phase_b=False)
    assert verdict["status"] == "DRIFT"
    assert outcome["status"] == "BINDING_DRIFT_REVIEW"
    assert candidate is None


# 11 ── dos miembros de un grupo no pueden reclamar el mismo nombre físico
def test_z11_group_with_duplicated_physical_name_is_flagged():
    from app.core.buyer_groups import BuyerGroup
    group = BuyerGroup(group_id="g", version="1", label="g",
                       members=("M1", "M2"), aliases=())
    records = []
    for name in ("M1", "M2"):
        rec = canonical(name, nit="700",
                        bound=binding({P6DX: ["NOMBRE COMPARTIDO"], JBBY: ["NOMBRE COMPARTIDO"]}))
        records.append({"canonical_name": name, "status": "ALREADY_BOUND_VERIFIED",
                        "already_bound": True, "binding": rec.binding, "candidate": None,
                        "datasets": {}, "kind": "buyer_unit", "identity": None})
    report = audit.group_readiness(records, {"g": group})["g"]
    assert report["status"] != "READY_TODAY" or report.get("duplicated_source_names"), (
        "dos miembros distintos reclaman el mismo nombre físico y el grupo se "
        "declara READY_TODAY sin señalarlo")


# 12 ── un grupo que excedería el tope tras revisión no puede quedar listo
def test_z12_group_exceeding_max_names_after_review_is_not_ready():
    from app.core.buyer_groups import BuyerGroup
    members = tuple(f"M{i}" for i in range(audit.MAX_GROUP_SOURCE_NAMES + 1))
    group = BuyerGroup(group_id="g", version="1", label="g",
                       members=members, aliases=())
    records = []
    for name in members:
        records.append({"canonical_name": name, "status": "EXACT_BINDABLE",
                        "already_bound": False, "binding": None,
                        "candidate": {"sources": {P6DX: [name], JBBY: [name]}},
                        "datasets": {}, "kind": "buyer_unit", "identity": None})
    report = audit.group_readiness(records, {"g": group})["g"]
    assert report["would_exceed_after_review"] is True
    assert report["status"] != "READY_AFTER_REVIEW", (
        "el grupo excede MAX_GROUP_SOURCE_NAMES tras revisión y aún así se "
        "declara listo para revisión")


# 13 ── un registro de evidencia corrupto debe detectarse
def test_z13_corrupted_evidence_record_is_detected(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    audit.run(_args(tmp_path), source_factory=lambda s, a: FakeSource(like={}))
    record_files = list((tmp_path / "records").glob("*.json"))
    assert record_files
    record_files[0].write_text("{corrupto", encoding="utf-8")
    audit.run(_args(tmp_path, summary_only=True), source_factory=lambda s, a: FakeSource(like={}))
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary.get("evidence_integrity"), (
        "la reconstrucción no verifica la evidencia por registro: un "
        "records/<hash>.json corrupto no cambia nada")


# 14 ── una clase bindable sin payload de candidato es incoherente
def test_z14_bindable_status_without_candidate_is_incoherent():
    record = canonical("ENTIDAD COHERENTE", nit="888")
    source = FakeSource(
        like={P6DX: [("ENTIDAD COHERENTE", "888", 4)], JBBY: [("ENTIDAD COHERENTE", "888", 4)]},
        counts={P6DX: (4, 4), JBBY: (4, 4)})
    _, _, outcome, candidate = observe(source, record)
    if outcome["status"] in ("EXACT_BINDABLE", "SOURCE_NAME_SPLIT"):
        assert candidate is not None


# 15 ── un payload de candidato con clase no permitida se rechaza
@pytest.mark.parametrize("status", ["NO_STABLE_IDENTITY", "AMBIGUOUS_IDENTITY",
                                    "INCONCLUSIVE", "BUYER_GROUP_CANDIDATE",
                                    "MULTI_NAME_REVIEW", "NOT_EQUIVALENT",
                                    "FOREIGN_IDENTITY_COLLISION", "ERROR"])
def test_z15_candidate_never_built_for_disallowed_status(status):
    record = canonical("ENTIDAD", nit="999")
    measurement = {"datasets": {P6DX: {"equivalence": {"candidate_names": ["ENTIDAD"],
                                                       "same_universe": True},
                                       "like_universe": [{"name": "ENTIDAD", "nit": "999", "rows": 1}]}},
                   "errors": []}
    assert audit.build_candidate(record, measurement, status) is None
