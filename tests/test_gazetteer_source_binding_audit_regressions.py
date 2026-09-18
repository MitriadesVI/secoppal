"""ADR-073 gazetteer-wide — regresiones de la REMEDIACIÓN (revisión independiente).

Cada caso aquí es un hallazgo del review independiente convertido en contrato
permanente. Las expectativas no se rebajan: si un caso vuelve a rojo, el defecto
volvió.

- P1-1 / P2-3: la evidencia raw que sostiene los candidatos existe en el commit y
  su hash se verifica ANTES de reconstruir el resumen.
- P1-2: una repetición en `progress.jsonl` sin declaración explícita falla el
  reporte; nunca se elige por orden de línea.
- P1-3: la procedencia del lote no se reescribe por un micro-reproceso ni por una
  reconstrucción de reportes.
- P2-1/P2-6: lo que se midió se declara sin inflarlo; el cero es cobertura, no
  ausencia de dato.
- P2-4/P2-5: un grupo que no cabe o que pisa nombres físicos está BLOCKED.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from app.core.buyer_groups import BuyerGroup
from scripts.audits import audit_gazetteer_source_bindings_v1 as audit
from tests.test_gazetteer_source_binding_audit import (  # harness del auditor
    FakeSource, P6DX, JBBY, _args, binding, canonical,
)

ROOT = Path(__file__).resolve().parents[1]
ARTIFACT = ROOT / "artifacts/audits/gazetteer_source_bindings_v1"


def _group(members) -> BuyerGroup:
    return BuyerGroup(group_id="g", version="1", label="g", aliases=(), members=tuple(members))


def _record(name, status, *, bound=False, candidate=None) -> dict:
    return {
        "canonical_name": name, "status": status, "already_bound": bound,
        "candidate": candidate,
        "binding": binding({P6DX: [f"{name} PH"], JBBY: [f"{name} PH"]}) if bound else None,
        "datasets": {},
    }


# ── P2-4 / P2-5: precedencia del grupo ──────────────────────────────────────


def test_group_over_max_after_review_blocks_even_with_every_member_ready():
    """`would_exceed_after_review` no puede perder contra `READY_AFTER_REVIEW`."""
    members = tuple(f"M{i}" for i in range(audit.MAX_GROUP_SOURCE_NAMES + 1))
    records = [
        {"canonical_name": name, "status": "EXACT_BINDABLE", "already_bound": False,
         "binding": None, "candidate": {"sources": {P6DX: [name], JBBY: [name]}},
         "datasets": {}, "kind": "buyer_unit", "identity": None}
        for name in members
    ]
    report = audit.group_readiness(records, {"g": _group(members)})["g"]
    assert report["members_candidate_ready"] == audit.MAX_GROUP_SOURCE_NAMES + 1
    assert report["would_exceed_after_review"] is True
    assert report["status"] == "BLOCKED"


def test_group_with_exactly_max_names_after_review_is_allowed():
    """El tope es `> 40`, la misma comparación que `buyer_groups.names_for`."""
    members = tuple(f"M{i}" for i in range(audit.MAX_GROUP_SOURCE_NAMES))
    records = [
        {"canonical_name": name, "status": "EXACT_BINDABLE", "already_bound": False,
         "binding": None, "candidate": {"sources": {P6DX: [name], JBBY: [name]}},
         "datasets": {}, "kind": "buyer_unit", "identity": None}
        for name in members
    ]
    report = audit.group_readiness(records, {"g": _group(members)})["g"]
    assert report["would_exceed_after_review"] is False
    assert report["status"] == "READY_AFTER_REVIEW"


def test_group_with_duplicated_physical_name_is_blocked_today_and_after_review():
    """Dos miembros distintos no pueden reclamar el mismo nombre físico.

    `buyer_groups.names_for` lo rechaza en producción; contarlo sin mirarlo lo
    escondía.
    """
    shared = "NOMBRE COMPARTIDO"
    today = [
        {"canonical_name": name, "status": "ALREADY_BOUND_VERIFIED", "already_bound": True,
         "binding": binding({P6DX: [shared], JBBY: [shared]}), "candidate": None,
         "datasets": {}, "kind": "buyer_unit", "identity": None}
        for name in ("M1", "M2")
    ]
    report = audit.group_readiness(today, {"g": _group(["M1", "M2"])})["g"]
    assert report["status"] == "BLOCKED"
    detail = report["duplicated_source_names"]["today"]
    assert detail[P6DX] == [{"name": shared, "members": ["M1", "M2"]}]
    assert detail[JBBY] == [{"name": shared, "members": ["M1", "M2"]}]

    after = [
        {"canonical_name": name, "status": "EXACT_BINDABLE", "already_bound": False,
         "binding": None, "candidate": {"sources": {P6DX: [shared], JBBY: [shared]}},
         "datasets": {}, "kind": "buyer_unit", "identity": None}
        for name in ("M1", "M2")
    ]
    report_after = audit.group_readiness(after, {"g": _group(["M1", "M2"])})["g"]
    assert report_after["status"] == "BLOCKED"
    assert report_after["duplicated_source_names"]["after_review"][P6DX] == [
        {"name": shared, "members": ["M1", "M2"]}]


# ── P2-4 (gate por rol) y P2-6 (cero visible) ───────────────────────────────


@pytest.mark.parametrize("kind", ["buyer_unit", "ecosystem"])
@pytest.mark.parametrize("role", ["national_parent", "regional_ecosystem", "macro_region",
                                  "unknown_role"])
def test_ecosystem_role_gate_blocks_composition_whatever_the_kind(kind, role):
    """El gate que separa unidad de composición es `role`, no `kind`."""
    record = canonical("SENA", kind=kind)
    record.identity = audit.Identity("BUYER_UNIT", "SENA", "sena_units.json")
    record.ecosystem = {"family": "SENA", "role": role}
    measurement = {
        "datasets": {P6DX: {"equivalence": {"candidate_names": ["SENA"], "same_universe": True},
                            "like_universe": [{"name": "SENA", "nit": "899999034", "rows": 5}]}},
        "errors": [],
    }
    assert audit.build_candidate(record, measurement, "EXACT_BINDABLE") is None


def test_declared_unit_keeps_its_candidate():
    record = canonical("SENA SECRETARIA GENERAL")
    record.identity = audit.Identity("BUYER_UNIT", "SENA SECRETARIA GENERAL", "sena_units.json")
    record.ecosystem = {"family": "SENA", "role": "declared_unit"}
    measurement = {
        "datasets": {P6DX: {"equivalence": {"candidate_names": ["SENA SECRETARIA GENERAL"],
                                            "same_universe": True},
                            "like_universe": [{"name": "SENA SECRETARIA GENERAL",
                                               "nit": "899999034", "rows": 11}]}},
        "errors": [],
    }
    candidate = audit.build_candidate(record, measurement, "EXACT_BINDABLE")
    assert candidate is not None and candidate["sources"][P6DX] == ["SENA SECRETARIA GENERAL"]


def test_zero_rows_dataset_is_reported_as_unbalanced_coverage(tmp_path, monkeypatch):
    """`1000 / 0` es cobertura desbalanceada; con `if value` el caso desaparecía."""
    monkeypatch.setattr(audit, "build_inventory",
                        lambda authorities: [canonical("UNO SOLO DE DOS", nit="123")])
    source = FakeSource(
        like={P6DX: [("UNO SOLO DE DOS", "123", 1000)], JBBY: []},
        counts={P6DX: (1000, 1000), JBBY: (0, 0)},
    )
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    coverage = summary["top_lists"]["unbalanced_dataset_coverage"]
    assert coverage["count"] == 1
    entry = coverage["top"][0]
    assert entry["canonical_name"] == "UNO SOLO DE DOS"
    assert entry["rows_by_dataset"] == {P6DX: 1000, JBBY: 0}
    # Umbral de REPORTE: el cero se ve, pero no cambia la clasificación ni habilita
    # candidato (el auditor sólo lo publica).
    assert summary["classes"][entry["status"]] == 1
    assert summary["bindings"]["candidate_ready"] == 0


def test_ecosystem_unit_counts_exclude_composition_entries(tmp_path, monkeypatch):
    units = [canonical(name) for name in ("UNIDAD A", "UNIDAD B")]
    for record in units:
        record.identity = audit.Identity("BUYER_UNIT", record.canonical_name, "icbf_regionals.json")
        record.ecosystem = {"family": "ICBF", "role": "declared_unit"}
    parent = canonical("ECOSYSTEM:ICBF:national", kind="ecosystem")
    parent.ecosystem = {"family": "ICBF", "role": "national_parent"}
    parent.ecosystem_projection = {"mode": "official_names",
                                   "official_names": [record.canonical_name for record in units]}
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [*units, parent])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    # El padre NO es una unidad: contarlo daba 3 donde el catálogo declara 2.
    assert summary["ecosystems"]["icbf_units"] == 2
    assert summary["ecosystems"]["institutional"] == 1


# ── P2-1: lo medido, sin inflarlo ───────────────────────────────────────────


def test_binding_taxonomy_separates_liveness_from_foreign_identity_check(tmp_path, monkeypatch):
    nit_bound = canonical("CON NIT ACREDITADO", nit="899999083",
                          bound=binding({P6DX: ["CON NIT ACREDITADO"], JBBY: ["CON NIT ACREDITADO"]},
                                        "NIT:899999083"))
    unit_bound = canonical("CON IDENTIDAD DE UNIDAD",
                           bound=binding({P6DX: ["CON IDENTIDAD DE UNIDAD"],
                                          JBBY: ["CON IDENTIDAD DE UNIDAD"]}, "BUYER_UNIT:X"))
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [nit_bound, unit_bound])
    # La fuente falsa no filtra por nombre: declara el MISMO NIT en el universo de
    # los dos, para no inventar una identidad ajena que el auditor tendría —con
    # razón— que marcar como drift.
    source = FakeSource(bound={P6DX: [("CON NIT ACREDITADO", "899999083", 5),
                                      ("CON IDENTIDAD DE UNIDAD", "899999083", 5)],
                               JBBY: [("CON NIT ACREDITADO", "899999083", 5),
                                      ("CON IDENTIDAD DE UNIDAD", "899999083", 5)]})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    bindings = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))["bindings"]
    assert bindings["existing_bindings_total"] == 2
    assert bindings["source_name_liveness_verified"] == 2
    assert bindings["foreign_identity_check"]["NIT_authoritative_checked"] == 1
    assert bindings["foreign_identity_check"]["BUYER_UNIT_structural_only"] == 1


# ── P1-2: supersession explícita o ERROR ────────────────────────────────────


def test_recheck_self_declares_what_it_replaces(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    source = FakeSource(like={})
    assert audit.run(_args(tmp_path), source_factory=lambda settings, args: source) == 0
    first = json.loads((tmp_path / "progress.jsonl").read_text().splitlines()[0])

    assert audit.run(_args(tmp_path, only=["A"], recheck_existing=True),
                     source_factory=lambda settings, args: source) == 0
    lines = [json.loads(line) for line in (tmp_path / "progress.jsonl").read_text().splitlines()]
    assert len(lines) == 2
    second = lines[1]
    assert (second["record_id"], second["attempt"]) == (f"{first['canonical_hash']}:2", 2)
    assert second["supersedes_record_id"] == first["canonical_hash"] + ":1"
    assert second["supersession_reason"] == "recheck_existing"

    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["progress"]["progress_lines"] == 2
    assert summary["progress"]["canonical_unique"] == 1
    assert summary["progress"]["superseded_records"] == 1
    assert summary["progress"]["active_records"] == 1
    assert summary["total"] == 1
    assert summary["progress"]["supersessions"][0]["declared_by"] == "record"
    assert summary["report_status"]["ok"] is True


def test_undeclared_duplicate_progress_fails_the_report(tmp_path, monkeypatch):
    """Dos mediciones del mismo canónico sin declaración: el reporte NO elige."""
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    progress = tmp_path / "progress.jsonl"
    original = json.loads(progress.read_text().splitlines()[0])
    poisoned = dict(original, status="EXACT_BINDABLE",
                    candidate={"canonical_name": "A", "sources": {P6DX: ["A"]}})
    with progress.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(poisoned, ensure_ascii=False) + "\n")

    assert audit.run(_args(tmp_path, summary_only=True),
                     source_factory=lambda settings, args: source) == 2
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["report_status"]["ok"] is False
    assert summary["contradictory_records"][0]["divergent_fields"] == ["candidate", "status"]
    assert summary["undeclared_duplicates"][0]["line"] == 1
    # El canónico queda SIN resultado final: no se queda con el último.
    assert summary["progress"]["active_records"] == 0
    assert summary["total"] == 0
    assert summary["classes"]["EXACT_BINDABLE"] == 0


def test_ledger_declaration_that_does_not_match_the_bitacora_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    audit.run(_args(tmp_path, only=["A"], recheck_existing=True),
              source_factory=lambda settings, args: source)
    first = json.loads((tmp_path / "progress.jsonl").read_text().splitlines()[0])
    successor = first["canonical_hash"] + ":2"
    (tmp_path / audit.SUPERSESSIONS_FILE).write_text(json.dumps({
        "superseded_record_id": "0000000000000000:9", "successor_record_id": successor,
        "reason": "recheck_existing"}) + "\n", encoding="utf-8")

    assert audit.run(_args(tmp_path, summary_only=True),
                     source_factory=lambda settings, args: source) == 2
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["report_status"]["ok"] is False
    assert any("no coinciden" in problem for problem in summary["report_status"]["problems"])


# ── P1-3: la procedencia del lote no se reescribe ───────────────────────────


def test_summary_only_preserves_batch_provenance_and_changes_only_the_invocation(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    manifest_path = tmp_path / "manifest.json"
    first = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert first["batch"]["records"] == 1
    first["batch"]["injected_by_operator"] = "no debe desaparecer"
    manifest_path.write_text(json.dumps(first), encoding="utf-8")

    for _ in range(2):
        audit.run(_args(tmp_path, summary_only=True), source_factory=lambda settings, args: source)
    after = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert after["batch"] == first["batch"], "un rebuild reescribió la procedencia del lote"
    assert after["last_invocation"]["mode"] == "rebuild"
    assert after["last_invocation"]["processed"] == 0
    assert after["last_invocation"]["pending"] == 0
    # Y no midió nada: la bitácora sigue con un solo asiento.
    assert len((tmp_path / audit.PROGRESS_FILE).read_text().splitlines()) == 1


def test_only_recheck_preserves_batch_provenance(tmp_path, monkeypatch):
    inventory = [canonical("A"), canonical("B")]
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: list(inventory))
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    manifest_path = tmp_path / "manifest.json"
    batch_before = json.loads(manifest_path.read_text(encoding="utf-8"))["batch"]

    audit.run(_args(tmp_path, only=["A"], recheck_existing=True),
              source_factory=lambda settings, args: source)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["batch"] == batch_before
    assert manifest["last_invocation"]["mode"] == "recheck"
    assert manifest["last_invocation"]["processed"] == 1
    assert manifest["last_invocation"]["selected"] == 1


def test_manifest_keeps_legacy_batch_keys_it_did_not_write(tmp_path, monkeypatch):
    """Claves de lote escritas por el manifiesto viejo no se borran al reprocesar."""
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    manifest_path = tmp_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["batch_totals"] = {"records": 1, "run_log": "/tmp/prueba.log"}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    audit.run(_args(tmp_path, only=["A"], recheck_existing=True),
              source_factory=lambda settings, args: source)
    after = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert after["batch_totals"] == {"records": 1, "run_log": "/tmp/prueba.log"}
    assert "batch_totals" in after["last_invocation"]["carried_batch_keys"]


def test_summary_reports_the_whole_batch_not_the_selected_subset(tmp_path, monkeypatch):
    inventory = [canonical(name) for name in ("A", "B", "C")]
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: list(inventory))
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    inventory.append(canonical("D"))

    audit.run(_args(tmp_path, only=["D"]), source_factory=lambda settings, args: source)
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["total"] == 4
    for status, filename in audit.CLASS_FILES.items():
        rows = [line for line in (tmp_path / filename).read_text().splitlines() if line.strip()]
        assert len(rows) == summary["classes"][status], filename
    assert sum(summary["classes"].values()) == 4
    assert (tmp_path / audit.RECORDS_HASH_FILE).read_text().count("\n") == 4


# ── P2-3: integridad de la evidencia raw ────────────────────────────────────


def test_corrupt_raw_record_fails_the_rebuild_and_is_not_laundered(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    hash_file = tmp_path / audit.RECORDS_HASH_FILE
    hashes_before = hash_file.read_text(encoding="utf-8")
    record_file = next((tmp_path / audit.RECORDS_SUBDIR).glob("*.json"))
    payload = json.loads(record_file.read_text(encoding="utf-8"))
    payload["measurement"]["datasets"] = {}   # JSON válido, evidencia ALTERADA
    record_file.write_text(json.dumps(payload), encoding="utf-8")

    assert audit.run(_args(tmp_path, summary_only=True),
                     source_factory=lambda settings, args: source) == 2
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    integrity = summary["evidence_integrity"]
    assert integrity["ok"] is False
    assert integrity["mismatched"][0]["path"] == f"{audit.RECORDS_SUBDIR}/{record_file.name}"
    assert summary["report_status"]["ok"] is False
    # Regenerar el archivo de hashes sobre evidencia alterada lavaría la alteración.
    assert hash_file.read_text(encoding="utf-8") == hashes_before


def test_unreadable_raw_record_is_declared_not_ignored(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    record_file = next((tmp_path / audit.RECORDS_SUBDIR).glob("*.json"))
    record_file.write_text("{corrupto", encoding="utf-8")

    assert audit.run(_args(tmp_path, summary_only=True),
                     source_factory=lambda settings, args: source) == 2
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["evidence_integrity"]["unparsable"][0]["path"] == \
        f"{audit.RECORDS_SUBDIR}/{record_file.name}"


def test_missing_raw_record_is_declared(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    record_file = next((tmp_path / audit.RECORDS_SUBDIR).glob("*.json"))
    record_file.unlink()

    assert audit.run(_args(tmp_path, summary_only=True),
                     source_factory=lambda settings, args: source) == 2
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["evidence_integrity"]["missing"] == [f"{audit.RECORDS_SUBDIR}/{record_file.name}"]


# ── P1-1 / P2-3 sobre el artefacto que se entrega ───────────────────────────

pytestmark_artifact = pytest.mark.skipif(not ARTIFACT.exists(), reason="artefacto no disponible")


@pytestmark_artifact
def test_shipped_artifact_keeps_every_attempt_and_declares_the_supersessions():
    entries = audit.build_progress_index(ARTIFACT / audit.PROGRESS_FILE)
    declared, problems = audit.read_supersessions(ARTIFACT / audit.SUPERSESSIONS_FILE)
    reconciliation = audit.reconcile_progress(entries, declared)
    reconciliation.problems.extend(problems)
    assert reconciliation.progress_lines == 1137
    assert reconciliation.canonical_unique == 1134
    assert len(reconciliation.supersessions) == 3
    assert reconciliation.active_records == 1134
    assert reconciliation.undeclared == [] and reconciliation.contradictory == []
    assert reconciliation.problems == []


@pytestmark_artifact
def test_shipped_artifact_summary_agrees_with_its_own_evidence():
    summary = json.loads((ARTIFACT / "summary.json").read_text(encoding="utf-8"))
    assert summary["progress"]["progress_lines"] == 1137
    assert summary["progress"]["superseded_records"] == 3
    assert summary["progress"]["active_records"] == 1134
    assert summary["telemetry"]["all_attempts"]["source_calls"] == 2312
    assert summary["telemetry"]["active_records"]["source_calls"] == 2298
    assert summary["telemetry"]["superseded_attempts"]["source_calls"] == 14
    assert summary["report_status"]["ok"] is True
    assert summary["bindings"]["existing_bindings_total"] == 593
    assert summary["bindings"]["foreign_identity_check"] == {
        "NIT_authoritative_checked": 18, "BUYER_UNIT_structural_only": 575,
        "note": summary["bindings"]["foreign_identity_check"]["note"]}


@pytestmark_artifact
def test_shipped_artifact_records_hash_file_covers_every_active_record_and_verifies():
    entries = audit.build_progress_index(ARTIFACT / audit.PROGRESS_FILE)
    declared, problems = audit.read_supersessions(ARTIFACT / audit.SUPERSESSIONS_FILE)
    reconciliation = audit.reconcile_progress(entries, declared)
    assert problems == []
    expected = sorted(audit.records_relative_path(name) for name in reconciliation.active)
    integrity = audit.verify_evidence(ARTIFACT, expected)
    assert integrity["declared_entries"] == 1134
    assert integrity["expected"] == 1134
    assert integrity["checked"] == 1134
    assert integrity["ok"] is True, integrity["problems"]


@pytestmark_artifact
def test_shipped_artifact_candidate_evidence_exists_in_the_commit():
    candidates = [json.loads(line) for line in
                  (ARTIFACT / "bindings_ready.jsonl").read_text(encoding="utf-8").splitlines()
                  if line.strip()]
    assert len(candidates) == 2
    for candidate in candidates:
        assert candidate["promotion_status"] == "CANDIDATE_NOT_PROMOTED"
        evidence = ARTIFACT / candidate["evidence_record"]
        assert evidence.exists(), candidate["evidence_record"]
        digest = hashlib.sha256(evidence.read_bytes()).hexdigest()
        declared = dict(line.split("  ")[::-1] for line in
                        (ARTIFACT / audit.RECORDS_HASH_FILE).read_text(encoding="utf-8").splitlines()
                        if line.strip())
        assert declared[candidate["evidence_record"]] == digest


@pytestmark_artifact
def test_shipped_artifact_raw_records_are_git_tracked():
    """Un clon de la rama tiene que conservar la evidencia que sostiene los candidatos."""
    if shutil.which("git") is None:  # pragma: no cover - entorno sin git
        pytest.skip("git no disponible")
    inside = subprocess.run(["git", "rev-parse", "--is-inside-work-tree"], cwd=ROOT,
                            capture_output=True, text=True, check=False)
    if inside.returncode != 0:  # pragma: no cover - checkout sin .git
        pytest.skip("no es un worktree de git")
    tracked = subprocess.run(["git", "ls-files", "artifacts/audits/gazetteer_source_bindings_v1/records/"],
                             cwd=ROOT, capture_output=True, text=True, check=False).stdout.split()
    on_disk = sorted((ARTIFACT / audit.RECORDS_SUBDIR).glob("*.json"))
    assert len(on_disk) == 1134
    assert len(tracked) == 1134, f"sólo {len(tracked)} de 1134 raw records están trackeados"
