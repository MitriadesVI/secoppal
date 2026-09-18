"""Auditoría ADR-073 gazetteer-wide: los contratos que el auditor NO puede romper.

Estos casos no consultan SECOP: sustituyen la fuente por una que responde con
formas conocidas y verifican que la clasificación, el candidato y el
checkpointing se comportan como el encargo exige. Lo que se protege:

- el candidato contiene EXACTAMENTE lo que el LIKE ya alcanza (nunca una forma
  no alcanzada, nunca una identidad ajena);
- un timeout no es equivalencia;
- una identidad no acreditada se declara, no se inventa;
- un binding vigente se verifica, no se modifica;
- SENA/ICBF no se aplanan;
- el batch sobrevive a un fallo individual y el checkpoint queda en disco.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from app.core.buyer_groups import BuyerGroup
from app.core.soql_builder import SoQLBuilder
from scripts.audits import audit_gazetteer_source_bindings_v1 as audit

ROOT = Path(__file__).resolve().parents[1]
SIDECAR = ROOT / "app/data/buyer_source_names.json"
P6DX, JBBY = audit.PROCESSES, audit.CONTRACTS
FIELDS = {P6DX: "entidad", JBBY: "nombre_entidad"}


class FakeSource:
    """Fuente falsa con la forma de `Source`: responde por tipo de consulta.

    Se clasifica la consulta por su texto y NO se reconstruye ningún predicado:
    el auditor sigue emitiendo SoQL real y aquí sólo se decide qué devolver.
    """

    transport_name = "fake"

    def __init__(self, *, like=None, forms=None, bound=None, counts=None, failures=None):
        self.like = like or {}
        self.forms = forms or {}
        self.bound = bound or {}
        self.counts = counts or {}
        self.failures = failures or {}
        self.calls: list[tuple[str, str]] = []
        self.telemetry = {"calls": 0, "ok": 0, "timeouts": 0, "errors": 0, "retries": 0}
        self._calls_here = 0

    def reset_canonical(self) -> None:
        self._calls_here = 0

    def note_status(self, status: str) -> None:
        if status != "OK":
            self.telemetry["timeouts"] += 1

    def canonical_network(self) -> dict:
        return {"calls": self._calls_here, "timeouts": 0, "retries": 0, "latency_ms": 0.0}

    def query(self, dataset: str, soql: str):
        self.calls.append((dataset, soql))
        self._calls_here += 1
        self.telemetry["calls"] += 1
        field = FIELDS[dataset]
        if "nit_entidad='" in soql:
            kind = "forms"
        elif "SELECT count(*) AS cnt" in soql:
            kind = "count"
        elif f"{field} = " in soql:
            kind = "bound"
        elif "LIKE" in soql:
            kind = "like"
        else:  # pragma: no cover - el auditor no emite otra forma
            raise AssertionError(f"consulta inesperada: {soql}")
        status = self.failures.get(f"{kind}:{dataset}")
        if status:
            self.telemetry["timeouts"] += 1
            return audit.CallResult(dataset, soql, status, [], 1.0, 2, "forzado por el test")
        self.telemetry["ok"] += 1
        if kind == "count":
            like_count, exact_count = self.counts.get(dataset, (0, 0))
            value = like_count if "LIKE" in soql else exact_count
            return audit.CallResult(dataset, soql, "OK", [{"cnt": str(value)}], 1.0, 1)
        if kind == "forms":
            rows = [{field: name, "cnt": str(rows)} for name, rows in self.forms.get(dataset, [])]
        elif kind == "bound":
            rows = [{field: name, "nit_entidad": nit, "cnt": str(rows)}
                    for name, nit, rows in self.bound.get(dataset, [])]
        else:
            rows = [{field: name, "nit_entidad": nit, "cnt": str(rows)}
                    for name, nit, rows in self.like.get(dataset, [])]
        return audit.CallResult(dataset, soql, "OK", rows, 1.0, 1)


def canonical(name: str, *, nit=None, bound=None, groups=(), ecosystem=None, kind="buyer_unit"):
    identity = audit.Identity("NIT", nit, "test") if nit else None
    return audit.Canonical(
        canonical_name=name, kind=kind, origin="test", identity=identity,
        declared_groups=list(groups), ecosystem=ecosystem,
        binding=bound,
    )


def binding(names_by_dataset, identity_key="NIT:123"):
    return {
        "identity_key": identity_key,
        "sources": {dataset: list(names) for dataset, names in names_by_dataset.items()},
        "observed_at": "2026-09-01",
        "observed_rows": {dataset: 10 for dataset in names_by_dataset},
        "evidence": "artifacts/audits/test.json",
    }


def observe(source, record, *, phase_b=True):
    measurement = audit.measure_canonical(source, record, phase_b=phase_b)
    verdict = audit.evaluate_binding(record, measurement)
    outcome = audit.classify(record, measurement, verdict)
    candidate = audit.build_candidate(record, measurement, outcome["status"])
    return measurement, verdict, outcome, candidate


# ── 1. canónico == físico ───────────────────────────────────────────────────


def test_canonical_equal_to_physical_is_exact_bindable():
    record = canonical("MUNICIPIO DE PRUEBA", nit="123")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO DE PRUEBA", "123", 10)],
              JBBY: [("MUNICIPIO DE PRUEBA", "123", 8)]},
        forms={P6DX: [("MUNICIPIO DE PRUEBA", 10)], JBBY: [("MUNICIPIO DE PRUEBA", 8)]},
        counts={P6DX: (10, 10), JBBY: (8, 8)},
    )
    measurement, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "EXACT_BINDABLE"
    assert measurement["datasets"][P6DX]["equivalence"]["same_universe"] is True
    assert candidate["sources"] == {P6DX: ["MUNICIPIO DE PRUEBA"], JBBY: ["MUNICIPIO DE PRUEBA"]}
    assert candidate["classification"] == "EXACT_BINDABLE"
    assert candidate["promotion_status"] == "CANDIDATE_NOT_PROMOTED"


# ── 2. nombres distintos por dataset ────────────────────────────────────────


def test_different_physical_names_per_dataset_are_source_name_split():
    record = canonical("MUNICIPIO DE OTRO", nit="456")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO DE OTRO..", "456", 5)],
              JBBY: [("MUNICIPIO DE OTRO.", "456", 6)]},
        forms={P6DX: [("MUNICIPIO DE OTRO..", 5)], JBBY: [("MUNICIPIO DE OTRO.", 6)]},
        counts={P6DX: (5, 5), JBBY: (6, 6)},
    )
    measurement, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "SOURCE_NAME_SPLIT"
    # El split no autoriza a sumar la forma "de un punto" en el dataset de dos
    # puntos ni al revés: cada dataset lleva su propia forma.
    assert candidate["sources"][P6DX] == ["MUNICIPIO DE OTRO.."]
    assert candidate["sources"][JBBY] == ["MUNICIPIO DE OTRO."]


# ── 3. identidad ajena ──────────────────────────────────────────────────────


def test_like_reaching_foreign_nit_is_collision_and_blocks_candidate():
    record = canonical("MUNICIPIO DE COLISION", nit="789")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO DE COLISION", "789", 10), ("MUNICIPIO DE COLISION AJENA", "999", 3)],
              JBBY: [("MUNICIPIO DE COLISION", "789", 10)]},
        forms={P6DX: [("MUNICIPIO DE COLISION", 10)], JBBY: [("MUNICIPIO DE COLISION", 10)]},
        counts={P6DX: (13, 10), JBBY: (10, 10)},
    )
    measurement, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "FOREIGN_IDENTITY_COLLISION"
    assert "999" in outcome["reasons"][0]
    assert candidate is None


def test_candidate_is_refused_when_a_reached_name_belongs_to_another_nit():
    """Aunque la clasificación llegue a bindable, la forma ajena no entra."""
    record = canonical("MUNICIPIO DE MIXTO", nit="111")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO DE MIXTO", "111", 4)],
              JBBY: [("MUNICIPIO DE MIXTO", "222", 4)]},
        forms={P6DX: [("MUNICIPIO DE MIXTO", 4)], JBBY: [("MUNICIPIO DE MIXTO", 4)]},
        counts={P6DX: (4, 4), JBBY: (4, 4)},
    )
    measurement, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "FOREIGN_IDENTITY_COLLISION"
    assert candidate is None


# ── 4. varias formas bajo el mismo NIT ──────────────────────────────────────


def test_multiple_physical_forms_reached_under_one_nit_is_not_auto_promoted():
    """Dos formas token-distintas entran al universo bajo el MISMO NIT.

    Es exactamente el caso `SAME_NIT != SAME_BUYER_UNIT`: el LIKE alcanza más de
    una forma y ninguna es prefijo de la otra, así que no se afirma que sean
    meras grafías ni se promueve un binding con las dos.
    """
    record = canonical("DE RAIZ", nit="555")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO DE RAIZ", "555", 9), ("SECRETARIA DE RAIZ Y OBRAS", "555", 4)],
              JBBY: [("MUNICIPIO DE RAIZ", "555", 9)]},
        forms={P6DX: [("MUNICIPIO DE RAIZ", 9), ("SECRETARIA DE RAIZ Y OBRAS", 4)],
               JBBY: [("MUNICIPIO DE RAIZ", 9)]},
        counts={P6DX: (13, 13), JBBY: (9, 9)},
    )
    _, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "MULTI_NAME_REVIEW"
    assert candidate is None


def test_units_the_like_does_not_reach_are_declared_and_stay_out_of_the_binding():
    """La identidad publica una unidad hija que el LIKE no alcanza: se declara
    (regla F) y NO entra al candidato; incorporarla ampliaría el universo."""
    record = canonical("MUNICIPIO PARCIAL", nit="777")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO PARCIAL", "777", 10)], JBBY: [("MUNICIPIO PARCIAL", "777", 10)]},
        forms={P6DX: [("MUNICIPIO PARCIAL", 10), ("MUNICIPIO PARCIAL - ESD", 5)],
               JBBY: [("MUNICIPIO PARCIAL", 10)]},
        counts={P6DX: (10, 10), JBBY: (10, 10)},
    )
    measurement, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "EXACT_BINDABLE"
    assert measurement["datasets"][P6DX]["identity_forms"][1]["name"] == "MUNICIPIO PARCIAL - ESD"
    summary = audit.dataset_summary(measurement["datasets"][P6DX])
    assert summary["forms_not_reached_by_like"] == ["MUNICIPIO PARCIAL - ESD"]
    assert candidate["sources"][P6DX] == ["MUNICIPIO PARCIAL"]


def test_child_shaped_forms_are_buyer_group_candidate_not_binding():
    record = canonical("MUNICIPIO DE RAIZ", nit="555")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO DE RAIZ", "555", 9),
                     ("MUNICIPIO DE RAIZ - SECRETARIA DE OBRAS", "555", 7)],
              JBBY: [("MUNICIPIO DE RAIZ", "555", 9)]},
        forms={P6DX: [("MUNICIPIO DE RAIZ", 9)], JBBY: [("MUNICIPIO DE RAIZ", 9)]},
        counts={P6DX: (16, 16), JBBY: (9, 9)},
    )
    _, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "BUYER_GROUP_CANDIDATE"
    assert candidate is None


# ── 5. timeout ──────────────────────────────────────────────────────────────


def test_like_timeout_is_inconclusive_never_equivalence():
    record = canonical("MUNICIPIO LENTO", nit="321")
    source = FakeSource(failures={f"like:{JBBY}": "TIMEOUT"})
    measurement, _, outcome, _ = observe(source, record)
    assert outcome["status"] == "INCONCLUSIVE"
    assert measurement["datasets"][JBBY]["like_status"] == "TIMEOUT"
    assert measurement["datasets"][JBBY]["equivalence"]["same_universe"] is None


def test_unfinished_equivalence_is_inconclusive():
    record = canonical("MUNICIPIO SIN CONTEO", nit="321")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO SIN CONTEO", "321", 3)], JBBY: [("MUNICIPIO SIN CONTEO", "321", 3)]},
        forms={P6DX: [("MUNICIPIO SIN CONTEO", 3)], JBBY: [("MUNICIPIO SIN CONTEO", 3)]},
        failures={f"count:{JBBY}": "TIMEOUT"},
    )
    measurement, _, outcome, candidate = observe(source, record)
    assert measurement["datasets"][JBBY]["equivalence"]["same_universe"] is None
    assert outcome["status"] == "INCONCLUSIVE"
    assert candidate is None


def test_not_equivalent_when_like_and_exact_counts_differ():
    record = canonical("MUNICIPIO DIVERGENTE", nit="321")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO DIVERGENTE", "321", 3)], JBBY: [("MUNICIPIO DIVERGENTE", "321", 3)]},
        forms={P6DX: [("MUNICIPIO DIVERGENTE", 3)], JBBY: [("MUNICIPIO DIVERGENTE", 3)]},
        counts={P6DX: (3, 3), JBBY: (5, 3)},
    )
    _, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "NOT_EQUIVALENT"
    assert candidate is None


# ── 6. sin identidad acreditada ─────────────────────────────────────────────


def test_missing_identity_is_declared_not_invented():
    record = canonical("COMPRADOR SIN IDENTIDAD")
    source = FakeSource(like={P6DX: [("COMPRADOR SIN IDENTIDAD", None, 4)],
                              JBBY: [("COMPRADOR SIN IDENTIDAD", None, 4)]})
    _, _, outcome, candidate = observe(source, record, phase_b=False)
    assert outcome["status"] == "NO_STABLE_IDENTITY"
    assert candidate is None


def test_single_unaccredited_nit_is_still_no_stable_identity():
    record = canonical("COMPRADOR CON NIT OBSERVADO")
    source = FakeSource(like={P6DX: [("COMPRADOR CON NIT OBSERVADO", "900123", 4)],
                              JBBY: [("COMPRADOR CON NIT OBSERVADO", "900123", 4)]})
    _, _, outcome, candidate = observe(source, record, phase_b=False)
    assert outcome["status"] == "NO_STABLE_IDENTITY"
    assert "900123" in outcome["reasons"][0]
    assert candidate is None


def test_two_observed_identities_are_ambiguous_identity():
    record = canonical("MUNICIPIO CONFUNDIDO")
    source = FakeSource(like={P6DX: [("MUNICIPIO CONFUNDIDO", "111", 4)],
                              JBBY: [("MUNICIPIO CONFUNDIDO", "222", 4)]})
    _, _, outcome, candidate = observe(source, record, phase_b=False)
    assert outcome["status"] == "AMBIGUOUS_IDENTITY"
    assert candidate is None


def test_unaccredited_multi_unit_nit_is_flagged_not_silently_accepted():
    """Sin identidad acreditada no hay binding, pero el fenómeno se declara:
    un solo NIT observado publicando varias unidades (sección 16)."""
    record = canonical("GOBERNACION DE EJEMPLO")
    source = FakeSource(like={P6DX: [("GOBERNACION DE EJEMPLO", "800", 40),
                                     ("GOBERNACION DE EJEMPLO - SECRETARIA DE HACIENDA", "800", 12)],
                              JBBY: [("GOBERNACION DE EJEMPLO", "800", 30)]})
    _, _, outcome, candidate = observe(source, record, phase_b=False)
    assert outcome["status"] == "BUYER_GROUP_CANDIDATE"
    assert candidate is None
    assert "sin acreditar" in outcome["reasons"][0]


def test_unaccredited_multiple_units_without_root_shape_is_multi_name_review():
    record = canonical("DE EJEMPLO")
    source = FakeSource(like={P6DX: [("MUNICIPIO DE EJEMPLO", "800", 40),
                                     ("FONDO DE EJEMPLO Y CULTURA", "800", 12)]})
    _, _, outcome, candidate = observe(source, record, phase_b=False)
    assert outcome["status"] == "MULTI_NAME_REVIEW"
    assert candidate is None


def test_like_blind_to_accredited_forms_is_not_equivalent():
    """El nombre canónico no se publica y el LIKE no alcanza nada: un binding
    con las formas reales AMPLIARÍA el universo, así que no hay equivalencia."""
    record = canonical("NOMBRE LARGO CANONICO", nit="899")
    source = FakeSource(
        like={P6DX: [], JBBY: []},
        forms={P6DX: [("CORTO", 17)], JBBY: [("CORTO", 4)]},
    )
    _, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "NOT_EQUIVALENT"
    assert candidate is None


# ── 7. binding vigente ──────────────────────────────────────────────────────


def test_existing_binding_verified_and_left_untouched():
    declared = {P6DX: ["DAPRE"], JBBY: ["DAPRE"]}
    record = canonical("DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA", nit="899999083",
                       bound=binding(declared, "NIT:899999083"))
    source = FakeSource(bound={P6DX: [("DAPRE", "899999083", 17128)],
                              JBBY: [("DAPRE", "899999083", 4351)]})
    measurement, verdict, outcome, candidate = observe(source, record)
    assert verdict["status"] == "VERIFIED"
    assert outcome["status"] == "ALREADY_BOUND_VERIFIED"
    assert record.binding["sources"] == declared
    # Producción no pasa por LIKE para una identidad bindeada: no se mide.
    assert measurement["datasets"][P6DX]["like_status"] == "SKIPPED_ALREADY_BOUND"
    assert candidate is None


def test_binding_whose_name_vanished_is_drift():
    declared = {P6DX: ["DAPRE"], JBBY: ["DAPRE"]}
    record = canonical("DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA", nit="899999083",
                       bound=binding(declared, "NIT:899999083"))
    source = FakeSource(bound={P6DX: [], JBBY: [("DAPRE", "899999083", 4351)]})
    _, verdict, outcome, _ = observe(source, record)
    assert verdict["status"] == "DRIFT"
    assert verdict["missing_names"] == {P6DX: ["DAPRE"]}
    assert outcome["status"] == "BINDING_DRIFT_REVIEW"


def test_binding_reaching_foreign_nit_is_drift():
    declared = {P6DX: ["ALGO"], JBBY: ["ALGO"]}
    record = canonical("ALGO", nit="100", bound=binding(declared, "NIT:100"))
    source = FakeSource(bound={P6DX: [("ALGO", "999", 3)], JBBY: [("ALGO", "100", 3)]})
    _, verdict, outcome, _ = observe(source, record)
    assert verdict["foreign_nits"] == {P6DX: ["999"]}
    assert outcome["status"] == "BINDING_DRIFT_REVIEW"


def test_binding_unverified_when_source_does_not_answer():
    declared = {P6DX: ["ALGO"], JBBY: ["ALGO"]}
    record = canonical("ALGO", nit="100", bound=binding(declared, "NIT:100"))
    source = FakeSource(failures={f"bound:{JBBY}": "TIMEOUT"},
                        bound={P6DX: [("ALGO", "100", 3)]})
    _, _, outcome, _ = observe(source, record)
    assert outcome["status"] == "INCONCLUSIVE"


# ── 8/9. Buyer Groups ───────────────────────────────────────────────────────


def _record(name, status, *, bound=False, candidate=None):
    return {
        "canonical_name": name, "status": status, "already_bound": bound,
        "candidate": candidate, "binding": binding({P6DX: [f"{name} PH"], JBBY: [f"{name} PH"]})
        if bound else None,
        "datasets": {},
    }


def test_partial_group_reports_missing_bindings():
    group = BuyerGroup("g", "v1", "Grupo", ("alias",), ("UNO", "DOS"))
    ready_record = _record("UNO", "ALREADY_BOUND_VERIFIED", bound=True)
    blocked_record = _record("DOS", "NO_STABLE_IDENTITY")
    report = audit.group_readiness([ready_record, blocked_record], {"g": group})
    assert report["g"]["members_already_bound"] == 1
    assert report["g"]["members_blocked"] == 1
    assert report["g"]["missing_bindings"] == ["DOS"]
    assert report["g"]["status"] == "PARTIAL"


def test_group_fully_bound_is_ready_today():
    group = BuyerGroup("g", "v1", "Grupo", ("alias",), ("UNO", "DOS"))
    records = [_record("UNO", "ALREADY_BOUND_VERIFIED", bound=True),
               _record("DOS", "ALREADY_BOUND_VERIFIED", bound=True)]
    report = audit.group_readiness(records, {"g": group})
    assert report["g"]["status"] == "READY_TODAY"
    assert report["g"]["members_blocked"] == 0


def test_group_with_candidate_member_is_ready_after_review_only():
    group = BuyerGroup("g", "v1", "Grupo", ("alias",), ("UNO", "DOS"))
    candidate = {
        "canonical_name": "DOS", "identity_key": {"type": "NIT", "value": "1"},
        "sources": {P6DX: ["DOS PH"], JBBY: ["DOS PH"]}, "observed_rows": {P6DX: 1, JBBY: 1},
        "semantics_preserved": True, "promotion_status": "CANDIDATE_NOT_PROMOTED",
    }
    records = [_record("UNO", "ALREADY_BOUND_VERIFIED", bound=True),
               _record("DOS", "BUYER_GROUP_MEMBER", candidate=candidate)]
    report = audit.group_readiness(records, {"g": group})
    assert report["g"]["status"] == "READY_AFTER_REVIEW"
    assert report["g"]["members_candidate_ready"] == 1


# ── 10. ecosistemas institucionales ─────────────────────────────────────────


def test_inventory_registers_sena_and_icbf_as_ecosystems_not_buyer_units():
    authorities = audit.load_authorities()
    inventory = audit.build_inventory(authorities)
    ecosystems = {item.canonical_name: item for item in inventory if item.kind == "ecosystem"}
    assert "ECOSYSTEM:SENA:national" in ecosystems
    assert "ECOSYSTEM:ICBF:national" in ecosystems
    assert any(name.startswith("ECOSYSTEM:SENA:regional:") for name in ecosystems)
    # El padre SENA no se aplana en un binding ni recibe identidad.
    parent = ecosystems["ECOSYSTEM:SENA:national"]
    assert parent.identity is None and parent.binding is None


def test_sena_parent_classifies_as_institutional_ecosystem():
    authorities = audit.load_authorities()
    inventory = audit.build_inventory(authorities)
    parent = next(item for item in inventory if item.canonical_name == "ECOSYSTEM:SENA:national")
    source = FakeSource()
    measurement, verdict, outcome, candidate = observe(source, parent)
    assert outcome["status"] == "INSTITUTIONAL_ECOSYSTEM"
    assert candidate is None
    # No se mide el nombre sintético del padre: se mide su proyección declarada,
    # y esa proyección es una igualdad exacta contra los nombres oficiales.
    assert all("LIKE" not in entry["like_where"] for entry in measurement["datasets"].values())
    assert all("ECOSYSTEM" not in entry["like_where"] for entry in measurement["datasets"].values())


def test_sena_regional_ecosystem_is_measured_with_its_declared_projection():
    authorities = audit.load_authorities()
    inventory = audit.build_inventory(authorities)
    regional = next(item for item in inventory
                    if item.canonical_name.startswith("ECOSYSTEM:SENA:regional:"))
    source = FakeSource(like={P6DX: [("SENA REGIONAL X", "899999034", 7)], JBBY: []})
    measurement, _, outcome, _ = observe(source, regional)
    assert outcome["status"] == "INSTITUTIONAL_ECOSYSTEM"
    where = measurement["datasets"][P6DX]["like_where"]
    # contains_all: los tokens declarados entran como AND de LIKEs, no como nombre.
    assert where.count("LIKE") == len(regional.ecosystem_projection["contains_all"])


def test_icbf_national_ecosystem_declares_its_group():
    authorities = audit.load_authorities()
    inventory = audit.build_inventory(authorities)
    icbf = next(item for item in inventory if item.canonical_name == "ECOSYSTEM:ICBF:national")
    assert icbf.ecosystem["declared_group"] == "icbf_national"
    assert len(icbf.ecosystem_projection["official_names"]) >= 33


# ── 11/12. resumen, resume y checkpoints ────────────────────────────────────


def _args(tmp_path, **overrides) -> argparse.Namespace:
    base = dict(workers=1, limit=0, only=None, start_after=None, recheck_existing=False,
                summary_only=False, transport="soda3", timeout=1.0, retries=0, resume=False)
    base.update(overrides)
    return argparse.Namespace(output=str(tmp_path), **base)


def test_run_checkpoints_every_canonical(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory",
                        lambda authorities: [canonical("A"), canonical("B"), canonical("C")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    lines = (tmp_path / "progress.jsonl").read_text().strip().splitlines()
    assert len(lines) == 3
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["total"] == 3
    assert (tmp_path / "manifest.json").exists()
    assert (tmp_path / "buyer_group_readiness.json").exists()


def test_resume_skips_canonicals_already_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory",
                        lambda authorities: [canonical("A"), canonical("B")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path, resume=True), source_factory=lambda settings, args: source)
    first_calls = len(source.calls)
    assert (tmp_path / "progress.jsonl").read_text().count("\n") == 2

    second_source = FakeSource(like={})
    audit.run(_args(tmp_path, resume=True), source_factory=lambda settings, args: second_source)
    assert (tmp_path / "progress.jsonl").read_text().count("\n") == 2, "resume reescribió"
    assert len(second_source.calls) == 0
    assert first_calls > 0


def test_crash_after_first_canonical_keeps_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory",
                        lambda authorities: [canonical("A"), canonical("B")])

    class Crashing(FakeSource):
        def query(self, dataset, soql):
            if "'%B%'" in soql:  # el canónico B, no cualquier "B" del SoQL
                raise KeyboardInterrupt("crash simulado")
            return super().query(dataset, soql)

    with pytest.raises(KeyboardInterrupt):
        audit.run(_args(tmp_path), source_factory=lambda settings, args: Crashing(like={}))
    raw = (tmp_path / "progress.jsonl").read_text().strip().splitlines()
    assert len(raw) == 1
    record = json.loads(raw[0])
    assert record["canonical_name"] == "A"
    assert record["finished_at"]


def test_malformed_source_result_is_error_for_that_identity_only(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory",
                        lambda authorities: [canonical("ROTA"), canonical("SANA")])

    class Malformed(FakeSource):
        def query(self, dataset, soql):
            result = super().query(dataset, soql)
            if "ROTA" in soql:
                result.rows = [{"name": "ROTA", "nit_entidad": "1", "cnt": "no-es-numero"}]
            return result

    source = Malformed(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    records = [json.loads(line) for line in (tmp_path / "progress.jsonl").read_text().splitlines()]
    by_name = {record["canonical_name"]: record for record in records}
    assert by_name["ROTA"]["status"] == "ERROR"
    assert by_name["ROTA"]["error"]
    assert by_name["SANA"]["status"] in audit.CLASSES
    errors_file = (tmp_path / "errors.jsonl").read_text().strip().splitlines()
    assert len(errors_file) == 1


def test_summary_only_rebuilds_without_touching_the_source(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    source = FakeSource(like={})
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    calls = len(source.calls)
    audit.run(_args(tmp_path, summary_only=True), source_factory=lambda settings, args: source)
    assert len(source.calls) == calls
    assert json.loads((tmp_path / "summary.json").read_text())["total"] == 1


# ── 14/15. invariantes del candidato ────────────────────────────────────────


def test_candidate_never_contains_forms_not_reached_by_like():
    record = canonical("MUNICIPIO PARCIAL", nit="777")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO PARCIAL", "777", 10)], JBBY: [("MUNICIPIO PARCIAL", "777", 10)]},
        forms={P6DX: [("MUNICIPIO PARCIAL", 10), ("MUNICIPIO PARCIAL - ESD", 5)],
               JBBY: [("MUNICIPIO PARCIAL", 10)]},
        counts={P6DX: (10, 10), JBBY: (10, 10)},
    )
    _, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "EXACT_BINDABLE"
    for names in candidate["sources"].values():
        assert "MUNICIPIO PARCIAL - ESD" not in names
    assert candidate["observed_rows"][P6DX] == 10


def test_candidate_never_contains_foreign_identity():
    record = canonical("MUNICIPIO CON AJENO EN CANDIDATO", nit="888")
    source = FakeSource(
        like={P6DX: [("MUNICIPIO CON AJENO EN CANDIDATO", "888", 2)],
              JBBY: [("MUNICIPIO CON AJENO EN CANDIDATO", "999", 2)]},
        forms={P6DX: [("MUNICIPIO CON AJENO EN CANDIDATO", 2)],
               JBBY: [("MUNICIPIO CON AJENO EN CANDIDATO", 2)]},
        counts={P6DX: (2, 2), JBBY: (2, 2)},
    )
    _, _, _, candidate = observe(source, record)
    assert candidate is None


# ── Ecosistemas: la unidad individual sí, la composición no ─────────────────


def test_declared_catalog_unit_can_produce_a_candidate():
    """Una unidad individual declarada por un catálogo institucional es una
    BuyerUnit: su binding es una proyección 1:1 normal."""
    record = canonical("SENA SECRETARIA GENERAL", bound=None)
    record.identity = audit.Identity("BUYER_UNIT", "SENA SECRETARIA GENERAL", "sena_units.json")
    record.ecosystem = {"family": "SENA", "role": "declared_unit"}
    source = FakeSource(
        like={P6DX: [("SENA SECRETARIA GENERAL", "899999034", 11)],
              JBBY: [("SENA SECRETARIA GENERAL", "899999034", 10)]},
        counts={P6DX: (11, 11), JBBY: (10, 10)},
    )
    _, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "EXACT_BINDABLE"
    assert candidate is not None
    assert candidate["sources"][P6DX] == ["SENA SECRETARIA GENERAL"]


def test_ecosystem_parent_never_produces_a_candidate():
    """El padre SENA representa «muchas unidades»: su lugar es el Buyer Group."""
    record = canonical("ECOSYSTEM:SENA:national", kind="ecosystem")
    record.ecosystem = {"family": "SENA", "role": "national_parent"}
    record.ecosystem_projection = {"mode": "official_names", "official_names": ["SENA SECRETARIA GENERAL"]}
    measurement = {"datasets": {}, "errors": []}
    assert audit.build_candidate(record, measurement, "EXACT_BINDABLE") is None
    regional = canonical("ECOSYSTEM:SENA:regional:Antioquia", kind="ecosystem")
    regional.ecosystem = {"family": "SENA", "role": "regional_ecosystem"}
    assert audit.build_candidate(regional, measurement, "EXACT_BINDABLE") is None


def test_bindable_classification_always_carries_a_candidate():
    """Invariante de coherencia: si la clase afirma que la proyección exacta
    preserva la semántica, el candidato tiene que existir. Un `EXACT_BINDABLE`
    sin candidato deja al revisor con una afirmación que no puede ejecutar."""
    cases = {
        "EXACT_BINDABLE": (
            {P6DX: [("MUNICIPIO COHERENTE", "123", 5)], JBBY: [("MUNICIPIO COHERENTE", "123", 5)]},
            {P6DX: [("MUNICIPIO COHERENTE", 5)], JBBY: [("MUNICIPIO COHERENTE", 5)]},
        ),
        "SOURCE_NAME_SPLIT": (
            {P6DX: [("MUNICIPIO COHERENTE..", "123", 5)],
             JBBY: [("MUNICIPIO COHERENTE.", "123", 5)]},
            {P6DX: [("MUNICIPIO COHERENTE..", 5)], JBBY: [("MUNICIPIO COHERENTE.", 5)]},
        ),
    }
    for status, (like, forms) in cases.items():
        record = canonical("MUNICIPIO COHERENTE", nit="123")
        source = FakeSource(like=like, forms=forms, counts={P6DX: (5, 5), JBBY: (5, 5)})
        _, _, outcome, candidate = observe(source, record)
        assert outcome["status"] == status, status
        assert candidate is not None, f"{status} sin candidato"
        assert candidate["classification"] == status
        assert candidate["semantics_preserved"] is True


def test_declared_name_absent_from_source_says_so():
    record = canonical("UNIDAD QUE NADIE PUBLICA")
    record.identity = audit.Identity("BUYER_UNIT", "UNIDAD QUE NADIE PUBLICA", "sena_units.json")
    record.ecosystem = {"family": "SENA", "role": "declared_unit"}
    source = FakeSource(like={P6DX: [], JBBY: []})
    _, _, outcome, candidate = observe(source, record)
    assert outcome["status"] == "INCONCLUSIVE"
    assert "no se publica en la fuente" in outcome["reasons"][0]
    assert candidate is None


def test_sidecar_is_never_written_by_the_audit(tmp_path, monkeypatch):
    before = SIDECAR.read_bytes()
    monkeypatch.setattr(audit, "build_inventory",
                        lambda authorities: [canonical("MUNICIPIO DE PRUEBA", nit="123")])
    source = FakeSource(
        like={P6DX: [("MUNICIPIO DE PRUEBA", "123", 10)],
              JBBY: [("MUNICIPIO DE PRUEBA", "123", 10)]},
        forms={P6DX: [("MUNICIPIO DE PRUEBA", 10)], JBBY: [("MUNICIPIO DE PRUEBA", 10)]},
        counts={P6DX: (10, 10), JBBY: (10, 10)},
    )
    audit.run(_args(tmp_path), source_factory=lambda settings, args: source)
    assert SIDECAR.read_bytes() == before
    candidates = (tmp_path / "bindings_ready.jsonl").read_text().strip().splitlines()
    assert len(candidates) == 1
    payload = json.loads(candidates[0])
    assert payload["promotion_status"] == "CANDIDATE_NOT_PROMOTED"
    assert "promoted" not in json.dumps(payload).lower() or payload["promotion_status"]


def test_audit_uses_the_builder_primitives_not_a_local_copy():
    record = canonical("MUNICIPIO DE PRIMITIVA", nit="123")
    source = FakeSource(like={P6DX: [("MUNICIPIO DE PRIMITIVA", "123", 1)]})
    measurement, _, _, _ = observe(source, record, phase_b=False)
    where = measurement["datasets"][P6DX]["like_where"]
    assert where == SoQLBuilder._accent_fold_like("entidad", "MUNICIPIO DE PRIMITIVA")
    assert audit.exact_clause("entidad", ["A", "B"]) == SoQLBuilder._exact_entity_clause(
        "entidad", ("A", "B")
    )
