"""Gazetteer-Wide Schema-3-aware: la adjudicación, gate por gate.

Cada test construye una identidad mínima y verifica UN gate. Los casos
negativos importan tanto como el positivo: un auditor que siempre dice
CANDIDATE pasaría el test feliz y promovería mezclas.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "audit_schema3", ROOT / "scripts" / "audits" / "audit_gazetteer_source_bindings_schema3.py",
)
audit = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
sys.modules[_SPEC.name] = audit
_SPEC.loader.exec_module(audit)

P, J = "p6dx-8zbt", "jbjy-vk9h"


class _EmptyRegistry:
    def bindings(self):
        return ()

    def binding_for(self, _canonical):
        return None


def _ds(*groups, complete=True, status="OK"):
    return {"status": status, "complete": complete,
            "universe": [{"name": n, "nit": nit, "rows": rows} for n, nit, rows in groups]}


def _att(nits=("800100200",), admissibility="ADMISSIBLE", shared="NOT_SHARED", status="NIT_FOUND_UNIQUE_ACCOUNT"):
    return {"canonical_name": "x", "observed_nits": list(nits), "fiscal_status": status,
            "matched_directory_nits": list(nits), "name_match": "EQUAL_TOKENS",
            "admissibility": admissibility, "shared_nit": shared, "directory_account_count": 1}


def _unit(canonical="HOSPITAL LOCAL DE PRUEBA", p=None, j=None, att=None, groups=None,
          identity=None, kind="buyer_unit"):
    form = canonical
    return audit.Unit(
        canonical=canonical, kind=kind, v1_status="NO_STABLE_IDENTITY", v1_identity=identity,
        declared_groups=groups or [],
        datasets={P: p if p is not None else _ds((form, "800100200", 120)),
                  J: j if j is not None else _ds((form, "800100200", 80))},
        binding=None, attestation=_att() if att is None else att, v1_record_hash="h",
    )


def _run(unit, *others, registry=None):
    units = [unit, *others]
    index = audit.build_collision_index(units, registry or _EmptyRegistry())
    return audit.adjudicate(unit, index)


def test_clean_unit_with_directory_corroboration_is_candidate():
    result = _run(_unit())
    assert result["verdict"] == audit.CANDIDATE
    cand = result["candidate"]
    assert cand["source_names"] == {P: ["HOSPITAL LOCAL DE PRUEBA"], J: ["HOSPITAL LOCAL DE PRUEBA"]}
    assert cand["observed_rows"] == {P: 120, J: 80}
    assert cand["promotion_status"] == "NOT_PROMOTED"
    assert result["datasets"][P]["equivalent_to_fallback"] is True


@pytest.mark.parametrize("admissibility", ["REVIEW", "NO_EVIDENCE"])
def test_directory_without_admissibility_does_not_reject_but_does_not_corroborate(admissibility):
    result = _run(_unit(att=_att(admissibility=admissibility, status="NIT_NOT_FOUND")))
    assert result["verdict"] == "STRUCTURAL_SAFE_UNCORROBORATED"
    assert result["failed_gates"] == ["STRUCTURAL_SAFE_UNCORROBORATED"]


def test_shared_nit_is_a_dimension_not_a_rejection():
    catalog = {"accredited": True, "provenance": "app/data/sena_units.json", "type": "BUYER_UNIT"}
    result = _run(_unit(att=_att(admissibility="REVIEW", shared="SHARED",
                                 status="NIT_FOUND_MULTI_ACCOUNT"), identity=catalog))
    assert result["verdict"] == audit.CANDIDATE
    assert result["corroboration"] == "DECLARED_CATALOG"


def test_two_nits_under_one_form_is_review():
    unit = _unit(p=_ds(("HOSPITAL LOCAL DE PRUEBA", "800100200", 100),
                       ("HOSPITAL LOCAL DE PRUEBA", "899999111", 20)))
    assert _run(unit)["verdict"] == "MULTI_NIT_REVIEW"


def test_nits_diverging_between_datasets_is_review():
    unit = _unit(j=_ds(("HOSPITAL LOCAL DE PRUEBA", "899999111", 80)))
    assert _run(unit)["verdict"] == "MULTI_NIT_REVIEW"


def test_two_forms_in_one_dataset_is_review():
    unit = _unit(p=_ds(("HOSPITAL LOCAL DE PRUEBA", "800100200", 100),
                       ("HOSPITAL LOCAL DE PRUEBA.", "800100200", 3)))
    assert _run(unit)["verdict"] == "MULTI_FORM_REVIEW"


def test_form_extending_the_canonical_is_review():
    form = "HOSPITAL LOCAL DE PRUEBA - SEDE NORTE"
    unit = _unit(p=_ds((form, "800100200", 100)), j=_ds((form, "800100200", 80)))
    assert _run(unit)["verdict"] == "NAME_EXTENSION_REVIEW"


def test_punctuation_only_variant_is_accepted_as_equal_tokens():
    form = "HOSPITAL LOCAL DE PRUEBA."
    unit = _unit(p=_ds((form, "800100200", 100)), j=_ds((form, "800100200", 80)))
    result = _run(unit)
    assert result["verdict"] == audit.CANDIDATE
    assert result["candidate"]["name_relation"] == {form: "EQUAL_TOKENS"}


def test_form_claimed_by_another_unbound_unit_is_collision():
    a = _unit("HOSPITAL LOCAL DE PRUEBA")
    b = _unit("Hospital Local de Prueba.")  # otro canónico cuya forma normaliza distinto pero reclama la misma grafía
    b.datasets = {P: _ds(("HOSPITAL LOCAL DE PRUEBA", "800100200", 120)), J: _ds()}
    assert _run(a, b)["verdict"] == "CROSS_UNIT_FORM_COLLISION"


def test_form_already_bound_to_another_unit_is_collision():
    class _Registry(_EmptyRegistry):
        def bindings(self):
            return (type("B", (), {"canonical_name": "OTRA UNIDAD",
                                   "source_names": {P: ("HOSPITAL LOCAL DE PRUEBA",)}})(),)

    assert _run(_unit(), registry=_Registry())["verdict"] == "CROSS_UNIT_FORM_COLLISION"


def test_substring_reach_by_another_units_like_does_not_block():
    """`ANI` alcanza `ARTESANIAS ...` por substring: defecto de ANI, no identidad de la forma."""
    target = _unit("ARTESANIAS DE COLOMBIA S.A")
    target.datasets = {P: _ds(("ARTESANIAS DE COLOMBIA S.A", "800100200", 50)),
                       J: _ds(("ARTESANIAS DE COLOMBIA S.A", "800100200", 40))}
    ani = _unit("ANI")
    ani.datasets = {P: _ds(("ARTESANIAS DE COLOMBIA S.A", "800100200", 50),
                           ("AGENCIA NACIONAL DE INFRAESTRUCTURA", "830125996", 9000)), J: _ds()}
    result = _run(target, ani)
    assert result["verdict"] == audit.CANDIDATE
    assert result["candidate"]["reached_by_like_of"] == {"ARTESANIAS DE COLOMBIA S.A": ["ANI"]}


def test_truncated_like_universe_is_never_equivalence():
    unit = _unit(p=_ds(("HOSPITAL LOCAL DE PRUEBA", "800100200", 100), complete=False))
    result = _run(unit)
    assert result["verdict"] == "INCOMPLETE_OBSERVATION"
    assert result["datasets"][P]["equivalent_to_fallback"] is None


def test_timeout_is_not_zero():
    unit = _unit(j=_ds(status="TIMEOUT", complete=None))
    assert _run(unit)["verdict"] == "INCOMPLETE_OBSERVATION"


def test_directory_nit_disagreeing_with_source_is_fiscal_conflict():
    assert _run(_unit(att=_att(nits=("811222333",))))["verdict"] == "FISCAL_CONFLICT_REVIEW"


def test_declared_group_member_is_not_bound_in_batch():
    assert _run(_unit(groups=["bogota_central"]))["verdict"] == "DECLARED_GROUP_MEMBER_REVIEW"


def test_absent_dataset_is_left_unprojected_not_bound_to_nothing():
    result = _run(_unit(j=_ds()))
    assert result["verdict"] == audit.CANDIDATE
    assert set(result["candidate"]["source_names"]) == {P}
    assert result["candidate"]["datasets_without_presence"] == [J]


def test_imbalanced_dataset_is_withheld_not_bound():
    """Behavior-identical either way; binding it would DECLARE a false projection."""
    result = _run(_unit(p=_ds(("HOSPITAL LOCAL DE PRUEBA", "800100200", 44658)),
                        j=_ds(("HOSPITAL LOCAL DE PRUEBA", "800100200", 34))))
    cand = result["candidate"]
    assert result["verdict"] == audit.CANDIDATE
    assert set(cand["source_names"]) == {P}
    assert set(cand["withheld_datasets"]) == {J}
    assert set(cand["observed_rows"]) == {P}


def test_no_presence_anywhere_is_not_a_candidate():
    assert _run(_unit(p=_ds(), j=_ds()))["verdict"] == "NO_SOURCE_PRESENCE"


def test_ecosystem_never_becomes_candidate():
    assert _run(_unit(kind="ecosystem"))["verdict"] == audit.ECOSYSTEM


def test_tampered_v1_record_aborts(tmp_path):
    v1 = tmp_path / "v1"
    (v1 / "records").mkdir(parents=True)
    rec = v1 / "records" / "a.json"
    rec.write_text("{}", encoding="utf-8")
    (v1 / "records.sha256").write_text(f"{audit.sha256_file(rec)}  records/a.json\n", encoding="utf-8")
    assert len(audit.load_v1_records(v1)) == 1
    rec.write_text('{"x": 1}', encoding="utf-8")
    with pytest.raises(audit.InputIntegrityError):
        audit.load_v1_records(v1)


# ── Contra el artefacto congelado ──────────────────────────────────────────

ARTIFACT = ROOT / "artifacts" / "audits" / "gazetteer_source_bindings_schema3"


@pytest.mark.skipif(not (ARTIFACT / "candidates.jsonl").exists(), reason="artefacto no generado")
def test_frozen_candidates_are_schema3_loadable_next_to_current_sidecar():
    """Preflight del Promotion Batch: los borradores no chocan con el sidecar vigente."""
    from app.core.buyer_source_names import parse_registry

    sidecar = json.loads((ROOT / "app" / "data" / "buyer_source_names.json").read_text(encoding="utf-8"))
    candidates = [json.loads(line) for line in (ARTIFACT / "candidates.jsonl").read_text(encoding="utf-8").splitlines()]
    drafts = [{
        "canonical_name": c["canonical_name"],
        "source_names": c["source_names"],
        "projection_evidence": {"observed_at": c["observed_at"], "evidence": c["evidence_record"],
                                "observed_rows": c["observed_rows"]},
        "fiscal_evidence": [],
    } for c in candidates]
    registry = parse_registry({**sidecar, "buyer_units": sidecar["buyer_units"] + drafts})
    assert len(registry) == len(sidecar["buyer_units"]) + len(candidates)


@pytest.mark.skipif(not (ARTIFACT / "records.jsonl").exists(), reason="artefacto no generado")
def test_frozen_artifact_invariants():
    records = [json.loads(line) for line in (ARTIFACT / "records.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(records) == 1134
    by_verdict = {}
    for r in records:
        by_verdict.setdefault(r["verdict"], []).append(r)
    assert len(by_verdict[audit.BOUND]) == 593
    assert len(by_verdict[audit.ECOSYSTEM]) == 23
    for r in by_verdict.get(audit.CANDIDATE, []):
        c = r["candidate"]
        # una forma por dataset y exact == fallback
        assert c["source_names"], "un candidato sin ningún dataset proyectado no es candidato"
        assert all(len(v) == 1 for v in c["source_names"].values())
        assert not set(c["source_names"]) & set(c["withheld_datasets"])
        for ds in c["source_names"]:
            assert r["datasets"][ds]["equivalent_to_fallback"] is True
        assert c["promotion_status"] == "NOT_PROMOTED"
        assert not r["declared_groups"]
    # ningún NOT_FOUND/REVIEW del Directorio se convirtió en rechazo de identidad
    assert "IDENTITY_DISPROVED" not in by_verdict
