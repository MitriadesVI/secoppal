"""Prueba de no-vacuidad de los seis oráculos reescritos en ORACLE_PATCH_V1.

No toca runtime ni el runner: reusa ``check_turn``/``material_frame`` del runner
HCA y les entrega el registro REAL de la corrida post-patch mutado con el defecto
conceptual que cada nuevo oráculo debe impedir. Un oráculo es NONVACUOUS si el
registro real pasa y el registro mutado falla.

Uso:
    .venv/bin/python docs/audits/evidence/2026-08-26_HCA_ORACLE_PATCH_V1/mutation_probe.py \
        --results <out>/results.jsonl [--json <salida>]
"""
from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

import yaml  # noqa: E402

from scripts.audits.run_conversation_holistic_audit import (  # noqa: E402
    check_turn,
    material_frame,
)

CORPUS = ROOT / "tests" / "fixtures" / "audits" / "conversation_holistic_v1.yaml"


def m_actor_004(rec: dict) -> dict:
    """El fuzzy auto-resuelve identidad y ejecuta sin selección del usuario."""
    p = rec["params"]
    p.pop("actor_pending", None)
    p["actor_resolution_status"] = "EXACT"
    p["actor_scope"] = {"document_type": "NIT", "base_number": "900739049"}
    rec["needs_clarification"] = False
    return rec


def m_topic_007(rec: dict) -> dict:
    """Se pierde el tema de alimentación escolar; queda sólo el territorio."""
    rec["params"]["objeto"] = []
    rec["soql"] = "SELECT id_del_proceso WHERE departamento_entidad = 'La Guajira' LIMIT 50"
    return rec


def m_entity_001(rec: dict) -> dict:
    """El comprador se reinterpreta como territorio (ADR-040 INV-7)."""
    p = rec["params"]
    p.pop("entidad_resolved", None)
    p["departamento_resolved"] = "Atlántico"
    rec["soql"] = (
        "SELECT id_contrato WHERE departamento = 'Atlántico' "
        "AND fecha_de_firma >= '2026-01-01' AND fecha_de_firma <= '2026-12-31'"
    )
    return rec


def m_followup_007(rec: dict) -> dict:
    """Al quitar modalidad se pierde también el tipo contractual Obra."""
    rec["params"]["contract_type_include"] = []
    rec["soql"] = "SELECT id_contrato WHERE departamento = 'Atlántico' LIMIT 50"
    return rec


def m_followup_016(rec: dict) -> dict:
    """Reordenar reintroduce compradores derivados en el universo."""
    rec["params"]["entidad_ecosystem"] = ["ALCALDIA DE CALI"]
    rec["soql"] = rec["soql"].replace(
        "WHERE ",
        "WHERE (unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%ALCALDIA DE CALI%')) "
        "OR unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%EMCALI%'))) AND ",
        1,
    )
    return rec


TURN_MUTATIONS = {
    ("HCA-ACTOR-004", 1): m_actor_004,
    ("HCA-TOPIC-007", 1): m_topic_007,
    ("HCA-ENTITY-001", 1): m_entity_001,
    ("HCA-FOLLOWUP-007", 2): m_followup_007,
    ("HCA-FOLLOWUP-016", 2): m_followup_016,
}

MUTATION_LABEL = {
    "HCA-ACTOR-004": "fuzzy candidate auto-authorizes identity",
    "HCA-TOPIC-007": "canonical school-feeding topic dropped",
    "HCA-ENTITY-001": "buyer reinterpreted as territory",
    "HCA-FOLLOWUP-007": "structured contract type Obra lost with modality",
    "HCA-FOLLOWUP-016": "derived buyers reappear on ordering-only turn",
    "HCA-META-001B": "both frames collapse onto the same universe dimension",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", required=True)
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    corpus = yaml.safe_load(CORPUS.read_text(encoding="utf-8"))
    cases = {str(c["id"]): c for c in corpus["cases"]}
    records: dict[tuple[str, int], dict] = {}
    for line in Path(args.results).read_text(encoding="utf-8").splitlines():
        r = json.loads(line)
        records[(r["case"], r["turn"])] = r

    report = []
    for (cid, turn_no), mutate in TURN_MUTATIONS.items():
        turn = cases[cid]["turns"][turn_no - 1]
        real = records[(cid, turn_no)]
        baseline = check_turn(turn, real)
        mutated = check_turn(turn, mutate(copy.deepcopy(real)))
        report.append({
            "case_id": cid,
            "turn": turn_no,
            "mutation": MUTATION_LABEL[cid],
            "real_fails": baseline,
            "mutated_fails": mutated,
            "nonvacuous": not baseline and bool(mutated),
        })

    # HCA-META-001B: la aserción es de caso (differ_from), no de turno.
    anti = cases["HCA-META-001B"]["differ_from"]
    frame_a = material_frame(records[("HCA-META-001A", 1)]["params"])
    frame_b = material_frame(records[("HCA-META-001B", 1)]["params"])
    collapsed = dict(frame_b)
    collapsed.pop("departamento_resolved", None)
    collapsed["entidad_resolved"] = frame_a.get("entidad_resolved")

    def anti_fails(frame: dict) -> list[str]:
        return [
            f"anti-metamorphic: field {f} identical to {anti['case']}"
            for f in anti["fields"] if frame_a.get(f) == frame.get(f)
        ]

    baseline = anti_fails(frame_b)
    mutated = anti_fails(collapsed)
    report.append({
        "case_id": "HCA-META-001B",
        "turn": None,
        "mutation": MUTATION_LABEL["HCA-META-001B"],
        "real_fails": baseline,
        "mutated_fails": mutated,
        "nonvacuous": not baseline and bool(mutated),
    })

    n_ok = sum(1 for r in report if r["nonvacuous"])
    out = {"patch_nonvacuous": f"{n_ok}/{len(report)}", "probes": report}
    text = json.dumps(out, ensure_ascii=False, indent=1)
    if args.json:
        Path(args.json).write_text(text + "\n", encoding="utf-8")
    print(text)
    if n_ok != len(report):
        sys.exit(1)


if __name__ == "__main__":
    main()
