"""C0 freeze: assemble the DEV corpus, the frozen prompt copy, and the manifest.

Provenance rules:
- the 38 NJC V1 cases are copied verbatim from the evidence branch and the copy
  is refused unless its sha256 matches the sha recorded in the V1 manifest;
- the V2 cases are appended, never edited into the V1 block;
- the frozen prompt is a byte copy of app/core/natural_alpha/prompt.py;
- every hash written to manifest.json is computed here, never transcribed.

Run: python experiments/natural_job_prelegacy_v2/freeze.py
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXPERIMENT = Path(__file__).resolve().parent
V1_REF = "mo/natural-job-credential-v1"
V1_DIR = "experiments/natural_job_credential_v1"
PROMPT_RUNTIME = ROOT / "app" / "core" / "natural_alpha" / "prompt.py"

V2_CASES = [
    {
        "case_id": "X01",
        "family": "incident_2026",
        "raw_text": "con quien ha contratado la alcaldia de galapa en 2025",
        "owner": "SUPPLIERS",
        "outcome": "EXECUTE",
        "buyer": "galapa",
        "period": ["2025-01-01", "2025-12-31"],
        "forbid": ["TOPIC_FILTER", "ACTOR_SUBJECT", "BUYER_MENTION_SWALLOWS_QUESTION", "SILENT_PERIOD_LOSS"],
        "source": "incident_2026",
        "note": "Incidente real (plan V2 4.1/25): parafrasis SUPPLIERS con comprador y periodo. "
                "El incidente de Cartagena 2026 ya vive en el corpus DEV como U01.",
    },
    {
        "case_id": "X02",
        "family": "incident_2026",
        "raw_text": "cuales son los proveedores con contratos mas grandes de alcaldia de barranquilla",
        "owner": "SUPPLIERS",
        "outcome": "EXECUTE",
        "buyer": "barranquilla",
        "period": None,
        "ranking": "value",
        "forbid": ["TOPIC_FILTER", "ACTOR_SUBJECT"],
        "source": "incident_2026",
        "note": "Incidente real (plan V2 25): proveedores de Barranquilla con ranking por valor. "
                "El ranking se mide como dato informativo del harness, no como gate duro.",
    },
    {
        "case_id": "X03",
        "family": "incident_2026",
        "raw_text": "proveedores de educación de Secretaría de Educación de Cali",
        "owner": "SUPPLIERS",
        "outcome": "HONEST_CLARIFY",
        "buyer": "secretaría de educación de cali",
        "period": None,
        "forbid": ["SILENT_FILTER_LOSS"],
        "source": "incident_2026",
        "note": "Control del plan V2 25: filtro material (educación) que la vista de proveedores "
                "no soporta; el comprador sí es resoluble, así que aclarar, nunca ejecutar sin el filtro.",
    },
    {
        "case_id": "X04",
        "family": "multi_job",
        "raw_text": "dame los proveedores de galapa y las modalidades de contratación que usa",
        "owner": "MULTI_JOB",
        "outcome": "SAFE_NON_EXECUTION",
        "buyer": None,
        "period": None,
        "forbid": ["WRONG_CAPABILITY_EXECUTED"],
        "source": "incident_2026",
        "note": "Control del plan V2 25: multi-job debe resolverse en forma segura (bloqueo honesto "
                "o aclaración). Ejecutar cualquiera de los dos trabajos como si fuera el pedido completo "
                "es exactamente WRONG_CAPABILITY_EXECUTED.",
    },
]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _v1_file(relative: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{V1_REF}:{V1_DIR}/{relative}"],
        cwd=ROOT, capture_output=True, check=True,
    )
    return result.stdout


def main() -> None:
    v1_manifest = json.loads(_v1_file("manifest.json"))
    v1_corpus_bytes = _v1_file("corpus.json")
    v1_sha = _sha256(v1_corpus_bytes)
    if v1_sha != v1_manifest["corpus_sha256"]:
        raise SystemExit(f"V1 corpus sha mismatch: {v1_sha} != {v1_manifest['corpus_sha256']}")
    v1_corpus = json.loads(v1_corpus_bytes)
    if len(v1_corpus) != v1_manifest["corpus_cases"]:
        raise SystemExit(f"V1 case count mismatch: {len(v1_corpus)} != {v1_manifest['corpus_cases']}")

    existing = {case["case_id"] for case in v1_corpus}
    collisions = [case["case_id"] for case in V2_CASES if case["case_id"] in existing]
    if collisions:
        raise SystemExit(f"V2 case ids collide with the preserved corpus: {collisions}")

    corpus = v1_corpus + V2_CASES
    corpus_bytes = (json.dumps(corpus, ensure_ascii=False, indent=1) + "\n").encode("utf-8")
    (EXPERIMENT / "corpus_dev.json").write_bytes(corpus_bytes)

    prompt_bytes = PROMPT_RUNTIME.read_bytes()
    (EXPERIMENT / "prompt_v3.frozen.txt").write_bytes(prompt_bytes)

    sys.path.insert(0, str(ROOT))
    from app.core.natural_alpha.prompt import instructions  # noqa: PLC0415
    instructions_sha = _sha256(instructions().encode("utf-8"))

    base_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, check=True, text=True,
    ).stdout.strip()
    plan_path = EXPERIMENT.parents[1] / "docs" / "planes" / (
        "2026-09-16_natural_job_prelegacy_v2_implementation_plan.md")
    plan_sha = _sha256(plan_path.read_bytes())

    manifest = {
        "mission": "NATURAL_JOB_PRELEGACY_V2 — PRE-LEGACY CONTROL PLANE",
        "frozen_at": "2026-09-16",
        "plan": {"path": str(plan_path.relative_to(ROOT)), "sha256": plan_sha},
        "base_commit": base_commit,
        "evidence_branch": {
            "name": V1_REF,
            "sha": "411a03d042865f824f4d6ab206660bef949d7f47",
            "role": "evidencia a preservar; no se mergea",
        },
        "corpus_dev": {
            "path": "experiments/natural_job_prelegacy_v2/corpus_dev.json",
            "sha256": _sha256(corpus_bytes),
            "cases": len(corpus),
            "njc_v1_cases": len(v1_corpus),
            "njc_v1_corpus_sha256": v1_sha,
            "v2_cases": [case["case_id"] for case in V2_CASES],
            "provenance": "38 casos NJC V1 copiados sin edición + 4 controles del plan V2 (25). "
                          "No es holdout.",
        },
        "alpha_prompt_version": "V3",
        "alpha_prompt_file": "app/core/natural_alpha/prompt.py",
        "alpha_prompt_file_sha256": _sha256(prompt_bytes),
        "alpha_prompt_instructions_sha256": instructions_sha,
        "alpha_prompt_frozen_copy": "experiments/natural_job_prelegacy_v2/prompt_v3.frozen.txt",
        "alpha_prompt_delta":
            "V3 = V2 (definiciones conceptuales SUPPLIERS/TOP_ENTITIES) + (a) el fragmento de evidencia "
            "mínimo que demuestra el trabajo y no absorbe comprador/fecha/monto/territorio/tema, "
            "(b) 'el qué contrató una entidad es SEARCH, nunca SUPPLIERS'. Definiciones tomadas del "
            "plan V2 4.2; congeladas antes de cualquier replay V3 y no se reajustan mirando resultados.",
        "alpha_provider_frozen": {
            "model": "deepseek-v4-flash",
            "temperature": 0,
            "reasoning_effort": "none",
            "tools": [],
            "max_output_tokens": 1024,
            "response_format": {"type": "json_object"},
            "schema": "jobs[{action, aspect, evidence_quote, occurrence}] (estricto, sin claves extra)",
        },
        "hard_gates": {
            "WRONG_CAPABILITY_EXECUTED": 0,
            "WRONG_BUYER": 0,
            "SILENT_FILTER_LOSS": 0,
            "ACTOR_STOLEN": 0,
            "NEGATED_JOB_EXECUTED": 0,
            "UNSUPPORTED_CARDINALITY_EXECUTED": 0,
            "LEGACY_FALLBACK_AFTER_VALID_CREDENTIAL": 0,
            "QUERY_ROUTER_CALLS_ON_CREDENTIALED_TURNS": 0,
            "LEGACY_GRAPH_ENTERED_WITH_VALID_CREDENTIAL": 0,
        },
        "product_metrics": {
            "CLEAR_SUPPLIERS_EXECUTION_RATE": ">= 0.90 sobre los casos SUPPLIERS claros y soportados",
            "secondary": ["HONEST_CLARIFY_RATE", "WRONG_EXECUTION_RATE"],
            "note": "EXECUTE + CLARIFY no es titular; reportar n exacto (plan V2 23).",
        },
        "stop_conditions": [
            "copiar o reimplementar gran parte de QueryRouter",
            "whitelist de verbos SUPPLIERS",
            "tercer runtime de perfil",
            "el LLM emite parámetros ejecutables",
            "Topic Evidence general nuevo",
            "otro sidecar para el happy path",
            "reglas por paráfrasis en vez de authorities genéricas",
            "duplicar Frame V1.2 en vez de adaptarlo",
        ],
        "abandonment_rule":
            "Última prueba seria del enfoque. Con credentials correctas, CONTROL_PLANE hard gates != 0 o "
            "CLEAR_SUPPLIERS_EXECUTION_RATE < 90% por defectos del control plane/residual/authorities "
            "produce NO_GO_ARCHITECTURE y se abandona esta vía.",
        "prohibited_remedy":
            "Prohibido arreglar una paráfrasis añadiendo vocabulario al host; prohibido enumerar "
            "vende/compra/adjudica/trabaja/gana en una whitelist. Si aparecen, STOP.",
    }
    (EXPERIMENT / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

    sys.stdout.write(json.dumps({
        "corpus_cases": len(corpus),
        "corpus_sha256": manifest["corpus_dev"]["sha256"],
        "prompt_file_sha256": manifest["alpha_prompt_file_sha256"],
        "prompt_instructions_sha256": instructions_sha,
        "base_commit": base_commit,
    }, indent=1) + "\n")


if __name__ == "__main__":
    main()
