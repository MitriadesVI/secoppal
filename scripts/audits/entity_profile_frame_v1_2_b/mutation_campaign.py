"""Campaña de mutaciones conductuales de V1.2-B (sección 15.2 del diseño).

Cada mutación reintroduce EXACTAMENTE un defecto que el trayecto nuevo cierra, y
debe morir por una aserción de comportamiento. Un `ImportError`, un fallo de
recolección o un timeout NO cuentan como muerte: por eso la clasificación
distingue DETECTADA de INCONCLUSA.

El archivo mutado se restaura siempre y su sha256 se compara antes y después.

Uso:

    .venv/bin/python scripts/audits/entity_profile_frame_v1_2_b/mutation_campaign.py

Sin red, sin app levantada y sin `Settings()` fuera de pytest: cada mutación se
evalúa ejecutando pruebas del carril en un subproceso.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[3]
# El intérprete es el que ejecuta este script: el venv canónico vive en el
# worktree principal y un worktree de carril no tiene el suyo.
PYTEST = [sys.executable, "-m", "pytest", "-q", "--tb=line"]

_INCONCLUSIVE_MARKERS = (
    "ImportError", "ModuleNotFoundError", "error during collection",
    "errors during collection", "INTERNALERROR", "Timeout",
)


@dataclass(frozen=True)
class Mutation:
    mutation_id: str
    thesis: str
    path: str
    old: str
    new: str
    kills: tuple[str, ...]


MUTATIONS = (
    Mutation(
        "M1-pending-without-selection",
        "Aplicar el parche pendiente aunque la entrada no elija su alternativa.",
        "app/core/entity_profile/pending_resolution.py",
        "        if all(offered.get(key) == value for key, value in changes.items()):",
        "        if True:",
        (
            "tests/test_entity_profile_frame_b_integration.py::"
            "test_r5d_a_pending_period_never_reaches_a_confirmed_execution",
        ),
    ),
    Mutation(
        "M2-old-cursor-after-corruption",
        "Continuar una paginación aunque la cola publicada esté corrupta.",
        "app/core/workflow/admission_rules.py",
        "    if envelope_status is EnvelopeStatus.CORRUPT and shape in _NEEDS_REFERENT:",
        "    if False and shape in _NEEDS_REFERENT:",
        (
            "tests/test_entity_profile_frame_b_publication.py::"
            "test_a_corrupt_queue_stops_a_pagination_instead_of_continuing",
        ),
    ),
    Mutation(
        "M3-seed-bypasses-admission",
        "Dejar que un seed tabular adquiera autoridad sobre un Frame de perfil.",
        "app/core/workflow/admission_rules.py",
        "    shape = InputShape.SEED_REFINEMENT\n    if owner is ContinuationOwner.ENTITY_PROFILE:",
        "    shape = InputShape.SEED_REFINEMENT\n    if False:",
        (
            "tests/test_entity_profile_frame_b_admission.py::"
            "test_a_ui_refinement_over_a_profile_runs_no_query",
        ),
    ),
    Mutation(
        "M4-subject-and-event-swapped",
        "Aceptar un hecho fechado que pertenece a la otra fuente.",
        "app/core/entity_profile/frame_compiler.py",
        "        if event.source is not None and event.source is not source:",
        "        if False:",
        (
            "tests/test_entity_profile_frame_b_semantics.py::"
            "test_an_incompatible_subject_and_event_clarifies_without_swapping_them",
        ),
    ),
    Mutation(
        "M5-or-flattened-into-and",
        "Presentar una alternativa temática como si fuera una conjunción.",
        "app/core/entity_profile/frame_render.py",
        '        joined = " o ".join(p for p in parts if p)',
        '        joined = " y ".join(p for p in parts if p)',
        (
            "tests/test_entity_profile_frame_b_semantics.py::"
            "test_or_and_and_are_not_rendered_or_queried_alike",
        ),
    ),
    Mutation(
        "M6-exclusion-dropped-from-scope",
        "Ejecutar la exclusión y no declararla en el alcance visible.",
        "app/core/entity_profile/frame_render.py",
        "    if selection.exclude:\n        excluded = _labels(selection.exclude) if labels else selection.exclude\n        parts.append(\"excepto \" + \", \".join(excluded))",
        "    if False:\n        excluded = _labels(selection.exclude) if labels else selection.exclude\n        parts.append(\"excepto \" + \", \".join(excluded))",
        (
            "tests/test_entity_profile_frame_b_semantics.py::"
            "test_a_modality_exclusion_stays_in_the_sql_and_is_declared",
        ),
    ),
    Mutation(
        "M7-legacy-reader-back-in-the-path",
        "Devolver el turno de perfil a la cadena legacy, que hereda params.",
        "app/core/orchestrator.py",
        "        elif admitted.route is AdmissionRoute.PROFILE:",
        "        elif False:",
        (
            "tests/test_entity_profile_frame_b_semantics.py::"
            "test_tampering_with_legacy_params_changes_neither_frame_nor_sql",
            "tests/test_entity_profile_frame_b_integration.py::"
            "test_r5a_inherited_period_is_in_every_query_after_a_real_reload",
        ),
    ),
    Mutation(
        "M8-transport-success-fabricated",
        "Convertir un fallo de transporte en un SUCCESS y, con él, en validez.",
        "app/core/entity_profile/frame_runner.py",
        '        rows, status = (), ("TIMEOUT" if _is_timeout(exc) else "ERROR")',
        '        rows, status = (), "SUCCESS"',
        (
            "tests/test_entity_profile_frame_b_semantics.py::"
            "test_a_total_source_failure_keeps_the_confirmed_intent",
        ),
    ),
    Mutation(
        "M9-nonfinite-metric-accredited",
        "Acreditar un monto no finito como si fuera una cifra observada.",
        "app/core/entity_profile/frame_evidence.py",
        "    if not amount.is_finite():",
        "    if False:",
        (
            "tests/test_entity_profile_frame_b_semantics.py::"
            "test_a_non_finite_metric_is_never_accredited",
        ),
    ),
    Mutation(
        "M10-month-globalised",
        "Dar a una fuente el periodo que otra cláusula reclamó para la otra.",
        "app/core/entity_profile/claim_composition.py",
        "        if owner is not None and owner.id in c.related(RelationKind.HAS_SUBJECT)",
        "        if True or owner.id in c.related(RelationKind.HAS_SUBJECT)",
        (
            "tests/test_entity_profile_frame_b_semantics.py::"
            "test_two_sources_with_different_periods_are_not_globalised",
        ),
    ),
    Mutation(
        "M11-injected-clock-ignored",
        "Resolver un periodo relativo con la fecha del sistema y no con el reloj inyectado.",
        "app/core/entity_profile/frame_turn.py",
        "        user_query, clean, turn_id=turn_id, buyer_resolution=resolution,\n        today=today,",
        "        user_query, clean, turn_id=turn_id, buyer_resolution=resolution,\n        today=None,",
        (
            "tests/test_entity_profile_frame_b_semantics.py::"
            "test_the_injected_clock_is_the_one_that_resolves_relative_periods",
        ),
    ),
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _run(targets: tuple[str, ...]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [*PYTEST, *targets], cwd=ROOT, capture_output=True, text=True, timeout=900,
    )


def _classify(process: subprocess.CompletedProcess) -> str:
    output = process.stdout + process.stderr
    if any(marker in output for marker in _INCONCLUSIVE_MARKERS):
        return "INCONCLUSIVE"
    if process.returncode == 0:
        return "SURVIVED"
    if "AssertionError" in output or "assert" in output or " failed" in output:
        return "DETECTED"
    return "INCONCLUSIVE"


def main() -> int:
    results = []
    baseline_targets = tuple(
        target for mutation in MUTATIONS for target in mutation.kills
    )
    baseline = _run(tuple(dict.fromkeys(baseline_targets)))
    if baseline.returncode != 0:
        print("BASELINE ROJO — la campaña no es interpretable:")
        print(baseline.stdout[-4000:])
        return 2
    print(f"baseline verde: {baseline.stdout.strip().splitlines()[-1]}\n")

    for mutation in MUTATIONS:
        path = ROOT / mutation.path
        before = _sha256(path)
        original = path.read_text(encoding="utf-8")
        if mutation.old not in original:
            results.append({
                "id": mutation.mutation_id, "verdict": "ANCHOR_NOT_FOUND",
                "path": mutation.path,
            })
            print(f"{mutation.mutation_id}: ANCHOR_NOT_FOUND")
            continue
        path.write_text(original.replace(mutation.old, mutation.new, 1), encoding="utf-8")
        try:
            process = _run(mutation.kills)
            verdict = _classify(process)
            tail = (process.stdout.strip().splitlines() or [""])[-1]
        finally:
            path.write_text(original, encoding="utf-8")
        after = _sha256(path)
        restored = before == after
        results.append({
            "id": mutation.mutation_id, "thesis": mutation.thesis,
            "path": mutation.path, "kills": list(mutation.kills),
            "verdict": verdict, "restored": restored,
            "sha256": before, "pytest_tail": tail,
        })
        print(f"{mutation.mutation_id}: {verdict} (restored={restored}) — {tail}")

    summary = {
        "detected": sum(r["verdict"] == "DETECTED" for r in results),
        "survived": sum(r["verdict"] == "SURVIVED" for r in results),
        "inconclusive": sum(r["verdict"] == "INCONCLUSIVE" for r in results),
        "anchor_not_found": sum(r["verdict"] == "ANCHOR_NOT_FOUND" for r in results),
        "total": len(results),
    }
    report = {"summary": summary, "mutations": results}
    out = ROOT / "docs/audits/entity_profile_frame_v1_2_b/mutation_campaign.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n{json.dumps(summary)}")
    print(f"reporte: {out.relative_to(ROOT)}")
    return 0 if summary["survived"] == 0 and summary["anchor_not_found"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
