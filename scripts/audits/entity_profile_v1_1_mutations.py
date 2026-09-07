"""Campaña de mutación acotada sobre las propiedades P13–P23.

Una propiedad que pasa no vale nada si también pasaría con el defecto puesto.
Cada mutación reintroduce EXACTAMENTE uno de los siete P1 de la revisión
independiente y comprueba que la propiedad correspondiente se pone roja.

Las mutaciones se aplican SÓLO en memoria, en un proceso pytest separado por
mutación; las fuentes del repositorio nunca se tocan.

Uso:  .venv/bin/python scripts/audits/entity_profile_v1_1_mutations.py
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[2]
PROPERTIES = "tests/test_entity_profile_remediation_properties.py"

# (id, P1 que reintroduce, código del plugin, test que DEBE morir)
MUTATIONS: list[tuple[str, str, str, str]] = [
    (
        "collapse_temporal_claims", "F1",
        """
import app.core.entity_profile.temporal_event as te
_original = te.resolve_temporal_events
def _mutated(user_query, params):
    resolution = _original(user_query, params)
    # Defecto V1: un escalar, la primera reclamación gana.
    return te.TemporalResolution(
        claims=resolution.claims[:1],
        unsupported_terms=resolution.unsupported_terms,
        current_state_spans=resolution.current_state_spans,
    )
te.resolve_temporal_events = _mutated
import app.core.entity_profile.semantic_request as sr
sr.resolve_temporal_events = _mutated
""",
        "test_p14_two_temporal_claims_never_collapse_silently",
    ),
    (
        "global_current_marker", "F2",
        """
import app.core.entity_profile.temporal_event as te
_original = te.resolve_temporal_events
def _mutated(user_query, params):
    from app.utils.money import normalize_text
    resolution = _original(user_query, params)
    # Defecto V1: el marcador se busca en TODA la frase.
    if te._CURRENT_MARKER.search(normalize_text(user_query)):
        return te.TemporalResolution(
            claims=resolution.claims, unsupported_terms=(),
            current_state_spans=resolution.current_state_spans,
        )
    return resolution
te.resolve_temporal_events = _mutated
import app.core.entity_profile.semantic_request as sr
sr.resolve_temporal_events = _mutated
""",
        "test_p15_a_current_marker_cannot_erase_another_clauses_event",
    ),
    (
        "source_word_anywhere", "F3",
        """
import app.core.entity_profile.source_request as sq
def _mutated(text):
    # Defecto V1: presencia en cualquier parte del texto.
    return bool(sq._PROCESS_WORDS.search(text)), bool(sq._CONTRACT_WORDS.search(text))
sq._clause_head_sources = _mutated
""",
        "test_p16_a_source_word_inside_the_topic_creates_no_source",
    ),
    (
        "hide_material_filters", "F4",
        """
import app.core.entity_profile.effective_scope as es
_original = es.scope_filter_lines
def _mutated(snapshot):
    # Defecto V1: el alcance sólo declaraba el tema, aplanado.
    return tuple(
        line for line in _original(snapshot) if line.startswith("Tema:")
    )
es.scope_filter_lines = _mutated
import app.core.entity_profile.render as rd
rd.scope_filter_lines = _mutated
""",
        "test_p17_every_material_filter_in_sql_is_declared_in_the_scope",
    ),
    (
        "flatten_topic_operators", "F4",
        """
import app.core.entity_profile.material_filters as mf
def _mutated(self):
    # Defecto V1: AND y OR se declaraban igual.
    terms = [*self.all_terms, *self.any_terms, *self.phrases]
    return ", ".join(terms) if terms else None
mf.TopicExpression.describe = _mutated
""",
        "test_p18_conjunction_and_disjunction_never_share_a_declaration",
    ),
    (
        "drop_execution_ownership", "F5",
        """
import app.core.workflow.result_ownership as ro
_mutated = lambda result=None, state=None: False
ro.owns_execution_and_presentation = _mutated
import app.core.workflow.dual_dataset as dd
dd.owns_execution_and_presentation = _mutated
import app.core.workflow.handlers.normal_flow as nf
nf.owns_execution_and_presentation = _mutated
""",
        "test_p19_observed_calls_exactly_equal_planned_calls",
    ),
    (
        "clarification_overwrites_confirmed", "F6",
        """
import app.core.entity_profile.conversation_state as cs
# Defecto V1: la lectura que provocó la aclaración se guardaba como vigente.
cs.EntityProfileConversationState.hold = (
    lambda self, request, question: cs.EntityProfileConversationState(
        confirmed=request, pending=request, pending_question=question,
    )
)
""",
        "test_p20_a_pending_clarification_cannot_mutate_the_confirmed_frame",
    ),
    (
        "temporal_followup_resets_focus", "F6",
        """
import app.core.entity_profile.turn as tn
# Defecto V1: un cambio de periodo volvía a overview.
tn._current_focus = lambda params: "overview"
""",
        "test_p21_a_temporal_followup_preserves_focus_event_and_topic",
    ),
    (
        "truncating_count_parser", "F7",
        """
import app.core.entity_profile.aspects.common as common
def _mutated(value):
    # Defecto V1/BASE: int(float(value)) trunca 0.5 -> 0 y admite -1.
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
common.as_optional_int = _mutated
import app.core.entity_profile.aspects.activity as activity
activity.as_optional_int = _mutated
import app.core.entity_profile.aspects.temporal as temporal
temporal.as_optional_int = _mutated
""",
        "test_p22_an_invalid_count_never_becomes_an_integer_fact",
    ),
    (
        "asymmetric_contradiction", "F7",
        """
import app.core.entity_profile.evidence as ev
# Defecto V1/BASE: sólo contratos se reconciliaba.
ev._CONTRADICTED_BY = {
    "contract_values": ("recent_contracts", "supplier_count", "supplier_value"),
    "process_activity": (),
}
""",
        "test_p23_a_zero_denied_by_same_scope_evidence_is_inconsistent",
    ),
    (
        "partial_explicit_execution", "F1",
        """
import app.core.entity_profile.execution_plan as ep
import app.core.entity_profile.source_request as sq
_original = ep.build_execution_plan
def _mutated(request, params):
    # Defecto V1: la procedencia no cambiaba nada; siempre se acotaba.
    from dataclasses import replace
    return _original(
        replace(request, sources_provenance=sq.SourceProvenance.DEFAULT), params,
    )
ep.build_execution_plan = _mutated
import app.core.entity_profile.turn as tn
tn.build_execution_plan = _mutated
""",
        "test_p13_an_explicit_source_is_answered_or_asked_about",
    ),
]


def _run(mutation: str, target: str) -> tuple[bool, bool]:
    """Devuelve (muere_con_mutación, pasa_sin_mutación)."""
    with tempfile.TemporaryDirectory() as tmp:
        plugin = Path(tmp) / "mutation_plugin.py"
        plugin.write_text(mutation)
        mutated = subprocess.run(
            [sys.executable, "-m", "pytest", f"{PROPERTIES}::{target}",
             "-p", "mutation_plugin", "-q", "--no-header"],
            cwd=ROOT, capture_output=True, text=True,
            env={**_env(), "PYTHONPATH": f"{tmp}:{ROOT}:{ROOT / 'tests'}"},
        )
    control = subprocess.run(
        [sys.executable, "-m", "pytest", f"{PROPERTIES}::{target}", "-q", "--no-header"],
        cwd=ROOT, capture_output=True, text=True, env=_env(),
    )
    return mutated.returncode != 0, control.returncode == 0


def _env() -> dict:
    import os

    return {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}


def main() -> int:
    results = []
    for name, finding, mutation, target in MUTATIONS:
        killed, control_green = _run(mutation, target)
        results.append({
            "mutation": name, "reintroduces": finding, "target": target,
            "killed": killed, "control_green": control_green,
        })
        status = "KILLED" if killed and control_green else "SURVIVED"
        print(f"{status:9} {name:38} {finding}  ->  {target}")
    killed = sum(r["killed"] and r["control_green"] for r in results)
    print(f"\n{killed}/{len(results)} mutaciones detectadas")
    Path(ROOT / "docs/audits/entity_profile_semantic_v1_1/mutations.json").write_text(
        json.dumps(results, indent=2) + "\n",
    )
    return 0 if killed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
