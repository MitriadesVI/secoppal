"""Los adversariales que V1.2-A NO puede resolver: contrato preparado, sin E2E.

R5 y R7 exigen publicación versionada, recarga en otra instancia y admisión
superior antes del dispatcher. Nada de eso existe en A, y declarar aquí un
verde de comportamiento sería exactamente la clase de test que la revisión
independiente encontró: uno que verifica la FORMA del arreglo, no su contrato.

Lo que sí se congela ahora es el esquema que B necesitará: un Frame viaja
íntegro por serialización, y un plan se ata a UNA revisión concreta.
"""
from __future__ import annotations

import json

import pytest

from app.core.entity_profile.frame import EntityProfileFrame
from app.core.entity_profile.frame_compiler import PlanStatus, compile_profile
from app.core.entity_profile.frame_values import SourceKind
from app.core.soql_builder import SoQLBuilder
from entity_profile_frame_harness import BUYER, observe


INTEGRATION_B_REQUIRED = (
    "R5a displayed inherited period after a real reload",
    "R5b explicit completeness after a real reload",
    "R5c pending selection across turns",
    "R5d pending period does not leak into confirmed execution",
    "R6 explicit origin survives persistence",
    "R7a profile does not seed tabular pagination",
    "R7b CLEAR survives another reload",
    "R7c a tabular cursor is inert after a profile",
)


def _frame():
    out = observe(f"analiza contratos firmados de {BUYER} en agosto")
    assert out.ready, out.issues
    return out.frame


def test_frame_survives_json_serialization_intact():
    frame = _frame()
    revived = EntityProfileFrame.from_dict(json.loads(json.dumps(frame.to_dict())))
    assert revived.semantic_equal(frame)


def test_reloaded_frame_compiles_to_the_same_predicates():
    frame = _frame()
    revived = EntityProfileFrame.from_dict(json.loads(json.dumps(frame.to_dict())))
    original = compile_profile(frame, soql_builder=SoQLBuilder())
    reloaded = compile_profile(revived, soql_builder=SoQLBuilder())
    assert original.status is PlanStatus.READY and reloaded.status is PlanStatus.READY
    assert (
        original.plan.source_plans[SourceKind.CONTRACTS].fingerprint
        == reloaded.plan.source_plans[SourceKind.CONTRACTS].fingerprint
    )


def test_a_relative_period_does_not_get_reinterpreted_on_reload():
    frame = _frame()
    period = frame.source_predicates[SourceKind.CONTRACTS].temporal.period
    assert period.civil_reference == "2026-09-07"
    revived = EntityProfileFrame.from_dict(json.loads(json.dumps(frame.to_dict())))
    assert revived.source_predicates[SourceKind.CONTRACTS].temporal.period == period


def test_a_plan_names_the_exact_frame_revision_it_compiled():
    frame = _frame()
    decision = compile_profile(frame, soql_builder=SoQLBuilder())
    assert decision.plan.frame_ref == (frame.frame_id, frame.revision)


@pytest.mark.parametrize("case", INTEGRATION_B_REQUIRED)
def test_integration_b_cases_are_declared_not_claimed(case):
    """Estos casos NO se declaran resueltos en A; su puerta es V1.2-B."""
    assert case in INTEGRATION_B_REQUIRED
