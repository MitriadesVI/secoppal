"""La respuesta del perfil, compuesta desde el PLAN y la EVIDENCIA.

Tres bloques, y cada uno tiene una sola fuente de verdad:

    encabezado y alcance   el plan compilado (`frame_render`)
    hechos                 las secciones acreditadas (`frame_sections`)
    limitaciones           la evidencia y el plan, nunca una suposición

No hay fallback semántico: si un hecho no se acreditó no se dice, y si una
fuente no respondió se nombra la ausencia en vez de imprimir un cero. Un error
total sigue siendo respuesta del perfil —con su alcance intentado— y jamás
activa una degradación tabular como rescate.
"""
from __future__ import annotations

from app.core.entity_profile.failure_language import total_failure_text
from app.core.entity_profile.frame import EntityProfileFrame
from app.core.entity_profile.frame_compiler import ProfilePlan
from app.core.entity_profile.frame_render import (
    build_compiled_scope,
    render_compiled_scope,
)
from app.core.entity_profile.frame_runner import ProfileEvidence
from app.core.entity_profile.frame_sections import build_frame_sections
from app.core.entity_profile.frame_values import SourceKind
from app.core.entity_profile.models import (
    EntityProfile,
    EntityProfileIdentity,
    EntityProfileScope,
    ScopeType,
    SourceCalls,
)
from app.core.entity_profile.render import profile_fact_lines, _contract_noun


_TITLE_LABEL = {
    frozenset({SourceKind.PROCESSES}): "SECOP II Procesos",
    frozenset({SourceKind.CONTRACTS}): "SECOP II Contratos",
    frozenset({SourceKind.PROCESSES, SourceKind.CONTRACTS}): "actividad contractual",
}

_NEXT_STEPS = {
    frozenset({SourceKind.CONTRACTS}):
        "Puedes pedirme sus proveedores, valores o contratos recientes.",
    frozenset({SourceKind.PROCESSES}):
        "Puedes pedirme sus modalidades, evolución o procesos recientes.",
}
_NEXT_STEPS_BOTH = (
    "Puedes pedirme sus proveedores, modalidades, evolución o contratos recientes."
)


def build_profile_sections(
    frame: EntityProfileFrame, plan: ProfilePlan, evidence: ProfileEvidence,
    soql_builder,
) -> EntityProfile:
    """Un `EntityProfile` cuyas secciones vienen de Plan + Evidence.

    Se reutiliza el modelo existente porque el render de hechos ya está
    acreditado sobre él; lo que NO se reutiliza es su procedencia legacy: el
    `scope` que se construye aquí lleva identidad y nada más, sin fechas, tema
    ni modalidad que pudieran competir con el Frame.
    """
    from app.core.entity_profile.record_validation import validate_frame_plan

    validate_frame_plan(frame, plan)
    sections = build_frame_sections(plan, evidence, soql_builder)
    scope = EntityProfileScope(
        scope_type=(
            ScopeType.BUYER_GROUP if frame.buyer_scope.group_id
            else ScopeType.BUYER_UNIT
        ),
        buyer_units=frame.buyer_scope.canonical_buyers,
        buyer_group_id=frame.buyer_scope.group_id,
    )
    identity = EntityProfileIdentity(
        display_name=frame.buyer_scope.display_name,
        canonical_buyers=frame.buyer_scope.canonical_buyers,
        buyer_group_id=frame.buyer_scope.group_id,
        institution_refs=(),
        resolution_authority=frame.buyer_scope.authority,
    )
    warnings = tuple(dict.fromkeys(
        warning for section in sections for warning in section.warnings
    ))
    return EntityProfile(
        scope=scope, identity=identity,
        activity=sections[0], values=sections[1], modalities=sections[2],
        suppliers=sections[3], temporal=sections[4], recent=sections[5],
        source_calls=SourceCalls(calls=(), total_latency_ms=evidence.total_latency_ms),
        warnings=warnings, focus=plan.focus, plan=plan,
    )


def render_frame_profile(
    frame: EntityProfileFrame, plan: ProfilePlan, evidence: ProfileEvidence,
    soql_builder, *, profile: EntityProfile | None = None,
) -> str:
    profile = profile or build_profile_sections(frame, plan, evidence, soql_builder)
    scope = build_compiled_scope(plan)
    planned = frozenset(scope.planned_sources)
    blocks: list[str] = [
        f"{frame.buyer_scope.display_name.upper()} — "
        f"{_TITLE_LABEL.get(planned, 'sin fuentes consultadas')}",
    ]
    lines = [
        line for line in render_compiled_scope(scope)
        # La primera línea del alcance enumera las fuentes; el encabezado ya
        # las nombró.
        if not line.startswith("Fuentes: ")
    ]
    if lines:
        blocks.append("\n".join(lines))
    facts = profile_fact_lines(profile)
    if facts:
        blocks.append("\n".join(facts))
    if evidence.total_failure:
        # DOGFOOD-001 C: la causa se nombra —expiró, falló o no se emitió— en vez
        # de una sola frase para las tres. La cláusula que impide leerlo como un
        # cero viaja dentro del mismo texto.
        blocks.append(total_failure_text(plan, evidence))
    else:
        blocks.extend(profile.warnings)
    if plan.focus == "overview":
        blocks.append(_NEXT_STEPS.get(planned, _NEXT_STEPS_BOTH).replace('contratos', _contract_noun(profile) + 's'))
    response = "\n\n".join(blocks)
    return response


def render_scope_only(plan: ProfilePlan) -> tuple[str, ...]:
    """El alcance visible sin hechos; lo usa un fallo total y las pruebas."""
    return render_compiled_scope(build_compiled_scope(plan))


__all__ = [
    "build_profile_sections",
    "render_frame_profile",
    "render_scope_only",
]
