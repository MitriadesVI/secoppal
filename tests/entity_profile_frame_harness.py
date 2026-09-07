"""Adapter de OBSERVACIÓN de los contraejemplos sobre el core V1.2-A.

Los adversariales de la auditoría independiente observaban la API de V1.1
(`classify_entity_profile_turn`, `EntityProfileSemanticRequest`, params). El
diseño congelado reemplaza esas piezas, así que portar el contraejemplo
copiando su API convertiría en contrato lo que el diseño retira.

Este módulo traduce: recibe la MISMA entrada —texto real y los params que
produce ``QueryRouter``— y expone lo que el core nuevo acredita: claims,
Frame, decisión de compilación, SQL por fuente y alcance visible. Vive en
`tests/` a propósito: no es una capa de producción, y ninguna ruta de runtime
lo importa.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from app.core.entity_profile.claim_adapters import collect_claims
from app.core.entity_profile.claim_composition import compose_patch
from app.core.entity_profile.claims import ClaimSet
from app.core.entity_profile.frame import EntityProfileFrame
from app.core.entity_profile.frame_compiler import PlanStatus, compile_profile
from app.core.entity_profile.frame_render import (
    build_compiled_scope,
    render_compiled_scope,
)
from app.core.entity_profile.frame_values import SourceKind
from app.core.entity_profile.patch import apply_patch
from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_profile.turn import profile_query_params
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


TODAY = date(2026, 9, 7)
BUYER = "alcaldia de baranoa"
ROUTER = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


@dataclass(frozen=True, slots=True)
class Observation:
    """Lo observable de un turno: entrada, significado, plan y SQL."""

    query: str
    params: dict
    claims: ClaimSet
    frame: EntityProfileFrame | None
    decision: Any
    invalid_reason: str | None = None

    @property
    def status(self) -> str:
        if self.decision is None:
            return "INVALID_CONTEXT"
        return self.decision.status.value

    @property
    def ready(self) -> bool:
        return self.decision is not None and self.decision.status is PlanStatus.READY

    @property
    def clarifies(self) -> bool:
        return (
            self.decision is not None
            and self.decision.status is PlanStatus.NEEDS_CLARIFICATION
        )

    @property
    def jobs(self) -> tuple:
        plan = getattr(self.decision, "plan", None)
        return plan.jobs if plan is not None else ()

    @property
    def sources(self) -> frozenset[SourceKind]:
        if self.frame is None:
            return frozenset()
        return self.frame.source_selection.requested

    def predicates(self, source: SourceKind):
        return self.frame.source_predicates[source]

    def sql_for(self, source: SourceKind) -> tuple[str, ...]:
        return tuple(job.sql for job in self.jobs if job.source is source)

    @property
    def sql(self) -> tuple[str, ...]:
        return tuple(job.sql for job in self.jobs)

    @property
    def scope_lines(self) -> tuple[str, ...]:
        plan = getattr(self.decision, "plan", None)
        if plan is None:
            return ()
        return render_compiled_scope(build_compiled_scope(plan))

    @property
    def issues(self) -> tuple[str, ...]:
        if self.decision is None:
            return (self.invalid_reason or "INVALID_CONTEXT",)
        return tuple(issue.code for issue in self.decision.issues)


def parse(query: str, overrides: dict | None = None) -> dict:
    params = ROUTER.parse(query, today=TODAY).params
    params.update(overrides or {})
    return params


def observe(
    query: str,
    *,
    overrides: dict | None = None,
    base: EntityProfileFrame | None = None,
    turn_id: str = "t1",
) -> Observation:
    """Texto -> claims -> patch -> Frame -> plan compilado, sin runtime legacy."""
    params = parse(query, overrides)
    clean = profile_query_params(params)
    resolution = resolve_profile_scope(clean)
    claims = collect_claims(
        query, clean, turn_id=turn_id, buyer_resolution=resolution,
    )
    proposal = compose_patch(claims, base_frame=base)
    if proposal.patch is None:
        return Observation(query, params, claims, None, None, proposal.reason)
    outcome = apply_patch(base, proposal.patch)
    if outcome.frame is None:
        return Observation(query, params, claims, None, None, outcome.reason)
    decision = compile_profile(outcome.frame, soql_builder=SoQLBuilder())
    return Observation(query, params, claims, outcome.frame, decision)


def conversation(texts: tuple[str, ...]) -> tuple[Observation, ...]:
    """Turnos encadenados sobre el Frame CONFIRMADO, sin persistencia real."""
    observations: list[Observation] = []
    confirmed: EntityProfileFrame | None = None
    for index, text in enumerate(texts):
        observation = observe(text, base=confirmed, turn_id=f"t{index + 1}")
        observations.append(observation)
        if observation.ready:
            confirmed = observation.frame
    return tuple(observations)
