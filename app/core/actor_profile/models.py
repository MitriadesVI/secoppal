"""Tipos del perfil contractual de un actor — AP2-C.

Qué representa ``ActorProfileResult``
------------------------------------
La **huella contractual observada** de un actor en SECOP II: lo que esa
fuente publicó sobre él, con la fecha en que se consultó y con lo que no
se pudo obtener declarado como tal. No es un dossier, no es una
verificación, y no emite juicios — INV-NO-VERDICT.

Tres decisiones que sostienen el tipo
-------------------------------------
**``queried_at`` es obligatorio y con zona.** AP0 lo llamó "el hallazgo
silencioso": ni ``Evidence``, ni ``SourceError``, ni ``Outcome`` llevan
fecha de consulta. Una afirmación sobre la actividad de un tercero sin la
hora en que se miró no se puede auditar ni desmentir después, porque la
fuente cambia todos los días.

**El estado por aspecto reusa ``outcome.py``.** Ese módulo ya separa el
vacío confirmado por la fuente del fallo de la fuente —*un fetch que falló
por timeout NO es EMPTY*— y esa es exactamente la distinción que un
perfil parcial necesita. Inventar una taxonomía paralela habría duplicado
la doctrina con otras palabras.

**Los nombres son observaciones, no razones sociales.** Un mismo NIT
acumula once escrituras distintas en la fuente, varias de ellas nombres de
uniones temporales que solo lo incluyen. ``ObservedName`` conserva el
documento y el tipo documental junto al nombre precisamente para que nadie
pueda tratarlo como identidad registral.

Puro: dataclasses y stdlib. La ejecución vive en ``assembler``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.core.outcome import Outcome, OutcomeReason
from app.core.actor_profile.scope_models import ActorProfileAppliedScope

__all__ = [
    "ActorProfileResult",
    "AspectStatus",
    "ObservedName",
]


@dataclass(frozen=True, slots=True)
class AspectStatus:
    """Qué pasó con UNA pieza del perfil.

    ``outcome``/``reason`` son los vocabularios de ``app.core.outcome``, sin
    traducción: ``EMPTY`` significa "la fuente confirmó que no hay",
    ``ERROR`` que no se pudo saber. Confundirlos es cómo un timeout se
    convierte en "este actor no tiene contratos".
    """

    name: str
    outcome: str = Outcome.SUCCESS
    reason: str = OutcomeReason.RESULTS_FOUND

    @property
    def is_available(self) -> bool:
        return self.outcome != Outcome.ERROR

    @property
    def failed(self) -> bool:
        return self.outcome == Outcome.ERROR


@dataclass(frozen=True, slots=True)
class ObservedName:
    """Un nombre tal como la fuente lo escribió, con su documento y su tipo.

    Se llama *observed* y no *legal* o *registered* a propósito: es la
    cadena que la entidad compradora tecleó en ese contrato. AP3 podrá
    reusarlos como alias de alta confianza — nunca como razón social.
    """

    name: str
    document: str
    document_type: str | None = None
    contract_count: int = 0


@dataclass(frozen=True, slots=True)
class ActorProfileResult:
    """La huella contractual observada de un actor, con sus límites a la vista."""

    queried_at: datetime
    identity: dict
    dataset_id: str
    contract_count: int | None = None
    total_reported_value: float | None = None
    first_observed_date: str | None = None
    last_observed_date: str | None = None
    buyers: tuple[tuple[str, int], ...] = ()
    years: tuple[tuple[str, int], ...] = ()
    modalities: tuple[tuple[str, int], ...] = ()
    territories: tuple[tuple[str, int], ...] = ()
    observed_names: tuple[ObservedName, ...] = ()
    recent_contracts: tuple[dict, ...] = ()
    aspects: tuple[AspectStatus, ...] = ()
    applied_scope: ActorProfileAppliedScope = field(default_factory=ActorProfileAppliedScope)

    def __post_init__(self) -> None:
        # INV-CUTOFF. Un datetime naive no dice cuándo se consultó: dice una
        # hora sin lugar, y sobre eso no se puede fechar una afirmación.
        if self.queried_at.tzinfo is None or self.queried_at.utcoffset() is None:
            raise ValueError("queried_at must be timezone-aware")

    @property
    def partial(self) -> bool:
        """True si alguna pieza no se pudo obtener."""
        return any(aspect.failed for aspect in self.aspects)

    @property
    def failed_aspects(self) -> tuple[str, ...]:
        return tuple(aspect.name for aspect in self.aspects if aspect.failed)

    @property
    def has_core_metrics(self) -> bool:
        """True si el conteo de contratos —el hecho central— está disponible."""
        return self.contract_count is not None

    @property
    def document_types_observed(self) -> tuple[str, ...]:
        """Tipos documentales distintos vistos bajo este documento.

        Más de uno significa que las filas pueden pertenecer a actores
        distintos que comparten número. El perfil lo declara; no elige uno
        ni los suma como si fueran el mismo (INV-ID-TYPE).
        """
        seen = [n.document_type for n in self.observed_names if n.document_type]
        return tuple(dict.fromkeys(seen))

    @property
    def has_mixed_document_types(self) -> bool:
        return len(self.document_types_observed) > 1
