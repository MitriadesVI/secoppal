"""scheduler.py — corre los radares activos según su ``frequency``.

Núcleo reusable del scheduler (sin systemd ni red propia): decide qué radares
"tocan" según su frecuencia y la última corrida, y los ejecuta vía el
``run_radar`` existente, aislando errores por radar (uno que falle no tumba al
resto, igual que el pull-to-refresh de la UI).

v1.1 **solo refresca el feed** — no notifica. Pero ``run_due_radars`` devuelve,
por cada radar con novedad, lo necesario (owner, digest, conteos) para que un
futuro ``notifier`` (WhatsApp/email a elección del usuario) se enchufe sin
refactor. El disparo real lo hace un timer de systemd (ver ``deploy/``) que
invoca ``scripts/run_radar_scheduler.py``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from app.core.radar import follows
from app.core.radar.runner import run_radar
from app.core.radar.store import Radar, RadarStore

if TYPE_CHECKING:
    from app.core.workflow.deps import WorkflowDeps

logger = logging.getLogger(__name__)

# Intervalos por frecuencia. 'manual' no está → nunca corre solo.
FREQUENCY_INTERVALS: dict[str, timedelta] = {
    "cada_6h": timedelta(hours=6),
    "diaria": timedelta(days=1),
    "semanal": timedelta(weeks=1),
}

# Holgura para que el jitter del timer no salte una corrida en el límite
# (un radar 'cada_6h' cuya última corrida fue hace 5h59m sí debe correr).
DUE_TOLERANCE = timedelta(minutes=10)


@dataclass
class RadarOutcome:
    """Resultado de evaluar/ejecutar un radar en una pasada del scheduler."""
    radar: Radar
    status: str            # "ran" | "skipped" | "error"
    new_count: int = 0
    changed_count: int = 0
    digest: dict | None = None
    error: str | None = None

    @property
    def owner(self) -> str:
        return self.radar.owner

    @property
    def has_novelty(self) -> bool:
        return bool(self.new_count or self.changed_count)


@dataclass
class SchedulerSummary:
    checked: int = 0
    ran: int = 0
    skipped: int = 0
    errors: int = 0
    new_total: int = 0
    changed_total: int = 0
    follows: int = 0
    outcomes: list[RadarOutcome] = field(default_factory=list)

    def as_line(self) -> str:
        return (
            f"radares revisados={self.checked} corridos={self.ran} "
            f"saltados={self.skipped} errores={self.errors} "
            f"nuevos={self.new_total} cambios={self.changed_total} "
            f"seguidos_refrescados={self.follows}"
        )


def _parse_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def is_due(radar: Radar, now: datetime | None = None) -> bool:
    """¿Le toca correr a este radar según su frecuencia y última corrida?

    'manual' (o frecuencia desconocida) nunca corre solo. Sin ``last_run_at``
    siempre toca (nunca se ha corrido). Si no, toca cuando el tiempo transcurrido
    alcanza el intervalo (menos la tolerancia).
    """
    interval = FREQUENCY_INTERVALS.get(radar.frequency)
    if interval is None:
        return False
    last = _parse_ts(radar.last_run_at)
    if last is None:
        return True
    now = now or datetime.now(timezone.utc)
    return (now - last) >= (interval - DUE_TOLERANCE)


def run_due_radars(
    deps: "WorkflowDeps",
    store: RadarStore,
    *,
    now: datetime | None = None,
    force: bool = False,
) -> SchedulerSummary:
    """Corre los radares activos que toquen (o todos si ``force``).

    Aísla el error de cada radar: uno que falle queda registrado y el scheduler
    sigue con los demás.
    """
    now = now or datetime.now(timezone.utc)
    summary = SchedulerSummary()

    for radar in store.list_all_enabled_radars():
        summary.checked += 1

        # Radar sintético de "procesos seguidos": no es una búsqueda, se refresca
        # siempre (chequea el estado actual de cada proceso seguido).
        if radar.frequency == follows.FOLLOWS_FREQUENCY:
            try:
                res = follows.refresh_follows(deps, store, radar.owner)
            except Exception as exc:
                logger.exception("radar scheduler: falló refresh_follows owner=%s",
                                 radar.owner)
                summary.errors += 1
                summary.outcomes.append(
                    RadarOutcome(radar=radar, status="error", error=str(exc))
                )
                continue
            changed = res["changed_state"] + res["changed_value"]
            summary.follows += 1
            summary.changed_total += changed
            summary.outcomes.append(
                RadarOutcome(radar=radar, status="follows", changed_count=changed)
            )
            continue

        if not force and not is_due(radar, now):
            summary.skipped += 1
            summary.outcomes.append(RadarOutcome(radar=radar, status="skipped"))
            continue
        try:
            result = run_radar(deps, store, radar)
        except Exception as exc:  # cinturón: un radar no debe tumbar la pasada
            logger.exception("radar scheduler: falló radar id=%s name=%s",
                             radar.id, radar.name)
            summary.errors += 1
            summary.outcomes.append(
                RadarOutcome(radar=radar, status="error", error=str(exc))
            )
            continue

        if result.status == "error":
            summary.errors += 1
            summary.outcomes.append(
                RadarOutcome(radar=radar, status="error", error=result.error)
            )
            continue

        new_count = result.diff.new_count if result.diff else 0
        changed_count = result.diff.changed_count if result.diff else 0
        summary.ran += 1
        summary.new_total += new_count
        summary.changed_total += changed_count
        summary.outcomes.append(
            RadarOutcome(
                radar=radar, status="ran", new_count=new_count,
                changed_count=changed_count, digest=result.digest,
            )
        )

    logger.info("radar scheduler: %s", summary.as_line())
    return summary
