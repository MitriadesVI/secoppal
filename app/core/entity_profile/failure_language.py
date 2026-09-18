"""Por qué no hay cifras: expiró, falló o nunca se emitió. Nunca «no hay nada».

`_TOTAL_FAILURE` decía una sola frase para tres causas distintas, y la sección
decía «la fuente no respondió» incluso cuando la consulta no había llegado a
salir. Las tres se leen igual para quien pregunta, pero significan cosas
opuestas para quien opera:

    TIMEOUT           salió y SECOP no contestó dentro del presupuesto
    ERROR             salió y el transporte falló
    BUDGET_EXHAUSTED  no salió: al turno no le quedaba presupuesto útil

Un turno que expira en sus dos consultas no es un turno sin actividad. Este
módulo sólo nombra la causa; no decide presupuestos, no reintenta y no toca la
evidencia.
"""
from __future__ import annotations

from app.core.entity_profile.focus_labels import focus_label


# La cláusula que impide leer el silencio como un cero. Es la segunda frase del
# texto anterior y se conserva literal: era la parte correcta.
NOT_ZERO = (
    "No interpreto ese silencio como ausencia de actividad: el alcance de "
    "arriba es lo que se intentó consultar."
)

_COUNT_PHRASE = {
    1: "la consulta",
    2: "las dos consultas",
    3: "las tres consultas",
    4: "las cuatro consultas",
    5: "las cinco consultas",
    6: "las seis consultas",
}

_CAUSE_HEAD = {
    "TIMEOUT": "SECOP no respondió a tiempo en {consultas} sobre {foco}.",
    "ERROR": "SECOP falló al responder {consultas} sobre {foco}.",
    "BUDGET_EXHAUSTED": (
        "No alcancé a emitir {consultas} sobre {foco}: el turno se quedó sin "
        "presupuesto antes de consultar a SECOP."
    ),
    "UNAVAILABLE": "No obtuve respuesta de SECOP en {consultas} sobre {foco}.",
}

_MIXED = (
    "Ninguna de {consultas} sobre {foco} dejó resultado: {detalle}."
)

_CAUSE_DETAIL = {
    "TIMEOUT": ("una expiró", "{n} expiraron"),
    "ERROR": ("una falló", "{n} fallaron"),
    "BUDGET_EXHAUSTED": ("una no se emitió por falta de presupuesto",
                         "{n} no se emitieron por falta de presupuesto"),
    "UNAVAILABLE": ("una no dejó rastro", "{n} no dejaron rastro"),
}

# Lo que ya tenía voz propia en las secciones y no es un fallo de transporte.
_RESPONSE_DEFECTS = ("INCONSISTENT_RESPONSE", "INVALID_RESPONSE")


def count_phrase(count: int) -> str:
    return _COUNT_PHRASE.get(count, f"las {count} consultas")


def total_failure_text(plan, evidence) -> str:
    """El bloque de un fallo TOTAL, con su causa y sin convertirla en un cero."""
    statuses = _failed_statuses(evidence)
    foco = focus_label(plan.focus) if plan is not None else "este análisis"
    return f"{_headline(statuses, foco)} {NOT_ZERO}"


def failure_clause(statuses) -> str:
    """Cómo terminó un grupo de llamadas fallidas, en una cláusula reutilizable.

    Devuelve `""` cuando el conjunto no habla de transporte (una respuesta
    contradictoria o ilegible ya tiene su propia frase en las secciones).
    """
    counts = _counts(statuses)
    if not counts or any(status in _RESPONSE_DEFECTS for status in statuses):
        return ""
    if len(counts) == 1:
        cause = next(iter(counts))
        return {
            "TIMEOUT": "la fuente no respondió a tiempo",
            "ERROR": "la fuente falló al responder",
            "BUDGET_EXHAUSTED": "la consulta no se emitió por falta de presupuesto",
            "UNAVAILABLE": "la fuente no respondió",
        }[cause]
    return "la fuente no respondió"


def _headline(statuses: tuple[str, ...], foco: str) -> str:
    counts = _counts(statuses)
    total = sum(counts.values())
    consultas = count_phrase(total) if total else "las consultas"
    if not counts:
        return _CAUSE_HEAD["UNAVAILABLE"].format(consultas=consultas, foco=foco)
    if len(counts) == 1:
        cause = next(iter(counts))
        return _CAUSE_HEAD[cause].format(consultas=consultas, foco=foco)
    detalle = ", ".join(
        _CAUSE_DETAIL[cause][0] if number == 1
        else _CAUSE_DETAIL[cause][1].format(n=number)
        for cause, number in sorted(counts.items())
    )
    return _MIXED.format(consultas=consultas, foco=foco, detalle=detalle)


def _counts(statuses) -> dict[str, int]:
    counts: dict[str, int] = {}
    for status in statuses:
        cause = status if status in _CAUSE_DETAIL else "UNAVAILABLE"
        counts[cause] = counts.get(cause, 0) + 1
    return counts


def _failed_statuses(evidence) -> tuple[str, ...]:
    if evidence is None:
        return ()
    return tuple(
        call.status for call in evidence.calls if call.status != "SUCCESS"
    )


__all__ = [
    "NOT_ZERO",
    "count_phrase",
    "failure_clause",
    "total_failure_text",
]
