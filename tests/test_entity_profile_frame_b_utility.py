"""La puerta de UTILIDAD: lo soportado tiene que ejecutar y responder.

Las salidas flexibles del contraejemplo original —«ejecuta correctamente o
aclara»— siguen valiendo como puerta de seguridad, y por eso mismo un sistema
que aclarara SIEMPRE las pasaría todas. Este archivo cierra esa puerta: cada
caso de aquí está soportado, y aclararlo sería una regresión.

También se comprueba el presupuesto: ninguna consulta extra respecto del
catálogo, y tantas llamadas como jobs planificó el plan.
"""
from __future__ import annotations

import pytest

from app.core.entity_profile.frame_compiler import PROFILE_QUERY_BUDGET
from entity_profile_frame_b_harness import (  # noqa: F401 — fixture por nombre
    BUYER,
    Conversation,
    frame_v12,
)


# (etiqueta, consulta, jobs esperados)
_UTILITY = (
    ("buyer reconocido, BOTH por defecto", f"analiza {BUYER} en 2026", 6),
    ("foco overview explícito", f"analiza contratos de {BUYER} en 2026", 4),
    ("foco proveedores", f"analiza proveedores de {BUYER} en 2026", 2),
    ("foco montos", f"analiza montos de {BUYER} en 2026", 1),
    ("foco modalidades", f"analiza modalidades de {BUYER} en 2026", 1),
    ("foco recientes", f"analiza recientes de {BUYER} en 2026", 2),
    ("foco evolución", f"analiza evolucion de {BUYER}", 2),
    (
        "periodos distintos inequívocos",
        f"analiza procesos publicados en julio y contratos firmados en agosto de {BUYER}",
        6,
    ),
    ("AND temático", f"analiza contratos de mantenimiento y vias de {BUYER}", 4),
    ("OR temático", f"analiza contratos de mantenimiento o vias de {BUYER}", 4),
    (
        "exclusión de modalidad",
        f"analiza contratos excepto contratacion directa de {BUYER}",
        4,
    ),
)


@pytest.mark.parametrize("label,query,jobs", _UTILITY, ids=[c[0] for c in _UTILITY])
def test_a_supported_request_executes_and_answers(frame_v12, label, query, jobs):
    chat = Conversation(f"util-{abs(hash(label)) % 10**8}")
    turn = chat.say(query)
    assert not turn.clarifies, f"{label} regresó a «aclarar»: {turn.response}"
    assert turn.owns_profile
    assert turn.planned_jobs == jobs, label
    # Ni una llamada de más respecto del catálogo, ni una de menos que el plan.
    assert len(turn.calls) == jobs <= PROFILE_QUERY_BUDGET
    assert chat.profile_state().confirmed_frame is not None


def test_a_temporal_change_over_a_profile_executes(frame_v12):
    chat = Conversation("util-time")
    chat.say(f"analiza contratos firmados de {BUYER} en 2026")
    changed = chat.say("en 2025")
    assert not changed.clarifies
    assert changed.planned_jobs == 4
    for soql in changed.sql:
        assert "2025-01-01" in soql and "2026-01-01" not in soql


def test_a_pending_selection_executes_instead_of_asking_again(frame_v12):
    chat = Conversation("util-pending")
    chat.say(f"analiza procesos de {BUYER} en agosto")
    assert chat.say("proveedores").clarifies
    selected = chat.say("solo contratos")
    assert not selected.clarifies
    assert selected.planned_jobs == 2
    assert len(selected.calls) == 2


def test_clearing_the_period_executes_instead_of_asking(frame_v12):
    chat = Conversation("util-clear")
    chat.say(f"analiza contratos firmados de {BUYER} en agosto")
    cleared = chat.say("sin filtro de fecha")
    assert not cleared.clarifies
    assert cleared.planned_jobs == 4
    assert len(cleared.calls) == 4


def test_no_supported_case_spends_more_than_the_declared_budget(frame_v12):
    chat = Conversation("util-budget")
    for query in (
        f"analiza {BUYER} en 2026", "proveedores", "modalidades", "recientes",
        "evolucion", "montos",
    ):
        turn = chat.say(query)
        assert len(turn.calls) <= PROFILE_QUERY_BUDGET, query
        assert len(turn.calls) == turn.planned_jobs, query
