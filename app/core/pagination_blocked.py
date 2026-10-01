"""«Ver más» after a turn that never executed has no page to continue (one responsibility).

A blocked turn (a period question, an invalid date, any clarification) is persisted with the scope
it understood and without a date, so that its answer can complete it. Pagination reads the last
turn's params as «the search whose next page is wanted»: after a blocked turn it re-executed that
scope without the period («contratos de la Alcaldía de Galapa en los últimos 3 meses de 2025» ->
«ver más» -> every Galapa contract, any date), a silent execution the person never asked for.

Pagination refuses instead. The refusal is not persisted as a turn (the blocked frame stays the last
one: the question it asked is still open and a second «ver más» is refused too).

Pure: it reads the persisted ``Turn`` and the marker of ``temporal_period_question``.
"""
from __future__ import annotations

from typing import Any

from app.core.outcome import Outcome
from app.core.temporal_period_question import MARKER

MESSAGE = (
    "No hay una página que continuar: tu última consulta no se ejecutó porque necesito que me "
    "aclares algo. Respóndeme eso, o escribe la búsqueda completa, y después te muestro más."
)


def is_blocked(turn: Any) -> bool:
    """True when ``turn`` (a persisted conversation turn) asked a question instead of executing."""
    params = getattr(turn, "parsed_params", None) or {}
    return MARKER in params or getattr(turn, "outcome", "") == Outcome.CLARIFICATION
