"""«Ver más» after a turn that a TEMPORAL error blocked has no page to continue (one responsibility).

A turn whose period could not be resolved (a window next to another period, an invalid date) is
persisted with the scope it understood and without a date, so that its answer can complete it.
Pagination reads the last turn's params as «the search whose next page is wanted»: after such a turn
it re-executed that scope without the period («contratos de la Alcaldía de Galapa en los últimos 3
meses de 2025» -> «ver más» -> every Galapa contract, any date), a silent execution the person never
asked for. That is the defect wave 1 introduced, and the only one this module owns.

The persisted signal is ``temporal_period_question.MARKER``: the temporal gate leaves it in the
frame of EVERY turn it blocks (any temporal error code). A clarification of another route (P15, an
ambiguous actor…) carries no marker and paginates as in the base: that is not this module's
problem (declared debt PAGINATION_AFTER_NON_TEMPORAL_CLARIFICATION).

Pagination refuses instead. The refusal is not persisted as a turn (the blocked frame stays the last
one: the question it asked is still open and a second «ver más» is refused too).

Pure: it reads the persisted ``Turn`` and the marker of ``temporal_period_question``.
"""
from __future__ import annotations

from typing import Any

from app.core.temporal_period_question import MARKER

MESSAGE = (
    "No hay una página que continuar: tu última consulta no se ejecutó porque necesito que me "
    "aclares el periodo. Respóndeme eso, o escribe la búsqueda completa, y después te muestro más."
)


def is_blocked(turn: Any) -> bool:
    """True when ``turn`` (a persisted conversation turn) was blocked by a temporal error."""
    params = getattr(turn, "parsed_params", None) or {}
    return MARKER in params
