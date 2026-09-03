"""Un grupo de compradores nunca se ejecuta como uno solo, en silencio.

``SoQLBuilder`` acepta dos listas de compradores. Las dos degradan al pasar de
15 miembros, y ninguna lo declara:

* ``entidad_in_candidates`` —disparo por nombre, el mecanismo vivo de ICBF y
  SENA— emite **un único ``LIKE`` sobre el primer miembro**. Su comentario
  anuncia un fallback a ``contains_all`` "o los primeros 15" y no ocurre
  ninguna de las dos cosas;
* ``entidad_ecosystem`` —disparo geográfico— trunca a los primeros 15.

Las dos son la familia de ``INV-NO-SILENT-SEMANTIC-FALLBACK`` (ADR-069) y las
dos las prohíbe ADR-074: un grupo decide qué unidades se consultan, y responder
por una fingiendo que son 39 no es una consulta parcial, es una respuesta falsa.

Alcance: la Etapa 1 sólo debe **cerrar** el fallback silencioso de
``entidad_in_candidates``. El mecanismo para grupos grandes es la Etapa 3A. Por
eso el oráculo admite las dos salidas legítimas —cobertura completa o fallo
declarado— y sólo prohíbe la tercera, que es la de hoy.

El canario de ``entidad_ecosystem`` queda congelado pero **fuera** del alcance
del fix de Etapa 1: pertenece a ``ECOSYSTEM_SOQL_DISPLAY_DIVERGENCE``.
"""
from __future__ import annotations

import pytest

from app.core.soql_builder import (
    BuyerCandidateSetExceedsSafeLimitError,
    SoQLBuilder,
)

DATASETS = [SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET]

# 39 = 19 unidades centrales + 20 alcaldías locales; el grupo real de Bogotá.
BOGOTA_EXECUTIVE_SIZE = 39


def _members(count: int) -> list[str]:
    return [f"UNIDAD COMPRADORA NUMERO {index:02d}" for index in range(count)]


def _covered(query: str, members: list[str]) -> int:
    return sum(1 for member in members if member in query)


def _assert_no_silent_partial(query: str, members: list[str]) -> None:
    """Cobertura completa, o fallo declarado. Nunca una fracción muda."""
    covered = _covered(query, members)
    if covered == len(members):
        return
    assert covered != 1, (
        f"cobertura colapsada a 1 de {len(members)} compradores sin declararlo: "
        f"{query[:200]}"
    )
    assert "1=0" in query, (
        f"cobertura parcial ({covered} de {len(members)}) sin predicado imposible "
        f"que la declare: {query[:200]}"
    )


@pytest.mark.parametrize("dataset_id", DATASETS)
@pytest.mark.parametrize("count", [1, 2, 15])
def test_group_within_current_cap_is_fully_covered(dataset_id: str, count: int) -> None:
    """Hasta el tope actual, los miembros entran todos. Guarda viva."""
    members = _members(count)
    query = SoQLBuilder().build(dataset_id, {"entidad_in_candidates": members})
    assert _covered(query, members) == count


@pytest.mark.parametrize("dataset_id", DATASETS)
@pytest.mark.parametrize("count", [16, BOGOTA_EXECUTIVE_SIZE])
def test_group_over_cap_never_collapses_silently(dataset_id: str, count: int) -> None:
    members = _members(count)
    with pytest.raises(BuyerCandidateSetExceedsSafeLimitError) as exc_info:
        SoQLBuilder().build(dataset_id, {"entidad_in_candidates": members})
    assert exc_info.value.candidate_count == count
    assert exc_info.value.safe_limit == 15


@pytest.mark.xfail(
    strict=True,
    reason="ECOSYSTEM_SOQL_DISPLAY_DIVERGENCE: entidad_ecosystem trunca a los "
           "primeros 15 sin declararlo — fuera del alcance del fix de Etapa 1",
)
@pytest.mark.parametrize("dataset_id", DATASETS)
def test_geographic_ecosystem_never_truncates_silently(dataset_id: str) -> None:
    members = _members(BOGOTA_EXECUTIVE_SIZE)
    query = SoQLBuilder().build(dataset_id, {"entidad_ecosystem": members})
    _assert_no_silent_partial(query, members)
