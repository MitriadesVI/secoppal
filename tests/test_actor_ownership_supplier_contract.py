"""Ownership de turno: un actor positivo acreditado no es asunto de A2.

Esta prueba pertenece a A2 SUPPLIERS, no al foundation de negación de actores.
Vive aquí para que se reaplique **sola** cuando A2 vuelva a estar encima de
esta rama: sin ``capability_binding`` en el árbol se salta, y con él corre sin
que nadie tenga que acordarse de restaurarla.

El foundation se prueba sin A2; A2 conserva su prueba de propiedad.
"""
from __future__ import annotations

import pytest

from app.core.query_router import QueryRouter

supplier_contract = pytest.importorskip(
    "app.core.capability_binding.supplier_contract",
    reason="A2 SUPPLIERS no está en este árbol; el foundation no depende de él",
)

router = QueryRouter()


class CountingSidecar:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, *_args, **_kwargs):
        self.calls += 1
        raise AssertionError("an actor-owned turn reached the A2 sidecar")


@pytest.mark.parametrize(
    "query",
    [
        "menos de 5 proveedores del proveedor NIT 900711074",
        "diferente de 5 proveedores del proveedor NIT 900711074",
        "distinto de 5 proveedores del proveedor NIT 900711074",
        "mas o menos 5 proveedores del proveedor NIT 900711074",
        "5 proveedores en adelante del proveedor NIT 900711074",
    ],
)
def test_free_cardinality_before_document_keeps_the_turn_out_of_a2(query: str) -> None:
    parsed = router.parse(query)
    sidecar = CountingSidecar()

    decision = supplier_contract.decide(
        query, parsed.params, polarity_veto=sidecar,
    )

    assert decision.outcome is supplier_contract.Outcome.NOT_BOUND
    assert decision.reason == "ACTOR_SUBJECT"
    assert sidecar.calls == 0
