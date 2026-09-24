"""Todo test bajo ``tests/product_contract`` corre con la red prohibida por construcción.

No es opt-in: el guard es autouse y verifica el ledger al cerrar, así que un
intento atrapado por el runtime también pone el test en rojo. Un test ``live``
no puede vivir aquí: pertenece a canaries/dogfood, nunca a ``test-product``.
"""
from __future__ import annotations

import pytest

from tests.product_contract.network_guard import NetworkLedger, install


@pytest.fixture(autouse=True)
def network_ledger(monkeypatch, request):
    if request.node.get_closest_marker("live"):
        pytest.fail("`live` tests are forbidden under tests/product_contract (network is never allowed here)")
    # Burr trae la telemetría encendida por defecto; es salida real a la red.
    monkeypatch.setenv("BURR_TELEMETRY_ENABLED", "false")
    import burr.telemetry

    monkeypatch.setattr(burr.telemetry, "g_telemetry_enabled", False)
    ledger = install(monkeypatch, NetworkLedger())
    yield ledger
    ledger.verify()
