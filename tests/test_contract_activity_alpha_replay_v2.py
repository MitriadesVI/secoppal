from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from app.config import Settings
from app.core.capability_binding.contract_activity_binding import ContractActivityBinding
from app.core.capability_binding.registry import SUPPLIERS, capability_for
from app.core.natural_alpha.models import GroundedJob, Observation
from app.core.natural_alpha.provider import ProviderResult
from tests.test_contract_activity_admission_v2 import _flow, _run


def _settings(**overrides) -> Settings:
    return Settings(_env_file=None, **overrides)


def test_contract_activity_flag_is_independent_from_suppliers_flag() -> None:
    observation = Observation(
        "alpha", "MATCHED", (GroundedJob("ANALYZE", "CONTRACT_COUNT", 0, 9),),
    )
    supplier_only = _settings(
        secoppal_a2_suppliers_binding_enabled=True,
        secoppal_contract_activity_binding_enabled=False,
    )
    contract_only = _settings(
        secoppal_a2_suppliers_binding_enabled=False,
        secoppal_contract_activity_binding_enabled=True,
    )
    assert capability_for(observation, supplier_only) is None
    binding = capability_for(observation, contract_only)
    assert isinstance(binding, ContractActivityBinding)


def test_shadow_and_multi_job_never_bind_contract_activity() -> None:
    job = GroundedJob("ANALYZE", "CONTRACT_COUNT", 0, 9)
    settings = _settings(secoppal_contract_activity_binding_enabled=True)
    assert capability_for(Observation("shadow", "MATCHED", (job,)), settings) is None
    assert capability_for(
        Observation("alpha", "MULTI_JOB_BLOCKED", (job, job)), settings,
    ) is None


def test_supplier_owner_is_unchanged_when_contract_activity_flag_is_on() -> None:
    supplier = Observation(
        "alpha", "MATCHED", (GroundedJob("ANALYZE", "SUPPLIERS", 0, 11),),
    )
    both = _settings(
        secoppal_contract_activity_binding_enabled=True,
        secoppal_a2_suppliers_binding_enabled=True,
    )
    assert capability_for(supplier, both) == SUPPLIERS


@pytest.mark.parametrize(
    ("raw", "action", "aspect", "quote"),
    [
        (
            "Lista los contratos de la Alcaldía de Galapa en 2025.",
            "SEARCH", "CONTRACTS", "Lista los contratos",
        ),
        (
            "¿Cuáles son los proveedores de la Alcaldía de Galapa?",
            "ANALYZE", "SUPPLIERS", "proveedores",
        ),
        (
            "¿Cuánto suman los contratos de la Alcaldía de Galapa?",
            "ANALYZE", "TOTAL_VALUE", "Cuánto suman los contratos",
        ),
    ],
)
def test_neighbor_jobs_never_make_contract_activity_source_calls(
    tmp_path: Path, raw: str, action: str, aspect: str, quote: str,
) -> None:
    payload = json.dumps({"jobs": [{
        "action": action,
        "aspect": aspect,
        "evidence_quote": quote,
        "occurrence": 1,
    }]}, ensure_ascii=False)
    flow = _flow(tmp_path, {"raw": raw, "payload": payload})

    result = _run(flow, raw)

    assert result.get("analytical_intent") != "contract_activity"
    assert result.get("route_reason") != "analytics_contract_activity"
    assert all(
        "count(distinct id_contrato)" not in soql and "HAVING count(*)" not in soql
        for _, _, soql in flow.secop_client.calls
    )


def test_flag_off_preserves_non_contract_activity_route(tmp_path: Path) -> None:
    raw = "¿Cuántos contratos firmó la Alcaldía de Galapa en 2025?"
    payload = json.dumps({"jobs": [{
        "action": "ANALYZE",
        "aspect": "CONTRACT_COUNT",
        "evidence_quote": "Cuántos contratos",
        "occurrence": 1,
    }]}, ensure_ascii=False)
    flow = _flow(tmp_path, {"raw": raw, "payload": payload}, enabled=False)

    result = _run(flow, raw)

    assert result.get("analytical_intent") != "contract_activity"
    assert result.get("route_reason") not in {
        "analytics_contract_activity", "analytics_contract_activity_clarify",
    }


def test_ineligible_identity_costs_zero_provider_calls_and_cannot_bind(tmp_path: Path) -> None:
    raw = "¿Cuántos contratos firmó la Alcaldía de Galapa en 2025?"
    record = {
        "raw": raw,
        "payload": json.dumps({"jobs": [{
            "action": "ANALYZE", "aspect": "CONTRACT_COUNT",
            "evidence_quote": "Cuántos contratos", "occurrence": 1,
        }]}, ensure_ascii=False),
    }
    flow = _flow(tmp_path, record)
    provider = Mock(observe=Mock(return_value=ProviderResult(record["payload"], provider_calls=1)))
    flow.natural_alpha_provider = provider

    result = flow.run_query(raw, channel="streamlit", user_identity={"email": "alpha@example.test"})

    provider.observe.assert_not_called()
    assert result.get("analytical_intent") != "contract_activity"
