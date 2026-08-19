"""La adquisición AP3-B es acotada, por lotes y agregada en servidor."""
from __future__ import annotations

import pytest

from app.data.actor_alias_source import acquire_actor_alias_source


class _FakeSocrata:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def get(self, dataset_id: str, **kwargs):
        self.calls.append((dataset_id, kwargs))
        if kwargs.get("order") == ":id":
            if "Cédula" in kwargs["where"]:
                return [
                    {"tipodocproveedor": "Cédula de Ciudadanía", "documento_proveedor": "1140835135"},
                    {"tipodocproveedor": "Cédula de Ciudadanía", "documento_proveedor": "1140835135"},
                ]
            return [
                {"tipodocproveedor": "NIT", "documento_proveedor": "900111222"},
                {"tipodocproveedor": "NIT", "documento_proveedor": "900111222"},
                {"tipodocproveedor": "NIT", "documento_proveedor": "900333444"},
            ]
        return []


def test_source_policy_never_requests_an_unbounded_or_raw_contract_scan():
    client = _FakeSocrata()
    snapshot = acquire_actor_alias_source(
        client,
        anchor_per_type=3,
        selected_per_type=1,
        batch_size=2,
        seed=7,
    )

    anchor_calls = [kwargs for _, kwargs in client.calls if kwargs.get("order") == ":id"]
    aggregate_calls = [kwargs for _, kwargs in client.calls if kwargs.get("group")]
    assert len(anchor_calls) == 2
    assert all(call["limit"] == 3 for call in anchor_calls)
    assert all(call["select"].startswith(":id,") for call in anchor_calls)
    assert aggregate_calls
    assert all(" IN (" in call["where"] for call in aggregate_calls)
    assert all("count(*) AS occurrences" in call["select"] for call in aggregate_calls)
    assert all(call["limit"] <= 10_000 for call in aggregate_calls)
    assert snapshot.acquisition["global_group_scan"] is False
    assert snapshot.acquisition["raw_contract_download"] is False
    assert snapshot.acquisition["targeted_batches"] == len(aggregate_calls)


@pytest.mark.parametrize(
    ("kwargs", "name"),
    [
        ({"anchor_per_type": 100_001}, "anchor_per_type"),
        ({"selected_per_type": 10_001}, "selected_per_type"),
        ({"batch_size": 101}, "batch_size"),
        ({"batch_size": 0}, "batch_size"),
    ],
)
def test_source_policy_enforces_hard_bounds_before_network(kwargs, name):
    client = _FakeSocrata()
    with pytest.raises(ValueError, match=name):
        acquire_actor_alias_source(client, **kwargs)
    assert client.calls == []
