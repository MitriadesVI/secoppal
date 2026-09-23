"""Projection-only adapter for frozen Schema 2 audit snapshots.

Historical municipal gates compare exact source names. They do not infer a
BuyerUnit from the old identity_key, and production never accepts Schema 2.
"""

from __future__ import annotations

from typing import Any

from app.core.buyer_source_names import BuyerSourceNameRegistry, parse_registry


def parse_historical_projection(payload: dict[str, Any]) -> BuyerSourceNameRegistry:
    if payload.get("schema_version") == 3:
        return parse_registry(payload)
    if payload.get("schema_version") != 2:
        raise ValueError("historical projection adapter only accepts Schema 2 or 3")
    units = []
    for binding in payload["bindings"]:
        units.append({
            "canonical_name": binding["canonical_name"],
            "source_names": binding["sources"],
            "projection_evidence": {
                "observed_at": binding["observed_at"],
                "evidence": binding["evidence"],
                "observed_rows": binding.get("observed_rows", {}),
            },
            "fiscal_evidence": [],
        })
    return parse_registry({"schema_version": 3, "authority": payload.get("authority"),
                           "buyer_units": units})
