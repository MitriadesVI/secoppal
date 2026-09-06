"""Bounded, replayable official-source observations; never runtime transport."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/audits/relationship_foundation_v0"
DATASETS = ("p6dx-8zbt", "jbjy-vk9h", "hgi6-6wh3", "ceth-n4bn")


def render(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(render(value))


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def in_values(field, values):
    if not values:
        raise ValueError("empty scope is forbidden")
    return field + " in (" + ",".join(literal(v) for v in sorted(set(values))) + ")"


class Probe:
    """Each immutable receipt records request, time, hash and full bounded payload."""

    def __init__(self, live=False, directory=OUT / "observations"):
        self.live = live
        self.directory = directory
        self.calls = 0

    def get(self, label, dataset, params=None, metadata=False):
        if dataset not in DATASETS:
            raise ValueError("source not allowlisted")
        endpoint = (f"https://www.datos.gov.co/api/views/{dataset}.json" if metadata
                    else f"https://www.datos.gov.co/resource/{dataset}.json")
        params = dict(params or {})
        if not metadata:
            params.setdefault("$limit", 10000)
            if not 1 <= int(params["$limit"]) <= 10000:
                raise ValueError("row cap exceeded")
        path = self.directory / (label + ".json")
        if path.exists():
            receipt = json.loads(path.read_text())
            if receipt["endpoint"] != endpoint or receipt["params"] != params:
                raise ValueError(f"frozen request drift: {label}")
            if hashlib.sha256(render(receipt["payload"])).hexdigest() != receipt["sha256"]:
                raise ValueError(f"corrupt observation: {label}")
        else:
            if not self.live:
                raise ValueError(f"missing frozen observation: {label}")
            if self.calls >= 120:
                raise ValueError("request budget exhausted")
            self.calls += 1
            receipt = {"endpoint": endpoint, "params": params,
                       "observed_at": datetime.now(timezone.utc).isoformat()}
            try:
                with requests.get(endpoint, params=params, timeout=35, stream=True) as response:
                    response.raise_for_status()
                    chunks, size = [], 0
                    for chunk in response.iter_content(65536):
                        size += len(chunk)
                        if size > 8_000_000:
                            raise ValueError("response byte cap exceeded")
                        chunks.append(chunk)
                    payload = json.loads(b"".join(chunks))
                if metadata:
                    payload = {k: payload.get(k) for k in (
                        "id", "name", "description", "attribution", "rowsUpdatedAt", "columns")}
                elif not isinstance(payload, list):
                    raise ValueError("resource payload is not a list")
                receipt.update(status="MATERIALIZED", payload=payload, error=None)
            except (requests.RequestException, ValueError) as exc:
                receipt.update(status="FAILED", payload=None, error=str(exc))
            receipt["sha256"] = hashlib.sha256(render(receipt["payload"])).hexdigest()
            save(path, receipt)
            print(label, receipt["status"], flush=True)
        if receipt["status"] != "MATERIALIZED":
            raise ValueError(f"non-affirmative source observation: {label}: {receipt['error']}")
        return receipt["payload"]


def metadata(probe):
    return {dataset: probe.get("schema_" + dataset, dataset, metadata=True)
            for dataset in DATASETS}
