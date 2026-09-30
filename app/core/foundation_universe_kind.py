"""The universe_kind wire contract of the B/C producer (prompt 4.2 onward).

With prompts up to 4.1 the producer states an opportunity twice: as the request's
universe evidence and as a RETRIEVAL_INTENT condition citing the same evidence.
DeepSeek splits the two when a noun and an intention coexist («licitaciones …
para presentarme»), and the contract rejects the whole answer. Here the producer
says it once, "universe_kind": "opportunities", and the decoder writes the
REQUESTED_UNIVERSE condition from the producer's own universe evidence. It adds
no meaning: the claim and its evidence are the producer's; only the duplication
disappears. Admission and projection are unchanged.
"""

from __future__ import annotations

from copy import deepcopy

from app.core.interpretation_contract import ContractError

UNIVERSE_KIND_VERSIONS = frozenset({"4.2"})
KINDS = ("records", "opportunities")


def _with_atom(expression, condition_id: str) -> dict:
    atom = {"operator": "ATOM", "condition_id": condition_id}
    if expression is None:
        return atom
    if isinstance(expression, dict) and expression.get("operator") == "AND":
        return {**expression, "children": [atom, *expression.get("children", [])]}
    return {"operator": "AND", "children": [atom, expression]}


def expand_universe_kind(payload, arm: str):
    """The payload in the pre-4.2 wire shape; a malformed one is left for the decoder to reject."""
    if not isinstance(payload, dict) or not isinstance(payload.get("requests"), list):
        return payload
    conditions = payload.get("conditions")
    if not isinstance(conditions, list):
        return payload
    if any(isinstance(c, dict) and (c.get("dimension") == "RETRIEVAL_INTENT"
                                    or c.get("role") == "REQUESTED_UNIVERSE") for c in conditions):
        raise ContractError("RETRIEVAL_INTENT_REPLACED_BY_UNIVERSE_KIND")
    payload = deepcopy(payload)
    taken = {c.get("condition_id") for c in payload["conditions"] if isinstance(c, dict)}
    for request in payload["requests"]:
        if not isinstance(request, dict):
            continue
        # The claim is mandatory: a missing one must not read as "records" (ADR-101).
        if "universe_kind" not in request:
            raise ContractError("MISSING_UNIVERSE_KIND")
        kind = request.pop("universe_kind")
        if kind not in KINDS:
            raise ContractError("INVALID_UNIVERSE_KIND")
        if kind == "records":
            continue
        condition_id = f"opportunity-{request.get('request_id')}"
        while condition_id in taken:
            condition_id = "x-" + condition_id
        taken.add(condition_id)
        if arm == "B":
            evidence = {"candidate_ids": deepcopy(request.get("universe_evidence_candidate_ids"))}
        else:
            evidence = {"evidence": [deepcopy(request.get("universe_evidence"))]}
        payload["conditions"].append({
            "condition_id": condition_id, "request_ids": [request.get("request_id")],
            "dimension": "RETRIEVAL_INTENT", "operator": "is", "polarity": "POSITIVE",
            "role": "REQUESTED_UNIVERSE", **evidence,
        })
        request["condition_expression"] = _with_atom(request.get("condition_expression"), condition_id)
    return payload
