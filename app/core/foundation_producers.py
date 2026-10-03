"""The Foundation linguistic producer (arm B, prompt v4.2) and its wire decoder.

The producer proposes linguistic meaning only; the host admits it. Only arm B is a
product producer. The decoder also reads the raw-span wire shape ("C") because
tests and replay fixtures state proposals that way; no setting can select it.

The v4.2 wire is IR v1 (operation on each request); the decoder lifts it into the IR v2 with
``foundation_lift_v1`` and adds nothing of its own (INV-C0-2).
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from types import MappingProxyType

from app.core.foundation_decoder_v2 import decode_proposal_v2
from app.core import foundation_prompts_v5
from app.core.foundation_lift_v1 import lift_level, lift_operation, lift_subject, v1_dimension, v1_job_specs
from app.core.foundation_prompts import PRODUCER_ARM, PROMPT_SHA256, PROMPT_VERSION, system_prompt
from app.core.foundation_universe_kind import expand_universe_kind
from app.core.interpretation_contract import (
    AcceptanceStatus, ConditionExpression, ConditionRole, ContractError, InterpretationAcceptance,
    InterpretationProposal, LogicOperator, Omission, Polarity, ProposedCondition, ProposedJob,
    ProposedOmission, ProposedRelation, ProposedRequest, RawSpan, RelationKind,
)


PRODUCER_VERSION = PROMPT_VERSION


def _keys(record, allowed, required=()):
    if not isinstance(record, dict) or set(record) - set(allowed) or not set(required) <= set(record):
        raise ContractError("INVALID_LINGUISTIC_RECORD_KEYS")


def host_candidates(raw: str) -> tuple[dict, ...]:
    """Bounded host span inventory; its contents are suggestions for adjudication."""
    if type(raw) is not str or len(raw) > 1200:
        raise ContractError("RAW_CAPACITY_EXCEEDED")
    words = list(re.finditer(r"\w+(?:[’'-]\w+)*", raw, re.UNICODE))
    spans = {(0, len(raw))} if raw else set()
    for width in range(1, 5):
        spans.update((words[at].start(), words[at + width - 1].end())
                     for at in range(len(words) - width + 1))
    if len(spans) > 160:
        raise ContractError("HOST_CANDIDATE_CAPACITY_EXCEEDED")
    return tuple({"candidate_id": f"span-{at}", "start": start, "end": end,
                  "quote": raw[start:end]} for at, (start, end) in enumerate(sorted(spans)))


def decode_proposal(payload: dict, raw: str, *, arm: str, candidates=()) -> InterpretationProposal:
    if arm not in {"B", "C"}:
        raise ContractError("INVALID_WIRE_ARM")
    payload = expand_universe_kind(payload, arm)
    _keys(payload, {"requests", "conditions", "relations", "omissions"}, {"requests", "conditions"})
    inventory = {candidate["candidate_id"]: candidate for candidate in candidates}

    def spans(record, field="evidence"):
        if arm == "B":
            ids = record.get("candidate_ids" if field == "evidence" else field + "_candidate_ids")
            if type(ids) is not list or not ids or any(identifier not in inventory for identifier in ids):
                raise ContractError("FOREIGN_OR_MISSING_HOST_CANDIDATE")
            values = [inventory[identifier] for identifier in ids]
        else:
            values = record.get(field)
            if field != "evidence":
                values = [values]
            if type(values) is not list or not values:
                raise ContractError("MISSING_RAW_EVIDENCE")
        result = []
        for value in values:
            _keys(value, {"start", "end", "quote", "candidate_id"}, {"start", "end", "quote"})
            result.append(RawSpan(value["start"], value["end"], value["quote"]))
        return tuple(result)

    def expression(value):
        if value is None:
            return None
        _keys(value, {"operator", "condition_id", "children"}, {"operator"})
        return ConditionExpression(LogicOperator(value["operator"]), value.get("condition_id"),
                                   tuple(expression(child) for child in value.get("children", [])))

    requests, operations = [], []
    evidence_keys = ({"candidate_ids", "universe_evidence_candidate_ids", "source_evidence_candidate_ids"}
                     if arm == "B" else {"evidence", "universe_evidence", "source_evidence"})
    for request in payload["requests"]:
        _keys(request, {"request_id", "scope_id", "operation", "subject", "condition_expression"} | evidence_keys,
              {"request_id", "scope_id", "operation", "subject"})
        universe = spans(request, "universe_evidence")[0]
        source_key = "source_evidence_candidate_ids" if arm == "B" else "source_evidence"
        source = spans(request, "source_evidence")[0] if request.get(source_key) is not None else None
        evidence = spans(request)
        operations.append((request["request_id"], lift_operation(request["operation"]), evidence))
        requests.append(ProposedRequest(
            request["request_id"], request["scope_id"], lift_subject(request["subject"]),
            evidence, source.quote if source else None, source,
            universe.quote, universe, expression(request.get("condition_expression")),
        ))
    conditions = []
    for condition in payload["conditions"]:
        _keys(condition, {"condition_id", "request_ids", "dimension", "operator", "polarity", "role",
                          "unsupported_label", "candidate_ids" if arm == "B" else "evidence"},
              {"condition_id", "request_ids", "dimension", "operator", "polarity"})
        evidence = spans(condition)
        dimension = v1_dimension(condition["dimension"])
        conditions.append(ProposedCondition(
            condition["condition_id"], tuple(condition["request_ids"]), dimension,
            condition["operator"], Polarity(condition["polarity"]), evidence[0].quote, evidence,
            condition.get("unsupported_label"), ConditionRole(condition.get("role", "FILTER")),
            lift_level(dimension),
        ))
    relations, omissions = [], []
    for relation in payload.get("relations", []):
        _keys(relation, {"kind", "members"}, {"kind", "members"})
        relations.append(ProposedRelation(RelationKind(relation["kind"]), tuple(relation["members"])))
    for omission in payload.get("omissions", []):
        _keys(omission, {"request_id", "dimension", "state"}, {"request_id", "dimension", "state"})
        omissions.append(ProposedOmission(omission["request_id"], v1_dimension(omission["dimension"]),
                                         Omission(omission["state"])))
    proposal = InterpretationProposal(hashlib.sha256(raw.encode("utf-8", errors="surrogatepass")).hexdigest(),
                                      f"foundation-{arm}", PRODUCER_VERSION, tuple(requests),
                                      tuple(conditions), tuple(relations), tuple(omissions),
                                      tuple(ProposedJob(**spec) for spec in v1_job_specs(operations)))
    proposal.validate_raw(raw)
    return proposal


@dataclass(frozen=True)
class LinguisticProfile:
    version: str
    prompt_sha256: str
    prompt: object
    decoder: object


PROFILES = MappingProxyType({
    "foundation_v3": LinguisticProfile(PROMPT_VERSION, PROMPT_SHA256, system_prompt, decode_proposal),
    "foundation_v5": LinguisticProfile(foundation_prompts_v5.PROMPT_VERSION,
                                       foundation_prompts_v5.PROMPT_SHA256,
                                       foundation_prompts_v5.system_prompt, decode_proposal_v2),
})


@dataclass
class ProducerReceipt:
    """Telemetry of one producer call: no raw text, no provider payload."""

    producer_version: str = PRODUCER_VERSION
    prompt_sha256: str = PROMPT_SHA256
    provider_model_reported: str | None = None
    provider_latency_ms: float | None = None
    provider_error: str | None = None
    provider_calls: int = 0

    def to_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass
class LinguisticProducer:
    """Explicit closed prompt/decoder profile; the active default remains v3."""

    provider: object
    profile: str = field(default="foundation_v3", kw_only=True)
    receipt: ProducerReceipt = field(default_factory=ProducerReceipt, init=False)

    def __post_init__(self):
        if type(self.profile) is not str or self.profile not in PROFILES:
            raise ContractError("INVALID_PRODUCER_PROFILE")

    def produce(self, raw: str):
        self.__post_init__()
        profile = PROFILES[self.profile]
        self.receipt = ProducerReceipt(producer_version=profile.version, prompt_sha256=profile.prompt_sha256)
        try:
            candidates = host_candidates(raw)
        except (ContractError, ValueError, TypeError) as exc:
            return InterpretationAcceptance(None, AcceptanceStatus.INVALID_PROPOSAL, reason=str(exc))
        prompt = {"arm": PRODUCER_ARM, "raw": raw, "host_candidates": candidates}
        started = time.perf_counter()
        self.receipt.provider_calls = 1
        try:
            payload = self.provider(profile.prompt(), prompt)
        except Exception as exc:
            self.receipt.provider_latency_ms = round((time.perf_counter() - started) * 1000, 1)
            self.receipt.provider_error = type(exc).__name__
            return InterpretationAcceptance(None, AcceptanceStatus.PROVIDER_ERROR,
                                            reason=f"PROVIDER_ERROR:{type(exc).__name__}")
        self.receipt.provider_latency_ms = round((time.perf_counter() - started) * 1000, 1)
        if isinstance(payload, ProviderReply):
            self.receipt.provider_model_reported = payload.model
            payload = payload.content
        # Anything the provider returned is a proposal to judge, never an outage.
        try:
            payload = json.loads(payload) if isinstance(payload, str) else payload
            return profile.decoder(payload, raw, arm=PRODUCER_ARM, candidates=candidates)
        except Exception as exc:  # noqa: BLE001 - a malformed answer is INVALID_PROPOSAL, not an outage
            reason = str(exc) if isinstance(exc, ContractError) else f"MALFORMED_PROPOSAL:{type(exc).__name__}"
            return InterpretationAcceptance(None, AcceptanceStatus.INVALID_PROPOSAL, reason=reason)


@dataclass(frozen=True)
class ProviderReply:
    content: object
    model: str | None = None


def configured_producer(workflow) -> LinguisticProducer:
    def provider(system, payload):
        from app.core.llm_handler import OpenAI

        handler = workflow.llm_handler
        if OpenAI is None or not handler.api_key:
            raise RuntimeError("PROVIDER_UNAVAILABLE")
        client = OpenAI(api_key=handler.api_key, base_url=handler.base_url, timeout=15, max_retries=0)
        response = client.chat.completions.create(
            model=workflow.settings.deepseek_model, temperature=0, max_tokens=2400,
            response_format={"type": "json_object"}, timeout=15,
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
        )
        return ProviderReply(response.choices[0].message.content, getattr(response, "model", None))

    return LinguisticProducer(provider)
