"""C4B — the productive residual-role contract cannot drift from the frozen winner.

The laboratory is closed: B_LLM won on a frozen instruction, a frozen request and
a host validation whose every failure fails closed. These tests pin the three of
them against the artifacts the C4A.4/C4A.5 commits froze, and pin the activation
rule (an empty residual is zero calls).

The frozen files are read only; nothing here rewrites them.
"""
import hashlib
import importlib.util
import json
import pathlib

import pytest

from app.config import Settings
from app.core.accreditation import SUPPLIERS, capability_spec, credential_from_observation
from app.core.accreditation.assembly import AssemblyStatus, assemble
from app.core.accreditation.residual_verifier import (
    FROZEN_INSTRUCTION_SHA256,
    LABELS,
    MODEL,
    NOT_CALLED,
    PROVIDER_ABSENT,
    SUPPLIERS_CONTRACT,
    SUPPLIERS_HUMAN_JOB,
    SUPPLIERS_ROLE,
    ResidualUnit,
    instruction_sha256,
    provider_request,
    residual_role_contract,
    units_of,
    validate,
    verify_residual,
)
from app.core.entity_resolver import EntityResolver
from app.core.natural_alpha.models import GroundedJob, Observation
from app.core.natural_alpha.provider import ProviderResult

ROOT = pathlib.Path(__file__).resolve().parents[1]
EXPERIMENT = ROOT / "experiments" / "natural_job_prelegacy_v2"
FROZEN_PROMPT = EXPERIMENT / "residual_role_prompt_b.txt"
MANIFEST = EXPERIMENT / "residual_role_manifest.json"
SPIKE = EXPERIMENT / "residual_role_spike.py"
RAW = "con quien ha contratado la alcaldia de galapa en 2025"


def frozen_spike():
    """The frozen spike module, loaded straight from its artifact path."""
    spec = importlib.util.spec_from_file_location("residual_role_spike_frozen", SPIKE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def world(tmp_path):
    settings = Settings(
        _env_file=None, secoppal_a2_suppliers_binding_enabled=True,
        secop_feedback_path=tmp_path / "traces.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.db",
    )
    return settings, EntityResolver(settings.secop_alias_db_path)


def assemble_with(provider, *, raw=RAW, settings, resolver):
    observation = Observation("alpha", "MATCHED",
                              (GroundedJob("ANALYZE", "SUPPLIERS", 0, len(raw)),),
                              provider_calls=1)
    credential = credential_from_observation(observation, raw, settings=settings)
    assert credential is not None
    spec = capability_spec(SUPPLIERS)
    assert spec is not None
    return assemble(raw, credential, spec, entity_resolver=resolver,
                    residual_verifier=provider)


class RecordingProvider:
    """A provider that answers with whatever payload it was handed and counts calls."""

    def __init__(self, payload: str = "", error: str = ""):
        self.payload = payload
        self.error = error
        self.calls: list[dict] = []

    def observe(self, call_key, request):
        self.calls.append(request)
        return ProviderResult(payload=self.payload, error=self.error, provider_calls=1)


# ── the frozen instruction ───────────────────────────────────────────────────

def test_productive_instruction_is_the_frozen_prompt_byte_for_byte():
    frozen = FROZEN_PROMPT.read_bytes()
    assert hashlib.sha256(frozen).hexdigest() == FROZEN_INSTRUCTION_SHA256
    assert SUPPLIERS_ROLE.instructions.encode("utf-8") == frozen
    assert instruction_sha256() == FROZEN_INSTRUCTION_SHA256
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert manifest["files"]["residual_role_prompt_b.txt"]["sha256"] == FROZEN_INSTRUCTION_SHA256


def test_the_contract_is_declared_for_suppliers_and_for_nothing_else():
    assert residual_role_contract(SUPPLIERS) is SUPPLIERS_ROLE
    assert residual_role_contract("entity_profile.other") is None
    assert residual_role_contract(None) is None
    assert SUPPLIERS_ROLE.human_job == SUPPLIERS_HUMAN_JOB
    assert SUPPLIERS_ROLE.contract == SUPPLIERS_CONTRACT
    assert SUPPLIERS_ROLE.name == "residual-role-base-v1+residual-role-suppliers-v1"


# ── the wire: same request the frozen spike sent ─────────────────────────────

def test_request_matches_the_frozen_spike_exactly():
    units = units_of(("contratado", "educación"))
    spike = frozen_spike()
    expected = spike._request(
        SUPPLIERS_ROLE.instructions, RAW,
        [{"id": unit.id, "text": unit.text} for unit in units],
    )
    assert provider_request(SUPPLIERS_ROLE, RAW, units) == expected
    # The wire keeps the spike's rendering: a list of `id: "text"` strings, not objects.
    body = json.loads(json.loads(provider_request(SUPPLIERS_ROLE, RAW, units)["input"])["text"]
                      .split("\n\nENTRADA\n", 1)[1])
    assert body == {"raw_query": RAW, "human_job": SUPPLIERS_HUMAN_JOB,
                    "residual_units": ['R1: "contratado"', 'R2: "educación"']}
    assert provider_request(SUPPLIERS_ROLE, RAW, units)["model"] == MODEL == spike.MODEL


def test_request_keeps_the_frozen_provider_parameters():
    request = provider_request(SUPPLIERS_ROLE, RAW, units_of(("contratado",)))
    assert request["model"] == MODEL == "deepseek-v4-flash"
    assert request["temperature"] == 0
    assert request["reasoning"] == {"effort": "none"}
    assert request["tools"] == [] and request["max_output_tokens"] == 512
    assert request["text"] == {"format": {"type": "json_object"}}


# ── host validation: identical to the spike, and every failure fails closed ──

def test_validation_agrees_with_the_frozen_spike():
    units = units_of(("contratado", "mas"))
    spike = frozen_spike()
    spike_units = [{"id": unit.id, "text": unit.text} for unit in units]
    payloads = (
        '{"labels": [{"id": "R1", "label": "JOB_LANGUAGE"}, {"id": "R2", "label": "MATERIAL"}]}',
        '{"labels": [{"id": "R1", "label": "JOB_LANGUAGE"}]}',
        '{"labels": [{"id": "R1", "label": "JOB_LANGUAGE"}, {"id": "R2", "label": "JOB_LANGUAGE"}, '
        '{"id": "R3", "label": "JOB_LANGUAGE"}]}',
        '{"labels": [{"id": "R1", "label": "JOB_LANGUAGE"}, {"id": "R1", "label": "MATERIAL"}]}',
        '{"labels": [{"id": "R1", "label": "MAYBE"}]}',
        '{"labels": [{"id": "R1"}]}',
        "not json",
        '{"jobs": []}',
    )
    for payload in payloads:
        assert validate(payload, units) == spike._validate(payload, spike_units), payload


@pytest.mark.parametrize("payload,status", [
    ('{"labels": [{"id": "R1", "label": "JOB_LANGUAGE"}]}', "MISSING_ID:['R2']"),
    ('{"labels": [{"id": "R1", "label": "JOB_LANGUAGE"}, {"id": "R2", "label": "JOB_LANGUAGE"},'
     ' {"id": "R9", "label": "JOB_LANGUAGE"}]}', "UNKNOWN_ID:['R9']"),
    ('{"labels": [{"id": "R1", "label": "JOB_LANGUAGE"}, {"id": "R1", "label": "MATERIAL"},'
     ' {"id": "R2", "label": "JOB_LANGUAGE"}]}', "DUPLICATE_ID:R1"),
    ('{"labels": [{"id": "R1", "label": "MAYBE"}, {"id": "R2", "label": "JOB_LANGUAGE"}]}',
     "INVALID_LABEL:MAYBE"),
    ("{oops", "MALFORMED:JSONDecodeError"),
    ('[1, 2]', "MALFORMED:TypeError"),
    ('{"labels": [{"id": "R1", "label": "JOB_LANGUAGE"}, 7]}', "MALFORMED:SHAPE"),
])
def test_every_invalid_answer_is_refused_with_the_same_status_the_spike_used(payload, status):
    units = units_of(("contratado", "mas"))
    labels, validation = validate(payload, units)
    assert labels is None and validation == status
    assert status.split(":")[0] in ("MALFORMED", "INVALID_LABEL", "DUPLICATE_ID", "UNKNOWN_ID",
                                    "MISSING_ID")


def test_the_closed_label_set_is_the_frozen_one():
    assert set(LABELS) == {"JOB_LANGUAGE", "MATERIAL", "UNKNOWN"}
    units = units_of(("contratado",))
    for label in LABELS:
        labels, status = validate(f'{{"labels": [{{"id": "R1", "label": "{label}"}}]}}', units)
        assert labels == {"R1": label} and status == "OK"


# ── activation: an empty residual is zero calls, and every error is a refusal ──

def test_empty_residual_never_builds_a_provider(tmp_path):
    spy = RecordingProvider()
    verification = verify_residual(RAW, (), contract=SUPPLIERS_ROLE, provider=spy)
    assert verification.called is False and verification.status == NOT_CALLED
    assert verification.units == () and spy.calls == []
    assert verification.adjudicated is False
    receipts = verification.receipt_fields()
    assert receipts["residual_verifier_called"] is False
    assert receipts["residual_labels"] == {} and receipts["residual_units"] == []


def test_several_residual_units_get_exactly_one_id_each(tmp_path):
    spy = RecordingProvider(json.dumps({"labels": [
        {"id": "R1", "label": "JOB_LANGUAGE"}, {"id": "R2", "label": "MATERIAL"}]}))
    verification = verify_residual(RAW, ("contratado", "educación"),
                                   contract=SUPPLIERS_ROLE, provider=spy)
    assert [unit.id for unit in verification.units] == ["R1", "R2"]
    assert verification.called is True and verification.status == "OK"
    assert verification.adjudicated is False
    assert verification.receipt_fields()["residual_labels"] == {"R1": "JOB_LANGUAGE",
                                                                "R2": "MATERIAL"}
    assert len(spy.calls) == 1


def test_a_missing_provider_is_a_refusal_never_an_approval():
    verification = verify_residual(RAW, ("contratado",), contract=SUPPLIERS_ROLE, provider=None)
    assert verification.called is False and verification.status == PROVIDER_ABSENT
    assert verification.adjudicated is False


@pytest.mark.parametrize("error", ["PROVIDER_FAILURE", "PROVIDER_TIMEOUT",
                                   "MISSING_OUTPUT_TEXT", "PROVIDER_CONTRACT_INCOMPATIBLE"])
def test_provider_errors_and_timeouts_are_refusals(error):
    spy = RecordingProvider(error=error)
    verification = verify_residual(RAW, ("contratado",), contract=SUPPLIERS_ROLE, provider=spy)
    assert verification.called is True
    assert verification.status == f"PROVIDER_ERROR:{error}"
    assert verification.adjudicated is False
    assert len(spy.calls) == 1  # one bounded attempt, never a retry


@pytest.mark.parametrize("payload", [
    "{oops", '{"labels": []}', '{"labels": [{"id": "R1", "label": "MAYBE"}]}',
    '{"labels": [{"id": "R7", "label": "JOB_LANGUAGE"}]}',
])
def test_a_refused_answer_stops_the_turn_without_calling_the_weak_guard(tmp_path, payload):
    """Material residual plus any refused answer is CLARIFY, and the guard is not asked."""
    settings, resolver = world(tmp_path)
    spy = RecordingProvider(payload)
    raw = "proveedores de galapa en obras civiles"
    assembly = assemble_with(spy, raw=raw, settings=settings, resolver=resolver)
    verification = assembly.residual_verification
    assert verification is not None and verification.status != "OK"
    assert assembly.status is AssemblyStatus.CLARIFY
    assert assembly.residual_material, "the residual stayed visible"
    assert assembly.weak_actor_guard_called is False
    assert assembly.authority_receipts[-1].status == "NOT_RUN"
    assert len(spy.calls) == 1


def test_an_approving_answer_lets_the_weak_guard_be_asked(tmp_path):
    """The other half of the order: approval is what reaches the weak actor guard."""
    settings, resolver = world(tmp_path)
    spy = RecordingProvider(json.dumps({"labels": [
        {"id": "R1", "label": "JOB_LANGUAGE"}, {"id": "R2", "label": "JOB_LANGUAGE"}]}))
    raw = "proveedores de galapa en obras civiles"
    assembly = assemble_with(spy, raw=raw, settings=settings, resolver=resolver)
    assert assembly.weak_actor_guard_called is True
    assert assembly.authority_receipts[-1].status in ("ABSENT", "CLARIFY")
    assert assembly.status is AssemblyStatus.READY or assembly.reason == "ACTOR_MENTION_PRESENT"
    assert len(spy.calls) == 1


def test_the_receipt_never_carries_the_provider_credential(tmp_path):
    spy = RecordingProvider(json.dumps({"labels": [{"id": "R1", "label": "JOB_LANGUAGE"}]}))
    verification = verify_residual(RAW, ("contratado",), contract=SUPPLIERS_ROLE, provider=spy)
    receipt = json.dumps(verification.receipt_fields(), ensure_ascii=False)
    assert "sk-" not in receipt and "api_key" not in receipt
    assert receipt == json.dumps(verification.receipt_fields(), ensure_ascii=False)


def test_the_assembly_reports_the_verifier_and_the_guard_it_did_not_call(tmp_path):
    settings, resolver = world(tmp_path)
    spy = RecordingProvider(json.dumps({"labels": [
        {"id": "R1", "label": "JOB_LANGUAGE"}, {"id": "R2", "label": "MATERIAL"}]}))
    raw = "proveedores de galapa en obras civiles"
    assembly = assemble_with(spy, raw=raw, settings=settings, resolver=resolver)
    assert assembly.status is AssemblyStatus.CLARIFY
    assert assembly.reason == "UNACCREDITED_RESIDUAL"
    assert assembly.weak_actor_guard_called is False
    verification = assembly.residual_verification
    assert verification is not None and verification.called is True
    assert verification.status == "OK" and verification.adjudicated is False
    receipts = assembly.receipt_fields()
    assert receipts["weak_actor_guard_called"] is False
    assert receipts["residual_verifier_called"] is True
    assert receipts["residual_labels"] == {"R1": "JOB_LANGUAGE", "R2": "MATERIAL"}
    assert receipts["residual_units"] == [{"id": "R1", "text": "obras"},
                                          {"id": "R2", "text": "civiles"}]


def test_units_are_numbered_by_the_host_and_never_by_the_model():
    units = units_of(("uno", "dos", "tres"))
    assert units == (ResidualUnit("R1", "uno"), ResidualUnit("R2", "dos"), ResidualUnit("R3", "tres"))
