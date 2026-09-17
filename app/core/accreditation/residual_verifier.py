"""The residual-role verifier the C4A.4 spike adjudicated (B_LLM), as a product V1.

The laboratory is closed: B won on a frozen instruction, a frozen request and a
host validation whose every failure fails closed. This module carries the winner
into the runtime without tuning anything:

- ``ResidualRoleBase V1`` — the literal instruction (its sha256 is the frozen
  artifact's), the request shape, the closed label set and the host validation.
- ``ResidualRoleContract V1`` — one per capability; today only SUPPLIERS
  (``ANALYZE/SUPPLIERS``).

The verifier never resolves anything. It receives the raw turn, the human job and
the residual units the host already computed, and answers exactly one label per
unit id. It gets no executable params, no SQL, no QueryPlan, no tools and cannot
produce spans. Its answer can only veto: ``JOB_LANGUAGE`` for every unit lets the
assembly continue, anything else — and every error — stops the turn.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass

# The frozen contract (experiments/natural_job_prelegacy_v2/residual_role_manifest.json).
MODEL = "deepseek-v4-flash"
TEMPERATURE = 0
REASONING_EFFORT = "none"
MAX_OUTPUT_TOKENS = 512
LABELS = ("JOB_LANGUAGE", "MATERIAL", "UNKNOWN")
FROZEN_INSTRUCTION_SHA256 = "756fe27464e623d6f31334e471d6b628c8aca435eb4c7e6605b813424794bec9"

BASE_CONTRACT = "residual-role-base-v1"
SUPPLIERS_CONTRACT = "residual-role-suppliers-v1"
SUPPLIERS_HUMAN_JOB = "ANALYZE/SUPPLIERS"

NOT_CALLED = "NOT_CALLED"
NOT_REACHED = "NOT_REACHED"
OK = "OK"
PROVIDER_ABSENT = "PROVIDER_ABSENT"

# Verbatim copy of experiments/natural_job_prelegacy_v2/residual_role_prompt_b.txt.
# tests/test_residual_verifier_contract.py pins the hash of this constant against
# the frozen file: the productive contract cannot drift from the winner silently.
_INSTRUCTIONS = """Eres un verificador de residual para un buscador de contratacion publica colombiana.
Un turno ya fue interpretado como: trabajo = ANALYZE/SUPPLIERS dentro del universo de CONTRATOS.
El resto del turno quedo como unidades residuales que ninguna autoridad del host acredito.

Tu unica tarea: decidir para cada unidad si es JOB_LANGUAGE, MATERIAL o UNKNOWN.

DEFINICIONES

JOB_LANGUAGE:
lenguaje relacional o gramatical necesario para expresar quien provee, vende,
contrata, compra, adjudica o trabaja con el comprador, sin restringir que
bienes, servicios, objetos o propositos cuentan.

MATERIAL:
texto que restringe o describe materia, bien, servicio, objeto, actividad,
proposito o condicion de los contratos que deben contarse.

UNKNOWN:
no puede acreditarse con seguridad como uno de los anteriores.

REGLAS

- Juzga cada unidad en el contexto de la frase completa.
- Una unidad puede ser material aunque este en infinitivo, gerundio o participio,
  y aunque sea un verbo: describir la actividad contratada es material.
- Una unidad no es material solo porque sea un sustantivo: nombrar a un actor,
  a una cantidad o al propio acto de contratar no restringe la materia.
- Si dudas, responde UNKNOWN.
- No inventes unidades: devuelve exactamente los ids que recibiste, cada uno una vez.

FORMATO DE SALIDA (JSON, sin texto adicional)

{"labels": [{"id": "R1", "label": "JOB_LANGUAGE"}, {"id": "R2", "label": "MATERIAL"}]}

Labels permitidos: JOB_LANGUAGE, MATERIAL, UNKNOWN.
"""


@dataclass(frozen=True, slots=True)
class ResidualUnit:
    """One unit the host could not accredit. ``id`` is the host's, never the model's."""

    id: str
    text: str


@dataclass(frozen=True, slots=True)
class ResidualRoleContract:
    """What a capability declares to the verifier. Nothing else travels with it."""

    base: str
    contract: str
    human_job: str
    instructions: str = _INSTRUCTIONS

    @property
    def name(self) -> str:
        return f"{self.base}+{self.contract}"


SUPPLIERS_ROLE = ResidualRoleContract(
    base=BASE_CONTRACT, contract=SUPPLIERS_CONTRACT, human_job=SUPPLIERS_HUMAN_JOB,
)

_ROLE_BY_CAPABILITY = {"entity_profile.suppliers": SUPPLIERS_ROLE}


def residual_role_contract(capability: object) -> ResidualRoleContract | None:
    """The declared contract for a capability, or ``None`` when none is declared."""
    return _ROLE_BY_CAPABILITY.get(capability) if type(capability) is str else None


def instruction_sha256(contract: ResidualRoleContract | None = None) -> str:
    instructions = (contract or SUPPLIERS_ROLE).instructions
    return hashlib.sha256(instructions.encode("utf-8")).hexdigest()


def units_of(residual_material: tuple[str, ...]) -> tuple[ResidualUnit, ...]:
    """The ids exist only here: the model receives them and can never add one."""
    return tuple(
        ResidualUnit(f"R{index + 1}", text)
        for index, text in enumerate(residual_material)
    )


class _SettingsResidualProvider:
    """The verifier's provider, built only when a call actually happens.

    Same bounded Responses call the rest of the runtime makes (the spike's own
    ``DeepSeekResponsesProvider``): with no key the call fails closed and the turn
    clarifies. The timeout reuses the existing bounded-provider budget instead of
    adding a knob nobody asked for.
    """

    def __init__(self, settings):
        self._settings = settings
        self._provider = None

    def observe(self, call_key: str, request: dict):
        from app.core.natural_alpha.provider import DeepSeekResponsesProvider

        if self._provider is None:
            self._provider = DeepSeekResponsesProvider(
                api_key=self._settings.deepseek_api_key,
                timeout_seconds=self._settings.secoppal_natural_alpha_timeout_seconds,
            )
        return self._provider.observe(call_key, request)


def settings_residual_provider(settings) -> _SettingsResidualProvider:
    """Zero cost until the first call: no client, no import, no network."""
    return _SettingsResidualProvider(settings)


def provider_request(contract: ResidualRoleContract, raw: str,
                     units: tuple[ResidualUnit, ...]) -> dict:
    """The exact request the frozen spike sent (``residual_role_spike._request``)."""
    body = {
        "raw_query": raw,
        "human_job": contract.human_job,
        "residual_units": [f'{unit.id}: "{unit.text}"' for unit in units],
    }
    return {
        "model": MODEL,
        "temperature": TEMPERATURE,
        "reasoning": {"effort": REASONING_EFFORT},
        "tools": [],
        "max_output_tokens": MAX_OUTPUT_TOKENS,
        "text": {"format": {"type": "json_object"}},
        "input": json.dumps(
            {"text": contract.instructions + "\n\nENTRADA\n"
                     + json.dumps(body, ensure_ascii=False)},
            ensure_ascii=False,
        ),
    }


def validate(payload: str, units: tuple[ResidualUnit, ...]) -> tuple[dict | None, str]:
    """Host validation, identical to the spike's: exact ids, valid labels, all of them.

    Anything unexpected — malformed JSON, wrong shape, an unknown label, a
    duplicated id, an invented id, a missing id — returns ``None`` and a status
    the receipt can carry. A refused answer never becomes a partial one.
    """
    try:
        parsed = json.loads(payload)
        labels = parsed["labels"]
    except Exception as exc:  # noqa: BLE001 -- every failure mode fails closed
        return None, f"MALFORMED:{type(exc).__name__}"
    seen: dict[str, str] = {}
    for entry in labels:
        if not isinstance(entry, dict) or "id" not in entry or "label" not in entry:
            return None, "MALFORMED:SHAPE"
        if entry["label"] not in LABELS:
            return None, f"INVALID_LABEL:{entry.get('label')}"
        if entry["id"] in seen:
            return None, f"DUPLICATE_ID:{entry['id']}"
        seen[entry["id"]] = entry["label"]
    expected = {unit.id for unit in units}
    if set(seen) - expected:
        return None, f"UNKNOWN_ID:{sorted(set(seen) - expected)}"
    if expected - set(seen):
        return None, f"MISSING_ID:{sorted(expected - set(seen))}"
    return seen, OK


@dataclass(frozen=True, slots=True)
class ResidualVerification:
    """The verifier's receipt: what was asked, what it answered and how it ended."""

    called: bool
    contract: str
    status: str
    units: tuple[ResidualUnit, ...] = ()
    labels: tuple[tuple[str, str], ...] = ()
    latency_ms: float = 0.0

    @property
    def adjudicated(self) -> bool:
        """Only every unit labelled JOB_LANGUAGE lets the assembly continue."""
        return (self.called and self.status == OK and bool(self.units)
                and all(label == "JOB_LANGUAGE" for _, label in self.labels))

    def receipt_fields(self) -> dict:
        return {
            "residual_verifier_called": self.called,
            "residual_verifier_contract": self.contract,
            "residual_units": [{"id": unit.id, "text": unit.text} for unit in self.units],
            "residual_labels": dict(self.labels),
            "residual_verifier_status": self.status,
            "residual_verifier_latency_ms": self.latency_ms,
        }


def _not_called(contract: ResidualRoleContract, status: str,
                units: tuple[ResidualUnit, ...] = ()) -> ResidualVerification:
    return ResidualVerification(called=False, contract=contract.name, status=status,
                                units=units)


def not_reached(capability: object) -> ResidualVerification:
    """The receipt of a turn that never got to the residual point.

    A deterministic guard (modality, money, geography, negation, scope) stopped
    the turn before the residual, so the verifier was neither called nor needed:
    the receipt says exactly that instead of staying silent.
    """
    contract = residual_role_contract(capability)
    return ResidualVerification(called=False, contract=contract.name if contract else "",
                                status=NOT_REACHED)


def verify_residual(raw: str, residual_material: tuple[str, ...], *,
                    contract: ResidualRoleContract | None, provider=None,
                    call_key: str = "residual-role") -> ResidualVerification:
    """Ask the verifier about the residual units, or say honestly why it was not asked.

    An empty residual is the zero-call path: there is nothing to adjudicate, so no
    provider is built and no call is made. With units, a missing provider or a
    provider failure is a refusal, never an approval.
    """
    if contract is None:
        return ResidualVerification(called=False, contract="", status=PROVIDER_ABSENT)
    units = units_of(tuple(residual_material))
    if not units:
        return _not_called(contract, NOT_CALLED)
    if provider is None:
        return _not_called(contract, PROVIDER_ABSENT, units)
    started = time.perf_counter()
    try:
        response = provider.observe(call_key, provider_request(contract, raw, units))
    except Exception:  # noqa: BLE001 -- a provider that raises is a refusal, never an approval
        # The runtime contract is "answer or refuse": an exception thrown by the
        # provider must not travel into run_query. It becomes the same fail-closed
        # receipt a provider-reported error would leave.
        return ResidualVerification(
            called=True, contract=contract.name, status="PROVIDER_ERROR:EXCEPTION",
            units=units, latency_ms=round((time.perf_counter() - started) * 1000, 3),
        )
    latency = round((time.perf_counter() - started) * 1000, 3)
    error = getattr(response, "error", "")
    if error:
        return ResidualVerification(called=True, contract=contract.name,
                                    status=f"PROVIDER_ERROR:{error}", units=units,
                                    latency_ms=latency)
    labels, validation = validate(getattr(response, "payload", "") or "", units)
    if labels is None:
        return ResidualVerification(called=True, contract=contract.name, status=validation,
                                    units=units, latency_ms=latency)
    return ResidualVerification(called=True, contract=contract.name, status=OK, units=units,
                                labels=tuple(labels.items()), latency_ms=latency)


__all__ = [
    "BASE_CONTRACT",
    "FROZEN_INSTRUCTION_SHA256",
    "LABELS",
    "MODEL",
    "NOT_CALLED",
    "NOT_REACHED",
    "OK",
    "PROVIDER_ABSENT",
    "SUPPLIERS_CONTRACT",
    "SUPPLIERS_HUMAN_JOB",
    "SUPPLIERS_ROLE",
    "ResidualRoleContract",
    "ResidualUnit",
    "ResidualVerification",
    "instruction_sha256",
    "not_reached",
    "provider_request",
    "residual_role_contract",
    "settings_residual_provider",
    "units_of",
    "validate",
    "verify_residual",
]
