"""Adaptador de observación E2E para V1.2-B: workflow real, disco real, sin red.

`entity_profile_frame_harness.py` observa el NÚCLEO (claims → Frame → plan).
Aquí se observa el TRAYECTO: `run_query` con la admisión encendida, un
`SecopalWorkflow` nuevo por turno y un `ConversationStore` en disco temporal.
Un workflow nuevo por turno no es adorno: es la única forma de comprobar que el
estado viajó por lo que se PUBLICÓ y no por un atributo que sobrevivió en
memoria.

El cliente SECOP es el simulado de `test_entity_profile_workflow`; no se abre
ninguna conexión y no se construye `Settings()` fuera de las fixtures de pytest.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from app.config import Settings, get_settings
from app.core.conversation_store import ConversationStore
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.orchestrator import SecopalWorkflow
from app.core.workflow.continuation import ContinuationContract
from test_entity_profile_workflow import WorkflowClient


TODAY = date(2026, 9, 7)
BUYER = "alcaldia de baranoa"


@dataclass(frozen=True)
class TurnObservation:
    """Lo observable de un turno publicado: entrada, ruta, SQL y sobre."""

    query: str
    result: dict
    calls: tuple[tuple[str, str, str], ...]

    @property
    def route(self) -> str:
        return self.result.get("route_reason", "")

    @property
    def response(self) -> str:
        return self.result.get("response", "")

    @property
    def clarifies(self) -> bool:
        return bool(self.result.get("needs_clarification"))

    @property
    def sql(self) -> tuple[str, ...]:
        return tuple(soql for _, _, soql in self.calls)

    @property
    def datasets(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(dataset for _, dataset, _ in self.calls))

    @property
    def revision(self) -> int:
        return int(self.result.get("conversation_revision") or 0)

    @property
    def owns_profile(self) -> bool:
        return self.result.get("analytical_intent") == "entity_profile"

    @property
    def planned_jobs(self) -> int:
        return int(self.result.get("profile_planned_jobs") or 0)

    @property
    def continuation(self) -> ContinuationContract | None:
        raw = self.result.get("continuation")
        return ContinuationContract.from_dict(raw) if raw else None


class Conversation:
    """Un chat: cada turno construye su propio workflow y su propio cliente."""

    def __init__(self, chat_id: str, *, client_factory=WorkflowClient) -> None:
        self.chat_id = chat_id
        self._client_factory = client_factory
        self.turns: list[TurnObservation] = []

    def workflow(self, client=None) -> tuple[SecopalWorkflow, WorkflowClient]:
        get_settings.cache_clear()
        workflow = SecopalWorkflow(Settings())
        fake = client if client is not None else self._client_factory()
        workflow.secop_client = fake
        workflow.narrator.narrate_with_grounding = lambda **_kwargs: None
        return workflow, fake

    def say(self, query: str, *, client=None, **kwargs) -> TurnObservation:
        workflow, fake = self.workflow(client)
        result = workflow.run_query(
            query, channel="streamlit", chat_id=self.chat_id, today=TODAY, **kwargs,
        )
        observation = TurnObservation(query, result, tuple(fake.calls))
        self.turns.append(observation)
        return observation

    # ── Lo que quedó PUBLICADO, leído como lo leería otra instancia ────────

    def published(self):
        return ConversationStore().load_envelope(self.chat_id)

    def profile_state(self) -> ProfileConversationState:
        read = self.published()
        if not read.usable:
            return ProfileConversationState()
        return ProfileConversationState.from_dict(read.envelope.profile_state)

    def contract(self) -> ContinuationContract | None:
        read = self.published()
        if not read.usable or not read.envelope.continuation:
            return None
        return ContinuationContract.from_dict(read.envelope.continuation)

    def execution_record(self) -> dict | None:
        read = self.published()
        return read.envelope.execution_record if read.usable else None


class FailingClient(WorkflowClient):
    """Todas las fuentes fallan: un fallo TOTAL no es una ausencia de actividad."""

    def aggregate(self, dataset: str, soql: str):
        self._record("aggregate", dataset, soql)
        raise TimeoutError("source unavailable")

    def query(self, dataset: str, soql: str):
        self._record("query", dataset, soql)
        raise TimeoutError("source unavailable")


class UnreadableValueClient(WorkflowClient):
    """El transporte responde, pero el monto es ilegible; el conteo no lo es."""

    def aggregate(self, dataset: str, soql: str):
        rows = super().aggregate(dataset, soql)
        return [
            {**row, "total_contract_value": "NaN"}
            if "total_contract_value" in row else row
            for row in rows
        ]


def sql_mentions(observation: TurnObservation, needle: str) -> tuple[bool, ...]:
    return tuple(needle in soql for soql in observation.sql)


__all__ = [
    "BUYER",
    "TODAY",
    "Conversation",
    "FailingClient",
    "TurnObservation",
    "UnreadableValueClient",
    "sql_mentions",
]


import pytest  # noqa: E402 — la fixture vive junto a lo que activa


@pytest.fixture
def frame_v12(monkeypatch):
    """Enciende el trayecto V1.2 COMPLETO para una prueba.

    Por entorno y no parcheando funciones: así se ejercita la ruta real de
    configuración, igual que hace la fixture de AP2. El default productivo
    sigue apagado y `test_entity_profile_frame_b_rollback` lo comprueba.
    """
    from app.config import get_settings

    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
