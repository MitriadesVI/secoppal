"""Harness de Product Contract: workflow real, fuente congelada, persistencia confinada.

Composición (plan §1.2): consulta → ``SecopalWorkflow.run_query`` real → resolución
buyer/entity/actor real → SoQL real evaluado sobre filas congeladas → persistencia
y follow-up reales en un directorio del escenario → resultado visible.
La red la prohíbe ``conftest.py`` (ledger autouse); aquí no se mockea ninguna ruta.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from app.config import Settings, get_settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from tests.product_contract.assertions import evaluate
from tests.product_contract.frozen_source import FrozenTransport, InjectedFailure, frozen_client

DEFAULT_TODAY = date(2026, 9, 8)


@dataclass
class Turn:
    text: str
    chat_id: str
    result: dict
    source_calls: list[dict]
    previous_params: dict | None = None
    lineage_views: tuple[Any, ...] = ()

    @property
    def params(self) -> dict:
        return self.result.get("resolved_params") or {}

    def violations(self, expect: Mapping) -> list[str]:
        return evaluate(self.result, self.source_calls, expect, self.previous_params,
                        lineage_views=self.lineage_views)


@dataclass
class ProductHarness:
    root: Path
    monkeypatch: object
    rows_by_dataset: Mapping[str, Sequence[Mapping]] = field(default_factory=dict)
    failures: Sequence[InjectedFailure] = ()
    actor_index: Path | None = None
    frame: bool = False
    today: date = DEFAULT_TODAY
    turns: list[Turn] = field(default_factory=list)
    lineage_views: list[Any] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        mp = self.monkeypatch
        mp.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "true" if self.frame else "false")
        mp.setenv("SECOPPAL_ACTOR_PROFILE_ENABLED", "true")
        mp.setenv("CONVERSATION_STORE_DIR", str(self.root / "conversations"))
        get_settings.cache_clear()
        self.settings = Settings(
            deepseek_api_key=None,
            secop_app_token=None,
            secop_actor_alias_index_path=self.actor_index or self.root / "no_actor_index.sqlite",
            secop_feedback_path=self.root / "feedback.jsonl",
            secop_feedback_db_path=self.root / "secoppal.db",
            secoppal_entity_profile_frame_enabled=self.frame,
        )
        self.transport = FrozenTransport(self.rows_by_dataset, failures=self.failures)
        self.flow = SecopalWorkflow(self.settings)
        self.flow.conv_store = ConversationStore(self.root / "conversations")
        self.flow.secop_client = frozen_client(self.transport, timeout=self.settings.secop_timeout_seconds)
        # QueryRouter's public opt-in returns the product's own immutable trace.
        # Observe each parser invocation; no lineage model is reconstructed here.
        parse = self.flow.query_router.parse

        def traced_parse(*args, **kwargs):
            kwargs.setdefault("text_lineage_request_id", f"product-contract:{len(self.lineage_views)}")
            parsed = parse(*args, **kwargs)
            if parsed.text_lineage is not None:
                self.lineage_views.append(parsed.text_lineage)
            return parsed

        self.flow.query_router.parse = traced_parse
        escaped = self.unconfined_paths()
        if escaped:
            raise AssertionError(f"product harness persistence escapes the scenario root: {escaped}")

    def unconfined_paths(self) -> list[str]:
        """Toda ruta de escritura del workflow debe vivir bajo ``root`` (persistencia determinista)."""
        root = self.root.resolve()
        candidates: list[Path] = [
            Path(self.settings.secop_feedback_path),
            Path(self.settings.secop_feedback_db_path),
            Path(self.settings.secop_actor_alias_index_path),
            Path(self.flow.conv_store.base_path),
        ]
        for store in (self.flow.feedback, self.flow.radar_store):
            candidates += [v for v in vars(store).values() if isinstance(v, Path)]
        return sorted({str(p) for p in candidates if not p.resolve().is_relative_to(root)})

    def say(self, text: str, *, chat_id: str = "product-contract", **run_kwargs) -> Turn:
        start = len(self.transport.calls)
        lineage_start = len(self.lineage_views)
        result = self.flow.run_query(text, chat_id=chat_id, today=self.today, **run_kwargs)
        calls = self.transport.calls[start:]
        errors = [call for call in calls if "error" in call]
        if errors:
            raise AssertionError(f"frozen source rejected emitted SoQL: {errors}")
        previous = next((t for t in reversed(self.turns) if t.chat_id == chat_id), None)
        turn = Turn(text, chat_id, result, calls, dict(previous.params) if previous else None,
                    tuple(self.lineage_views[lineage_start:]))
        self.turns.append(turn)
        return turn
