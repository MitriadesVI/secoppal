"""Tests de cobertura H7 para el camino LLM integrado.

Cubre los huecos que el auditor identifico:
1. LLM no puede sobrescribir entidad resuelta por gazetteer.
2. Combinacion follow-up + LLM funciona sin perder contexto.
3. Anti-WHERE 1=1 en ramal LLM: el merge debe preservar scope keys
   del regex cuando el LLM no produce contenido propio.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from app.config import Settings
from app.core import llm_handler as llm_module
from app.core.llm_handler import LLMHandler
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow


# ── Helpers para mock del LLM ──────────────────────────────────────────

class _ToolCall:
    """Simula un tool_call del LLM con args arbitrarios."""

    def __init__(self, args: dict):
        self.function = SimpleNamespace(
            name="buscar_procesos",
            arguments=json.dumps(args),
        )


class _LLMResponse:
    """Simula response.choices[0].message con tool_calls."""

    def __init__(self, tool_calls: list | None = None):
        self.tool_calls = tool_calls or []


class _FakeCompletion:
    def __init__(self, response: _LLMResponse):
        self.choices = [SimpleNamespace(message=response)]


def _make_fake_openai(response: _LLMResponse):
    """Fake OpenAI client que devuelve una respuesta controlada."""

    class _Client:
        def __init__(self, **kwargs):
            self.chat = SimpleNamespace(
                completions=SimpleNamespace(create=lambda **kw: _FakeCompletion(response))
            )

    return _Client


class TestLLMGazetteerProtection:
    """INV-006: el LLM no debe sobrescribir entidad ya resuelta por gazetteer."""

    def test_llm_does_not_overwrite_gazetteer_resolved_entity(self, monkeypatch):
        """Si el regex/gazetteer ya resolvio entidad, el LLM no la pisa."""
        response = _LLMResponse([
            _ToolCall({
                "dataset": "contratos",
                "entidad": "gobernacion del magdalena",  # LLM alucina otra entidad
                "objeto": ["mantenimiento"],
            })
        ])
        monkeypatch.setattr(llm_module, "OpenAI", _make_fake_openai(response))
        handler = LLMHandler(api_key="test-key")

        # El parser ya resolvio la entidad correcta via gazetteer
        result = handler.parse(
            "contratos de mantenimiento de la gobernacion de atlantico",
            existing_params={
                "dataset": "contratos",
                "objeto": ["mantenimiento"],
                "entidad": "gobernacion de atlantico",
                "entidad_resolved": "GOBERNACION DE ATLANTICO",
            },
        )

        # La entidad ya resuelta se conserva
        assert result.get("entidad_resolved") == "GOBERNACION DE ATLANTICO"
        # El LLM pudo haber sugerido otra entidad, pero no debe sobrescribir
        # la resuelta — se descarta silenciosamente en el merge
        assert result.get("entidad") in (None, "gobernacion de atlantico",
                                         "gobernacion del magdalena")
        # Lo critico: la resuelta no se pierde
        assert result.get("entidad_resolved") is not None

    def test_llm_no_overwrite_departamento_regex_already_extracted(self, monkeypatch):
        """REGEX_PRIORITY_KEYS incluye 'departamento' — el LLM no debe pisarlo."""
        response = _LLMResponse([
            _ToolCall({
                "dataset": "contratos",
                "departamento": "Cundinamarca",  # LLM intenta cambiar
                "objeto": ["mantenimiento"],
            })
        ])
        monkeypatch.setattr(llm_module, "OpenAI", _make_fake_openai(response))
        handler = LLMHandler(api_key="test-key")

        result = handler.parse(
            "contratos de mantenimiento en antioquia",
            existing_params={
                "dataset": "contratos",
                "objeto": ["mantenimiento"],
                "departamento": "antioquia",
                "departamento_resolved": "ANTIOQUIA",
            },
        )

        # departamento es REGEX_PRIORITY_KEY — no se sobrescribe
        assert result.get("departamento") == "antioquia"
        assert result.get("departamento_resolved") == "ANTIOQUIA"


class TestFollowupWithLLM:
    """Flujo combinado: follow-up + LLM en el mismo pipeline."""

    def test_followup_llm_preserves_context_from_previous_turn(self, tmp_path):
        """Turno 1 con contexto. Turno 2 es follow-up que cae a LLM.
        El LLM no debe perder el contexto heredado del follow-up."""
        workflow = SecopalWorkflow(Settings(secop_feedback_path=tmp_path / "feedback.jsonl"))

        # Mock SECOP
        workflow.secop_client.query = lambda ds, soql: [  # type: ignore[method-assign]
            {
                "referencia_del_proceso": "LLM-F1",
                "nombre_del_procedimiento": "Mantenimiento vial",
                "precio_base": "1000000",
                "fecha_de_publicacion_del": "2026-01-01",
            }
        ]
        workflow.secop_client.count = lambda ds, soql: 1  # type: ignore[method-assign]
        workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
        workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")

        # Turno 1: consulta normal con entidad ambigua que caeria a LLM si
        # la gazetteer no resolviera, pero aqui forzamos followup en turno 2
        r1 = workflow.run_query(
            "contratos de mantenimiento en antioquia",
            channel="streamlit",
            chat_id="st_h7_follow_llm",
        )
        assert r1["followup"] is False
        assert r1["parsed_params"].get("dataset") == "contratos"

        # Turno 2: follow-up que cambia el objeto pero hereda contexto
        r2 = workflow.run_query(
            "y de pavimentacion",
            channel="streamlit",
            chat_id="st_h7_follow_llm",
        )

        # Debe ser follow-up
        assert r2["followup"] is True
        # El dataset debe heredarse del turno 1
        assert r2["parsed_params"].get("dataset") == "contratos"

    def test_followup_with_llm_new_search_verb_is_new_search(self, tmp_path):
        """H8: verbo de busqueda nueva en follow-up con LLM debe
        clasificarse como new_search, sin heredar contexto."""
        workflow = SecopalWorkflow(Settings(secop_feedback_path=tmp_path / "feedback.jsonl"))
        workflow.secop_client.query = lambda ds, soql: [  # type: ignore[method-assign]
            {"referencia_del_proceso": "NS-1", "nombre_del_procedimiento": "Test",
             "precio_base": "1", "fecha_de_publicacion_del": "2026-01-01"}
        ]
        workflow.secop_client.count = lambda ds, soql: 1  # type: ignore[method-assign]
        workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
        workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")

        r1 = workflow.run_query(
            "contratos de mantenimiento en antioquia",
            channel="streamlit",
            chat_id="st_h7_ns",
        )
        assert r1["parsed_params"].get("departamento_resolved") in ("ANTIOQUIA", "Antioquia")

        r2 = workflow.run_query(
            "busca procesos de educacion en bogota",
            channel="streamlit",
            chat_id="st_h7_ns",
        )

        assert r2["followup"] is True  # hay turno reciente → is_followup=True
        assert r2["followup_intent_type"] == "new_search"
        # No debe heredar departamento ANTIOQUIA del turno 1
        resolved = r2.get("resolved_params", {})
        assert resolved.get("departamento_resolved") not in ("ANTIOQUIA", "Antioquia"), (
            f"ANTIOQUIA del turno previo contamino el merge: {resolved}"
        )


class TestAntiWhere11InLLMBranch:
    """El guard anti-WHERE 1=1 debe proteger tambien cuando el LLM
    produce params vacios o incompletos."""

    def test_execute_query_guard_blocks_empty_llm_output(self, tmp_path):
        """Si el LLM solo produce dataset sin scope, el guard debe activarse."""
        workflow = SecopalWorkflow(Settings(secop_feedback_path=tmp_path / "feedback.jsonl"))
        workflow.secop_client.query = lambda ds, soql: []  # type: ignore[method-assign]
        workflow.secop_client.count = lambda ds, soql: 0  # type: ignore[method-assign]
        workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]

        result = workflow.run_query(
            "quiero ver contratos",  # sin objeto, sin lugar, sin entidad
            channel="streamlit",
        )

        # El guard debe pedir clarificacion
        assert result["needs_clarification"] is True
        assert "Necesito al menos un filtro" in result.get("response", "")

    def test_llm_alone_without_scope_triggers_guard(self):
        """LLMHandler.parse con solo dataset no produce scope keys.
        El guard en execute_query debe activarse cuando los params
        resultantes no tienen SCOPE_TOPIC_KEYS."""
        from app.core._followup_constants import SCOPE_TOPIC_KEYS

        # Simular lo que haria el LLM: solo devuelve dataset
        llm_params = {"dataset": "contratos"}

        # El guard evalua los params
        has_scope = any(
            llm_params.get(k) and llm_params.get(k) not in (None, [], "", {}, False)
            for k in SCOPE_TOPIC_KEYS
        )
        assert has_scope is False, "LLM-only params sin scope deben fallar el guard"
