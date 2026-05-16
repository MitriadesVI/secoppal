from __future__ import annotations

import json
from types import SimpleNamespace

from app.core import llm_handler as llm_module
from app.core.llm_handler import LLMHandler, SECOPAL_TOOLS


class _FakeCompletions:
    def create(self, **kwargs):
        user_query = kwargs["messages"][-1]["content"].lower()
        if "firmad" in user_query or "suscrit" in user_query:
            args = {
                "dataset": "contratos",
                "objeto": ["mantenimiento"],
                "ciudad": "Bogotá",
                "estado": "Celebrado",
                "estado_contrato": ["Celebrado"],
                "estado_field": "estado_contrato",
            }
        elif "ejec" in user_query:
            args = {
                "dataset": "contratos",
                "objeto": ["mantenimiento"],
                "ciudad": "Bogotá",
                "estado": "En ejecucion",
                "estado_field": "estado_contrato",
            }
        elif "celebrad" in user_query:
            args = {"dataset": "contratos", "estado": "Celebrado", "estado_field": "estado_contrato"}
        else:
            args = {"dataset": "procesos"}

        tool_call = SimpleNamespace(
            function=SimpleNamespace(
                name="buscar_procesos",
                arguments=json.dumps(args),
            )
        )
        message = SimpleNamespace(tool_calls=[tool_call])
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])


class _FakeOpenAI:
    def __init__(self, **kwargs):
        self.chat = SimpleNamespace(completions=_FakeCompletions())


def _parse_with_fake_llm(monkeypatch, query: str) -> dict:
    monkeypatch.setattr(llm_module, "OpenAI", _FakeOpenAI)
    handler = LLMHandler(api_key="test-key")
    return handler.parse(query, existing_params={})


def test_llm_firmados_no_emite_estado(monkeypatch):
    """
    INV-005: 'firmados' es selección de dataset, no estado.
    Incluso si el LLM intenta emitir estado, el handler debe descartarlo.
    """
    result = _parse_with_fake_llm(monkeypatch, "contratos firmados de mantenimiento en Bogotá")

    assert result["dataset"] == "contratos"
    assert not result.get("estado_contrato")
    assert not result.get("estado")
    assert not result.get("estado_field")


def test_llm_en_ejecucion_emite_familia_activa(monkeypatch):
    """
    'En ejecución' es subconjunto operativo, sí emite estado_contrato.
    """
    result = _parse_with_fake_llm(monkeypatch, "contratos en ejecución de mantenimiento en Bogotá")

    assert result["dataset"] == "contratos"
    estados = result.get("estado_contrato", [])
    assert "En ejecución" in estados
    assert "Modificado" in estados
    assert "Prorrogado" in estados


def test_llm_firmados_que_esten_en_ejecucion_mantiene_estado_activo(monkeypatch):
    """
    B9: 'firmados que estén en ejecución' debe forzar dataset=contratos
    y aplicar el filtro de estado activo (no debe borrarlo por el kill-switch de firmados).
    """
    result = _parse_with_fake_llm(monkeypatch, "contratos firmados que estén en ejecución de mantenimiento en Bogotá")
    assert result["dataset"] == "contratos"
    estados = result.get("estado_contrato", [])
    assert "En ejecución" in estados
    assert "Modificado" in estados
    assert "Prorrogado" in estados


def test_llm_no_emite_estados_inexistentes(monkeypatch):
    """
    No emitir valores que no existen en el dataset real jbjy-vk9h.
    """
    invalid = {"Firmado", "Celebrado"}
    for query in [
        "contratos firmados",
        "contratos firmados de salud",
        "contratos en ejecución",
        "contratos celebrados",
    ]:
        result = _parse_with_fake_llm(monkeypatch, query)
        emitted = set(result.get("estado_contrato") or []) | {result.get("estado")} | {result.get("estado_field")}
        emitted.discard(None)
        assert not (emitted & invalid), f"Emitió estado inválido para '{query}': {emitted & invalid}"


def test_llm_tool_estado_enum_no_declara_estados_inexistentes():
    invalid = {"Firmado", "Celebrado"}
    estado_enum = SECOPAL_TOOLS[0]["function"]["parameters"]["properties"]["estado"]["enum"]

    assert not (invalid & set(estado_enum))
