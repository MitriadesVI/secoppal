"""B2 — DISCOURSE-FILLER: verbos interrogativos scrubeados."""

import pytest

from app.core.dialogue_policy import classify


@pytest.mark.parametrize("query", [
    "tienes algun proceso abierto para construir",
    "hay procesos de mantenimiento en bogota",
    "puedes mostrarme contratos de educación",
    "sabes si hay licitaciones abiertas",
])
def test_interrogative_verbs_scrubbed(query):
    params = {"objeto": query.split()}
    signals = classify(query, params, None)
    filler = [s for s in signals if s.kind == "discourse_filler"]
    assert filler
    first_token = query.split()[0].lower()
    assert first_token in filler[0].payload["tokens_to_scrub"]


@pytest.mark.parametrize("query,verb", [
    ("busco contratos de mantenimiento", "busco"),
    ("quiero ver licitaciones abiertas", "quiero"),
    ("necesito procesos en bogotá", "necesito"),
])
def test_first_person_verbs_NOT_scrubbed(query, verb):
    params = {"objeto": query.split()}
    signals = classify(query, params, None)
    filler = [s for s in signals if s.kind == "discourse_filler"]
    if filler:
        assert verb not in filler[0].payload.get("tokens_to_scrub", [])
